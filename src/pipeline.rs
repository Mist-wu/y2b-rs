use crate::bilibili_api::{self, CcCue};
use crate::config::{BatchMode, Config};
use crate::db::Database;
use crate::model::{
    AiUsage, Job, JobStatus, PreparedUpload, PublicationMetadata, TransferMode, VideoMetadata,
};
use crate::monitor::{Monitor, is_live_content_pending, ytdlp_command};
use crate::process::{ProcessOutput, run_monitored};
use crate::subtitle::{self, Cue};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use regex::Regex;
use serde_json::{Value, json};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::time::sleep;

const NEXT_BILIBILI_SUBMIT_AT: &str = "bilibili.next_submit_at";

pub struct Pipeline {
    pub config: Config,
    pub db: Database,
}

#[derive(Debug)]
struct PiResult {
    value: Value,
    usage: AiUsage,
    output: ProcessOutput,
}

#[derive(Debug)]
struct PreparedSubtitle {
    cues: Vec<Cue>,
}

impl Pipeline {
    pub fn new(config: Config, db: Database) -> Self {
        Self { config, db }
    }

    pub async fn run_job(&self, job: Job) -> Result<()> {
        self.prepare_job(job.clone()).await?;
        let Some(prepared) = self.db.get_job(&job.id)? else {
            bail!("准备完成后任务不存在: {}", job.id)
        };
        if prepared.status == JobStatus::ReadyToUpload {
            self.upload_prepared_job(prepared).await?;
        }
        Ok(())
    }

    pub async fn prepare_job(&self, job: Job) -> Result<()> {
        let result = self.prepare_job_inner(&job).await;
        if let Err(e) = &result {
            let live_pending = is_live_content_pending(e);
            let attempt = if live_pending {
                job.attempt
            } else {
                self.db.increment_attempt(&job.id)?
            };
            let status = if live_pending {
                // 直播预告/直播回放按策略永久跳过，不反复重试。
                JobStatus::DeadLetter
            } else if attempt >= self.config.monitor.max_attempts as i64 {
                self.cleanup_large(&job.id).ok();
                JobStatus::DeadLetter
            } else {
                JobStatus::RetryWait
            };
            self.db
                .update_job_status(&job.id, status, Some(&e.to_string()))?;
            self.db.event(
                Some(&job.id),
                if live_pending { "info" } else { "error" },
                &e.to_string(),
            )?;
        }
        result
    }

    pub async fn upload_prepared_job(&self, job: Job) -> Result<()> {
        let result = self.upload_prepared_job_inner(&job).await;
        if let Err(error) = &result {
            let attempt = self.db.increment_attempt(&job.id)?;
            let rate_limited = error.to_string().contains("21566");
            if rate_limited {
                self.defer_bilibili_submissions(self.config.bilibili.rate_limit_cooldown_seconds)?;
            }
            let status = if attempt >= self.config.monitor.max_attempts as i64 {
                JobStatus::Paused
            } else {
                JobStatus::UploadRetryWait
            };
            self.db
                .update_job_status(&job.id, status, Some(&error.to_string()))?;
            self.db.event(Some(&job.id), "error", &error.to_string())?;
        }
        result
    }

    async fn prepare_job_inner(&self, job: &Job) -> Result<()> {
        self.ensure_disk()?;
        if let Some(limit) = self.config.ai.daily_token_limit
            && self.db.ai_tokens_today()? >= limit
        {
            bail!("已达到每日 token 上限 {limit}")
        }
        self.db.set_job_model(
            &job.id,
            &self.config.ai.provider,
            &self.config.ai.model,
            &self.config.ai.thinking,
        )?;
        self.db
            .update_job_status(&job.id, JobStatus::Inspecting, None)?;
        let stage = self.db.start_stage(&job.id, "metadata", None, None, None)?;
        let monitor = Monitor::new(self.config.clone(), self.db.clone())?;
        let metadata_started = Instant::now();
        let (meta, peak, duration) = match monitor.fetch_metadata(&job.url).await {
            Ok(result) => result,
            Err(error) => {
                self.db.finish_stage(
                    stage,
                    if is_live_content_pending(&error) {
                        "deferred"
                    } else {
                        "failed"
                    },
                    metadata_started.elapsed().as_millis() as i64,
                    0,
                    Some(&error.to_string()),
                )?;
                return Err(error);
            }
        };
        self.db
            .finish_stage(stage, "completed", duration, peak, None)?;
        self.db.update_job_metadata(
            &job.id,
            &meta.title,
            meta.is_short(),
            meta.duration,
            meta.width,
            meta.height,
        )?;
        self.db.save_source_metadata(&job.id, &meta)?;
        let work = self.config.runtime.download_dir.join(&meta.id);
        fs::create_dir_all(&work)?;
        self.db
            .update_job_status(&job.id, JobStatus::Processing, None)?;
        if !requires_translated_pipeline(job.transfer_mode) {
            return self.run_direct(job, &meta, &work).await;
        }

        // 投稿失败重试（如 21566 冷却）：发布元数据已存在时直接复用下载好的
        // 原片和翻译缓存，跳过重复翻译。
        if self.db.publication_metadata(&job.id)?.is_some()
            && let Some(video) = find_video(&work, &meta.id)
        {
            self.db.event(
                Some(&job.id),
                "info",
                "检测到已下载原片，跳过字幕翻译并重试投稿",
            )?;
            self.db
                .set_job_paths(&job.id, Some(&video.to_string_lossy()))?;
            let cover = self.download_cover(&job.id, &meta, &work).await?;
            self.db.queue_prepared_upload(
                &job.id,
                &PreparedUpload::Submission {
                    video_path: video.to_string_lossy().into_owned(),
                    cover_path: cover.to_string_lossy().into_owned(),
                    mode: TransferMode::Translated,
                    completion_status: JobStatus::Completed,
                },
            )?;
            return Ok(());
        }

        let video_fut = self.download_video(&job.id, &job.url, &meta, &work);
        let subtitle_fut = self.prepare_translated_subtitle(job, &meta, &work);
        let (video, subtitle) = try_join_branches(video_fut, subtitle_fut).await?;
        self.db
            .set_job_paths(&job.id, Some(&video.to_string_lossy()))?;
        let Some(subtitle) = subtitle else {
            // 无字幕时直传原片：之后用 `y2b subtitle` 命令补 CC 字幕。
            self.publish_metadata(&job.id, TransferMode::Direct, &meta, None)
                .await?;
            return self
                .prepare_direct_upload(
                    job,
                    &meta,
                    &video,
                    JobStatus::UploadedOriginalPendingSubtitle,
                )
                .await;
        };
        self.publish_metadata(
            &job.id,
            TransferMode::Translated,
            &meta,
            Some(&subtitle.cues),
        )
        .await?;
        let prepared = {
            let cover = self.download_cover(&job.id, &meta, &work).await?;
            PreparedUpload::Submission {
                video_path: video.to_string_lossy().into_owned(),
                cover_path: cover.to_string_lossy().into_owned(),
                mode: TransferMode::Translated,
                completion_status: JobStatus::Completed,
            }
        };
        self.db.queue_prepared_upload(&job.id, &prepared)?;
        Ok(())
    }

    async fn run_direct(&self, job: &Job, meta: &VideoMetadata, work: &Path) -> Result<()> {
        let video_fut = self.download_video(&job.id, &job.url, meta, work);
        let metadata_fut = self.publish_metadata(&job.id, TransferMode::Direct, meta, None);
        let (video, _) = try_join_branches(video_fut, metadata_fut).await?;
        self.db
            .set_job_paths(&job.id, Some(&video.to_string_lossy()))?;
        self.prepare_direct_upload(job, meta, &video, JobStatus::Completed)
            .await
    }

    async fn prepare_direct_upload(
        &self,
        job: &Job,
        meta: &VideoMetadata,
        video: &Path,
        completion_status: JobStatus,
    ) -> Result<()> {
        self.probe_media(&job.id, video, "original_probe").await?;
        let work = video.parent().context("视频文件没有工作目录")?;
        let cover = self.download_cover(&job.id, meta, work).await?;
        self.db.queue_prepared_upload(
            &job.id,
            &PreparedUpload::Submission {
                video_path: video.to_string_lossy().into_owned(),
                cover_path: cover.to_string_lossy().into_owned(),
                mode: TransferMode::Direct,
                completion_status,
            },
        )?;
        Ok(())
    }

    async fn download_cover(
        &self,
        job_id: &str,
        meta: &VideoMetadata,
        work: &Path,
    ) -> Result<PathBuf> {
        let cover = work.join(format!("{}.youtube-cover.jpg", meta.id));
        if cover.metadata().is_ok_and(|metadata| metadata.len() > 0) {
            return Ok(cover);
        }
        let thumbnail_url = meta
            .thumbnail_url
            .as_deref()
            .context("YouTube 元数据缺少封面 URL")?;
        let stage = self
            .db
            .start_stage(job_id, "cover_download", None, None, None)?;
        let started = Instant::now();
        let source = work.join(format!("{}.youtube-cover.source", meta.id));
        let result = async {
            let response = reqwest::Client::new()
                .get(thumbnail_url)
                .send()
                .await?
                .error_for_status()?;
            let bytes = response.bytes().await?;
            if bytes.is_empty() {
                bail!("YouTube 封面为空")
            }
            fs::write(&source, bytes)?;
            let mut cmd = Command::new(&self.config.render.ffmpeg);
            cmd.arg("-y")
                .arg("-i")
                .arg(&source)
                .args(["-frames:v", "1", "-q:v", "2", "-update", "1"])
                .arg(&cover);
            let output = run_monitored(cmd, Duration::from_secs(120)).await?;
            if !cover.metadata().is_ok_and(|metadata| metadata.len() > 0) {
                bail!("FFmpeg 未生成 YouTube 封面")
            }
            Ok::<u64, anyhow::Error>(output.peak_rss_kib)
        }
        .await;
        let _ = fs::remove_file(&source);
        let duration_ms = started.elapsed().as_millis() as i64;
        match result {
            Ok(peak_rss_kib) => {
                self.db.finish_stage(
                    stage,
                    "completed",
                    duration_ms,
                    peak_rss_kib,
                    Some(thumbnail_url),
                )?;
                Ok(cover)
            }
            Err(error) => {
                self.db
                    .finish_stage(stage, "failed", duration_ms, 0, Some(&error.to_string()))?;
                Err(error).context("下载 YouTube 封面失败")
            }
        }
    }

    async fn prepare_translated_subtitle(
        &self,
        job: &Job,
        meta: &VideoMetadata,
        work: &Path,
    ) -> Result<Option<PreparedSubtitle>> {
        let segmented = work.join(format!("{}.en.segmented.json", meta.id));
        let translated = work.join(format!("{}.en-zh-CN.translated.json", meta.id));

        let cached = match load_segmented_cache(&segmented) {
            Ok(Some(cues)) => {
                self.db.event(
                    Some(&job.id),
                    "info",
                    &format!("复用分句缓存: {} cues", cues.len()),
                )?;
                Some(cues)
            }
            Ok(None) => None,
            Err(error) => {
                self.db
                    .event(Some(&job.id), "warn", &format!("忽略无效分句缓存: {error}"))?;
                None
            }
        };
        let mut cues = match cached {
            Some(cues) => cues,
            None => {
                let Some(fresh) = self.segment_uncached(job, meta, work, &segmented).await? else {
                    return Ok(None);
                };
                fresh
            }
        };

        match load_translation_checkpoint(&translated, &cues) {
            Ok(Some(cached)) if translation_checkpoint_complete(&cached) => {
                self.db.event(
                    Some(&job.id),
                    "info",
                    &format!("复用翻译缓存: {} cues", cached.len()),
                )?;
                cues = cached;
            }
            Ok(Some(cached)) => {
                let completed = cached
                    .iter()
                    .filter(|cue| cue.translation.is_some())
                    .count();
                self.db.event(
                    Some(&job.id),
                    "info",
                    &format!("续传翻译检查点: {completed}/{} cues", cached.len()),
                )?;
                cues = cached;
                self.translate_and_save(&job.id, &mut cues, &translated)
                    .await?;
            }
            Ok(None) => {
                self.translate_and_save(&job.id, &mut cues, &translated)
                    .await?
            }
            Err(error) => {
                self.db
                    .event(Some(&job.id), "warn", &format!("忽略无效翻译缓存: {error}"))?;
                self.translate_and_save(&job.id, &mut cues, &translated)
                    .await?;
            }
        }
        Ok(Some(PreparedSubtitle { cues }))
    }

    /// 无缓存时完整跑一遍：下载字幕 → 解析 VTT → 分句 → 落盘。
    async fn segment_uncached(
        &self,
        job: &Job,
        meta: &VideoMetadata,
        work: &Path,
        segmented: &Path,
    ) -> Result<Option<Vec<Cue>>> {
        let Some(raw_sub) = self
            .download_subtitle(&job.id, &job.url, &meta.id, work)
            .await?
        else {
            return Ok(None);
        };
        let source = subtitle::parse_vtt(&raw_sub)?;
        let cues = self.segment(&job.id, &source).await?;
        subtitle::save_json(&cues, segmented)?;
        Ok(Some(cues))
    }

    /// 翻译并原子写检查点，供翻译缓存缺省/续传后复用。
    async fn translate_and_save(
        &self,
        job_id: &str,
        cues: &mut [Cue],
        checkpoint: &Path,
    ) -> Result<()> {
        self.translate(job_id, cues, checkpoint).await?;
        subtitle::save_json(cues, checkpoint)?;
        Ok(())
    }

    fn ensure_disk(&self) -> Result<()> {
        let bytes = fs2::available_space(&self.config.runtime.data_dir).unwrap_or(u64::MAX);
        let gib = bytes / (1024 * 1024 * 1024);
        if gib < self.config.storage.stop_free_gib {
            bail!(
                "剩余磁盘 {gib}GiB，低于停止阈值 {}GiB",
                self.config.storage.stop_free_gib
            )
        }
        if gib < self.config.storage.warn_free_gib {
            tracing::warn!(free_gib = gib, "磁盘空间低");
        }
        Ok(())
    }

    async fn download_subtitle(
        &self,
        job_id: &str,
        url: &str,
        video_id: &str,
        work: &Path,
    ) -> Result<Option<PathBuf>> {
        let stage = self
            .db
            .start_stage(job_id, "subtitle_download", None, None, None)?;
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--skip-download",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            "en.*,en",
            "--sub-format",
            "vtt",
            "--no-playlist",
            "--no-overwrites",
        ]);
        cmd.arg("-o")
            .arg(work.join(format!("{video_id}.%(language)s.%(ext)s")));
        cmd.arg(url);
        let result = run_monitored(cmd, Duration::from_secs(180)).await;
        match result {
            Ok(out) => {
                let found = fs::read_dir(work)?
                    .filter_map(|e| e.ok().map(|x| x.path()))
                    .find(|p| {
                        p.extension().is_some_and(|x| x == "vtt")
                            && p.metadata().is_ok_and(|m| m.len() > 0)
                    });
                self.db.finish_stage(
                    stage,
                    if found.is_some() {
                        "completed"
                    } else {
                        "missing"
                    },
                    out.duration_ms,
                    out.peak_rss_kib,
                    None,
                )?;
                Ok(found)
            }
            Err(e) => {
                self.db
                    .finish_stage(stage, "failed", 0, 0, Some(&e.to_string()))?;
                Err(e).context("字幕下载失败")
            }
        }
    }

    async fn download_video(
        &self,
        job_id: &str,
        url: &str,
        meta: &VideoMetadata,
        work: &Path,
    ) -> Result<PathBuf> {
        let video_id = &meta.id;
        if let Some(p) = find_video(work, video_id) {
            return Ok(p);
        }
        let stage = self
            .db
            .start_stage(job_id, "video_download", None, None, None)?;
        let mut cmd = ytdlp_command(&self.config.youtube);
        let format = download_format_selector(
            meta,
            self.config.youtube.max_pixels,
            self.config.youtube.max_fps,
        );
        cmd.args([
            "--no-playlist",
            "--concurrent-fragments",
            "1",
            "--merge-output-format",
            "mp4",
            "-f",
            &format,
        ]);
        cmd.arg("-o")
            .arg(work.join(format!("{video_id}.raw.%(ext)s")));
        cmd.arg(url);
        let out = run_monitored(cmd, Duration::from_secs(7200)).await?;
        let path = find_video(work, video_id).context("yt-dlp 完成但未找到视频")?;
        self.db.finish_stage(
            stage,
            "completed",
            out.duration_ms,
            out.peak_rss_kib,
            Some(&path.to_string_lossy()),
        )?;
        Ok(path)
    }

    async fn segment(&self, job_id: &str, cues: &[Cue]) -> Result<Vec<Cue>> {
        let stage = self.db.start_stage(
            job_id,
            "segmentation",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let budget = self.ai_token_budget()?;
        let estimated = estimate_segment_tokens(cues);
        let estimated_bytes = estimate_segment_argument_bytes(cues);
        let wall_started = Instant::now();
        let mut ranges = Vec::new();
        let mut duration = 0;
        let mut peak = 0;
        let mut calls = 0;
        let fits_tokens = estimated <= budget && estimated_bytes <= PI_MAX_PROMPT_ARGUMENT_BYTES;
        let fits_window = fits_tokens && cues.len() <= self.config.ai.segment_max_cues;
        if self.config.ai.batch_mode == BatchMode::WholeVideo || fits_window {
            if !fits_tokens {
                bail!(
                    "whole_video 分句超过安全输入阈值: estimated_tokens={estimated}/{budget}, bytes={estimated_bytes}/{PI_MAX_PROMPT_ARGUMENT_BYTES}；请改用 adaptive"
                )
            }
            let batch = self
                .segment_batch(job_id, stage, cues, 0, cues.len().saturating_sub(1))
                .await;
            let (local, elapsed, rss) = match batch {
                Ok(result) => result,
                Err(error) => {
                    self.db.finish_stage(
                        stage,
                        "failed",
                        wall_started.elapsed().as_millis() as i64,
                        0,
                        Some(&error.to_string()),
                    )?;
                    return Err(error);
                }
            };
            ranges = local;
            duration += elapsed;
            peak = peak.max(rss);
            calls = 1;
        } else {
            let overlap = self.config.ai.segment_overlap_cues;
            let mut cursor = 0;
            while cursor < cues.len() {
                let window_start = cursor.saturating_sub(overlap);
                let budget_end = max_segment_window_end(
                    cues,
                    window_start,
                    budget,
                    PI_MAX_PROMPT_ARGUMENT_BYTES,
                )?;
                let window_end = budget_end.min(window_start + self.config.ai.segment_max_cues - 1);
                if window_end < cursor {
                    bail!("安全 token 阈值过小，无法容纳分句核心字幕")
                }
                let has_more = window_end + 1 < cues.len();
                let preferred_end = if has_more {
                    window_end.saturating_sub(overlap).max(cursor)
                } else {
                    cues.len() - 1
                };
                let batch = self
                    .segment_batch(
                        job_id,
                        stage,
                        &cues[window_start..=window_end],
                        cursor - window_start,
                        preferred_end - window_start,
                    )
                    .await;
                let (local, elapsed, rss) = match batch {
                    Ok(result) => result,
                    Err(error) => {
                        self.db.finish_stage(
                            stage,
                            "failed",
                            wall_started.elapsed().as_millis() as i64,
                            peak,
                            Some(&error.to_string()),
                        )?;
                        return Err(error);
                    }
                };
                let chosen_end = if has_more {
                    choose_adaptive_boundary(
                        &local,
                        window_start,
                        cursor,
                        preferred_end,
                        window_end,
                    )?
                } else {
                    cues.len() - 1
                };
                append_core_ranges(&mut ranges, &local, window_start, cursor, chosen_end)?;
                cursor = chosen_end + 1;
                duration += elapsed;
                peak = peak.max(rss);
                calls += 1;
            }
        }
        let result = subtitle::apply_ranges(cues, &ranges)?;
        self.db.finish_stage(
            stage,
            "completed",
            duration,
            peak,
            Some(&format!(
                "{} -> {} cues; mode={}; estimated_tokens={estimated}; estimated_bytes={estimated_bytes}; calls={calls}",
                cues.len(),
                result.len(),
                batch_mode_name(self.config.ai.batch_mode)
            )),
        )?;
        Ok(result)
    }

    async fn segment_batch(
        &self,
        job_id: &str,
        stage: i64,
        cues: &[Cue],
        core_start: usize,
        preferred_end: usize,
    ) -> Result<(Vec<(usize, usize)>, i64, u64)> {
        let payload = json!({
            "task":"segment",
            "source_lang":self.config.translation.source_lang,
            "core_start":core_start,
            "preferred_end":preferred_end,
            "tokens":cues.iter().enumerate().map(|(i,c)|json!({
                "i":i,
                "start":c.start,
                "end":c.end,
                "text":c.source
            })).collect::<Vec<_>>()
        });
        let input_json = payload.to_string();
        // 分句输出偶发 JSON 截断（deepseek 输出限制），失败时复用翻译的重试次数退避重试。
        let attempts = self.config.ai.translation_batch_retries.saturating_add(1);
        let mut last_error = None;
        let mut result = None;
        for attempt in 1..=attempts {
            let r = self
                .call_pi(payload.clone(), &self.config.ai.translation_thinking)
                .await;
            match r {
                Ok(r) => match parse_ranges(&r.value).and_then(|local| {
                    validate_ranges_cover(cues.len(), &local)?;
                    Ok(local)
                }) {
                    Ok(local) => {
                        result = Some((r, local));
                        break;
                    }
                    Err(error) => last_error = Some(error),
                },
                Err(error) => last_error = Some(error),
            }
            if attempt < attempts {
                self.db.event(
                    Some(job_id),
                    "warn",
                    &format!(
                        "分句窗口 {}..{} 第 {attempt}/{attempts} 次失败: {}",
                        core_start,
                        preferred_end,
                        last_error.as_ref().expect("分句重试前必有错误")
                    ),
                )?;
                sleep(Duration::from_secs((1u64 << (attempt - 1)).min(30))).await;
            }
        }
        let (r, local) = result.ok_or_else(|| {
            last_error
                .unwrap_or_else(|| anyhow::anyhow!("分句窗口 {core_start}..{preferred_end} 失败"))
        })?;
        self.db.record_ai_call(
            job_id,
            stage,
            "segment",
            &self.config.ai.provider,
            &self.config.ai.model,
            &self.config.ai.thinking,
            &r.usage,
            r.output.duration_ms,
            &input_json,
            &r.value.to_string(),
        )?;
        Ok((local, r.output.duration_ms, r.output.peak_rss_kib))
    }

    async fn translate(&self, job_id: &str, cues: &mut [Cue], checkpoint: &Path) -> Result<()> {
        let stage = self.db.start_stage(
            job_id,
            "translation",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let budget = self.ai_token_budget()?;
        let estimated = estimate_translation_tokens(cues);
        let batches = if self.config.ai.batch_mode == BatchMode::WholeVideo {
            if estimated > budget {
                bail!(
                    "whole_video 翻译预计需要 {estimated} tokens，超过安全阈值 {budget}；请改用 adaptive"
                )
            }
            vec![(0, cues.len())]
        } else {
            translation_batches(cues, budget, self.config.ai.translation_batch_cues)?
        };
        if self.config.ai.translation_concurrency == 0 {
            bail!("translation_concurrency 必须大于 0")
        }
        let batch_inputs = batches
            .iter()
            .filter(|(start, end)| !translation_batch_checkpointed(cues, *start, *end))
            .map(|(start, end)| (*start, cues[*start..*end].to_vec()))
            .collect::<Vec<_>>();
        let reused_batches = batches.len() - batch_inputs.len();
        let concurrency = self
            .config
            .ai
            .translation_concurrency
            .min(batch_inputs.len().max(1));
        let wall_started = Instant::now();
        let mut results = stream::iter(batch_inputs)
            .map(|(start, chunk)| async move {
                self.translate_batch_with_retry(job_id, stage, start, &chunk)
                    .await
            })
            .buffer_unordered(concurrency);
        let mut aggregate_call_ms = 0;
        let mut peak = 0;
        let mut checkpointed_batches = reused_batches;
        while let Some(result) = results.next().await {
            let (start, local, duration_ms, peak_rss_kib) = match result {
                Ok(result) => result,
                Err(error) => {
                    self.db.finish_stage(
                        stage,
                        "failed",
                        wall_started.elapsed().as_millis() as i64,
                        peak,
                        Some(&format!(
                            "checkpointed={checkpointed_batches}/{}; {error}",
                            batches.len()
                        )),
                    )?;
                    return Err(error);
                }
            };
            let global = local
                .into_iter()
                .map(|(index, translation)| (index + start, translation))
                .collect::<Vec<_>>();
            subtitle::apply_translations(cues, &global)?;
            subtitle::save_json(cues, checkpoint)?;
            checkpointed_batches += 1;
            aggregate_call_ms += duration_ms;
            peak = peak.max(peak_rss_kib);
        }
        let wall_duration_ms = wall_started.elapsed().as_millis() as i64;
        self.db.finish_stage(
            stage,
            "completed",
            wall_duration_ms,
            peak,
            Some(&format!(
                "{} cues; mode={}; estimated_tokens={estimated}; calls={}; reused_batches={reused_batches}; concurrency={concurrency}; aggregate_call_ms={aggregate_call_ms}",
                cues.len(),
                batch_mode_name(self.config.ai.batch_mode),
                batches.len() - reused_batches
            )),
        )?;
        Ok(())
    }

    async fn translate_batch_with_retry(
        &self,
        job_id: &str,
        stage: i64,
        start: usize,
        chunk: &[Cue],
    ) -> Result<(usize, Vec<(usize, String)>, i64, u64)> {
        let items = chunk
            .iter()
            .enumerate()
            .map(|(i, cue)| json!({"i":i,"text":cue.source}))
            .collect::<Vec<_>>();
        let payload = json!({
            "task":"translate",
            "source_lang":self.config.translation.source_lang,
            "target_lang":self.config.translation.target_lang,
            "items":items
        });
        let input_json = payload.to_string();
        let attempts = self.config.ai.translation_batch_retries.saturating_add(1);
        let mut aggregate_duration_ms = 0;
        let mut peak_rss_kib = 0;
        let mut last_error = None;
        let mut feedback: Option<String> = None;

        for attempt in 1..=attempts {
            let mut call_payload = payload.clone();
            if let Some(message) = &feedback {
                call_payload["feedback"] = json!(message);
            }
            let input_json = call_payload.to_string();
            match self
                .call_pi(call_payload, &self.config.ai.translation_thinking)
                .await
            {
                Ok(result) => {
                    aggregate_duration_ms += result.output.duration_ms;
                    peak_rss_kib = peak_rss_kib.max(result.output.peak_rss_kib);
                    self.db.record_ai_call(
                        job_id,
                        stage,
                        "translate",
                        &self.config.ai.provider,
                        &self.config.ai.model,
                        &self.config.ai.thinking,
                        &result.usage,
                        result.output.duration_ms,
                        &input_json,
                        &result.value.to_string(),
                    )?;
                    match parse_translations(&result.value).and_then(|translations| {
                        validate_translation_indexes(chunk.len(), &translations)?;
                        Ok(translations)
                    }) {
                        Ok(translations) => {
                            return Ok((start, translations, aggregate_duration_ms, peak_rss_kib));
                        }
                        Err(error) => {
                            let error_message = error.to_string();
                            last_error = Some(error);
                            // 输出结构无效时原样重试大概率再次失败：先带反馈让 AI 修正格式；
                            // 反馈仍无效则减半拆分重试，定位问题批次，避免整批 token 白烧。
                            if attempt < attempts {
                                feedback = Some(format!(
                                    "上一轮输出未通过解析/校验：{error_message}。请只输出符合要求的 JSON，不要输出解释、Markdown 或额外字段。"
                                ));
                                self.db.event(
                                    Some(job_id),
                                    "warn",
                                    &format!(
                                        "翻译批次 {}..{} 输出无效，带反馈重试: {error_message}",
                                        start,
                                        start + chunk.len()
                                    ),
                                )?;
                                continue;
                            }
                            if chunk.len() > 1 {
                                return self
                                    .split_translation_batch(
                                        job_id,
                                        stage,
                                        start,
                                        chunk,
                                        aggregate_duration_ms,
                                        peak_rss_kib,
                                    )
                                    .await;
                            }
                        }
                    }
                }
                Err(error) => last_error = Some(error),
            }

            let error = last_error
                .as_ref()
                .expect("translation retry must retain the preceding error");
            self.db.event(
                Some(job_id),
                if attempt < attempts { "warn" } else { "error" },
                &format!(
                    "翻译批次 {}..{} 第 {attempt}/{attempts} 次失败: {error}",
                    start,
                    start + chunk.len()
                ),
            )?;
            if attempt < attempts {
                // 指数退避：1s, 2s, 4s… 对偶发故障收敛，同时限制上游 API 压力。
                sleep(Duration::from_secs((1u64 << (attempt - 1)).min(30))).await;
            }
        }

        // 重试耗尽仍失败：对可拆批次降半重试（缩小请求面定位问题），
        // 而不是让整个批次（可能 50-100 条）白烧后直接失败。
        if chunk.len() > 1 {
            self.db.event(
                Some(job_id),
                "warn",
                &format!(
                    "翻译批次 {}..{} 重试耗尽，自动降半重试",
                    start,
                    start + chunk.len()
                ),
            )?;
            return self
                .split_translation_batch(
                    job_id,
                    stage,
                    start,
                    chunk,
                    aggregate_duration_ms,
                    peak_rss_kib,
                )
                .await;
        }

        Err(last_error
            .expect("translation retry loop must run at least once")
            .context(format!(
                "翻译批次 {}..{} 在 {attempts} 次尝试后失败",
                start,
                start + chunk.len()
            )))
    }

    /// 把翻译批次拆成左右两半分别重试，合并结果；用于结构无效或持续失败定位问题。
    async fn split_translation_batch(
        &self,
        job_id: &str,
        stage: i64,
        start: usize,
        chunk: &[Cue],
        aggregate_duration_ms: i64,
        peak_rss_kib: u64,
    ) -> Result<(usize, Vec<(usize, String)>, i64, u64)> {
        let mid = chunk.len() / 2;
        let (left, right) = chunk.split_at(mid);
        let (_, left_result, left_ms, left_peak) =
            Box::pin(self.translate_batch_with_retry(job_id, stage, start, left)).await?;
        let (_, right_result, right_ms, right_peak) =
            Box::pin(self.translate_batch_with_retry(job_id, stage, start + mid, right)).await?;
        let merged = left_result
            .into_iter()
            .chain(
                right_result
                    .into_iter()
                    .map(|(index, text)| (index + mid, text)),
            )
            .collect::<Vec<_>>();
        Ok((
            start,
            merged,
            aggregate_duration_ms + left_ms + right_ms,
            peak_rss_kib.max(left_peak).max(right_peak),
        ))
    }

    fn ai_token_budget(&self) -> Result<usize> {
        let context = self.config.ai.context_window_tokens;
        let safe = self.config.ai.safe_context_tokens;
        if context == 0 || safe == 0 || safe > context {
            bail!("AI token 配置无效: safe_context_tokens={safe}, context_window_tokens={context}")
        }
        Ok(safe)
    }

    async fn publish_metadata(
        &self,
        job_id: &str,
        mode: TransferMode,
        meta: &VideoMetadata,
        cues: Option<&[Cue]>,
    ) -> Result<PublicationMetadata> {
        if let Some(saved) = self.db.publication_metadata(job_id)? {
            validate_publication_metadata(&saved)?;
            return Ok(saved);
        }
        let budget = self.ai_token_budget()?;
        let stage = self.db.start_stage(
            job_id,
            "publish_metadata",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let mut feedback: Option<String> = None;
        for attempt in 0..=PUBLICATION_FEEDBACK_RETRIES {
            let mut payload = build_publication_payload(mode, meta, cues, budget)?;
            if let Some(message) = &feedback {
                payload["feedback"] = json!(message);
            }
            let input_json = payload.to_string();
            let r = match self.call_pi(payload, &self.config.ai.thinking).await {
                Ok(result) => result,
                Err(error) => {
                    self.db
                        .finish_stage(stage, "failed", 0, 0, Some(&error.to_string()))?;
                    return Err(error);
                }
            };
            self.db.record_ai_call(
                job_id,
                stage,
                "publish_metadata",
                &self.config.ai.provider,
                &self.config.ai.model,
                &self.config.ai.thinking,
                &r.usage,
                r.output.duration_ms,
                &input_json,
                &r.value.to_string(),
            )?;
            let metadata = match parse_publication_metadata(&r.value) {
                Ok(metadata) => metadata,
                Err(error) => {
                    self.db.finish_stage(
                        stage,
                        "failed",
                        r.output.duration_ms,
                        r.output.peak_rss_kib,
                        Some(&error.to_string()),
                    )?;
                    return Err(error);
                }
            };
            if let Err(error) = validate_publication_metadata(&metadata) {
                if attempt < PUBLICATION_FEEDBACK_RETRIES {
                    feedback = Some(format!(
                        "上一轮输出被拒绝：{error}。请按原因修正（通常需要缩短标题/动态）后重新输出完整 JSON。"
                    ));
                    continue;
                }
                self.db.finish_stage(
                    stage,
                    "failed",
                    r.output.duration_ms,
                    r.output.peak_rss_kib,
                    Some(&error.to_string()),
                )?;
                return Err(error);
            }
            if let Err(error) = self.db.save_publication_metadata(job_id, &metadata) {
                self.db.finish_stage(
                    stage,
                    "failed",
                    r.output.duration_ms,
                    r.output.peak_rss_kib,
                    Some(&error.to_string()),
                )?;
                return Err(error);
            }
            self.db.finish_stage(
                stage,
                "completed",
                r.output.duration_ms,
                r.output.peak_rss_kib,
                Some(&format!("{} 次尝试后通过校验", attempt + 1)),
            )?;
            return Ok(metadata);
        }
        unreachable!("publish_metadata 重试循环必须收敛")
    }

    async fn call_pi(&self, payload: Value, thinking: &str) -> Result<PiResult> {
        let mut cmd = Command::new(&self.config.ai.pi);
        cmd.args([
            "--mode",
            "json",
            "--print",
            "--no-session",
            "--no-tools",
            "--no-skills",
            "--no-context-files",
            "--no-prompt-templates",
            "--no-extensions",
            "--extension",
        ]);
        cmd.arg(&self.config.ai.extension);
        cmd.args([
            "--provider",
            &self.config.ai.provider,
            "--model",
            &self.config.ai.model,
            "--thinking",
            thinking,
            "--no-approve",
        ]);
        cmd.env("Y2B_PI_POLICY_PATH", &self.config.ai.policy);
        cmd.arg(payload.to_string());
        let out = run_monitored(cmd, Duration::from_secs(self.config.ai.timeout_seconds)).await?;
        let (value, usage) = parse_pi_stream(&out.stdout)?;
        Ok(PiResult {
            value,
            usage,
            output: out,
        })
    }

    async fn upload_prepared_job_inner(&self, job: &Job) -> Result<()> {
        if !matches!(
            job.status,
            JobStatus::ReadyToUpload | JobStatus::UploadRetryWait
        ) {
            bail!("任务 {} 当前状态不是待上传: {}", job.id, job.status)
        }
        if self
            .db
            .get_setting("auth.bilibili")?
            .is_some_and(|status| status.starts_with("failed"))
        {
            bail!("Bilibili 认证失效，暂停上传并保留现有文件")
        }
        let prepared = self
            .db
            .prepared_upload(&job.id)?
            .with_context(|| format!("待上传任务 {} 缺少持久化上传计划", job.id))?;
        let (bvid, completion_status) = match prepared {
            PreparedUpload::Submission {
                video_path,
                cover_path,
                mode: _,
                completion_status,
            } => {
                if !matches!(
                    completion_status,
                    JobStatus::Completed | JobStatus::UploadedOriginalPendingSubtitle
                ) {
                    bail!("待上传任务 {} 的完成状态无效: {completion_status}", job.id)
                }
                let video = PathBuf::from(video_path);
                let cover = PathBuf::from(cover_path);
                ensure_prepared_file(&video, "视频")?;
                ensure_prepared_file(&cover, "封面")?;
                let publication = self
                    .db
                    .publication_metadata(&job.id)?
                    .with_context(|| format!("待上传任务 {} 缺少投稿元数据", job.id))?;
                let meta = self
                    .db
                    .source_metadata(&job.id)?
                    .with_context(|| format!("待上传任务 {} 缺少来源元数据", job.id))?;
                if !self.wait_for_bilibili_submission(&job.id).await?
                    || !self
                        .db
                        .claim_prepared_upload(&job.id, JobStatus::Uploading)?
                {
                    return Ok(());
                }
                let bvid = self
                    .upload(&job.id, &video, &publication, &meta, Some(&cover))
                    .await?;
                (bvid, completion_status)
            }
        };
        self.db
            .finish_prepared_upload(&job.id, &bvid, completion_status)?;
        // 上传成功后自动补中文 CC 字幕（只复用刚生成的翻译缓存）。
        // 失败不阻塞任务：标记待补字幕状态，`y2b subtitle --all` 会捞起重试。
        if completion_status == JobStatus::Completed {
            let fresh = self
                .db
                .get_job(&job.id)?
                .with_context(|| format!("自动补字幕时任务不存在: {}", job.id))?;
            match self.auto_submit_cc_subtitle(&fresh).await {
                Ok(message) => tracing::info!(job_id = %job.id, "{message}"),
                Err(error) => {
                    tracing::warn!(job_id = %job.id, error = %error, "CC 字幕自动提交失败");
                    self.db.event(
                        Some(&job.id),
                        "warn",
                        &format!("CC 字幕自动提交失败: {error:#}"),
                    )?;
                    self.db.update_job_status(
                        &job.id,
                        JobStatus::UploadedOriginalPendingSubtitle,
                        Some(&format!("CC 字幕提交失败: {error:#}")),
                    )?;
                }
            }
        }
        let (raw, _, _) = self.db.job_paths(&job.id)?;
        let paths = [raw]
            .into_iter()
            .flatten()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let path_refs = paths.iter().map(PathBuf::as_path).collect::<Vec<_>>();
        if let Err(error) = self.after_upload(&job.id, &path_refs) {
            tracing::warn!(job_id = %job.id, error = %error, "上传成功后的文件清理失败");
            self.db.event(
                Some(&job.id),
                "warn",
                &format!("上传成功后的文件清理失败: {error}"),
            )?;
        }
        Ok(())
    }

    async fn upload(
        &self,
        job_id: &str,
        video: &Path,
        publication: &PublicationMetadata,
        meta: &VideoMetadata,
        cover: Option<&Path>,
    ) -> Result<String> {
        let stage = self.db.start_stage(job_id, "upload", None, None, None)?;
        let mut cmd = Command::new(&self.config.bilibili.biliup);
        cmd.arg("-u")
            .arg(&self.config.bilibili.cookies)
            .arg("upload")
            .arg(video)
            .args(build_upload_args(publication, meta));
        if let Some(c) = cover {
            cmd.arg("--cover").arg(c);
        }
        let out = match run_monitored(cmd, Duration::from_secs(14400)).await {
            Ok(output) => output,
            Err(error) => {
                self.db
                    .finish_stage(stage, "failed", 0, 0, Some(&error.to_string()))?;
                return Err(error);
            }
        };
        let merged = out.stdout.clone() + "\n" + &out.stderr;
        let bvid = match Regex::new(r"\bBV[0-9A-Za-z]+\b")?.find(&merged) {
            Some(value) => value.as_str().to_string(),
            None => {
                let error = anyhow::anyhow!("biliup 未返回 BV 号");
                self.db.finish_stage(
                    stage,
                    "failed",
                    out.duration_ms,
                    out.peak_rss_kib,
                    Some(&error.to_string()),
                )?;
                return Err(error);
            }
        };
        self.db.finish_stage(
            stage,
            "completed",
            out.duration_ms,
            out.peak_rss_kib,
            Some(&bvid),
        )?;
        self.defer_bilibili_submissions(self.config.bilibili.submit_interval_seconds)?;
        Ok(bvid)
    }

    /// 给已投稿视频补中文 CC 字幕（B站软字幕，提交后走平台审核）。
    ///
    /// 素材优先复用下载目录里的翻译缓存；缺失时重新下载英文字幕、分句并调 Pi
    /// 翻译。返回给人看的说明文字。
    pub async fn backfill_cc_subtitle(&self, bvid: &str) -> Result<String> {
        let job = self
            .db
            .job_by_bvid(bvid)?
            .with_context(|| format!("数据库中找不到 bvid={bvid} 的任务"))?;
        self.backfill_cc_subtitle_for_job(&job, true).await
    }

    /// 上传成功后的自动 CC 提交：只复用刚生成的翻译缓存，素材缺失时不重新下载。
    async fn auto_submit_cc_subtitle(&self, job: &Job) -> Result<String> {
        self.backfill_cc_subtitle_for_job(job, false).await
    }

    async fn backfill_cc_subtitle_for_job(
        &self,
        job: &Job,
        redownload_if_missing: bool,
    ) -> Result<String> {
        let bvid = job.bvid.as_deref().unwrap_or_default();
        if bvid.is_empty() {
            bail!("任务 {} 没有 BVID", job.id)
        }
        let meta = self
            .db
            .source_metadata(&job.id)?
            .unwrap_or_else(|| VideoMetadata {
                // 个别早期任务未持久化来源元数据：用 job 自身的 video_id/url 兜底。
                id: job.video_id.clone(),
                url: job.url.clone(),
                title: String::new(),
                description: None,
                uploader: None,
                upload_date: None,
                channel: None,
                channel_id: None,
                timestamp: None,
                duration: None,
                width: None,
                height: None,
                fps: None,
                thumbnail_url: None,
                webpage_url: None,
                live_status: None,
            });
        let client =
            bilibili_api::BiliSubtitleClient::from_cookies_file(&self.config.bilibili.cookies)?;
        let view = client.view(bvid).await?;
        if client.has_subtitle_lan(view.cid, "zh").await? {
            return Ok(format!("{bvid} 已有中文字幕，跳过"));
        }
        let work = self.config.runtime.download_dir.join(&meta.id);
        let translated = work.join(format!("{}.en-zh-CN.translated.json", meta.id));
        let cues = if let Ok(cached) = subtitle::load_json(&translated) {
            self.db.event(
                Some(&job.id),
                "info",
                &format!("复用翻译缓存: {} cues", cached.len()),
            )?;
            cached
        } else if redownload_if_missing {
            fs::create_dir_all(&work)?;
            let segmented = work.join(format!("{}.en.segmented.json", meta.id));
            let Some(cues) = self
                .segment_uncached(&job, &meta, &work, &segmented)
                .await?
            else {
                bail!("无法获取英文字幕，跳过 CC 字幕补交")
            };
            let mut cues = cues;
            self.translate_and_save(&job.id, &mut cues, &translated)
                .await?;
            cues
        } else {
            bail!("缺少翻译素材，跳过自动 CC 提交（可手动执行 y2b subtitle add {bvid}）")
        };
        let cc_cues = cc_cues_from(&cues, Some(view.duration));
        if cc_cues.is_empty() {
            bail!("翻译结果为空，没有可提交的中文字幕")
        }
        client.submit(&view, "zh", &cc_cues).await?;
        self.db.event(
            Some(&job.id),
            "info",
            &format!("已提交中文 CC 字幕（{} 条），等待 B站审核", cc_cues.len()),
        )?;
        Ok(format!(
            "{bvid} 已提交中文 CC 字幕（{} 条），等待 B站审核",
            cc_cues.len()
        ))
    }

    async fn wait_for_bilibili_submission(&self, job_id: &str) -> Result<bool> {
        self.wait_for_bilibili_submission_with_poll(job_id, Duration::from_secs(2))
            .await
    }

    async fn wait_for_bilibili_submission_with_poll(
        &self,
        job_id: &str,
        poll_interval: Duration,
    ) -> Result<bool> {
        let Some(value) = self.db.get_setting(NEXT_BILIBILI_SUBMIT_AT)? else {
            return Ok(true);
        };
        let not_before = DateTime::parse_from_rfc3339(&value)
            .with_context(|| format!("投稿冷却时间无效: {value}"))?
            .with_timezone(&Utc);
        let Ok(wait) = (not_before - Utc::now()).to_std() else {
            return Ok(true);
        };
        if wait.is_zero() {
            return Ok(true);
        }
        self.db.event(
            Some(job_id),
            "info",
            &format!(
                "等待 Bilibili 投稿窗口至 {}（约 {} 分钟）",
                not_before.to_rfc3339(),
                wait.as_secs().div_ceil(60)
            ),
        )?;
        let poll_interval = poll_interval.max(Duration::from_millis(1));
        loop {
            let job = self
                .db
                .get_job(job_id)?
                .with_context(|| format!("等待投稿窗口时任务不存在: {job_id}"))?;
            if !matches!(
                job.status,
                JobStatus::ReadyToUpload | JobStatus::UploadRetryWait
            ) {
                self.db.event(
                    Some(job_id),
                    "info",
                    &format!("已停止等待 Bilibili 投稿窗口，任务状态为 {}", job.status),
                )?;
                return Ok(false);
            }
            let Ok(remaining) = (not_before - Utc::now()).to_std() else {
                return Ok(true);
            };
            if remaining.is_zero() {
                return Ok(true);
            }
            sleep(remaining.min(poll_interval)).await;
        }
    }

    fn defer_bilibili_submissions(&self, seconds: u64) -> Result<()> {
        if seconds == 0 {
            return Ok(());
        }
        let seconds = i64::try_from(seconds).unwrap_or(i64::MAX);
        let not_before = Utc::now()
            .checked_add_signed(chrono::Duration::seconds(seconds))
            .context("Bilibili 投稿冷却时间溢出")?;
        self.db
            .set_setting(NEXT_BILIBILI_SUBMIT_AT, &not_before.to_rfc3339())
    }
    async fn probe_media(&self, job_id: &str, path: &Path, label: &str) -> Result<()> {
        let stage = self.db.start_stage(job_id, label, None, None, None)?;
        let mut cmd = Command::new(&self.config.render.ffprobe);
        cmd.args([
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_type,codec_name,width,height,r_frame_rate,pix_fmt:format=duration",
            "-of",
            "json",
        ])
        .arg(path);
        let out = run_monitored(cmd, Duration::from_secs(60)).await?;
        serde_json::from_str::<Value>(&out.stdout).context("ffprobe 输出不是 JSON")?;
        self.db.finish_stage(
            stage,
            "completed",
            out.duration_ms,
            out.peak_rss_kib,
            Some(out.stdout.trim()),
        )?;
        Ok(())
    }
    fn after_upload(&self, job_id: &str, paths: &[&Path]) -> Result<()> {
        if self.config.storage.delete_large_after_upload {
            for p in paths {
                if p.exists() {
                    fs::remove_file(p)?;
                }
            }
            self.db
                .event(Some(job_id), "info", "上传成功，已清理大型视频文件")?;
        }
        Ok(())
    }
    fn cleanup_large(&self, job_id: &str) -> Result<()> {
        let (raw, _, _) = self.db.job_paths(job_id)?;
        if let Some(raw) = raw {
            let path = PathBuf::from(raw);
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
}

fn ensure_prepared_file(path: &Path, label: &str) -> Result<()> {
    if !path.metadata().is_ok_and(|metadata| metadata.len() > 0) {
        bail!("待上传{label}不存在或为空: {}", path.display())
    }
    Ok(())
}

const PI_PROMPT_OVERHEAD_TOKENS: usize = 2_048;
const PI_METADATA_OUTPUT_RESERVE_TOKENS: usize = 1_024;
/// 分句窗口的单次参数大小上限：2GB 内存的服务器上 pi 处理超过 ~96KB
/// 的输入会因内存/swap 拖慢甚至 OOM（实测 128KB 窗口触发 kill），
/// 96KB（≈30k tokens）窗口单次约 160s，可稳定完成。
const PI_MAX_PROMPT_ARGUMENT_BYTES: usize = 96 * 1024;
const BILIBILI_TID: i64 = 172;
const CORE_TAG: &str = "荒野乱斗";
const MAX_TITLE_WIDTH: usize = 70;
const MAX_DYNAMIC_WIDTH: usize = 120;

/// 投稿元数据校验失败（如标题宽度超限）时，把原因作为反馈让 AI 重写的最大次数。
const PUBLICATION_FEEDBACK_RETRIES: usize = 2;
const MAX_TAG_CHARS: usize = 20;
const MAX_TAGS: usize = 4;

fn download_format_selector(meta: &VideoMetadata, max_pixels: u64, max_fps: f64) -> String {
    let vertical = matches!((meta.width, meta.height), (Some(width), Some(height)) if height >= width)
        || meta.url.contains("/shorts/")
        || meta
            .webpage_url
            .as_deref()
            .is_some_and(|url| url.contains("/shorts/"));
    let ((primary_width, primary_height), (secondary_width, secondary_height)) = if vertical {
        ((1080, 1920), (1920, 1080))
    } else {
        ((1920, 1080), (1080, 1920))
    };
    let square = (max_pixels as f64).sqrt().floor() as u64;
    format!(
        "bv*[vcodec^=avc1][fps<={max_fps}][width<={primary_width}][height<={primary_height}]+ba[acodec^=mp4a]/bv*[vcodec^=avc1][fps<={max_fps}][width<={secondary_width}][height<={secondary_height}]+ba[acodec^=mp4a]/bv*[vcodec^=avc1][fps<={max_fps}][width<={square}][height<={square}]+ba[acodec^=mp4a]/b[vcodec^=avc1][acodec^=mp4a][fps<={max_fps}][width<={primary_width}][height<={primary_height}]/b[vcodec^=avc1][acodec^=mp4a][fps<={max_fps}][width<={secondary_width}][height<={secondary_height}]/bv*[fps<={max_fps}][width<={primary_width}][height<={primary_height}]+ba/bv*[fps<={max_fps}][width<={secondary_width}][height<={secondary_height}]+ba/b[fps<={max_fps}][width<={primary_width}][height<={primary_height}]/b[fps<={max_fps}][width<={secondary_width}][height<={secondary_height}]"
    )
}

fn build_publication_payload(
    mode: TransferMode,
    meta: &VideoMetadata,
    cues: Option<&[Cue]>,
    budget: usize,
) -> Result<Value> {
    if mode == TransferMode::Translated && cues.is_none_or(<[Cue]>::is_empty) {
        bail!("翻译字幕模式缺少双语字幕，不能生成投稿元数据")
    }
    let all_cues = cues.unwrap_or_default();
    let full_indices = (0..all_cues.len()).collect::<Vec<_>>();
    let full = publication_payload_value(mode, meta, all_cues, &full_indices, false);
    if publication_payload_fits(&full, budget) {
        return Ok(full);
    }
    if mode == TransferMode::Direct {
        bail!(
            "YouTube 元数据超过 Pi 输入限制: estimated_tokens={}, bytes={}",
            estimate_publication_tokens(&full),
            full.to_string().len()
        )
    }

    let mut count = all_cues.len().saturating_sub(1).max(2);
    loop {
        let indices = uniform_sample_indices(all_cues.len(), count);
        let sampled = publication_payload_value(mode, meta, all_cues, &indices, true);
        let estimated = estimate_publication_tokens(&sampled);
        let bytes = sampled.to_string().len();
        if estimated <= budget && bytes <= PI_MAX_PROMPT_ARGUMENT_BYTES {
            return Ok(sampled);
        }
        if count == 2 {
            bail!(
                "保留首尾字幕后投稿元数据仍超过 Pi 输入限制: estimated_tokens={estimated}, bytes={bytes}"
            )
        }
        let token_count = count
            .saturating_mul(budget)
            .checked_div(estimated)
            .unwrap_or(0);
        let byte_count = count
            .saturating_mul(PI_MAX_PROMPT_ARGUMENT_BYTES)
            .checked_div(bytes)
            .unwrap_or(0);
        let shrunk = token_count.min(byte_count);
        count = shrunk.clamp(2, count - 1);
    }
}

fn publication_payload_value(
    mode: TransferMode,
    meta: &VideoMetadata,
    cues: &[Cue],
    indices: &[usize],
    sampled: bool,
) -> Value {
    let source_url = meta.webpage_url.as_deref().unwrap_or(&meta.url);
    json!({
        "task": "publish_metadata",
        "transfer_mode": mode.to_string(),
        "youtube": {
            "title": meta.title,
            "description": meta.description.as_deref().unwrap_or(""),
            "url": source_url,
            "uploader": meta.uploader.as_deref().or(meta.channel.as_deref()).unwrap_or(""),
            "published_date": publication_date(meta),
        },
        "subtitle_sampling": {
            "sampled": sampled,
            "total": cues.len(),
            "included": indices.len(),
        },
        "subtitles": indices.iter().map(|&i| {
            let cue = &cues[i];
            json!({
                "i": i,
                "start": cue.start,
                "end": cue.end,
                "source": cue.source,
                "translation": cue.translation.as_deref().unwrap_or(""),
            })
        }).collect::<Vec<_>>(),
    })
}

fn estimate_publication_tokens(payload: &Value) -> usize {
    PI_PROMPT_OVERHEAD_TOKENS
        .saturating_add(PI_METADATA_OUTPUT_RESERVE_TOKENS)
        .saturating_add(estimate_text_tokens(&payload.to_string()))
}

fn publication_payload_fits(payload: &Value, token_budget: usize) -> bool {
    estimate_publication_tokens(payload) <= token_budget
        && payload.to_string().len() <= PI_MAX_PROMPT_ARGUMENT_BYTES
}

fn uniform_sample_indices(len: usize, count: usize) -> Vec<usize> {
    if count >= len {
        return (0..len).collect();
    }
    if len == 0 || count == 0 {
        return Vec::new();
    }
    if count == 1 {
        return vec![0];
    }
    (0..count)
        .map(|slot| slot * (len - 1) / (count - 1))
        .collect()
}

fn parse_publication_metadata(value: &Value) -> Result<PublicationMetadata> {
    let title = sanitize_publication_text(
        value
            .get("title")
            .and_then(Value::as_str)
            .context("Pi 投稿元数据缺少字符串 title")?,
    );
    let dynamic = sanitize_publication_text(
        value
            .get("dynamic")
            .and_then(Value::as_str)
            .context("Pi 投稿元数据缺少字符串 dynamic")?,
    );
    let raw_tags = value
        .get("tags")
        .and_then(Value::as_array)
        .context("Pi 投稿元数据缺少数组 tags")?;
    if raw_tags.is_empty() {
        bail!("Pi 投稿元数据 tags 为空")
    }
    let raw_tags = raw_tags
        .iter()
        .map(|tag| {
            tag.as_str()
                .map(str::to_string)
                .context("Pi 投稿元数据 tags 含非字符串项")
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = PublicationMetadata {
        title,
        dynamic,
        tags: sanitize_tags(&raw_tags),
        tid: BILIBILI_TID,
        raw_json: value.to_string(),
    };
    validate_publication_metadata(&metadata)?;
    Ok(metadata)
}

fn validate_publication_metadata(metadata: &PublicationMetadata) -> Result<()> {
    validate_text_field("标题", &metadata.title, MAX_TITLE_WIDTH)?;
    validate_text_field("动态", &metadata.dynamic, MAX_DYNAMIC_WIDTH)?;
    if metadata.title.contains('#')
        || metadata.title.contains('＃')
        || ["http://", "https://", "www."]
            .iter()
            .any(|needle| metadata.title.to_ascii_lowercase().contains(needle))
    {
        bail!("标题含链接或话题")
    }
    if metadata.tid != BILIBILI_TID {
        bail!("投稿分区必须为 {BILIBILI_TID}，实际为 {}", metadata.tid)
    }
    if metadata.tags.is_empty()
        || metadata.tags.len() > MAX_TAGS
        || metadata.tags.first().map(String::as_str) != Some(CORE_TAG)
    {
        bail!("投稿标签必须以“{CORE_TAG}”开头且总数为 1～{MAX_TAGS}")
    }
    if metadata
        .tags
        .iter()
        .any(|tag| tag.is_empty() || tag.chars().count() > MAX_TAG_CHARS)
    {
        bail!("投稿标签为空或超过 {MAX_TAG_CHARS} 字")
    }
    if metadata.dynamic.contains('#')
        || metadata.dynamic.contains('＃')
        || ["http://", "https://", "www."]
            .iter()
            .any(|needle| metadata.dynamic.to_ascii_lowercase().contains(needle))
        || ["关注我", "点赞", "投币", "三连", "订阅频道", "转发"]
            .iter()
            .any(|needle| metadata.dynamic.contains(needle))
    {
        bail!("动态含链接、话题或引导互动内容")
    }
    if metadata.title.chars().any(is_emoji) || metadata.dynamic.chars().any(is_emoji) {
        bail!("标题或动态含 emoji")
    }
    let sentence_ends = metadata
        .dynamic
        .chars()
        .filter(|ch| matches!(ch, '。' | '！' | '？' | '!' | '?'))
        .count();
    if sentence_ends > 2 {
        bail!("动态超过 2 句")
    }
    Ok(())
}

fn validate_text_field(label: &str, text: &str, max_width: usize) -> Result<()> {
    if text.trim().is_empty() || text.chars().any(char::is_control) {
        bail!("{label}为空或含控制字符")
    }
    let width = chinese_width(text);
    if width > max_width {
        bail!("{label}宽度 {width} 超过上限 {max_width}")
    }
    Ok(())
}

fn chinese_width(text: &str) -> usize {
    text.chars()
        .map(|ch| if ch.is_ascii() { 1 } else { 2 })
        .sum()
}

fn is_emoji(ch: char) -> bool {
    matches!(ch as u32,
        0x1F000..=0x1FAFF | 0x2600..=0x26FF | 0x2700..=0x27BF | 0xFE00..=0xFE0F)
}

fn sanitize_publication_text(text: &str) -> String {
    text.chars()
        .filter(|ch| !is_emoji(*ch) && !matches!(*ch, '\u{200D}' | '\u{20E3}'))
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn sanitize_tags(raw: &[String]) -> Vec<String> {
    let mut tags = vec![CORE_TAG.to_string()];
    for tag in raw {
        let clean = tag
            .chars()
            .filter(|ch| !ch.is_control() && !matches!(ch, '#' | '＃' | ',' | '，'))
            .collect::<String>();
        let clean = clean
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(MAX_TAG_CHARS)
            .collect::<String>();
        if clean.is_empty() || tags.iter().any(|existing| existing == &clean) {
            continue;
        }
        tags.push(clean);
        if tags.len() == MAX_TAGS {
            break;
        }
    }
    tags
}

fn publication_date(meta: &VideoMetadata) -> String {
    if let Some(date) = meta.upload_date.as_deref()
        && date.len() == 8
        && date.chars().all(|ch| ch.is_ascii_digit())
    {
        return format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..]);
    }
    meta.timestamp
        .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "未知".to_string())
}

fn build_description(meta: &VideoMetadata) -> String {
    let source_url = meta.webpage_url.as_deref().unwrap_or(&meta.url);
    let uploader = meta
        .uploader
        .as_deref()
        .or(meta.channel.as_deref())
        .unwrap_or("未知");
    let mut lines = Vec::with_capacity(5);
    if let Some(title) = original_title_without_hashtags(&meta.title) {
        lines.push(format!("原标题：{title}"));
    }
    lines.extend([
        format!("来源：{source_url}"),
        format!("原作者：{uploader}"),
        format!("原视频发布时间：{}", publication_date(meta)),
        "处理工具：https://github.com/Mist-wu/y2b-rs".to_string(),
    ]);
    lines.join("\n")
}

fn original_title_without_hashtags(title: &str) -> Option<String> {
    let clean = title
        .split_whitespace()
        .filter(|part| !part.starts_with('#') && !part.starts_with('＃'))
        .collect::<Vec<_>>()
        .join(" ");
    (!clean.is_empty()).then_some(clean)
}

fn build_upload_args(metadata: &PublicationMetadata, meta: &VideoMetadata) -> Vec<String> {
    vec![
        "--submit".into(),
        "web".into(),
        "--title".into(),
        metadata.title.clone(),
        "--desc".into(),
        build_description(meta),
        "--tag".into(),
        metadata.tags.join(","),
        "--tid".into(),
        BILIBILI_TID.to_string(),
        "--dynamic".into(),
        metadata.dynamic.clone(),
        "--copyright".into(),
        "1".into(),
        "--no-reprint".into(),
        "0".into(),
        "--limit".into(),
        "1".into(),
    ]
}

fn requires_translated_pipeline(mode: TransferMode) -> bool {
    mode == TransferMode::Translated
}

/// 把双语 cue 转成 B站 CC 字幕条目：只保留有非空翻译且时间合法（end > start）的。
fn cc_cues_from(cues: &[Cue], max_to: Option<f64>) -> Vec<CcCue> {
    cues.iter()
        .filter(|c| {
            c.translation
                .as_deref()
                .is_some_and(|t| !t.trim().is_empty())
                && c.end > c.start
        })
        .filter_map(|c| {
            let from = c.start;
            let mut to = c.end;
            if let Some(max) = max_to {
                if from >= max {
                    return None;
                }
                to = to.min(max);
            }
            if to <= from {
                return None;
            }
            Some(CcCue {
                from,
                to,
                content: c
                    .translation
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
            })
        })
        .collect()
}

async fn try_join_branches<A, B, FA, FB>(left: FA, right: FB) -> Result<(A, B)>
where
    FA: Future<Output = Result<A>>,
    FB: Future<Output = Result<B>>,
{
    tokio::try_join!(left, right)
}

fn batch_mode_name(mode: BatchMode) -> &'static str {
    match mode {
        BatchMode::WholeVideo => "whole_video",
        BatchMode::Adaptive => "adaptive",
    }
}

fn estimate_text_tokens(text: &str) -> usize {
    let mut tokens: usize = 0;
    let mut ascii_run: usize = 0;
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '\'') {
            ascii_run += 1;
            continue;
        }
        if ascii_run > 0 {
            tokens += ascii_run.div_ceil(4);
            ascii_run = 0;
        }
        if !ch.is_whitespace() {
            tokens += 1;
        }
    }
    if ascii_run > 0 {
        tokens += ascii_run.div_ceil(4);
    }
    tokens.max(1)
}

fn segment_cue_tokens(cue: &Cue) -> usize {
    estimate_text_tokens(&cue.source) + 22
}

fn translation_cue_tokens(cue: &Cue) -> usize {
    let source = estimate_text_tokens(&cue.source);
    source.saturating_mul(2) + 20
}

fn estimate_segment_tokens(cues: &[Cue]) -> usize {
    PI_PROMPT_OVERHEAD_TOKENS + cues.iter().map(segment_cue_tokens).sum::<usize>()
}

fn segment_cue_argument_bytes(index: usize, cue: &Cue) -> usize {
    serde_json::to_string(&json!({
        "i": index,
        "start": cue.start,
        "end": cue.end,
        "text": cue.source
    }))
    .map_or(usize::MAX, |value| value.len().saturating_add(1))
}

fn estimate_segment_argument_bytes(cues: &[Cue]) -> usize {
    512usize.saturating_add(
        cues.iter()
            .enumerate()
            .map(|(index, cue)| segment_cue_argument_bytes(index, cue))
            .sum::<usize>(),
    )
}

fn estimate_translation_tokens(cues: &[Cue]) -> usize {
    PI_PROMPT_OVERHEAD_TOKENS + cues.iter().map(translation_cue_tokens).sum::<usize>()
}

fn max_segment_window_end(
    cues: &[Cue],
    start: usize,
    token_budget: usize,
    byte_budget: usize,
) -> Result<usize> {
    if start >= cues.len() {
        bail!("分句窗口起点越界: {start}/{}", cues.len())
    }
    let mut total = PI_PROMPT_OVERHEAD_TOKENS;
    let mut bytes = 512usize;
    let mut end = None;
    for (index, cue) in cues.iter().enumerate().skip(start) {
        let item = segment_cue_tokens(cue);
        let item_bytes = segment_cue_argument_bytes(index - start, cue);
        if total.saturating_add(item) > token_budget
            || bytes.saturating_add(item_bytes) > byte_budget
        {
            break;
        }
        total += item;
        bytes += item_bytes;
        end = Some(index);
    }
    end.with_context(|| {
        format!(
            "单条字幕已超过安全输入阈值: cue={start}, estimated_tokens={}/{token_budget}, bytes={}/{byte_budget}",
            PI_PROMPT_OVERHEAD_TOKENS + segment_cue_tokens(&cues[start]),
            512 + segment_cue_argument_bytes(0, &cues[start])
        )
    })
}

fn translation_batches(
    cues: &[Cue],
    budget: usize,
    max_cues: usize,
) -> Result<Vec<(usize, usize)>> {
    if cues.is_empty() {
        return Ok(Vec::new());
    }
    if max_cues == 0 {
        bail!("translation_batch_cues 必须大于 0")
    }
    let mut batches = Vec::new();
    let mut start = 0;
    let mut total = PI_PROMPT_OVERHEAD_TOKENS;
    for (index, cue) in cues.iter().enumerate() {
        let item = translation_cue_tokens(cue);
        if PI_PROMPT_OVERHEAD_TOKENS.saturating_add(item) > budget {
            bail!(
                "单句翻译已超过安全 token 阈值 {budget}: cue={index}, estimated={}",
                PI_PROMPT_OVERHEAD_TOKENS + item
            )
        }
        if index > start && (index - start >= max_cues || total.saturating_add(item) > budget) {
            batches.push((start, index));
            start = index;
            total = PI_PROMPT_OVERHEAD_TOKENS;
        }
        total += item;
    }
    batches.push((start, cues.len()));
    Ok(batches)
}

fn validate_ranges_cover(len: usize, ranges: &[(usize, usize)]) -> Result<()> {
    let mut expected = 0;
    for &(start, end) in ranges {
        if start != expected || end < start || end >= len {
            bail!("Pi 分句范围不连续或越界: {start}..{end}, expected={expected}, len={len}")
        }
        expected = end + 1;
    }
    if expected != len {
        bail!("Pi 分句没有覆盖完整窗口: {expected}/{len}")
    }
    Ok(())
}

fn validate_translation_indexes(len: usize, translations: &[(usize, String)]) -> Result<()> {
    if translations.len() != len {
        bail!("Pi 翻译数量不匹配: {}/{}", translations.len(), len)
    }
    for (expected, (index, _)) in translations.iter().enumerate() {
        if *index != expected {
            bail!("Pi 翻译索引无序或缺失: index={index}, expected={expected}")
        }
    }
    Ok(())
}

fn load_segmented_cache(path: &Path) -> Result<Option<Vec<Cue>>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut cues = subtitle::load_json(path).with_context(|| format!("读取 {}", path.display()))?;
    validate_cached_cues(&cues)?;
    for cue in &mut cues {
        cue.translation = None;
    }
    Ok(Some(cues))
}

fn load_translation_checkpoint(path: &Path, source: &[Cue]) -> Result<Option<Vec<Cue>>> {
    if !path.exists() {
        return Ok(None);
    }
    let cues = subtitle::load_json(path).with_context(|| format!("读取 {}", path.display()))?;
    validate_cached_cues(&cues)?;
    if cues.len() != source.len() {
        bail!("翻译缓存数量不匹配: {}/{}", cues.len(), source.len())
    }
    for (index, (cached, original)) in cues.iter().zip(source).enumerate() {
        if cached.source != original.source
            || (cached.start - original.start).abs() > 0.001
            || (cached.end - original.end).abs() > 0.001
        {
            bail!("翻译缓存第 {index} 条与分句缓存不匹配")
        }
        if let Some(translation) = cached.translation.as_deref() {
            if translation.trim().is_empty() && cached.source.chars().any(char::is_alphanumeric) {
                bail!("翻译缓存第 {index} 条译文为空")
            }
            if translation.chars().any(|ch| ch.is_control() && ch != '\n') {
                bail!("翻译缓存第 {index} 条含非法控制字符")
            }
        }
    }
    Ok(Some(cues))
}

fn translation_checkpoint_complete(cues: &[Cue]) -> bool {
    cues.iter().all(|cue| cue.translation.is_some())
}

fn translation_batch_checkpointed(cues: &[Cue], start: usize, end: usize) -> bool {
    cues[start..end].iter().all(|cue| cue.translation.is_some())
}

fn validate_cached_cues(cues: &[Cue]) -> Result<()> {
    if cues.is_empty() {
        bail!("字幕缓存为空")
    }
    for (index, cue) in cues.iter().enumerate() {
        if !cue.start.is_finite() || !cue.end.is_finite() || cue.start < 0.0 || cue.end <= cue.start
        {
            bail!("字幕缓存第 {index} 条时间无效")
        }
        if cue.source.trim().is_empty() {
            bail!("字幕缓存第 {index} 条原文为空")
        }
        if index > 0 && cue.start + 0.001 < cues[index - 1].start {
            bail!("字幕缓存第 {index} 条开始时间早于前一条")
        }
    }
    Ok(())
}

fn choose_adaptive_boundary(
    local: &[(usize, usize)],
    window_start: usize,
    cursor: usize,
    preferred_end: usize,
    window_end: usize,
) -> Result<usize> {
    local
        .iter()
        .map(|(_, end)| window_start + end)
        .filter(|&end| end >= cursor && end <= window_end)
        .min_by_key(|&end| {
            (
                end.abs_diff(preferred_end),
                usize::from(end > preferred_end),
            )
        })
        .context("Pi 分句窗口中没有可用边界")
}

fn append_core_ranges(
    output: &mut Vec<(usize, usize)>,
    local: &[(usize, usize)],
    window_start: usize,
    core_start: usize,
    core_end: usize,
) -> Result<()> {
    let mut expected = core_start;
    for &(start, end) in local {
        let global_start = window_start + start;
        let global_end = window_start + end;
        if global_end < core_start {
            continue;
        }
        if global_start > core_end {
            break;
        }
        let clipped_start = global_start.max(core_start);
        let clipped_end = global_end.min(core_end);
        if clipped_start != expected || clipped_end < clipped_start {
            bail!("重叠分句合并失败: {clipped_start}..{clipped_end}, expected={expected}")
        }
        output.push((clipped_start, clipped_end));
        expected = clipped_end + 1;
    }
    if expected != core_end + 1 {
        bail!("重叠分句未覆盖核心窗口: {expected}/{}", core_end + 1)
    }
    Ok(())
}

fn find_video(work: &Path, video_id: &str) -> Option<PathBuf> {
    fs::read_dir(work)
        .ok()?
        .filter_map(|e| e.ok().map(|x| x.path()))
        .find(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(&format!("{video_id}.raw.")))
                && p.metadata().is_ok_and(|m| m.len() > 1024)
        })
}
fn parse_pi_stream(stream: &str) -> Result<(Value, AiUsage)> {
    let mut text = None;
    let mut usage = AiUsage {
        input: 0,
        output: 0,
        reasoning: 0,
        cache_read: 0,
        cache_write: 0,
        total: 0,
        cost: None,
    };
    for line in stream.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if v["type"] == "agent_end"
            && let Some(messages) = v["messages"].as_array()
            && let Some(m) = messages.iter().rev().find(|m| m["role"] == "assistant")
        {
            text = m["content"]
                .as_array()
                .and_then(|a| {
                    a.iter()
                        .filter_map(|x| x.get("text").and_then(Value::as_str))
                        .next_back()
                })
                .map(str::to_string);
            usage = parse_usage(&m["usage"]);
        }
    }
    let raw = text.context("Pi JSON 流中没有最终文本")?;
    let clean = raw
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    Ok((
        serde_json::from_str(clean).context("Pi 最终文本不是 JSON")?,
        usage,
    ))
}
fn parse_usage(v: &Value) -> AiUsage {
    AiUsage {
        input: v["input"].as_i64().unwrap_or(0),
        output: v["output"].as_i64().unwrap_or(0),
        reasoning: v["reasoning"].as_i64().unwrap_or(0),
        cache_read: v["cacheRead"].as_i64().unwrap_or(0),
        cache_write: v["cacheWrite"].as_i64().unwrap_or(0),
        total: v["totalTokens"].as_i64().unwrap_or(0),
        cost: v["cost"]["total"].as_f64(),
    }
}
fn parse_ranges(v: &Value) -> Result<Vec<(usize, usize)>> {
    v.get("ranges")
        .and_then(Value::as_array)
        .context("分句结果缺少 ranges")?
        .iter()
        .map(|x| {
            Ok((
                x["start"].as_u64().context("range.start")? as usize,
                x["end"].as_u64().context("range.end")? as usize,
            ))
        })
        .collect()
}
fn parse_translations(v: &Value) -> Result<Vec<(usize, String)>> {
    v.get("translations")
        .and_then(Value::as_array)
        .context("翻译结果缺少 translations")?
        .iter()
        .map(|x| {
            Ok((
                x["i"].as_u64().context("translation.i")? as usize,
                x["text"].as_str().context("translation.text")?.to_string(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::NewJob;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    fn cue(index: usize, text: &str) -> Cue {
        Cue {
            start: index as f64 * 2.0,
            end: index as f64 * 2.0 + 1.8,
            source: text.into(),
            translation: None,
        }
    }

    fn metadata() -> VideoMetadata {
        VideoMetadata {
            id: "video".into(),
            url: "https://youtube.com/watch?v=video".into(),
            title: "Best Ranked Match 2026".into(),
            description: Some("A close Brawl Stars ranked match.".into()),
            uploader: Some("Player One".into()),
            upload_date: Some("20260803".into()),
            channel: Some("Player One".into()),
            channel_id: Some("UC-test".into()),
            timestamp: Some(1_775_347_200),
            duration: Some(120.0),
            width: Some(1920),
            height: Some(1080),
            fps: Some(60.0),
            thumbnail_url: Some("https://i.ytimg.com/vi/video/maxresdefault.jpg".into()),
            webpage_url: Some("https://www.youtube.com/watch?v=video".into()),
            live_status: Some("not_live".into()),
        }
    }

    #[test]
    fn pi_json() {
        let s = r#"{"type":"agent_end","messages":[{"role":"assistant","content":[{"type":"text","text":"{\"ranges\":[{\"start\":0,\"end\":1}]}"}],"usage":{"input":2,"output":3,"totalTokens":5}}]}"#;
        let (v, u) = parse_pi_stream(s).unwrap();
        assert_eq!(parse_ranges(&v).unwrap(), vec![(0, 1)]);
        assert_eq!(u.total, 5);
    }

    #[test]
    fn thirty_minute_subtitles_fit_default_safe_budget() {
        let cues = (0..1_800)
            .map(|i| {
                cue(
                    i,
                    "this is a representative subtitle sentence with useful context",
                )
            })
            .collect::<Vec<_>>();
        assert!(estimate_segment_tokens(&cues) < 200_000);
        assert!(estimate_translation_tokens(&cues) < 200_000);
    }

    #[test]
    fn adaptive_translation_batches_are_contiguous() {
        let cues = (0..20)
            .map(|i| cue(i, "a moderately sized subtitle sentence"))
            .collect::<Vec<_>>();
        let batches = translation_batches(&cues, 2_300, 50).unwrap();
        assert!(batches.len() > 1);
        assert_eq!(batches.first().map(|x| x.0), Some(0));
        assert_eq!(batches.last().map(|x| x.1), Some(cues.len()));
        assert!(batches.windows(2).all(|pair| pair[0].1 == pair[1].0));
    }

    #[test]
    fn adaptive_translation_batches_respect_cue_limit() {
        let cues = (0..107)
            .map(|i| cue(i, "short subtitle"))
            .collect::<Vec<_>>();
        assert_eq!(
            translation_batches(&cues, 200_000, 50).unwrap(),
            vec![(0, 50), (50, 100), (100, 107)]
        );
    }

    #[test]
    fn adaptive_segmentation_respects_process_argument_byte_limit() {
        // 5.5KB 长句 × 100 条 ≈ 556KB，超过 512KB 窗口上限但不超过 token 预算。
        let long_text = "x".repeat(5_500);
        let cues = (0..100)
            .map(|index| cue(index, &long_text))
            .collect::<Vec<_>>();
        assert!(estimate_segment_tokens(&cues) < 200_000);
        assert!(estimate_segment_argument_bytes(&cues) > PI_MAX_PROMPT_ARGUMENT_BYTES);

        let end = max_segment_window_end(&cues, 0, 200_000, PI_MAX_PROMPT_ARGUMENT_BYTES).unwrap();
        assert!(end < cues.len() - 1);
        assert!(estimate_segment_argument_bytes(&cues[..=end]) <= PI_MAX_PROMPT_ARGUMENT_BYTES);
    }

    #[test]
    fn adaptive_segmentation_uses_overlap_and_model_boundary() {
        let local = vec![(0, 2), (3, 5), (6, 8)];
        validate_ranges_cover(9, &local).unwrap();
        let boundary = choose_adaptive_boundary(&local, 10, 12, 15, 18).unwrap();
        assert_eq!(boundary, 15);
        let mut output = Vec::new();
        append_core_ranges(&mut output, &local, 10, 12, boundary).unwrap();
        assert_eq!(output, vec![(12, 12), (13, 15)]);
    }

    #[test]
    fn translation_batch_requires_every_index_in_order() {
        let valid = vec![(0, "甲".into()), (1, "乙".into())];
        validate_translation_indexes(2, &valid).unwrap();
        let duplicate = vec![(0, "甲".into()), (0, "乙".into())];
        assert!(validate_translation_indexes(2, &duplicate).is_err());
    }

    #[test]
    fn cached_cues_require_valid_timeline_and_source() {
        let valid = vec![cue(0, "hello"), cue(1, "world")];
        validate_cached_cues(&valid).unwrap();

        let mut invalid_time = valid.clone();
        invalid_time[1].start = f64::NAN;
        assert!(validate_cached_cues(&invalid_time).is_err());

        let mut invalid_source = valid;
        invalid_source[0].source.clear();
        assert!(validate_cached_cues(&invalid_source).is_err());
    }

    #[test]
    fn partial_translation_checkpoint_resumes_only_missing_batches() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("translated.json");
        let source = vec![cue(0, "hello"), cue(1, "world"), cue(2, "again")];
        let mut partial = source.clone();
        partial[0].translation = Some("你好".into());
        partial[1].translation = Some("世界".into());
        subtitle::save_json(&partial, &path).unwrap();

        let loaded = load_translation_checkpoint(&path, &source)
            .unwrap()
            .unwrap();
        assert!(!translation_checkpoint_complete(&loaded));
        assert!(translation_batch_checkpointed(&loaded, 0, 2));
        assert!(!translation_batch_checkpointed(&loaded, 2, 3));

        let mut completed = loaded;
        completed[2].translation = Some("再来".into());
        subtitle::save_json(&completed, &path).unwrap();
        let loaded = load_translation_checkpoint(&path, &source)
            .unwrap()
            .unwrap();
        assert!(translation_checkpoint_complete(&loaded));
    }

    #[test]
    fn publication_metadata_cleans_tags_and_forces_core_tag() {
        let value = json!({
            "title": "2026年最佳排位赛",
            "dynamic": "最后一局上演极限翻盘。",
            "tags": ["#排位赛", "荒野乱斗", "排位赛", "英雄，技巧", "超长标签超长标签超长标签超长标签超长标签"]
        });
        let parsed = parse_publication_metadata(&value).unwrap();
        assert_eq!(parsed.tid, 172);
        assert_eq!(parsed.tags[0], "荒野乱斗");
        assert_eq!(parsed.tags.len(), 4);
        assert!(
            parsed
                .tags
                .iter()
                .all(|tag| !tag.contains(['#', ',', '，']))
        );
        assert!(parsed.tags.iter().all(|tag| tag.chars().count() <= 20));
    }

    #[test]
    fn publication_metadata_rejects_invalid_title_or_dynamic() {
        assert!(
            parse_publication_metadata(&json!({
                "title": "",
                "dynamic": "精彩对局。",
                "tags": ["荒野乱斗"]
            }))
            .is_err()
        );
        assert!(
            parse_publication_metadata(&json!({
                "title": "精彩对局",
                "dynamic": "欢迎点赞投币关注我！",
                "tags": ["荒野乱斗"]
            }))
            .is_err()
        );
    }

    #[test]
    fn publication_metadata_removes_emoji_from_text_fields() {
        let parsed = parse_publication_metadata(&json!({
            "title": "新英雄登场 💀🔥",
            "dynamic": "本期展示冠军对局。🏆",
            "tags": ["荒野乱斗"]
        }))
        .unwrap();
        assert_eq!(parsed.title, "新英雄登场");
        assert_eq!(parsed.dynamic, "本期展示冠军对局。");
    }

    #[test]
    fn metadata_payload_uses_full_or_uniformly_sampled_bilingual_subtitles() {
        let mut cues = (0..100)
            .map(|i| cue(i, "a representative subtitle sentence with context"))
            .collect::<Vec<_>>();
        for (index, cue) in cues.iter_mut().enumerate() {
            cue.translation = Some(format!("第{index}句译文"));
        }
        let full =
            build_publication_payload(TransferMode::Translated, &metadata(), Some(&cues), 200_000)
                .unwrap();
        assert_eq!(full["subtitle_sampling"]["sampled"], false);
        assert_eq!(full["subtitles"].as_array().unwrap().len(), cues.len());

        let minimum = publication_payload_value(
            TransferMode::Translated,
            &metadata(),
            &cues,
            &[0, cues.len() - 1],
            true,
        );
        let sampled = build_publication_payload(
            TransferMode::Translated,
            &metadata(),
            Some(&cues),
            estimate_publication_tokens(&minimum) + 250,
        )
        .unwrap();
        let included = sampled["subtitles"].as_array().unwrap();
        assert_eq!(sampled["subtitle_sampling"]["sampled"], true);
        assert!(included.len() < cues.len());
        assert_eq!(included.first().unwrap()["i"], 0);
        assert_eq!(included.last().unwrap()["i"], cues.len() - 1);

        let mut large = (0..1_075)
            .map(|i| cue(i, "a representative subtitle sentence with context"))
            .collect::<Vec<_>>();
        for (index, cue) in large.iter_mut().enumerate() {
            cue.translation = Some(format!("第{index}句包含足够长度的中文字幕内容"));
        }
        let bounded =
            build_publication_payload(TransferMode::Translated, &metadata(), Some(&large), 200_000)
                .unwrap();
        assert!(bounded.to_string().len() <= PI_MAX_PROMPT_ARGUMENT_BYTES);
        // 128KB 上限下 1075 条双语字幕（约 300KB）触发均匀采样。
        assert_eq!(bounded["subtitle_sampling"]["sampled"], true);
        assert!(bounded["subtitles"].as_array().unwrap().len() < large.len());
    }

    #[test]
    fn direct_metadata_accepts_empty_description_but_translated_needs_subtitles() {
        let mut meta = metadata();
        meta.description = None;
        let payload =
            build_publication_payload(TransferMode::Direct, &meta, None, 200_000).unwrap();
        assert_eq!(payload["youtube"]["description"], "");
        assert!(build_publication_payload(TransferMode::Translated, &meta, None, 200_000).is_err());
    }

    #[test]
    fn download_selector_prioritizes_the_video_orientation() {
        let landscape = metadata();
        let landscape_selector = download_format_selector(&landscape, 2_073_600, 60.0);
        assert!(
            landscape_selector
                .split('/')
                .next()
                .unwrap()
                .contains("[width<=1920][height<=1080]")
        );

        let mut vertical = metadata();
        vertical.url = "https://www.youtube.com/shorts/video".into();
        vertical.width = Some(1080);
        vertical.height = Some(1920);
        let vertical_selector = download_format_selector(&vertical, 2_073_600, 60.0);
        assert!(
            vertical_selector
                .split('/')
                .next()
                .unwrap()
                .contains("[width<=1080][height<=1920]")
        );
    }

    #[test]
    fn upload_args_are_fixed_original_metadata_and_detailed_description() {
        let publication = parse_publication_metadata(&json!({
            "title": "2026年最佳排位赛",
            "dynamic": "最后一局上演极限翻盘。",
            "tags": ["荒野乱斗", "排位赛"]
        }))
        .unwrap();
        let args = build_upload_args(&publication, &metadata());
        let value_after = |flag: &str| {
            let index = args.iter().position(|value| value == flag).unwrap();
            args[index + 1].as_str()
        };
        assert_eq!(value_after("--tid"), "172");
        assert_eq!(value_after("--submit"), "web");
        assert_eq!(value_after("--copyright"), "1");
        assert_eq!(value_after("--no-reprint"), "0");
        assert!(!args.iter().any(|value| value == "--source"));
        assert_eq!(value_after("--tag"), "荒野乱斗,排位赛");
        assert_eq!(value_after("--dynamic"), "最后一局上演极限翻盘。");
        let description = value_after("--desc");
        assert!(description.contains("原标题：Best Ranked Match 2026"));
        assert!(description.contains("来源：https://www.youtube.com/watch?v=video"));
        assert!(description.contains("原作者：Player One"));
        assert!(description.contains("原视频发布时间："));
        assert!(!description.contains("原发布日期："));
        assert!(description.contains("处理工具：https://github.com/Mist-wu/y2b-rs"));
        assert!(!description.contains("处理方式"));
    }

    #[test]
    fn description_removes_hashtags_and_includes_publication_date() {
        let mut meta = metadata();
        meta.title = "Poor   Alli #bs #brawlstars ＃keepbrawlalive".into();
        meta.url = "https://www.youtube.com/watch?v=F8yN5-ctCZw".into();
        meta.webpage_url = Some(meta.url.clone());
        meta.uploader = Some("Bazilious".into());
        let description = build_description(&meta);
        let expected = format!(
            "原标题：Poor Alli\n来源：https://www.youtube.com/watch?v=F8yN5-ctCZw\n原作者：Bazilious\n原视频发布时间：{}\n处理工具：https://github.com/Mist-wu/y2b-rs",
            publication_date(&meta)
        );
        assert_eq!(description, expected);
    }

    #[test]
    fn description_omits_title_when_it_contains_only_hashtags() {
        let mut meta = metadata();
        meta.title = "#bs ＃brawlstars".into();
        let description = build_description(&meta);
        assert!(!description.contains("原标题："));
        assert!(description.starts_with("来源：https://www.youtube.com/watch?v=video\n"));
        assert!(description.contains("处理工具：https://github.com/Mist-wu/y2b-rs"));
        assert!(!description.contains("处理方式"));
    }

    #[test]
    fn transfer_mode_routes_only_requested_jobs_through_subtitles() {
        assert!(!requires_translated_pipeline(TransferMode::Direct));
        assert!(requires_translated_pipeline(TransferMode::Translated));
    }

    #[test]
    fn cc_cues_keep_only_translated_in_order() {
        let cues = vec![
            Cue {
                start: 0.0,
                end: 1.5,
                source: "hi".into(),
                translation: Some(" 你好 ".into()),
            },
            Cue {
                start: 1.5,
                end: 3.0,
                source: "empty translation".into(),
                translation: Some("   ".into()),
            },
            Cue {
                start: 3.0,
                end: 2.0,
                source: "inverted".into(),
                translation: Some("倒序".into()),
            },
            Cue {
                start: 4.0,
                end: 5.0,
                source: "none".into(),
                translation: None,
            },
        ];
        let cc = cc_cues_from(&cues, None);
        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0].content, "你好");
        assert_eq!(cc[0].from, 0.0);
        assert_eq!(cc[0].to, 1.5);
    }

    #[test]
    fn cc_cues_clip_beyond_video_duration() {
        let cues = vec![
            Cue {
                start: 0.0,
                end: 5.0,
                source: "a".into(),
                translation: Some("正常".into()),
            },
            Cue {
                start: 4.0,
                end: 9.0,
                source: "b".into(),
                translation: Some("跨越结尾".into()),
            },
            Cue {
                start: 7.0,
                end: 8.0,
                source: "c".into(),
                translation: Some("整体超出".into()),
            },
        ];
        let cc = cc_cues_from(&cues, Some(6.5));
        assert_eq!(cc.len(), 2);
        assert_eq!(cc[0].to, 5.0);
        assert_eq!(cc[1].to, 6.5);
    }

    #[tokio::test]
    async fn media_branches_start_concurrently() {
        let barrier = Arc::new(Barrier::new(2));
        let left_barrier = barrier.clone();
        let right_barrier = barrier.clone();
        let joined = tokio::time::timeout(
            Duration::from_secs(1),
            try_join_branches(
                async move {
                    left_barrier.wait().await;
                    Ok::<_, anyhow::Error>("video")
                },
                async move {
                    right_barrier.wait().await;
                    Ok::<_, anyhow::Error>("subtitle")
                },
            ),
        )
        .await
        .expect("branches did not run concurrently")
        .unwrap();
        assert_eq!(joined, ("video", "subtitle"));
    }

    #[tokio::test]
    async fn pausing_job_interrupts_submission_window_wait() {
        let temp = tempfile::tempdir().unwrap();
        let db = Database::open(&temp.path().join("state.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "pause-window",
                url: "https://youtu.be/pause-window",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        db.queue_prepared_upload(
            &id,
            &PreparedUpload::Submission {
                video_path: "/tmp/pause-window.mp4".into(),
                cover_path: "/tmp/pause-window.jpg".into(),
                mode: TransferMode::Direct,
                completion_status: JobStatus::Completed,
            },
        )
        .unwrap();
        db.set_setting(
            NEXT_BILIBILI_SUBMIT_AT,
            &(Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
        )
        .unwrap();
        let pipeline = Pipeline::new(Config::default(), db.clone());
        let wait_id = id.clone();
        let waiter = tokio::spawn(async move {
            pipeline
                .wait_for_bilibili_submission_with_poll(&wait_id, Duration::from_millis(5))
                .await
        });
        tokio::task::yield_now().await;
        db.update_job_status(&id, JobStatus::Paused, None).unwrap();

        let should_upload = tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("暂停后仍未退出投稿窗口等待")
            .unwrap()
            .unwrap();
        assert!(!should_upload);
        assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Paused);
    }

    #[tokio::test]
    async fn submission_window_handles_missing_past_and_invalid_values() {
        let temp = tempfile::tempdir().unwrap();
        let db = Database::open(&temp.path().join("state.db")).unwrap();
        let pipeline = Pipeline::new(Config::default(), db.clone());

        assert!(
            pipeline
                .wait_for_bilibili_submission("unused")
                .await
                .unwrap()
        );

        db.set_setting(
            NEXT_BILIBILI_SUBMIT_AT,
            &(Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
        )
        .unwrap();
        assert!(
            pipeline
                .wait_for_bilibili_submission("unused")
                .await
                .unwrap()
        );

        db.set_setting(NEXT_BILIBILI_SUBMIT_AT, "not-a-timestamp")
            .unwrap();
        assert!(
            pipeline
                .wait_for_bilibili_submission("unused")
                .await
                .unwrap_err()
                .to_string()
                .contains("投稿冷却时间无效")
        );
    }
}
