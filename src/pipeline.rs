use crate::config::{BatchMode, Config};
use crate::db::Database;
use crate::model::{AiUsage, Job, JobStatus, PublicationMetadata, TransferMode, VideoMetadata};
use crate::monitor::Monitor;
use crate::process::{ProcessOutput, run_monitored};
use crate::subtitle::{self, Cue};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde_json::{Value, json};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::Command;

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
    ass: PathBuf,
    cues: Vec<Cue>,
}

impl Pipeline {
    pub fn new(config: Config, db: Database) -> Self {
        Self { config, db }
    }

    pub async fn run_job(&self, job: Job) -> Result<()> {
        let result = self.run_job_inner(&job).await;
        if let Err(e) = &result {
            let attempt = self.db.increment_attempt(&job.id)?;
            let failed_status = self.db.get_job(&job.id)?.map(|job| job.status);
            let upload_failure = matches!(
                failed_status,
                Some(JobStatus::Uploading | JobStatus::Appending)
            );
            let rate_limited = e.to_string().contains("21566");
            let status = if rate_limited
                || (upload_failure && attempt >= self.config.monitor.max_attempts as i64)
            {
                JobStatus::Paused
            } else if attempt >= self.config.monitor.max_attempts as i64 {
                self.cleanup_large(&job.id).ok();
                JobStatus::DeadLetter
            } else {
                JobStatus::RetryWait
            };
            self.db
                .update_job_status(&job.id, status, Some(&e.to_string()))?;
            self.db.event(Some(&job.id), "error", &e.to_string())?;
        }
        result
    }

    async fn run_job_inner(&self, job: &Job) -> Result<()> {
        self.ensure_disk()?;
        if let Some(limit) = self.config.ai.daily_token_limit
            && self.db.ai_tokens_today()? >= limit
        {
            bail!("已达到每日 token 上限 {limit}")
        }
        if self
            .db
            .get_setting("auth.bilibili")?
            .is_some_and(|s| s.starts_with("failed"))
        {
            bail!("Bilibili 认证失效，暂停处理并保留现有文件")
        }
        let append_to = job.append_to_bvid.clone();
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
        let (meta, peak, duration) = monitor.fetch_metadata(&job.url).await?;
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
        let work = self.config.runtime.download_dir.join(&meta.id);
        fs::create_dir_all(&work)?;
        self.db
            .update_job_status(&job.id, JobStatus::Processing, None)?;
        if !requires_translated_pipeline(job.transfer_mode, append_to.is_some()) {
            return self.run_direct(job, &meta, &work).await;
        }

        // A Bilibili submission can fail after the complete media upload (for
        // example, rate-limit code 21566).  Retrying that failure must reuse the
        // expensive rendered file instead of translating and encoding again.
        if append_to.is_none()
            && let Some(publication) = self.db.publication_metadata(&job.id)?
            && let Some(rendered) = self.reusable_render(&meta.id).await?
        {
            self.db.event(
                Some(&job.id),
                "info",
                "检测到有效成片，跳过字幕翻译和压制并重试投稿",
            )?;
            self.db
                .set_job_paths(&job.id, None, None, Some(&rendered.to_string_lossy()))?;
            self.probe_media(&job.id, &rendered, "rendered_probe")
                .await?;
            let cover = self.download_cover(&job.id, &meta, &work).await?;
            let bvid = self
                .upload(
                    &job.id,
                    &rendered,
                    &publication,
                    &meta,
                    TransferMode::Translated,
                    Some(&cover),
                )
                .await?;
            self.db.set_job_bvid(&job.id, &bvid)?;
            self.db
                .update_job_status(&job.id, JobStatus::Completed, None)?;
            let (raw, _, _) = self.db.job_paths(&job.id)?;
            if let Some(raw) = raw {
                self.after_upload(&job.id, &[Path::new(&raw), &rendered])?;
            } else {
                self.after_upload(&job.id, &[&rendered])?;
            }
            return Ok(());
        }

        let video_fut = self.download_video(&job.id, &job.url, &meta, &work);
        let subtitle_fut = self.prepare_translated_subtitle(job, &meta, &work);
        let (video, subtitle) = try_join_branches(video_fut, subtitle_fut).await?;
        self.db
            .set_job_paths(&job.id, Some(&video.to_string_lossy()), None, None)?;
        let Some(subtitle) = subtitle else {
            if append_to.is_some() {
                self.cleanup_large(&job.id)?;
                self.db.update_job_status(
                    &job.id,
                    JobStatus::UploadedOriginalPendingSubtitle,
                    Some("仍无可用字幕"),
                )?;
                return Ok(());
            }
            let publication = self
                .publish_metadata(&job.id, TransferMode::Direct, &meta, None)
                .await?;
            return self
                .finish_direct_upload(job, &meta, &video, &publication, true)
                .await;
        };
        let publication = if append_to.is_none() {
            Some(
                self.publish_metadata(
                    &job.id,
                    TransferMode::Translated,
                    &meta,
                    Some(&subtitle.cues),
                )
                .await?,
            )
        } else {
            None
        };
        let rendered = self
            .render(&job.id, &video, &subtitle.ass, &meta.id)
            .await?;
        self.db
            .set_job_paths(&job.id, None, None, Some(&rendered.to_string_lossy()))?;
        self.probe_media(&job.id, &rendered, "rendered_probe")
            .await?;
        let bvid = if let Some(existing) = append_to.as_deref() {
            self.append(&job.id, &rendered, existing).await?;
            existing.to_owned()
        } else {
            let cover = self.download_cover(&job.id, &meta, &work).await?;
            self.upload(
                &job.id,
                &rendered,
                publication.as_ref().context("投稿元数据未生成")?,
                &meta,
                TransferMode::Translated,
                Some(&cover),
            )
            .await?
        };
        self.db.set_job_bvid(&job.id, &bvid)?;
        if append_to.is_some() {
            self.db.clear_job_append_target(&job.id)?;
        }
        self.db
            .update_job_status(&job.id, JobStatus::Completed, None)?;
        self.after_upload(&job.id, &[&video, &rendered])?;
        Ok(())
    }

    async fn run_direct(&self, job: &Job, meta: &VideoMetadata, work: &Path) -> Result<()> {
        let video_fut = self.download_video(&job.id, &job.url, meta, work);
        let metadata_fut = self.publish_metadata(&job.id, TransferMode::Direct, meta, None);
        let (video, publication) = try_join_branches(video_fut, metadata_fut).await?;
        self.db
            .set_job_paths(&job.id, Some(&video.to_string_lossy()), None, None)?;
        self.finish_direct_upload(job, meta, &video, &publication, false)
            .await
    }

    async fn finish_direct_upload(
        &self,
        job: &Job,
        meta: &VideoMetadata,
        video: &Path,
        publication: &PublicationMetadata,
        pending_subtitle: bool,
    ) -> Result<()> {
        self.probe_media(&job.id, video, "original_probe").await?;
        let work = video.parent().context("视频文件没有工作目录")?;
        let cover = self.download_cover(&job.id, meta, work).await?;
        let bvid = self
            .upload(
                &job.id,
                video,
                publication,
                meta,
                TransferMode::Direct,
                Some(&cover),
            )
            .await?;
        self.db.set_job_bvid(&job.id, &bvid)?;
        self.db.update_job_status(
            &job.id,
            if pending_subtitle {
                JobStatus::UploadedOriginalPendingSubtitle
            } else {
                JobStatus::Completed
            },
            None,
        )?;
        self.after_upload(&job.id, &[video])?;
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
        let Some(raw_sub) = self
            .download_subtitle(&job.id, &job.url, &meta.id, work)
            .await?
        else {
            return Ok(None);
        };
        let mut cues = subtitle::parse_vtt(&raw_sub)?;
        cues = self.segment(&job.id, &cues).await?;
        let segmented = work.join(format!("{}.en.segmented.json", meta.id));
        subtitle::save_json(&cues, &segmented)?;
        self.translate(&job.id, &mut cues).await?;
        let translated = work.join(format!("{}.en-zh-CN.translated.json", meta.id));
        subtitle::save_json(&cues, &translated)?;
        let ass = work.join(format!("{}.bilingual.ass", meta.id));
        subtitle::write_ass(
            &cues,
            &ass,
            meta.width.unwrap_or(1920),
            meta.height.unwrap_or(1080),
            &self.config.render.font_cn,
            &self.config.render.font_en,
        )?;
        self.db
            .set_job_paths(&job.id, None, Some(&ass.to_string_lossy()), None)?;
        Ok(Some(PreparedSubtitle { ass, cues }))
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
        let mut cmd = Command::new(&self.config.youtube.yt_dlp);
        cmd.args([
            "--js-runtimes",
            "node",
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
        if self.config.youtube.cookies.exists() {
            cmd.arg("--cookies").arg(&self.config.youtube.cookies);
        }
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
        let mut cmd = Command::new(&self.config.youtube.yt_dlp);
        let format = download_format_selector(
            meta,
            self.config.youtube.max_pixels,
            self.config.youtube.max_fps,
        );
        cmd.args([
            "--js-runtimes",
            "node",
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
        if self.config.youtube.cookies.exists() {
            cmd.arg("--cookies").arg(&self.config.youtube.cookies);
        }
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
        let mut ranges = Vec::new();
        let mut duration = 0;
        let mut peak = 0;
        let mut calls = 0;
        if self.config.ai.batch_mode == BatchMode::WholeVideo || estimated <= budget {
            if estimated > budget {
                bail!(
                    "whole_video 分句预计需要 {estimated} tokens，超过安全阈值 {budget}；请改用 adaptive"
                )
            }
            let (local, elapsed, rss) = self
                .segment_batch(job_id, stage, cues, 0, cues.len().saturating_sub(1))
                .await?;
            ranges = local;
            duration += elapsed;
            peak = peak.max(rss);
            calls = 1;
        } else {
            let overlap = self.config.ai.segment_overlap_cues;
            let mut cursor = 0;
            while cursor < cues.len() {
                let window_start = cursor.saturating_sub(overlap);
                let window_end = max_segment_window_end(cues, window_start, budget)?;
                if window_end < cursor {
                    bail!("安全 token 阈值过小，无法容纳分句核心字幕")
                }
                let has_more = window_end + 1 < cues.len();
                let preferred_end = if has_more {
                    window_end.saturating_sub(overlap).max(cursor)
                } else {
                    cues.len() - 1
                };
                let (local, elapsed, rss) = self
                    .segment_batch(
                        job_id,
                        stage,
                        &cues[window_start..=window_end],
                        cursor - window_start,
                        preferred_end - window_start,
                    )
                    .await?;
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
                "{} -> {} cues; mode={}; estimated_tokens={estimated}; calls={calls}",
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
        let r = self.call_pi(payload).await?;
        let local = parse_ranges(&r.value)?;
        validate_ranges_cover(cues.len(), &local)?;
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

    async fn translate(&self, job_id: &str, cues: &mut [Cue]) -> Result<()> {
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
        let mut all = Vec::new();
        let mut duration = 0;
        let mut peak = 0;
        for &(start, end) in &batches {
            let chunk = &cues[start..end];
            let payload = json!({"task":"translate","source_lang":self.config.translation.source_lang,"target_lang":self.config.translation.target_lang,"items":chunk.iter().enumerate().map(|(i,c)|json!({"i":i,"text":c.source})).collect::<Vec<_>>()});
            let input_json = payload.to_string();
            let r = self.call_pi(payload).await?;
            let local = parse_translations(&r.value)?;
            validate_translation_indexes(chunk.len(), &local)?;
            for (i, t) in local {
                all.push((i + start, t));
            }
            self.db.record_ai_call(
                job_id,
                stage,
                "translate",
                &self.config.ai.provider,
                &self.config.ai.model,
                &self.config.ai.thinking,
                &r.usage,
                r.output.duration_ms,
                &input_json,
                &r.value.to_string(),
            )?;
            duration += r.output.duration_ms;
            peak = peak.max(r.output.peak_rss_kib);
        }
        subtitle::apply_translations(cues, &all)?;
        self.db.finish_stage(
            stage,
            "completed",
            duration,
            peak,
            Some(&format!(
                "{} cues; mode={}; estimated_tokens={estimated}; calls={}",
                cues.len(),
                batch_mode_name(self.config.ai.batch_mode),
                batches.len()
            )),
        )?;
        Ok(())
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
        let payload = build_publication_payload(mode, meta, cues, self.ai_token_budget()?)?;
        let input_json = payload.to_string();
        let stage = self.db.start_stage(
            job_id,
            "publish_metadata",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let r = match self.call_pi(payload).await {
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
            None,
        )?;
        Ok(metadata)
    }

    async fn call_pi(&self, payload: Value) -> Result<PiResult> {
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
            &self.config.ai.thinking,
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

    async fn render(
        &self,
        job_id: &str,
        video: &Path,
        ass: &Path,
        video_id: &str,
    ) -> Result<PathBuf> {
        self.db
            .update_job_status(job_id, JobStatus::Rendering, None)?;
        let stage = self.db.start_stage(job_id, "render", None, None, None)?;
        fs::create_dir_all(&self.config.runtime.output_dir)?;
        let output = self
            .config
            .runtime
            .output_dir
            .join(format!("{video_id}.bilingual.mp4"));
        if self.render_file_is_valid(&output).await {
            self.db.finish_stage(
                stage,
                "completed",
                0,
                0,
                Some(&format!("reused {}", output.display())),
            )?;
            return Ok(output);
        }
        let temporary = self
            .config
            .runtime
            .output_dir
            .join(format!("{video_id}.bilingual.tmp.mp4"));
        if temporary.exists() {
            fs::remove_file(&temporary)?;
        }
        let filter = format!(
            "ass=filename='{}':fontsdir='{}'",
            escape_filter(ass),
            escape_filter(&self.config.render.fonts_dir)
        );
        let mut cmd = Command::new(&self.config.render.ffmpeg);
        cmd.args(["-nostdin", "-y", "-loglevel", "warning"])
            .arg("-i")
            .arg(video)
            .args([
                "-vf",
                &filter,
                "-threads",
                "1",
                "-c:v",
                "libx264",
                "-preset",
                &self.config.render.preset,
                "-crf",
                &self.config.render.crf.to_string(),
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-movflags",
                "+faststart",
            ])
            .arg(&temporary);
        let out = match run_monitored(cmd, Duration::from_secs(14400)).await {
            Ok(output) => output,
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                self.db
                    .finish_stage(stage, "failed", 0, 0, Some(&error.to_string()))?;
                return Err(error);
            }
        };
        if !self.render_file_is_valid(&temporary).await {
            let _ = fs::remove_file(&temporary);
            let error = anyhow::anyhow!("FFmpeg 生成的临时成片未通过 ffprobe 校验");
            self.db.finish_stage(
                stage,
                "failed",
                out.duration_ms,
                out.peak_rss_kib,
                Some(&error.to_string()),
            )?;
            return Err(error);
        }
        fs::rename(&temporary, &output)?;
        self.db.finish_stage(
            stage,
            "completed",
            out.duration_ms,
            out.peak_rss_kib,
            Some(&output.to_string_lossy()),
        )?;
        Ok(output)
    }

    async fn reusable_render(&self, video_id: &str) -> Result<Option<PathBuf>> {
        let output = self
            .config
            .runtime
            .output_dir
            .join(format!("{video_id}.bilingual.mp4"));
        Ok(self.render_file_is_valid(&output).await.then_some(output))
    }

    async fn render_file_is_valid(&self, path: &Path) -> bool {
        if !path.metadata().is_ok_and(|metadata| metadata.len() > 0) {
            return false;
        }
        let mut cmd = Command::new(&self.config.render.ffprobe);
        cmd.args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height,pix_fmt:format=duration",
            "-of",
            "json",
        ])
        .arg(path);
        let Ok(output) = run_monitored(cmd, Duration::from_secs(120)).await else {
            return false;
        };
        let Ok(value) = serde_json::from_str::<Value>(&output.stdout) else {
            return false;
        };
        let Some(stream) = value["streams"]
            .as_array()
            .and_then(|streams| streams.first())
        else {
            return false;
        };
        stream["codec_name"] == "h264"
            && stream["width"].as_u64().is_some_and(|width| width > 0)
            && stream["height"].as_u64().is_some_and(|height| height > 0)
            && value["format"]["duration"]
                .as_str()
                .and_then(|duration| duration.parse::<f64>().ok())
                .is_some_and(|duration| duration > 0.0)
    }

    async fn upload(
        &self,
        job_id: &str,
        video: &Path,
        publication: &PublicationMetadata,
        meta: &VideoMetadata,
        mode: TransferMode,
        cover: Option<&Path>,
    ) -> Result<String> {
        self.db
            .update_job_status(job_id, JobStatus::Uploading, None)?;
        let stage = self.db.start_stage(job_id, "upload", None, None, None)?;
        let mut cmd = Command::new(&self.config.bilibili.biliup);
        cmd.arg("-u")
            .arg(&self.config.bilibili.cookies)
            .arg("upload")
            .arg(video)
            .args(build_upload_args(publication, meta, mode));
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
        Ok(bvid)
    }
    async fn append(&self, job_id: &str, video: &Path, bvid: &str) -> Result<()> {
        if let Some(parts) = self.bilibili_part_count(bvid).await?
            && parts >= self.config.bilibili.max_parts
        {
            bail!(
                "稿件 {bvid} 已有 {parts}P，达到安全上限 {}P",
                self.config.bilibili.max_parts
            )
        }
        self.db
            .update_job_status(job_id, JobStatus::Appending, None)?;
        let stage = self.db.start_stage(job_id, "append", None, None, None)?;
        let mut cmd = Command::new(&self.config.bilibili.biliup);
        cmd.arg("-u")
            .arg(&self.config.bilibili.cookies)
            .args(build_append_args(bvid))
            .arg(video);
        let out = run_monitored(cmd, Duration::from_secs(14400)).await?;
        self.db.finish_stage(
            stage,
            "completed",
            out.duration_ms,
            out.peak_rss_kib,
            Some(bvid),
        )?;
        Ok(())
    }
    async fn bilibili_part_count(&self, bvid: &str) -> Result<Option<usize>> {
        let mut cmd = Command::new(&self.config.bilibili.biliup);
        cmd.arg("-u")
            .arg(&self.config.bilibili.cookies)
            .arg("show")
            .arg(bvid);
        let out = match run_monitored(cmd, Duration::from_secs(120)).await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(error=%e,"无法读取现有分P数量");
                return Ok(None);
            }
        };
        let merged = out.stdout + "\n" + &out.stderr;
        let value = serde_json::from_str::<Value>(merged.trim())
            .ok()
            .or_else(|| {
                merged
                    .find('{')
                    .and_then(|start| serde_json::from_str::<Value>(&merged[start..]).ok())
            });
        Ok(value
            .as_ref()
            .and_then(|v| v.get("videos").or_else(|| v.get("pages")))
            .and_then(Value::as_array)
            .map(Vec::len))
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
        let (raw, _, rendered) = self.db.job_paths(job_id)?;
        for p in [raw, rendered].into_iter().flatten() {
            let path = PathBuf::from(p);
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
}

const PI_PROMPT_OVERHEAD_TOKENS: usize = 2_048;
const PI_METADATA_OUTPUT_RESERVE_TOKENS: usize = 1_024;
const PI_MAX_PROMPT_ARGUMENT_BYTES: usize = 96 * 1024;
const BILIBILI_TID: i64 = 172;
const CORE_TAG: &str = "荒野乱斗";
const MAX_TITLE_WIDTH: usize = 70;
const MAX_DYNAMIC_WIDTH: usize = 120;
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
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .context("Pi 投稿元数据缺少字符串 title")?
        .trim()
        .to_string();
    let dynamic = value
        .get("dynamic")
        .and_then(Value::as_str)
        .context("Pi 投稿元数据缺少字符串 dynamic")?
        .trim()
        .to_string();
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

fn build_description(meta: &VideoMetadata, mode: TransferMode) -> String {
    let source_url = meta.webpage_url.as_deref().unwrap_or(&meta.url);
    let uploader = meta
        .uploader
        .as_deref()
        .or(meta.channel.as_deref())
        .unwrap_or("未知");
    let treatment = match mode {
        TransferMode::Direct => "仅翻译标题",
        TransferMode::Translated => "中英双语字幕翻译压制",
    };
    let mut lines = Vec::with_capacity(5);
    if let Some(title) = original_title_without_hashtags(&meta.title) {
        lines.push(format!("原标题：{title}"));
    }
    lines.extend([
        format!("来源：{source_url}"),
        format!("原作者：{uploader}"),
        format!("处理方式：{treatment}"),
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

fn build_upload_args(
    metadata: &PublicationMetadata,
    meta: &VideoMetadata,
    mode: TransferMode,
) -> Vec<String> {
    vec![
        "--submit".into(),
        "web".into(),
        "--title".into(),
        metadata.title.clone(),
        "--desc".into(),
        build_description(meta, mode),
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

fn build_append_args(bvid: &str) -> Vec<String> {
    vec![
        "append".into(),
        "--vid".into(),
        bvid.into(),
        "--limit".into(),
        "1".into(),
    ]
}

fn requires_translated_pipeline(mode: TransferMode, has_append_target: bool) -> bool {
    mode == TransferMode::Translated || has_append_target
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

fn estimate_translation_tokens(cues: &[Cue]) -> usize {
    PI_PROMPT_OVERHEAD_TOKENS + cues.iter().map(translation_cue_tokens).sum::<usize>()
}

fn max_segment_window_end(cues: &[Cue], start: usize, budget: usize) -> Result<usize> {
    if start >= cues.len() {
        bail!("分句窗口起点越界: {start}/{}", cues.len())
    }
    let mut total = PI_PROMPT_OVERHEAD_TOKENS;
    let mut end = None;
    for (index, cue) in cues.iter().enumerate().skip(start) {
        let item = segment_cue_tokens(cue);
        if total.saturating_add(item) > budget {
            break;
        }
        total += item;
        end = Some(index);
    }
    end.with_context(|| {
        format!(
            "单条字幕已超过安全 token 阈值 {budget}: cue={start}, estimated={}",
            PI_PROMPT_OVERHEAD_TOKENS + segment_cue_tokens(&cues[start])
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
fn escape_filter(p: &Path) -> String {
    p.to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "\\'")
        .replace(':', "\\:")
        .replace(',', "\\,")
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
        assert_eq!(bounded["subtitle_sampling"]["sampled"], true);
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
        let args = build_upload_args(&publication, &metadata(), TransferMode::Translated);
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
        assert!(!description.contains("原发布日期："));
        assert!(description.contains("处理方式：中英双语字幕翻译压制"));
    }

    #[test]
    fn description_removes_hashtags_and_publication_date() {
        let mut meta = metadata();
        meta.title = "Poor   Alli #bs #brawlstars ＃keepbrawlalive".into();
        meta.url = "https://www.youtube.com/watch?v=F8yN5-ctCZw".into();
        meta.webpage_url = Some(meta.url.clone());
        meta.uploader = Some("Bazilious".into());
        let description = build_description(&meta, TransferMode::Direct);
        assert_eq!(
            description,
            "原标题：Poor Alli\n来源：https://www.youtube.com/watch?v=F8yN5-ctCZw\n原作者：Bazilious\n处理方式：仅翻译标题\n处理工具：https://github.com/Mist-wu/y2b-rs"
        );
    }

    #[test]
    fn description_omits_title_when_it_contains_only_hashtags() {
        let mut meta = metadata();
        meta.title = "#bs ＃brawlstars".into();
        let description = build_description(&meta, TransferMode::Translated);
        assert!(!description.contains("原标题："));
        assert!(description.starts_with("来源：https://www.youtube.com/watch?v=video\n"));
        assert!(description.contains("处理方式：中英双语字幕翻译压制"));
    }

    #[test]
    fn append_args_only_target_existing_bvid() {
        assert_eq!(
            build_append_args("BV1test"),
            ["append", "--vid", "BV1test", "--limit", "1"]
        );
    }

    #[test]
    fn transfer_mode_routes_only_requested_jobs_through_subtitles() {
        assert!(!requires_translated_pipeline(TransferMode::Direct, false));
        assert!(requires_translated_pipeline(
            TransferMode::Translated,
            false
        ));
        assert!(requires_translated_pipeline(TransferMode::Direct, true));
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
}
