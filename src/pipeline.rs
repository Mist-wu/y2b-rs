use crate::config::Config;
use crate::db::Database;
use crate::model::{AiUsage, Job, JobStatus};
use crate::monitor::Monitor;
use crate::process::{ProcessOutput, run_monitored};
use crate::subtitle::{self, Cue};
use anyhow::{Context, Result, bail};
use regex::Regex;
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
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

impl Pipeline {
    pub fn new(config: Config, db: Database) -> Self {
        Self { config, db }
    }

    pub async fn run_job(&self, job: Job) -> Result<()> {
        let result = self.run_job_inner(&job).await;
        if let Err(e) = &result {
            let attempt = self.db.increment_attempt(&job.id)?;
            let status = if attempt >= self.config.monitor.max_attempts as i64 {
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
        let append_to = if job.status == JobStatus::UploadedOriginalPendingSubtitle {
            job.bvid.clone()
        } else {
            None
        };
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
        let subtitle = self
            .download_subtitle(&job.id, &job.url, &meta.id, &work)
            .await?;
        if append_to.is_some() && subtitle.is_none() {
            self.db.update_job_status(
                &job.id,
                JobStatus::UploadedOriginalPendingSubtitle,
                Some("仍无可用字幕"),
            )?;
            return Ok(());
        }
        let video = self
            .download_video(&job.id, &job.url, &meta.id, &work)
            .await?;
        self.db.set_job_paths(
            &job.id,
            Some(&video.to_string_lossy()),
            subtitle.as_ref().map(|p| p.to_string_lossy()).as_deref(),
            None,
        )?;
        if subtitle.is_none() {
            let title = self
                .translate_title(&job.id, &meta.title)
                .await
                .unwrap_or_else(|_| meta.title.clone());
            self.probe_media(&job.id, &video, "original_probe").await?;
            let bvid = self.upload(&job.id, &video, &title, &job.url, None).await?;
            self.db.set_job_bvid(&job.id, &bvid)?;
            self.db
                .update_job_status(&job.id, JobStatus::UploadedOriginalPendingSubtitle, None)?;
            self.after_upload(&job.id, &[&video])?;
            return Ok(());
        }
        let raw_sub = subtitle.unwrap();
        let mut cues = subtitle::parse_vtt(&raw_sub)?;
        cues = self.segment(&job.id, &cues).await?;
        let segmented = work.join(format!("{}.en.segmented.json", meta.id));
        subtitle::save_json(&cues, &segmented)?;
        self.translate(&job.id, &mut cues).await?;
        let translated = work.join(format!("{}.en-zh-CN.translated.json", meta.id));
        subtitle::save_json(&cues, &translated)?;
        let width = meta.width.unwrap_or(1920);
        let height = meta.height.unwrap_or(1080);
        let ass = work.join(format!("{}.bilingual.ass", meta.id));
        subtitle::write_ass(
            &cues,
            &ass,
            width,
            height,
            &self.config.render.font_cn,
            &self.config.render.font_en,
        )?;
        self.db
            .set_job_paths(&job.id, None, Some(&ass.to_string_lossy()), None)?;
        let rendered = self.render(&job.id, &video, &ass, &meta.id).await?;
        self.db
            .set_job_paths(&job.id, None, None, Some(&rendered.to_string_lossy()))?;
        let title = self
            .translate_title(&job.id, &meta.title)
            .await
            .unwrap_or_else(|_| meta.title.clone());
        self.probe_media(&job.id, &rendered, "rendered_probe")
            .await?;
        let bvid = if let Some(existing) = append_to {
            self.append(&job.id, &rendered, &existing).await?;
            existing
        } else {
            self.upload(&job.id, &rendered, &title, &job.url, None)
                .await?
        };
        self.db.set_job_bvid(&job.id, &bvid)?;
        self.db
            .update_job_status(&job.id, JobStatus::Completed, None)?;
        self.after_upload(&job.id, &[&video, &rendered])?;
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
                    .finish_stage(stage, "missing", 0, 0, Some(&e.to_string()))?;
                Ok(None)
            }
        }
    }

    async fn download_video(
        &self,
        job_id: &str,
        url: &str,
        video_id: &str,
        work: &Path,
    ) -> Result<PathBuf> {
        if let Some(p) = find_video(work, video_id) {
            return Ok(p);
        }
        self.db
            .update_job_status(job_id, JobStatus::Downloading, None)?;
        let stage = self
            .db
            .start_stage(job_id, "video_download", None, None, None)?;
        let mut cmd = Command::new(&self.config.youtube.yt_dlp);
        let square = (self.config.youtube.max_pixels as f64).sqrt().floor() as u64;
        let fps = self.config.youtube.max_fps;
        let format = format!(
            "bv*[vcodec^=avc1][fps<={fps}][width<=1920][height<=1080]+ba[acodec^=mp4a]/bv*[vcodec^=avc1][fps<={fps}][width<=1080][height<=1920]+ba[acodec^=mp4a]/bv*[vcodec^=avc1][fps<={fps}][width<={square}][height<={square}]+ba[acodec^=mp4a]/b[vcodec^=avc1][acodec^=mp4a][fps<={fps}][width<=1920][height<=1080]/b[vcodec^=avc1][acodec^=mp4a][fps<={fps}][width<=1080][height<=1920]/bv*[fps<={fps}][width<=1920][height<=1080]+ba/bv*[fps<={fps}][width<=1080][height<=1920]+ba/b[fps<={fps}][width<=1920][height<=1080]/b[fps<={fps}][width<=1080][height<=1920]"
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
        self.db
            .update_job_status(job_id, JobStatus::Segmenting, None)?;
        let stage = self.db.start_stage(
            job_id,
            "segmentation",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let mut ranges = Vec::new();
        let mut duration = 0;
        let mut peak = 0;
        for (chunk_idx, chunk) in cues.chunks(self.config.ai.batch_size * 4).enumerate() {
            let payload = json!({"task":"segment","source_lang":self.config.translation.source_lang,"tokens":chunk.iter().enumerate().map(|(i,c)|json!({"i":i,"text":c.source})).collect::<Vec<_>>()});
            let input_json = payload.to_string();
            let r = self.call_pi(payload).await?;
            let local = parse_ranges(&r.value)?;
            let offset = chunk_idx * self.config.ai.batch_size * 4;
            for (a, b) in local {
                ranges.push((a + offset, b + offset));
            }
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
            duration += r.output.duration_ms;
            peak = peak.max(r.output.peak_rss_kib);
        }
        let result = subtitle::apply_ranges(cues, &ranges)?;
        self.db.finish_stage(
            stage,
            "completed",
            duration,
            peak,
            Some(&format!("{} -> {} cues", cues.len(), result.len())),
        )?;
        Ok(result)
    }

    async fn translate(&self, job_id: &str, cues: &mut [Cue]) -> Result<()> {
        self.db
            .update_job_status(job_id, JobStatus::Translating, None)?;
        let stage = self.db.start_stage(
            job_id,
            "translation",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let mut all = Vec::new();
        let mut duration = 0;
        let mut peak = 0;
        for (chunk_idx, chunk) in cues.chunks(self.config.ai.batch_size).enumerate() {
            let payload = json!({"task":"translate","source_lang":self.config.translation.source_lang,"target_lang":self.config.translation.target_lang,"items":chunk.iter().enumerate().map(|(i,c)|json!({"i":i,"text":c.source})).collect::<Vec<_>>()});
            let input_json = payload.to_string();
            let r = self.call_pi(payload).await?;
            for (i, t) in parse_translations(&r.value)? {
                all.push((i + chunk_idx * self.config.ai.batch_size, t));
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
            Some(&format!("{} cues", cues.len())),
        )?;
        Ok(())
    }

    async fn translate_title(&self, job_id: &str, title: &str) -> Result<String> {
        let stage = self.db.start_stage(
            job_id,
            "title_translation",
            Some(&self.config.ai.provider),
            Some(&self.config.ai.model),
            Some(&self.config.ai.thinking),
        )?;
        let payload = json!({"task":"title","text":title});
        let input_json = payload.to_string();
        let r = self.call_pi(payload).await?;
        let translated = r
            .value
            .get("title")
            .and_then(Value::as_str)
            .context("Pi 标题结果缺少 title")?
            .trim()
            .to_string();
        self.db.record_ai_call(
            job_id,
            stage,
            "title",
            &self.config.ai.provider,
            &self.config.ai.model,
            &self.config.ai.thinking,
            &r.usage,
            r.output.duration_ms,
            &input_json,
            &r.value.to_string(),
        )?;
        self.db.finish_stage(
            stage,
            "completed",
            r.output.duration_ms,
            r.output.peak_rss_kib,
            None,
        )?;
        Ok(translated)
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
        let filter = format!(
            "ass=filename='{}':fontsdir='{}'",
            escape_filter(ass),
            escape_filter(&self.config.render.fonts_dir)
        );
        let mut cmd = Command::new(&self.config.render.ffmpeg);
        cmd.arg("-y")
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
            .arg(&output);
        let out = run_monitored(cmd, Duration::from_secs(14400)).await?;
        self.db.finish_stage(
            stage,
            "completed",
            out.duration_ms,
            out.peak_rss_kib,
            Some(&output.to_string_lossy()),
        )?;
        Ok(output)
    }

    async fn upload(
        &self,
        job_id: &str,
        video: &Path,
        title: &str,
        source: &str,
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
            .args([
                "--title",
                title,
                "--desc",
                &format!("来源：{source}\n由 y2b-rs 自动翻译压制"),
                "--tag",
                &self.config.bilibili.default_tags.join(","),
                "--tid",
                &self.config.bilibili.default_tid.to_string(),
                "--copyright",
                "2",
                "--source",
                source,
                "--limit",
                "1",
            ]);
        if let Some(c) = cover {
            cmd.arg("--cover").arg(c);
        }
        let out = run_monitored(cmd, Duration::from_secs(14400)).await?;
        let merged = out.stdout.clone() + "\n" + &out.stderr;
        let bvid = Regex::new(r"\bBV[0-9A-Za-z]+\b")?
            .find(&merged)
            .context("biliup 未返回 BV 号")?
            .as_str()
            .to_string();
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
            .arg("append")
            .args(["--vid", bvid, "--limit", "1"])
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
    #[test]
    fn pi_json() {
        let s = r#"{"type":"agent_end","messages":[{"role":"assistant","content":[{"type":"text","text":"{\"ranges\":[{\"start\":0,\"end\":1}]}"}],"usage":{"input":2,"output":3,"totalTokens":5}}]}"#;
        let (v, u) = parse_pi_stream(s).unwrap();
        assert_eq!(parse_ranges(&v).unwrap(), vec![(0, 1)]);
        assert_eq!(u.total, 5);
    }
}
