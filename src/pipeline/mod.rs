//! 流水线编排：任务准备、原片/封面下载和投稿收尾。
//!
//! 具体环节分散在子模块中：`ai` 负责 Pi 调用与 token 估算，`subtitle_flow`
//! 负责字幕获取/分句/翻译，`publication` 负责投稿元数据，`upload` 负责 biliup
//! 与投稿窗口，`cc` 负责中文 CC 字幕补交。
use crate::config::Config;
use crate::db::Database;
use crate::model::{Job, JobStatus, PreparedUpload, TransferMode, VideoMetadata};
use crate::monitor::{Monitor, is_live_content_pending, ytdlp_command};
use crate::process::run_monitored;
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::process::Command;

// 子模块保持私有：拆分是内部结构调整，crate 对外暴露的仍然只有 `Pipeline`
// 和少数几个常量，与拆分前一致。
mod ai;
mod cc;
mod publication;
mod subtitle_flow;
mod upload;

pub use cc::{CC_INITIAL_DELAY_SECONDS, CC_MAX_ATTEMPTS};

#[cfg(test)]
mod testing;

pub struct Pipeline {
    pub config: Config,
    pub db: Database,
}

/// `stage_runs` 行的 RAII 句柄。
///
/// 阶段收尾此前靠手写配对 `start_stage`/`finish_stage`，任何 `?` 或 `bail!`
/// 提前返回都会留下一行永久停在 `running` 的记录，污染 `y2b jobs show` 的审计
/// 输出。这里改为：显式 `finish` 负责成功/自定义状态，未显式收尾时由 `Drop`
/// 兜底写入 `failed`。
struct StageGuard {
    db: Database,
    id: i64,
    started: Instant,
    finished: bool,
}

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

async fn try_join_branches<A, B, FA, FB>(left: FA, right: FB) -> Result<(A, B)>
where
    FA: Future<Output = Result<A>>,
    FB: Future<Output = Result<B>>,
{
    tokio::try_join!(left, right)
}

fn requires_translated_pipeline(mode: TransferMode) -> bool {
    mode == TransferMode::Translated
}

impl StageGuard {
    fn start(
        db: &Database,
        job_id: &str,
        stage: &str,
        provider: Option<&str>,
        model: Option<&str>,
        thinking: Option<&str>,
    ) -> Result<Self> {
        let id = db.start_stage(job_id, stage, provider, model, thinking)?;
        Ok(Self {
            db: db.clone(),
            id,
            started: Instant::now(),
            finished: false,
        })
    }

    fn id(&self) -> i64 {
        self.id
    }

    /// 从阶段开始至今的挂钟耗时，供没有子进程耗时可用的失败路径使用。
    fn elapsed_ms(&self) -> i64 {
        self.started.elapsed().as_millis() as i64
    }

    fn finish(
        &mut self,
        status: &str,
        duration_ms: i64,
        peak_rss_kib: u64,
        detail: Option<&str>,
    ) -> Result<()> {
        self.finished = true;
        self.db
            .finish_stage(self.id, status, duration_ms, peak_rss_kib, detail)
    }

    /// 以失败收尾并原样返回错误，供 `stage.fail(error)?` 这种单行写法使用。
    fn fail(&mut self, error: anyhow::Error, duration_ms: i64, peak_rss_kib: u64) -> anyhow::Error {
        if let Err(nested) = self.finish(
            "failed",
            duration_ms,
            peak_rss_kib,
            Some(&error.to_string()),
        ) {
            tracing::warn!(stage_id = self.id, error = %nested, "写入阶段失败状态出错");
        }
        error
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let elapsed = self.elapsed_ms();
        if let Err(error) = self.db.finish_stage(
            self.id,
            "failed",
            elapsed,
            0,
            Some("阶段提前返回，未正常收尾"),
        ) {
            tracing::warn!(stage_id = self.id, error = %error, "兜底关闭阶段行失败");
        }
    }
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
        let mut stage = StageGuard::start(&self.db, &job.id, "metadata", None, None, None)?;
        let monitor = Monitor::new(self.config.clone(), self.db.clone())?;
        let (meta, peak, duration) = match monitor.fetch_metadata(&job.url).await {
            Ok(result) => result,
            Err(error) => {
                // 直播/预约内容不是故障，单独标记为 deferred 便于审计区分。
                let status = if is_live_content_pending(&error) {
                    "deferred"
                } else {
                    "failed"
                };
                let elapsed = stage.elapsed_ms();
                stage.finish(status, elapsed, 0, Some(&error.to_string()))?;
                return Err(error);
            }
        };
        stage.finish("completed", duration, peak, None)?;
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
        let mut stage = StageGuard::start(&self.db, job_id, "cover_download", None, None, None)?;
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
        let duration_ms = stage.elapsed_ms();
        match result {
            Ok(peak_rss_kib) => {
                stage.finish("completed", duration_ms, peak_rss_kib, Some(thumbnail_url))?;
                Ok(cover)
            }
            Err(error) => Err(stage.fail(error, duration_ms, 0)).context("下载 YouTube 封面失败"),
        }
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
        let mut stage = StageGuard::start(&self.db, job_id, "video_download", None, None, None)?;
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
        stage.finish(
            "completed",
            out.duration_ms,
            out.peak_rss_kib,
            Some(&path.to_string_lossy()),
        )?;
        Ok(path)
    }

    async fn probe_media(&self, job_id: &str, path: &Path, label: &str) -> Result<()> {
        let mut stage = StageGuard::start(&self.db, job_id, label, None, None, None)?;
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
        stage.finish(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::NewJob;
    use crate::pipeline::testing::metadata;
    use std::sync::Arc;
    use tokio::sync::Barrier;

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
    fn stage_guard_closes_the_row_on_early_return() {
        let temp = tempfile::tempdir().unwrap();
        let db = Database::open(&temp.path().join("state.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "stage-guard",
                url: "https://youtu.be/stage-guard",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();

        // 提前返回（`?`/`bail!`）不再留下永久 running 的阶段行。
        (|| -> Result<()> {
            let _stage = StageGuard::start(&db, &id, "segmentation", None, None, None)?;
            bail!("安全 token 阈值过小")
        })()
        .unwrap_err();
        let stages = db.list_stages(&id).unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].status, "failed");
        assert_eq!(
            stages[0].detail.as_deref(),
            Some("阶段提前返回，未正常收尾")
        );

        // 显式收尾后 Drop 不再覆写状态。
        {
            let mut stage = StageGuard::start(&db, &id, "translation", None, None, None).unwrap();
            stage.finish("completed", 12, 34, Some("ok")).unwrap();
        }
        let stages = db.list_stages(&id).unwrap();
        assert_eq!(stages[1].status, "completed");
        assert_eq!(stages[1].duration_ms, Some(12));
        assert_eq!(stages[1].peak_rss_kib, Some(34));

        // fail() 把真实错误写进 detail 并原样返回错误。
        {
            let mut stage = StageGuard::start(&db, &id, "upload", None, None, None).unwrap();
            let returned = stage.fail(anyhow::anyhow!("biliup 未返回 BV 号"), 7, 8);
            assert_eq!(returned.to_string(), "biliup 未返回 BV 号");
        }
        let stages = db.list_stages(&id).unwrap();
        assert_eq!(stages[2].status, "failed");
        assert_eq!(stages[2].detail.as_deref(), Some("biliup 未返回 BV 号"));
        assert_eq!(stages[2].duration_ms, Some(7));
    }

    #[test]
    fn transfer_mode_routes_only_requested_jobs_through_subtitles() {
        assert!(!requires_translated_pipeline(TransferMode::Direct));
        assert!(requires_translated_pipeline(TransferMode::Translated));
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
