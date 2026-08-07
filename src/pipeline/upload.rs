//! biliup 投稿与全局投稿窗口（限流冷却）。
use super::cc::CC_INITIAL_DELAY_SECONDS;
use super::publication::build_upload_args;
use super::{Pipeline, StageGuard};
use crate::model::{
    Job, JobStatus, PreparedUpload, PublicationMetadata, TransferMode, VideoMetadata,
};
use crate::process::run_monitored;
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::sleep;

pub(super) const NEXT_BILIBILI_SUBMIT_AT: &str = "bilibili.next_submit_at";

pub(super) fn ensure_prepared_file(path: &Path, label: &str) -> Result<()> {
    if !path.metadata().is_ok_and(|metadata| metadata.len() > 0) {
        bail!("待上传{label}不存在或为空: {}", path.display())
    }
    Ok(())
}

impl Pipeline {
    pub(super) async fn upload_prepared_job_inner(&self, job: &Job) -> Result<()> {
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
        let (bvid, completion_status, mode) = match prepared {
            PreparedUpload::Submission {
                video_path,
                cover_path,
                mode,
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
                (bvid, completion_status, mode)
            }
        };
        self.db
            .finish_prepared_upload(&job.id, &bvid, completion_status)?;
        // CC 字幕补交交给独立队列，不占用上传 worker。
        //
        // 此前这里同步等待最多 90s + 8×60s ≈ 9.5 分钟，期间下一个 ready_to_upload
        // 任务无法被领取。只有 translated 任务入队：direct 任务不下载字幕，
        // 而 completion_status 已是待补字幕的（原视频无字幕直传）也没有素材可用。
        if completion_status == JobStatus::Completed && mode == TransferMode::Translated {
            self.db
                .queue_pending_subtitle(&job.id, CC_INITIAL_DELAY_SECONDS)?;
            self.db
                .event(Some(&job.id), "info", "已投稿，等待自动补交中文 CC 字幕")?;
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

    pub(super) async fn upload(
        &self,
        job_id: &str,
        video: &Path,
        publication: &PublicationMetadata,
        meta: &VideoMetadata,
        cover: Option<&Path>,
    ) -> Result<String> {
        let mut stage = StageGuard::start(&self.db, job_id, "upload", None, None, None)?;
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
                let elapsed = stage.elapsed_ms();
                return Err(stage.fail(error, elapsed, 0));
            }
        };
        let merged = out.stdout.clone() + "\n" + &out.stderr;
        let bvid = match Regex::new(r"\bBV[0-9A-Za-z]+\b")?.find(&merged) {
            Some(value) => value.as_str().to_string(),
            None => {
                let error = anyhow::anyhow!("biliup 未返回 BV 号");
                return Err(stage.fail(error, out.duration_ms, out.peak_rss_kib));
            }
        };
        stage.finish("completed", out.duration_ms, out.peak_rss_kib, Some(&bvid))?;
        self.defer_bilibili_submissions(self.config.bilibili.submit_interval_seconds)?;
        Ok(bvid)
    }

    pub(super) async fn wait_for_bilibili_submission(&self, job_id: &str) -> Result<bool> {
        self.wait_for_bilibili_submission_with_poll(job_id, Duration::from_secs(2))
            .await
    }

    pub(super) async fn wait_for_bilibili_submission_with_poll(
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

    pub(super) fn defer_bilibili_submissions(&self, seconds: u64) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::db::{Database, NewJob};
    use crate::model::JobStatus;

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
