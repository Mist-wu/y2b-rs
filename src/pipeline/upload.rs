//! biliup 投稿与全局投稿窗口（限流冷却）。
use super::cc::CC_INITIAL_DELAY_SECONDS;
use super::publication::build_upload_args;
use super::{Pipeline, StageGuard};
use crate::model::{Job, JobStatus, PreparedUpload, PublicationMetadata, VideoMetadata};
use crate::process::{ProcessFailure, run_monitored};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::sleep;

pub use crate::db::NEXT_BILIBILI_SUBMIT_AT;

#[derive(Debug, Clone, PartialEq, Eq)]
enum BiliupSubmissionResponse {
    Accepted(String),
    AcceptedWithoutBvid,
    Rejected(i64),
}

#[derive(Debug, Error)]
#[error("{detail}")]
struct BiliupRejectedError {
    detail: String,
}

#[derive(Debug, Error)]
#[error("{detail}")]
pub(super) struct UploadUncertainError {
    detail: String,
}

pub(super) fn is_upload_uncertain(error: &anyhow::Error) -> bool {
    error.downcast_ref::<UploadUncertainError>().is_some()
}

fn valid_bvid(value: &str) -> bool {
    Regex::new(r"^BV[0-9A-Za-z]{10}$")
        .expect("固定 BVID 正则必须有效")
        .is_match(value)
}

fn response_from_json_line(line: &str) -> Option<BiliupSubmissionResponse> {
    for (start, character) in line
        .char_indices()
        .filter(|(_, character)| *character == '{')
    {
        debug_assert_eq!(character, '{');
        let parsed = serde_json::Deserializer::from_str(&line[start..])
            .into_iter::<Value>()
            .next()
            .and_then(std::result::Result::ok);
        let Some(value) = parsed else {
            continue;
        };
        let Some(code) = value.get("code").and_then(Value::as_i64) else {
            continue;
        };
        if code != 0 {
            return Some(BiliupSubmissionResponse::Rejected(code));
        }
        return Some(
            value
                .pointer("/data/bvid")
                .and_then(Value::as_str)
                .filter(|bvid| valid_bvid(bvid))
                .map(|bvid| BiliupSubmissionResponse::Accepted(bvid.to_string()))
                .unwrap_or(BiliupSubmissionResponse::AcceptedWithoutBvid),
        );
    }
    None
}

fn response_from_debug_line(line: &str) -> Option<BiliupSubmissionResponse> {
    let response =
        Regex::new(r#"ResponseData\s*\{\s*code:\s*(-?\d+),\s*data:\s*(.*),\s*message:\s*"#)
            .expect("固定 ResponseData 正则必须有效");
    let captures = response.captures(line)?;
    let code = captures.get(1)?.as_str().parse::<i64>().ok()?;
    if code != 0 {
        return Some(BiliupSubmissionResponse::Rejected(code));
    }
    let bvid = Regex::new(r#""bvid":\s*String\("(BV[0-9A-Za-z]{10})"\)"#)
        .expect("固定 biliup Debug BVID 正则必须有效")
        .captures(captures.get(2)?.as_str())
        .and_then(|captures| captures.get(1))
        .map(|value| value.as_str().to_string());
    Some(
        bvid.map(BiliupSubmissionResponse::Accepted)
            .unwrap_or(BiliupSubmissionResponse::AcceptedWithoutBvid),
    )
}

fn parse_biliup_submission(output: &str) -> Option<BiliupSubmissionResponse> {
    output
        .lines()
        .filter_map(|line| response_from_json_line(line).or_else(|| response_from_debug_line(line)))
        .next_back()
}

fn parse_creator_archives(output: &str) -> Vec<(String, String)> {
    output
        .lines()
        .filter_map(|line| {
            let mut columns = line.splitn(3, '\t');
            let bvid = columns.next()?.trim();
            let title = columns.next()?.trim();
            valid_bvid(bvid).then(|| (bvid.to_string(), title.to_string()))
        })
        .collect()
}

pub(super) fn ensure_prepared_file(path: &Path, label: &str) -> Result<()> {
    if !path.metadata().is_ok_and(|metadata| metadata.len() > 0) {
        bail!("待上传{label}不存在或为空: {}", path.display())
    }
    Ok(())
}

impl Pipeline {
    /// 对 `upload_uncertain` 任务读取创作中心最近稿件，按持久化投稿标题唯一匹配。
    /// 找不到或同名多条时保持不确定态，绝不自动重投。
    pub async fn reconcile_uncertain_upload(&self, job_id: &str) -> Result<String> {
        let job = self
            .db
            .get_job(job_id)?
            .with_context(|| format!("任务不存在: {job_id}"))?;
        if job.status != JobStatus::UploadUncertain {
            bail!(
                "任务 {job_id} 当前状态不是 upload_uncertain: {}",
                job.status
            )
        }
        let prepared = self
            .db
            .prepared_upload(job_id)?
            .with_context(|| format!("不确定投稿任务 {job_id} 缺少上传计划"))?;
        let (mode, completion_status) = match prepared {
            PreparedUpload::Submission {
                mode,
                completion_status,
                ..
            } => (mode, completion_status),
        };
        let publication = self
            .db
            .publication_metadata(job_id)?
            .with_context(|| format!("不确定投稿任务 {job_id} 缺少投稿元数据"))?;
        let mut command = Command::new(&self.config.bilibili.biliup);
        command
            .arg("-u")
            .arg(&self.config.bilibili.cookies)
            .arg("list")
            .arg("--from-page")
            .arg("1")
            .arg("--max-pages")
            .arg("5");
        let output = run_monitored(command, Duration::from_secs(120)).await?;
        let mut matches = parse_creator_archives(&output.stdout)
            .into_iter()
            .filter(|(_, title)| title == &publication.title)
            .map(|(bvid, _)| bvid)
            .collect::<Vec<_>>();
        matches.sort_unstable();
        matches.dedup();
        let bvid = match matches.as_slice() {
            [bvid] => bvid.clone(),
            [] => bail!(
                "创作中心最近稿件中没有标题完全匹配的记录，任务保持 upload_uncertain: {}",
                publication.title
            ),
            _ => bail!(
                "创作中心存在 {} 条同名稿件，无法唯一确认，任务保持 upload_uncertain: {}",
                matches.len(),
                publication.title
            ),
        };
        let subtitle_queued = self.db.confirm_uncertain_upload(
            job_id,
            &bvid,
            completion_status,
            mode,
            CC_INITIAL_DELAY_SECONDS,
        )?;
        let _ = self.defer_bilibili_submissions(self.config.bilibili.submit_interval_seconds);
        self.db.event(
            Some(job_id),
            "info",
            &format!(
                "创作中心核对确认投稿成功: {bvid}{}",
                if subtitle_queued {
                    "；已原子排队 CC 字幕"
                } else {
                    ""
                }
            ),
        )?;
        Ok(format!("已从创作中心确认 {job_id} -> {bvid}"))
    }

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
        let subtitle_queued = match prepared {
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
                if !self.wait_for_bilibili_submission(&job.id).await? {
                    return Ok(());
                }
                let Some(attempt_id) = self.db.begin_prepared_upload(&job.id)? else {
                    return Ok(());
                };
                let execution = self
                    .run_with_claim_heartbeat(
                        &job.id,
                        crate::db::UPLOAD_CLAIM_KIND,
                        self.upload(&job.id, &video, &publication, &meta, Some(&cover)),
                    )
                    .await;
                let bvid = match execution {
                    Ok(bvid) => bvid,
                    Err(error) if error.downcast_ref::<BiliupRejectedError>().is_some() => {
                        self.db
                            .fail_upload_attempt(&job.id, &attempt_id, &error.to_string())?;
                        return Err(error);
                    }
                    Err(error) => {
                        let detail = format!(
                            "投稿结果不确定（attempt={attempt_id}），请用 jobs reconcile-upload 核对创作中心后再处理: {error:#}"
                        );
                        let marked =
                            self.db
                                .mark_upload_attempt_uncertain(&job.id, &attempt_id, &detail);
                        let detail = match marked {
                            Ok(()) => detail,
                            Err(mark_error) => {
                                format!("{detail}；写入不确定状态失败: {mark_error:#}")
                            }
                        };
                        return Err(UploadUncertainError { detail }.into());
                    }
                };
                match self.db.finish_upload_attempt(
                    &job.id,
                    &attempt_id,
                    &bvid,
                    completion_status,
                    mode,
                    CC_INITIAL_DELAY_SECONDS,
                ) {
                    Ok(subtitle_queued) => subtitle_queued,
                    Err(error) => {
                        let detail = format!(
                            "Bilibili 已返回成功 {bvid}，但本地确认失败（attempt={attempt_id}）: {error:#}"
                        );
                        let _ =
                            self.db
                                .mark_upload_attempt_uncertain(&job.id, &attempt_id, &detail);
                        return Err(UploadUncertainError { detail }.into());
                    }
                }
            }
        };
        // 投稿 attempt 与任务终态已经由 finish_upload_attempt 同事务持久化。
        if let Err(error) =
            self.defer_bilibili_submissions(self.config.bilibili.submit_interval_seconds)
        {
            tracing::warn!(job_id = %job.id, error = %error, "投稿成功后写入冷却时间失败");
            let _ = self.db.event(
                Some(&job.id),
                "warn",
                &format!("投稿成功后写入冷却时间失败: {error}"),
            );
        }
        // CC 字幕状态和首次到期时间已与投稿完成同事务写入，不占用上传 worker。
        if subtitle_queued {
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
        match run_monitored(cmd, Duration::from_secs(14400)).await {
            Ok(output) => {
                let merged = output.stdout.clone() + "\n" + &output.stderr;
                match parse_biliup_submission(&merged) {
                    Some(BiliupSubmissionResponse::Accepted(bvid)) => {
                        stage.finish(
                            "completed",
                            output.duration_ms,
                            output.peak_rss_kib,
                            Some(&bvid),
                        )?;
                        Ok(bvid)
                    }
                    Some(BiliupSubmissionResponse::Rejected(code)) => {
                        let error = BiliupRejectedError {
                            detail: format!("biliup 投稿被平台拒绝: code={code}"),
                        };
                        Err(stage.fail(error.into(), output.duration_ms, output.peak_rss_kib))
                    }
                    Some(BiliupSubmissionResponse::AcceptedWithoutBvid) => {
                        let error = UploadUncertainError {
                            detail: "biliup 返回成功响应，但响应中没有合法 BVID".into(),
                        };
                        Err(stage.fail(error.into(), output.duration_ms, output.peak_rss_kib))
                    }
                    None => {
                        let error = UploadUncertainError {
                            detail: "biliup 退出成功，但没有可验证的结构化投稿响应".into(),
                        };
                        Err(stage.fail(error.into(), output.duration_ms, output.peak_rss_kib))
                    }
                }
            }
            Err(error) => {
                if let Some(failure) = error.downcast_ref::<ProcessFailure>() {
                    let output = failure.output();
                    let merged = output.stdout.clone() + "\n" + &output.stderr;
                    match parse_biliup_submission(&merged) {
                        Some(BiliupSubmissionResponse::Accepted(bvid)) => {
                            stage.finish(
                                "completed",
                                output.duration_ms,
                                output.peak_rss_kib,
                                Some(&bvid),
                            )?;
                            return Ok(bvid);
                        }
                        Some(BiliupSubmissionResponse::Rejected(code)) => {
                            let rejected = BiliupRejectedError {
                                detail: format!("biliup 投稿被平台拒绝: code={code}: {error}"),
                            };
                            return Err(stage.fail(
                                rejected.into(),
                                output.duration_ms,
                                output.peak_rss_kib,
                            ));
                        }
                        Some(BiliupSubmissionResponse::AcceptedWithoutBvid) | None => {}
                    }
                }
                let elapsed = stage.elapsed_ms();
                if error.to_string().contains("启动子进程失败") {
                    let rejected = BiliupRejectedError {
                        detail: format!("biliup 未启动，未产生投稿: {error:#}"),
                    };
                    return Err(stage.fail(rejected.into(), elapsed, 0));
                }
                let uncertain = UploadUncertainError {
                    detail: format!("biliup 执行中断，无法确认平台是否已接受投稿: {error:#}"),
                };
                Err(stage.fail(uncertain.into(), elapsed, 0))
            }
        }
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
    use crate::model::{JobStatus, TransferMode};

    #[test]
    fn biliup_bvid_requires_a_structured_success_response() {
        assert_eq!(
            parse_biliup_submission(
                r#"2026-08-30 INFO ResponseData { code: 0, data: Some(Object {"aid": Number(1), "bvid": String("BV1uxE16ZE7e")}), message: "0", ttl: Some(1) }"#
            ),
            Some(BiliupSubmissionResponse::Accepted("BV1uxE16ZE7e".into()))
        );
        assert_eq!(
            parse_biliup_submission(r#"{"code":0,"data":{"aid":1,"bvid":"BV17x411w7KC"}}"#),
            Some(BiliupSubmissionResponse::Accepted("BV17x411w7KC".into()))
        );
        assert_eq!(
            parse_biliup_submission(
                r#"2026-08-30 INFO ResponseData { code: 21566, data: None, message: "rate limited", ttl: Some(1) }"#
            ),
            Some(BiliupSubmissionResponse::Rejected(21566))
        );

        // 标题、参数或普通日志里的 BV 号绝不能被当成投稿结果。
        assert_eq!(
            parse_biliup_submission("upload title: 回顾 BV1uxE16ZE7e\nWeb 接口投稿成功"),
            None
        );
        assert_eq!(
            parse_biliup_submission(r#"{"code":0,"data":{"bvid":"BV-short"}}"#),
            Some(BiliupSubmissionResponse::AcceptedWithoutBvid)
        );
    }

    #[test]
    fn creator_archive_parser_accepts_only_tsv_rows_with_exact_bvids() {
        let rows = parse_creator_archives(
            "2026-08-30 INFO login ok\nBV1uxE16ZE7e\t标题 A\t\u{1b}[1;92m已通过\u{1b}[0m\nnot-a-bvid\t标题 B\t失败\n",
        );
        assert_eq!(rows, vec![("BV1uxE16ZE7e".into(), "标题 A".into())]);
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
        db.pause_job(&id).unwrap();

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
