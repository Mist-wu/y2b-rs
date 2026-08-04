use crate::{
    config::{Config, YoutubeConfig},
    db::{Database, NewJob},
    model::{Job, TransferMode, VideoMetadata},
    process::run_monitored,
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use feed_rs::parser;
use serde_json::Value;
use std::time::Duration;
use tokio::process::Command;

pub struct Monitor {
    config: Config,
    db: Database,
    client: reqwest::Client,
}

/// 构造带公共参数（js 运行时、cookies）的 yt-dlp 命令，供各子命令复用。
pub(crate) fn ytdlp_command(config: &YoutubeConfig) -> Command {
    let mut cmd = Command::new(&config.yt_dlp);
    cmd.args(["--js-runtimes", "node"]);
    if config.cookies.exists() {
        cmd.arg("--cookies").arg(&config.cookies);
    }
    cmd
}

#[derive(Debug, Clone)]
pub struct ResolvedChannel {
    pub channel_id: String,
    pub name: String,
    pub url: String,
    pub feed_url: String,
}

#[derive(Debug, Clone)]
pub struct EnqueueOutcome {
    pub job: Job,
    pub created: bool,
}

const LIVE_CONTENT_PENDING_PREFIX: &str = "直播或预约内容暂不处理";

pub fn is_live_content_pending(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().starts_with(LIVE_CONTENT_PENDING_PREFIX))
}

fn validate_single_video(v: &Value) -> Result<()> {
    if v.get("_type").and_then(Value::as_str) == Some("playlist")
        || v.get("entries").and_then(Value::as_array).is_some()
    {
        bail!("请输入单个 YouTube 视频 URL，不支持播放列表")
    }
    let live = v.get("live_status").and_then(Value::as_str);
    if matches!(live, Some("is_live" | "is_upcoming" | "post_live")) {
        bail!("{LIVE_CONTENT_PENDING_PREFIX}: {live:?}")
    }
    Ok(())
}

fn extract_thumbnail_url(v: &Value) -> Option<String> {
    let thumbnails = v.get("thumbnails").and_then(Value::as_array);
    // 优先选带尺寸的项里面积最大者（yt-dlp 通常把最高清缩略图列在首位）。
    let best_sized = thumbnails.and_then(|items| {
        items
            .iter()
            .filter_map(|item| {
                let url = item.get("url")?.as_str()?;
                let width = item.get("width")?.as_u64()?;
                let height = item.get("height")?.as_u64()?;
                Some((width.saturating_mul(height), url))
            })
            .max_by_key(|(area, _)| *area)
            .map(|(_, url)| url.to_string())
    });
    if let Some(url) = best_sized {
        return Some(url);
    }
    // 退而求其次：YouTube 选中图，或缩略图列表末尾项。
    v.get("thumbnail")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            thumbnails?
                .iter()
                .rev()
                .find_map(|item| item.get("url")?.as_str().map(str::to_string))
        })
}

fn reconcile_after_baseline(
    baseline: Option<DateTime<Utc>>,
    timestamp: Option<i64>,
) -> Option<bool> {
    let Some(baseline) = baseline else {
        return Some(true);
    };
    let published = timestamp.and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))?;
    Some(published > baseline)
}

impl Monitor {
    pub fn new(config: Config, db: Database) -> Result<Self> {
        Ok(Self {
            config,
            db,
            client: reqwest::Client::builder()
                .user_agent("y2b-rs/0.1")
                .build()?,
        })
    }

    pub async fn resolve_channel(&self, url: &str) -> Result<ResolvedChannel> {
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--flat-playlist",
            "--playlist-items",
            "1",
            "--dump-single-json",
            "--skip-download",
            url,
        ]);
        let out = run_monitored(cmd, Duration::from_secs(90)).await?;
        let v: Value = serde_json::from_str(out.stdout.trim()).context("yt-dlp 频道 JSON 无效")?;
        let channel_id = v
            .get("channel_id")
            .or_else(|| v.get("uploader_id"))
            .and_then(Value::as_str)
            .context("无法解析 channel_id")?
            .to_string();
        let name = v
            .get("channel")
            .or_else(|| v.get("uploader"))
            .and_then(Value::as_str)
            .unwrap_or(&channel_id)
            .to_string();
        Ok(ResolvedChannel {
            feed_url: format!("https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"),
            channel_id,
            name,
            url: url.to_string(),
        })
    }

    pub async fn add_channel(&self, url: &str, transfer_mode: TransferMode) -> Result<i64> {
        let r = self.resolve_channel(url).await?;
        let id = self
            .db
            .add_channel(&r.channel_id, &r.name, &r.url, &r.feed_url, transfer_mode)?;
        self.poll_channel(id, false).await?;
        Ok(id)
    }

    pub async fn enqueue_video(
        &self,
        url: &str,
        transfer_mode: TransferMode,
    ) -> Result<EnqueueOutcome> {
        let (meta, _, _) = self.fetch_metadata(url).await?;
        if meta.id.trim().is_empty() {
            bail!("无法解析 YouTube video ID")
        }
        if let Some(job) = self.db.get_job_by_video_id(&meta.id)? {
            return Ok(EnqueueOutcome {
                job,
                created: false,
            });
        }
        let id = self
            .db
            .create_job(NewJob {
                channel_id: None,
                video_id: &meta.id,
                url,
                title: Some(&meta.title),
                published: None,
                updated: None,
                transfer_mode,
            })?
            .context("视频已在并发请求中入队")?;
        let job = self.db.get_job(&id)?.context("已创建任务但无法重新读取")?;
        Ok(EnqueueOutcome { job, created: true })
    }

    pub async fn poll_all(&self) -> Result<usize> {
        let mut count = 0;
        for c in self.db.list_channels()?.into_iter().filter(|x| x.enabled) {
            match self.poll_channel(c.id, true).await {
                Ok(n) => count += n,
                Err(e) => {
                    self.db.mark_channel_checked(c.id, Some(&e.to_string()))?;
                    tracing::warn!(channel=%c.name,error=%e,"频道轮询失败");
                }
            }
        }
        Ok(count)
    }

    pub async fn reconcile_all(&self) -> Result<usize> {
        let mut count = 0;
        for c in self.db.list_channels()?.into_iter().filter(|x| x.enabled) {
            match self.reconcile_channel(c.id, &c.url).await {
                Ok(n) => {
                    count += n;
                    self.db.mark_channel_reconciled(c.id, None)?
                }
                Err(e) => {
                    self.db
                        .mark_channel_reconciled(c.id, Some(&e.to_string()))?;
                    tracing::warn!(channel=%c.name,error=%e,"频道校对失败");
                }
            }
        }
        Ok(count)
    }

    async fn reconcile_channel(&self, id: i64, url: &str) -> Result<usize> {
        let transfer_mode = self.db.channel_transfer_mode(id)?;
        let baseline = self.db.channel_baseline(id)?;
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--flat-playlist",
            "--playlist-end",
            &self.config.monitor.reconcile_limit.to_string(),
            "--dump-single-json",
            "--skip-download",
            url,
        ]);
        let out = run_monitored(cmd, Duration::from_secs(180)).await?;
        let v: Value = serde_json::from_str(out.stdout.trim()).context("yt-dlp 校对 JSON 无效")?;
        let mut added = 0;
        for e in v
            .get("entries")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(video_id) = e.get("id").and_then(Value::as_str) else {
                continue;
            };
            let link = e
                .get("url")
                .or_else(|| e.get("webpage_url"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("https://www.youtube.com/watch?v={video_id}"));
            let title = e.get("title").and_then(Value::as_str);
            if self.db.get_job_by_video_id(video_id)?.is_some() {
                continue;
            }
            let metadata = match self.fetch_metadata(&link).await {
                Ok((metadata, _, _)) => metadata,
                Err(error) => {
                    tracing::warn!(video_id, error = %error, "校对候选元数据获取失败，跳过");
                    continue;
                }
            };
            match reconcile_after_baseline(baseline, metadata.timestamp) {
                Some(true) => {}
                Some(false) => break,
                None => {
                    tracing::warn!(video_id, "校对候选缺少发布时间，跳过以避免补录历史视频");
                    continue;
                }
            }
            let published = metadata
                .timestamp
                .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0));
            if self
                .db
                .create_job(NewJob {
                    channel_id: Some(id),
                    video_id,
                    url: &link,
                    title,
                    published,
                    updated: None,
                    transfer_mode,
                })?
                .is_some()
            {
                added += 1;
            }
        }
        Ok(added)
    }

    pub async fn poll_channel(&self, id: i64, enqueue: bool) -> Result<usize> {
        let url = self.db.channel_feed(id)?;
        let baseline = self.db.channel_baseline(id)?;
        let transfer_mode = self.db.channel_transfer_mode(id)?;
        let bytes = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let feed = parser::parse(bytes.as_ref()).context("YouTube RSS 格式无效")?;
        let mut added = 0;
        for e in feed.entries {
            let published = e.published.or(e.updated);
            if enqueue && baseline.is_some() && published <= baseline {
                continue;
            }
            let video_id = e.id.strip_prefix("yt:video:").unwrap_or(&e.id).to_string();
            if video_id.len() < 6 {
                continue;
            }
            let link = e
                .links
                .iter()
                .find(|l| l.rel.as_deref() == Some("alternate"))
                .or_else(|| e.links.first())
                .map(|l| l.href.clone())
                .unwrap_or_else(|| format!("https://www.youtube.com/watch?v={video_id}"));
            let title = e.title.as_ref().map(|x| x.content.as_str());
            if enqueue
                && self
                    .db
                    .create_job(NewJob {
                        channel_id: Some(id),
                        video_id: &video_id,
                        url: &link,
                        title,
                        published,
                        updated: e.updated,
                        transfer_mode,
                    })?
                    .is_some()
            {
                added += 1;
            }
        }
        self.db.mark_channel_checked(id, None)?;
        Ok(added)
    }

    pub async fn fetch_metadata(&self, url: &str) -> Result<(VideoMetadata, u64, i64)> {
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--dump-single-json",
            "--skip-download",
            "--no-playlist",
            url,
        ]);
        let out = run_monitored(cmd, Duration::from_secs(120)).await?;
        let v: Value = serde_json::from_str(out.stdout.trim())?;
        validate_single_video(&v)?;
        let live = v
            .get("live_status")
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok((
            VideoMetadata {
                id: v["id"].as_str().unwrap_or_default().into(),
                url: url.into(),
                title: v["title"].as_str().unwrap_or("Untitled").into(),
                description: v
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                uploader: v
                    .get("uploader")
                    .or_else(|| v.get("channel"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                upload_date: v
                    .get("upload_date")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                channel: v.get("channel").and_then(Value::as_str).map(str::to_string),
                channel_id: v
                    .get("channel_id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                timestamp: v.get("timestamp").and_then(Value::as_i64),
                duration: v.get("duration").and_then(Value::as_f64),
                width: v.get("width").and_then(Value::as_i64),
                height: v.get("height").and_then(Value::as_i64),
                fps: v.get("fps").and_then(Value::as_f64),
                thumbnail_url: extract_thumbnail_url(&v),
                webpage_url: v
                    .get("webpage_url")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                live_status: live,
            },
            out.peak_rss_kib,
            out.duration_ms,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_queue_rejects_playlists_and_live_content() {
        assert!(
            validate_single_video(&serde_json::json!({
                "_type": "playlist",
                "entries": []
            }))
            .unwrap_err()
            .to_string()
            .contains("播放列表")
        );
        assert!(
            validate_single_video(&serde_json::json!({ "live_status": "is_live" }))
                .unwrap_err()
                .to_string()
                .contains("直播")
        );
        validate_single_video(&serde_json::json!({
            "_type": "video",
            "live_status": "not_live"
        }))
        .unwrap();
    }

    #[test]
    fn live_content_pending_error_is_classified_through_context() {
        let error = validate_single_video(&serde_json::json!({
            "live_status": "is_live"
        }))
        .unwrap_err()
        .context("metadata probe failed");
        assert!(is_live_content_pending(&error));
        assert!(!is_live_content_pending(&anyhow::anyhow!(
            "network timeout"
        )));
    }

    #[test]
    fn metadata_uses_youtube_selected_thumbnail_or_best_fallback() {
        assert_eq!(
            extract_thumbnail_url(&serde_json::json!({
                "thumbnail": "https://i.ytimg.com/selected.jpg",
                "thumbnails": [{"url": "https://i.ytimg.com/fallback.jpg"}]
            }))
            .as_deref(),
            Some("https://i.ytimg.com/selected.jpg")
        );
        assert_eq!(
            extract_thumbnail_url(&serde_json::json!({
                "thumbnail": "https://i.ytimg.com/selected-low-resolution.jpg",
                "thumbnails": [
                    {"url": "https://i.ytimg.com/small.jpg", "width": 480, "height": 360},
                    {"url": "https://i.ytimg.com/maxres.jpg", "width": 1920, "height": 1080}
                ]
            }))
            .as_deref(),
            Some("https://i.ytimg.com/maxres.jpg")
        );
        assert_eq!(
            extract_thumbnail_url(&serde_json::json!({
                "thumbnails": [
                    {"url": "https://i.ytimg.com/small.jpg"},
                    {"url": "https://i.ytimg.com/best.jpg"},
                    {"width": 1920, "height": 1080}
                ]
            }))
            .as_deref(),
            Some("https://i.ytimg.com/best.jpg")
        );
    }

    #[test]
    fn reconcile_only_accepts_videos_published_after_channel_baseline() {
        let baseline = DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap();
        assert_eq!(
            reconcile_after_baseline(Some(baseline), Some(1_800_000_001)),
            Some(true)
        );
        assert_eq!(
            reconcile_after_baseline(Some(baseline), Some(1_800_000_000)),
            Some(false)
        );
        assert_eq!(reconcile_after_baseline(Some(baseline), None), None);
        assert_eq!(reconcile_after_baseline(None, None), Some(true));
    }
}
