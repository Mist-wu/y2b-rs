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
use std::sync::Mutex;
use std::time::Duration;
use tokio::process::Command;

pub struct Monitor {
    config: Config,
    db: Database,
    client: reqwest::Client,
    /// 最近一次确认「本轮不入队」的时间（按 video_id），用于限制重复拉取元数据。
    ///
    /// 两类来源：直播内容尚未就绪，以及时长超过上限。前者结束后会变成可搬运的
    /// 回放，所以不能永久拉黑——否则直播中发现的视频永远等不到回放被入队；
    /// 后者虽然不会变化，但同一份 TTL 也让 max_duration_seconds 改配置后能在
    /// 一个复查周期内生效。
    deferred_videos: Mutex<std::collections::HashMap<String, std::time::Instant>>,
    /// 最近一次 yt-dlp 回退检查时间（按频道），防止 RSS 故障时高频拉取。
    last_fallback_at: Mutex<std::collections::HashMap<i64, std::time::Instant>>,
}

/// 构造带公共参数（js 运行时、cookies）的 yt-dlp 命令，供各子命令复用。
///
/// `--force-ipv4`：googlevideo 的 DNS 同时返回 A 和 AAAA，而部署机没有 IPv6
/// 出口，yt-dlp 连到 AAAA 地址会直接 `[Errno 101] Network is unreachable`。
/// 元数据走 youtube.com 不受影响，所以症状是「元数据成功、下载必失败」，
/// 重试 5 次后进 dead_letter——线上有视频因此晚了两天才搬。
pub(crate) fn ytdlp_command(config: &YoutubeConfig) -> Command {
    let mut cmd = Command::new(&config.yt_dlp);
    cmd.args(["--force-ipv4", "--js-runtimes", "node"]);
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

const LIVE_CONTENT_PENDING_PREFIX: &str = "直播内容尚未就绪，暂不处理";

/// 跳过某个视频后，多久允许重新拉取一次元数据。
///
/// 直播中/预告的视频最终会变成 `was_live` 回放并可以搬运，所以必须周期性复查；
/// 间隔取 30 分钟，兼顾及时性和 yt-dlp 调用成本。
const RECHECK_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// 尚未就绪、需要稍后复查的 `live_status`。
///
/// - `is_live`：直播进行中，还没有完整回放
/// - `is_upcoming`：预约中，尚未开始
/// - `post_live`：直播刚结束，YouTube 仍在生成回放，此时下载会拿到残片
///
/// `was_live`（直播回放）不在此列——回放已经是完整视频，按普通视频搬运。
const LIVE_STATUS_NOT_READY: &[&str] = &["is_live", "is_upcoming", "post_live"];

const DURATION_LIMIT_PREFIX: &str = "视频时长超过上限";

/// 时长超限的视频永远不会变短，属于永久跳过（区别于直播的「稍后复查」）。
pub fn exceeds_duration_limit(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().starts_with(DURATION_LIMIT_PREFIX))
}

/// 时长上限校验。`limit_seconds` 为 0 表示不限制；元数据缺少 duration 时放行
/// （拿不到时长不作为拒绝理由，保持原有行为）。
fn validate_duration(duration: Option<f64>, limit_seconds: u64) -> Result<()> {
    if limit_seconds == 0 {
        return Ok(());
    }
    let Some(duration) = duration else {
        return Ok(());
    };
    if duration > limit_seconds as f64 {
        bail!(
            "{DURATION_LIMIT_PREFIX}: {:.0}s > {limit_seconds}s",
            duration
        )
    }
    Ok(())
}

/// yt-dlp 在元数据阶段就会拒绝未开始的直播/预约事件（不返回 JSON，直接报错）。
const LIVE_EVENT_NOT_STARTED_MARKERS: &[&str] = &[
    "This live event will begin",
    "This live stream will begin",
    "has not yet started",
    "This live event has ended",
];

pub fn is_live_content_pending(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        if message.starts_with(LIVE_CONTENT_PENDING_PREFIX) {
            return true;
        }
        contains_live_markers(&message)
    })
}

/// 检测输出文本里是否有直播/预约特征（yt-dlp 的英文错误消息或元数据字段）。
pub fn contains_live_markers(text: &str) -> bool {
    LIVE_EVENT_NOT_STARTED_MARKERS
        .iter()
        .any(|marker| text.contains(marker))
}

fn validate_single_video(v: &Value) -> Result<()> {
    if v.get("_type").and_then(Value::as_str) == Some("playlist")
        || v.get("entries").and_then(Value::as_array).is_some()
    {
        bail!("请输入单个 YouTube 视频 URL，不支持播放列表")
    }
    let live = v.get("live_status").and_then(Value::as_str);
    if live.is_some_and(|status| LIVE_STATUS_NOT_READY.contains(&status)) {
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

/// 自动发现时是否应当跳过这条直播回放。
///
/// 只对 `was_live` 生效：普通视频不受游标限制。拿不到开播时间的回放按积压
/// 处理——宁可漏掉一条，也不要把历史直播灌进队列。
fn is_backlog_replay(meta: &VideoMetadata, cutoff: DateTime<Utc>) -> bool {
    if meta.live_status.as_deref() != Some("was_live") {
        return false;
    }
    let Some(started) = meta
        .timestamp
        .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))
    else {
        return true;
    };
    started < cutoff
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
            deferred_videos: Mutex::new(std::collections::HashMap::new()),
            last_fallback_at: Mutex::new(std::collections::HashMap::new()),
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
        let replay_cutoff = self.db.live_replay_cutoff()?;
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
                // 直播未就绪和超时长是预期内的跳过，不该记成故障。
                Err(error) if is_live_content_pending(&error) || exceeds_duration_limit(&error) => {
                    tracing::info!(video_id, error = %error, "校对候选不满足搬运条件，跳过");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(video_id, error = %error, "校对候选元数据获取失败，跳过");
                    continue;
                }
            };
            if is_backlog_replay(&metadata, replay_cutoff) {
                tracing::info!(video_id, "历史直播回放，不自动入队");
                continue;
            }
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
        let replay_cutoff = self.db.live_replay_cutoff()?;
        let bytes = match self.client.get(&url).send().await {
            Ok(response) => match response.error_for_status() {
                Ok(response) => response.bytes().await?,
                Err(error) => return self.fallback_poll_channel(id, error.into()).await,
            },
            Err(error) => return self.fallback_poll_channel(id, error.into()).await,
        };
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
            if enqueue {
                if self.recheck_is_throttled(&video_id) {
                    continue;
                }
                if self.db.get_job_by_video_id(&video_id)?.is_some() {
                    continue;
                }
                // 先拉元数据筛掉不该入队的内容：尚未就绪的直播、超长视频，
                // 以及策略生效之前就开播的历史回放。
                match self.fetch_metadata(&link).await {
                    Ok((meta, _, _)) if is_backlog_replay(&meta, replay_cutoff) => {
                        tracing::info!(
                            video_id,
                            started = ?meta.timestamp,
                            "历史直播回放，不自动入队"
                        );
                        self.defer_video(&video_id);
                        continue;
                    }
                    Ok(_) => {}
                    Err(error) if is_live_content_pending(&error) => {
                        tracing::info!(
                            video_id,
                            error = %error,
                            "直播内容尚未就绪，稍后复查是否已有回放"
                        );
                        self.defer_video(&video_id);
                        continue;
                    }
                    Err(error) if exceeds_duration_limit(&error) => {
                        tracing::info!(video_id, error = %error, "视频超过时长上限，跳过");
                        self.defer_video(&video_id);
                        continue;
                    }
                    Err(_) => {
                        // 其他错误（网络等）不阻塞入队，交给流水线重试。
                    }
                }
            }
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

    /// 领取本频道的回退名额：冷却期内返回 false。
    ///
    /// 单独成函数是为了把 `MutexGuard` 的生存期限制在同步作用域内——回退本身要
    /// `await` 一个分钟级的 yt-dlp 调用，跨 await 持有 `std::sync::Mutex` 在多个
    /// 任务并发访问同一个 Monitor 时会死锁。
    fn claim_fallback_slot(&self, id: i64) -> bool {
        const FALLBACK_COOLDOWN: Duration = Duration::from_secs(600);
        let now = std::time::Instant::now();
        let mut map = self.last_fallback_at.lock().unwrap();
        if let Some(last) = map.get(&id)
            && now.duration_since(*last) < FALLBACK_COOLDOWN
        {
            return false;
        }
        map.insert(id, now);
        true
    }

    /// 距上次确认不足 `RECHECK_INTERVAL` 时跳过本轮元数据拉取。
    ///
    /// 锁不跨 await：只在这里做一次判断就释放。
    fn recheck_is_throttled(&self, video_id: &str) -> bool {
        self.deferred_videos
            .lock()
            .unwrap()
            .get(video_id)
            .is_some_and(|at| at.elapsed() < RECHECK_INTERVAL)
    }

    /// 记录本轮跳过的时间，抑制下一轮的重复元数据拉取。
    fn defer_video(&self, video_id: &str) {
        self.deferred_videos
            .lock()
            .unwrap()
            .insert(video_id.to_string(), std::time::Instant::now());
    }

    /// RSS 拉取失败时回退到 yt-dlp 频道列表（带每频道冷却，防止高频拉取）。
    async fn fallback_poll_channel(&self, id: i64, error: anyhow::Error) -> Result<usize> {
        if !self.claim_fallback_slot(id) {
            tracing::debug!(channel_id = id, "RSS 故障，回退冷却中，跳过本轮");
            return Ok(0);
        }
        tracing::warn!(
            channel_id = id,
            error = %error,
            "RSS 拉取失败，回退 yt-dlp 频道列表"
        );
        self.reconcile_channel(id, &self.db.channel_url(id)?).await
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
        let v: Value = match serde_json::from_str(out.stdout.trim()) {
            Ok(v) => v,
            Err(error) => {
                // yt-dlp 对直播/预约内容可能不输出 JSON（原因在 stderr）；
                // 解析失败时合并 stderr 检查直播特征，避免漏检导致反复重试。
                let merged = format!("{}\n{}", out.stdout, out.stderr);
                if contains_live_markers(&merged) {
                    bail!("{LIVE_CONTENT_PENDING_PREFIX}: yt-dlp 无 JSON 输出")
                }
                return Err(error).context("yt-dlp 输出不是 JSON");
            }
        };
        validate_single_video(&v)?;
        validate_duration(
            v.get("duration").and_then(Value::as_f64),
            self.config.youtube.max_duration_seconds,
        )?;
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
        // 直播中/预告/回放生成中：暂不搬运，等回放就绪后复查。
        for live in ["is_live", "is_upcoming", "post_live"] {
            assert!(
                validate_single_video(&serde_json::json!({ "live_status": live }))
                    .unwrap_err()
                    .to_string()
                    .contains("直播"),
                "live_status={live} 应被推迟"
            );
        }
        // 直播回放已是完整视频，按普通视频搬运。
        validate_single_video(&serde_json::json!({
            "_type": "video",
            "live_status": "was_live"
        }))
        .unwrap();
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
        // yt-dlp 对直播可能不输出 JSON（stdout 为空、原因在 stderr）：
        // 合并输出里出现直播特征时应归类为直播内容。
        assert!(contains_live_markers("ERROR: This live event has ended"));
        assert!(!contains_live_markers("ERROR: network timeout"));
    }

    #[test]
    fn ytdlp_live_event_begin_error_is_classified_as_live_content() {
        for message in [
            "ERROR: [youtube] ZIGzQ-zJFWc: This live event will begin in 3 days.",
            "This live stream will begin in 12 days.",
        ] {
            assert!(
                is_live_content_pending(&anyhow::anyhow!(message)),
                "消息应被识别为直播内容: {message}"
            );
        }
        assert!(!is_live_content_pending(&anyhow::anyhow!(
            "ERROR: [youtube] abc: Video unavailable"
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
    fn backlog_replays_are_skipped_but_new_replays_and_normal_videos_pass() {
        let cutoff = DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap();
        let replay = |status: &str, ts: Option<i64>| VideoMetadata {
            live_status: Some(status.into()),
            timestamp: ts,
            ..crate::pipeline::testing::metadata()
        };
        // 策略生效前开播的回放：跳过。
        assert!(is_backlog_replay(
            &replay("was_live", Some(1_799_999_999)),
            cutoff
        ));
        // 生效后开播的回放：搬运。
        assert!(!is_backlog_replay(
            &replay("was_live", Some(1_800_000_001)),
            cutoff
        ));
        // 普通视频不受游标限制，哪怕发布得很早。
        assert!(!is_backlog_replay(&replay("not_live", Some(1)), cutoff));
        // 回放缺少开播时间时按积压处理，避免误灌历史直播。
        assert!(is_backlog_replay(&replay("was_live", None), cutoff));
    }

    #[test]
    fn duration_limit_rejects_only_videos_over_the_cap() {
        // 2 小时上限：正好 2 小时放行，超出一秒拒绝。
        validate_duration(Some(7200.0), 7200).unwrap();
        let error = validate_duration(Some(7201.0), 7200).unwrap_err();
        assert!(exceeds_duration_limit(&error), "应被识别为时长超限");
        assert!(!is_live_content_pending(&error), "不应被误判为直播内容");
        // 上下文包装后仍要能识别（流水线拿到的是带 context 的错误）。
        assert!(exceeds_duration_limit(
            &validate_duration(Some(14400.0), 7200)
                .unwrap_err()
                .context("元数据校验失败")
        ));
        // 缺少时长不作为拒绝理由；0 表示不限制。
        validate_duration(None, 7200).unwrap();
        validate_duration(Some(99999.0), 0).unwrap();
    }

    #[test]
    fn live_recheck_throttles_but_does_not_blacklist_forever() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("x.db")).unwrap();
        let monitor = Monitor::new(Config::default(), db).unwrap();
        assert!(
            !monitor.recheck_is_throttled("abc"),
            "没记录过的视频应立即检查"
        );
        monitor
            .deferred_videos
            .lock()
            .unwrap()
            .insert("abc".into(), std::time::Instant::now());
        assert!(monitor.recheck_is_throttled("abc"), "刚确认过应节流");
        // 超过复查间隔后必须重新检查，否则直播中发现的视频永远等不到回放入队。
        monitor
            .deferred_videos
            .lock()
            .unwrap()
            .insert("abc".into(), std::time::Instant::now() - RECHECK_INTERVAL);
        assert!(!monitor.recheck_is_throttled("abc"));
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
