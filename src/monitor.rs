use crate::{
    config::{Config, YoutubeConfig},
    db::{Database, NewJob, NewVideoCandidate},
    model::{
        CandidateSource, Channel, ChannelPriority, Job, TransferMode, VideoCandidate, VideoMetadata,
    },
    process::run_monitored,
    youtube_api::{
        PlaylistVideo, QuotaDegradation, ResponseBodyError, YoutubeDataApi, bounded_http_client,
        read_response_body,
    },
};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike, Timelike, Utc};
use chrono_tz::Tz;
use feed_rs::parser;
use reqwest::{StatusCode, header::RETRY_AFTER};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{
    Mutex, Once,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::process::Command;
use uuid::Uuid;

pub struct Monitor {
    config: Config,
    db: Database,
    client: reqwest::Client,
    /// RSS 故障时的 yt-dlp 回退限流：既限制单频道频率，也限制全局风暴。
    fallback_limiter: Mutex<FallbackLimiter>,
    /// 整个频道集合的 RSS 结果滑动窗口。打开时刻落库，窗口本身只用于判定。
    rss_circuit: Mutex<RssCircuitWindow>,
    data_api: Option<YoutubeDataApi>,
    uploads_refreshed_this_process: AtomicBool,
}

const YOUTUBE_EXTRACTOR_ARGS: &str = "youtube:player_client=web_creator";

/// 构造带公共参数（YouTube 客户端、js 运行时、cookies）的 yt-dlp 命令，供各子命令复用。
///
/// 注意：不要加 `--force-ipv4`。曾经为了绕开 `[Errno 101] Network is unreachable`
/// 加过，但那是误诊——真正的原因是部署机到 `74.125.0.0/16` 整段不可达，两个地址
/// 族都连不上。而 `--force-ipv4`（实现上等价于 `--source-address 0.0.0.0`）在当前
/// yt-dlp 版本下会让本来正常的域名解析报 `[Errno -9] Address family for hostname
/// not supported`，属于净负面。
pub(crate) fn ytdlp_command(config: &YoutubeConfig) -> Command {
    let mut cmd = Command::new(&config.yt_dlp);
    // 2026-08 起默认的 web_safari 客户端会在服务器账号上稳定返回
    // `The page needs to be reloaded`。字幕路径曾单独指定 web_creator，导致
    // 字幕可用但元数据和原片下载仍失败。统一放在公共入口，确保发现、元数据、
    // 字幕和原片下载使用同一客户端；GVS token 由已部署的 bgutil provider 生成。
    cmd.args([
        "--js-runtimes",
        "node",
        "--extractor-args",
        YOUTUBE_EXTRACTOR_ARGS,
    ]);
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

/// 一批闸门处理的结果。
///
/// 必须把「处理了多少」和「晋级了多少」分开报：整批候选都被拒（早于 baseline、
/// 超时长、历史回放）是常态，此时 `promoted` 为 0 但活是干了的。只看晋级数的
/// 调用方会把这种情况误判成「没活可干」而提前收手，把候选留在表里不动。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GateOutcome {
    /// 本批实际取出并处理的候选数。只有它为 0 才代表没有到期候选。
    pub processed: usize,
    /// 其中晋级为任务的数量。
    pub promoted: usize,
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

const RSS_MAX_RETRIES: usize = 2;
const RSS_RETRY_BASE: Duration = Duration::from_secs(1);
const RSS_BODY_LIMIT: usize = 1024 * 1024;
const FALLBACK_CHANNEL_COOLDOWN: Duration = Duration::from_secs(10 * 60);
const FALLBACK_GLOBAL_WINDOW: Duration = Duration::from_secs(10 * 60);
const FALLBACK_GLOBAL_LIMIT: usize = 3;
const RSS_CIRCUIT_STATE_KEY: &str = "rss_circuit_open_until";
const RSS_CIRCUIT_WINDOW: Duration = Duration::from_secs(10 * 60);
const RSS_CIRCUIT_DURATION: chrono::Duration = chrono::Duration::minutes(10);
const RSS_CIRCUIT_MIN_SAMPLES: usize = 8;
const RSS_CIRCUIT_FAILURE_PERCENT: usize = 60;
const RSS_CIRCUIT_PROBES: usize = 2;
const CHANNEL_FAILURE_BACKOFF_CAP: Duration = Duration::from_secs(6 * 60 * 60);
const API_UPLOADS_REFRESHED_AT_KEY: &str = "uploads_playlist_refreshed_at";
const API_UPLOADS_REFRESH_INTERVAL: chrono::Duration = chrono::Duration::days(1);
const DEGRADED_COLD_INTERVAL_MULTIPLIER: u32 = 2;
const DEGRADED_HOT_WINDOW_DIVISOR: u64 = 2;

/// 只让 yt-dlp 输出流水线实际使用的元数据字段。
///
/// `--dump-single-json` 会把直播回放的 HLS fragments/formats 全部展开；线上一条
/// `post_live` 视频产生了约 74 MiB JSON，超过进程捕获上限后只剩尾部，稳定触发
/// “yt-dlp 输出不是 JSON”。yt-dlp 的对象选择输出模板可以保留所需字段，同时把
/// 输出压缩到几 KiB 以内。
const VIDEO_METADATA_TEMPLATE: &str = "%(.{_type,id,title,description,uploader,upload_date,channel,channel_id,timestamp,duration,width,height,fps,thumbnail,webpage_url,live_status})j";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FallbackClaim {
    Allowed,
    ChannelCooldown,
    GlobalCircuitOpen,
}

#[derive(Default)]
struct FallbackLimiter {
    per_channel: HashMap<i64, Instant>,
    global_starts: VecDeque<Instant>,
}

#[derive(Default)]
struct RssCircuitWindow {
    samples: VecDeque<(Instant, bool)>,
}

impl RssCircuitWindow {
    /// 返回当前窗口是否达到开闸阈值。`failed` 表示这次 RSS 样本失败。
    fn record(&mut self, failed: bool, now: Instant) -> bool {
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) >= RSS_CIRCUIT_WINDOW)
        {
            self.samples.pop_front();
        }
        self.samples.push_back((now, failed));
        let failures = self.samples.iter().filter(|(_, failed)| *failed).count();
        self.samples.len() >= RSS_CIRCUIT_MIN_SAMPLES
            && failures * 100 > self.samples.len() * RSS_CIRCUIT_FAILURE_PERCENT
    }
}

#[derive(Debug, Error)]
enum FeedFetchError {
    #[error("RSS HTTP {status}")]
    Http {
        status: StatusCode,
        retry_at: Option<DateTime<Utc>>,
    },
    #[error("RSS 请求失败: {source}")]
    Request {
        #[source]
        source: reqwest::Error,
    },
    #[error("RSS 响应体超过上限 {limit} 字节")]
    BodyTooLarge { limit: usize },
}

struct PollExecution {
    result: Result<usize>,
    rss_failed: bool,
    retry_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DataApiPollMode {
    Priority,
    WebSubFallback,
    InsufficientHistory,
    PredictedHot,
    PredictedCold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DataApiPollDecision {
    interval: Duration,
    mode: DataApiPollMode,
}

fn websub_lease_active(config: &Config, channel: &Channel, now: DateTime<Utc>) -> bool {
    config.websub.enabled
        && channel
            .websub_lease_expires_at
            .is_some_and(|expires_at| expires_at > now)
}

/// RSS 探针成功后的下一次间隔。WebSub 租约有效时 RSS 与 Data API 一样退到
/// `websub.data_api_poll_minutes` 兜底：优先频道每分钟一次的 RSS 本来只是为了
/// 补 Data API 的发现延迟，推送到位后继续每分钟请求 YouTube 只会增加 429 风险。
fn rss_poll_interval(config: &Config, channel: &Channel, now: DateTime<Utc>) -> Duration {
    if websub_lease_active(config, channel, now) {
        Duration::from_secs(config.websub.data_api_poll_minutes.saturating_mul(60))
    } else {
        Duration::from_secs(config.monitor.poll_seconds.max(1))
    }
}

fn prediction_poll_decision(
    config: &Config,
    history: &[DateTime<Utc>],
    now: DateTime<Utc>,
    timezone: Tz,
    degradation: QuotaDegradation,
    websub_active: bool,
) -> DataApiPollDecision {
    if websub_active {
        return DataApiPollDecision {
            interval: Duration::from_secs(config.websub.data_api_poll_minutes.saturating_mul(60)),
            mode: DataApiPollMode::WebSubFallback,
        };
    }
    if history.len() < config.monitor.prediction_min_samples {
        return DataApiPollDecision {
            interval: Duration::from_secs(
                config
                    .monitor
                    .prediction_fallback_poll_minutes
                    .saturating_mul(60),
            ),
            mode: DataApiPollMode::InsufficientHistory,
        };
    }

    let mut counts = [[0_u32; 24]; 7];
    for published_at in history {
        let local = published_at.with_timezone(&timezone);
        let weekday = local.weekday().num_days_from_monday() as usize;
        counts[weekday][local.hour() as usize] += 1;
    }
    let mut predicted_hours = [None; 7];
    for (weekday, hours) in counts.iter().enumerate() {
        let mut best_hour = None;
        let mut best_count = 0;
        for (hour, count) in hours.iter().copied().enumerate() {
            if count > best_count {
                best_count = count;
                best_hour = Some(hour as i64);
            }
        }
        predicted_hours[weekday] = best_hour;
    }

    let local_now = now.with_timezone(&timezone);
    let week_seconds = 7_i64 * 24 * 60 * 60;
    let now_seconds = i64::from(local_now.weekday().num_days_from_monday()) * 24 * 60 * 60
        + i64::from(local_now.num_seconds_from_midnight());
    let mut window_minutes = config.monitor.prediction_window_minutes;
    if degradation >= QuotaDegradation::NarrowHot {
        window_minutes = window_minutes
            .saturating_div(DEGRADED_HOT_WINDOW_DIVISOR)
            .max(1);
    }
    let half_window_seconds =
        i64::try_from(window_minutes.saturating_mul(60) / 2).unwrap_or(i64::MAX);
    let predicted_centers = predicted_hours
        .iter()
        .enumerate()
        .filter_map(|(weekday, hour)| hour.map(|hour| (weekday as i64, hour)))
        .map(|(weekday, hour)| {
            // 小时桶以 :30 为中心；默认 2 小时窗覆盖完整预测小时并前后各留 30 分钟。
            weekday * 24 * 60 * 60 + hour * 60 * 60 + 30 * 60
        })
        .collect::<Vec<_>>();
    let hot = predicted_centers.iter().copied().any(|center| {
        let direct = (now_seconds - center).abs();
        direct.min(week_seconds - direct) <= half_window_seconds
    });
    if hot {
        DataApiPollDecision {
            interval: Duration::from_secs(config.monitor.prediction_hot_poll_seconds),
            mode: DataApiPollMode::PredictedHot,
        }
    } else {
        let mut interval = Duration::from_secs(
            config
                .monitor
                .prediction_cold_poll_minutes
                .saturating_mul(60),
        );
        if degradation >= QuotaDegradation::ExtendCold {
            interval = interval.saturating_mul(DEGRADED_COLD_INTERVAL_MULTIPLIER);
        }
        // 冷区周期不能跨过下一个热窗起点，否则“热窗 60 秒”可能在刚开始时仍睡
        // 最长 30/60 分钟。必要时提前在边界唤醒，不额外发边界前的 API 请求。
        if let Some(seconds_until_hot) = predicted_centers
            .iter()
            .map(|center| (center - half_window_seconds).rem_euclid(week_seconds))
            .map(|start| (start - now_seconds).rem_euclid(week_seconds))
            .filter(|seconds| *seconds > 0)
            .min()
        {
            interval = interval.min(Duration::from_secs(seconds_until_hot as u64));
        }
        DataApiPollDecision {
            interval,
            mode: DataApiPollMode::PredictedCold,
        }
    }
}

/// 不表示具体语言的 ISO 值：`zxx` = 无语言内容（纯游戏画面／音乐），
/// `und` = 未确定。它们不是「另一种语言」，当成不符会产生大量误报——线上首轮
/// 扫描里 `zxx` 一个值就占了 67 条告警。
const UNKNOWN_LANGUAGE_TAGS: &[&str] = &["zxx", "und"];

fn is_unknown_language(actual: &str) -> bool {
    let primary = actual.split(['-', '_']).next().unwrap_or(actual).trim();
    primary.is_empty()
        || UNKNOWN_LANGUAGE_TAGS
            .iter()
            .any(|tag| primary.eq_ignore_ascii_case(tag))
}

fn source_language_matches(expected: &str, actual: &str) -> bool {
    // 无法判定语言时一律放行，不算不符。
    if is_unknown_language(actual) {
        return true;
    }
    let expected = expected.split(['-', '_']).next().unwrap_or(expected).trim();
    let actual = actual.split(['-', '_']).next().unwrap_or(actual).trim();
    !expected.is_empty() && expected.eq_ignore_ascii_case(actual)
}

impl FeedFetchError {
    fn retryable(&self) -> bool {
        match self {
            Self::Http { status, .. } => status.is_server_error(),
            Self::Request { source } => source.is_timeout() || source.is_connect(),
            Self::BodyTooLarge { .. } => false,
        }
    }

    fn retry_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Http { retry_at, .. } => *retry_at,
            Self::Request { .. } | Self::BodyTooLarge { .. } => None,
        }
    }
}

impl FallbackLimiter {
    fn claim(&mut self, channel_id: i64, now: Instant) -> FallbackClaim {
        if self
            .per_channel
            .get(&channel_id)
            .is_some_and(|last| now.duration_since(*last) < FALLBACK_CHANNEL_COOLDOWN)
        {
            return FallbackClaim::ChannelCooldown;
        }
        while self
            .global_starts
            .front()
            .is_some_and(|started| now.duration_since(*started) >= FALLBACK_GLOBAL_WINDOW)
        {
            self.global_starts.pop_front();
        }
        if self.global_starts.len() >= FALLBACK_GLOBAL_LIMIT {
            return FallbackClaim::GlobalCircuitOpen;
        }
        self.per_channel.insert(channel_id, now);
        self.global_starts.push_back(now);
        FallbackClaim::Allowed
    }

    #[cfg(test)]
    fn last_attempt(&self, channel_id: i64) -> Option<Instant> {
        self.per_channel.get(&channel_id).copied()
    }
}

fn is_youtube_video_id(value: &str) -> bool {
    value.len() == 11
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn validate_youtube_video_id(value: &str) -> Result<()> {
    anyhow::ensure!(
        is_youtube_video_id(value),
        "YouTube video ID 必须是 11 位 ASCII 字母、数字、连字符或下划线: {value:?}"
    );
    Ok(())
}

fn canonical_youtube_video_url(video_id: &str) -> Result<String> {
    validate_youtube_video_id(video_id)?;
    Ok(format!("https://www.youtube.com/watch?v={video_id}"))
}

fn parse_operator_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value.trim()).context("请输入完整的 YouTube URL")?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "YouTube URL 只允许 http 或 https"
    );
    anyhow::ensure!(
        url.username().is_empty() && url.password().is_none(),
        "YouTube URL 不允许包含用户信息"
    );
    anyhow::ensure!(url.port().is_none(), "YouTube URL 不允许自定义端口");
    Ok(url)
}

fn is_youtube_host(host: &str) -> bool {
    matches!(
        host,
        "youtube.com" | "www.youtube.com" | "m.youtube.com" | "music.youtube.com"
    )
}

fn canonicalize_youtube_video_url(value: &str) -> Result<(String, String)> {
    let url = parse_operator_url(value)?;
    let host = url.host_str().context("YouTube URL 缺少域名")?;
    let segments = url
        .path_segments()
        .context("YouTube URL 路径无效")?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let video_id = if host == "youtu.be" {
        anyhow::ensure!(segments.len() == 1, "youtu.be URL 必须只包含 video ID");
        segments[0].to_string()
    } else {
        anyhow::ensure!(
            is_youtube_host(host),
            "视频 URL 只允许 youtube.com 或 youtu.be 域名"
        );
        match segments.as_slice() {
            ["watch"] => {
                let mut values = url
                    .query_pairs()
                    .filter(|(key, _)| key == "v")
                    .map(|(_, value)| value.into_owned());
                let video_id = values.next().context("YouTube watch URL 缺少 video ID")?;
                anyhow::ensure!(
                    values.next().is_none(),
                    "YouTube watch URL 包含多个 video ID"
                );
                video_id
            }
            [route, video_id] if matches!(*route, "shorts" | "live" | "embed") => {
                (*video_id).to_string()
            }
            _ => bail!("请输入单个 YouTube 视频 URL"),
        }
    };
    validate_youtube_video_id(&video_id)?;
    let canonical_url = canonical_youtube_video_url(&video_id)?;
    Ok((video_id, canonical_url))
}

/// yt-dlp 对裸频道 URL 返回的首层 entries 是 videos/shorts/streams 标签页，不是
/// 视频。新频道入库和旧频道校对都在运行时规范化；用户明确给出的标签页则保留。
fn normalize_channel_url(value: &str) -> Result<String> {
    let url = parse_operator_url(value)?;
    let host = url.host_str().context("YouTube 频道 URL 缺少域名")?;
    anyhow::ensure!(is_youtube_host(host), "频道 URL 只允许 youtube.com 域名");
    let path = url.path().trim_end_matches('/');
    anyhow::ensure!(!path.is_empty(), "YouTube 频道 URL 缺少频道路径");
    let path = if ["/videos", "/shorts", "/streams"]
        .iter()
        .any(|suffix| path.ends_with(suffix))
    {
        path.to_string()
    } else {
        format!("{path}/videos")
    };
    Ok(format!("https://www.youtube.com{path}"))
}

fn random_jitter_factor() -> f64 {
    let bytes = Uuid::new_v4().into_bytes();
    let sample = f64::from(u16::from_be_bytes([bytes[0], bytes[1]])) / f64::from(u16::MAX);
    0.5 + sample
}

fn jittered_backoff(base: Duration, exponent: u32, jitter: f64) -> Duration {
    let multiplier = 2_u64.saturating_pow(exponent.min(20));
    base.saturating_mul(multiplier as u32)
        .mul_f64(jitter.clamp(0.5, 1.5))
}

fn parse_retry_after(
    value: &reqwest::header::HeaderValue,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let raw = value.to_str().ok()?.trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        let seconds = i64::try_from(seconds).ok()?;
        return now.checked_add_signed(chrono::Duration::seconds(seconds));
    }
    DateTime::parse_from_rfc2822(raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

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
        let api_key = std::env::var("YOUTUBE_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty());
        if api_key.is_none() {
            static WARN_MISSING_KEY: Once = Once::new();
            WARN_MISSING_KEY.call_once(|| {
                tracing::warn!("未设置 YOUTUBE_API_KEY，Data API 已停用，将降级到 RSS 与 yt-dlp");
            });
        }
        Self::build(config, db, api_key, None)
    }

    fn build(
        config: Config,
        db: Database,
        api_key: Option<String>,
        api_base_url: Option<&str>,
    ) -> Result<Self> {
        let client = bounded_http_client("y2b-rs/0.1")?;
        let data_api = api_key.map(|api_key| match api_base_url {
            Some(base_url) => {
                YoutubeDataApi::with_base_url(client.clone(), db.clone(), api_key, base_url)
            }
            None => YoutubeDataApi::new(client.clone(), db.clone(), api_key),
        });
        Ok(Self {
            config,
            db,
            client,
            fallback_limiter: Mutex::new(FallbackLimiter::default()),
            rss_circuit: Mutex::new(RssCircuitWindow::default()),
            data_api,
            uploads_refreshed_this_process: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    fn new_with_data_api(
        config: Config,
        db: Database,
        api_key: Option<&str>,
        api_base_url: &str,
    ) -> Result<Self> {
        Self::build(config, db, api_key.map(str::to_string), Some(api_base_url))
    }

    pub fn has_data_api(&self) -> bool {
        self.data_api.is_some()
    }

    pub fn data_api_primary_enabled(&self) -> Result<bool> {
        let Some(api) = self.data_api.as_ref() else {
            return Ok(false);
        };
        Ok(api.quota_policy()?.degradation != QuotaDegradation::FallbackOnly)
    }

    /// 有效 WebSub 租约意味着新视频由 hub 秒级推送到本地回调，Data API 与 RSS
    /// 都只剩兜底职责。优先频道也不例外：它们原本每分钟一次的 Data API 轮询占
    /// 日配额八成以上，正是 WebSub 要替代的部分。
    fn websub_lease_active(&self, channel: &Channel, now: DateTime<Utc>) -> bool {
        websub_lease_active(&self.config, channel, now)
    }

    fn data_api_poll_decision(
        &self,
        channel: &Channel,
        now: DateTime<Utc>,
        degradation: QuotaDegradation,
    ) -> Result<DataApiPollDecision> {
        let websub_active = self.websub_lease_active(channel, now);
        if channel.priority == ChannelPriority::Priority && !websub_active {
            return Ok(DataApiPollDecision {
                interval: Duration::from_secs(self.config.monitor.poll_seconds.max(1)),
                mode: DataApiPollMode::Priority,
            });
        }
        let timezone = self
            .config
            .runtime
            .timezone
            .parse::<Tz>()
            .with_context(|| {
                format!(
                    "runtime.timezone 不是有效 IANA 时区: {}",
                    self.config.runtime.timezone
                )
            })?;
        let history = self.db.channel_publication_history(channel.id)?;
        Ok(prediction_poll_decision(
            &self.config,
            &history,
            now,
            timezone,
            degradation,
            websub_active,
        ))
    }

    fn schedule_data_api_channel(
        &self,
        channel_id: i64,
        now: DateTime<Utc>,
        degradation: QuotaDegradation,
        etag: Option<&str>,
    ) -> Result<DataApiPollDecision> {
        let channel = self.db.channel(channel_id)?;
        let decision = self.data_api_poll_decision(&channel, now, degradation)?;
        let next_poll_at = now + chrono::Duration::from_std(decision.interval)?;
        self.db
            .schedule_data_api_poll(channel.id, next_poll_at, etag)?;
        if decision.mode == DataApiPollMode::WebSubFallback {
            tracing::info!(
                channel_id = channel.id,
                channel = %channel.name,
                next_poll_at = %next_poll_at,
                fallback_minutes = self.config.websub.data_api_poll_minutes,
                "WebSub 租约生效，Data API 自动降为纯兜底轮询"
            );
        }
        Ok(decision)
    }

    pub async fn resolve_channel(&self, url: &str) -> Result<ResolvedChannel> {
        let normalized_url = normalize_channel_url(url)?;
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--flat-playlist",
            "--playlist-items",
            "1",
            "--dump-single-json",
            "--skip-download",
            &normalized_url,
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
            url: normalized_url,
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

    /// Data API 主发现：只读取每频道持久化调度中已经到期的频道。
    pub async fn poll_data_api(&self) -> Result<usize> {
        let Some(api) = self.data_api.as_ref() else {
            return Ok(0);
        };
        let now = Utc::now();
        let channels = self.db.list_due_data_api_channels(now)?;
        if channels.is_empty() {
            return Ok(0);
        }
        let policy = api.quota_policy()?;
        if policy.degradation == QuotaDegradation::FallbackOnly {
            for channel in channels {
                self.db
                    .schedule_data_api_poll(channel.id, policy.reset_at, None)?;
            }
            return Ok(0);
        }
        if let Err(error) = self.refresh_upload_playlists(api).await {
            let current_policy = api.quota_policy()?;
            for channel in channels {
                if current_policy.degradation == QuotaDegradation::FallbackOnly {
                    self.db
                        .schedule_data_api_poll(channel.id, current_policy.reset_at, None)?;
                } else {
                    self.schedule_data_api_channel(
                        channel.id,
                        now,
                        current_policy.degradation,
                        None,
                    )?;
                }
            }
            return Err(error);
        }

        let channels = channels
            .into_iter()
            .map(|channel| self.db.channel(channel.id))
            .collect::<Result<Vec<_>>>()?;
        let mut discovered = 0;
        let mut first_error = None;
        for channel in channels {
            let before_request = api.quota_policy()?;
            if before_request.degradation == QuotaDegradation::FallbackOnly {
                self.db
                    .schedule_data_api_poll(channel.id, before_request.reset_at, None)?;
                continue;
            }
            let Some(playlist_id) = channel.uploads_playlist_id.as_deref() else {
                tracing::warn!(
                    channel = %channel.name,
                    youtube_channel_id = %channel.youtube_channel_id,
                    "频道缺少 uploads 播放列表，跳过本轮 Data API 发现"
                );
                self.schedule_data_api_channel(
                    channel.id,
                    Utc::now(),
                    before_request.degradation,
                    None,
                )?;
                continue;
            };
            match api
                .playlist_items(
                    playlist_id,
                    self.config.monitor.data_api_max_results,
                    channel.data_api_etag.as_deref(),
                )
                .await
            {
                Ok(page) => {
                    if page.not_modified {
                        tracing::debug!(channel = %channel.name, "playlistItems.list 返回 304，无新内容");
                    } else {
                        discovered += self.persist_playlist_candidates(&channel, page.videos)?;
                    }
                    let current_policy = api.quota_policy()?;
                    if current_policy.degradation == QuotaDegradation::FallbackOnly {
                        self.db.schedule_data_api_poll(
                            channel.id,
                            current_policy.reset_at,
                            page.etag.as_deref(),
                        )?;
                    } else {
                        self.schedule_data_api_channel(
                            channel.id,
                            Utc::now(),
                            current_policy.degradation,
                            page.etag.as_deref(),
                        )?;
                    }
                }
                Err(error) => {
                    let current_policy = api.quota_policy()?;
                    if current_policy.degradation == QuotaDegradation::FallbackOnly {
                        self.db.schedule_data_api_poll(
                            channel.id,
                            current_policy.reset_at,
                            None,
                        )?;
                    } else {
                        self.schedule_data_api_channel(
                            channel.id,
                            Utc::now(),
                            current_policy.degradation,
                            None,
                        )?;
                    }
                    if first_error.is_none() {
                        first_error = Some(anyhow::Error::new(error));
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error.context("一个或多个频道的 Data API 主发现失败")),
            None => Ok(discovered),
        }
    }

    fn persist_playlist_candidates(
        &self,
        channel: &Channel,
        videos: Vec<PlaylistVideo>,
    ) -> Result<usize> {
        let mut discovered = 0;
        for video in videos {
            if !is_youtube_video_id(&video.video_id) {
                tracing::warn!(video_id = %video.video_id, "Data API 返回非法 video_id，跳过");
                continue;
            }
            if self.db.get_job_by_video_id(&video.video_id)?.is_some()
                || self.db.is_over_duration_video(
                    &video.video_id,
                    self.config.youtube.max_duration_seconds,
                )?
            {
                continue;
            }
            let url = canonical_youtube_video_url(&video.video_id)?;
            if self.db.insert_video_candidate(NewVideoCandidate {
                video_id: &video.video_id,
                channel_id: Some(channel.id),
                url: &url,
                title: video.title.as_deref(),
                published_at: video.published_at,
                source: CandidateSource::DataApi,
            })? {
                discovered += 1;
            }
        }
        Ok(discovered)
    }

    async fn refresh_upload_playlists(&self, api: &YoutubeDataApi) -> Result<()> {
        let now = Utc::now();
        let refreshed_this_process = self.uploads_refreshed_this_process.load(Ordering::Acquire);
        let last_refresh = self
            .db
            .get_discovery_state(API_UPLOADS_REFRESHED_AT_KEY)?
            .as_deref()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc));
        let periodic_refresh_due = !refreshed_this_process
            || last_refresh.is_none_or(|last| now - last >= API_UPLOADS_REFRESH_INTERVAL);
        let new_channel_missing_uploads = match last_refresh {
            Some(last) => self.db.has_missing_uploads_playlist_created_after(last)?,
            None => false,
        };
        let refresh_due = periodic_refresh_due || new_channel_missing_uploads;
        if !refresh_due {
            return Ok(());
        }

        let channels = self.db.list_channels()?;
        let channel_ids = channels
            .iter()
            .map(|channel| channel.youtube_channel_id.clone())
            .collect::<Vec<_>>();
        let uploads = api.channel_upload_playlists(&channel_ids).await?;
        for channel in &channels {
            match uploads.get(&channel.youtube_channel_id) {
                Some(playlist_id) => self
                    .db
                    .set_channel_uploads_playlist(&channel.youtube_channel_id, playlist_id)?,
                None => tracing::warn!(
                    channel = %channel.name,
                    youtube_channel_id = %channel.youtube_channel_id,
                    "channels.list 未返回频道，可能是无效或不可访问频道"
                ),
            }
        }
        self.db
            .set_discovery_state(API_UPLOADS_REFRESHED_AT_KEY, &now.to_rfc3339())?;
        self.uploads_refreshed_this_process
            .store(true, Ordering::Release);
        Ok(())
    }

    pub async fn enqueue_video(
        &self,
        url: &str,
        transfer_mode: TransferMode,
    ) -> Result<EnqueueOutcome> {
        let (meta, _, _) = self.fetch_metadata(url).await?;
        validate_youtube_video_id(&meta.id)?;
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
                url: &meta.url,
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
        self.poll_channels_with_fallback(self.db.list_due_channels(Utc::now())?)
            .await
    }

    pub async fn poll_all_normal(&self) -> Result<usize> {
        self.poll_channels_with_fallback(self.db.list_due_normal_channels(Utc::now())?)
            .await
    }

    async fn poll_channels_with_fallback(&self, channels: Vec<Channel>) -> Result<usize> {
        let mut count = 0;
        let mut circuit_probes = 0;
        for c in channels {
            if let Some(open_until) = self.rss_circuit_open_until()? {
                if circuit_probes >= RSS_CIRCUIT_PROBES {
                    self.db.defer_channel_poll_until(c.id, open_until)?;
                    continue;
                }
                circuit_probes += 1;
            }
            match self.poll_channel_with_fallback(c.id, true, true).await {
                Ok(n) => count += n,
                Err(e) => {
                    let detail = format!("{e:#}");
                    tracing::warn!(channel=%c.name,error=%detail,"频道轮询失败");
                }
            }
        }
        Ok(count)
    }

    /// Data API 正常时 RSS 仅作为低频探针；失败不立即拉起 yt-dlp。yt-dlp 深度
    /// 校对只在每日 API 深扫失败或 Data API 整体不可用时运行。
    pub async fn poll_rss_probes(&self, limit: usize) -> Result<usize> {
        self.poll_rss_channels(self.db.list_due_channels(Utc::now())?, Some(limit))
            .await
    }

    pub async fn poll_normal_rss_probes(&self, limit: usize) -> Result<usize> {
        self.poll_rss_channels(self.db.list_due_normal_channels(Utc::now())?, Some(limit))
            .await
    }

    pub async fn poll_priority_rss(&self) -> Result<usize> {
        self.poll_rss_channels(self.db.list_due_priority_channels(Utc::now())?, None)
            .await
    }

    async fn poll_rss_channels(
        &self,
        channels: Vec<Channel>,
        limit: Option<usize>,
    ) -> Result<usize> {
        let mut count = 0;
        for channel in channels.into_iter().take(limit.unwrap_or(usize::MAX)) {
            match self
                .poll_channel_with_fallback(channel.id, true, false)
                .await
            {
                Ok(discovered) => count += discovered,
                Err(error) => tracing::warn!(
                    channel = %channel.name,
                    error = %error,
                    "RSS 探针失败"
                ),
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
                    let detail = format!("{e:#}");
                    self.db.mark_channel_reconciled(c.id, Some(&detail))?;
                    tracing::warn!(channel=%c.name,error=%detail,"频道校对失败");
                }
            }
        }
        Ok(count)
    }

    /// 每日深扫优先使用 playlistItems.list(maxResults=50)。配额降级的第一阶段会
    /// 主动停掉这项非关键扫描；无 key、配额完全不可用或 API 请求失败时，才调用
    /// 保留下来的 yt-dlp reconcile 路径。
    pub async fn deep_scan_all(&self) -> Result<usize> {
        let Some(api) = self.data_api.as_ref() else {
            tracing::warn!("Data API 未配置，每日深扫回落到 yt-dlp 校对");
            return self.reconcile_all().await;
        };
        let policy = api.quota_policy()?;
        match policy.degradation {
            QuotaDegradation::SkipDeepScan
            | QuotaDegradation::ExtendCold
            | QuotaDegradation::NarrowHot => {
                tracing::debug!(used = policy.used, "配额降级已停止本日 Data API 深扫");
                return Ok(0);
            }
            QuotaDegradation::FallbackOnly => {
                tracing::warn!(
                    used = policy.used,
                    reset_at = %policy.reset_at,
                    "Data API 配额不可用，每日深扫回落到 yt-dlp 校对"
                );
                return self.reconcile_all().await;
            }
            QuotaDegradation::Normal => {}
        }

        if let Err(error) = self.refresh_upload_playlists(api).await {
            tracing::warn!(error = %error, "Data API 深扫初始化失败，回落到 yt-dlp 校对");
            return self.reconcile_all().await;
        }

        let mut added = 0;
        for channel in self
            .db
            .list_channels()?
            .into_iter()
            .filter(|channel| channel.enabled)
        {
            let current_policy = api.quota_policy()?;
            if current_policy.degradation != QuotaDegradation::Normal {
                tracing::debug!(
                    used = current_policy.used,
                    "每日 API 深扫途中达到降级阈值，停止剩余频道"
                );
                break;
            }
            let Some(playlist_id) = channel.uploads_playlist_id.as_deref() else {
                tracing::warn!(
                    channel = %channel.name,
                    "API 深扫缺少 uploads 播放列表，回落到 yt-dlp"
                );
                match self.reconcile_channel(channel.id, &channel.url).await {
                    Ok(count) => {
                        added += count;
                        self.db.mark_channel_reconciled(channel.id, None)?;
                    }
                    Err(error) => {
                        let detail = format!("{error:#}");
                        self.db.mark_channel_reconciled(channel.id, Some(&detail))?;
                        tracing::warn!(channel = %channel.name, error = %detail, "yt-dlp 深扫兜底失败");
                    }
                }
                continue;
            };
            match api
                .playlist_items(playlist_id, self.config.monitor.data_api_max_results, None)
                .await
            {
                Ok(page) => {
                    added += self.persist_playlist_candidates(&channel, page.videos)?;
                    if let Some(etag) = page.etag.as_deref() {
                        self.db.set_channel_data_api_etag(channel.id, etag)?;
                    }
                    self.db.mark_channel_reconciled(channel.id, None)?;
                }
                Err(api_error) => {
                    tracing::warn!(
                        channel = %channel.name,
                        error = %api_error,
                        "Data API 深扫失败，回落到 yt-dlp 校对"
                    );
                    match self.reconcile_channel(channel.id, &channel.url).await {
                        Ok(count) => {
                            added += count;
                            self.db.mark_channel_reconciled(channel.id, None)?;
                        }
                        Err(error) => {
                            let detail = format!("API: {api_error}; yt-dlp: {error:#}");
                            self.db.mark_channel_reconciled(channel.id, Some(&detail))?;
                            tracing::warn!(channel = %channel.name, error = %detail, "API 与 yt-dlp 深扫均失败");
                        }
                    }
                }
            }
        }
        Ok(added)
    }

    async fn reconcile_channel(&self, id: i64, url: &str) -> Result<usize> {
        let baseline = self.db.channel_baseline(id)?;
        let normalized_url = normalize_channel_url(url)?;
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--flat-playlist",
            "--playlist-end",
            &self.config.monitor.reconcile_limit.to_string(),
            "--dump-single-json",
            "--skip-download",
            &normalized_url,
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
            if !is_youtube_video_id(video_id) {
                tracing::debug!(video_id, "校对条目不是 YouTube 视频，跳过");
                continue;
            }
            let link = canonical_youtube_video_url(video_id)?;
            let title = e.get("title").and_then(Value::as_str);
            let timestamp = e
                .get("timestamp")
                .or_else(|| e.get("release_timestamp"))
                .and_then(Value::as_i64);
            if matches!(reconcile_after_baseline(baseline, timestamp), Some(false)) {
                break;
            }
            if self.db.get_job_by_video_id(video_id)?.is_some()
                || self
                    .db
                    .is_over_duration_video(video_id, self.config.youtube.max_duration_seconds)?
            {
                continue;
            }
            if self.db.insert_video_candidate(NewVideoCandidate {
                channel_id: Some(id),
                video_id,
                url: &link,
                title,
                published_at: timestamp.and_then(|value| DateTime::<Utc>::from_timestamp(value, 0)),
                source: CandidateSource::Ytdlp,
            })? {
                added += 1;
            }
        }
        Ok(added)
    }

    async fn fetch_feed_bytes(&self, url: &str) -> std::result::Result<Vec<u8>, FeedFetchError> {
        for retry in 0..=RSS_MAX_RETRIES {
            let result = match self.client.get(url).send().await {
                Ok(response) if response.status().is_success() => {
                    match read_response_body(response, RSS_BODY_LIMIT).await {
                        Ok(bytes) => Ok(bytes),
                        Err(ResponseBodyError::Request(source)) => {
                            Err(FeedFetchError::Request { source })
                        }
                        Err(ResponseBodyError::TooLarge { limit }) => {
                            Err(FeedFetchError::BodyTooLarge { limit })
                        }
                    }
                }
                Ok(response) => {
                    let status = response.status();
                    let retry_at = (status == StatusCode::TOO_MANY_REQUESTS)
                        .then(|| {
                            response
                                .headers()
                                .get(RETRY_AFTER)
                                .and_then(|value| parse_retry_after(value, Utc::now()))
                        })
                        .flatten();
                    Err(FeedFetchError::Http { status, retry_at })
                }
                Err(source) => Err(FeedFetchError::Request { source }),
            };
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(error) if retry < RSS_MAX_RETRIES && error.retryable() => {
                    let delay = jittered_backoff(
                        RSS_RETRY_BASE,
                        u32::try_from(retry).unwrap_or(u32::MAX),
                        random_jitter_factor(),
                    );
                    tracing::debug!(attempt = retry + 1, ?delay, url, error = %error, "RSS 暂时失败，退避后重试");
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("RSS 重试循环至少执行一次")
    }

    pub async fn poll_channel(&self, id: i64, enqueue: bool) -> Result<usize> {
        self.poll_channel_with_fallback(id, enqueue, true).await
    }

    async fn poll_channel_with_fallback(
        &self,
        id: i64,
        enqueue: bool,
        allow_fallback: bool,
    ) -> Result<usize> {
        let failures = self.db.channel_consecutive_failures(id)?;
        let execution = match self
            .poll_channel_unrecorded(id, enqueue, allow_fallback)
            .await
        {
            Ok(execution) => execution,
            Err(error) => PollExecution {
                result: Err(error),
                rss_failed: false,
                retry_at: None,
            },
        };
        let detail = execution
            .result
            .as_ref()
            .err()
            .map(|error| format!("{error:#}"));
        let failed = execution.rss_failed || execution.result.is_err();
        let now = Utc::now();
        let next_poll_at = if let Some(retry_at) = execution.retry_at {
            retry_at
        } else if failed {
            let base = Duration::from_secs(self.config.monitor.poll_seconds.max(1));
            let delay = jittered_backoff(base, failures, random_jitter_factor())
                .min(CHANNEL_FAILURE_BACKOFF_CAP);
            now + chrono::Duration::from_std(delay)?
        } else {
            let channel = self.db.channel(id)?;
            now + chrono::Duration::from_std(rss_poll_interval(&self.config, &channel, now))?
        };
        self.db
            .finish_channel_poll(id, detail.as_deref(), failed, next_poll_at)?;
        execution.result
    }

    async fn poll_channel_unrecorded(
        &self,
        id: i64,
        enqueue: bool,
        allow_fallback: bool,
    ) -> Result<PollExecution> {
        let url = self.db.channel_feed(id)?;
        let baseline = self.db.channel_baseline(id)?;
        let bytes = match self.fetch_feed_bytes(&url).await {
            Ok(bytes) => bytes,
            Err(error) => {
                let retry_at = error.retry_at();
                let record_result = self.record_rss_sample(true);
                let result = match record_result {
                    Ok(()) if allow_fallback => self.fallback_poll_channel(id, error.into()).await,
                    Ok(()) => Err(anyhow::Error::new(error).context("RSS 探针不可用")),
                    Err(record_error) => Err(record_error.context("记录 RSS 熔断样本失败")),
                };
                return Ok(PollExecution {
                    result,
                    rss_failed: true,
                    retry_at,
                });
            }
        };
        let feed = match parser::parse(bytes.as_slice()).context("YouTube RSS 格式无效") {
            Ok(feed) => feed,
            Err(error) => {
                let result = match self.record_rss_sample(true) {
                    Ok(()) => Err(error),
                    Err(record_error) => Err(record_error.context(error.to_string())),
                };
                return Ok(PollExecution {
                    result,
                    rss_failed: true,
                    retry_at: None,
                });
            }
        };
        if let Err(error) = self.record_rss_sample(false) {
            return Ok(PollExecution {
                result: Err(error.context("记录 RSS 熔断样本失败")),
                rss_failed: false,
                retry_at: None,
            });
        }
        let mut added = 0;
        for e in feed.entries {
            let published = e.published.or(e.updated);
            if enqueue && baseline.is_some() && published <= baseline {
                continue;
            }
            let video_id = e.id.strip_prefix("yt:video:").unwrap_or(&e.id).to_string();
            if !is_youtube_video_id(&video_id) {
                tracing::warn!(video_id, "RSS 返回非法 video_id，跳过");
                continue;
            }
            let link = canonical_youtube_video_url(&video_id)?;
            let title = e.title.as_ref().map(|x| x.content.as_str());
            if enqueue
                && (self.db.get_job_by_video_id(&video_id)?.is_some()
                    || self.db.is_over_duration_video(
                        &video_id,
                        self.config.youtube.max_duration_seconds,
                    )?)
            {
                continue;
            }
            if enqueue
                && self.db.insert_video_candidate(NewVideoCandidate {
                    channel_id: Some(id),
                    video_id: &video_id,
                    url: &link,
                    title,
                    published_at: published,
                    source: CandidateSource::Rss,
                })?
            {
                added += 1;
            }
        }
        Ok(PollExecution {
            result: Ok(added),
            rss_failed: false,
            retry_at: None,
        })
    }

    /// 领取 yt-dlp 回退名额。锁只覆盖纯内存判定，不跨分钟级 await。
    fn claim_fallback_slot(&self, id: i64) -> FallbackClaim {
        self.fallback_limiter
            .lock()
            .unwrap()
            .claim(id, Instant::now())
    }

    fn record_rss_sample(&self, failed: bool) -> Result<()> {
        let should_open = self
            .rss_circuit
            .lock()
            .unwrap()
            .record(failed, Instant::now());
        if should_open && self.rss_circuit_open_until()?.is_none() {
            let open_until = Utc::now() + RSS_CIRCUIT_DURATION;
            self.db
                .set_discovery_state(RSS_CIRCUIT_STATE_KEY, &open_until.to_rfc3339())?;
            tracing::warn!(%open_until, "RSS 全局失败率超过阈值，熔断十分钟");
        }
        Ok(())
    }

    fn rss_circuit_open_until(&self) -> Result<Option<DateTime<Utc>>> {
        let Some(raw) = self.db.get_discovery_state(RSS_CIRCUIT_STATE_KEY)? else {
            return Ok(None);
        };
        let open_until = match DateTime::parse_from_rfc3339(&raw) {
            Ok(value) => value.with_timezone(&Utc),
            Err(error) => {
                tracing::warn!(value = %raw, error = %error, "RSS 熔断时间无效，清除状态");
                self.db.delete_discovery_state(RSS_CIRCUIT_STATE_KEY)?;
                return Ok(None);
            }
        };
        if open_until <= Utc::now() {
            self.db.delete_discovery_state(RSS_CIRCUIT_STATE_KEY)?;
            Ok(None)
        } else {
            Ok(Some(open_until))
        }
    }

    /// 处理一批到期候选。发现源只负责写表，所有元数据与策略判断都收敛在这里。
    pub async fn gate_pending_candidates(&self, limit: usize) -> Result<GateOutcome> {
        let mut candidates = self.db.due_video_candidates(Utc::now(), limit)?;
        let processed = candidates.len();
        let mut promoted = 0;
        let mut pending = Vec::with_capacity(candidates.len());
        for candidate in candidates.drain(..) {
            if self.db.get_job_by_video_id(&candidate.video_id)?.is_some() {
                if self.db.promote_video_candidate(
                    &candidate,
                    candidate.title.as_deref(),
                    candidate.published_at,
                )? {
                    promoted += 1;
                }
            } else {
                pending.push(candidate);
            }
        }
        if pending.is_empty() {
            return Ok(GateOutcome {
                processed,
                promoted,
            });
        }

        if let Some(api) = self.data_api.as_ref() {
            let video_ids = pending
                .iter()
                .map(|candidate| candidate.video_id.clone())
                .collect::<Vec<_>>();
            match api.videos(&video_ids).await {
                Ok(mut metadata_by_id) => {
                    for candidate in pending {
                        if let Some(metadata) = metadata_by_id.remove(&candidate.video_id) {
                            if self.gate_candidate_with_metadata(&candidate, metadata)? {
                                promoted += 1;
                            }
                        } else {
                            self.defer_gate_error(
                                &candidate,
                                "videos.list 未返回该视频，稍后复查",
                            )?;
                        }
                    }
                    return Ok(GateOutcome {
                        processed,
                        promoted,
                    });
                }
                Err(error) => {
                    if error.is_quota_exceeded() {
                        tracing::warn!(error = %error, "Data API 配额不可用，gate 降级到 yt-dlp");
                    } else {
                        tracing::warn!(error = %error, "Data API 元数据不可用，gate 降级到 yt-dlp");
                    }
                }
            }
        }

        for candidate in pending {
            if self.gate_candidate_with_ytdlp(&candidate).await? {
                promoted += 1;
            }
        }
        Ok(GateOutcome {
            processed,
            promoted,
        })
    }

    async fn gate_candidate_with_ytdlp(&self, candidate: &VideoCandidate) -> Result<bool> {
        let metadata = match self.fetch_metadata(&candidate.url).await {
            Ok((metadata, _, _)) => metadata,
            Err(error) if is_live_content_pending(&error) => {
                self.defer_gate_error(candidate, &error.to_string())?;
                return Ok(false);
            }
            Err(error) if exceeds_duration_limit(&error) => {
                self.reject_over_duration(candidate, &error)?;
                return Ok(false);
            }
            Err(error) => {
                self.defer_gate_error(candidate, &format!("元数据获取失败: {error:#}"))?;
                tracing::warn!(video_id = %candidate.video_id, error = %error, "候选元数据获取失败，延后重试");
                return Ok(false);
            }
        };
        self.gate_candidate_with_metadata(candidate, metadata)
    }

    fn gate_candidate_with_metadata(
        &self,
        candidate: &VideoCandidate,
        metadata: VideoMetadata,
    ) -> Result<bool> {
        let source_language_mismatch =
            metadata
                .default_audio_language
                .as_deref()
                .is_some_and(|actual| {
                    !source_language_matches(&self.config.translation.source_lang, actual)
                });
        self.db.mark_video_candidate_source_language(
            &candidate.video_id,
            metadata.default_audio_language.as_deref(),
            source_language_mismatch,
        )?;
        if metadata
            .live_status
            .as_deref()
            .is_some_and(|status| LIVE_STATUS_NOT_READY.contains(&status))
        {
            self.defer_gate_error(candidate, LIVE_CONTENT_PENDING_PREFIX)?;
            return Ok(false);
        }
        if let Err(error) =
            validate_duration(metadata.duration, self.config.youtube.max_duration_seconds)
        {
            self.reject_over_duration(candidate, &error)?;
            return Ok(false);
        }

        if is_backlog_replay(&metadata, self.db.live_replay_cutoff()?) {
            self.db
                .reject_video_candidate(&candidate.video_id, "历史直播回放，不自动入队")?;
            tracing::info!(video_id = %candidate.video_id, "历史直播回放，不自动入队");
            return Ok(false);
        }

        let published_at = metadata
            .timestamp
            .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))
            .or(candidate.published_at);
        if let Some(channel_id) = candidate.channel_id
            && let Some(baseline) = self.db.channel_baseline(channel_id)?
        {
            match published_at {
                Some(published_at) if published_at > baseline => {}
                Some(_) => {
                    self.db.reject_video_candidate(
                        &candidate.video_id,
                        "候选发布时间不晚于频道 baseline",
                    )?;
                    return Ok(false);
                }
                None => {
                    self.db.reject_video_candidate(
                        &candidate.video_id,
                        "候选缺少发布时间，拒绝补录历史视频",
                    )?;
                    return Ok(false);
                }
            }
        }

        // 语言告警和硬闸门放在最后：对一个马上要因为 baseline／时长／历史回放
        // 被拒的候选报「语言不符」毫无意义，而首轮扫描里这类候选占 99%（线上
        // 1384 条里 1379 条被拒），早报会刷出几百条 WARN 把真问题淹掉。
        // 走到这里说明候选确实要入队了，此时语言不符才值得关注。
        if source_language_mismatch {
            let actual = metadata
                .default_audio_language
                .as_deref()
                .expect("mismatch=true 时必有语言值");
            tracing::warn!(
                video_id = %candidate.video_id,
                expected = %self.config.translation.source_lang,
                actual,
                hard_gate = self.config.translation.enforce_source_lang,
                "即将入队的视频默认音频语言与配置源语言不符"
            );
            if self.config.translation.enforce_source_lang {
                self.db.reject_video_candidate(
                    &candidate.video_id,
                    &format!(
                        "源语言不匹配: defaultAudioLanguage={actual}, expected={}",
                        self.config.translation.source_lang
                    ),
                )?;
                return Ok(false);
            }
        }

        self.db
            .promote_video_candidate(candidate, Some(&metadata.title), published_at)
    }

    fn defer_gate_error(&self, candidate: &VideoCandidate, error: &str) -> Result<()> {
        let next_gate_at = Utc::now() + chrono::Duration::from_std(RECHECK_INTERVAL)?;
        self.db
            .defer_video_candidate(&candidate.video_id, next_gate_at, error)?;
        tracing::info!(video_id = %candidate.video_id, %next_gate_at, "候选延后复查");
        Ok(())
    }

    fn reject_over_duration(
        &self,
        candidate: &VideoCandidate,
        error: &anyhow::Error,
    ) -> Result<()> {
        self.db.record_over_duration_video(
            &candidate.video_id,
            candidate.channel_id,
            self.config.youtube.max_duration_seconds,
            &error.to_string(),
        )?;
        self.db
            .reject_video_candidate(&candidate.video_id, &error.to_string())?;
        tracing::info!(video_id = %candidate.video_id, error = %error, "候选超过时长上限，持久化拒绝");
        Ok(())
    }

    /// RSS 拉取失败时回退到 yt-dlp 频道列表（带每频道冷却，防止高频拉取）。
    async fn fallback_poll_channel(&self, id: i64, error: anyhow::Error) -> Result<usize> {
        match self.claim_fallback_slot(id) {
            FallbackClaim::Allowed => {}
            FallbackClaim::ChannelCooldown => {
                return Err(error).context("RSS 仍不可用，单频道 yt-dlp 回退冷却中");
            }
            FallbackClaim::GlobalCircuitOpen => {
                return Err(error).context("RSS 大面积异常，yt-dlp 全局回退熔断中");
            }
        }
        tracing::warn!(
            channel_id = id,
            error = %error,
            "RSS 重试耗尽，回退 yt-dlp 频道列表"
        );
        self.reconcile_channel(id, &self.db.channel_url(id)?).await
    }

    pub async fn fetch_metadata(&self, url: &str) -> Result<(VideoMetadata, u64, i64)> {
        let (requested_video_id, canonical_url) = canonicalize_youtube_video_url(url)?;
        let mut cmd = ytdlp_command(&self.config.youtube);
        cmd.args([
            "--print",
            VIDEO_METADATA_TEMPLATE,
            "--skip-download",
            "--no-playlist",
            &canonical_url,
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
        // 该值会直接成为下载目录组件；即使输入 URL 已校验，也不能信任 yt-dlp 输出。
        let video_id = v
            .get("id")
            .and_then(Value::as_str)
            .context("yt-dlp 未返回 YouTube video ID")?;
        validate_youtube_video_id(video_id)?;
        anyhow::ensure!(
            video_id == requested_video_id,
            "yt-dlp 返回的 video ID 与输入 URL 不一致: {video_id} != {requested_video_id}"
        );
        let live = v
            .get("live_status")
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok((
            VideoMetadata {
                id: video_id.into(),
                url: canonical_url.clone(),
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
                webpage_url: Some(canonical_url),
                live_status: live,
                default_audio_language: None,
            },
            out.peak_rss_kib,
            out.duration_ms,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn ytdlp_command_uses_web_creator_for_every_youtube_call() {
        let cmd = ytdlp_command(&YoutubeConfig::default());
        let args = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert!(
            args.windows(2)
                .any(|pair| { pair[0] == "--extractor-args" && pair[1] == YOUTUBE_EXTRACTOR_ARGS })
        );
    }

    async fn mock_response(
        status: u16,
        extra_headers: &str,
        body: &str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let headers = extra_headers.to_string();
        let body = body.to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 {status} Test\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (format!("http://{address}/feed"), server)
    }

    async fn mock_json_sequence(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 16 * 1024];
                let _ = socket.read(&mut request).await.unwrap();
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}/youtube/v3"), server)
    }

    fn empty_ytdlp(dir: &std::path::Path) -> String {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("fake-yt-dlp");
        std::fs::write(&path, "#!/bin/sh\nprintf '%s\\n' '{\"entries\":[]}'\n").unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn gate_ytdlp(dir: &std::path::Path) -> String {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("gate-yt-dlp");
        std::fs::write(
            &path,
            r#"#!/bin/sh
url=""
for arg in "$@"; do url="$arg"; done
case "$url" in
  *livevideo01*) printf '%s\n' '{"_type":"video","id":"livevideo01","title":"live","duration":10,"timestamp":2000000000,"live_status":"is_live"}' ;;
  *longvideo01*) printf '%s\n' '{"_type":"video","id":"longvideo01","title":"long","duration":8000,"timestamp":2000000000,"live_status":"not_live"}' ;;
  *oldreplay01*) printf '%s\n' '{"_type":"video","id":"oldreplay01","title":"old replay","duration":10,"timestamp":1600000000,"live_status":"was_live"}' ;;
  *baseline001*) printf '%s\n' '{"_type":"video","id":"baseline001","title":"old normal","duration":10,"timestamp":1600000000,"live_status":"not_live"}' ;;
  *deferred001*) printf '%s\n' '{"_type":"video","id":"deferred001","title":"deferred","duration":10,"timestamp":2000000000,"live_status":"not_live"}' ;;
  *) printf '%s\n' '{"_type":"video","id":"normalvid01","title":"normal","duration":10,"timestamp":2000000000,"live_status":"not_live"}' ;;
esac
"#,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn discovery_only_ytdlp(dir: &std::path::Path) -> String {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join("discovery-yt-dlp");
        std::fs::write(
            &path,
            r#"#!/bin/sh
case " $* " in
  *" --flat-playlist "*) printf '%s\n' '{"entries":[{"id":"normalvid01","title":"normal","url":"https://example.invalid/forged"}]}' ;;
  *) exit 97 ;;
esac
"#,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn fixed_metadata_ytdlp(dir: &std::path::Path, name: &str, video_id: &str) -> String {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join(name);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' '{{\"_type\":\"video\",\"id\":\"{video_id}\",\"title\":\"test\",\"duration\":10,\"live_status\":\"not_live\"}}'\n"
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn add_test_channel(db: &Database, feed_url: &str, suffix: &str) -> i64 {
        db.add_channel(
            &format!("UC-{suffix}"),
            suffix,
            &format!("https://www.youtube.com/@{suffix}"),
            feed_url,
            TransferMode::Direct,
        )
        .unwrap()
    }

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
        let path = dir.path().join("x.db");
        let db = Database::open(&path).unwrap();
        db.insert_video_candidate(NewVideoCandidate {
            video_id: "abcdefghijk",
            channel_id: None,
            url: "https://www.youtube.com/watch?v=abcdefghijk",
            title: None,
            published_at: None,
            source: CandidateSource::Rss,
        })
        .unwrap();
        let next_gate_at = Utc::now() + chrono::Duration::from_std(RECHECK_INTERVAL).unwrap();
        db.defer_video_candidate("abcdefghijk", next_gate_at, "直播尚未就绪")
            .unwrap();
        assert!(db.due_video_candidates(Utc::now(), 10).unwrap().is_empty());
        drop(db);

        // 延后时间跨重启保留，到点后重新进入 gate，而不是永久拉黑。
        let reopened = Database::open(&path).unwrap();
        assert!(
            reopened
                .due_video_candidates(next_gate_at - chrono::Duration::milliseconds(1), 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            reopened.due_video_candidates(next_gate_at, 10).unwrap()[0].video_id,
            "abcdefghijk"
        );
    }

    #[test]
    fn youtube_urls_and_video_ids_are_strictly_canonicalized() {
        const VIDEO_ID: &str = "dQw4w9WgXcQ";
        const CANONICAL: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";

        assert!(is_youtube_video_id("P3ncIFdXrO0"));
        assert!(is_youtube_video_id("-lfxKdAm3vA"));
        assert!(!is_youtube_video_id("UCoFbVpsJ-XP8zl77ntGWBqw"));
        assert!(!is_youtube_video_id("playlist"));

        for input in [
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PL123#chapter",
            "http://youtu.be/dQw4w9WgXcQ?t=43",
            "https://m.youtube.com/shorts/dQw4w9WgXcQ?feature=share",
            "https://music.youtube.com/live/dQw4w9WgXcQ",
            "https://youtube.com/embed/dQw4w9WgXcQ",
        ] {
            let (video_id, url) = canonicalize_youtube_video_url(input).unwrap();
            assert_eq!(video_id, VIDEO_ID);
            assert_eq!(url, CANONICAL);
        }

        for input in [
            "dQw4w9WgXcQ",
            "ftp://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "https://example.com/watch?v=dQw4w9WgXcQ",
            "https://youtube.com.evil.example/watch?v=dQw4w9WgXcQ",
            "https://youtu.be.evil.example/dQw4w9WgXcQ",
            "https://www.youtube.com:444/watch?v=dQw4w9WgXcQ",
            "https://www.youtube.com/watch?v=too-short",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&v=P3ncIFdXrO0",
            "https://www.youtube.com/playlist?list=PL123",
            "https://youtu.be/dQw4w9WgXcQ/extra",
        ] {
            assert!(
                canonicalize_youtube_video_url(input).is_err(),
                "非法 URL 应被拒绝: {input}"
            );
        }

        assert_eq!(
            normalize_channel_url("http://m.youtube.com/@WiiBrawlStars/?view=0").unwrap(),
            "https://www.youtube.com/@WiiBrawlStars/videos"
        );
        for tab in ["videos", "shorts", "streams"] {
            let url = format!("https://www.youtube.com/@channel/{tab}");
            assert_eq!(normalize_channel_url(&url).unwrap(), url);
        }
        for input in [
            "https://example.com/@channel",
            "https://youtube.com.evil.example/@channel",
            "https://youtu.be/dQw4w9WgXcQ",
        ] {
            assert!(normalize_channel_url(input).is_err());
        }
    }

    #[tokio::test]
    async fn manual_queue_persists_only_the_canonical_url() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.youtube.yt_dlp = fixed_metadata_ytdlp(dir.path(), "canonical-yt-dlp", "dQw4w9WgXcQ");
        config.runtime.download_dir = dir.path().join("downloads");
        let db = Database::open(&dir.path().join("canonical.db")).unwrap();
        let monitor = Monitor::new(config.clone(), db).unwrap();

        let outcome = monitor
            .enqueue_video("http://youtu.be/dQw4w9WgXcQ?t=43", TransferMode::Direct)
            .await
            .unwrap();
        assert_eq!(outcome.job.video_id, "dQw4w9WgXcQ");
        assert_eq!(
            outcome.job.url,
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        );
        assert!(!config.runtime.download_dir.exists());
    }

    #[tokio::test]
    async fn invalid_operator_url_is_rejected_without_creating_a_directory() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("unexpected-directory");
        let script = dir.path().join("must-not-run-yt-dlp");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nmkdir -p \"{}\"\nprintf '%s\\n' '{{\"_type\":\"video\",\"id\":\"dQw4w9WgXcQ\",\"title\":\"test\"}}'\n",
                marker.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();

        let mut config = Config::default();
        config.youtube.yt_dlp = script.to_string_lossy().into_owned();
        config.runtime.download_dir = dir.path().join("downloads");
        let db = Database::open(&dir.path().join("invalid-input.db")).unwrap();
        let monitor = Monitor::new(config.clone(), db.clone()).unwrap();
        let error = monitor
            .enqueue_video(
                "https://example.invalid/watch?v=dQw4w9WgXcQ",
                TransferMode::Direct,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("域名"));
        assert!(db.get_job_by_video_id("dQw4w9WgXcQ").unwrap().is_none());
        assert!(!marker.exists(), "非法输入不应启动 yt-dlp");
        assert!(!config.runtime.download_dir.exists());
    }

    #[tokio::test]
    async fn metadata_id_is_revalidated_before_it_can_become_a_directory_component() {
        let dir = tempfile::tempdir().unwrap();
        let download_dir = dir.path().join("downloads");
        for (name, returned_id, expected_error) in [
            ("unsafe-id-yt-dlp", "../escape", "必须是 11 位"),
            ("mismatch-id-yt-dlp", "normalvid01", "不一致"),
        ] {
            let mut config = Config::default();
            config.youtube.yt_dlp = fixed_metadata_ytdlp(dir.path(), name, returned_id);
            config.runtime.download_dir = download_dir.clone();
            let db = Database::open(&dir.path().join(format!("{name}.db"))).unwrap();
            let monitor = Monitor::new(config, db).unwrap();
            let error = monitor
                .fetch_metadata("https://youtu.be/dQw4w9WgXcQ")
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(expected_error),
                "返回 ID {returned_id:?} 的错误不明确: {error}"
            );
            assert!(!download_dir.exists(), "非法 ID 不得产生下载目录");
        }
    }

    #[tokio::test]
    async fn rss_fetch_retries_before_succeeding() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for status in [500, 502, 200] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 1024];
                let _ = socket.read(&mut request).await.unwrap();
                let body = if status == 200 { "feed" } else { "error" };
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("rss.db")).unwrap();
        let monitor = Monitor::new(Config::default(), db).unwrap();
        let bytes = monitor
            .fetch_feed_bytes(&format!("http://{address}/feed"))
            .await
            .unwrap();
        assert_eq!(bytes, b"feed");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_rss_response_is_rejected() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                RSS_BODY_LIMIT + 1
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("large-rss.db")).unwrap();
        let monitor = Monitor::new(Config::default(), db).unwrap();
        let error = monitor
            .fetch_feed_bytes(&format!("http://{address}/feed"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            FeedFetchError::BodyTooLarge {
                limit: RSS_BODY_LIMIT
            }
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rss_404_is_not_retried() {
        let (url, server) = mock_response(404, "", "missing").await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("rss.db")).unwrap();
        let monitor = Monitor::new(Config::default(), db).unwrap();
        let error = monitor.fetch_feed_bytes(&url).await.unwrap_err();
        assert!(matches!(
            error,
            FeedFetchError::Http {
                status: StatusCode::NOT_FOUND,
                ..
            }
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rss_429_honors_retry_after() {
        let before = Utc::now();
        let (url, server) = mock_response(429, "Retry-After: 123\r\n", "limited").await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("rss.db")).unwrap();
        let monitor = Monitor::new(Config::default(), db).unwrap();
        let error = monitor.fetch_feed_bytes(&url).await.unwrap_err();
        let retry_at = error.retry_at().expect("应读取 Retry-After");
        let seconds = (retry_at - before).num_seconds();
        assert!((122..=123).contains(&seconds), "实际退避 {seconds}s");
        server.await.unwrap();
    }

    #[test]
    fn channel_backoff_has_jitter_and_is_persisted() {
        let base = Duration::from_secs(60);
        assert_eq!(jittered_backoff(base, 0, 0.5), Duration::from_secs(30));
        assert_eq!(jittered_backoff(base, 0, 1.5), Duration::from_secs(90));
        assert_eq!(jittered_backoff(base, 2, 1.0), Duration::from_secs(240));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("poll-state.db");
        let db = Database::open(&path).unwrap();
        let id = add_test_channel(&db, "https://example.invalid/feed", "persist");
        let next = Utc::now() + chrono::Duration::minutes(5);
        db.finish_channel_poll(id, Some("failed"), true, next)
            .unwrap();
        drop(db);

        let reopened = Database::open(&path).unwrap();
        let channel = reopened.list_channels().unwrap().remove(0);
        assert_eq!(channel.consecutive_failures, 1);
        assert_eq!(channel.last_error.as_deref(), Some("failed"));
        assert!(
            channel
                .next_poll_at
                .is_some_and(|stored| stored.timestamp_millis() == next.timestamp_millis())
        );
        assert!(
            reopened
                .list_due_channels(next - chrono::Duration::milliseconds(1))
                .unwrap()
                .is_empty()
        );
        assert_eq!(reopened.list_due_channels(next).unwrap()[0].id, id);
    }

    #[test]
    fn predictive_window_uses_timezone_fallback_and_rolling_history() {
        let mut config = Config::default();
        config.monitor.prediction_window_minutes = 60;
        let timezone: Tz = "America/New_York".parse().unwrap();
        let published = |month, day| {
            timezone
                .with_ymd_and_hms(2026, month, day, 10, 10, 0)
                .single()
                .unwrap()
                .with_timezone(&Utc)
        };
        let mut history = vec![
            published(1, 5),
            published(1, 12),
            published(1, 19),
            published(1, 26),
        ];
        let now = timezone
            .with_ymd_and_hms(2026, 7, 6, 10, 30, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc);

        let fallback = prediction_poll_decision(
            &config,
            &history,
            now,
            timezone,
            QuotaDegradation::Normal,
            false,
        );
        assert_eq!(fallback.mode, DataApiPollMode::InsufficientHistory);
        assert_eq!(fallback.interval, Duration::from_secs(5 * 60));

        // 第 5 条发布记录加入后不重启、不用缓存重建，下一次计算立即进入热窗。
        history.push(published(2, 2));
        let rolled = prediction_poll_decision(
            &config,
            &history,
            now,
            timezone,
            QuotaDegradation::Normal,
            false,
        );
        assert_eq!(rolled.mode, DataApiPollMode::PredictedHot);
        assert_eq!(rolled.interval, Duration::from_secs(60));

        // 冬夏令时下 UTC 小时不同；若忽略 runtime.timezone，这里会被误判为冷区。
        let wrong_timezone = prediction_poll_decision(
            &config,
            &history,
            now,
            chrono_tz::UTC,
            QuotaDegradation::Normal,
            false,
        );
        assert_eq!(wrong_timezone.mode, DataApiPollMode::PredictedCold);

        let just_before_window = timezone
            .with_ymd_and_hms(2026, 7, 6, 9, 50, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc);
        let boundary_wakeup = prediction_poll_decision(
            &config,
            &history,
            just_before_window,
            timezone,
            QuotaDegradation::Normal,
            false,
        );
        assert_eq!(boundary_wakeup.mode, DataApiPollMode::PredictedCold);
        assert_eq!(boundary_wakeup.interval, Duration::from_secs(10 * 60));

        let hot_window_edge = timezone
            .with_ymd_and_hms(2026, 7, 6, 10, 10, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            prediction_poll_decision(
                &config,
                &history,
                hot_window_edge,
                timezone,
                QuotaDegradation::Normal,
                false,
            )
            .mode,
            DataApiPollMode::PredictedHot
        );
        assert_eq!(
            prediction_poll_decision(
                &config,
                &history,
                hot_window_edge,
                timezone,
                QuotaDegradation::NarrowHot,
                false,
            )
            .mode,
            DataApiPollMode::PredictedCold
        );

        let cold_now = timezone
            .with_ymd_and_hms(2026, 7, 6, 14, 30, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc);
        let cold = prediction_poll_decision(
            &config,
            &history,
            cold_now,
            timezone,
            QuotaDegradation::ExtendCold,
            false,
        );
        assert_eq!(cold.mode, DataApiPollMode::PredictedCold);
        assert_eq!(cold.interval, Duration::from_secs(60 * 60));
    }

    #[test]
    fn rss_circuit_uses_sliding_failure_rate_threshold() {
        let start = Instant::now();
        let mut circuit = RssCircuitWindow::default();
        for index in 0..7 {
            assert!(!circuit.record(index < 5, start + Duration::from_secs(index)));
        }
        assert!(circuit.record(false, start + Duration::from_secs(7)));

        let mut below_threshold = RssCircuitWindow::default();
        for index in 0..8 {
            assert!(!below_threshold.record(index < 4, start + Duration::from_secs(index)));
        }
        // 窗口外的失败样本必须被淘汰，不能永久污染全局判定。
        assert!(!below_threshold.record(false, start + RSS_CIRCUIT_WINDOW));
    }

    #[tokio::test]
    async fn every_poll_exit_path_writes_channel_state() {
        const EMPTY_FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <id>test</id><title>test</title><updated>2026-08-15T00:00:00Z</updated>
</feed>"#;

        // 1. RSS 成功。
        let dir = tempfile::tempdir().unwrap();
        let (url, server) =
            mock_response(200, "Content-Type: application/atom+xml\r\n", EMPTY_FEED).await;
        let db = Database::open(&dir.path().join("success.db")).unwrap();
        let id = add_test_channel(&db, &url, "success");
        let monitor = Monitor::new(Config::default(), db.clone()).unwrap();
        assert_eq!(monitor.poll_channel(id, true).await.unwrap(), 0);
        server.await.unwrap();
        let state = db.list_channels().unwrap().remove(0);
        assert!(state.last_checked_at.is_some());
        assert!(state.last_error.is_none());

        // 2. RSS 返回无效 Atom。
        let dir = tempfile::tempdir().unwrap();
        let (url, server) = mock_response(200, "", "not atom").await;
        let db = Database::open(&dir.path().join("invalid.db")).unwrap();
        let id = add_test_channel(&db, &url, "invalid");
        let monitor = Monitor::new(Config::default(), db.clone()).unwrap();
        assert!(monitor.poll_channel(id, true).await.is_err());
        server.await.unwrap();
        let state = db.list_channels().unwrap().remove(0);
        assert!(state.last_checked_at.is_some());
        assert!(state.last_error.is_some());

        // 3. RSS 失败，但获准的 yt-dlp 回退成功；旧错误必须被清掉。
        let dir = tempfile::tempdir().unwrap();
        let (url, server) = mock_response(404, "", "missing").await;
        let db = Database::open(&dir.path().join("fallback-success.db")).unwrap();
        let id = add_test_channel(&db, &url, "fallback-success");
        db.mark_channel_checked(id, Some("old error")).unwrap();
        let mut config = Config::default();
        config.youtube.yt_dlp = empty_ytdlp(dir.path());
        let monitor = Monitor::new(config, db.clone()).unwrap();
        assert_eq!(monitor.poll_channel(id, true).await.unwrap(), 0);
        server.await.unwrap();
        let state = db.list_channels().unwrap().remove(0);
        assert!(state.last_checked_at.is_some());
        assert!(state.last_error.is_none());
        assert_eq!(state.consecutive_failures, 1);

        // 4. RSS 与回退都失败（这里用单频道冷却稳定触发）。
        let dir = tempfile::tempdir().unwrap();
        let (url, server) = mock_response(404, "", "missing").await;
        let db = Database::open(&dir.path().join("fallback-error.db")).unwrap();
        let id = add_test_channel(&db, &url, "fallback-error");
        let monitor = Monitor::new(Config::default(), db.clone()).unwrap();
        assert_eq!(monitor.claim_fallback_slot(id), FallbackClaim::Allowed);
        assert!(monitor.poll_channel(id, true).await.is_err());
        server.await.unwrap();
        let state = db.list_channels().unwrap().remove(0);
        assert!(state.last_checked_at.is_some());
        assert!(state.last_error.is_some());
        assert_eq!(state.consecutive_failures, 1);
    }

    #[test]
    fn fallback_limiter_enforces_channel_cooldown_and_global_circuit() {
        let start = Instant::now();
        let mut limiter = FallbackLimiter::default();
        assert_eq!(limiter.claim(1, start), FallbackClaim::Allowed);
        assert_eq!(
            limiter.claim(1, start + Duration::from_secs(1)),
            FallbackClaim::ChannelCooldown
        );
        assert_eq!(limiter.claim(2, start), FallbackClaim::Allowed);
        assert_eq!(limiter.claim(3, start), FallbackClaim::Allowed);
        assert_eq!(limiter.claim(4, start), FallbackClaim::GlobalCircuitOpen);
        assert_eq!(
            limiter.claim(4, start + FALLBACK_GLOBAL_WINDOW),
            FallbackClaim::Allowed
        );
    }

    #[test]
    fn fallback_priority_moves_never_attempted_channels_to_the_front() {
        let start = Instant::now();
        let mut limiter = FallbackLimiter::default();
        assert_eq!(limiter.claim(1, start), FallbackClaim::Allowed);
        assert_eq!(limiter.claim(2, start), FallbackClaim::Allowed);
        assert_eq!(limiter.claim(3, start), FallbackClaim::Allowed);

        let mut channel_ids = [1, 2, 3, 4, 5, 6];
        channel_ids.sort_by_key(|channel_id| limiter.last_attempt(*channel_id));
        assert_eq!(&channel_ids[..3], &[4, 5, 6]);
    }

    #[test]
    fn metadata_template_excludes_unbounded_format_and_fragment_lists() {
        for field in ["id", "title", "duration", "thumbnail", "live_status"] {
            assert!(VIDEO_METADATA_TEMPLATE.contains(field), "缺少字段: {field}");
        }
        assert!(!VIDEO_METADATA_TEMPLATE.contains("formats"));
        assert!(!VIDEO_METADATA_TEMPLATE.contains("fragments"));
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

    #[tokio::test]
    async fn discovery_sources_only_persist_candidates() {
        const FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">
  <id>test</id><title>test</title><updated>2035-01-01T00:00:00Z</updated>
  <entry>
    <id>yt:video:normalvid01</id><yt:videoId>normalvid01</yt:videoId>
    <title>normal</title>
    <link rel="alternate" href="https://example.invalid/forged" />
    <published>2035-01-01T00:00:00Z</published><updated>2035-01-01T00:00:00Z</updated>
  </entry>
</feed>"#;

        let dir = tempfile::tempdir().unwrap();
        let (url, server) =
            mock_response(200, "Content-Type: application/atom+xml\r\n", FEED).await;
        let db = Database::open(&dir.path().join("rss-candidate.db")).unwrap();
        let id = add_test_channel(&db, &url, "rss-candidate");
        let mut config = Config::default();
        config.youtube.yt_dlp = dir
            .path()
            .join("must-not-run")
            .to_string_lossy()
            .into_owned();
        let monitor = Monitor::new(config, db.clone()).unwrap();
        assert_eq!(monitor.poll_channel(id, true).await.unwrap(), 1);
        server.await.unwrap();
        let candidate = db.get_video_candidate("normalvid01").unwrap().unwrap();
        assert_eq!(candidate.source, CandidateSource::Rss);
        assert_eq!(candidate.url, "https://www.youtube.com/watch?v=normalvid01");
        assert_eq!(candidate.gate_state, crate::model::GateState::Pending);
        assert!(db.get_job_by_video_id("normalvid01").unwrap().is_none());

        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("ytdlp-candidate.db")).unwrap();
        let id = add_test_channel(&db, "https://example.invalid/feed", "ytdlp-candidate");
        let mut config = Config::default();
        config.youtube.yt_dlp = discovery_only_ytdlp(dir.path());
        let monitor = Monitor::new(config, db.clone()).unwrap();
        assert_eq!(
            monitor
                .reconcile_channel(id, "https://www.youtube.com/@candidate")
                .await
                .unwrap(),
            1
        );
        let candidate = db.get_video_candidate("normalvid01").unwrap().unwrap();
        assert_eq!(candidate.source, CandidateSource::Ytdlp);
        assert_eq!(candidate.url, "https://www.youtube.com/watch?v=normalvid01");
        assert!(db.get_job_by_video_id("normalvid01").unwrap().is_none());
    }

    #[tokio::test]
    async fn gate_worker_covers_every_state_transition() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("gate.db")).unwrap();
        let channel_id = add_test_channel(&db, "https://example.invalid/feed", "gate");
        for video_id in [
            "livevideo01",
            "longvideo01",
            "oldreplay01",
            "baseline001",
            "normalvid01",
            "deferred001",
        ] {
            db.insert_video_candidate(NewVideoCandidate {
                video_id,
                channel_id: Some(channel_id),
                url: &format!("https://www.youtube.com/watch?v={video_id}"),
                title: None,
                published_at: None,
                source: CandidateSource::Rss,
            })
            .unwrap();
        }
        db.defer_video_candidate(
            "deferred001",
            Utc::now() - chrono::Duration::seconds(1),
            "先前暂缓",
        )
        .unwrap();

        let mut config = Config::default();
        config.youtube.yt_dlp = gate_ytdlp(dir.path());
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), None, "http://127.0.0.1:9/youtube/v3")
                .unwrap();
        assert_eq!(
            monitor.gate_pending_candidates(20).await.unwrap().promoted,
            2
        );

        let state = |video_id| db.get_video_candidate(video_id).unwrap().unwrap();
        let live = state("livevideo01");
        assert_eq!(live.gate_state, crate::model::GateState::Deferred);
        assert!(live.next_gate_at.is_some());
        assert_eq!(live.gate_attempts, 1);

        for video_id in ["longvideo01", "oldreplay01", "baseline001"] {
            let candidate = state(video_id);
            assert_eq!(candidate.gate_state, crate::model::GateState::Rejected);
            assert_eq!(candidate.gate_attempts, 1);
        }
        assert!(db.is_over_duration_video("longvideo01", 7200).unwrap());

        for video_id in ["normalvid01", "deferred001"] {
            let candidate = state(video_id);
            assert_eq!(candidate.gate_state, crate::model::GateState::Promoted);
            assert!(db.get_job_by_video_id(video_id).unwrap().is_some());
        }
        assert_eq!(state("deferred001").gate_attempts, 2);
        assert!(db.due_video_candidates(Utc::now(), 20).unwrap().is_empty());
    }

    #[tokio::test]
    async fn data_api_discovers_upload_playlist_candidates() {
        let channels = r#"{"items":[{"id":"UC-data-api","contentDetails":{"relatedPlaylists":{"uploads":"UU-data-api"}}}]}"#;
        let playlist = r#"{"items":[{"snippet":{"title":"from api","publishedAt":"2035-01-01T00:00:00Z","resourceId":{"videoId":"normalvid01"}},"contentDetails":{"videoId":"normalvid01"}}]}"#;
        let (base_url, server) = mock_json_sequence(vec![(200, channels), (200, playlist)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("data-api.db")).unwrap();
        add_test_channel(&db, "https://example.invalid/feed", "data-api");
        let monitor =
            Monitor::new_with_data_api(Config::default(), db.clone(), Some("test-key"), &base_url)
                .unwrap();
        assert_eq!(monitor.poll_data_api().await.unwrap(), 1);
        server.await.unwrap();

        let channel = db.list_channels().unwrap().remove(0);
        assert_eq!(channel.uploads_playlist_id.as_deref(), Some("UU-data-api"));
        let candidate = db.get_video_candidate("normalvid01").unwrap().unwrap();
        assert_eq!(candidate.source, CandidateSource::DataApi);
        assert_eq!(candidate.url, "https://www.youtube.com/watch?v=normalvid01");
        assert_eq!(candidate.title.as_deref(), Some("from api"));
        assert!(db.get_job_by_video_id("normalvid01").unwrap().is_none());
        assert_eq!(
            db.get_discovery_state("quota_used_today")
                .unwrap()
                .as_deref(),
            Some("2")
        );
    }

    #[tokio::test]
    async fn data_api_refreshes_channel_added_after_recent_playlist_refresh() {
        let channels = r#"{"items":[{"id":"UC-late-channel","contentDetails":{"relatedPlaylists":{"uploads":"UU-late-channel"}}}]}"#;
        let playlist = r#"{"items":[{"snippet":{"title":"late upload","publishedAt":"2035-01-01T00:00:00Z","resourceId":{"videoId":"latevideo01"}},"contentDetails":{"videoId":"latevideo01"}}]}"#;
        let (base_url, server) = mock_json_sequence(vec![(200, channels), (200, playlist)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("late-channel.db")).unwrap();
        db.set_discovery_state(
            API_UPLOADS_REFRESHED_AT_KEY,
            &(Utc::now() - chrono::Duration::minutes(1)).to_rfc3339(),
        )
        .unwrap();
        add_test_channel(&db, "https://example.invalid/feed", "late-channel");
        let monitor =
            Monitor::new_with_data_api(Config::default(), db.clone(), Some("test-key"), &base_url)
                .unwrap();
        monitor
            .uploads_refreshed_this_process
            .store(true, Ordering::Release);

        assert_eq!(monitor.poll_data_api().await.unwrap(), 1);
        server.await.unwrap();

        let channel = db.list_channels().unwrap().remove(0);
        assert_eq!(
            channel.uploads_playlist_id.as_deref(),
            Some("UU-late-channel")
        );
        assert_eq!(
            db.get_video_candidate("latevideo01")
                .unwrap()
                .unwrap()
                .source,
            CandidateSource::DataApi
        );
    }

    #[tokio::test]
    async fn data_api_does_not_repeat_recent_refresh_for_still_missing_channel() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("missing-channel.db")).unwrap();
        add_test_channel(&db, "https://example.invalid/feed", "missing-channel");
        db.set_discovery_state(
            API_UPLOADS_REFRESHED_AT_KEY,
            &(Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
        )
        .unwrap();
        let monitor = Monitor::new_with_data_api(
            Config::default(),
            db.clone(),
            Some("test-key"),
            "http://127.0.0.1:9/youtube/v3",
        )
        .unwrap();
        monitor
            .uploads_refreshed_this_process
            .store(true, Ordering::Release);

        assert_eq!(monitor.poll_data_api().await.unwrap(), 0);
        assert!(db.list_channels().unwrap()[0].uploads_playlist_id.is_none());
        assert_eq!(
            db.get_discovery_state("quota_used_today")
                .unwrap()
                .as_deref(),
            Some("0")
        );
    }

    #[tokio::test]
    async fn failed_api_deep_scan_falls_back_to_ytdlp_reconcile() {
        let channels = r#"{"items":[{"id":"UC-deep-scan","contentDetails":{"relatedPlaylists":{"uploads":"UU-deep-scan"}}}]}"#;
        let (base_url, server) = mock_json_sequence(vec![
            (200, channels),
            (500, r#"{"error":{"message":"temporary"}}"#),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("deep-scan.db")).unwrap();
        add_test_channel(&db, "https://example.invalid/feed", "deep-scan");
        let mut config = Config::default();
        config.youtube.yt_dlp = discovery_only_ytdlp(dir.path());
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), Some("test-key"), &base_url).unwrap();

        assert_eq!(monitor.deep_scan_all().await.unwrap(), 1);
        server.await.unwrap();
        let candidate = db.get_video_candidate("normalvid01").unwrap().unwrap();
        assert_eq!(candidate.source, CandidateSource::Ytdlp);
    }

    #[tokio::test]
    async fn first_quota_degradation_stops_daily_deep_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("skip-deep-scan.db")).unwrap();
        add_test_channel(&db, "https://example.invalid/feed", "skip-deep-scan");
        db.set_discovery_state(
            "quota_used_today",
            &crate::youtube_api::QUOTA_SKIP_DEEP_SCAN_AT.to_string(),
        )
        .unwrap();
        db.set_discovery_state(
            "quota_reset_at",
            &(Utc::now() + chrono::Duration::hours(6)).to_rfc3339(),
        )
        .unwrap();
        let mut config = Config::default();
        config.youtube.yt_dlp = dir
            .path()
            .join("must-not-run")
            .to_string_lossy()
            .into_owned();
        let monitor = Monitor::new_with_data_api(
            config,
            db,
            Some("test-key"),
            "http://127.0.0.1:9/youtube/v3",
        )
        .unwrap();
        assert_eq!(monitor.deep_scan_all().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn data_api_metadata_drives_gate_without_ytdlp() {
        let videos = r#"{"items":[
          {"id":"normalvid01","snippet":{"title":"normal api","publishedAt":"2035-01-01T00:00:00Z","channelId":"UC-api-gate","liveBroadcastContent":"none"},"contentDetails":{"duration":"PT10S"}},
          {"id":"livevideo01","snippet":{"title":"live api","publishedAt":"2035-01-01T00:00:00Z","channelId":"UC-api-gate","liveBroadcastContent":"live"},"contentDetails":{"duration":"PT10S"},"liveStreamingDetails":{"actualStartTime":"2035-01-01T00:00:00Z"}}
        ]}"#;
        let (base_url, server) = mock_json_sequence(vec![(200, videos)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("api-gate.db")).unwrap();
        let channel_id = add_test_channel(&db, "https://example.invalid/feed", "api-gate");
        for video_id in ["normalvid01", "livevideo01"] {
            db.insert_video_candidate(NewVideoCandidate {
                video_id,
                channel_id: Some(channel_id),
                url: &format!("https://www.youtube.com/watch?v={video_id}"),
                title: None,
                published_at: None,
                source: CandidateSource::DataApi,
            })
            .unwrap();
        }
        let mut config = Config::default();
        config.youtube.yt_dlp = dir
            .path()
            .join("must-not-run")
            .to_string_lossy()
            .into_owned();
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), Some("test-key"), &base_url).unwrap();
        assert_eq!(
            monitor.gate_pending_candidates(10).await.unwrap().promoted,
            1
        );
        server.await.unwrap();
        assert!(db.get_job_by_video_id("normalvid01").unwrap().is_some());
        assert_eq!(
            db.get_video_candidate("livevideo01")
                .unwrap()
                .unwrap()
                .gate_state,
            crate::model::GateState::Deferred
        );
    }

    #[tokio::test]
    async fn source_language_mismatch_warns_and_marks_but_does_not_block_by_default() {
        let videos = r#"{"items":[{"id":"language001","snippet":{"title":"language","publishedAt":"2035-01-01T00:00:00Z","channelId":"UC-language","liveBroadcastContent":"none","defaultAudioLanguage":"ja"},"contentDetails":{"duration":"PT10S"},"status":{"privacyStatus":"public"}}]}"#;
        let (base_url, server) = mock_json_sequence(vec![(200, videos)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("language.db")).unwrap();
        let channel_id = add_test_channel(&db, "https://example.invalid/feed", "language");
        db.insert_video_candidate(NewVideoCandidate {
            video_id: "language001",
            channel_id: Some(channel_id),
            url: "https://www.youtube.com/watch?v=language001",
            title: None,
            published_at: None,
            source: CandidateSource::DataApi,
        })
        .unwrap();
        let config = Config::default();
        assert!(!config.translation.enforce_source_lang);
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), Some("test-key"), &base_url).unwrap();

        assert_eq!(
            monitor.gate_pending_candidates(10).await.unwrap().promoted,
            1
        );
        server.await.unwrap();
        assert!(db.get_job_by_video_id("language001").unwrap().is_some());
        let candidate = db.get_video_candidate("language001").unwrap().unwrap();
        assert_eq!(candidate.source_language.as_deref(), Some("ja"));
        assert!(candidate.source_language_mismatch);
        assert!(source_language_matches("en", "en-GB"));
    }

    /// `zxx`（无语言内容）和 `und`（未确定）不是「另一种语言」，不能算不符。
    /// 线上首轮扫描里 `zxx` 一个值就刷了 67 条误报告警。
    #[test]
    fn unknown_language_tags_are_not_treated_as_mismatch() {
        for tag in ["zxx", "und", "ZXX", "zxx-ZZ", ""] {
            assert!(
                source_language_matches("en", tag),
                "无法判定语言时应放行: {tag:?}"
            );
            assert!(
                source_language_matches("ja", tag),
                "与期望语言无关，一律放行: {tag:?}"
            );
        }
        // 地区变体仍然算同一种语言。
        for tag in ["en-GB", "en-US", "en-CA", "en_AU"] {
            assert!(source_language_matches("en", tag), "地区变体应匹配: {tag}");
        }
        // 真正的外语仍然要报出来。
        for tag in ["ru", "pt", "de-DE", "ja"] {
            assert!(
                !source_language_matches("en", tag),
                "真实外语不应放行: {tag}"
            );
        }
    }

    #[tokio::test]
    async fn missing_api_key_gracefully_uses_ytdlp_gate_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("no-key.db")).unwrap();
        db.insert_video_candidate(NewVideoCandidate {
            video_id: "normalvid01",
            channel_id: None,
            url: "https://www.youtube.com/watch?v=normalvid01",
            title: None,
            published_at: None,
            source: CandidateSource::Rss,
        })
        .unwrap();
        let mut config = Config::default();
        config.youtube.yt_dlp = gate_ytdlp(dir.path());
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), None, "http://127.0.0.1:9/youtube/v3")
                .unwrap();
        assert!(!monitor.has_data_api());
        assert_eq!(monitor.poll_data_api().await.unwrap(), 0);
        assert_eq!(
            monitor.gate_pending_candidates(10).await.unwrap().promoted,
            1
        );
        assert!(db.get_job_by_video_id("normalvid01").unwrap().is_some());
    }

    #[tokio::test]
    async fn unavailable_data_api_uses_ytdlp_gate_fallback() {
        let (base_url, server) =
            mock_json_sequence(vec![(500, r#"{"error":{"message":"temporary"}}"#)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("api-fallback.db")).unwrap();
        db.insert_video_candidate(NewVideoCandidate {
            video_id: "normalvid01",
            channel_id: None,
            url: "https://www.youtube.com/watch?v=normalvid01",
            title: None,
            published_at: None,
            source: CandidateSource::DataApi,
        })
        .unwrap();
        let mut config = Config::default();
        config.youtube.yt_dlp = gate_ytdlp(dir.path());
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), Some("test-key"), &base_url).unwrap();
        assert_eq!(
            monitor.gate_pending_candidates(10).await.unwrap().promoted,
            1
        );
        server.await.unwrap();
        assert!(db.get_job_by_video_id("normalvid01").unwrap().is_some());
    }

    #[test]
    fn priority_channel_forces_one_minute_data_api_polling() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("priority-interval.db")).unwrap();
        let channel_id = add_test_channel(&db, "https://example.invalid/feed", "priority");
        db.set_channel_priority(channel_id, ChannelPriority::Priority)
            .unwrap();
        let mut config = Config::default();
        config.monitor.poll_seconds = 60;
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), None, "http://127.0.0.1:9/youtube/v3")
                .unwrap();
        let decision = monitor
            .data_api_poll_decision(
                &db.channel(channel_id).unwrap(),
                Utc::now(),
                QuotaDegradation::NarrowHot,
            )
            .unwrap();
        assert_eq!(decision.mode, DataApiPollMode::Priority);
        assert_eq!(decision.interval, Duration::from_secs(60));
    }

    #[test]
    fn active_websub_lease_uses_configurable_thirty_minute_data_api_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("websub-interval.db")).unwrap();
        let channel_id = add_test_channel(&db, "https://example.invalid/feed", "websub");
        let now = Utc::now();
        db.mark_websub_lease(channel_id, now + chrono::Duration::days(1))
            .unwrap();
        let mut config = Config::default();
        config.websub.enabled = true;
        config.websub.callback_base_url = "https://push.example.com".into();
        assert_eq!(config.websub.data_api_poll_minutes, 30);
        let monitor =
            Monitor::new_with_data_api(config, db.clone(), None, "http://127.0.0.1:9/youtube/v3")
                .unwrap();
        let channel = db.channel(channel_id).unwrap();
        let decision = monitor
            .data_api_poll_decision(&channel, now, QuotaDegradation::Normal)
            .unwrap();
        assert_eq!(decision.mode, DataApiPollMode::WebSubFallback);
        assert_eq!(decision.interval, Duration::from_secs(30 * 60));
    }

    #[test]
    fn active_websub_lease_overrides_priority_polling_for_data_api_and_rss() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("websub-priority.db")).unwrap();
        let channel_id = add_test_channel(&db, "https://example.invalid/feed", "priority");
        db.set_channel_priority(channel_id, ChannelPriority::Priority)
            .unwrap();
        let now = Utc::now();
        let mut config = Config::default();
        config.monitor.poll_seconds = 60;
        config.websub.enabled = true;
        config.websub.callback_base_url = "https://push.example.com".into();
        config.websub.data_api_poll_minutes = 20;
        let monitor = Monitor::new_with_data_api(
            config.clone(),
            db.clone(),
            None,
            "http://127.0.0.1:9/youtube/v3",
        )
        .unwrap();

        // 未订阅成功前优先频道保持每分钟 Data API + RSS。
        let channel = db.channel(channel_id).unwrap();
        let decision = monitor
            .data_api_poll_decision(&channel, now, QuotaDegradation::Normal)
            .unwrap();
        assert_eq!(decision.mode, DataApiPollMode::Priority);
        assert_eq!(
            rss_poll_interval(&config, &channel, now),
            Duration::from_secs(60)
        );

        // 租约生效后两条通道都退到 data_api_poll_minutes 兜底。
        db.mark_websub_lease(channel_id, now + chrono::Duration::days(1))
            .unwrap();
        let channel = db.channel(channel_id).unwrap();
        let decision = monitor
            .data_api_poll_decision(&channel, now, QuotaDegradation::Normal)
            .unwrap();
        assert_eq!(decision.mode, DataApiPollMode::WebSubFallback);
        assert_eq!(decision.interval, Duration::from_secs(20 * 60));
        assert_eq!(
            rss_poll_interval(&config, &channel, now),
            Duration::from_secs(20 * 60)
        );

        // 租约过期或 WebSub 关闭时立即恢复优先频道的高频轮询。
        let expired = now + chrono::Duration::days(2);
        let decision = monitor
            .data_api_poll_decision(&channel, expired, QuotaDegradation::Normal)
            .unwrap();
        assert_eq!(decision.mode, DataApiPollMode::Priority);
        assert_eq!(
            rss_poll_interval(&config, &channel, expired),
            Duration::from_secs(60)
        );
        let mut disabled = config.clone();
        disabled.websub.enabled = false;
        assert_eq!(
            rss_poll_interval(&disabled, &channel, now),
            Duration::from_secs(60)
        );
    }
}
