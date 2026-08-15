use crate::{
    db::{Database, DiscoveryQuota},
    model::VideoMetadata,
};
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::America::Los_Angeles;
use reqwest::{
    StatusCode,
    header::{ETAG, IF_NONE_MATCH},
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::OnceLock;
use thiserror::Error;

pub const YOUTUBE_DAILY_QUOTA: u32 = 10_000;
pub const QUOTA_SKIP_DEEP_SCAN_AT: u32 = 8_000;
pub const QUOTA_EXTEND_COLD_AT: u32 = 8_500;
pub const QUOTA_NARROW_HOT_AT: u32 = 9_000;
pub const QUOTA_FALLBACK_ONLY_AT: u32 = 9_500;
const DEFAULT_API_BASE_URL: &str = "https://www.googleapis.com/youtube/v3";
const QUOTA_DEGRADATION_STATE_KEY: &str = "quota_degradation_warned_for_reset";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QuotaDegradation {
    Normal,
    SkipDeepScan,
    ExtendCold,
    NarrowHot,
    FallbackOnly,
}

impl QuotaDegradation {
    pub fn for_used(used: u32) -> Self {
        match used {
            QUOTA_FALLBACK_ONLY_AT.. => Self::FallbackOnly,
            QUOTA_NARROW_HOT_AT.. => Self::NarrowHot,
            QUOTA_EXTEND_COLD_AT.. => Self::ExtendCold,
            QUOTA_SKIP_DEEP_SCAN_AT.. => Self::SkipDeepScan,
            _ => Self::Normal,
        }
    }

    fn level(self) -> u8 {
        match self {
            Self::Normal => 0,
            Self::SkipDeepScan => 1,
            Self::ExtendCold => 2,
            Self::NarrowHot => 3,
            Self::FallbackOnly => 4,
        }
    }

    fn from_level(level: u8) -> Self {
        match level {
            1 => Self::SkipDeepScan,
            2 => Self::ExtendCold,
            3 => Self::NarrowHot,
            4.. => Self::FallbackOnly,
            _ => Self::Normal,
        }
    }

    fn action(self) -> &'static str {
        match self {
            Self::Normal => "正常",
            Self::SkipDeepScan => "停止每日 Data API 深扫",
            Self::ExtendCold => "冷区轮询间隔延长为两倍",
            Self::NarrowHot => "预测热窗宽度收窄为一半",
            Self::FallbackOnly => "Data API 主发现停用，整体回落到 RSS/yt-dlp",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QuotaPolicy {
    pub used: u32,
    pub reset_at: DateTime<Utc>,
    pub degradation: QuotaDegradation,
}

#[derive(Debug, Error)]
pub enum YoutubeApiError {
    #[error("YouTube Data API 本地日配额已用 {used}/{budget}，将在 {reset_at} 重置")]
    LocalQuota {
        used: u32,
        budget: u32,
        reset_at: DateTime<Utc>,
    },
    #[error("YouTube Data API HTTP {status}: {reason}")]
    Http {
        status: StatusCode,
        reason: String,
        quota_exceeded: bool,
    },
    #[error("YouTube Data API 请求失败: {0}")]
    Request(#[from] reqwest::Error),
    #[error("YouTube Data API 响应无效: {0}")]
    InvalidResponse(#[from] serde_json::Error),
    #[error("YouTube Data API 状态读写失败: {0:#}")]
    State(anyhow::Error),
}

impl YoutubeApiError {
    pub fn is_quota_exceeded(&self) -> bool {
        matches!(
            self,
            Self::LocalQuota { .. }
                | Self::Http {
                    quota_exceeded: true,
                    ..
                }
        )
    }
}

#[derive(Debug, Clone)]
pub struct PlaylistVideo {
    pub video_id: String,
    pub title: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct PlaylistPage {
    pub videos: Vec<PlaylistVideo>,
    pub etag: Option<String>,
    pub not_modified: bool,
}

struct ApiResponse {
    body: Vec<u8>,
    etag: Option<String>,
    not_modified: bool,
}

#[derive(Clone)]
pub struct YoutubeDataApi {
    client: reqwest::Client,
    db: Database,
    api_key: String,
    base_url: String,
}

impl YoutubeDataApi {
    pub fn new(client: reqwest::Client, db: Database, api_key: String) -> Self {
        Self::with_base_url(client, db, api_key, DEFAULT_API_BASE_URL)
    }

    pub(crate) fn with_base_url(
        client: reqwest::Client,
        db: Database,
        api_key: String,
        base_url: &str,
    ) -> Self {
        Self {
            client,
            db,
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    pub fn quota_status(&self) -> Result<DiscoveryQuota, YoutubeApiError> {
        let now = Utc::now();
        self.db
            .consume_discovery_quota(0, YOUTUBE_DAILY_QUOTA, now, next_pacific_midnight(now))
            .map_err(YoutubeApiError::State)
    }

    pub fn quota_policy(&self) -> Result<QuotaPolicy, YoutubeApiError> {
        let quota = self.quota_status()?;
        self.record_degradation_warnings(quota)?;
        Ok(QuotaPolicy {
            used: quota.used,
            reset_at: quota.reset_at,
            degradation: QuotaDegradation::for_used(quota.used),
        })
    }

    pub async fn channel_upload_playlists(
        &self,
        channel_ids: &[String],
    ) -> Result<HashMap<String, String>, YoutubeApiError> {
        let mut uploads = HashMap::new();
        for chunk in channel_ids.chunks(50) {
            let response: ListResponse<ChannelItem> = self
                .get(
                    "channels",
                    &[
                        ("part", "contentDetails".to_string()),
                        ("id", chunk.join(",")),
                    ],
                )
                .await?;
            for item in response.items {
                uploads.insert(item.id, item.content_details.related_playlists.uploads);
            }
        }
        Ok(uploads)
    }

    pub async fn playlist_items(
        &self,
        playlist_id: &str,
        max_results: usize,
        etag: Option<&str>,
    ) -> Result<PlaylistPage, YoutubeApiError> {
        let response = self
            .get_response(
                "playlistItems",
                &[
                    ("part", "snippet,contentDetails".to_string()),
                    ("playlistId", playlist_id.to_string()),
                    ("maxResults", max_results.clamp(1, 50).to_string()),
                ],
                etag,
            )
            .await?;
        if response.not_modified {
            return Ok(PlaylistPage {
                videos: Vec::new(),
                etag: response.etag,
                not_modified: true,
            });
        }
        let parsed: ListResponse<PlaylistItem> = serde_json::from_slice(&response.body)?;
        // 正文 etag 优先，响应头只作兜底（真实接口不发头，见 ListResponse 注释）。
        let etag = parsed.etag.or(response.etag);
        let videos = parsed
            .items
            .into_iter()
            .filter_map(|item| {
                let video_id = item
                    .content_details
                    .and_then(|details| details.video_id)
                    .or_else(|| {
                        item.snippet
                            .resource_id
                            .and_then(|resource| resource.video_id)
                    })?;
                Some(PlaylistVideo {
                    video_id,
                    title: item.snippet.title,
                    published_at: item.snippet.published_at,
                })
            })
            .collect();
        Ok(PlaylistPage {
            videos,
            etag,
            not_modified: false,
        })
    }

    pub async fn videos(
        &self,
        video_ids: &[String],
    ) -> Result<HashMap<String, VideoMetadata>, YoutubeApiError> {
        let mut videos = HashMap::new();
        for chunk in video_ids.chunks(50) {
            let response: ListResponse<VideoItem> = self
                .get(
                    "videos",
                    &[
                        (
                            "part",
                            "snippet,contentDetails,liveStreamingDetails,status".to_string(),
                        ),
                        ("id", chunk.join(",")),
                    ],
                )
                .await?;
            for item in response.items {
                let metadata = item.into_metadata()?;
                videos.insert(metadata.id.clone(), metadata);
            }
        }
        Ok(videos)
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&'static str, String)],
    ) -> Result<T, YoutubeApiError> {
        let response = self.get_response(path, query, None).await?;
        if response.not_modified {
            return Err(YoutubeApiError::InvalidResponse(serde_json::Error::io(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{path} 未使用 ETag 却返回 304"),
                ),
            )));
        }
        Ok(serde_json::from_slice(&response.body)?)
    }

    async fn get_response(
        &self,
        path: &str,
        query: &[(&'static str, String)],
        etag: Option<&str>,
    ) -> Result<ApiResponse, YoutubeApiError> {
        // 配额在发请求前预留；即使服务器随后返回 304，也按一次调用记 1 单位。
        // Google 在前端按请求计费，ETag 这里只节省带宽与 JSON 解析，不计作配额节省。
        let quota = self.reserve_quota()?;
        self.record_degradation_warnings(quota)?;
        let mut request = self
            .client
            .get(format!("{}/{path}", self.base_url))
            .header("X-goog-api-key", &self.api_key)
            .query(query);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let response = request.send().await?;
        let status = response.status();
        let response_etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = response.bytes().await?;
        if status == StatusCode::NOT_MODIFIED {
            return Ok(ApiResponse {
                body: Vec::new(),
                etag: response_etag,
                not_modified: true,
            });
        }
        if !status.is_success() {
            let (reason, quota_exceeded) = api_error_detail(&body);
            if status == StatusCode::FORBIDDEN && quota_exceeded {
                let now = Utc::now();
                let exhausted = self
                    .db
                    .exhaust_discovery_quota(YOUTUBE_DAILY_QUOTA, now, next_pacific_midnight(now))
                    .map_err(YoutubeApiError::State)?;
                self.record_degradation_warnings(exhausted)?;
            }
            return Err(YoutubeApiError::Http {
                status,
                reason,
                quota_exceeded,
            });
        }
        Ok(ApiResponse {
            body: body.to_vec(),
            etag: response_etag,
            not_modified: false,
        })
    }

    fn reserve_quota(&self) -> Result<DiscoveryQuota, YoutubeApiError> {
        let now = Utc::now();
        let quota = self
            .db
            .consume_discovery_quota(1, YOUTUBE_DAILY_QUOTA, now, next_pacific_midnight(now))
            .map_err(YoutubeApiError::State)?;
        if !quota.allowed {
            return Err(YoutubeApiError::LocalQuota {
                used: quota.used,
                budget: YOUTUBE_DAILY_QUOTA,
                reset_at: quota.reset_at,
            });
        }
        Ok(quota)
    }

    fn record_degradation_warnings(&self, quota: DiscoveryQuota) -> Result<(), YoutubeApiError> {
        let current = QuotaDegradation::for_used(quota.used);
        if current == QuotaDegradation::Normal {
            return Ok(());
        }
        let reset_key = quota.reset_at.to_rfc3339();
        let previous = self
            .db
            .get_discovery_state(QUOTA_DEGRADATION_STATE_KEY)
            .map_err(YoutubeApiError::State)?
            .and_then(|raw| {
                let (stored_reset, stored_level) = raw.rsplit_once('|')?;
                (stored_reset == reset_key)
                    .then(|| stored_level.parse::<u8>().ok())
                    .flatten()
            })
            .unwrap_or(0)
            .min(QuotaDegradation::FallbackOnly.level());
        for level in (previous + 1)..=current.level() {
            let degradation = QuotaDegradation::from_level(level);
            tracing::warn!(
                used = quota.used,
                budget = YOUTUBE_DAILY_QUOTA,
                reset_at = %quota.reset_at,
                level,
                action = degradation.action(),
                "YouTube Data API 配额进入降级阶段"
            );
        }
        self.db
            .set_discovery_state(
                QUOTA_DEGRADATION_STATE_KEY,
                &format!("{reset_key}|{}", current.level()),
            )
            .map_err(YoutubeApiError::State)
    }
}

fn api_error_detail(body: &[u8]) -> (String, bool) {
    let value: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    let reasons = value
        .pointer("/error/errors")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("reason")?.as_str())
        .collect::<Vec<_>>();
    let quota_exceeded = reasons
        .iter()
        .any(|reason| matches!(*reason, "quotaExceeded" | "dailyLimitExceeded"));
    let message = value
        .pointer("/error/message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("未知错误");
    let reason = if reasons.is_empty() {
        message.to_string()
    } else {
        format!("{} ({})", message, reasons.join(","))
    };
    (reason, quota_exceeded)
}

pub fn parse_iso8601_duration(value: &str) -> Option<f64> {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    let captures = PATTERN
        .get_or_init(|| {
            regex::Regex::new(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$")
                .expect("固定 ISO-8601 duration 正则必须有效")
        })
        .captures(value)?;
    if captures.iter().skip(1).all(|part| part.is_none()) {
        return None;
    }
    let number = |index: usize| {
        captures
            .get(index)
            .and_then(|part| part.as_str().parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    Some(number(1) * 86_400.0 + number(2) * 3_600.0 + number(3) * 60.0 + number(4))
}

pub fn next_pacific_midnight(now: DateTime<Utc>) -> DateTime<Utc> {
    let local = now.with_timezone(&Los_Angeles);
    let next_date = local
        .date_naive()
        .succ_opt()
        .expect("chrono 支持范围内应有下一天");
    let midnight = next_date.and_hms_opt(0, 0, 0).expect("午夜时间必须有效");
    Los_Angeles
        .from_local_datetime(&midnight)
        .earliest()
        .expect("洛杉矶午夜必须存在")
        .with_timezone(&Utc)
}

#[derive(Deserialize)]
struct ListResponse<T> {
    items: Vec<T>,
    /// YouTube Data API 只在 JSON 正文里给 etag，**不发 `ETag` 响应头**（已对真实
    /// 接口验证过）。所以条件请求的验证符必须从这里取，不能只读响应头——否则
    /// `If-None-Match` 永远发不出去，304 也就永远不会发生。
    #[serde(default)]
    etag: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChannelItem {
    id: String,
    content_details: ChannelContentDetails,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChannelContentDetails {
    related_playlists: RelatedPlaylists,
}

#[derive(Deserialize)]
struct RelatedPlaylists {
    uploads: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistItem {
    snippet: PlaylistSnippet,
    content_details: Option<PlaylistContentDetails>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistSnippet {
    title: Option<String>,
    published_at: Option<DateTime<Utc>>,
    resource_id: Option<ResourceId>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceId {
    video_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistContentDetails {
    video_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VideoItem {
    id: String,
    snippet: VideoSnippet,
    content_details: VideoContentDetails,
    live_streaming_details: Option<LiveStreamingDetails>,
}

impl VideoItem {
    fn into_metadata(self) -> Result<VideoMetadata, YoutubeApiError> {
        let duration = parse_iso8601_duration(&self.content_details.duration).ok_or_else(|| {
            YoutubeApiError::InvalidResponse(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("无法解析视频 {} 的 duration", self.id),
            )))
        })?;
        let live = self.live_streaming_details.as_ref();
        let live_status = match self.snippet.live_broadcast_content.as_deref() {
            Some("live") => "is_live",
            Some("upcoming") => "is_upcoming",
            _ if live.is_some_and(|details| {
                details.actual_start_time.is_some() && details.actual_end_time.is_some()
            }) =>
            {
                "was_live"
            }
            _ if live.is_some() => "post_live",
            _ => "not_live",
        };
        let timestamp = live
            .and_then(|details| details.actual_start_time)
            .or(self.snippet.published_at)
            .map(|value| value.timestamp());
        Ok(VideoMetadata {
            id: self.id.clone(),
            url: format!("https://www.youtube.com/watch?v={}", self.id),
            title: self.snippet.title.unwrap_or_else(|| "Untitled".to_string()),
            description: None,
            uploader: None,
            upload_date: None,
            channel: None,
            channel_id: self.snippet.channel_id,
            timestamp,
            duration: Some(duration),
            width: None,
            height: None,
            fps: None,
            thumbnail_url: None,
            webpage_url: None,
            live_status: Some(live_status.to_string()),
            default_audio_language: self.snippet.default_audio_language,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VideoSnippet {
    title: Option<String>,
    published_at: Option<DateTime<Utc>>,
    channel_id: Option<String>,
    live_broadcast_content: Option<String>,
    default_audio_language: Option<String>,
}

// 不解析也不使用 contentDetails.caption：实测有自动字幕的视频仍返回 false，
// 该字段只反映人工上传轨。captions.list 虽精确但每次 50 单位，也刻意不调用。
#[derive(Deserialize)]
struct VideoContentDetails {
    duration: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveStreamingDetails {
    actual_start_time: Option<DateTime<Utc>>,
    actual_end_time: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn mock_api(
        bodies: Vec<(u16, &'static str)>,
    ) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        mock_api_with_headers(
            bodies
                .into_iter()
                .map(|(status, body)| (status, "", body))
                .collect(),
        )
        .await
    }

    async fn mock_api_with_headers(
        bodies: Vec<(u16, &'static str, &'static str)>,
    ) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let server = tokio::spawn(async move {
            for (status, extra_headers, body) in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0_u8; 16 * 1024];
                let read = socket.read(&mut buffer).await.unwrap();
                captured
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buffer[..read]).into_owned());
                let response = format!(
                    "HTTP/1.1 {status} Test\r\n{extra_headers}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{address}/youtube/v3"), requests, server)
    }

    #[test]
    fn parses_iso_8601_duration() {
        assert_eq!(parse_iso8601_duration("PT0S"), Some(0.0));
        assert_eq!(parse_iso8601_duration("PT1H2M3S"), Some(3723.0));
        assert_eq!(parse_iso8601_duration("P1DT2H"), Some(93_600.0));
        assert_eq!(parse_iso8601_duration("PT1M3.5S"), Some(63.5));
        assert_eq!(parse_iso8601_duration("P"), None);
        assert_eq!(parse_iso8601_duration("1:23"), None);
    }

    #[test]
    fn quota_resets_at_next_pacific_midnight() {
        let winter = DateTime::parse_from_rfc3339("2026-01-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            next_pacific_midnight(winter),
            DateTime::parse_from_rfc3339("2026-01-16T08:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        let summer = DateTime::parse_from_rfc3339("2026-08-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            next_pacific_midnight(summer),
            DateTime::parse_from_rfc3339("2026-08-16T07:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn quota_degrades_in_the_required_four_step_order() {
        assert_eq!(
            QuotaDegradation::for_used(QUOTA_SKIP_DEEP_SCAN_AT - 1),
            QuotaDegradation::Normal
        );
        assert_eq!(
            QuotaDegradation::for_used(QUOTA_SKIP_DEEP_SCAN_AT),
            QuotaDegradation::SkipDeepScan
        );
        assert_eq!(
            QuotaDegradation::for_used(QUOTA_EXTEND_COLD_AT),
            QuotaDegradation::ExtendCold
        );
        assert_eq!(
            QuotaDegradation::for_used(QUOTA_NARROW_HOT_AT),
            QuotaDegradation::NarrowHot
        );
        assert_eq!(
            QuotaDegradation::for_used(QUOTA_FALLBACK_ONLY_AT),
            QuotaDegradation::FallbackOnly
        );
        assert!(QuotaDegradation::SkipDeepScan < QuotaDegradation::ExtendCold);
        assert!(QuotaDegradation::ExtendCold < QuotaDegradation::NarrowHot);
        assert!(QuotaDegradation::NarrowHot < QuotaDegradation::FallbackOnly);
    }

    #[tokio::test]
    async fn batches_more_than_fifty_ids_and_keeps_key_out_of_url() {
        let (base_url, requests, server) =
            mock_api(vec![(200, r#"{"items":[]}"#), (200, r#"{"items":[]}"#)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("api.db")).unwrap();
        let api = YoutubeDataApi::with_base_url(
            reqwest::Client::new(),
            db.clone(),
            "header-secret".to_string(),
            &base_url,
        );
        let ids = (0..51)
            .map(|index| format!("UC{index:049}"))
            .collect::<Vec<_>>();
        assert!(api.channel_upload_playlists(&ids).await.unwrap().is_empty());
        server.await.unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-goog-api-key: header-secret")
            );
            assert!(!request.lines().next().unwrap().contains("key="));
        }
        assert_eq!(api.quota_status().unwrap().used, 2);
    }

    #[tokio::test]
    async fn videos_list_chunks_more_than_fifty_ids() {
        let (base_url, requests, server) =
            mock_api(vec![(200, r#"{"items":[]}"#), (200, r#"{"items":[]}"#)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("videos.db")).unwrap();
        let api = YoutubeDataApi::with_base_url(
            reqwest::Client::new(),
            db,
            "test-key".to_string(),
            &base_url,
        );
        let ids = (0..51)
            .map(|index| format!("vid{index:08}"))
            .collect::<Vec<_>>();
        assert!(api.videos(&ids).await.unwrap().is_empty());
        server.await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| {
            let request_line = request.lines().next().unwrap_or_default();
            request_line.contains("/videos?")
                && request_line.contains("snippet")
                && request_line.contains("contentDetails")
                && request_line.contains("liveStreamingDetails")
                && request_line.contains("status")
        }));
    }

    /// 回归测试：真实的 YouTube Data API **不发 `ETag` 响应头**，etag 只在 JSON
    /// 正文里。此前的实现只读响应头，导致 etag 永远是 None、`If-None-Match`
    /// 永远发不出去——mock 当时发了响应头，所以测试通过而线上不生效。
    /// 这里的 mock 刻意不发响应头，复现真实接口的行为。
    #[tokio::test]
    async fn etag_is_taken_from_body_when_server_sends_no_etag_header() {
        let (base_url, requests, server) =
            mock_api(vec![(200, r#"{"etag":"BODY_ETAG","items":[]}"#), (304, "")]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("etag_body.db")).unwrap();
        let api = YoutubeDataApi::with_base_url(
            reqwest::Client::new(),
            db,
            "test-key".to_string(),
            &base_url,
        );

        let first = api.playlist_items("UUtest", 50, None).await.unwrap();
        assert_eq!(
            first.etag.as_deref(),
            Some("BODY_ETAG"),
            "必须从正文取到 etag，否则条件请求永远不会生效"
        );

        let second = api
            .playlist_items("UUtest", 50, first.etag.as_deref())
            .await
            .unwrap();
        assert!(second.not_modified);
        server.await.unwrap();

        let requests = requests.lock().unwrap();
        assert!(
            !requests[0].to_lowercase().contains("if-none-match"),
            "第一次没有 etag，不应带条件头"
        );
        let second_request = requests[1].to_lowercase();
        assert!(
            second_request.contains("if-none-match: body_etag"),
            "第二次必须带上正文里拿到的 etag，实际请求：{}",
            requests[1]
        );
    }

    #[tokio::test]
    async fn etag_304_still_consumes_one_quota_unit() {
        let (base_url, requests, server) = mock_api_with_headers(vec![
            (200, "ETag: \"playlist-v1\"\r\n", r#"{"items":[]}"#),
            (304, "", ""),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("etag.db")).unwrap();
        let api = YoutubeDataApi::with_base_url(
            reqwest::Client::new(),
            db,
            "test-key".to_string(),
            &base_url,
        );
        let first = api.playlist_items("UU-test", 50, None).await.unwrap();
        assert!(!first.not_modified);
        assert_eq!(first.etag.as_deref(), Some("\"playlist-v1\""));
        let second = api
            .playlist_items("UU-test", 50, first.etag.as_deref())
            .await
            .unwrap();
        assert!(second.not_modified);
        assert_eq!(api.quota_status().unwrap().used, 2);
        server.await.unwrap();

        let requests = requests.lock().unwrap();
        assert!(
            requests[0]
                .lines()
                .next()
                .unwrap()
                .contains("maxResults=50")
        );
        assert!(
            requests[1]
                .to_ascii_lowercase()
                .contains("if-none-match: \"playlist-v1\"")
        );
    }

    #[tokio::test]
    async fn quota_exceeded_403_exhausts_local_budget() {
        let body = r#"{"error":{"message":"quota gone","errors":[{"reason":"quotaExceeded"}]}}"#;
        let (base_url, _, server) = mock_api(vec![(403, body)]).await;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("quota.db")).unwrap();
        let api = YoutubeDataApi::with_base_url(
            reqwest::Client::new(),
            db,
            "test-key".to_string(),
            &base_url,
        );
        let error = api.playlist_items("UU-test", 50, None).await.unwrap_err();
        assert!(error.is_quota_exceeded());
        assert_eq!(api.quota_status().unwrap().used, YOUTUBE_DAILY_QUOTA);
        assert_eq!(
            api.quota_policy().unwrap().degradation,
            QuotaDegradation::FallbackOnly
        );
        server.await.unwrap();
    }
}
