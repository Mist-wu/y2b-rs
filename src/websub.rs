use crate::{
    config::WebSubConfig,
    db::{Database, NewVideoCandidate, WebSubChannel},
    model::CandidateSource,
    youtube_api::bounded_http_client,
};
use anyhow::{Context, Result};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use feed_rs::parser;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha1::Sha1;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub const WEBSUB_LEASE_SECONDS: u64 = 432_000;
pub const WEBSUB_BODY_LIMIT: usize = 128 * 1024;
const WEBSUB_RENEWAL_PERCENT: u64 = 80;
const WEBSUB_HUB_URL: &str = "https://pubsubhubbub.appspot.com/subscribe";

#[derive(Clone)]
struct WebSubState {
    db: Database,
    data_api_poll_minutes: u64,
}

#[derive(Clone)]
pub struct WebSubService {
    config: WebSubConfig,
    db: Database,
    client: reqwest::Client,
    hub_url: String,
}

impl WebSubService {
    pub fn new(config: WebSubConfig, db: Database) -> Result<Self> {
        config_callback_base(&config)?;
        Ok(Self {
            config,
            db,
            client: bounded_http_client("y2b-rs/0.1")?,
            hub_url: WEBSUB_HUB_URL.to_string(),
        })
    }

    #[cfg(test)]
    fn with_hub_url(config: WebSubConfig, db: Database, hub_url: &str) -> Result<Self> {
        let mut service = Self::new(config, db)?;
        service.hub_url = hub_url.to_string();
        Ok(service)
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/websub/{callback_path}",
                get(verify_subscription).post(receive_notification),
            )
            .layer(DefaultBodyLimit::max(WEBSUB_BODY_LIMIT))
            .with_state(Arc::new(WebSubState {
                db: self.db.clone(),
                data_api_poll_minutes: self.config.data_api_poll_minutes,
            }))
    }

    pub async fn renew_due_subscriptions(&self) -> Result<usize> {
        let now = Utc::now();
        let renew_before = now + chrono::Duration::seconds(renewal_lead_seconds() as i64);
        let channels = self.db.due_websub_channels(renew_before)?;
        let mut accepted = 0;
        for channel in channels {
            let channel_id = channel.id;
            let channel_name = channel.name.clone();
            match self.subscribe_channel(channel).await {
                Ok(()) => accepted += 1,
                // reqwest 的顶层信息只有 "error sending request"，根因（连接/读超时、
                // DNS）在 source 链里；hub 偶发 15 秒无响应时需要看到它。
                Err(error) => tracing::warn!(
                    channel_id,
                    channel = %channel_name,
                    error = %format!("{error:#}"),
                    "WebSub 订阅或续订失败"
                ),
            }
        }
        Ok(accepted)
    }

    /// 手动强制向 hub 提交所有启用频道的订阅请求，不受当前租约是否到期影响。
    pub async fn subscribe_all(&self) -> Result<usize> {
        let mut accepted = 0;
        let mut failed = 0;
        for channel in self
            .db
            .list_websub_channels()?
            .into_iter()
            .filter(|channel| channel.enabled)
        {
            let channel_id = channel.id;
            let channel_name = channel.name.clone();
            match self.subscribe_channel(channel).await {
                Ok(()) => accepted += 1,
                Err(error) => {
                    failed += 1;
                    tracing::warn!(
                        channel_id,
                        channel = %channel_name,
                        error = %format!("{error:#}"),
                        "手动 WebSub 订阅失败"
                    );
                }
            }
        }
        anyhow::ensure!(
            failed == 0,
            "WebSub 订阅部分失败: 成功 {accepted}，失败 {failed}"
        );
        Ok(accepted)
    }

    /// `<id>` 同时接受 y2b 内部数字 id 与 YouTube `UC...` channel id。
    pub async fn subscribe_identifier(&self, identifier: &str) -> Result<()> {
        let channel = self
            .db
            .websub_channel(identifier)?
            .with_context(|| format!("频道不存在: {identifier}"))?;
        anyhow::ensure!(channel.enabled, "频道已禁用: {identifier}");
        self.subscribe_channel(channel).await
    }

    async fn subscribe_channel(&self, channel: WebSubChannel) -> Result<()> {
        let callback_path = channel
            .callback_path
            .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
        let secret = channel.secret.unwrap_or_else(random_secret);
        let channel = self
            .db
            .ensure_websub_credentials(channel.id, &callback_path, &secret)?;
        let callback_path = channel
            .callback_path
            .as_deref()
            .context("WebSub callback path 未写入")?;
        let secret = channel.secret.as_deref().context("WebSub secret 未写入")?;
        let callback = format!(
            "{}/websub/{callback_path}",
            self.config.callback_base_url.trim_end_matches('/')
        );
        let topic = channel_topic(&channel.youtube_channel_id);
        let lease_seconds = WEBSUB_LEASE_SECONDS.to_string();
        let response = self
            .client
            .post(&self.hub_url)
            .form(&[
                ("hub.mode", "subscribe"),
                ("hub.topic", topic.as_str()),
                ("hub.callback", callback.as_str()),
                ("hub.verify", "async"),
                ("hub.secret", secret),
                ("hub.lease_seconds", lease_seconds.as_str()),
            ])
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "WebSub hub 返回 HTTP {}",
            response.status()
        );
        tracing::info!(
            channel_id = channel.id,
            channel = %channel.name,
            "WebSub 订阅请求已接受，等待异步验证"
        );
        Ok(())
    }
}

pub async fn run(config: WebSubConfig, db: Database) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    let service = WebSubService::new(config.clone(), db)?;
    let listener = tokio::net::TcpListener::bind(&config.bind_addr)
        .await
        .with_context(|| format!("绑定 WebSub 监听地址失败: {}", config.bind_addr))?;
    tracing::info!(bind_addr = %config.bind_addr, "WebSub 回调服务已启动");
    let renewal_service = service.clone();
    let renewal = async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60 * 60));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            match renewal_service.renew_due_subscriptions().await {
                Ok(accepted) if accepted > 0 => {
                    tracing::info!(accepted, "WebSub 订阅续租扫描完成")
                }
                Ok(_) => {}
                Err(error) => tracing::error!(error = %error, "WebSub 订阅续租扫描失败"),
            }
        }
    };
    tokio::select! {
        result = axum::serve(listener, service.router()) => result.context("WebSub HTTP 服务退出"),
        _ = renewal => unreachable!("WebSub 续租循环不应自行退出"),
    }
}

fn config_callback_base(config: &WebSubConfig) -> Result<()> {
    anyhow::ensure!(
        config.callback_base_url.starts_with("https://"),
        "WebSub callback_base_url 必须是 HTTPS 地址"
    );
    Ok(())
}

fn random_secret() -> String {
    let first = Uuid::new_v4().into_bytes();
    let second = Uuid::new_v4().into_bytes();
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(&first);
    bytes[16..].copy_from_slice(&second);
    hex::encode(bytes)
}

fn channel_topic(youtube_channel_id: &str) -> String {
    format!("https://www.youtube.com/xml/feeds/videos.xml?channel_id={youtube_channel_id}")
}

fn renewal_lead_seconds() -> u64 {
    WEBSUB_LEASE_SECONDS * (100 - WEBSUB_RENEWAL_PERCENT) / 100
}

pub fn lease_renewal_due(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    expires_at <= now + chrono::Duration::seconds(renewal_lead_seconds() as i64)
}

#[derive(Deserialize)]
struct VerificationQuery {
    #[serde(rename = "hub.mode")]
    mode: String,
    #[serde(rename = "hub.topic")]
    topic: String,
    #[serde(rename = "hub.challenge")]
    challenge: String,
    #[serde(rename = "hub.lease_seconds")]
    lease_seconds: Option<u64>,
}

async fn verify_subscription(
    State(state): State<Arc<WebSubState>>,
    Path(callback_path): Path<String>,
    Query(query): Query<VerificationQuery>,
) -> Response {
    let channel = match state.db.websub_channel_by_callback(&callback_path) {
        Ok(Some(channel)) => channel,
        Ok(None) => return (StatusCode::NOT_FOUND, "unknown callback").into_response(),
        Err(error) => {
            tracing::error!(error = %error, "读取 WebSub 回调频道失败");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    if query.mode != "subscribe" || query.topic != channel_topic(&channel.youtube_channel_id) {
        return (StatusCode::BAD_REQUEST, "invalid verification topic").into_response();
    }
    let lease_seconds = query
        .lease_seconds
        .unwrap_or(WEBSUB_LEASE_SECONDS)
        .min(WEBSUB_LEASE_SECONDS);
    let expires_at =
        Utc::now() + chrono::Duration::seconds(i64::try_from(lease_seconds).unwrap_or(i64::MAX));
    if let Err(error) = state.db.mark_websub_lease(channel.id, expires_at) {
        tracing::error!(channel_id = channel.id, error = %error, "记录 WebSub 租约失败");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let next_data_api_poll_at = Utc::now()
        + chrono::Duration::minutes(i64::try_from(state.data_api_poll_minutes).unwrap_or(i64::MAX));
    if let Err(error) = state
        .db
        .schedule_data_api_poll(channel.id, next_data_api_poll_at, None)
    {
        tracing::error!(channel_id = channel.id, error = %error, "WebSub 生效后降低 Data API 频率失败");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    tracing::info!(
        channel_id = channel.id,
        %expires_at,
        %next_data_api_poll_at,
        fallback_minutes = state.data_api_poll_minutes,
        "WebSub 租约生效，Data API 改为低频纯兜底"
    );
    (StatusCode::OK, query.challenge).into_response()
}

async fn receive_notification(
    State(state): State<Arc<WebSubState>>,
    Path(callback_path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let channel = match state.db.websub_channel_by_callback(&callback_path) {
        Ok(Some(channel)) => channel,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(error = %error, "读取 WebSub 回调频道失败");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let Some(secret) = channel.secret.as_deref() else {
        tracing::warn!(
            channel_id = channel.id,
            "WebSub 回调缺少本地 secret，拒绝通知"
        );
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let signature = headers
        .get("X-Hub-Signature")
        .and_then(|value| value.to_str().ok());
    if !verify_signature(secret.as_bytes(), &body, signature) {
        tracing::warn!(channel_id = channel.id, "WebSub HMAC 验证失败，丢弃通知");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let expected_topic = channel_topic(&channel.youtube_channel_id);
    let videos = match parse_atom_notification(&body, &expected_topic) {
        Ok(videos) => videos,
        Err(error) => {
            tracing::warn!(channel_id = channel.id, error = %error, "WebSub Atom 通知无效");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    for video in videos {
        if let Err(error) = state.db.insert_video_candidate(NewVideoCandidate {
            video_id: &video.video_id,
            channel_id: Some(channel.id),
            url: &video.url,
            title: video.title.as_deref(),
            published_at: video.published_at,
            source: CandidateSource::Websub,
        }) {
            tracing::error!(channel_id = channel.id, error = %error, "写入 WebSub 候选失败");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    let received_at = Utc::now();
    let next_data_api_poll_at = received_at
        + chrono::Duration::minutes(i64::try_from(state.data_api_poll_minutes).unwrap_or(i64::MAX));
    if let Err(error) =
        state
            .db
            .mark_websub_received(channel.id, received_at, next_data_api_poll_at)
    {
        tracing::error!(channel_id = channel.id, error = %error, "记录 WebSub 最近推送时间失败");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    StatusCode::OK.into_response()
}

fn verify_signature(secret: &[u8], body: &[u8], signature: Option<&str>) -> bool {
    let Some((algorithm, encoded)) = signature.and_then(|value| value.split_once('=')) else {
        return false;
    };
    let Ok(expected) = hex::decode(encoded) else {
        return false;
    };
    match algorithm.to_ascii_lowercase().as_str() {
        "sha1" => Hmac::<Sha1>::new_from_slice(secret).is_ok_and(|mut mac| {
            mac.update(body);
            mac.verify_slice(&expected).is_ok()
        }),
        _ => false,
    }
}

struct NotificationVideo {
    video_id: String,
    url: String,
    title: Option<String>,
    published_at: Option<DateTime<Utc>>,
}

fn parse_atom_notification(body: &[u8], expected_topic: &str) -> Result<Vec<NotificationVideo>> {
    let feed = parser::parse(body).context("Atom XML 解析失败")?;
    anyhow::ensure!(
        feed.links.iter().any(|link| {
            link.rel.as_deref() == Some("self") && link.href.as_str() == expected_topic
        }),
        "通知 topic 不是已知频道"
    );
    Ok(feed
        .entries
        .into_iter()
        .filter_map(|entry| {
            let video_id = entry
                .id
                .strip_prefix("yt:video:")
                .unwrap_or(&entry.id)
                .to_string();
            if !is_youtube_video_id(&video_id) {
                return None;
            }
            let url = entry
                .links
                .iter()
                .find(|link| link.rel.as_deref() == Some("alternate"))
                .or_else(|| entry.links.first())
                .map(|link| link.href.clone())
                .unwrap_or_else(|| format!("https://www.youtube.com/watch?v={video_id}"));
            Some(NotificationVideo {
                video_id,
                url,
                title: entry.title.map(|title| title.content),
                published_at: entry.published.or(entry.updated),
            })
        })
        .collect())
}

fn is_youtube_video_id(value: &str) -> bool {
    value.len() == 11
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tower::ServiceExt;

    const CALLBACK_PATH: &str = "unguessable-callback";
    const SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn config() -> WebSubConfig {
        WebSubConfig {
            enabled: true,
            callback_base_url: "https://push.example.com".into(),
            ..WebSubConfig::default()
        }
    }

    fn setup() -> (tempfile::TempDir, Database, WebSubService, i64) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("websub.db")).unwrap();
        let id = db
            .add_channel(
                "UC-websub",
                "websub",
                "https://www.youtube.com/@websub",
                "https://www.youtube.com/feeds/videos.xml?channel_id=UC-websub",
                crate::model::TransferMode::Direct,
            )
            .unwrap();
        db.ensure_websub_credentials(id, CALLBACK_PATH, SECRET)
            .unwrap();
        let service = WebSubService::new(config(), db.clone()).unwrap();
        (dir, db, service, id)
    }

    #[tokio::test]
    async fn disabled_websub_returns_without_starting_listener() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("disabled.db")).unwrap();
        run(WebSubConfig::default(), db).await.unwrap();
    }

    fn atom(topic: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015" xmlns:at="http://purl.org/atompub/tombstones/1.0">
  <id>yt:channel:UC-websub</id><title>websub</title><updated>2035-01-01T00:00:00Z</updated>
  <link rel="self" href="{topic}" />
  <entry><id>yt:video:normalvid01</id><yt:videoId>normalvid01</yt:videoId><title>pushed</title><link rel="alternate" href="https://www.youtube.com/watch?v=normalvid01"/><published>2035-01-01T00:00:00Z</published><updated>2035-01-01T00:00:00Z</updated></entry>
  <at:deleted-entry ref="yt:video:deleted0001" when="2035-01-01T00:00:00Z" />
</feed>"#
        )
    }

    fn signature(body: &[u8]) -> String {
        let mut mac = Hmac::<Sha1>::new_from_slice(SECRET.as_bytes()).unwrap();
        mac.update(body);
        format!("sha1={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[tokio::test]
    async fn hmac_accepts_correct_and_rejects_wrong_or_missing_signature() {
        let (_dir, db, service, _) = setup();
        let body = atom(&channel_topic("UC-websub"));
        let request = |signature_header: Option<&str>| {
            let mut builder = Request::builder()
                .method("POST")
                .uri(format!("/websub/{CALLBACK_PATH}"));
            if let Some(value) = signature_header {
                builder = builder.header("X-Hub-Signature", value);
            }
            builder.body(Body::from(body.clone())).unwrap()
        };

        let response = service
            .router()
            .oneshot(request(Some(&signature(body.as_bytes()))))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let candidate = db.get_video_candidate("normalvid01").unwrap().unwrap();
        assert_eq!(candidate.source, CandidateSource::Websub);
        let status = db.list_websub_channels().unwrap().remove(0);
        assert!(status.last_received_at.is_some());
        assert!(
            db.channel(status.id)
                .unwrap()
                .next_data_api_poll_at
                .is_some()
        );

        let response = service
            .router()
            .oneshot(request(Some("sha1=00")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = service.router().oneshot(request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let wrong_topic_body = atom(&channel_topic("UC-another"));
        let response = service
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/websub/{CALLBACK_PATH}"))
                    .header("X-Hub-Signature", signature(wrong_topic_body.as_bytes()))
                    .body(Body::from(wrong_topic_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn verification_returns_challenge_and_records_lease() {
        let (_dir, db, service, id) = setup();
        let topic = channel_topic("UC-websub");
        let url = reqwest::Url::parse_with_params(
            &format!("http://localhost/websub/{CALLBACK_PATH}"),
            &[
                ("hub.mode", "subscribe"),
                ("hub.topic", topic.as_str()),
                ("hub.challenge", "challenge-body"),
                ("hub.lease_seconds", "432000"),
            ],
        )
        .unwrap();
        let uri = format!("{}?{}", url.path(), url.query().unwrap());
        let response = service
            .router()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], b"challenge-body");
        let channel = db
            .websub_channel_by_callback(CALLBACK_PATH)
            .unwrap()
            .unwrap();
        assert_eq!(channel.id, id);
        assert!(channel.lease_expires_at.is_some());
        let next_poll = db
            .channel(id)
            .unwrap()
            .next_data_api_poll_at
            .expect("WebSub 生效后应推迟 Data API 兜底轮询");
        let delay = (next_poll - Utc::now()).num_seconds();
        assert!((29 * 60..=30 * 60).contains(&delay));
    }

    #[test]
    fn renewal_becomes_due_at_eighty_percent_of_lease() {
        let now = Utc::now();
        let lead = chrono::Duration::seconds(renewal_lead_seconds() as i64);
        assert!(!lease_renewal_due(
            now + lead + chrono::Duration::seconds(1),
            now
        ));
        assert!(lease_renewal_due(now + lead, now));
    }

    #[tokio::test]
    async fn oversized_body_is_rejected() {
        let (_dir, _db, service, _) = setup();
        let body = vec![b'x'; WEBSUB_BODY_LIMIT + 1];
        let response = service
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/websub/{CALLBACK_PATH}"))
                    .header("X-Hub-Signature", signature(&body))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn renewal_posts_required_subscription_fields() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(String::new()));
        let request_copy = Arc::clone(&captured);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0_u8; 16 * 1024];
            let read = socket.read(&mut bytes).await.unwrap();
            *request_copy.lock().unwrap() = String::from_utf8_lossy(&bytes[..read]).into_owned();
            socket
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("subscribe.db")).unwrap();
        db.add_channel(
            "UC-subscribe",
            "subscribe",
            "https://www.youtube.com/@subscribe",
            "https://www.youtube.com/feeds/videos.xml?channel_id=UC-subscribe",
            crate::model::TransferMode::Direct,
        )
        .unwrap();
        let service = WebSubService::with_hub_url(
            config(),
            db.clone(),
            &format!("http://{address}/subscribe"),
        )
        .unwrap();
        assert_eq!(service.renew_due_subscriptions().await.unwrap(), 1);
        server.await.unwrap();
        let request = captured.lock().unwrap();
        assert!(request.contains("hub.mode=subscribe"));
        assert!(request.contains("hub.verify=async"));
        assert!(request.contains("hub.lease_seconds=432000"));
        assert!(request.contains("hub.secret="));
        assert!(request.contains("hub.callback=https%3A%2F%2Fpush.example.com%2Fwebsub%2F"));
        assert!(
            request.contains("hub.topic=https%3A%2F%2Fwww.youtube.com%2Fxml%2Ffeeds%2Fvideos.xml")
        );
        let channel = db
            .due_websub_channels(Utc::now() + chrono::Duration::days(10))
            .unwrap()
            .remove(0);
        assert!(channel.callback_path.is_some());
        assert!(channel.secret.is_some());
        assert!(channel.lease_expires_at.is_none());
    }
}
