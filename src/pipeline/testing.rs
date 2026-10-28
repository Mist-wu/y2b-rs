//! 各流水线子模块共用的测试夹具。

use crate::model::VideoMetadata;
use crate::subtitle::Cue;

pub fn cue(index: usize, text: &str) -> Cue {
    Cue {
        start: index as f64 * 2.0,
        end: index as f64 * 2.0 + 1.8,
        source: text.into(),
        translation: None,
    }
}

pub fn metadata() -> VideoMetadata {
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
        default_audio_language: Some("en".into()),
    }
}

/// 实际本地 HTTP 服务，覆盖请求序列化、鉴权和响应处理，不启动替代 CLI。
pub async fn mock_ai(
    config: &mut crate::config::Config,
    directory: &std::path::Path,
    respond: impl Fn(serde_json::Value) -> (axum::http::StatusCode, serde_json::Value)
    + Send
    + Sync
    + 'static,
) {
    use axum::{Json, Router, http::HeaderMap, routing::post};
    use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};
    config.ai.auth_file = directory.join("auth.json");
    fs::write(
        &config.ai.auth_file,
        r#"{"deepseek":{"type":"api_key","key":"fixture-key"}}"#,
    )
    .unwrap();
    fs::set_permissions(&config.ai.auth_file, fs::Permissions::from_mode(0o600)).unwrap();
    config.ai.policy = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("pi/policy.json");
    let respond = Arc::new(respond);
    let app = Router::new().route(
        "/chat/completions",
        post(
            move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
                let respond = respond.clone();
                async move {
                    assert_eq!(headers["authorization"], "Bearer fixture-key");
                    assert_eq!(body["model"], "deepseek-flash");
                    assert_eq!(body["thinking"]["type"], "disabled");
                    assert_eq!(body["response_format"]["type"], "json_object");
                    assert_eq!(body["stream"], false);
                    assert!(body.get("tools").is_none());
                    let (status, value) = respond(body);
                    (status, Json(value))
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.ai.base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
}

pub fn completion(value: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "choices": [{"finish_reason": "stop", "message": {"content": value.to_string()}}],
        "usage": {"prompt_tokens": 11, "prompt_cache_hit_tokens": 3, "completion_tokens": 7, "total_tokens": 18}
    })
}
