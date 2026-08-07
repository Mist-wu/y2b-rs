use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Inspecting,
    Processing,
    Downloading,
    Segmenting,
    Translating,
    Rendering,
    ReadyToUpload,
    Uploading,
    UploadRetryWait,
    UploadedOriginalPendingSubtitle,
    Appending,
    Completed,
    RetryWait,
    Paused,
    DeadLetter,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum TransferMode {
    Direct,
    #[default]
    Translated,
}

macro_rules! serde_enum_display_fromstr {
    ($ty:ty) => {
        impl Display for $ty {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{}",
                    serde_json::to_value(self).unwrap().as_str().unwrap()
                )
            }
        }
        impl FromStr for $ty {
            type Err = anyhow::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(serde_json::from_value(serde_json::Value::String(
                    s.to_owned(),
                ))?)
            }
        }
    };
}
serde_enum_display_fromstr!(TransferMode);

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::DeadLetter | Self::Failed)
    }
}

serde_enum_display_fromstr!(JobStatus);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Channel {
    pub id: i64,
    pub youtube_channel_id: String,
    pub name: String,
    pub url: String,
    pub enabled: bool,
    pub transfer_mode: TransferMode,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub channel_id: Option<i64>,
    pub video_id: String,
    pub url: String,
    pub title: Option<String>,
    pub status: JobStatus,
    pub transfer_mode: TransferMode,
    pub published_at: Option<DateTime<Utc>>,
    pub youtube_updated_at: Option<DateTime<Utc>>,
    pub discovered_at: DateTime<Utc>,
    pub is_short: bool,
    pub duration_seconds: Option<f64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub bvid: Option<String>,
    pub provider: Option<String>,
    pub ai_model: Option<String>,
    pub thinking: Option<String>,
    pub attempt: i64,
    pub error: Option<String>,
    /// CC 字幕补交队列已自动尝试的次数（与流水线的 `attempt` 独立计数）。
    pub subtitle_attempt: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageRun {
    pub id: i64,
    pub job_id: String,
    pub stage: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub peak_rss_kib: Option<i64>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub thinking: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiUsage {
    pub input: i64,
    pub output: i64,
    pub reasoning: i64,
    pub cache_read: i64,
    pub cache_write: i64,
    pub total: i64,
    pub cost: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicationMetadata {
    pub title: String,
    pub dynamic: String,
    pub tags: Vec<String>,
    pub tid: i64,
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreparedUpload {
    Submission {
        video_path: String,
        cover_path: String,
        mode: TransferMode,
        completion_status: JobStatus,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoMetadata {
    pub id: String,
    pub url: String,
    pub title: String,
    pub description: Option<String>,
    pub uploader: Option<String>,
    pub upload_date: Option<String>,
    pub channel: Option<String>,
    pub channel_id: Option<String>,
    pub timestamp: Option<i64>,
    pub duration: Option<f64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub fps: Option<f64>,
    pub thumbnail_url: Option<String>,
    pub webpage_url: Option<String>,
    pub live_status: Option<String>,
}

/// 2024-10-30 起 YouTube 将 Shorts 时长上限从 60 秒提升到 180 秒。
/// 发布时间早于该时间戳的视频按旧规则（60 秒）判定，之后按新规则。
const SHORTS_DURATION_60S_CUTOFF: i64 = 1_728_950_400;

impl VideoMetadata {
    pub fn is_short(&self) -> bool {
        let vertical_or_square = matches!((self.width, self.height), (Some(w), Some(h)) if h >= w);
        let shorts_url = self.url.contains("/shorts/")
            || self
                .webpage_url
                .as_deref()
                .is_some_and(|u| u.contains("/shorts/"));
        let max_duration = if self
            .timestamp
            .is_some_and(|ts| ts < SHORTS_DURATION_60S_CUTOFF)
        {
            60.0
        } else {
            180.0
        };
        (vertical_or_square || shorts_url) && self.duration.unwrap_or(f64::MAX) <= max_duration
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn video(ts: i64, duration: f64, w: i64, h: i64) -> VideoMetadata {
        VideoMetadata {
            id: "x".into(),
            url: "https://youtube.com/watch?v=x".into(),
            title: "x".into(),
            description: None,
            uploader: None,
            upload_date: None,
            channel: None,
            channel_id: None,
            timestamp: Some(ts),
            duration: Some(duration),
            width: Some(w),
            height: Some(h),
            fps: None,
            thumbnail_url: None,
            webpage_url: None,
            live_status: None,
        }
    }
    #[test]
    fn shorts_rules() {
        assert!(video(1_800_000_000, 180.0, 1080, 1920).is_short());
        assert!(!video(1_700_000_000, 61.0, 1080, 1920).is_short());
        assert!(!video(1_800_000_000, 180.0, 1920, 1080).is_short());
    }
}
