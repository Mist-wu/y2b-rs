use crate::model::{
    AiUsage, CandidateSource, Channel, ChannelPriority, GateState, Job, JobStatus, PreparedUpload,
    PublicationMetadata, StageRun, TransferMode, VideoCandidate,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
    /// 同一进程内的 clone 共享领取者标识；另一个进程/Database::open 会得到新标识。
    claim_owner: Arc<str>,
}

pub struct NewJob<'a> {
    pub channel_id: Option<i64>,
    pub video_id: &'a str,
    pub url: &'a str,
    pub title: Option<&'a str>,
    pub published: Option<DateTime<Utc>>,
    pub updated: Option<DateTime<Utc>>,
    pub transfer_mode: TransferMode,
}

pub struct NewVideoCandidate<'a> {
    pub video_id: &'a str,
    pub channel_id: Option<i64>,
    pub url: &'a str,
    pub title: Option<&'a str>,
    pub published_at: Option<DateTime<Utc>>,
    pub source: CandidateSource,
}

/// 投稿完成事务共同使用的字幕首次检查时间和全局投稿冷却时间。
pub struct UploadCompletionTiming {
    pub subtitle_delay_seconds: i64,
    pub next_submit_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubtitleAttempt {
    pub id: String,
    pub bvid: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubtitleAttemptDecision {
    Submit(String),
    QueryOnly(SubtitleAttempt),
}

#[derive(Debug, Clone, Copy)]
pub struct DiscoveryQuota {
    pub used: u32,
    pub reset_at: DateTime<Utc>,
    pub allowed: bool,
}

/// 全局维护锁。单例行只阻止领取新任务，不撤销已经发出的任务租约。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MaintenanceHold {
    pub owner: String,
    pub reason: String,
    pub acquired_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// 维护锁的获取、接管、续租和释放审计记录。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MaintenanceHoldEvent {
    pub id: i64,
    pub action: String,
    pub owner: String,
    pub previous_owner: Option<String>,
    pub reason: String,
    pub previous_reason: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

/// 维护前空闲判定中的一类阻塞项。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MaintenanceBlocker {
    pub kind: String,
    pub count: usize,
    pub details: Vec<String>,
}

/// 可直接序列化给部署脚本使用的完整空闲状态。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MaintenanceStatus {
    pub checked_at: DateTime<Utc>,
    pub idle: bool,
    pub hold: Option<MaintenanceHold>,
    pub expired_hold: Option<MaintenanceHold>,
    pub blockers: Vec<MaintenanceBlocker>,
}

/// WebSub 内部订阅信息。刻意不派生 Debug/Serialize，避免 secret 被意外写入日志。
pub struct WebSubChannel {
    pub id: i64,
    pub youtube_channel_id: String,
    pub name: String,
    pub enabled: bool,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub secret: Option<String>,
    pub callback_path: Option<String>,
    pub last_received_at: Option<DateTime<Utc>>,
}

const CHANNEL_COLUMNS: &str = "id,youtube_channel_id,name,url,enabled,transfer_mode,priority,last_checked_at,last_error,next_poll_at,consecutive_failures,uploads_playlist_id,next_data_api_poll_at,data_api_etag,websub_lease_expires_at,websub_last_received_at";
const CANDIDATE_COLUMNS: &str = "video_id,channel_id,url,title,published_at,source,discovered_at,gate_state,gate_attempts,next_gate_at,last_error,source_language,source_language_mismatch";
const WEBSUB_CHANNEL_COLUMNS: &str = "id,youtube_channel_id,name,enabled,websub_lease_expires_at,websub_secret,websub_callback_path,websub_last_received_at";
/// jobs 表业务列清单，供所有按 id/video_id/队列查询复用。
const JOB_COLUMNS: &str = "id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,is_short,duration_seconds,width,height,bvid,provider,ai_model,thinking,attempt,error,subtitle_attempt";
/// ai_calls 用量聚合列，供全局/按任务/按频道汇总复用。
const AI_USAGE_SELECT: &str = "COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),COALESCE(SUM(reasoning_tokens),0),COALESCE(SUM(cache_read_tokens),0),COALESCE(SUM(cache_write_tokens),0),COALESCE(SUM(total_tokens),0),SUM(cost)";
/// 原始调用与已归档汇总的统一用量数据源。
const AI_USAGE_ROWS: &str = "(SELECT job_id,input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_write_tokens,total_tokens,cost FROM ai_calls UNION ALL SELECT job_id,input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_write_tokens,total_tokens,cost FROM ai_usage_rollups) usage";
const UNCERTAIN_UPLOAD_RECOVERY_ERROR: &str = "服务重启时上传结果不确定，请确认 Bilibili 后再处理";
const CLAIM_LEASE_SECONDS: i64 = 300;
pub(crate) const PREPARE_CLAIM_KIND: &str = "prepare";
pub(crate) const SUBTITLE_CLAIM_KIND: &str = "subtitle";
pub(crate) const UPLOAD_CLAIM_KIND: &str = "upload";
pub const NEXT_BILIBILI_SUBMIT_AT: &str = "bilibili.next_submit_at";
const LEGACY_BILIBILI_UPLOAD_HOLD_OWNER: &str = "bilibili.upload_hold_owner";
const LEGACY_BILIBILI_UPLOAD_HOLD_PREVIOUS: &str = "bilibili.upload_hold_previous";
const LEGACY_BILIBILI_UPLOAD_HOLD_UNTIL: &str = "2099-12-31T23:59:59+00:00";
/// 当前二进制能够完整理解的数据库迁移版本。
pub const CURRENT_SCHEMA_VERSION: i64 = 22;

mod discovery;
mod jobs;
mod maintenance;
mod rows;
mod schema;
mod subtitles;
mod telemetry;

#[cfg(test)]
mod tests;
