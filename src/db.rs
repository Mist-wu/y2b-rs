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

#[derive(Debug, Clone, Copy)]
pub struct DiscoveryQuota {
    pub used: u32,
    pub reset_at: DateTime<Utc>,
    pub allowed: bool,
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
pub const BILIBILI_UPLOAD_HOLD_OWNER: &str = "bilibili.upload_hold_owner";
/// 当前二进制能够完整理解的数据库迁移版本。
pub const CURRENT_SCHEMA_VERSION: i64 = 19;

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("打开数据库失败: {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let db = Self {
            connection: Arc::new(Mutex::new(conn)),
            claim_owner: Arc::from(Uuid::new_v4().to_string()),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        self.conn().execute_batch(r#"
        CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS channels(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          youtube_channel_id TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL, url TEXT NOT NULL, feed_url TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          transfer_mode TEXT NOT NULL DEFAULT 'translated', baseline_at TEXT,
          priority TEXT NOT NULL DEFAULT 'normal',
          last_checked_at TEXT, last_reconcile_at TEXT, last_error TEXT,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs(
          id TEXT PRIMARY KEY, channel_id INTEGER REFERENCES channels(id),
          video_id TEXT NOT NULL UNIQUE, url TEXT NOT NULL, title TEXT,
          status TEXT NOT NULL, transfer_mode TEXT NOT NULL DEFAULT 'translated',
          published_at TEXT, youtube_updated_at TEXT,
          discovered_at TEXT NOT NULL, is_short INTEGER NOT NULL DEFAULT 0,
          duration_seconds REAL, width INTEGER, height INTEGER, fps REAL,
          bvid TEXT, append_to_bvid TEXT, provider TEXT, ai_model TEXT, thinking TEXT,
          attempt INTEGER NOT NULL DEFAULT 0, error TEXT,
          raw_video_path TEXT, rendered_path TEXT, subtitle_path TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, discovered_at);
        CREATE TABLE IF NOT EXISTS stage_runs(
          id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL REFERENCES jobs(id),
          stage TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 1,
          started_at TEXT NOT NULL, finished_at TEXT, duration_ms INTEGER,
          peak_rss_kib INTEGER, provider TEXT, model TEXT, thinking TEXT, detail TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_stage_job ON stage_runs(job_id, id);
        CREATE TABLE IF NOT EXISTS ai_calls(
          id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL REFERENCES jobs(id),
          stage_run_id INTEGER REFERENCES stage_runs(id), task TEXT NOT NULL,
          provider TEXT NOT NULL, model TEXT NOT NULL, thinking TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'success', error TEXT,
          input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
          reasoning_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0,
          cache_write_tokens INTEGER DEFAULT 0, total_tokens INTEGER DEFAULT 0,
          cost REAL, duration_ms INTEGER, input_json TEXT, output_json TEXT,
          created_at TEXT NOT NULL, finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT, level TEXT NOT NULL,
          message TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS commands(
          id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, payload TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'queued', created_at TEXT NOT NULL, processed_at TEXT, error TEXT
        );
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
        INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(1, CURRENT_TIMESTAMP);
        "#)?;
        // v2: ai_calls 记录完整请求/响应 JSON，便于审计与排障。
        for column in ["input_json", "output_json"] {
            if !self.has_column("ai_calls", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE ai_calls ADD COLUMN {column} TEXT"),
                    [],
                )?;
            }
        }
        self.conn()
            .execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(2,CURRENT_TIMESTAMP)",[])?;
        // v3: jobs 支持把翻译版追加到已投稿的原稿分P。
        if !self.has_column("jobs", "append_to_bvid")? {
            self.conn()
                .execute("ALTER TABLE jobs ADD COLUMN append_to_bvid TEXT", [])?;
        }
        self.conn()
            .execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(3,CURRENT_TIMESTAMP)",[])?;
        // v4: channels/jobs 记录转移模式（direct 或 translated）。
        for table in ["channels", "jobs"] {
            if !self.has_column(table, "transfer_mode")? {
                self.conn().execute(
                    &format!("ALTER TABLE {table} ADD COLUMN transfer_mode TEXT NOT NULL DEFAULT 'translated'"),
                    [],
                )?;
            }
        }
        self.conn()
            .execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(4,CURRENT_TIMESTAMP)",[])?;
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS publication_metadata(
              job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
              title TEXT NOT NULL, dynamic TEXT NOT NULL, tags_json TEXT NOT NULL,
              tid INTEGER NOT NULL, raw_json TEXT NOT NULL, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(5,CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS ai_usage_rollups(
              job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
              input_tokens INTEGER NOT NULL DEFAULT 0,
              output_tokens INTEGER NOT NULL DEFAULT 0,
              reasoning_tokens INTEGER NOT NULL DEFAULT 0,
              cache_read_tokens INTEGER NOT NULL DEFAULT 0,
              cache_write_tokens INTEGER NOT NULL DEFAULT 0,
              total_tokens INTEGER NOT NULL DEFAULT 0,
              cost REAL,
              updated_at TEXT NOT NULL
            );
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(6,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v7: 上传调度器可在重启后直接恢复投稿，无需重新准备成片和来源元数据。
        for column in ["source_metadata_json", "prepared_upload_json"] {
            if !self.has_column("jobs", column)? {
                self.conn()
                    .execute(&format!("ALTER TABLE jobs ADD COLUMN {column} TEXT"), [])?;
            }
        }
        self.conn()
            .execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(7,CURRENT_TIMESTAMP)",[])?;
        // v8: CC 字幕补交独立成队列，需要自己的重试计数和下次可领取时间。
        // 旧行的 subtitle_retry_at 为 NULL，视为立即到期，各获得一次补交机会。
        for (column, definition) in [
            ("subtitle_attempt", "INTEGER NOT NULL DEFAULT 0"),
            ("subtitle_retry_at", "TEXT"),
        ] {
            if !self.has_column("jobs", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE jobs ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        self.conn()
            .execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(8,CURRENT_TIMESTAMP)",[])?;
        // v9: 重试退避改为按尝试次数指数增长，需要显式的下次可领取时间。
        // 旧行为 NULL，按迁移前的固定 10 分钟处理（见 next_queued_job）。
        if !self.has_column("jobs", "retry_at")? {
            self.conn()
                .execute("ALTER TABLE jobs ADD COLUMN retry_at TEXT", [])?;
        }
        self.conn()
            .execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(9,CURRENT_TIMESTAMP)",[])?;
        // v10: 永久超时长的视频不能只放在进程内 TTL；服务重启或 30 分钟后反复
        // 拉元数据会浪费请求并放大 YouTube 429。记录判定时的上限；配置放宽后会
        // 自动重新检查，而不是永久锁死。
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS over_duration_videos(
              video_id TEXT PRIMARY KEY,
              channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
              limit_seconds INTEGER NOT NULL,
              detail TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_over_duration_channel
              ON over_duration_videos(channel_id);
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(10,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v11: 发现调度和 RSS 熔断必须跨进程重启保留，避免 Restart=on-failure
        // 每次都从满速请求重新开始。
        for (column, definition) in [
            ("next_poll_at", "TEXT"),
            ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !self.has_column("channels", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE channels ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS discovery_state(
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_channels_next_poll
              ON channels(enabled, next_poll_at);
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(11,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v12: 所有发现源只写候选；独立 gate worker 统一做元数据与策略判定。
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS video_candidates(
              video_id TEXT PRIMARY KEY,
              channel_id INTEGER REFERENCES channels(id),
              url TEXT NOT NULL,
              title TEXT,
              published_at TEXT,
              source TEXT NOT NULL,
              discovered_at TEXT NOT NULL,
              gate_state TEXT NOT NULL,
              gate_attempts INTEGER NOT NULL DEFAULT 0,
              next_gate_at TEXT,
              last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_video_candidates_gate
              ON video_candidates(gate_state, next_gate_at, discovered_at);
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(12,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v13: 缓存 uploads 播放列表；配额数值继续放 discovery_state，便于原子更新。
        if !self.has_column("channels", "uploads_playlist_id")? {
            self.conn().execute(
                "ALTER TABLE channels ADD COLUMN uploads_playlist_id TEXT",
                [],
            )?;
        }
        self.conn().execute(
            "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(13,CURRENT_TIMESTAMP)",
            [],
        )?;
        // v14: 每频道 WebSub 租约、HMAC secret 和不可猜回调路径。
        for (column, definition) in [
            ("websub_lease_expires_at", "TEXT"),
            ("websub_secret", "TEXT"),
            ("websub_callback_path", "TEXT"),
        ] {
            if !self.has_column("channels", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE channels ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        self.conn().execute_batch(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_websub_callback
              ON channels(websub_callback_path)
              WHERE websub_callback_path IS NOT NULL;
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(14,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v15: 每频道 Data API 预测调度、ETag、WebSub 最近推送，以及源语言标记。
        for (column, definition) in [
            ("next_data_api_poll_at", "TEXT"),
            ("data_api_etag", "TEXT"),
            ("websub_last_received_at", "TEXT"),
        ] {
            if !self.has_column("channels", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE channels ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        for (column, definition) in [
            ("source_language", "TEXT"),
            ("source_language_mismatch", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !self.has_column("video_candidates", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE video_candidates ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        self.conn().execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_channels_next_data_api_poll
              ON channels(enabled, next_data_api_poll_at);
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(15,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v16: AI 审计先登记调用、再写回结果。即使 Pi 输出无法解析、进程失败或
        // future 被取消，也会留下一条明确状态，而不是从费用统计中静默消失。
        for (column, definition) in [
            ("status", "TEXT NOT NULL DEFAULT 'success'"),
            ("error", "TEXT"),
            ("finished_at", "TEXT"),
        ] {
            if !self.has_column("ai_calls", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE ai_calls ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        self.conn().execute(
            "UPDATE ai_calls SET finished_at=created_at WHERE finished_at IS NULL AND status='success'",
            [],
        )?;
        self.conn().execute(
            "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(16,CURRENT_TIMESTAMP)",
            [],
        )?;
        // v17: 频道分为普通和优先两类。优先级同时控制发现轮询和任务队列顺序；
        // 旧频道保持 normal，避免迁移后意外改变现有调度。
        if !self.has_column("channels", "priority")? {
            self.conn().execute(
                "ALTER TABLE channels ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal'",
                [],
            )?;
        }
        self.conn().execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_channels_priority_next_poll
              ON channels(enabled, priority, next_poll_at);
            CREATE INDEX IF NOT EXISTS idx_channels_priority_next_data_api_poll
              ON channels(enabled, priority, next_data_api_poll_at);
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(17,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v18: 准备与 CC 字幕 worker 使用数据库租约原子领取。这样 watch、run 和
        // 手工 subtitle 命令并存时，同一任务也只会交给一个执行者。
        for (column, definition) in [
            ("claim_kind", "TEXT"),
            ("claim_owner", "TEXT"),
            ("claim_expires_at", "TEXT"),
        ] {
            if !self.has_column("jobs", column)? {
                self.conn().execute(
                    &format!("ALTER TABLE jobs ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        self.conn().execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_claim
              ON jobs(status, claim_kind, claim_expires_at, discovered_at);
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(18,CURRENT_TIMESTAMP);
            "#,
        )?;
        // v19: 每次真正调用 biliup 前持久化 attempt。进程在外部投稿与本地确认
        // 之间退出时，任务进入 upload_uncertain，绝不自动重投。
        self.conn().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS upload_attempts(
              id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
              status TEXT NOT NULL,
              bvid TEXT,
              detail TEXT,
              started_at TEXT NOT NULL,
              finished_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_upload_attempts_job
              ON upload_attempts(job_id, started_at DESC);
            "#,
        )?;
        self.conn().execute(
            "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(?,CURRENT_TIMESTAMP)",
            [CURRENT_SCHEMA_VERSION],
        )?;
        Ok(())
    }

    /// `retry_at` 为 NULL 的旧行沿用迁移前的固定退避。
    const LEGACY_RETRY_MINUTES: i64 = 10;

    /// 到期判定：`retry_at` 到点，或旧行的 `updated_at` 已超过固定退避。
    ///
    /// 不用 SQL 日期函数——`datetime()` 的输出格式和库里存的 RFC3339 字符串
    /// 无法直接比较，两个时间点都在 Rust 侧算好再传进去。
    fn retry_due_clause(column: &str) -> String {
        format!("(retry_at IS NULL AND {column}<=?) OR retry_at<=?")
    }

    fn retry_due_params() -> (String, String) {
        let now = Utc::now();
        (
            (now - chrono::Duration::minutes(Self::LEGACY_RETRY_MINUTES)).to_rfc3339(),
            now.to_rfc3339(),
        )
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.connection.lock().expect("database mutex poisoned")
    }

    /// 判断表是否已含某列（用于幂等迁移）。
    fn has_column(&self, table: &str, column: &str) -> Result<bool> {
        let c = self.conn();
        let mut q = c.prepare(&format!("PRAGMA table_info({table})"))?;
        let names = q
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(names.iter().any(|name| name == column))
    }

    pub fn integrity_check(&self) -> Result<String> {
        Ok(self
            .conn()
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))?)
    }
    pub fn schema_version(&self) -> Result<i64> {
        Ok(self.conn().query_row(
            "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
            [],
            |r| r.get(0),
        )?)
    }

    pub fn add_channel(
        &self,
        channel_id: &str,
        name: &str,
        url: &str,
        feed_url: &str,
        transfer_mode: TransferMode,
    ) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        let c = self.conn();
        c.execute("INSERT INTO channels(youtube_channel_id,name,url,feed_url,transfer_mode,baseline_at,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(youtube_channel_id) DO UPDATE SET name=excluded.name,url=excluded.url,feed_url=excluded.feed_url,transfer_mode=excluded.transfer_mode", params![channel_id,name,url,feed_url,transfer_mode.to_string(),now,now])?;
        Ok(c.query_row(
            "SELECT id FROM channels WHERE youtube_channel_id=?",
            [channel_id],
            |r| r.get(0),
        )?)
    }

    pub fn list_channels(&self) -> Result<Vec<Channel>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {CHANNEL_COLUMNS} FROM channels ORDER BY id"
        ))?;
        Ok(q.query_map([], channel_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn channel(&self, id: i64) -> Result<Channel> {
        Ok(self.conn().query_row(
            &format!("SELECT {CHANNEL_COLUMNS} FROM channels WHERE id=?"),
            [id],
            channel_from_row,
        )?)
    }

    /// 只返回已经到期的启用频道。时间统一存成 UTC RFC3339，使用固定毫秒精度，
    /// 因而 SQL 的字典序与时间顺序一致。
    pub fn list_due_channels(&self, now: DateTime<Utc>) -> Result<Vec<Channel>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {CHANNEL_COLUMNS} FROM channels WHERE enabled=1 AND (next_poll_at IS NULL OR next_poll_at<=?) ORDER BY CASE priority WHEN 'priority' THEN 0 ELSE 1 END,COALESCE(next_poll_at,''),id"
        ))?;
        Ok(q.query_map([format_timestamp(now)], channel_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_due_priority_channels(&self, now: DateTime<Utc>) -> Result<Vec<Channel>> {
        self.list_due_channels_by_priority(now, ChannelPriority::Priority)
    }

    pub fn list_due_normal_channels(&self, now: DateTime<Utc>) -> Result<Vec<Channel>> {
        self.list_due_channels_by_priority(now, ChannelPriority::Normal)
    }

    fn list_due_channels_by_priority(
        &self,
        now: DateTime<Utc>,
        priority: ChannelPriority,
    ) -> Result<Vec<Channel>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {CHANNEL_COLUMNS} FROM channels WHERE enabled=1 AND priority=? AND (next_poll_at IS NULL OR next_poll_at<=?) ORDER BY COALESCE(next_poll_at,''),id"
        ))?;
        Ok(q.query_map(
            params![priority.to_string(), format_timestamp(now)],
            channel_from_row,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_due_data_api_channels(&self, now: DateTime<Utc>) -> Result<Vec<Channel>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {CHANNEL_COLUMNS} FROM channels WHERE enabled=1 AND (next_data_api_poll_at IS NULL OR next_data_api_poll_at<=?) ORDER BY CASE priority WHEN 'priority' THEN 0 ELSE 1 END,COALESCE(next_data_api_poll_at,''),id"
        ))?;
        Ok(q.query_map([format_timestamp(now)], channel_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn schedule_data_api_poll(
        &self,
        id: i64,
        next_poll_at: DateTime<Utc>,
        etag: Option<&str>,
    ) -> Result<()> {
        let changed = match etag {
            Some(etag) => self.conn().execute(
                "UPDATE channels SET next_data_api_poll_at=?,data_api_etag=? WHERE id=?",
                params![format_timestamp(next_poll_at), etag, id],
            )?,
            None => self.conn().execute(
                "UPDATE channels SET next_data_api_poll_at=? WHERE id=?",
                params![format_timestamp(next_poll_at), id],
            )?,
        };
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }

    pub fn set_channel_data_api_etag(&self, id: i64, etag: &str) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE channels SET data_api_etag=? WHERE id=?",
            params![etag, id],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }

    pub fn channel_publication_history(&self, id: i64) -> Result<Vec<DateTime<Utc>>> {
        let c = self.conn();
        let mut q = c.prepare(
            "SELECT published_at FROM jobs WHERE channel_id=? AND published_at IS NOT NULL ORDER BY published_at",
        )?;
        Ok(q.query_map([id], |row| Ok(parse(row.get(0)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn channel_feed(&self, id: i64) -> Result<String> {
        Ok(self
            .conn()
            .query_row("SELECT feed_url FROM channels WHERE id=?", [id], |r| {
                r.get(0)
            })?)
    }
    pub fn channel_url(&self, id: i64) -> Result<String> {
        Ok(self
            .conn()
            .query_row("SELECT url FROM channels WHERE id=?", [id], |r| r.get(0))?)
    }
    pub fn channel_transfer_mode(&self, id: i64) -> Result<TransferMode> {
        let value: String =
            self.conn()
                .query_row("SELECT transfer_mode FROM channels WHERE id=?", [id], |r| {
                    r.get(0)
                })?;
        TransferMode::from_str(&value)
    }
    pub fn channel_baseline(&self, id: i64) -> Result<Option<DateTime<Utc>>> {
        Ok(parse_opt(self.conn().query_row(
            "SELECT baseline_at FROM channels WHERE id=?",
            [id],
            |r| r.get(0),
        )?))
    }
    pub fn channel_consecutive_failures(&self, id: i64) -> Result<u32> {
        Ok(self.conn().query_row(
            "SELECT consecutive_failures FROM channels WHERE id=?",
            [id],
            |row| row.get(0),
        )?)
    }
    pub fn set_channel_uploads_playlist(
        &self,
        youtube_channel_id: &str,
        uploads_playlist_id: &str,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE channels SET uploads_playlist_id=? WHERE youtube_channel_id=?",
            params![uploads_playlist_id, youtube_channel_id],
        )?;
        if changed == 0 {
            anyhow::bail!("YouTube 频道不存在: {youtube_channel_id}")
        }
        Ok(())
    }

    /// 最近一次全量刷新后新增的频道不能等到次日才获得 uploads 播放列表。
    ///
    /// 手工 CLI 可能没有 Data API Key，只能先用 yt-dlp 建立频道记录；常驻服务
    /// 随后看到这类新记录时，应立即触发一次补刷新。用 `created_at` 与全局刷新
    /// 时间比较，可保证无效频道不会每轮都重复消耗 channels.list 配额。
    pub fn has_missing_uploads_playlist_created_after(
        &self,
        refreshed_at: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.conn().query_row(
            "SELECT EXISTS(
               SELECT 1 FROM channels
               WHERE uploads_playlist_id IS NULL
                 AND julianday(created_at) > julianday(?)
             )",
            [format_timestamp(refreshed_at)],
            |row| row.get::<_, i64>(0),
        )? != 0)
    }

    pub fn due_websub_channels(&self, renew_before: DateTime<Utc>) -> Result<Vec<WebSubChannel>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {WEBSUB_CHANNEL_COLUMNS} FROM channels WHERE enabled=1 AND (websub_lease_expires_at IS NULL OR websub_lease_expires_at<=?) ORDER BY id"
        ))?;
        Ok(
            q.query_map([format_timestamp(renew_before)], websub_channel_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        )
    }

    pub fn list_websub_channels(&self) -> Result<Vec<WebSubChannel>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {WEBSUB_CHANNEL_COLUMNS} FROM channels ORDER BY id"
        ))?;
        Ok(q.query_map([], websub_channel_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn websub_channel(&self, identifier: &str) -> Result<Option<WebSubChannel>> {
        let c = self.conn();
        if let Ok(id) = identifier.parse::<i64>() {
            return Ok(c
                .query_row(
                    &format!("SELECT {WEBSUB_CHANNEL_COLUMNS} FROM channels WHERE id=?"),
                    [id],
                    websub_channel_from_row,
                )
                .optional()?);
        }
        Ok(c.query_row(
            &format!("SELECT {WEBSUB_CHANNEL_COLUMNS} FROM channels WHERE youtube_channel_id=?"),
            [identifier],
            websub_channel_from_row,
        )
        .optional()?)
    }

    pub fn ensure_websub_credentials(
        &self,
        id: i64,
        callback_path: &str,
        secret: &str,
    ) -> Result<WebSubChannel> {
        let c = self.conn();
        let changed = c.execute(
            "UPDATE channels SET websub_callback_path=COALESCE(websub_callback_path,?),websub_secret=COALESCE(websub_secret,?) WHERE id=?",
            params![callback_path, secret, id],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(c.query_row(
            &format!("SELECT {WEBSUB_CHANNEL_COLUMNS} FROM channels WHERE id=?"),
            [id],
            websub_channel_from_row,
        )?)
    }

    pub fn websub_channel_by_callback(&self, callback_path: &str) -> Result<Option<WebSubChannel>> {
        Ok(self
            .conn()
            .query_row(
                &format!("SELECT {WEBSUB_CHANNEL_COLUMNS} FROM channels WHERE enabled=1 AND websub_callback_path=?"),
                [callback_path],
                websub_channel_from_row,
            )
            .optional()?)
    }

    pub fn mark_websub_lease(&self, id: i64, lease_expires_at: DateTime<Utc>) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE channels SET websub_lease_expires_at=? WHERE id=?",
            params![format_timestamp(lease_expires_at), id],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }

    pub fn mark_websub_received(
        &self,
        id: i64,
        received_at: DateTime<Utc>,
        next_data_api_poll_at: DateTime<Utc>,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE channels SET websub_last_received_at=?,next_data_api_poll_at=? WHERE id=?",
            params![
                format_timestamp(received_at),
                format_timestamp(next_data_api_poll_at),
                id
            ],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }
    pub fn set_channel_enabled(&self, id: i64, enabled: bool) -> Result<()> {
        self.conn().execute(
            "UPDATE channels SET enabled=? WHERE id=?",
            params![enabled as i64, id],
        )?;
        Ok(())
    }
    pub fn set_channel_transfer_mode(&self, id: i64, transfer_mode: TransferMode) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE channels SET transfer_mode=? WHERE id=?",
            params![transfer_mode.to_string(), id],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }

    pub fn set_channel_priority(&self, id: i64, priority: ChannelPriority) -> Result<()> {
        let now = format_timestamp(Utc::now());
        let changed = self.conn().execute(
            "UPDATE channels SET priority=?,next_poll_at=?,next_data_api_poll_at=? WHERE id=?",
            params![priority.to_string(), now, now, id],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }
    pub fn mark_channel_checked(&self, id: i64, error: Option<&str>) -> Result<()> {
        self.conn().execute(
            "UPDATE channels SET last_checked_at=?,last_error=? WHERE id=?",
            params![Utc::now().to_rfc3339(), error, id],
        )?;
        Ok(())
    }

    /// 一次频道轮询唯一的状态提交点。
    ///
    /// `rss_failed` 与最终 `error` 分开：RSS 失败后 yt-dlp 回退成功时应清掉旧错误，
    /// 但仍需累计 RSS 连续失败并按其退避调度。
    pub fn finish_channel_poll(
        &self,
        id: i64,
        error: Option<&str>,
        rss_failed: bool,
        next_poll_at: DateTime<Utc>,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE channels SET last_checked_at=?,last_error=?,next_poll_at=?,consecutive_failures=CASE WHEN ?=1 THEN consecutive_failures+1 ELSE 0 END WHERE id=?",
            params![
                format_timestamp(Utc::now()),
                error,
                format_timestamp(next_poll_at),
                rss_failed as i64,
                id
            ],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }

    /// 熔断时把未获探针名额的频道推迟到熔断结束，不把它记成一次失败。
    pub fn defer_channel_poll_until(&self, id: i64, next_poll_at: DateTime<Utc>) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE channels SET next_poll_at=? WHERE id=?",
            params![format_timestamp(next_poll_at), id],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
        Ok(())
    }

    pub fn get_discovery_state(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT value FROM discovery_state WHERE key=?",
                [key],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn set_discovery_state(&self, key: &str, value: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO discovery_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn delete_discovery_state(&self, key: &str) -> Result<()> {
        self.conn()
            .execute("DELETE FROM discovery_state WHERE key=?", [key])?;
        Ok(())
    }

    /// 以数据库互斥锁和事务串行预留 Data API 配额，供发现与 gate 两个任务共享。
    pub fn consume_discovery_quota(
        &self,
        units: u32,
        budget: u32,
        now: DateTime<Utc>,
        next_reset_at: DateTime<Utc>,
    ) -> Result<DiscoveryQuota> {
        let mut c = self.conn();
        let tx = c.transaction()?;
        let raw_used = tx
            .query_row(
                "SELECT value FROM discovery_state WHERE key='quota_used_today'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let raw_reset = tx
            .query_row(
                "SELECT value FROM discovery_state WHERE key='quota_reset_at'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let stored_reset = raw_reset
            .as_deref()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc));
        let reset_due = stored_reset.is_none_or(|reset_at| reset_at <= now);
        let mut used = if reset_due {
            0
        } else {
            raw_used
                .as_deref()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(0)
        };
        let reset_at = if reset_due {
            next_reset_at
        } else {
            stored_reset.expect("reset_due=false 时必有 reset_at")
        };
        let allowed = used.saturating_add(units) <= budget;
        if allowed {
            used = used.saturating_add(units);
        }
        tx.execute(
            "INSERT INTO discovery_state(key,value) VALUES('quota_used_today',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [used.to_string()],
        )?;
        tx.execute(
            "INSERT INTO discovery_state(key,value) VALUES('quota_reset_at',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [format_timestamp(reset_at)],
        )?;
        tx.commit()?;
        Ok(DiscoveryQuota {
            used,
            reset_at,
            allowed,
        })
    }

    pub fn exhaust_discovery_quota(
        &self,
        budget: u32,
        now: DateTime<Utc>,
        next_reset_at: DateTime<Utc>,
    ) -> Result<DiscoveryQuota> {
        let status = self.consume_discovery_quota(0, budget, now, next_reset_at)?;
        self.set_discovery_state("quota_used_today", &budget.to_string())?;
        Ok(DiscoveryQuota {
            used: budget,
            ..status
        })
    }

    pub fn insert_video_candidate(&self, candidate: NewVideoCandidate<'_>) -> Result<bool> {
        let changed = self.conn().execute(
            "INSERT OR IGNORE INTO video_candidates(video_id,channel_id,url,title,published_at,source,discovered_at,gate_state) VALUES(?,?,?,?,?,?,?,'pending')",
            params![
                candidate.video_id,
                candidate.channel_id,
                candidate.url,
                candidate.title,
                candidate.published_at.map(format_timestamp),
                candidate.source.to_string(),
                format_timestamp(Utc::now()),
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn due_video_candidates(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<VideoCandidate>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {CANDIDATE_COLUMNS} FROM video_candidates WHERE gate_state='pending' OR (gate_state='deferred' AND next_gate_at<=?) ORDER BY COALESCE((SELECT CASE channels.priority WHEN 'priority' THEN 1 ELSE 0 END FROM channels WHERE channels.id=video_candidates.channel_id),0) DESC,discovered_at LIMIT ?"
        ))?;
        Ok(q.query_map(
            params![format_timestamp(now), limit as i64],
            candidate_from_row,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get_video_candidate(&self, video_id: &str) -> Result<Option<VideoCandidate>> {
        Ok(self
            .conn()
            .query_row(
                &format!("SELECT {CANDIDATE_COLUMNS} FROM video_candidates WHERE video_id=?"),
                [video_id],
                candidate_from_row,
            )
            .optional()?)
    }

    pub fn mark_video_candidate_source_language(
        &self,
        video_id: &str,
        source_language: Option<&str>,
        mismatch: bool,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE video_candidates SET source_language=?,source_language_mismatch=? WHERE video_id=?",
            params![source_language, mismatch as i64, video_id],
        )?;
        if changed == 0 {
            anyhow::bail!("候选视频不存在: {video_id}")
        }
        Ok(())
    }

    pub fn defer_video_candidate(
        &self,
        video_id: &str,
        next_gate_at: DateTime<Utc>,
        error: &str,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE video_candidates SET gate_state='deferred',gate_attempts=gate_attempts+1,next_gate_at=?,last_error=? WHERE video_id=?",
            params![format_timestamp(next_gate_at), error, video_id],
        )?;
        if changed == 0 {
            anyhow::bail!("候选视频不存在: {video_id}")
        }
        Ok(())
    }

    pub fn reject_video_candidate(&self, video_id: &str, error: &str) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE video_candidates SET gate_state='rejected',gate_attempts=gate_attempts+1,next_gate_at=NULL,last_error=? WHERE video_id=?",
            params![error, video_id],
        )?;
        if changed == 0 {
            anyhow::bail!("候选视频不存在: {video_id}")
        }
        Ok(())
    }

    /// 创建任务与候选晋级在同一事务里完成。任务已存在时仍把候选标成 promoted，
    /// 但返回 false，避免重复计数。
    pub fn promote_video_candidate(
        &self,
        candidate: &VideoCandidate,
        title: Option<&str>,
        published_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        let mut c = self.conn();
        let tx = c.transaction()?;
        let transfer_mode = match candidate.channel_id {
            Some(channel_id) => tx.query_row(
                "SELECT transfer_mode FROM channels WHERE id=?",
                [channel_id],
                |row| row.get::<_, String>(0),
            )?,
            None => TransferMode::default().to_string(),
        };
        let job_id = Uuid::new_v4().to_string();
        let now = format_timestamp(Utc::now());
        let created = tx.execute(
            "INSERT OR IGNORE INTO jobs(id,channel_id,video_id,url,title,status,transfer_mode,published_at,discovered_at,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?,?, ?,?)",
            params![
                job_id,
                candidate.channel_id,
                candidate.video_id,
                candidate.url,
                title.or(candidate.title.as_deref()),
                transfer_mode,
                published_at.or(candidate.published_at).map(format_timestamp),
                now,
                now,
                now,
            ],
        )? == 1;
        let changed = tx.execute(
            "UPDATE video_candidates SET title=COALESCE(?,title),published_at=COALESCE(?,published_at),gate_state='promoted',gate_attempts=gate_attempts+1,next_gate_at=NULL,last_error=NULL WHERE video_id=?",
            params![
                title,
                published_at.map(format_timestamp),
                candidate.video_id
            ],
        )?;
        if changed == 0 {
            anyhow::bail!("候选视频不存在: {}", candidate.video_id)
        }
        tx.commit()?;
        Ok(created)
    }
    pub fn mark_channel_reconciled(&self, id: i64, error: Option<&str>) -> Result<()> {
        self.conn().execute(
            "UPDATE channels SET last_reconcile_at=?,last_error=? WHERE id=?",
            params![Utc::now().to_rfc3339(), error, id],
        )?;
        Ok(())
    }

    /// 当前时长上限不高于此前拒绝时的上限时，无需再次请求 YouTube 元数据。
    /// 上限为 0（关闭限制）或配置已放宽时返回 false，让调用方重新确认一次。
    pub fn is_over_duration_video(&self, video_id: &str, limit_seconds: u64) -> Result<bool> {
        if limit_seconds == 0 {
            return Ok(false);
        }
        let rejected_limit = self
            .conn()
            .query_row(
                "SELECT limit_seconds FROM over_duration_videos WHERE video_id=?",
                [video_id],
                |row| row.get::<_, u64>(0),
            )
            .optional()?;
        Ok(rejected_limit.is_some_and(|stored| limit_seconds <= stored))
    }

    pub fn record_over_duration_video(
        &self,
        video_id: &str,
        channel_id: Option<i64>,
        limit_seconds: u64,
        detail: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.conn().execute(
            "INSERT INTO over_duration_videos(video_id,channel_id,limit_seconds,detail,created_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(video_id) DO UPDATE SET channel_id=excluded.channel_id,limit_seconds=excluded.limit_seconds,detail=excluded.detail,updated_at=excluded.updated_at",
            params![video_id, channel_id, limit_seconds, detail, now, now],
        )?;
        Ok(())
    }

    pub fn create_job(&self, job: NewJob<'_>) -> Result<Option<String>> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let changed=self.conn().execute("INSERT OR IGNORE INTO jobs(id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?)",params![id,job.channel_id,job.video_id,job.url,job.title,job.transfer_mode.to_string(),job.published.map(|x|x.to_rfc3339()),job.updated.map(|x|x.to_rfc3339()),now,now,now])?;
        Ok((changed == 1).then_some(id))
    }

    /// 按调用方给定的 WHERE 子句查询单条任务。
    fn job_opt(&self, sql: &str, params: impl rusqlite::Params) -> Result<Option<Job>> {
        self.conn()
            .query_row(sql, params, job_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn get_job_by_video_id(&self, video_id: &str) -> Result<Option<Job>> {
        self.job_opt(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE video_id=?"),
            [video_id],
        )
    }

    pub fn get_job(&self, id: &str) -> Result<Option<Job>> {
        self.job_opt(&format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id=?"), [id])
    }
    pub fn job_by_bvid(&self, bvid: &str) -> Result<Option<Job>> {
        self.job_opt(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE bvid=?"),
            [bvid],
        )
    }
    /// 已投稿且待补 CC 字幕的任务：完成或已直传待补字幕，且有 BVID。
    pub fn jobs_awaiting_subtitle(&self) -> Result<Vec<Job>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE bvid IS NOT NULL AND bvid<>'' \
             AND status IN ('completed','uploaded_original_pending_subtitle') \
             AND (claim_owner IS NULL OR claim_expires_at IS NULL OR claim_expires_at<=?) \
             ORDER BY discovered_at"
        ))?;
        Ok(q.query_map([Utc::now().to_rfc3339()], job_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub fn list_jobs(&self, limit: usize) -> Result<Vec<Job>> {
        let c = self.conn();
        let mut q = c.prepare(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs ORDER BY discovered_at DESC LIMIT ?"
        ))?;
        Ok(q.query_map([limit as i64], job_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn claim_deadline(now: DateTime<Utc>) -> String {
        (now + chrono::Duration::seconds(CLAIM_LEASE_SECONDS)).to_rfc3339()
    }

    /// 原子领取下一条准备任务，并直接切换到 `inspecting`。
    pub fn claim_next_prepare_job(&self) -> Result<Option<Job>> {
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let legacy_before =
            (now - chrono::Duration::minutes(Self::LEGACY_RETRY_MINUTES)).to_rfc3339();
        let sql = format!(
            "UPDATE jobs SET status='inspecting',claim_kind='{PREPARE_CLAIM_KIND}',claim_owner=?1,claim_expires_at=?2,error=NULL,updated_at=?3 \
             WHERE id=(SELECT id FROM jobs \
               WHERE (status='queued' OR (status='retry_wait' AND ((retry_at IS NULL AND updated_at<=?4) OR retry_at<=?5))) \
                 AND (claim_owner IS NULL OR claim_expires_at IS NULL OR claim_expires_at<=?5) \
               ORDER BY COALESCE((SELECT CASE channels.priority WHEN 'priority' THEN 1 ELSE 0 END FROM channels WHERE channels.id=jobs.channel_id),0) DESC,discovered_at LIMIT 1) \
             RETURNING {JOB_COLUMNS}"
        );
        self.job_opt(
            &sql,
            params![
                self.claim_owner.as_ref(),
                Self::claim_deadline(now),
                &now_text,
                legacy_before,
                &now_text
            ],
        )
    }

    /// 原子领取指定准备任务，供 `y2b run` 使用。
    pub fn claim_prepare_job(&self, id: &str) -> Result<Option<Job>> {
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let legacy_before =
            (now - chrono::Duration::minutes(Self::LEGACY_RETRY_MINUTES)).to_rfc3339();
        let sql = format!(
            "UPDATE jobs SET status='inspecting',claim_kind='{PREPARE_CLAIM_KIND}',claim_owner=?1,claim_expires_at=?2,error=NULL,updated_at=?3 \
             WHERE id=?4 \
               AND (status='queued' OR (status='retry_wait' AND ((retry_at IS NULL AND updated_at<=?5) OR retry_at<=?6))) \
               AND (claim_owner IS NULL OR claim_expires_at IS NULL OR claim_expires_at<=?6) \
             RETURNING {JOB_COLUMNS}"
        );
        self.job_opt(
            &sql,
            params![
                self.claim_owner.as_ref(),
                Self::claim_deadline(now),
                &now_text,
                id,
                legacy_before,
                &now_text
            ],
        )
    }

    pub fn renew_job_claim(&self, id: &str, kind: &str) -> Result<bool> {
        let now = Utc::now();
        let changed = self.conn().execute(
            "UPDATE jobs SET claim_expires_at=? WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                Self::claim_deadline(now),
                id,
                kind,
                self.claim_owner.as_ref()
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn owns_job_claim(&self, id: &str, kind: &str) -> Result<bool> {
        Ok(self.conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=? AND claim_kind=? AND claim_owner=?)",
            params![id, kind, self.claim_owner.as_ref()],
            |row| row.get(0),
        )?)
    }

    pub fn release_job_claim(&self, id: &str, kind: &str) -> Result<bool> {
        let changed = self.conn().execute(
            "UPDATE jobs SET claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![Utc::now().to_rfc3339(), id, kind, self.claim_owner.as_ref()],
        )?;
        Ok(changed == 1)
    }

    pub fn update_claimed_job_status(
        &self,
        id: &str,
        kind: &str,
        status: JobStatus,
        error: Option<&str>,
        release: bool,
    ) -> Result<()> {
        if matches!(status, JobStatus::RetryWait | JobStatus::UploadRetryWait) {
            anyhow::bail!("重试等待状态必须用 defer_claimed_job_retry 设置退避时间: {status}")
        }
        let changed = if release {
            self.conn().execute(
                "UPDATE jobs SET status=?,error=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
                 WHERE id=? AND claim_kind=? AND claim_owner=?",
                params![
                    status.to_string(),
                    error,
                    Utc::now().to_rfc3339(),
                    id,
                    kind,
                    self.claim_owner.as_ref()
                ],
            )?
        } else {
            self.conn().execute(
                "UPDATE jobs SET status=?,error=?,updated_at=? \
                 WHERE id=? AND claim_kind=? AND claim_owner=?",
                params![
                    status.to_string(),
                    error,
                    Utc::now().to_rfc3339(),
                    id,
                    kind,
                    self.claim_owner.as_ref()
                ],
            )?
        };
        if changed != 1 {
            anyhow::bail!("任务 {id} 的 {kind} 领取权已丢失")
        }
        Ok(())
    }

    pub fn increment_claimed_attempt(&self, id: &str, kind: &str) -> Result<i64> {
        self.conn()
            .query_row(
                "UPDATE jobs SET attempt=attempt+1,updated_at=? \
                 WHERE id=? AND claim_kind=? AND claim_owner=? RETURNING attempt",
                params![Utc::now().to_rfc3339(), id, kind, self.claim_owner.as_ref()],
                |row| row.get(0),
            )
            .optional()?
            .with_context(|| format!("任务 {id} 的 {kind} 领取权已丢失"))
    }

    pub fn defer_claimed_job_retry(
        &self,
        id: &str,
        kind: &str,
        status: JobStatus,
        error: &str,
        delay_seconds: i64,
    ) -> Result<()> {
        if !matches!(status, JobStatus::RetryWait | JobStatus::UploadRetryWait) {
            anyhow::bail!("defer_claimed_job_retry 只接受重试等待状态，收到 {status}")
        }
        let now = Utc::now();
        let changed = self.conn().execute(
            "UPDATE jobs SET status=?,error=?,retry_at=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                status.to_string(),
                error,
                (now + chrono::Duration::seconds(delay_seconds)).to_rfc3339(),
                now.to_rfc3339(),
                id,
                kind,
                self.claim_owner.as_ref()
            ],
        )?;
        if changed != 1 {
            anyhow::bail!("任务 {id} 的 {kind} 领取权已丢失")
        }
        Ok(())
    }

    pub fn next_queued_job(&self) -> Result<Option<Job>> {
        let (legacy_before, now) = Self::retry_due_params();
        let due = Self::retry_due_clause("updated_at");
        self.job_opt(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE status='queued' OR (status='retry_wait' AND ({due})) ORDER BY COALESCE((SELECT CASE channels.priority WHEN 'priority' THEN 1 ELSE 0 END FROM channels WHERE channels.id=jobs.channel_id),0) DESC,discovered_at LIMIT 1"
            ),
            params![legacy_before, now],
        )
    }

    pub fn next_ready_to_upload_job(&self) -> Result<Option<Job>> {
        let (legacy_before, now) = Self::retry_due_params();
        let due = Self::retry_due_clause("updated_at");
        self.job_opt(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE status='ready_to_upload' OR (status='upload_retry_wait' AND ({due})) ORDER BY COALESCE((SELECT CASE channels.priority WHEN 'priority' THEN 1 ELSE 0 END FROM channels WHERE channels.id=jobs.channel_id),0) DESC,discovered_at LIMIT 1"
            ),
            params![legacy_before, now],
        )
    }

    /// 把任务推入退避等待：设置状态、错误和下次可领取时间。
    ///
    /// 只有这一个入口会写 `retry_wait` / `upload_retry_wait`，避免出现
    /// 忘记设 `retry_at` 而被立即重新领取的紧凑重试循环。
    pub fn defer_job_retry(
        &self,
        id: &str,
        status: JobStatus,
        error: &str,
        delay_seconds: i64,
    ) -> Result<()> {
        if !matches!(status, JobStatus::RetryWait | JobStatus::UploadRetryWait) {
            anyhow::bail!("defer_job_retry 只接受重试等待状态，收到 {status}")
        }
        let now = Utc::now();
        self.conn().execute(
            "UPDATE jobs SET status=?,error=?,retry_at=?,updated_at=? WHERE id=?",
            params![
                status.to_string(),
                error,
                (now + chrono::Duration::seconds(delay_seconds)).to_rfc3339(),
                now.to_rfc3339(),
                id
            ],
        )?;
        Ok(())
    }

    pub fn recover_incomplete_jobs(&self) -> Result<usize> {
        let c = self.conn();
        let now = Utc::now().to_rfc3339();
        // 状态串里保留 downloading/segmenting/translating/rendering：这些变体已从
        // JobStatus 移除（从未被构造），但旧数据库里可能还留着这样的行，恢复时要一并捞回。
        let mut recovered = c.execute(
            "UPDATE jobs SET status='queued',error='领取租约过期后自动恢复',retry_at=NULL,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE status IN ('inspecting','processing','downloading','segmenting','translating','rendering') \
               AND (claim_expires_at IS NULL OR claim_expires_at<=?)",
            params![&now, &now],
        )?;
        c.execute(
            "UPDATE jobs SET claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE status='uploaded_original_pending_subtitle' AND claim_kind=? \
               AND (claim_expires_at IS NULL OR claim_expires_at<=?)",
            params![&now, SUBTITLE_CLAIM_KIND, &now],
        )?;
        c.execute(
            "UPDATE upload_attempts SET status='uncertain',detail=?,finished_at=? \
             WHERE status='running' AND job_id IN(SELECT id FROM jobs WHERE status='uploading' \
               AND (claim_expires_at IS NULL OR claim_expires_at<=?))",
            params![UNCERTAIN_UPLOAD_RECOVERY_ERROR, &now, &now],
        )?;
        recovered += c.execute(
            "UPDATE jobs SET status='upload_uncertain',error=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE status='uploading' AND (claim_expires_at IS NULL OR claim_expires_at<=?)",
            params![UNCERTAIN_UPLOAD_RECOVERY_ERROR, &now, &now],
        )?;
        c.execute(
            "UPDATE stage_runs SET status='failed',finished_at=?,duration_ms=COALESCE(duration_ms,CAST(MAX(0,(julianday(?) - julianday(started_at))*86400000) AS INTEGER)),detail=COALESCE(detail,'服务重启中断阶段') \
             WHERE status='running' AND NOT EXISTS(SELECT 1 FROM jobs WHERE jobs.id=stage_runs.job_id AND jobs.claim_expires_at>?)",
            params![&now, &now, &now],
        )?;
        Ok(recovered)
    }

    pub fn update_job_status(
        &self,
        id: &str,
        status: JobStatus,
        error: Option<&str>,
    ) -> Result<()> {
        if matches!(status, JobStatus::RetryWait | JobStatus::UploadRetryWait) {
            // 走这里会留下 retry_at=NULL，被当成旧行立即到期，形成紧凑重试循环。
            anyhow::bail!("重试等待状态必须用 defer_job_retry 设置退避时间: {status}")
        }
        self.conn().execute(
            "UPDATE jobs SET status=?,error=?,updated_at=? WHERE id=?",
            params![status.to_string(), error, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }

    /// 安全暂停尚未发生不可逆投稿副作用的任务。
    ///
    /// 准备中的任务会同时撤销领取租约；旧 worker 的后续 CAS 写入会失败，心跳也
    /// 会尽快取消其 future。`uploading` 和所有已投稿状态必须拒绝，不能用暂停伪装
    /// 成一个可直接重试的状态。
    pub fn pause_job(&self, id: &str) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET status='paused',error='用户暂停',retry_at=NULL,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND status IN ('queued','retry_wait','inspecting','processing','ready_to_upload','upload_retry_wait')",
            params![Utc::now().to_rfc3339(), id],
        )?;
        if changed == 1 {
            return Ok(());
        }
        let current = self
            .get_job(id)?
            .with_context(|| format!("任务不存在: {id}"))?;
        anyhow::bail!("任务 {id} 当前状态 {} 不允许暂停", current.status)
    }

    /// 原子检查全局投稿窗口、创建 attempt 并领取任务。
    ///
    /// `live_once` 与普通调度器都通过 SQLite 的写事务串行化：要么旁路先写入
    /// hold，普通任务无法领取；要么普通任务先进入 uploading，旁路会等待它结束。
    /// 返回 attempt ID；窗口未到、存在旁路 hold 或任务状态不匹配时返回 None。
    pub fn begin_prepared_upload(&self, id: &str) -> Result<Option<String>> {
        let attempt_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let hold_owner: Option<String> = tx
            .query_row(
                "SELECT value FROM settings WHERE key=?",
                [BILIBILI_UPLOAD_HOLD_OWNER],
                |row| row.get(0),
            )
            .optional()?;
        if hold_owner.is_some() {
            return Ok(None);
        }
        let deadline: Option<String> = tx
            .query_row(
                "SELECT value FROM settings WHERE key=?",
                [NEXT_BILIBILI_SUBMIT_AT],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(deadline) = deadline {
            let deadline = DateTime::parse_from_rfc3339(&deadline)
                .with_context(|| format!("设置 {NEXT_BILIBILI_SUBMIT_AT} 的时间无效: {deadline}"))?
                .with_timezone(&Utc);
            if deadline > now {
                return Ok(None);
            }
        }
        let changed = tx.execute(
            "UPDATE jobs SET status='uploading',error=NULL,claim_kind=?,claim_owner=?,claim_expires_at=?,updated_at=? \
             WHERE id=? AND status IN ('ready_to_upload','upload_retry_wait')",
            params![
                UPLOAD_CLAIM_KIND,
                self.claim_owner.as_ref(),
                Self::claim_deadline(now),
                &now_text,
                id
            ],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        tx.execute(
            "INSERT INTO upload_attempts(id,job_id,status,started_at) VALUES(?,?,'running',?)",
            params![attempt_id, id, &now_text],
        )?;
        tx.commit()?;
        Ok(Some(attempt_id))
    }

    /// biliup 明确返回成功后，把 attempt 与任务终态放在同一事务提交。
    pub fn finish_upload_attempt(
        &self,
        id: &str,
        attempt_id: &str,
        bvid: &str,
        completion_status: JobStatus,
        mode: TransferMode,
        subtitle_delay_seconds: i64,
    ) -> Result<bool> {
        if !matches!(
            completion_status,
            JobStatus::Completed | JobStatus::UploadedOriginalPendingSubtitle
        ) {
            anyhow::bail!("投稿完成状态无效: {completion_status}")
        }
        let queue_subtitle = completion_status == JobStatus::UploadedOriginalPendingSubtitle
            || (completion_status == JobStatus::Completed && mode == TransferMode::Translated);
        let final_status = if queue_subtitle {
            JobStatus::UploadedOriginalPendingSubtitle
        } else {
            completion_status
        };
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let subtitle_retry_at = queue_subtitle
            .then(|| (now + chrono::Duration::seconds(subtitle_delay_seconds)).to_rfc3339());
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let attempt_changed = tx.execute(
            "UPDATE upload_attempts SET status='succeeded',bvid=?,detail=NULL,finished_at=? \
             WHERE id=? AND job_id=? AND status='running'",
            params![bvid, &now_text, attempt_id, id],
        )?;
        let job_changed = tx.execute(
            "UPDATE jobs SET bvid=?,status=?,error=NULL,prepared_upload_json=NULL,retry_at=NULL,subtitle_attempt=0,subtitle_retry_at=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND status='uploading' AND claim_kind=? AND claim_owner=?",
            params![
                bvid,
                final_status.to_string(),
                subtitle_retry_at,
                &now_text,
                id,
                UPLOAD_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if attempt_changed != 1 || job_changed != 1 {
            anyhow::bail!("任务 {id} 的投稿 attempt {attempt_id} 已失效")
        }
        tx.commit()?;
        Ok(queue_subtitle)
    }

    /// 平台明确拒绝、确定没有产生稿件时结束 attempt，任务仍由既有退避逻辑处理。
    pub fn fail_upload_attempt(&self, id: &str, attempt_id: &str, detail: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let attempt_changed = tx.execute(
            "UPDATE upload_attempts SET status='failed',detail=?,finished_at=? \
             WHERE id=? AND job_id=? AND status='running'",
            params![detail, &now, attempt_id, id],
        )?;
        let job_changed = tx.execute(
            "UPDATE jobs SET claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND status='uploading' AND claim_kind=? AND claim_owner=?",
            params![&now, id, UPLOAD_CLAIM_KIND, self.claim_owner.as_ref()],
        )?;
        if attempt_changed != 1 || job_changed != 1 {
            anyhow::bail!("任务 {id} 的投稿 attempt {attempt_id} 已失效")
        }
        tx.commit()?;
        Ok(())
    }

    /// 投稿结果无法可靠判断：保留上传计划并进入人工核对态。
    pub fn mark_upload_attempt_uncertain(
        &self,
        id: &str,
        attempt_id: &str,
        detail: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let attempt_changed = tx.execute(
            "UPDATE upload_attempts SET status='uncertain',detail=?,finished_at=? \
             WHERE id=? AND job_id=? AND status='running'",
            params![detail, &now, attempt_id, id],
        )?;
        let job_changed = tx.execute(
            "UPDATE jobs SET status='upload_uncertain',error=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND status='uploading' AND claim_kind=? AND claim_owner=?",
            params![
                detail,
                &now,
                id,
                UPLOAD_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if attempt_changed != 1 || job_changed != 1 {
            anyhow::bail!("任务 {id} 的投稿 attempt {attempt_id} 已失效")
        }
        tx.commit()?;
        Ok(())
    }

    /// 创作中心已核对到唯一稿件后，安全确认不确定态任务。
    pub fn confirm_uncertain_upload(
        &self,
        id: &str,
        bvid: &str,
        completion_status: JobStatus,
        mode: TransferMode,
        subtitle_delay_seconds: i64,
    ) -> Result<bool> {
        if !matches!(
            completion_status,
            JobStatus::Completed | JobStatus::UploadedOriginalPendingSubtitle
        ) {
            anyhow::bail!("投稿完成状态无效: {completion_status}")
        }
        let queue_subtitle = completion_status == JobStatus::UploadedOriginalPendingSubtitle
            || (completion_status == JobStatus::Completed && mode == TransferMode::Translated);
        let final_status = if queue_subtitle {
            JobStatus::UploadedOriginalPendingSubtitle
        } else {
            completion_status
        };
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let subtitle_retry_at = queue_subtitle
            .then(|| (now + chrono::Duration::seconds(subtitle_delay_seconds)).to_rfc3339());
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let attempt_changed = tx.execute(
            "UPDATE upload_attempts SET status='reconciled',bvid=?,detail='创作中心核对确认',finished_at=? \
             WHERE id=(SELECT id FROM upload_attempts WHERE job_id=? ORDER BY started_at DESC LIMIT 1) \
               AND status='uncertain'",
            params![bvid, &now_text, id],
        )?;
        let changed = tx.execute(
            "UPDATE jobs SET bvid=?,status=?,error=NULL,prepared_upload_json=NULL,retry_at=NULL,subtitle_attempt=0,subtitle_retry_at=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND status='upload_uncertain'",
            params![
                bvid,
                final_status.to_string(),
                subtitle_retry_at,
                &now_text,
                id
            ],
        )?;
        if attempt_changed != 1 || changed != 1 {
            anyhow::bail!("任务 {id} 不在有效的投稿结果不确定状态")
        }
        tx.commit()?;
        Ok(queue_subtitle)
    }
    pub fn update_job_metadata(
        &self,
        id: &str,
        title: &str,
        is_short: bool,
        duration: Option<f64>,
        width: Option<i64>,
        height: Option<i64>,
    ) -> Result<()> {
        self.conn().execute("UPDATE jobs SET title=?,is_short=?,duration_seconds=?,width=?,height=?,updated_at=? WHERE id=?",params![title,is_short as i64,duration,width,height,Utc::now().to_rfc3339(),id])?;
        Ok(())
    }
    pub fn set_job_model(
        &self,
        id: &str,
        provider: &str,
        model: &str,
        thinking: &str,
    ) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET provider=?,ai_model=?,thinking=?,updated_at=? WHERE id=?",
            params![provider, model, thinking, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }
    pub fn save_source_metadata(
        &self,
        id: &str,
        metadata: &crate::model::VideoMetadata,
    ) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET source_metadata_json=?,updated_at=? WHERE id=?",
            params![
                serde_json::to_string(metadata)?,
                Utc::now().to_rfc3339(),
                id
            ],
        )?;
        Ok(())
    }
    pub fn source_metadata(&self, id: &str) -> Result<Option<crate::model::VideoMetadata>> {
        let value = self
            .conn()
            .query_row(
                "SELECT source_metadata_json FROM jobs WHERE id=?",
                [id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        value
            .map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }
    pub fn queue_prepared_upload(&self, id: &str, upload: &PreparedUpload) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET prepared_upload_json=?,status='ready_to_upload',error=NULL,updated_at=? \
             WHERE id=? AND status IN ('queued','retry_wait','inspecting','processing','failed','dead_letter')",
            params![serde_json::to_string(upload)?, Utc::now().to_rfc3339(), id],
        )?;
        if changed == 0 {
            let current = self
                .get_job(id)?
                .with_context(|| format!("任务不存在: {id}"))?;
            anyhow::bail!("任务 {id} 当前状态 {} 不允许写入待上传计划", current.status)
        }
        Ok(())
    }

    pub fn queue_claimed_prepared_upload(&self, id: &str, upload: &PreparedUpload) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET prepared_upload_json=?,status='ready_to_upload',error=NULL,retry_at=NULL,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                serde_json::to_string(upload)?,
                Utc::now().to_rfc3339(),
                id,
                PREPARE_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if changed != 1 {
            anyhow::bail!("任务 {id} 的准备领取权已丢失")
        }
        Ok(())
    }
    pub fn prepared_upload(&self, id: &str) -> Result<Option<PreparedUpload>> {
        let value = self
            .conn()
            .query_row(
                "SELECT prepared_upload_json FROM jobs WHERE id=?",
                [id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        value
            .map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }
    /// 投稿成功后把任务交给 CC 字幕队列：`delay_seconds` 之后才允许领取，
    /// 因为 B站稿件刚上传时查询 bvid 会短暂返回 -404。
    pub fn queue_pending_subtitle(&self, id: &str, delay_seconds: i64) -> Result<()> {
        let retry_at = Utc::now() + chrono::Duration::seconds(delay_seconds);
        let changed = self.conn().execute(
            "UPDATE jobs SET status=?,subtitle_attempt=0,subtitle_retry_at=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? WHERE id=?",
            params![
                JobStatus::UploadedOriginalPendingSubtitle.to_string(),
                retry_at.to_rfc3339(),
                Utc::now().to_rfc3339(),
                id
            ],
        )?;
        if changed == 0 {
            anyhow::bail!("任务不存在: {id}")
        }
        Ok(())
    }

    /// 取一条到期且未耗尽重试的待补字幕任务。
    ///
    /// `subtitle_retry_at IS NULL` 是 v8 之前就停在待补状态的旧行，视为立即到期。
    pub fn next_pending_subtitle_job(&self, max_attempts: i64) -> Result<Option<Job>> {
        self.job_opt(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE status='uploaded_original_pending_subtitle' \
                 AND bvid IS NOT NULL AND bvid<>'' AND subtitle_attempt<? \
                 AND (subtitle_retry_at IS NULL OR subtitle_retry_at<=?) \
                 AND (claim_owner IS NULL OR claim_expires_at IS NULL OR claim_expires_at<=?) \
                 ORDER BY discovered_at LIMIT 1"
            ),
            params![
                max_attempts,
                Utc::now().to_rfc3339(),
                Utc::now().to_rfc3339()
            ],
        )
    }

    /// 原子领取下一条到期的 CC 字幕任务。状态保持 pending，领取权由租约区分。
    pub fn claim_next_pending_subtitle_job(&self, max_attempts: i64) -> Result<Option<Job>> {
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let sql = format!(
            "UPDATE jobs SET claim_kind='{SUBTITLE_CLAIM_KIND}',claim_owner=?1,claim_expires_at=?2,updated_at=?3 \
             WHERE id=(SELECT id FROM jobs WHERE status='uploaded_original_pending_subtitle' \
               AND bvid IS NOT NULL AND bvid<>'' AND subtitle_attempt<?4 \
               AND (subtitle_retry_at IS NULL OR subtitle_retry_at<=?3) \
               AND (claim_owner IS NULL OR claim_expires_at IS NULL OR claim_expires_at<=?3) \
               ORDER BY discovered_at LIMIT 1) \
             RETURNING {JOB_COLUMNS}"
        );
        self.job_opt(
            &sql,
            params![
                self.claim_owner.as_ref(),
                Self::claim_deadline(now),
                &now_text,
                max_attempts
            ],
        )
    }

    /// 手工补字幕也先领取，避免与自动 worker 或另一条手工命令重复提交。
    pub fn claim_subtitle_job_now(&self, id: &str) -> Result<Option<Job>> {
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let sql = format!(
            "UPDATE jobs SET claim_kind='{SUBTITLE_CLAIM_KIND}',claim_owner=?1,claim_expires_at=?2,updated_at=?3 \
             WHERE id=?4 AND bvid IS NOT NULL AND bvid<>'' \
               AND status IN ('completed','uploaded_original_pending_subtitle') \
               AND (claim_owner IS NULL OR claim_expires_at IS NULL OR claim_expires_at<=?3) \
             RETURNING {JOB_COLUMNS}"
        );
        self.job_opt(
            &sql,
            params![
                self.claim_owner.as_ref(),
                Self::claim_deadline(now),
                &now_text,
                id
            ],
        )
    }

    /// CC 补交失败：计数加一并推迟 `delay_seconds` 后才允许再次领取。
    /// 退避策略由调用方决定（不同失败原因该等的时间不同）。
    pub fn defer_pending_subtitle(&self, id: &str, error: &str, delay_seconds: i64) -> Result<()> {
        let now = Utc::now();
        self.conn().execute(
            "UPDATE jobs SET subtitle_attempt=subtitle_attempt+1,subtitle_retry_at=?,error=?,updated_at=? WHERE id=?",
            params![
                (now + chrono::Duration::seconds(delay_seconds)).to_rfc3339(),
                error,
                now.to_rfc3339(),
                id
            ],
        )?;
        Ok(())
    }

    pub fn defer_claimed_pending_subtitle(
        &self,
        id: &str,
        error: &str,
        delay_seconds: i64,
    ) -> Result<()> {
        let now = Utc::now();
        let changed = self.conn().execute(
            "UPDATE jobs SET subtitle_attempt=subtitle_attempt+1,subtitle_retry_at=?,error=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                (now + chrono::Duration::seconds(delay_seconds)).to_rfc3339(),
                error,
                now.to_rfc3339(),
                id,
                SUBTITLE_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if changed != 1 {
            anyhow::bail!("任务 {id} 的字幕领取权已丢失")
        }
        Ok(())
    }

    /// 明确不可能自动成功（例如根本没有翻译素材）时直接耗尽重试，
    /// 任务留在待补状态供 `y2b subtitle add/--all` 手动处理。
    pub fn exhaust_pending_subtitle(&self, id: &str, max_attempts: i64, error: &str) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET subtitle_attempt=?,subtitle_retry_at=NULL,error=?,updated_at=? WHERE id=?",
            params![max_attempts, error, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }

    pub fn exhaust_claimed_pending_subtitle(
        &self,
        id: &str,
        max_attempts: i64,
        error: &str,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET subtitle_attempt=?,subtitle_retry_at=NULL,error=?,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                max_attempts,
                error,
                Utc::now().to_rfc3339(),
                id,
                SUBTITLE_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if changed != 1 {
            anyhow::bail!("任务 {id} 的字幕领取权已丢失")
        }
        Ok(())
    }

    pub fn finish_subtitle_claim(&self, id: &str, mark_completed: bool) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET status=CASE WHEN ? THEN 'completed' ELSE status END,error=NULL,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                mark_completed,
                Utc::now().to_rfc3339(),
                id,
                SUBTITLE_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if changed != 1 {
            anyhow::bail!("任务 {id} 的字幕领取权已丢失")
        }
        Ok(())
    }

    /// 重新武装 CC 字幕队列：清空计数并立即到期。
    pub fn rearm_pending_subtitle(&self, id: &str) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET subtitle_attempt=0,subtitle_retry_at=?,error=NULL,updated_at=? \
             WHERE id=? AND status='uploaded_original_pending_subtitle' \
               AND (claim_owner IS NULL OR claim_expires_at IS NULL OR claim_expires_at<=?)",
            params![
                Utc::now().to_rfc3339(),
                Utc::now().to_rfc3339(),
                id,
                Utc::now().to_rfc3339()
            ],
        )?;
        if changed == 0 {
            anyhow::bail!("任务不在待补字幕状态: {id}")
        }
        Ok(())
    }

    pub fn set_job_paths(&self, id: &str, raw: Option<&str>) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET raw_video_path=COALESCE(?,raw_video_path),updated_at=? WHERE id=?",
            params![raw, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }
    pub fn set_job_bvid(&self, id: &str, bvid: &str) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET bvid=?,updated_at=? WHERE id=?",
            params![bvid, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }
    pub fn publication_metadata(&self, job_id: &str) -> Result<Option<PublicationMetadata>> {
        self.conn()
            .query_row(
                "SELECT title,dynamic,tags_json,tid,raw_json FROM publication_metadata WHERE job_id=?",
                [job_id],
                |r| {
                    let tags_json: String = r.get(2)?;
                    let tags = serde_json::from_str(&tags_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok(PublicationMetadata {
                        title: r.get(0)?,
                        dynamic: r.get(1)?,
                        tags,
                        tid: r.get(3)?,
                        raw_json: r.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }
    pub fn save_publication_metadata(
        &self,
        job_id: &str,
        metadata: &PublicationMetadata,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let tags_json = serde_json::to_string(&metadata.tags)?;
        self.conn().execute(
            "INSERT INTO publication_metadata(job_id,title,dynamic,tags_json,tid,raw_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET title=excluded.title,dynamic=excluded.dynamic,tags_json=excluded.tags_json,tid=excluded.tid,raw_json=excluded.raw_json,updated_at=excluded.updated_at",
            params![job_id, metadata.title, metadata.dynamic, tags_json, metadata.tid, metadata.raw_json, now, now],
        )?;
        Ok(())
    }
    pub fn job_paths(&self, id: &str) -> Result<(Option<String>, Option<String>, Option<String>)> {
        Ok(self.conn().query_row(
            "SELECT raw_video_path,subtitle_path,rendered_path FROM jobs WHERE id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?)
    }
    /// 递增尝试次数并返回新值。
    ///
    /// 用 `RETURNING` 一条语句完成：拆成 UPDATE + SELECT 两次取锁时，另一个进程
    /// （例如与 `y2b watch` 并存的 `y2b run`）可能在两次之间也递增，读回别人的值。
    pub fn increment_attempt(&self, id: &str) -> Result<i64> {
        Ok(self.conn().query_row(
            "UPDATE jobs SET attempt=attempt+1,updated_at=? WHERE id=? RETURNING attempt",
            params![Utc::now().to_rfc3339(), id],
            |r| r.get(0),
        )?)
    }

    pub fn retry_job(&self, id: &str) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET status=CASE WHEN prepared_upload_json IS NOT NULL THEN 'ready_to_upload' ELSE 'queued' END,attempt=0,error=NULL,retry_at=NULL,updated_at=? \
             WHERE id=? AND (status IN ('dead_letter','failed') OR (status='paused' AND error IS NOT ?))",
            params![Utc::now().to_rfc3339(), id, UNCERTAIN_UPLOAD_RECOVERY_ERROR],
        )?;
        if changed == 0 {
            let current = self
                .get_job(id)?
                .with_context(|| format!("任务不存在: {id}"))?;
            if current.status == JobStatus::Paused
                && current.error.as_deref() == Some(UNCERTAIN_UPLOAD_RECOVERY_ERROR)
            {
                anyhow::bail!(
                    "任务 {id} 的投稿结果不确定；请先在 Bilibili 确认，禁止直接重试以免重复投稿"
                )
            }
            anyhow::bail!("任务 {id} 当前状态 {} 不允许重试", current.status)
        }
        Ok(())
    }

    pub fn start_stage(
        &self,
        job_id: &str,
        stage: &str,
        provider: Option<&str>,
        model: Option<&str>,
        thinking: Option<&str>,
    ) -> Result<i64> {
        let c = self.conn();
        c.execute("INSERT INTO stage_runs(job_id,stage,status,started_at,provider,model,thinking) VALUES(?,?,'running',?,?,?,?)",params![job_id,stage,Utc::now().to_rfc3339(),provider,model,thinking])?;
        Ok(c.last_insert_rowid())
    }
    pub fn finish_stage(
        &self,
        id: i64,
        status: &str,
        duration_ms: i64,
        peak_rss_kib: u64,
        detail: Option<&str>,
    ) -> Result<()> {
        self.conn().execute("UPDATE stage_runs SET status=?,finished_at=?,duration_ms=?,peak_rss_kib=?,detail=? WHERE id=?",params![status,Utc::now().to_rfc3339(),duration_ms,peak_rss_kib as i64,detail,id])?;
        Ok(())
    }
    pub fn list_stages(&self, job_id: &str) -> Result<Vec<StageRun>> {
        let c = self.conn();
        let mut q=c.prepare("SELECT id,job_id,stage,status,started_at,finished_at,duration_ms,peak_rss_kib,provider,model,thinking,detail FROM stage_runs WHERE job_id=? ORDER BY id")?;
        Ok(q.query_map([job_id], |r| {
            Ok(StageRun {
                id: r.get(0)?,
                job_id: r.get(1)?,
                stage: r.get(2)?,
                status: r.get(3)?,
                started_at: parse(r.get(4)?),
                finished_at: parse_opt(r.get(5)?),
                duration_ms: r.get(6)?,
                peak_rss_kib: r.get(7)?,
                provider: r.get(8)?,
                model: r.get(9)?,
                thinking: r.get(10)?,
                detail: r.get(11)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn begin_ai_call(
        &self,
        job_id: &str,
        stage_id: i64,
        task: &str,
        provider: &str,
        model: &str,
        thinking: &str,
        input_json: &str,
    ) -> Result<i64> {
        let c = self.conn();
        c.execute(
            "INSERT INTO ai_calls(job_id,stage_run_id,task,provider,model,thinking,status,input_json,created_at) VALUES(?,?,?,?,?,?,'started',?,?)",
            params![job_id,stage_id,task,provider,model,thinking,input_json,Utc::now().to_rfc3339()],
        )?;
        Ok(c.last_insert_rowid())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_ai_call(
        &self,
        id: i64,
        status: &str,
        usage: &AiUsage,
        duration_ms: i64,
        output_json: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        self.conn().execute(
            "UPDATE ai_calls SET status=?,error=?,input_tokens=?,output_tokens=?,reasoning_tokens=?,cache_read_tokens=?,cache_write_tokens=?,total_tokens=?,cost=?,duration_ms=?,output_json=?,finished_at=? WHERE id=?",
            params![status,error,usage.input,usage.output,usage.reasoning,usage.cache_read,usage.cache_write,usage.total,usage.cost,duration_ms,output_json,Utc::now().to_rfc3339(),id],
        )?;
        Ok(())
    }

    pub fn interrupt_ai_call(&self, id: i64, error: &str) -> Result<()> {
        self.conn().execute(
            "UPDATE ai_calls SET status='interrupted',error=?,finished_at=? WHERE id=? AND status='started'",
            params![error,Utc::now().to_rfc3339(),id],
        )?;
        Ok(())
    }
    pub fn ai_totals(&self) -> Result<AiUsage> {
        self.conn()
            .query_row(
                &format!("SELECT {AI_USAGE_SELECT} FROM {AI_USAGE_ROWS}"),
                [],
                usage_from_row,
            )
            .map_err(Into::into)
    }
    pub fn ai_totals_for_job(&self, job_id: &str) -> Result<AiUsage> {
        self.conn()
            .query_row(
                &format!("SELECT {AI_USAGE_SELECT} FROM {AI_USAGE_ROWS} WHERE job_id=?"),
                [job_id],
                usage_from_row,
            )
            .map_err(Into::into)
    }
    pub fn ai_totals_for_channel(&self, channel_id: i64) -> Result<AiUsage> {
        self.conn().query_row(&format!("SELECT {AI_USAGE_SELECT} FROM {AI_USAGE_ROWS} JOIN jobs j ON j.id=usage.job_id WHERE j.channel_id=?"), [channel_id], usage_from_row).map_err(Into::into)
    }
    pub fn ai_tokens_today(&self) -> Result<i64> {
        let day = Utc::now().format("%Y-%m-%d").to_string();
        Ok(self.conn().query_row(
            "SELECT COALESCE(SUM(total_tokens),0) FROM ai_calls WHERE substr(created_at,1,10)=?",
            [day],
            |r| r.get(0),
        )?)
    }
    pub fn event(&self, job_id: Option<&str>, level: &str, message: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO events(job_id,level,message,created_at) VALUES(?,?,?,?)",
            params![job_id, level, message, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }
    pub fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        self.conn().execute("INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",params![key,value,Utc::now().to_rfc3339()])?;
        Ok(())
    }
    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        self.conn()
            .query_row("SELECT value FROM settings WHERE key=?", [key], |r| {
                r.get(0)
            })
            .optional()
            .map_err(Into::into)
    }
    pub fn setting_deadline_due(&self, key: &str, now: DateTime<Utc>) -> Result<bool> {
        let Some(value) = self.get_setting(key)? else {
            return Ok(true);
        };
        let deadline = DateTime::parse_from_rfc3339(&value)
            .with_context(|| format!("设置 {key} 的时间无效: {value}"))?
            .with_timezone(&Utc);
        Ok(deadline <= now)
    }
    /// 调度器的只读快速判断；真正的窗口与 hold 校验仍在 `begin_prepared_upload`
    /// 的写事务内重复执行，避免检查后领取前的竞态。
    pub fn bilibili_submission_due(&self, now: DateTime<Utc>) -> Result<bool> {
        if self.get_setting(BILIBILI_UPLOAD_HOLD_OWNER)?.is_some() {
            return Ok(false);
        }
        self.setting_deadline_due(NEXT_BILIBILI_SUBMIT_AT, now)
    }
    /// 直播回放的自动入队起点：早于该时间开播的回放不入队。
    ///
    /// 放开 `was_live` 之后，频道 RSS 和 yt-dlp 校对里积压的历史回放会被一次性
    /// 全部扫进队列。首次读取时把游标初始化为当前时间，此后只有新开播的直播
    /// 留下的回放才会被搬运。手动 `y2b jobs add` 不受此限制。
    pub fn live_replay_cutoff(&self) -> Result<DateTime<Utc>> {
        const KEY: &str = "live_replay.enqueue_after";
        if let Some(value) = self.get_setting(KEY)? {
            match DateTime::parse_from_rfc3339(&value) {
                Ok(parsed) => return Ok(parsed.with_timezone(&Utc)),
                Err(error) => tracing::warn!(
                    key = KEY,
                    value,
                    error = %error,
                    "直播回放游标无法解析，按当前时间重置"
                ),
            }
        }
        let now = Utc::now();
        self.set_setting(KEY, &now.to_rfc3339())?;
        Ok(now)
    }

    pub fn backup(&self, dest: &Path) -> Result<()> {
        if let Some(p) = dest.parent() {
            std::fs::create_dir_all(p)?;
        }
        self.conn().backup(rusqlite::MAIN_DB, dest, None)?;
        Ok(())
    }

    /// 清理超过保留期的审计/事件/阶段记录，防止 state.db 无限增长。
    /// 返回 (ai_calls, events, stage_runs) 各删除的行数。
    pub fn prune_history(&self, keep_days: i64) -> Result<(usize, usize, usize)> {
        let cutoff = (Utc::now() - chrono::Duration::days(keep_days)).to_rfc3339();
        let now = Utc::now().to_rfc3339();
        let mut c = self.conn();
        let tx = c.transaction()?;
        tx.execute(
            r#"
            INSERT INTO ai_usage_rollups(
              job_id,input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,
              cache_write_tokens,total_tokens,cost,updated_at
            )
            SELECT job_id,COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),
              COALESCE(SUM(reasoning_tokens),0),COALESCE(SUM(cache_read_tokens),0),
              COALESCE(SUM(cache_write_tokens),0),COALESCE(SUM(total_tokens),0),SUM(cost),?
            FROM ai_calls WHERE created_at < ? GROUP BY job_id
            ON CONFLICT(job_id) DO UPDATE SET
              input_tokens=ai_usage_rollups.input_tokens+excluded.input_tokens,
              output_tokens=ai_usage_rollups.output_tokens+excluded.output_tokens,
              reasoning_tokens=ai_usage_rollups.reasoning_tokens+excluded.reasoning_tokens,
              cache_read_tokens=ai_usage_rollups.cache_read_tokens+excluded.cache_read_tokens,
              cache_write_tokens=ai_usage_rollups.cache_write_tokens+excluded.cache_write_tokens,
              total_tokens=ai_usage_rollups.total_tokens+excluded.total_tokens,
              cost=CASE
                WHEN ai_usage_rollups.cost IS NULL THEN excluded.cost
                WHEN excluded.cost IS NULL THEN ai_usage_rollups.cost
                ELSE ai_usage_rollups.cost+excluded.cost
              END,
              updated_at=excluded.updated_at
            "#,
            params![now, cutoff],
        )?;
        let ai_calls = tx.execute("DELETE FROM ai_calls WHERE created_at < ?", [&cutoff])?;
        let events = tx.execute("DELETE FROM events WHERE created_at < ?", [&cutoff])?;
        let stage_runs = tx.execute("DELETE FROM stage_runs WHERE started_at < ?", [&cutoff])?;
        tx.commit()?;
        Ok((ai_calls, events, stage_runs))
    }
}

fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn channel_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Channel> {
    Ok(Channel {
        id: r.get(0)?,
        youtube_channel_id: r.get(1)?,
        name: r.get(2)?,
        url: r.get(3)?,
        enabled: r.get::<_, i64>(4)? != 0,
        transfer_mode: TransferMode::from_str(&r.get::<_, String>(5)?).unwrap_or_default(),
        priority: ChannelPriority::from_str(&r.get::<_, String>(6)?).unwrap_or_default(),
        last_checked_at: parse_opt(r.get(7)?),
        last_error: r.get(8)?,
        next_poll_at: parse_opt(r.get(9)?),
        consecutive_failures: r.get(10)?,
        uploads_playlist_id: r.get(11)?,
        next_data_api_poll_at: parse_opt(r.get(12)?),
        data_api_etag: r.get(13)?,
        websub_lease_expires_at: parse_opt(r.get(14)?),
        websub_last_received_at: parse_opt(r.get(15)?),
    })
}

fn candidate_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<VideoCandidate> {
    let source = CandidateSource::from_str(&r.get::<_, String>(5)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, error.into())
    })?;
    let gate_state = GateState::from_str(&r.get::<_, String>(7)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, error.into())
    })?;
    Ok(VideoCandidate {
        video_id: r.get(0)?,
        channel_id: r.get(1)?,
        url: r.get(2)?,
        title: r.get(3)?,
        published_at: parse_opt(r.get(4)?),
        source,
        discovered_at: parse(r.get(6)?),
        gate_state,
        gate_attempts: r.get(8)?,
        next_gate_at: parse_opt(r.get(9)?),
        last_error: r.get(10)?,
        source_language: r.get(11)?,
        source_language_mismatch: r.get::<_, i64>(12)? != 0,
    })
}

fn websub_channel_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<WebSubChannel> {
    Ok(WebSubChannel {
        id: r.get(0)?,
        youtube_channel_id: r.get(1)?,
        name: r.get(2)?,
        enabled: r.get::<_, i64>(3)? != 0,
        lease_expires_at: parse_opt(r.get(4)?),
        secret: r.get(5)?,
        callback_path: r.get(6)?,
        last_received_at: parse_opt(r.get(7)?),
    })
}

fn parse(s: String) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&s)
        .map(|x| x.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}
fn parse_opt(s: Option<String>) -> Option<DateTime<Utc>> {
    s.map(parse)
}
fn job_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    Ok(Job {
        id: r.get(0)?,
        channel_id: r.get(1)?,
        video_id: r.get(2)?,
        url: r.get(3)?,
        title: r.get(4)?,
        status: JobStatus::from_str(&r.get::<_, String>(5)?).unwrap_or(JobStatus::Failed),
        transfer_mode: TransferMode::from_str(&r.get::<_, String>(6)?).unwrap_or_default(),
        published_at: parse_opt(r.get(7)?),
        youtube_updated_at: parse_opt(r.get(8)?),
        discovered_at: parse(r.get(9)?),
        is_short: r.get::<_, i64>(10)? != 0,
        duration_seconds: r.get(11)?,
        width: r.get(12)?,
        height: r.get(13)?,
        bvid: r.get(14)?,
        provider: r.get(15)?,
        ai_model: r.get(16)?,
        thinking: r.get(17)?,
        attempt: r.get(18)?,
        error: r.get(19)?,
        subtitle_attempt: r.get(20)?,
    })
}
fn usage_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<AiUsage> {
    Ok(AiUsage {
        input: r.get(0)?,
        output: r.get(1)?,
        reasoning: r.get(2)?,
        cache_read: r.get(3)?,
        cache_write: r.get(4)?,
        total: r.get(5)?,
        cost: r.get(6)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrates_existing_ai_calls_to_audited_lifecycle() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("v15-ai-calls.db");
        let old = rusqlite::Connection::open(&path).unwrap();
        old.execute_batch(
            r#"
            CREATE TABLE ai_calls(
              id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
              stage_run_id INTEGER, task TEXT NOT NULL,
              provider TEXT NOT NULL, model TEXT NOT NULL, thinking TEXT NOT NULL,
              input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
              reasoning_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0,
              cache_write_tokens INTEGER DEFAULT 0, total_tokens INTEGER DEFAULT 0,
              cost REAL, duration_ms INTEGER, input_json TEXT, output_json TEXT,
              created_at TEXT NOT NULL
            );
            INSERT INTO ai_calls(
              job_id,task,provider,model,thinking,total_tokens,cost,created_at
            ) VALUES('legacy-job','translate','deepseek','pro','off',42,0.01,'2026-08-01T00:00:00Z');
            "#,
        )
        .unwrap();
        drop(old);

        let db = Database::open(&path).unwrap();
        assert_eq!(db.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
        let migrated: (String, Option<String>, String) = db
            .conn()
            .query_row(
                "SELECT status,error,finished_at FROM ai_calls WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            migrated,
            ("success".into(), None, "2026-08-01T00:00:00Z".into())
        );
    }

    fn source_metadata() -> crate::model::VideoMetadata {
        crate::model::VideoMetadata {
            id: "ready-video".into(),
            url: "https://youtu.be/ready-video".into(),
            title: "Ready video".into(),
            description: Some("description".into()),
            uploader: Some("uploader".into()),
            upload_date: Some("20260804".into()),
            channel: Some("channel".into()),
            channel_id: Some("UC-ready".into()),
            timestamp: Some(1_800_000_000),
            duration: Some(60.0),
            width: Some(1920),
            height: Some(1080),
            fps: Some(60.0),
            thumbnail_url: Some("https://i.ytimg.com/ready.jpg".into()),
            webpage_url: Some("https://youtube.com/watch?v=ready-video".into()),
            live_status: Some("not_live".into()),
            default_audio_language: Some("en".into()),
        }
    }

    #[test]
    fn migrates_v3_rows_to_translated_mode() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("v3.db");
        let old = rusqlite::Connection::open(&path).unwrap();
        old.execute_batch(
            r#"
            CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
            INSERT INTO schema_migrations(version, applied_at)
              VALUES(1, CURRENT_TIMESTAMP), (2, CURRENT_TIMESTAMP), (3, CURRENT_TIMESTAMP);
            CREATE TABLE channels(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              youtube_channel_id TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL, url TEXT NOT NULL, feed_url TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1, baseline_at TEXT,
              last_checked_at TEXT, last_reconcile_at TEXT, last_error TEXT,
              created_at TEXT NOT NULL
            );
            CREATE TABLE jobs(
              id TEXT PRIMARY KEY, channel_id INTEGER REFERENCES channels(id),
              video_id TEXT NOT NULL UNIQUE, url TEXT NOT NULL, title TEXT,
              status TEXT NOT NULL, published_at TEXT, youtube_updated_at TEXT,
              discovered_at TEXT NOT NULL, is_short INTEGER NOT NULL DEFAULT 0,
              duration_seconds REAL, width INTEGER, height INTEGER, fps REAL,
              bvid TEXT, append_to_bvid TEXT, provider TEXT, ai_model TEXT, thinking TEXT,
              attempt INTEGER NOT NULL DEFAULT 0, error TEXT,
              raw_video_path TEXT, rendered_path TEXT, subtitle_path TEXT,
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            INSERT INTO channels(
              youtube_channel_id, name, url, feed_url, created_at
            ) VALUES(
              'UC-v3', 'v3 channel', 'https://youtube.com/@v3',
              'https://youtube.com/feeds/videos.xml?channel_id=UC-v3', CURRENT_TIMESTAMP
            );
            INSERT INTO jobs(
              id, channel_id, video_id, url, status, discovered_at, created_at, updated_at
            ) VALUES(
              'job-v3', 1, 'video-v3', 'https://youtu.be/video-v3', 'queued',
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            );
            "#,
        )
        .unwrap();
        drop(old);

        let db = Database::open(&path).unwrap();
        assert_eq!(db.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
        assert_eq!(
            db.list_channels().unwrap()[0].transfer_mode,
            TransferMode::Translated
        );
        assert_eq!(
            db.list_channels().unwrap()[0].priority,
            ChannelPriority::Normal
        );
        assert_eq!(
            db.get_job("job-v3").unwrap().unwrap().transfer_mode,
            TransferMode::Translated
        );
    }

    #[test]
    fn schema_and_dedup() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        assert_eq!(db.integrity_check().unwrap(), "ok");
        assert_eq!(db.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
        assert!(
            db.create_job(NewJob {
                channel_id: None,
                video_id: "abc",
                url: "https://youtu.be/abc",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .is_some()
        );
        assert!(
            db.create_job(NewJob {
                channel_id: None,
                video_id: "abc",
                url: "https://youtu.be/abc",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .is_none()
        );
        db.update_job_status(&db.list_jobs(1).unwrap()[0].id, JobStatus::Processing, None)
            .unwrap();
        assert_eq!(db.recover_incomplete_jobs().unwrap(), 1);
        assert_eq!(db.list_jobs(1).unwrap()[0].status, JobStatus::Queued);
    }

    #[test]
    fn jobs_awaiting_subtitle_lists_completed_with_bvid() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        for (video_id, mode) in [
            ("done-translated", TransferMode::Translated),
            ("done-direct", TransferMode::Direct),
            ("pending", TransferMode::Translated),
            ("no-bvid", TransferMode::Translated),
        ] {
            db.create_job(NewJob {
                channel_id: None,
                video_id,
                url: &format!("https://youtu.be/{video_id}"),
                title: None,
                published: None,
                updated: None,
                transfer_mode: mode,
            })
            .unwrap()
            .unwrap();
        }
        let ids = db.list_jobs(10).unwrap();
        for (video_id, status, bvid) in [
            ("done-translated", JobStatus::Completed, Some("BV1a")),
            ("done-direct", JobStatus::Completed, Some("BV1b")),
            (
                "pending",
                JobStatus::UploadedOriginalPendingSubtitle,
                Some("BV1c"),
            ),
            ("no-bvid", JobStatus::Completed, None),
        ] {
            let job = ids.iter().find(|j| j.video_id == video_id).unwrap();
            if let Some(bvid) = bvid {
                db.set_job_bvid(&job.id, bvid).unwrap();
            }
            db.update_job_status(&job.id, status, None).unwrap();
        }
        let awaiting = db.jobs_awaiting_subtitle().unwrap();
        let mut bvids: Vec<_> = awaiting
            .iter()
            .map(|j| j.bvid.as_deref().unwrap())
            .collect();
        bvids.sort_unstable();
        assert_eq!(bvids, ["BV1a", "BV1b", "BV1c"]);
    }

    #[test]
    fn completed_direct_job_is_not_listed_as_awaiting_until_has_bvid() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "direct-completed",
                url: "https://youtu.be/direct-completed",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        db.update_job_status(&id, JobStatus::Completed, None)
            .unwrap();
        assert!(db.jobs_awaiting_subtitle().unwrap().is_empty());
        db.set_job_bvid(&id, "BV1direct").unwrap();
        assert_eq!(db.jobs_awaiting_subtitle().unwrap().len(), 1);
    }

    #[test]
    fn channel_mode_is_snapshotted_on_new_jobs() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let channel_id = db
            .add_channel(
                "UC-test",
                "test",
                "https://youtube.com/@test/videos",
                "https://youtube.com/feeds/videos.xml?channel_id=UC-test",
                TransferMode::Direct,
            )
            .unwrap();
        let first = db
            .create_job(NewJob {
                channel_id: Some(channel_id),
                video_id: "first",
                url: "https://youtu.be/first",
                title: None,
                published: None,
                updated: None,
                transfer_mode: db.channel_transfer_mode(channel_id).unwrap(),
            })
            .unwrap()
            .unwrap();
        db.set_channel_transfer_mode(channel_id, TransferMode::Translated)
            .unwrap();
        let second = db
            .create_job(NewJob {
                channel_id: Some(channel_id),
                video_id: "second",
                url: "https://youtu.be/second",
                title: None,
                published: None,
                updated: None,
                transfer_mode: db.channel_transfer_mode(channel_id).unwrap(),
            })
            .unwrap()
            .unwrap();

        assert_eq!(
            db.get_job(&first).unwrap().unwrap().transfer_mode,
            TransferMode::Direct
        );
        assert_eq!(
            db.get_job(&second).unwrap().unwrap().transfer_mode,
            TransferMode::Translated
        );
    }

    #[test]
    fn channel_priority_controls_discovery_and_both_job_queues() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("priority.db")).unwrap();
        let normal_channel = db
            .add_channel(
                "UC-normal",
                "normal",
                "https://youtube.com/@normal/videos",
                "https://youtube.com/feeds/videos.xml?channel_id=UC-normal",
                TransferMode::Direct,
            )
            .unwrap();
        let priority_channel = db
            .add_channel(
                "UC-priority",
                "priority",
                "https://youtube.com/@priority/videos",
                "https://youtube.com/feeds/videos.xml?channel_id=UC-priority",
                TransferMode::Direct,
            )
            .unwrap();
        assert_eq!(
            db.channel(priority_channel).unwrap().priority,
            ChannelPriority::Normal
        );
        db.set_channel_priority(priority_channel, ChannelPriority::Priority)
            .unwrap();

        let due_at = Utc::now() + chrono::Duration::seconds(1);
        assert_eq!(
            db.list_due_priority_channels(due_at).unwrap()[0].id,
            priority_channel
        );
        assert_eq!(
            db.list_due_normal_channels(due_at).unwrap()[0].id,
            normal_channel
        );

        let create = |channel_id, video_id: &'static str| {
            db.create_job(NewJob {
                channel_id: Some(channel_id),
                video_id,
                url: "https://youtu.be/test",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap()
        };
        let normal_job = create(normal_channel, "normal-job");
        let priority_first = create(priority_channel, "priority-first");
        let priority_second = create(priority_channel, "priority-second");
        for (id, discovered_at) in [
            (&normal_job, "2026-01-01T00:00:00.000Z"),
            (&priority_first, "2026-01-01T00:01:00.000Z"),
            (&priority_second, "2026-01-01T00:02:00.000Z"),
        ] {
            db.conn()
                .execute(
                    "UPDATE jobs SET discovered_at=? WHERE id=?",
                    params![discovered_at, id],
                )
                .unwrap();
        }
        assert_eq!(db.next_queued_job().unwrap().unwrap().id, priority_first);
        db.update_job_status(&priority_first, JobStatus::Completed, None)
            .unwrap();
        assert_eq!(db.next_queued_job().unwrap().unwrap().id, priority_second);
        db.update_job_status(&priority_second, JobStatus::Completed, None)
            .unwrap();
        assert_eq!(db.next_queued_job().unwrap().unwrap().id, normal_job);

        let normal_upload = create(normal_channel, "normal-upload");
        let priority_upload = create(priority_channel, "priority-upload");
        let upload = PreparedUpload::Submission {
            video_path: "/tmp/video.mp4".into(),
            cover_path: "/tmp/cover.jpg".into(),
            mode: TransferMode::Direct,
            completion_status: JobStatus::Completed,
        };
        db.queue_prepared_upload(&normal_upload, &upload).unwrap();
        db.queue_prepared_upload(&priority_upload, &upload).unwrap();
        assert_eq!(
            db.next_ready_to_upload_job().unwrap().unwrap().id,
            priority_upload
        );

        db.insert_video_candidate(NewVideoCandidate {
            video_id: "normal-candidate",
            channel_id: Some(normal_channel),
            url: "https://youtu.be/normal-candidate",
            title: None,
            published_at: None,
            source: CandidateSource::Rss,
        })
        .unwrap();
        db.insert_video_candidate(NewVideoCandidate {
            video_id: "priority-candidate",
            channel_id: Some(priority_channel),
            url: "https://youtu.be/priority-candidate",
            title: None,
            published_at: None,
            source: CandidateSource::Rss,
        })
        .unwrap();
        assert_eq!(
            db.due_video_candidates(Utc::now(), 10).unwrap()[0].video_id,
            "priority-candidate"
        );
    }

    #[test]
    fn setting_deadline_prevents_early_worker_claim() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("deadline.db")).unwrap();
        let now = Utc::now();
        assert!(db.setting_deadline_due("missing", now).unwrap());
        db.set_setting(
            "deadline",
            &(now + chrono::Duration::minutes(1)).to_rfc3339(),
        )
        .unwrap();
        assert!(!db.setting_deadline_due("deadline", now).unwrap());
        assert!(
            db.setting_deadline_due("deadline", now + chrono::Duration::minutes(2))
                .unwrap()
        );
    }

    #[test]
    fn ai_call_audit_tracks_failures_and_interruptions() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("ai-audit.db")).unwrap();
        let job_id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "ai-audit",
                url: "https://youtu.be/ai-audit",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        let stage_id = db
            .start_stage(
                &job_id,
                "translation",
                Some("deepseek"),
                Some("deepseek-v4-pro"),
                Some("off"),
            )
            .unwrap();

        let failed_id = db
            .begin_ai_call(
                &job_id,
                stage_id,
                "translate",
                "deepseek",
                "deepseek-v4-pro",
                "off",
                r#"{"task":"translate"}"#,
            )
            .unwrap();
        let started: String = db
            .conn()
            .query_row(
                "SELECT status FROM ai_calls WHERE id=?",
                [failed_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(started, "started");

        let usage = AiUsage {
            input: 11,
            output: 7,
            reasoning: 0,
            cache_read: 3,
            cache_write: 0,
            total: 21,
            cost: Some(0.0123),
        };
        db.finish_ai_call(
            failed_id,
            "parse_error",
            &usage,
            456,
            Some("not json"),
            Some("Pi 最终文本不是 JSON"),
        )
        .unwrap();
        let failed: (String, i64, Option<f64>, String, String, bool) = db
            .conn()
            .query_row(
                "SELECT status,total_tokens,cost,output_json,error,finished_at IS NOT NULL FROM ai_calls WHERE id=?",
                [failed_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            failed,
            (
                "parse_error".into(),
                21,
                Some(0.0123),
                "not json".into(),
                "Pi 最终文本不是 JSON".into(),
                true,
            )
        );

        let interrupted_id = db
            .begin_ai_call(
                &job_id,
                stage_id,
                "segment",
                "deepseek",
                "deepseek-v4-flash",
                "off",
                r#"{"task":"segment"}"#,
            )
            .unwrap();
        db.interrupt_ai_call(interrupted_id, "future cancelled")
            .unwrap();
        let interrupted: (String, String) = db
            .conn()
            .query_row(
                "SELECT status,error FROM ai_calls WHERE id=?",
                [interrupted_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            interrupted,
            ("interrupted".into(), "future cancelled".into())
        );

        let totals = db.ai_totals_for_job(&job_id).unwrap();
        assert_eq!(totals.total, 21);
        assert_eq!(totals.cost, Some(0.0123));
    }

    #[test]
    fn publication_metadata_survives_reopen_and_retry() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("x.db");
        let db = Database::open(&path).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "metadata-video",
                url: "https://youtu.be/metadata-video",
                title: Some("Original title"),
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        let metadata = PublicationMetadata {
            title: "中文标题".into(),
            dynamic: "本期展示精彩对局。".into(),
            tags: vec!["荒野乱斗".into(), "排位赛".into()],
            tid: 172,
            raw_json: r#"{"title":"中文标题"}"#.into(),
        };
        db.save_publication_metadata(&id, &metadata).unwrap();
        db.defer_job_retry(&id, JobStatus::RetryWait, "test", 600)
            .unwrap();
        drop(db);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.publication_metadata(&id).unwrap(), Some(metadata));
    }

    #[test]
    fn retry_wait_jobs_observe_backoff_before_reclaim() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "backoff-video",
                url: "https://youtu.be/backoff-video",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        // 退避未到期不可领取。
        db.defer_job_retry(&id, JobStatus::RetryWait, "rate limited", 600)
            .unwrap();
        assert!(db.next_queued_job().unwrap().is_none());

        // 到期后可以领取。
        db.defer_job_retry(&id, JobStatus::RetryWait, "rate limited", -1)
            .unwrap();
        assert_eq!(db.next_queued_job().unwrap().unwrap().id, id);

        // 忘记设退避时间的写法直接被拒绝，避免紧凑重试循环。
        assert!(
            db.update_job_status(&id, JobStatus::RetryWait, Some("boom"))
                .is_err()
        );

        // v9 之前的行没有 retry_at，沿用固定 10 分钟退避。
        let now = Utc::now().to_rfc3339();
        db.conn()
            .execute(
                "UPDATE jobs SET status='retry_wait',retry_at=NULL,updated_at=? WHERE id=?",
                params![now, id],
            )
            .unwrap();
        assert!(db.next_queued_job().unwrap().is_none());
        let old = (Utc::now() - chrono::Duration::minutes(11)).to_rfc3339();
        db.conn()
            .execute("UPDATE jobs SET updated_at=? WHERE id=?", params![old, id])
            .unwrap();
        assert_eq!(db.next_queued_job().unwrap().unwrap().id, id);
    }

    #[test]
    fn prepare_claim_is_atomic_leased_and_rejects_stale_owner_writes() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("claims.db");
        let first = Database::open(&path).unwrap();
        let id = first
            .create_job(NewJob {
                channel_id: None,
                video_id: "claimed-prepare",
                url: "https://youtu.be/claimed-prepare",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        let second = Database::open(&path).unwrap();

        let claimed = first.claim_next_prepare_job().unwrap().unwrap();
        assert_eq!(claimed.id, id);
        assert_eq!(claimed.status, JobStatus::Inspecting);
        assert!(second.claim_next_prepare_job().unwrap().is_none());
        // 启动第二个进程不能把仍有有效租约的工作误判成重启残留。
        assert_eq!(second.recover_incomplete_jobs().unwrap(), 0);

        first
            .conn()
            .execute(
                "UPDATE jobs SET claim_expires_at=? WHERE id=?",
                params![(Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(), id],
            )
            .unwrap();
        assert_eq!(second.recover_incomplete_jobs().unwrap(), 1);
        assert_eq!(second.claim_next_prepare_job().unwrap().unwrap().id, id);
        assert!(
            first
                .update_claimed_job_status(
                    &id,
                    PREPARE_CLAIM_KIND,
                    JobStatus::Processing,
                    None,
                    false,
                )
                .is_err()
        );
    }

    #[test]
    fn subtitle_claim_blocks_automatic_and_manual_duplicate_submission() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("subtitle-claims.db");
        let first = Database::open(&path).unwrap();
        let id = first
            .create_job(NewJob {
                channel_id: None,
                video_id: "claimed-subtitle",
                url: "https://youtu.be/claimed-subtitle",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        first.set_job_bvid(&id, "BV1claimed").unwrap();
        first.queue_pending_subtitle(&id, -1).unwrap();
        let second = Database::open(&path).unwrap();

        assert_eq!(
            first
                .claim_next_pending_subtitle_job(16)
                .unwrap()
                .unwrap()
                .id,
            id
        );
        assert!(
            second
                .claim_next_pending_subtitle_job(16)
                .unwrap()
                .is_none()
        );
        assert!(second.claim_subtitle_job_now(&id).unwrap().is_none());
        assert!(second.jobs_awaiting_subtitle().unwrap().is_empty());

        first
            .defer_claimed_pending_subtitle(&id, "temporary", -1)
            .unwrap();
        assert_eq!(
            second
                .claim_next_pending_subtitle_job(16)
                .unwrap()
                .unwrap()
                .id,
            id
        );
        assert!(
            first
                .defer_claimed_pending_subtitle(&id, "stale", 60)
                .is_err()
        );
    }

    #[test]
    fn preparation_and_upload_queues_are_independent_and_durable() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("x.db");
        let db = Database::open(&path).unwrap();
        let upload_id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "ready-video",
                url: "https://youtu.be/ready-video",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        let queued_id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "queued-video",
                url: "https://youtu.be/queued-video",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        let plan = PreparedUpload::Submission {
            video_path: "/tmp/ready.mp4".into(),
            cover_path: "/tmp/ready.jpg".into(),
            mode: TransferMode::Translated,
            completion_status: JobStatus::Completed,
        };
        db.save_source_metadata(&upload_id, &source_metadata())
            .unwrap();
        db.queue_prepared_upload(&upload_id, &plan).unwrap();

        assert_eq!(db.next_queued_job().unwrap().unwrap().id, queued_id);
        assert_eq!(
            db.next_ready_to_upload_job().unwrap().unwrap().id,
            upload_id
        );
        drop(db);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.recover_incomplete_jobs().unwrap(), 0);
        let source = reopened.source_metadata(&upload_id).unwrap().unwrap();
        assert_eq!(source.id, "ready-video");
        assert_eq!(source.uploader.as_deref(), Some("uploader"));
        assert_eq!(reopened.prepared_upload(&upload_id).unwrap(), Some(plan));
        assert_eq!(
            reopened.next_ready_to_upload_job().unwrap().unwrap().id,
            upload_id
        );
    }

    #[test]
    fn upload_retry_wait_has_backoff_without_blocking_preparation_queue() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let upload_id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "upload-retry",
                url: "https://youtu.be/upload-retry",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        let queued_id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "prepare-now",
                url: "https://youtu.be/prepare-now",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        db.defer_job_retry(&upload_id, JobStatus::UploadRetryWait, "test", 600)
            .unwrap();

        // 上传退避不影响准备队列继续推进。
        assert!(db.next_ready_to_upload_job().unwrap().is_none());
        assert_eq!(db.next_queued_job().unwrap().unwrap().id, queued_id);

        db.defer_job_retry(&upload_id, JobStatus::UploadRetryWait, "test", -1)
            .unwrap();
        assert_eq!(
            db.next_ready_to_upload_job().unwrap().unwrap().id,
            upload_id
        );
    }

    #[test]
    fn finishing_upload_attempt_atomically_queues_translated_cc() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "upload-ready",
                url: "https://youtu.be/upload-ready",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        db.queue_prepared_upload(
            &id,
            &PreparedUpload::Submission {
                video_path: "/tmp/upload.mp4".into(),
                cover_path: "/tmp/cover.jpg".into(),
                mode: TransferMode::Translated,
                completion_status: JobStatus::Completed,
            },
        )
        .unwrap();
        let attempt_id = db.begin_prepared_upload(&id).unwrap().unwrap();
        // 任一事务前置条件不满足时，任务状态和上传计划都不能写一半。
        assert!(
            db.finish_upload_attempt(
                &id,
                "wrong-attempt",
                "BV1uxE16ZE7e",
                JobStatus::Completed,
                TransferMode::Translated,
                90,
            )
            .is_err()
        );
        assert_eq!(
            db.get_job(&id).unwrap().unwrap().status,
            JobStatus::Uploading
        );
        assert!(db.prepared_upload(&id).unwrap().is_some());

        assert!(
            db.finish_upload_attempt(
                &id,
                &attempt_id,
                "BV1uxE16ZE7e",
                JobStatus::Completed,
                TransferMode::Translated,
                90,
            )
            .unwrap()
        );
        let job = db.get_job(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::UploadedOriginalPendingSubtitle);
        assert_eq!(job.bvid.as_deref(), Some("BV1uxE16ZE7e"));
        assert!(db.prepared_upload(&id).unwrap().is_none());
        assert!(db.next_pending_subtitle_job(16).unwrap().is_none());
        let attempt: (String, Option<String>) = db
            .conn()
            .query_row(
                "SELECT status,bvid FROM upload_attempts WHERE id=?",
                [&attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(attempt, ("succeeded".into(), Some("BV1uxE16ZE7e".into())));
    }

    #[test]
    fn upload_claim_atomically_observes_deadline_and_live_hold() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("upload-window.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "upload-window",
                url: "https://youtu.be/upload-window",
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
                video_path: "/tmp/upload-window.mp4".into(),
                cover_path: "/tmp/upload-window.jpg".into(),
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
        assert!(db.begin_prepared_upload(&id).unwrap().is_none());
        assert_eq!(
            db.get_job(&id).unwrap().unwrap().status,
            JobStatus::ReadyToUpload
        );

        db.set_setting(
            NEXT_BILIBILI_SUBMIT_AT,
            &(Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
        )
        .unwrap();
        db.set_setting(BILIBILI_UPLOAD_HOLD_OWNER, "live-once:test")
            .unwrap();
        assert!(db.begin_prepared_upload(&id).unwrap().is_none());
        assert_eq!(
            db.get_job(&id).unwrap().unwrap().status,
            JobStatus::ReadyToUpload
        );

        db.conn()
            .execute(
                "DELETE FROM settings WHERE key=?",
                [BILIBILI_UPLOAD_HOLD_OWNER],
            )
            .unwrap();
        assert!(db.begin_prepared_upload(&id).unwrap().is_some());
        assert_eq!(
            db.get_job(&id).unwrap().unwrap().status,
            JobStatus::Uploading
        );
    }

    #[test]
    fn original_without_subtitles_also_observes_initial_cc_delay() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("original-cc.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "original-cc",
                url: "https://youtu.be/original-cc",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        db.queue_prepared_upload(
            &id,
            &PreparedUpload::Submission {
                video_path: "/tmp/original-cc.mp4".into(),
                cover_path: "/tmp/original-cc.jpg".into(),
                mode: TransferMode::Direct,
                completion_status: JobStatus::UploadedOriginalPendingSubtitle,
            },
        )
        .unwrap();
        let attempt_id = db.begin_prepared_upload(&id).unwrap().unwrap();
        assert!(
            db.finish_upload_attempt(
                &id,
                &attempt_id,
                "BV17x411w7KC",
                JobStatus::UploadedOriginalPendingSubtitle,
                TransferMode::Direct,
                90,
            )
            .unwrap()
        );
        assert_eq!(
            db.get_job(&id).unwrap().unwrap().status,
            JobStatus::UploadedOriginalPendingSubtitle
        );
        assert!(db.next_pending_subtitle_job(16).unwrap().is_none());
    }

    #[test]
    fn active_upload_lease_is_not_recovered_but_expired_attempt_becomes_uncertain() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("upload-recovery.db");
        let first = Database::open(&path).unwrap();
        let id = first
            .create_job(NewJob {
                channel_id: None,
                video_id: "uncertain-upload",
                url: "https://youtu.be/uncertain-upload",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        first
            .queue_prepared_upload(
                &id,
                &PreparedUpload::Submission {
                    video_path: "/tmp/uncertain.mp4".into(),
                    cover_path: "/tmp/uncertain.jpg".into(),
                    mode: TransferMode::Direct,
                    completion_status: JobStatus::Completed,
                },
            )
            .unwrap();
        let attempt_id = first.begin_prepared_upload(&id).unwrap().unwrap();
        let second = Database::open(&path).unwrap();

        assert_eq!(second.recover_incomplete_jobs().unwrap(), 0);
        assert!(second.begin_prepared_upload(&id).unwrap().is_none());
        assert_eq!(
            second.get_job(&id).unwrap().unwrap().status,
            JobStatus::Uploading
        );
        first
            .conn()
            .execute(
                "UPDATE jobs SET claim_expires_at=? WHERE id=?",
                params![(Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(), id],
            )
            .unwrap();

        assert_eq!(second.recover_incomplete_jobs().unwrap(), 1);
        let job = second.get_job(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::UploadUncertain);
        assert!(second.retry_job(&id).is_err());
        assert!(second.pause_job(&id).is_err());
        assert!(second.prepared_upload(&id).unwrap().is_some());
        let attempt_status: String = second
            .conn()
            .query_row(
                "SELECT status FROM upload_attempts WHERE id=?",
                [&attempt_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempt_status, "uncertain");
    }

    #[test]
    fn creator_reconciliation_confirms_uncertain_attempt_without_retrying() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("upload-confirm.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "confirmed-upload",
                url: "https://youtu.be/confirmed-upload",
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
                video_path: "/tmp/confirmed.mp4".into(),
                cover_path: "/tmp/confirmed.jpg".into(),
                mode: TransferMode::Direct,
                completion_status: JobStatus::Completed,
            },
        )
        .unwrap();
        let attempt_id = db.begin_prepared_upload(&id).unwrap().unwrap();
        db.mark_upload_attempt_uncertain(&id, &attempt_id, "lost response")
            .unwrap();
        assert!(
            !db.confirm_uncertain_upload(
                &id,
                "BV17x411w7KC",
                JobStatus::Completed,
                TransferMode::Direct,
                90,
            )
            .unwrap()
        );

        let job = db.get_job(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.bvid.as_deref(), Some("BV17x411w7KC"));
        assert!(db.prepared_upload(&id).unwrap().is_none());
        let status: String = db
            .conn()
            .query_row(
                "SELECT status FROM upload_attempts WHERE id=?",
                [&attempt_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "reconciled");
    }

    #[test]
    fn recovery_closes_running_stage_rows() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "interrupted-video",
                url: "https://youtu.be/interrupted-video",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        db.update_job_status(&id, JobStatus::Processing, None)
            .unwrap();
        let stage = db.start_stage(&id, "render", None, None, None).unwrap();

        assert_eq!(db.recover_incomplete_jobs().unwrap(), 1);
        let row: (String, Option<String>, Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT status,finished_at,duration_ms,detail FROM stage_runs WHERE id=?",
                [stage],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(row.0, "failed");
        assert!(row.1.is_some());
        assert!(row.2.is_some_and(|duration| duration >= 0));
        assert_eq!(row.3.as_deref(), Some("服务重启中断阶段"));
    }

    #[test]
    fn retry_job_resets_attempts_and_error() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "retry-video",
                url: "https://youtu.be/retry-video",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        db.increment_attempt(&id).unwrap();
        db.update_job_status(&id, JobStatus::DeadLetter, Some("failed"))
            .unwrap();

        db.retry_job(&id).unwrap();
        let job = db.get_job(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.attempt, 0);
        assert!(job.error.is_none());
    }

    #[test]
    fn retry_job_preserves_completed_preparation() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "retry-upload",
                url: "https://youtu.be/retry-upload",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        let plan = PreparedUpload::Submission {
            video_path: "/tmp/retry.mp4".into(),
            cover_path: "/tmp/retry.jpg".into(),
            mode: TransferMode::Direct,
            completion_status: JobStatus::Completed,
        };
        db.queue_prepared_upload(&id, &plan).unwrap();
        db.increment_attempt(&id).unwrap();
        db.pause_job(&id).unwrap();

        db.retry_job(&id).unwrap();
        let job = db.get_job(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::ReadyToUpload);
        assert_eq!(job.attempt, 0);
        assert!(job.error.is_none());
        assert_eq!(db.prepared_upload(&id).unwrap(), Some(plan));
    }

    #[test]
    fn retry_job_rejects_completed_active_and_uncertain_upload_states() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "unsafe-retry",
                url: "https://youtu.be/unsafe-retry",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();

        for status in [
            JobStatus::Queued,
            JobStatus::Inspecting,
            JobStatus::Processing,
            JobStatus::ReadyToUpload,
            JobStatus::Uploading,
            JobStatus::UploadUncertain,
            JobStatus::UploadRetryWait,
            JobStatus::Completed,
            JobStatus::RetryWait,
        ] {
            if matches!(status, JobStatus::RetryWait | JobStatus::UploadRetryWait) {
                db.defer_job_retry(&id, status, "waiting", 60).unwrap();
            } else {
                db.update_job_status(&id, status, None).unwrap();
            }
            assert!(db.retry_job(&id).is_err(), "{status} 不应允许手动重试");
            assert_eq!(db.get_job(&id).unwrap().unwrap().status, status);
        }

        db.update_job_status(
            &id,
            JobStatus::Paused,
            Some(UNCERTAIN_UPLOAD_RECOVERY_ERROR),
        )
        .unwrap();
        let error = db.retry_job(&id).unwrap_err().to_string();
        assert!(error.contains("投稿结果不确定"));
        assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Paused);
    }

    #[test]
    fn pause_revokes_prepare_claim_and_stale_worker_cannot_resume_it() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("pause-claim.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "pause-active",
                url: "https://youtu.be/pause-active",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        db.claim_prepare_job(&id).unwrap().unwrap();
        assert!(db.owns_job_claim(&id, PREPARE_CLAIM_KIND).unwrap());

        db.pause_job(&id).unwrap();
        assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Paused);
        assert!(!db.owns_job_claim(&id, PREPARE_CLAIM_KIND).unwrap());
        assert!(
            db.update_claimed_job_status(
                &id,
                PREPARE_CLAIM_KIND,
                JobStatus::Processing,
                None,
                false,
            )
            .is_err()
        );
        assert!(
            db.queue_claimed_prepared_upload(
                &id,
                &PreparedUpload::Submission {
                    video_path: "/tmp/stale.mp4".into(),
                    cover_path: "/tmp/stale.jpg".into(),
                    mode: TransferMode::Direct,
                    completion_status: JobStatus::Completed,
                },
            )
            .is_err()
        );
        assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Paused);
    }

    #[test]
    fn pause_rejects_uploading_post_upload_and_terminal_states() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("pause-states.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "pause-states",
                url: "https://youtu.be/pause-states",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();

        for status in [
            JobStatus::Uploading,
            JobStatus::UploadUncertain,
            JobStatus::UploadedOriginalPendingSubtitle,
            JobStatus::Completed,
            JobStatus::DeadLetter,
            JobStatus::Failed,
        ] {
            db.update_job_status(&id, status, None).unwrap();
            assert!(db.pause_job(&id).is_err(), "{status} 不应允许暂停");
            assert_eq!(db.get_job(&id).unwrap().unwrap().status, status);
        }
    }

    #[test]
    fn paused_job_cannot_be_overwritten_by_generic_preparation_completion() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("pause-plan.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "pause-plan",
                url: "https://youtu.be/pause-plan",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap();
        db.pause_job(&id).unwrap();
        assert!(
            db.queue_prepared_upload(
                &id,
                &PreparedUpload::Submission {
                    video_path: "/tmp/paused.mp4".into(),
                    cover_path: "/tmp/paused.jpg".into(),
                    mode: TransferMode::Direct,
                    completion_status: JobStatus::Completed,
                },
            )
            .is_err()
        );
        assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Paused);
    }

    #[test]
    fn recovery_still_rescues_statuses_removed_from_the_enum() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("s.db")).unwrap();
        let mut ids = Vec::new();
        // downloading/segmenting/translating/rendering 已从 JobStatus 移除（从未被
        // 构造），但旧数据库里可能还有这样的行，重启恢复必须照样把它们捞回队列。
        for (n, legacy) in ["downloading", "segmenting", "translating", "rendering"]
            .into_iter()
            .enumerate()
        {
            let id = db
                .create_job(NewJob {
                    channel_id: None,
                    video_id: &format!("legacy-{n}"),
                    url: "https://youtu.be/legacy",
                    title: None,
                    published: None,
                    updated: None,
                    transfer_mode: TransferMode::Translated,
                })
                .unwrap()
                .unwrap();
            db.conn()
                .execute("UPDATE jobs SET status=? WHERE id=?", params![legacy, id])
                .unwrap();
            // 未知状态串读回来会退化成 Failed，而不是 panic。
            assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Failed);
            ids.push(id);
        }
        assert_eq!(db.recover_incomplete_jobs().unwrap(), 4);
        for id in ids {
            assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Queued);
        }
    }

    #[test]
    fn pending_subtitle_queue_respects_delay_backoff_and_attempt_cap() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("s.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "cc-queue",
                url: "https://youtu.be/cc-queue",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        db.set_job_bvid(&id, "BV1cc").unwrap();

        // 首次入队要等 delay 过去才可领取（B站刚上传时查 bvid 会 -404）。
        db.queue_pending_subtitle(&id, 90).unwrap();
        assert!(db.next_pending_subtitle_job(8).unwrap().is_none());

        db.queue_pending_subtitle(&id, -1).unwrap();
        let due = db.next_pending_subtitle_job(8).unwrap().unwrap();
        assert_eq!(due.id, id);
        assert_eq!(due.subtitle_attempt, 0);

        // 失败后计数加一并推迟；未到期不再被领取。
        db.defer_pending_subtitle(&id, "boom", 600).unwrap();
        assert!(db.next_pending_subtitle_job(8).unwrap().is_none());
        db.defer_pending_subtitle(&id, "boom", -1).unwrap();
        let retried = db.next_pending_subtitle_job(8).unwrap().unwrap();
        assert_eq!(retried.subtitle_attempt, 2);
        assert_eq!(retried.error.as_deref(), Some("boom"));

        // 计数达到上限后不再自动领取，但仍留在待补状态供手动补交。
        db.exhaust_pending_subtitle(&id, 8, "缺少素材").unwrap();
        assert!(db.next_pending_subtitle_job(8).unwrap().is_none());
        assert_eq!(db.jobs_awaiting_subtitle().unwrap().len(), 1);

        // 重新武装后立即到期、计数归零。
        db.rearm_pending_subtitle(&id).unwrap();
        let rearmed = db.next_pending_subtitle_job(8).unwrap().unwrap();
        assert_eq!(rearmed.subtitle_attempt, 0);
        assert_eq!(rearmed.error, None);

        // 已完成的任务不属于这个队列；rearm 也只对待补状态生效。
        db.update_job_status(&id, JobStatus::Completed, None)
            .unwrap();
        assert!(db.next_pending_subtitle_job(8).unwrap().is_none());
        assert!(db.rearm_pending_subtitle(&id).is_err());
    }

    #[test]
    fn pending_subtitle_queue_picks_up_pre_v8_rows_immediately() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("s.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "legacy-cc",
                url: "https://youtu.be/legacy-cc",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        db.set_job_bvid(&id, "BV1legacy").unwrap();
        // v8 之前就停在待补状态的行：subtitle_retry_at 为 NULL，应视为立即到期。
        db.update_job_status(&id, JobStatus::UploadedOriginalPendingSubtitle, None)
            .unwrap();
        assert_eq!(
            db.next_pending_subtitle_job(8).unwrap().map(|j| j.id),
            Some(id)
        );
    }

    #[test]
    fn pending_subtitle_queue_skips_jobs_without_bvid() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("s.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "no-bvid",
                url: "https://youtu.be/no-bvid",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        db.queue_pending_subtitle(&id, -1).unwrap();
        assert!(db.next_pending_subtitle_job(8).unwrap().is_none());
    }

    #[test]
    fn paused_prepared_job_cannot_be_claimed_for_upload() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "pause-upload",
                url: "https://youtu.be/pause-upload",
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
                video_path: "/tmp/pause.mp4".into(),
                cover_path: "/tmp/pause.jpg".into(),
                mode: TransferMode::Direct,
                completion_status: JobStatus::Completed,
            },
        )
        .unwrap();
        db.pause_job(&id).unwrap();

        assert!(db.begin_prepared_upload(&id).unwrap().is_none());
        assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Paused);

        db.retry_job(&id).unwrap();
        assert!(db.begin_prepared_upload(&id).unwrap().is_some());
        assert_eq!(
            db.get_job(&id).unwrap().unwrap().status,
            JobStatus::Uploading
        );
    }

    #[test]
    fn over_duration_rejection_is_persistent_but_respects_config_changes() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("duration.db");
        let db = Database::open(&path).unwrap();
        db.record_over_duration_video("too-long", None, 7200, "9000s > 7200s")
            .unwrap();
        assert!(db.is_over_duration_video("too-long", 7200).unwrap());
        assert!(db.is_over_duration_video("too-long", 3600).unwrap());
        assert!(!db.is_over_duration_video("too-long", 8000).unwrap());
        assert!(!db.is_over_duration_video("too-long", 0).unwrap());
        drop(db);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
        assert!(reopened.is_over_duration_video("too-long", 7200).unwrap());
        reopened
            .record_over_duration_video("too-long", None, 8000, "9000s > 8000s")
            .unwrap();
        assert!(reopened.is_over_duration_video("too-long", 8000).unwrap());
    }

    #[test]
    fn prune_history_removes_only_rows_older_than_retention() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let channel_id = db
            .add_channel(
                "UC-prune",
                "prune",
                "https://youtube.com/@prune/videos",
                "https://youtube.com/feeds/videos.xml?channel_id=UC-prune",
                TransferMode::Translated,
            )
            .unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: Some(channel_id),
                video_id: "prune-video",
                url: "https://youtu.be/prune-video",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        let old = (Utc::now() - chrono::Duration::days(90)).to_rfc3339();
        let fresh = Utc::now().to_rfc3339();
        for (stamp, input_tokens, total_tokens, cost) in
            [(&old, 10, 12, Some(0.25)), (&fresh, 20, 24, None)]
        {
            db.conn()
                .execute(
                    "INSERT INTO ai_calls(job_id,task,provider,model,thinking,input_tokens,total_tokens,cost,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    params![id, "t", "p", "m", "h", input_tokens, total_tokens, cost, stamp],
                )
                .unwrap();
            db.conn()
                .execute(
                    "INSERT INTO events(job_id,level,message,created_at) VALUES(?,?,?,?)",
                    params![id, "info", "msg", stamp],
                )
                .unwrap();
            db.conn()
                .execute(
                    "INSERT INTO stage_runs(job_id,stage,status,started_at) VALUES(?,?,?,?)",
                    params![id, "s", "running", stamp],
                )
                .unwrap();
        }

        let before = db.ai_totals().unwrap();
        assert_eq!(before.input, 30);
        assert_eq!(before.total, 36);
        assert_eq!(before.cost, Some(0.25));
        let (ai, events, stages) = db.prune_history(30).unwrap();
        assert_eq!((ai, events, stages), (1, 1, 1));
        for usage in [
            db.ai_totals().unwrap(),
            db.ai_totals_for_job(&id).unwrap(),
            db.ai_totals_for_channel(channel_id).unwrap(),
        ] {
            assert_eq!(usage.input, before.input);
            assert_eq!(usage.total, before.total);
            assert_eq!(usage.cost, before.cost);
        }
        let (ai2, events2, stages2) = db.prune_history(30).unwrap();
        assert_eq!((ai2, events2, stages2), (0, 0, 0));
        let after_second_prune = db.ai_totals().unwrap();
        assert_eq!(after_second_prune.input, before.input);
        assert_eq!(after_second_prune.total, before.total);
        assert_eq!(after_second_prune.cost, before.cost);
    }
}
