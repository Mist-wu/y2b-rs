use crate::model::{
    AiUsage, Channel, Job, JobStatus, PreparedUpload, PublicationMetadata, StageRun, TransferMode,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct Database(Arc<Mutex<Connection>>);

pub struct NewJob<'a> {
    pub channel_id: Option<i64>,
    pub video_id: &'a str,
    pub url: &'a str,
    pub title: Option<&'a str>,
    pub published: Option<DateTime<Utc>>,
    pub updated: Option<DateTime<Utc>>,
    pub transfer_mode: TransferMode,
}

/// jobs 表业务列清单，供所有按 id/video_id/队列查询复用。
const JOB_COLUMNS: &str = "id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,is_short,duration_seconds,width,height,bvid,provider,ai_model,thinking,attempt,error,subtitle_attempt";
/// ai_calls 用量聚合列，供全局/按任务/按频道汇总复用。
const AI_USAGE_SELECT: &str = "COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),COALESCE(SUM(reasoning_tokens),0),COALESCE(SUM(cache_read_tokens),0),COALESCE(SUM(cache_write_tokens),0),COALESCE(SUM(total_tokens),0),SUM(cost)";
/// 原始调用与已归档汇总的统一用量数据源。
const AI_USAGE_ROWS: &str = "(SELECT job_id,input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_write_tokens,total_tokens,cost FROM ai_calls UNION ALL SELECT job_id,input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_write_tokens,total_tokens,cost FROM ai_usage_rollups) usage";

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
        let db = Self(Arc::new(Mutex::new(conn)));
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
          input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
          reasoning_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0,
          cache_write_tokens INTEGER DEFAULT 0, total_tokens INTEGER DEFAULT 0,
          cost REAL, duration_ms INTEGER, input_json TEXT, output_json TEXT, created_at TEXT NOT NULL
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
        self.0.lock().expect("database mutex poisoned")
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
        let mut q=c.prepare("SELECT id,youtube_channel_id,name,url,enabled,transfer_mode,last_checked_at,last_error FROM channels ORDER BY id")?;
        Ok(q.query_map([], |r| {
            Ok(Channel {
                id: r.get(0)?,
                youtube_channel_id: r.get(1)?,
                name: r.get(2)?,
                url: r.get(3)?,
                enabled: r.get::<_, i64>(4)? != 0,
                transfer_mode: TransferMode::from_str(&r.get::<_, String>(5)?).unwrap_or_default(),
                last_checked_at: parse_opt(r.get(6)?),
                last_error: r.get(7)?,
            })
        })?
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
    pub fn mark_channel_checked(&self, id: i64, error: Option<&str>) -> Result<()> {
        self.conn().execute(
            "UPDATE channels SET last_checked_at=?,last_error=? WHERE id=?",
            params![Utc::now().to_rfc3339(), error, id],
        )?;
        Ok(())
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
            "SELECT {JOB_COLUMNS} FROM jobs WHERE bvid IS NOT NULL AND bvid<>'' AND status IN ('completed','uploaded_original_pending_subtitle') ORDER BY discovered_at"
        ))?;
        Ok(q.query_map([], job_from_row)?
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
    pub fn next_queued_job(&self) -> Result<Option<Job>> {
        let (legacy_before, now) = Self::retry_due_params();
        let due = Self::retry_due_clause("updated_at");
        self.job_opt(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE status='queued' OR (status='retry_wait' AND ({due})) ORDER BY discovered_at LIMIT 1"
            ),
            params![legacy_before, now],
        )
    }

    pub fn next_ready_to_upload_job(&self) -> Result<Option<Job>> {
        let (legacy_before, now) = Self::retry_due_params();
        let due = Self::retry_due_clause("updated_at");
        self.job_opt(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE status='ready_to_upload' OR (status='upload_retry_wait' AND ({due})) ORDER BY discovered_at LIMIT 1"
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
        let recovered=c.execute("UPDATE jobs SET status='queued',error='服务重启后自动恢复',retry_at=NULL,updated_at=? WHERE status IN ('inspecting','processing','downloading','segmenting','translating','rendering')",[&now])?;
        c.execute("UPDATE jobs SET status='paused',error='服务重启时上传结果不确定，请确认 Bilibili 后手动重试',updated_at=? WHERE status IN ('uploading')",[&now])?;
        c.execute(
            "UPDATE stage_runs SET status='failed',finished_at=?,duration_ms=COALESCE(duration_ms,CAST(MAX(0,(julianday(?) - julianday(started_at))*86400000) AS INTEGER)),detail=COALESCE(detail,'服务重启中断阶段') WHERE status='running'",
            params![&now, &now],
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
    pub fn claim_prepared_upload(&self, id: &str, status: JobStatus) -> Result<bool> {
        if status != JobStatus::Uploading {
            anyhow::bail!("无效的上传领取状态: {status}")
        }
        let changed = self.conn().execute(
            "UPDATE jobs SET status=?,error=NULL,updated_at=? WHERE id=? AND status IN ('ready_to_upload','upload_retry_wait')",
            params![status.to_string(), Utc::now().to_rfc3339(), id],
        )?;
        Ok(changed == 1)
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
            "UPDATE jobs SET prepared_upload_json=?,status='ready_to_upload',error=NULL,updated_at=? WHERE id=?",
            params![serde_json::to_string(upload)?, Utc::now().to_rfc3339(), id],
        )?;
        if changed == 0 {
            anyhow::bail!("任务不存在: {id}")
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
            "UPDATE jobs SET status=?,subtitle_attempt=0,subtitle_retry_at=?,updated_at=? WHERE id=?",
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
                 ORDER BY discovered_at LIMIT 1"
            ),
            params![max_attempts, Utc::now().to_rfc3339()],
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

    /// 明确不可能自动成功（例如根本没有翻译素材）时直接耗尽重试，
    /// 任务留在待补状态供 `y2b subtitle add/--all` 手动处理。
    pub fn exhaust_pending_subtitle(&self, id: &str, max_attempts: i64, error: &str) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET subtitle_attempt=?,subtitle_retry_at=NULL,error=?,updated_at=? WHERE id=?",
            params![max_attempts, error, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }

    /// 重新武装 CC 字幕队列：清空计数并立即到期。
    pub fn rearm_pending_subtitle(&self, id: &str) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET subtitle_attempt=0,subtitle_retry_at=?,error=NULL,updated_at=? WHERE id=? AND status='uploaded_original_pending_subtitle'",
            params![Utc::now().to_rfc3339(), Utc::now().to_rfc3339(), id],
        )?;
        if changed == 0 {
            anyhow::bail!("任务不在待补字幕状态: {id}")
        }
        Ok(())
    }

    pub fn finish_prepared_upload(&self, id: &str, bvid: &str, status: JobStatus) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE jobs SET bvid=?,status=?,error=NULL,prepared_upload_json=NULL,updated_at=? WHERE id=?",
            params![bvid, status.to_string(), Utc::now().to_rfc3339(), id],
        )?;
        if changed == 0 {
            anyhow::bail!("任务不存在: {id}")
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
            "UPDATE jobs SET status=CASE WHEN prepared_upload_json IS NOT NULL THEN 'ready_to_upload' ELSE 'queued' END,attempt=0,error=NULL,retry_at=NULL,updated_at=? WHERE id=?",
            params![Utc::now().to_rfc3339(), id],
        )?;
        if changed == 0 {
            anyhow::bail!("任务不存在: {id}")
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
    pub fn record_ai_call(
        &self,
        job_id: &str,
        stage_id: i64,
        task: &str,
        provider: &str,
        model: &str,
        thinking: &str,
        usage: &AiUsage,
        duration_ms: i64,
        input_json: &str,
        output_json: &str,
    ) -> Result<()> {
        self.conn().execute("INSERT INTO ai_calls(job_id,stage_run_id,task,provider,model,thinking,input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_write_tokens,total_tokens,cost,duration_ms,input_json,output_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",params![job_id,stage_id,task,provider,model,thinking,usage.input,usage.output,usage.reasoning,usage.cache_read,usage.cache_write,usage.total,usage.cost,duration_ms,input_json,output_json,Utc::now().to_rfc3339()])?;
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
        assert_eq!(db.schema_version().unwrap(), 10);
        assert_eq!(
            db.list_channels().unwrap()[0].transfer_mode,
            TransferMode::Translated
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
        assert_eq!(db.schema_version().unwrap(), 10);
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
    fn finishing_prepared_upload_is_atomic() {
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
        db.set_job_bvid(&id, "BV1original").unwrap();
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
        db.finish_prepared_upload(&id, "BV1original", JobStatus::Completed)
            .unwrap();
        let job = db.get_job(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.bvid.as_deref(), Some("BV1original"));
        assert!(db.prepared_upload(&id).unwrap().is_none());
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
        db.update_job_status(&id, JobStatus::Paused, Some("failed"))
            .unwrap();

        db.retry_job(&id).unwrap();
        let job = db.get_job(&id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::ReadyToUpload);
        assert_eq!(job.attempt, 0);
        assert!(job.error.is_none());
        assert_eq!(db.prepared_upload(&id).unwrap(), Some(plan));
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
        db.update_job_status(&id, JobStatus::Paused, None).unwrap();

        assert!(!db.claim_prepared_upload(&id, JobStatus::Uploading).unwrap());
        assert_eq!(db.get_job(&id).unwrap().unwrap().status, JobStatus::Paused);

        db.retry_job(&id).unwrap();
        assert!(db.claim_prepared_upload(&id, JobStatus::Uploading).unwrap());
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
        assert_eq!(reopened.schema_version().unwrap(), 10);
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
