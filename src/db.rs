use crate::model::{AiUsage, Channel, Job, JobStatus, PublicationMetadata, StageRun, TransferMode};
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
        let c = self.conn();
        let mut columns = c.prepare("PRAGMA table_info(ai_calls)")?;
        let names = columns
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(columns);
        if !names.iter().any(|n| n == "input_json") {
            c.execute("ALTER TABLE ai_calls ADD COLUMN input_json TEXT", [])?;
        }
        if !names.iter().any(|n| n == "output_json") {
            c.execute("ALTER TABLE ai_calls ADD COLUMN output_json TEXT", [])?;
        }
        c.execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(2,CURRENT_TIMESTAMP)",[])?;
        let mut columns = c.prepare("PRAGMA table_info(jobs)")?;
        let names = columns
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(columns);
        if !names.iter().any(|n| n == "append_to_bvid") {
            c.execute("ALTER TABLE jobs ADD COLUMN append_to_bvid TEXT", [])?;
        }
        c.execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(3,CURRENT_TIMESTAMP)",[])?;
        let mut columns = c.prepare("PRAGMA table_info(channels)")?;
        let channel_names = columns
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(columns);
        if !channel_names.iter().any(|n| n == "transfer_mode") {
            c.execute(
                "ALTER TABLE channels ADD COLUMN transfer_mode TEXT NOT NULL DEFAULT 'translated'",
                [],
            )?;
        }
        let mut columns = c.prepare("PRAGMA table_info(jobs)")?;
        let job_names = columns
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(columns);
        if !job_names.iter().any(|n| n == "transfer_mode") {
            c.execute(
                "ALTER TABLE jobs ADD COLUMN transfer_mode TEXT NOT NULL DEFAULT 'translated'",
                [],
            )?;
        }
        c.execute("INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(4,CURRENT_TIMESTAMP)",[])?;
        c.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS publication_metadata(
              job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
              title TEXT NOT NULL, dynamic TEXT NOT NULL, tags_json TEXT NOT NULL,
              tid INTEGER NOT NULL, raw_json TEXT NOT NULL, created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            INSERT OR IGNORE INTO schema_migrations(version,applied_at)
              VALUES(5,CURRENT_TIMESTAMP);
            "#,
        )?;
        Ok(())
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.0.lock().expect("database mutex poisoned")
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

    pub fn create_job(&self, job: NewJob<'_>) -> Result<Option<String>> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let changed=self.conn().execute("INSERT OR IGNORE INTO jobs(id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?)",params![id,job.channel_id,job.video_id,job.url,job.title,job.transfer_mode.to_string(),job.published.map(|x|x.to_rfc3339()),job.updated.map(|x|x.to_rfc3339()),now,now,now])?;
        Ok((changed == 1).then_some(id))
    }

    pub fn get_job_by_video_id(&self, video_id: &str) -> Result<Option<Job>> {
        self.conn().query_row("SELECT id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,is_short,duration_seconds,width,height,bvid,append_to_bvid,provider,ai_model,thinking,attempt,error FROM jobs WHERE video_id=?",[video_id],job_from_row).optional().map_err(Into::into)
    }

    pub fn get_job(&self, id: &str) -> Result<Option<Job>> {
        self.conn().query_row("SELECT id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,is_short,duration_seconds,width,height,bvid,append_to_bvid,provider,ai_model,thinking,attempt,error FROM jobs WHERE id=?",[id],job_from_row).optional().map_err(Into::into)
    }
    pub fn list_jobs(&self, limit: usize) -> Result<Vec<Job>> {
        let c = self.conn();
        let mut q=c.prepare("SELECT id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,is_short,duration_seconds,width,height,bvid,append_to_bvid,provider,ai_model,thinking,attempt,error FROM jobs ORDER BY discovered_at DESC LIMIT ?")?;
        Ok(q.query_map([limit as i64], job_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub fn next_queued_job(&self) -> Result<Option<Job>> {
        self.conn().query_row("SELECT id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,is_short,duration_seconds,width,height,bvid,append_to_bvid,provider,ai_model,thinking,attempt,error FROM jobs WHERE status IN ('queued','retry_wait') ORDER BY discovered_at LIMIT 1",[],job_from_row).optional().map_err(Into::into)
    }

    pub fn recover_incomplete_jobs(&self) -> Result<usize> {
        let c = self.conn();
        let now = Utc::now().to_rfc3339();
        let recovered=c.execute("UPDATE jobs SET status='queued',error='服务重启后自动恢复',updated_at=? WHERE status IN ('inspecting','processing','downloading','segmenting','translating','rendering')",[&now])?;
        c.execute("UPDATE jobs SET status='paused',error='服务重启时上传或追加结果不确定，请确认 Bilibili 后手动重试',updated_at=? WHERE status IN ('uploading','appending')",[&now])?;
        Ok(recovered)
    }

    pub fn update_job_status(
        &self,
        id: &str,
        status: JobStatus,
        error: Option<&str>,
    ) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET status=?,error=?,updated_at=? WHERE id=?",
            params![status.to_string(), error, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
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
    pub fn set_job_paths(
        &self,
        id: &str,
        raw: Option<&str>,
        subtitle: Option<&str>,
        rendered: Option<&str>,
    ) -> Result<()> {
        self.conn().execute("UPDATE jobs SET raw_video_path=COALESCE(?,raw_video_path),subtitle_path=COALESCE(?,subtitle_path),rendered_path=COALESCE(?,rendered_path),updated_at=? WHERE id=?",params![raw,subtitle,rendered,Utc::now().to_rfc3339(),id])?;
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
    pub fn queue_subtitle_recheck(&self, id: &str) -> Result<()> {
        let c = self.conn();
        let (status, bvid, existing): (String, Option<String>, Option<String>) = c
            .query_row(
                "SELECT status,bvid,append_to_bvid FROM jobs WHERE id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .with_context(|| format!("任务不存在: {id}"))?;
        let target = existing
            .or_else(|| {
                (status == JobStatus::UploadedOriginalPendingSubtitle.to_string())
                    .then_some(bvid)
                    .flatten()
            })
            .filter(|value| !value.trim().is_empty())
            .with_context(|| format!("任务 {id} 没有可追加的原稿 BV"))?;
        c.execute(
            "UPDATE jobs SET status='queued',transfer_mode='translated',append_to_bvid=?,attempt=0,error=NULL,updated_at=? WHERE id=?",
            params![target, Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }
    pub fn clear_job_append_target(&self, id: &str) -> Result<()> {
        self.conn().execute(
            "UPDATE jobs SET append_to_bvid=NULL,updated_at=? WHERE id=?",
            params![Utc::now().to_rfc3339(), id],
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
    pub fn increment_attempt(&self, id: &str) -> Result<i64> {
        self.conn().execute(
            "UPDATE jobs SET attempt=attempt+1,updated_at=? WHERE id=?",
            params![Utc::now().to_rfc3339(), id],
        )?;
        Ok(self
            .conn()
            .query_row("SELECT attempt FROM jobs WHERE id=?", [id], |r| r.get(0))?)
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
        self.conn().query_row("SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),COALESCE(SUM(reasoning_tokens),0),COALESCE(SUM(cache_read_tokens),0),COALESCE(SUM(cache_write_tokens),0),COALESCE(SUM(total_tokens),0),SUM(cost) FROM ai_calls",[],|r|Ok(AiUsage{input:r.get(0)?,output:r.get(1)?,reasoning:r.get(2)?,cache_read:r.get(3)?,cache_write:r.get(4)?,total:r.get(5)?,cost:r.get(6)?})).map_err(Into::into)
    }
    pub fn ai_totals_for_job(&self, job_id: &str) -> Result<AiUsage> {
        self.conn().query_row("SELECT COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0),COALESCE(SUM(reasoning_tokens),0),COALESCE(SUM(cache_read_tokens),0),COALESCE(SUM(cache_write_tokens),0),COALESCE(SUM(total_tokens),0),SUM(cost) FROM ai_calls WHERE job_id=?",[job_id],usage_from_row).map_err(Into::into)
    }
    pub fn ai_totals_for_channel(&self, channel_id: i64) -> Result<AiUsage> {
        self.conn().query_row("SELECT COALESCE(SUM(a.input_tokens),0),COALESCE(SUM(a.output_tokens),0),COALESCE(SUM(a.reasoning_tokens),0),COALESCE(SUM(a.cache_read_tokens),0),COALESCE(SUM(a.cache_write_tokens),0),COALESCE(SUM(a.total_tokens),0),SUM(a.cost) FROM ai_calls a JOIN jobs j ON j.id=a.job_id WHERE j.channel_id=?",[channel_id],usage_from_row).map_err(Into::into)
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
    pub fn backup(&self, dest: &Path) -> Result<()> {
        if let Some(p) = dest.parent() {
            std::fs::create_dir_all(p)?;
        }
        self.conn().backup(rusqlite::MAIN_DB, dest, None)?;
        Ok(())
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
        append_to_bvid: r.get(15)?,
        provider: r.get(16)?,
        ai_model: r.get(17)?,
        thinking: r.get(18)?,
        attempt: r.get(19)?,
        error: r.get(20)?,
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
        assert_eq!(db.schema_version().unwrap(), 5);
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
        assert_eq!(db.schema_version().unwrap(), 5);
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
        db.update_job_status(&db.list_jobs(1).unwrap()[0].id, JobStatus::Rendering, None)
            .unwrap();
        assert_eq!(db.recover_incomplete_jobs().unwrap(), 1);
        assert_eq!(db.list_jobs(1).unwrap()[0].status, JobStatus::Queued);
    }

    #[test]
    fn subtitle_recheck_persists_append_target_across_recovery() {
        let t = tempfile::tempdir().unwrap();
        let db = Database::open(&t.path().join("x.db")).unwrap();
        let id = db
            .create_job(NewJob {
                channel_id: None,
                video_id: "abc",
                url: "https://youtu.be/abc",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();

        assert!(db.queue_subtitle_recheck(&id).is_err());
        db.set_job_bvid(&id, "BV1test").unwrap();
        db.update_job_status(&id, JobStatus::UploadedOriginalPendingSubtitle, None)
            .unwrap();
        db.queue_subtitle_recheck(&id).unwrap();

        let queued = db.get_job(&id).unwrap().unwrap();
        assert_eq!(queued.status, JobStatus::Queued);
        assert_eq!(queued.append_to_bvid.as_deref(), Some("BV1test"));

        db.update_job_status(&id, JobStatus::Rendering, None)
            .unwrap();
        assert_eq!(db.recover_incomplete_jobs().unwrap(), 1);
        let recovered = db.get_job(&id).unwrap().unwrap();
        assert_eq!(recovered.status, JobStatus::Queued);
        assert_eq!(recovered.append_to_bvid.as_deref(), Some("BV1test"));
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
        db.update_job_status(&id, JobStatus::RetryWait, Some("test"))
            .unwrap();
        drop(db);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.publication_metadata(&id).unwrap(), Some(metadata));
    }
}
