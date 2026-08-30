use super::{rows::*, *};

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

    /// 打开已经迁移完成的数据库，但不执行任何迁移写入。
    ///
    /// 维护命令会与线上服务并行查询或只更新维护锁，不能顺带重跑整套迁移。
    pub fn open_existing(path: &Path) -> Result<Self> {
        anyhow::ensure!(path.is_file(), "数据库不存在: {}", path.display());
        let conn = Connection::open(path)
            .with_context(|| format!("打开数据库失败: {}", path.display()))?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let db = Self {
            connection: Arc::new(Mutex::new(conn)),
            claim_owner: Arc::from(Uuid::new_v4().to_string()),
        };
        let version = db.schema_version()?;
        anyhow::ensure!(
            version == CURRENT_SCHEMA_VERSION,
            "数据库 schema 版本不兼容：当前 v{version}，需要 v{CURRENT_SCHEMA_VERSION}"
        );
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
            "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(19,CURRENT_TIMESTAMP)",
            [],
        )?;
        // v20: 一个 BVID 只能归属一个任务。NULL 表示尚未投稿，不参与唯一性约束。
        // 先单独检查历史重复值，避免把 SQLite 的索引错误直接暴露给运维人员。
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let duplicate: Option<(String, i64)> = tx
            .query_row(
                "SELECT bvid,COUNT(*) FROM jobs WHERE bvid IS NOT NULL \
                 GROUP BY bvid HAVING COUNT(*)>1 ORDER BY bvid LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((bvid, count)) = duplicate {
            anyhow::bail!(
                "迁移 v20 失败：jobs.bvid={bvid:?} 已被 {count} 个任务重复占用，请先人工修复重复 BVID"
            )
        }
        tx.execute_batch(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_bvid_unique
              ON jobs(bvid) WHERE bvid IS NOT NULL;
            "#,
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(20,CURRENT_TIMESTAMP)",
            [],
        )?;
        tx.commit()?;
        // v21: CC 字幕提交也使用持久化 attempt。started/uncertain/confirmed/reconciled
        // 都禁止创建下一次提交；只有平台明确 rejected 后才允许新 attempt。
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS subtitle_attempts(
              id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
              bvid TEXT NOT NULL,
              status TEXT NOT NULL CHECK(status IN(
                'started','rejected','uncertain','confirmed','reconciled'
              )),
              detail TEXT,
              started_at TEXT NOT NULL,
              finished_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_subtitle_attempts_job
              ON subtitle_attempts(job_id, started_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_subtitle_attempts_blocking
              ON subtitle_attempts(job_id)
              WHERE status IN('started','uncertain','confirmed','reconciled');
            "#,
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(21,CURRENT_TIMESTAMP)",
            [],
        )?;
        tx.commit()?;
        // v22: 部署和一次性旁路共用带租约的全局维护锁。锁的单例行保留最近一次
        // 到期持有者，后续接管时可以完整记录谁接管了谁，避免崩溃后永久阻塞。
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS maintenance_hold(
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              owner TEXT NOT NULL CHECK(owner<>''),
              reason TEXT NOT NULL,
              acquired_at TEXT NOT NULL,
              heartbeat_at TEXT NOT NULL,
              expires_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS maintenance_hold_events(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              action TEXT NOT NULL CHECK(action IN('acquired','taken_over','renewed','released')),
              owner TEXT NOT NULL,
              previous_owner TEXT,
              reason TEXT NOT NULL,
              previous_reason TEXT,
              occurred_at TEXT NOT NULL,
              expires_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_maintenance_hold_events_time
              ON maintenance_hold_events(occurred_at DESC,id DESC);
            "#,
        )?;
        // v21 live_once 可能遗留 2099 年的永久投稿窗口。迁移时恢复它之前保存的
        // 冷却时间，并把持有者转换成五分钟租约；即使旧进程已经崩溃也会自然失效。
        let legacy_owner: Option<String> = tx
            .query_row(
                "SELECT value FROM settings WHERE key=?",
                [LEGACY_BILIBILI_UPLOAD_HOLD_OWNER],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(legacy_owner) = legacy_owner {
            let previous_json: Option<String> = tx
                .query_row(
                    "SELECT value FROM settings WHERE key=?",
                    [LEGACY_BILIBILI_UPLOAD_HOLD_PREVIOUS],
                    |row| row.get(0),
                )
                .optional()?;
            let previous_deadline = previous_json
                .as_deref()
                .map(serde_json::from_str::<Option<String>>)
                .transpose()
                .context("迁移 v22 失败：旧 live_once 投稿窗口的 previous 值无效")?
                .flatten();
            let current_deadline: Option<String> = tx
                .query_row(
                    "SELECT value FROM settings WHERE key=?",
                    [NEXT_BILIBILI_SUBMIT_AT],
                    |row| row.get(0),
                )
                .optional()?;
            if current_deadline.as_deref() == Some(LEGACY_BILIBILI_UPLOAD_HOLD_UNTIL) {
                if let Some(previous_deadline) = previous_deadline {
                    tx.execute(
                        "UPDATE settings SET value=?,updated_at=? WHERE key=?",
                        params![
                            previous_deadline,
                            format_timestamp(Utc::now()),
                            NEXT_BILIBILI_SUBMIT_AT
                        ],
                    )?;
                } else {
                    tx.execute(
                        "DELETE FROM settings WHERE key=?",
                        [NEXT_BILIBILI_SUBMIT_AT],
                    )?;
                }
            }
            tx.execute(
                "DELETE FROM settings WHERE key IN (?,?)",
                params![
                    LEGACY_BILIBILI_UPLOAD_HOLD_OWNER,
                    LEGACY_BILIBILI_UPLOAD_HOLD_PREVIOUS
                ],
            )?;
            let now = Utc::now();
            let now_text = format_timestamp(now);
            let expires_at = format_timestamp(now + chrono::Duration::minutes(5));
            let reason = "从 v21 live_once 永久投稿 hold 迁移";
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO maintenance_hold(\
                   singleton,owner,reason,acquired_at,heartbeat_at,expires_at\
                 ) VALUES(1,?,?,?,?,?)",
                params![&legacy_owner, reason, &now_text, &now_text, &expires_at],
            )?;
            if inserted == 1 {
                tx.execute(
                    "INSERT INTO maintenance_hold_events(\
                       action,owner,reason,occurred_at,expires_at\
                     ) VALUES('acquired',?,?,?,?)",
                    params![&legacy_owner, reason, &now_text, &expires_at],
                )?;
            }
        }
        tx.execute(
            "INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(?,CURRENT_TIMESTAMP)",
            [CURRENT_SCHEMA_VERSION],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// `retry_at` 为 NULL 的旧行沿用迁移前的固定退避。
    pub(super) const LEGACY_RETRY_MINUTES: i64 = 10;

    /// 到期判定：`retry_at` 到点，或旧行的 `updated_at` 已超过固定退避。
    ///
    /// 不用 SQL 日期函数——`datetime()` 的输出格式和库里存的 RFC3339 字符串
    /// 无法直接比较，两个时间点都在 Rust 侧算好再传进去。
    pub(super) fn retry_due_clause(column: &str) -> String {
        format!("(retry_at IS NULL AND {column}<=?) OR retry_at<=?")
    }

    pub(super) fn retry_due_params() -> (String, String) {
        let now = Utc::now();
        (
            (now - chrono::Duration::minutes(Self::LEGACY_RETRY_MINUTES)).to_rfc3339(),
            now.to_rfc3339(),
        )
    }

    pub(super) fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
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
}
