use super::{rows::*, *};

impl Database {
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
        Ok(q.query_map([id], |row| parse(row.get(0)?))?
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
        )?)?)
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
        let changed = self.conn().execute(
            "UPDATE channels SET enabled=? WHERE id=?",
            params![enabled as i64, id],
        )?;
        if changed == 0 {
            anyhow::bail!("频道不存在: {id}")
        }
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
        self.update_discovery_quota(units, budget, now, next_reset_at, false)
    }

    fn update_discovery_quota(
        &self,
        units: u32,
        budget: u32,
        now: DateTime<Utc>,
        next_reset_at: DateTime<Utc>,
        force_exhausted: bool,
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
        let stored_reset = match raw_reset.as_deref() {
            Some(value) => match DateTime::parse_from_rfc3339(value) {
                Ok(reset_at) => Some(reset_at.with_timezone(&Utc)),
                Err(error) => {
                    tracing::warn!(
                        key = "quota_reset_at",
                        value,
                        error = %error,
                        "发现配额状态损坏，拒绝重置配额窗口"
                    );
                    anyhow::bail!("发现配额状态 quota_reset_at 无效: {value}: {error}")
                }
            },
            None => None,
        };
        let stored_used = match raw_used.as_deref() {
            Some(value) => match value.parse::<u32>() {
                Ok(used) => used,
                Err(error) => {
                    tracing::warn!(
                        key = "quota_used_today",
                        value,
                        error = %error,
                        "发现配额状态损坏，拒绝继续使用配额"
                    );
                    anyhow::bail!("发现配额状态 quota_used_today 无效: {value}: {error}")
                }
            },
            None => 0,
        };
        let (mut used, reset_at) = match stored_reset {
            Some(reset_at) if reset_at > now => (stored_used, reset_at),
            _ => (0, next_reset_at),
        };
        let allowed = used.saturating_add(units) <= budget;
        if force_exhausted {
            used = budget;
        } else if allowed {
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
        self.update_discovery_quota(0, budget, now, next_reset_at, true)
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
}
