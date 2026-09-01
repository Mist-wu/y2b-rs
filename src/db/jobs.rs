use super::{rows::*, *};

impl Database {
    pub fn create_job(&self, job: NewJob<'_>) -> Result<Option<String>> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let changed=self.conn().execute("INSERT OR IGNORE INTO jobs(id,channel_id,video_id,url,title,status,transfer_mode,published_at,youtube_updated_at,discovered_at,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?)",params![id,job.channel_id,job.video_id,job.url,job.title,job.transfer_mode.to_string(),job.published.map(|x|x.to_rfc3339()),job.updated.map(|x|x.to_rfc3339()),now,now,now])?;
        Ok((changed == 1).then_some(id))
    }

    /// 按调用方给定的 WHERE 子句查询单条任务。
    pub(super) fn job_opt(&self, sql: &str, params: impl rusqlite::Params) -> Result<Option<Job>> {
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

    /// 核对投稿时确认候选 BVID 没有归属于另一个任务。
    pub fn bvid_owned_by_other_job(&self, id: &str, bvid: &str) -> Result<bool> {
        Ok(self.conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE bvid=? AND id<>?)",
            params![bvid, id],
            |row| row.get(0),
        )?)
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

    pub(super) fn claim_deadline(now: DateTime<Utc>) -> String {
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
                 AND NOT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?5) \
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
               AND NOT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?6) \
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
                "SELECT {JOB_COLUMNS} FROM jobs WHERE (status='queued' OR (status='retry_wait' AND ({due}))) AND NOT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?) ORDER BY COALESCE((SELECT CASE channels.priority WHEN 'priority' THEN 1 ELSE 0 END FROM channels WHERE channels.id=jobs.channel_id),0) DESC,discovered_at LIMIT 1"
            ),
            params![legacy_before, &now, &now],
        )
    }

    pub fn next_ready_to_upload_job(&self) -> Result<Option<Job>> {
        let (legacy_before, now) = Self::retry_due_params();
        let due = Self::retry_due_clause("updated_at");
        self.job_opt(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE (status='ready_to_upload' OR (status='upload_retry_wait' AND ({due}))) AND NOT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?) ORDER BY COALESCE((SELECT CASE channels.priority WHEN 'priority' THEN 1 ELSE 0 END FROM channels WHERE channels.id=jobs.channel_id),0) DESC,discovered_at LIMIT 1"
            ),
            params![legacy_before, &now, &now],
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
            "UPDATE subtitle_attempts SET status='uncertain',detail=COALESCE(detail,'服务重启时字幕提交结果不确定'),finished_at=? \
             WHERE status='started' AND job_id IN(SELECT id FROM jobs WHERE claim_kind=? \
               AND (claim_expires_at IS NULL OR claim_expires_at<=?))",
            params![&now, SUBTITLE_CLAIM_KIND, &now],
        )?;
        c.execute(
            "UPDATE jobs SET claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE status IN('completed','uploaded_original_pending_subtitle') AND claim_kind=? \
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
    /// 维护租约，普通任务无法领取；要么普通任务先进入 uploading，旁路会等待它结束。
    /// 返回 attempt ID；窗口未到、维护租约生效或任务状态不匹配时返回 None。
    pub fn begin_prepared_upload(&self, id: &str) -> Result<Option<String>> {
        let attempt_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let maintenance_hold_active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?)",
            [&now_text],
            |row| row.get(0),
        )?;
        if maintenance_hold_active {
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

    /// 返回当前不确定投稿 attempt 的开始时间，供创作中心核对时间窗。
    pub fn uncertain_upload_started_at(&self, id: &str) -> Result<Option<DateTime<Utc>>> {
        let started_at = self
            .conn()
            .query_row(
                "SELECT started_at FROM upload_attempts \
                 WHERE job_id=? AND status='uncertain' ORDER BY started_at DESC LIMIT 1",
                [id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        started_at.map(parse).transpose().map_err(Into::into)
    }

    /// biliup 明确返回成功后，把 attempt、任务终态和投稿冷却放在同一事务提交。
    pub fn finish_upload_attempt(
        &self,
        id: &str,
        attempt_id: &str,
        bvid: &str,
        completion_status: JobStatus,
        mode: TransferMode,
        timing: UploadCompletionTiming,
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
            .then(|| (now + chrono::Duration::seconds(timing.subtitle_delay_seconds)).to_rfc3339());
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
        if let Some(next_submit_at) = timing.next_submit_at {
            tx.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                params![
                    NEXT_BILIBILI_SUBMIT_AT,
                    next_submit_at.to_rfc3339(),
                    &now_text
                ],
            )?;
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

    /// 创作中心已核对到唯一稿件后，连同投稿冷却安全确认不确定态任务。
    /// 人工核对确认这次投稿从未落地：结算 attempt，并把任务退回可重投状态。
    ///
    /// 与 `confirm_uncertain_upload` 相对。`upload_attempts` 里 `uncertain` 的
    /// 行是 maintenance 的**永久** blocker，而结算它的唯一入口原本是「创作中心
    /// 存在唯一同名稿件」。投稿真的没落地时（线上那条是 biliup 传封面时 DNS
    /// 失败），既核对不到稿件，`retry_job` 又不收 `upload_uncertain`，任务就
    /// 会一直挡住所有部署——错误信息让人工提供 BVID，却没有任何命令能提供。
    ///
    /// 调用方必须先拿到「创作中心没有这次投稿」的证据；这里只负责原子结算。
    pub fn discard_uncertain_upload(&self, id: &str, detail: &str) -> Result<JobStatus> {
        let now_text = Utc::now().to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let attempt_changed = tx.execute(
            "UPDATE upload_attempts SET status='failed',detail=?,finished_at=? \
             WHERE id=(SELECT id FROM upload_attempts WHERE job_id=? ORDER BY started_at DESC LIMIT 1) \
               AND status='uncertain'",
            params![detail, &now_text, id],
        )?;
        // 上传计划还在就直接回到 ready_to_upload，和 retry_job 一致；否则从头重跑。
        let changed = tx.execute(
            "UPDATE jobs SET status=CASE WHEN prepared_upload_json IS NOT NULL THEN 'ready_to_upload' ELSE 'queued' END,\
             attempt=0,error=NULL,retry_at=NULL,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND status='upload_uncertain'",
            params![&now_text, id],
        )?;
        if attempt_changed != 1 || changed != 1 {
            anyhow::bail!("任务 {id} 不在有效的投稿结果不确定状态")
        }
        let status: String =
            tx.query_row("SELECT status FROM jobs WHERE id=?", [id], |row| row.get(0))?;
        tx.commit()?;
        status.parse()
    }

    pub fn confirm_uncertain_upload(
        &self,
        id: &str,
        bvid: &str,
        completion_status: JobStatus,
        mode: TransferMode,
        timing: UploadCompletionTiming,
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
            .then(|| (now + chrono::Duration::seconds(timing.subtitle_delay_seconds)).to_rfc3339());
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
        if let Some(next_submit_at) = timing.next_submit_at {
            tx.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) \
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                params![
                    NEXT_BILIBILI_SUBMIT_AT,
                    next_submit_at.to_rfc3339(),
                    &now_text
                ],
            )?;
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
}
