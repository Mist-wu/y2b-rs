use super::*;

impl Database {
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
                 AND NOT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?) \
                 ORDER BY discovered_at LIMIT 1"
            ),
            params![
                max_attempts,
                Utc::now().to_rfc3339(),
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
               AND NOT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?3) \
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
               AND NOT EXISTS(SELECT 1 FROM maintenance_hold WHERE singleton=1 AND expires_at>?3) \
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

    /// 返回会阻止再次提交的字幕 attempt。rejected 是唯一允许新 attempt 的终态。
    pub(crate) fn blocking_subtitle_attempt(&self, id: &str) -> Result<Option<SubtitleAttempt>> {
        self.conn()
            .query_row(
                "SELECT id,bvid,status FROM subtitle_attempts \
                 WHERE job_id=? AND status<>'rejected' \
                 ORDER BY started_at DESC,id DESC LIMIT 1",
                [id],
                |row| {
                    Ok(SubtitleAttempt {
                        id: row.get(0)?,
                        bvid: row.get(1)?,
                        status: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// 真正调用字幕提交接口前创建 attempt；若已有非 rejected attempt，只允许查询。
    pub(crate) fn begin_subtitle_attempt(
        &self,
        id: &str,
        bvid: &str,
    ) -> Result<SubtitleAttemptDecision> {
        let attempt_id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let claim_valid: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=? AND bvid=? \
             AND status IN('completed','uploaded_original_pending_subtitle') \
             AND claim_kind=? AND claim_owner=?)",
            params![id, bvid, SUBTITLE_CLAIM_KIND, self.claim_owner.as_ref()],
            |row| row.get(0),
        )?;
        if !claim_valid {
            anyhow::bail!("任务 {id} 的字幕领取权已丢失或 BVID 已改变")
        }
        let blocking = tx
            .query_row(
                "SELECT id,bvid,status FROM subtitle_attempts \
                 WHERE job_id=? AND status<>'rejected' \
                 ORDER BY started_at DESC,id DESC LIMIT 1",
                [id],
                |row| {
                    Ok(SubtitleAttempt {
                        id: row.get(0)?,
                        bvid: row.get(1)?,
                        status: row.get(2)?,
                    })
                },
            )
            .optional()?;
        if let Some(blocking) = blocking {
            tx.commit()?;
            return Ok(SubtitleAttemptDecision::QueryOnly(blocking));
        }
        tx.execute(
            "INSERT INTO subtitle_attempts(id,job_id,bvid,status,started_at) \
             VALUES(?,?,?,'started',?)",
            params![&attempt_id, id, bvid, &now],
        )?;
        tx.commit()?;
        Ok(SubtitleAttemptDecision::Submit(attempt_id))
    }

    /// 平台明确拒绝后结束 attempt；这是唯一允许按策略再次提交的分支。
    pub(crate) fn reject_subtitle_attempt(
        &self,
        id: &str,
        attempt_id: &str,
        detail: &str,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE subtitle_attempts SET status='rejected',detail=?,finished_at=? \
             WHERE id=? AND job_id=? AND status='started'",
            params![detail, Utc::now().to_rfc3339(), attempt_id, id],
        )?;
        if changed != 1 {
            anyhow::bail!("任务 {id} 的字幕 attempt {attempt_id} 已失效")
        }
        Ok(())
    }

    /// 响应丢失、超时或进程中断后固定为 uncertain，之后只能查询平台。
    pub(crate) fn mark_subtitle_attempt_uncertain(
        &self,
        id: &str,
        attempt_id: &str,
        detail: &str,
    ) -> Result<()> {
        let changed = self.conn().execute(
            "UPDATE subtitle_attempts SET status='uncertain',detail=?,finished_at=? \
             WHERE id=? AND job_id=? AND status IN('started','uncertain')",
            params![detail, Utc::now().to_rfc3339(), attempt_id, id],
        )?;
        if changed != 1 {
            anyhow::bail!("任务 {id} 的字幕 attempt {attempt_id} 已失效")
        }
        Ok(())
    }

    /// 平台明确返回成功后，把 attempt 与任务/领取终态放在同一事务。
    pub(crate) fn finish_subtitle_attempt(
        &self,
        id: &str,
        attempt_id: &str,
        mark_completed: bool,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let attempt_changed = tx.execute(
            "UPDATE subtitle_attempts SET status='confirmed',detail='Bilibili 明确返回字幕提交成功',finished_at=? \
             WHERE id=? AND job_id=? AND status='started'",
            params![&now, attempt_id, id],
        )?;
        let job_changed = tx.execute(
            "UPDATE jobs SET status=CASE WHEN ? THEN 'completed' ELSE status END,error=NULL,subtitle_retry_at=CASE WHEN ? THEN NULL ELSE subtitle_retry_at END,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                mark_completed,
                mark_completed,
                &now,
                id,
                SUBTITLE_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if attempt_changed != 1 || job_changed != 1 {
            anyhow::bail!("任务 {id} 的字幕 attempt {attempt_id} 已失效")
        }
        tx.commit()?;
        Ok(())
    }

    /// 查询到平台已有 zh 字幕后，把不确定 attempt 核对为 reconciled 并完成任务。
    pub(crate) fn reconcile_subtitle_attempt(
        &self,
        id: &str,
        attempt_id: &str,
        mark_completed: bool,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let attempt_changed = tx.execute(
            "UPDATE subtitle_attempts SET status='reconciled',detail='查询到平台已有 zh 字幕',finished_at=? \
             WHERE id=? AND job_id=? AND status IN('started','uncertain')",
            params![&now, attempt_id, id],
        )?;
        let job_changed = tx.execute(
            "UPDATE jobs SET status=CASE WHEN ? THEN 'completed' ELSE status END,error=NULL,subtitle_retry_at=CASE WHEN ? THEN NULL ELSE subtitle_retry_at END,claim_kind=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=? \
             WHERE id=? AND claim_kind=? AND claim_owner=?",
            params![
                mark_completed,
                mark_completed,
                &now,
                id,
                SUBTITLE_CLAIM_KIND,
                self.claim_owner.as_ref()
            ],
        )?;
        if attempt_changed != 1 || job_changed != 1 {
            anyhow::bail!("任务 {id} 的字幕 attempt {attempt_id} 无法核对")
        }
        tx.commit()?;
        Ok(())
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
}
