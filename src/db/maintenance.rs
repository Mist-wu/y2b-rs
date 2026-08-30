use super::{rows::*, *};

impl Database {
    fn maintenance_deadline(now: DateTime<Utc>, lease_seconds: i64) -> Result<DateTime<Utc>> {
        anyhow::ensure!(lease_seconds > 0, "维护锁租约必须大于 0 秒");
        now.checked_add_signed(chrono::Duration::seconds(lease_seconds))
            .context("维护锁租约到期时间超出可表示范围")
    }

    /// 原子获取全局维护锁。仍有效的锁不会被覆盖；到期锁可被接管并写入审计表。
    pub fn acquire_maintenance_hold(
        &self,
        owner: &str,
        reason: &str,
        lease_seconds: i64,
    ) -> Result<bool> {
        anyhow::ensure!(!owner.trim().is_empty(), "维护锁 owner 不能为空");
        anyhow::ensure!(!reason.trim().is_empty(), "维护锁 reason 不能为空");
        let now = Utc::now();
        let expires_at = Self::maintenance_deadline(now, lease_seconds)?;
        let now_text = format_timestamp(now);
        let expires_text = format_timestamp(expires_at);
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous = tx
            .query_row(
                "SELECT owner,reason,acquired_at,heartbeat_at,expires_at \
                 FROM maintenance_hold WHERE singleton=1",
                [],
                maintenance_hold_from_row,
            )
            .optional()?;
        if previous.as_ref().is_some_and(|hold| hold.expires_at > now) {
            return Ok(false);
        }
        let action = if previous.is_some() {
            "taken_over"
        } else {
            "acquired"
        };
        tx.execute(
            "INSERT INTO maintenance_hold(singleton,owner,reason,acquired_at,heartbeat_at,expires_at) \
             VALUES(1,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET \
             owner=excluded.owner,reason=excluded.reason,acquired_at=excluded.acquired_at,\
             heartbeat_at=excluded.heartbeat_at,expires_at=excluded.expires_at",
            params![owner, reason, &now_text, &now_text, &expires_text],
        )?;
        tx.execute(
            "INSERT INTO maintenance_hold_events(\
               action,owner,previous_owner,reason,previous_reason,occurred_at,expires_at\
             ) VALUES(?,?,?,?,?,?,?)",
            params![
                action,
                owner,
                previous.as_ref().map(|hold| hold.owner.as_str()),
                reason,
                previous.as_ref().map(|hold| hold.reason.as_str()),
                &now_text,
                &expires_text
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// 只有当前且尚未到期的持有者才能续租，避免旧进程在接管后复活自己的锁。
    pub fn renew_maintenance_hold(&self, owner: &str, lease_seconds: i64) -> Result<bool> {
        anyhow::ensure!(!owner.trim().is_empty(), "维护锁 owner 不能为空");
        let now = Utc::now();
        let expires_at = Self::maintenance_deadline(now, lease_seconds)?;
        let now_text = format_timestamp(now);
        let expires_text = format_timestamp(expires_at);
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let reason = tx
            .query_row(
                "UPDATE maintenance_hold SET heartbeat_at=?1,expires_at=?2 \
                 WHERE singleton=1 AND owner=?3 AND expires_at>?1 RETURNING reason",
                params![&now_text, &expires_text, owner],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(reason) = reason else {
            return Ok(false);
        };
        tx.execute(
            "INSERT INTO maintenance_hold_events(\
               action,owner,reason,occurred_at,expires_at\
             ) VALUES('renewed',?,?,?,?)",
            params![owner, reason, &now_text, &expires_text],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// 释放只按 owner 做比较删除，绝不会误删已经由另一进程接管的锁。
    pub fn release_maintenance_hold(&self, owner: &str) -> Result<bool> {
        anyhow::ensure!(!owner.trim().is_empty(), "维护锁 owner 不能为空");
        let now_text = format_timestamp(Utc::now());
        let mut connection = self.conn();
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let hold = tx
            .query_row(
                "SELECT owner,reason,acquired_at,heartbeat_at,expires_at \
                 FROM maintenance_hold WHERE singleton=1",
                [],
                maintenance_hold_from_row,
            )
            .optional()?;
        let Some(hold) = hold.filter(|hold| hold.owner == owner) else {
            return Ok(false);
        };
        tx.execute(
            "DELETE FROM maintenance_hold WHERE singleton=1 AND owner=?",
            [owner],
        )?;
        tx.execute(
            "INSERT INTO maintenance_hold_events(\
               action,owner,reason,occurred_at,expires_at\
             ) VALUES('released',?,?,?,?)",
            params![
                owner,
                &hold.reason,
                &now_text,
                format_timestamp(hold.expires_at)
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// 查询当前有效维护锁；到期行仅保留给接管审计，不再视为持有中。
    pub fn maintenance_hold(&self) -> Result<Option<MaintenanceHold>> {
        let hold = self
            .conn()
            .query_row(
                "SELECT owner,reason,acquired_at,heartbeat_at,expires_at \
                 FROM maintenance_hold WHERE singleton=1",
                [],
                maintenance_hold_from_row,
            )
            .optional()?;
        Ok(hold.filter(|hold| hold.expires_at > Utc::now()))
    }

    pub fn maintenance_hold_events(&self, limit: usize) -> Result<Vec<MaintenanceHoldEvent>> {
        let connection = self.conn();
        let mut query = connection.prepare(
            "SELECT id,action,owner,previous_owner,reason,previous_reason,occurred_at,expires_at \
             FROM maintenance_hold_events ORDER BY id DESC LIMIT ?",
        )?;
        Ok(query
            .query_map(
                [i64::try_from(limit).unwrap_or(i64::MAX)],
                maintenance_hold_event_from_row,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// 返回维护所需的完整空闲快照。`owner` 可排除调用方自己持有的维护锁；
    /// 其他持有者（尤其 live_once）仍会作为明确阻塞项返回。
    pub fn maintenance_status(&self, owner: Option<&str>) -> Result<MaintenanceStatus> {
        let checked_at = Utc::now();
        let now_text = format_timestamp(checked_at);
        let connection = self.conn();
        let mut blockers = Vec::new();

        push_maintenance_blocker(
            &mut blockers,
            "active_jobs",
            query_text_rows(
                &connection,
                "SELECT id||':'||status FROM jobs \
                 WHERE status IN('inspecting','processing','downloading','segmenting','translating','rendering') \
                 ORDER BY id",
                [],
            )?,
        );
        push_maintenance_blocker(
            &mut blockers,
            "running_stages",
            query_text_rows(
                &connection,
                "SELECT CAST(id AS TEXT)||':'||job_id||':'||stage FROM stage_runs \
                 WHERE status='running' ORDER BY id",
                [],
            )?,
        );
        push_maintenance_blocker(
            &mut blockers,
            "active_claims",
            query_text_rows(
                &connection,
                "SELECT id||':'||COALESCE(NULLIF(claim_kind,''),'-')||':'||\
                   COALESCE(NULLIF(claim_owner,''),'-') FROM jobs \
                 WHERE (NULLIF(claim_kind,'') IS NOT NULL OR NULLIF(claim_owner,'') IS NOT NULL) \
                   AND (claim_expires_at IS NULL OR claim_expires_at>?) ORDER BY id",
                [&now_text],
            )?,
        );
        push_maintenance_blocker(
            &mut blockers,
            "uploading_jobs",
            query_text_rows(
                &connection,
                "SELECT id||':'||video_id FROM jobs WHERE status='uploading' ORDER BY id",
                [],
            )?,
        );
        push_maintenance_blocker(
            &mut blockers,
            "upload_attempts",
            query_text_rows(
                &connection,
                "SELECT id||':'||job_id||':'||status FROM upload_attempts \
                 WHERE status IN('running','uncertain') ORDER BY id",
                [],
            )?,
        );
        push_maintenance_blocker(
            &mut blockers,
            "subtitle_attempts",
            query_text_rows(
                &connection,
                "SELECT id||':'||job_id||':'||status FROM subtitle_attempts \
                 WHERE status IN('started','uncertain') ORDER BY id",
                [],
            )?,
        );

        let hold_record = connection
            .query_row(
                "SELECT owner,reason,acquired_at,heartbeat_at,expires_at \
                 FROM maintenance_hold WHERE singleton=1",
                [],
                maintenance_hold_from_row,
            )
            .optional()?;
        let hold = hold_record
            .as_ref()
            .filter(|hold| hold.expires_at > checked_at)
            .cloned();
        let expired_hold = hold_record.filter(|hold| hold.expires_at <= checked_at);
        if let Some(active_hold) = hold
            .as_ref()
            .filter(|hold| Some(hold.owner.as_str()) != owner)
        {
            let kind = if active_hold.owner.starts_with("LIVE_ONCE:") {
                "live_once_hold"
            } else {
                "maintenance_hold"
            };
            push_maintenance_blocker(
                &mut blockers,
                kind,
                vec![format!(
                    "owner={} expires_at={} reason={}",
                    active_hold.owner,
                    format_timestamp(active_hold.expires_at),
                    active_hold.reason
                )],
            );
        }

        Ok(MaintenanceStatus {
            checked_at,
            idle: blockers.is_empty(),
            hold,
            expired_hold,
            blockers,
        })
    }
}
