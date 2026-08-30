use super::{rows::*, *};

impl Database {
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
                started_at: parse(r.get(4)?)?,
                finished_at: parse_opt(r.get(5)?)?,
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
        if self.maintenance_hold()?.is_some() {
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
