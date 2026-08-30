use super::*;

pub(super) fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub(super) fn maintenance_hold_from_row(
    r: &rusqlite::Row<'_>,
) -> rusqlite::Result<MaintenanceHold> {
    Ok(MaintenanceHold {
        owner: r.get(0)?,
        reason: r.get(1)?,
        acquired_at: parse(r.get(2)?)?,
        heartbeat_at: parse(r.get(3)?)?,
        expires_at: parse(r.get(4)?)?,
    })
}

pub(super) fn maintenance_hold_event_from_row(
    r: &rusqlite::Row<'_>,
) -> rusqlite::Result<MaintenanceHoldEvent> {
    Ok(MaintenanceHoldEvent {
        id: r.get(0)?,
        action: r.get(1)?,
        owner: r.get(2)?,
        previous_owner: r.get(3)?,
        reason: r.get(4)?,
        previous_reason: r.get(5)?,
        occurred_at: parse(r.get(6)?)?,
        expires_at: parse_opt(r.get(7)?)?,
    })
}

pub(super) fn query_text_rows(
    connection: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<String>> {
    let mut query = connection.prepare(sql)?;
    Ok(query
        .query_map(params, |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub(super) fn push_maintenance_blocker(
    blockers: &mut Vec<MaintenanceBlocker>,
    kind: &str,
    details: Vec<String>,
) {
    if !details.is_empty() {
        blockers.push(MaintenanceBlocker {
            kind: kind.into(),
            count: details.len(),
            details,
        });
    }
}

pub(super) fn channel_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Channel> {
    let transfer_mode = TransferMode::from_str(&r.get::<_, String>(5)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, error.into())
    })?;
    let priority = ChannelPriority::from_str(&r.get::<_, String>(6)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, error.into())
    })?;
    Ok(Channel {
        id: r.get(0)?,
        youtube_channel_id: r.get(1)?,
        name: r.get(2)?,
        url: r.get(3)?,
        enabled: r.get::<_, i64>(4)? != 0,
        transfer_mode,
        priority,
        last_checked_at: parse_opt(r.get(7)?)?,
        last_error: r.get(8)?,
        next_poll_at: parse_opt(r.get(9)?)?,
        consecutive_failures: r.get(10)?,
        uploads_playlist_id: r.get(11)?,
        next_data_api_poll_at: parse_opt(r.get(12)?)?,
        data_api_etag: r.get(13)?,
        websub_lease_expires_at: parse_opt(r.get(14)?)?,
        websub_last_received_at: parse_opt(r.get(15)?)?,
    })
}

pub(super) fn candidate_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<VideoCandidate> {
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
        published_at: parse_opt(r.get(4)?)?,
        source,
        discovered_at: parse(r.get(6)?)?,
        gate_state,
        gate_attempts: r.get(8)?,
        next_gate_at: parse_opt(r.get(9)?)?,
        last_error: r.get(10)?,
        source_language: r.get(11)?,
        source_language_mismatch: r.get::<_, i64>(12)? != 0,
    })
}

pub(super) fn websub_channel_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<WebSubChannel> {
    Ok(WebSubChannel {
        id: r.get(0)?,
        youtube_channel_id: r.get(1)?,
        name: r.get(2)?,
        enabled: r.get::<_, i64>(3)? != 0,
        lease_expires_at: parse_opt(r.get(4)?)?,
        secret: r.get(5)?,
        callback_path: r.get(6)?,
        last_received_at: parse_opt(r.get(7)?)?,
    })
}

pub(super) fn parse(s: String) -> rusqlite::Result<DateTime<Utc>> {
    if let Ok(value) = DateTime::parse_from_rfc3339(&s) {
        return Ok(value.with_timezone(&Utc));
    }
    // 早期迁移和 SQLite 的 CURRENT_TIMESTAMP 会生成这一 UTC 格式；它不是损坏值。
    if let Ok(value) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
        return Ok(value.and_utc());
    }
    Err(rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        anyhow::anyhow!("无效时间戳: {s}").into(),
    ))
}
pub(super) fn parse_opt(s: Option<String>) -> rusqlite::Result<Option<DateTime<Utc>>> {
    s.map(parse).transpose()
}
pub(super) fn job_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let status = JobStatus::from_str(&r.get::<_, String>(5)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, error.into())
    })?;
    let transfer_mode = TransferMode::from_str(&r.get::<_, String>(6)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, error.into())
    })?;
    Ok(Job {
        id: r.get(0)?,
        channel_id: r.get(1)?,
        video_id: r.get(2)?,
        url: r.get(3)?,
        title: r.get(4)?,
        status,
        transfer_mode,
        published_at: parse_opt(r.get(7)?)?,
        youtube_updated_at: parse_opt(r.get(8)?)?,
        discovered_at: parse(r.get(9)?)?,
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
pub(super) fn usage_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<AiUsage> {
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
