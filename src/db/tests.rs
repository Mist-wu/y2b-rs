use super::rows::*;
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

fn maintenance_test_job(db: &Database, video_id: &str) -> String {
    db.create_job(NewJob {
        channel_id: None,
        video_id,
        url: &format!("https://youtu.be/{video_id}"),
        title: None,
        published: None,
        updated: None,
        transfer_mode: TransferMode::Direct,
    })
    .unwrap()
    .unwrap()
}

fn assert_maintenance_blocker(status: &MaintenanceStatus, kind: &str, detail: &str) {
    assert!(!status.idle);
    let blocker = status
        .blockers
        .iter()
        .find(|blocker| blocker.kind == kind)
        .unwrap_or_else(|| panic!("缺少 {kind} 阻塞项: {:?}", status.blockers));
    assert_eq!(blocker.count, blocker.details.len());
    assert!(
        blocker.details.iter().any(|value| value.contains(detail)),
        "{kind} 未指出 {detail}: {:?}",
        blocker.details
    );
}

#[test]
fn maintenance_hold_acquisition_is_atomic_between_processes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("maintenance-race.db");
    let first = Database::open(&path).unwrap();
    let second = Database::open(&path).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let first_barrier = barrier.clone();
    let second_barrier = barrier.clone();
    let first_thread = std::thread::spawn(move || {
        first_barrier.wait();
        first
            .acquire_maintenance_hold("deploy:first", "并发部署一", 300)
            .unwrap()
    });
    let second_thread = std::thread::spawn(move || {
        second_barrier.wait();
        second
            .acquire_maintenance_hold("deploy:second", "并发部署二", 300)
            .unwrap()
    });
    barrier.wait();
    let acquired = [first_thread.join().unwrap(), second_thread.join().unwrap()];
    assert_eq!(acquired.into_iter().filter(|value| *value).count(), 1);

    let reopened = Database::open_existing(&path).unwrap();
    assert!(matches!(
        reopened.maintenance_hold().unwrap().unwrap().owner.as_str(),
        "deploy:first" | "deploy:second"
    ));
    assert_eq!(reopened.maintenance_hold_events(10).unwrap().len(), 1);
}

#[test]
fn expired_maintenance_hold_can_be_taken_over_with_audit() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("maintenance-takeover.db")).unwrap();
    assert!(
        db.acquire_maintenance_hold("deploy:old", "旧版本部署", 300)
            .unwrap()
    );
    db.conn()
        .execute(
            "UPDATE maintenance_hold SET expires_at=? WHERE singleton=1",
            [format_timestamp(Utc::now() - chrono::Duration::seconds(1))],
        )
        .unwrap();
    assert!(db.maintenance_hold().unwrap().is_none());

    assert!(
        db.acquire_maintenance_hold("deploy:new", "接管过期部署", 300)
            .unwrap()
    );
    let hold = db.maintenance_hold().unwrap().unwrap();
    assert_eq!(hold.owner, "deploy:new");
    let event = &db.maintenance_hold_events(1).unwrap()[0];
    assert_eq!(event.action, "taken_over");
    assert_eq!(event.owner, "deploy:new");
    assert_eq!(event.previous_owner.as_deref(), Some("deploy:old"));
    assert_eq!(event.previous_reason.as_deref(), Some("旧版本部署"));
    assert_eq!(event.reason, "接管过期部署");
}

#[test]
fn maintenance_hold_can_be_renewed_but_not_released_by_another_owner() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("maintenance-owner.db")).unwrap();
    assert!(
        db.acquire_maintenance_hold("deploy:owner", "发布维护", 1)
            .unwrap()
    );
    let original_expiry = db.maintenance_hold().unwrap().unwrap().expires_at;
    assert!(db.renew_maintenance_hold("deploy:owner", 300).unwrap());
    assert!(
        db.maintenance_hold().unwrap().unwrap().expires_at > original_expiry,
        "续租没有延长到期时间"
    );
    assert!(!db.release_maintenance_hold("deploy:other").unwrap());
    assert_eq!(
        db.maintenance_hold().unwrap().unwrap().owner,
        "deploy:owner"
    );
    assert!(db.release_maintenance_hold("deploy:owner").unwrap());
    assert!(db.maintenance_hold().unwrap().is_none());
}

#[test]
fn idle_status_reports_active_job() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-active-job.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-active-job");
    db.update_job_status(&id, JobStatus::Processing, None)
        .unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "active_jobs", &format!("{id}:processing"));
}

#[test]
fn idle_status_reports_running_stage() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-running-stage.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-running-stage");
    let stage_id = db.start_stage(&id, "render", None, None, None).unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(
        &status,
        "running_stages",
        &format!("{stage_id}:{id}:render"),
    );
}

#[test]
fn idle_status_reports_only_unexpired_nonempty_claims() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-claim.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-claim");
    db.conn()
        .execute(
            "UPDATE jobs SET claim_kind='prepare',claim_owner='worker',claim_expires_at=? \
                 WHERE id=?",
            params![
                format_timestamp(Utc::now() + chrono::Duration::minutes(1)),
                &id
            ],
        )
        .unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "active_claims", &format!("{id}:prepare:worker"));

    db.conn()
        .execute(
            "UPDATE jobs SET claim_expires_at=? WHERE id=?",
            params![
                format_timestamp(Utc::now() - chrono::Duration::seconds(1)),
                &id
            ],
        )
        .unwrap();
    assert!(db.maintenance_status(None).unwrap().idle);
}

#[test]
fn idle_status_reports_uploading_job() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-uploading.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-uploading");
    db.update_job_status(&id, JobStatus::Uploading, None)
        .unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "uploading_jobs", &id);
}

#[test]
fn idle_status_reports_running_upload_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-upload-running.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-upload-running");
    db.conn()
        .execute(
            "INSERT INTO upload_attempts(id,job_id,status,started_at) \
                 VALUES('upload-running',?,'running',?)",
            params![&id, format_timestamp(Utc::now())],
        )
        .unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "upload_attempts", "upload-running");
}

#[test]
fn idle_status_reports_uncertain_upload_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-upload-uncertain.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-upload-uncertain");
    db.conn()
        .execute(
            "INSERT INTO upload_attempts(id,job_id,status,started_at) \
                 VALUES('upload-uncertain',?,'uncertain',?)",
            params![&id, format_timestamp(Utc::now())],
        )
        .unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "upload_attempts", "upload-uncertain");
}

#[test]
fn idle_status_reports_started_subtitle_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-subtitle-started.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-subtitle-started");
    db.conn()
        .execute(
            "INSERT INTO subtitle_attempts(id,job_id,bvid,status,started_at) \
                 VALUES('subtitle-started',?,'BV1idletest1','started',?)",
            params![&id, format_timestamp(Utc::now())],
        )
        .unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "subtitle_attempts", "subtitle-started");
}

#[test]
fn idle_status_reports_uncertain_subtitle_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-subtitle-uncertain.db")).unwrap();
    let id = maintenance_test_job(&db, "idle-subtitle-uncertain");
    db.conn()
        .execute(
            "INSERT INTO subtitle_attempts(id,job_id,bvid,status,started_at) \
                 VALUES('subtitle-uncertain',?,'BV1idletest2','uncertain',?)",
            params![&id, format_timestamp(Utc::now())],
        )
        .unwrap();
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "subtitle_attempts", "subtitle-uncertain");
}

#[test]
fn idle_status_reports_live_once_lease_but_exempts_its_caller() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("idle-live-once.db")).unwrap();
    let owner = "LIVE_ONCE:video:process";
    assert!(
        db.acquire_maintenance_hold(owner, "一次性直播投稿", 300)
            .unwrap()
    );
    let status = db.maintenance_status(None).unwrap();
    assert_maintenance_blocker(&status, "live_once_hold", owner);
    assert!(db.maintenance_status(Some(owner)).unwrap().idle);

    db.conn()
        .execute(
            "UPDATE maintenance_hold SET expires_at=? WHERE singleton=1",
            [format_timestamp(Utc::now() - chrono::Duration::seconds(1))],
        )
        .unwrap();
    let expired = db.maintenance_status(None).unwrap();
    assert!(expired.idle);
    assert_eq!(expired.expired_hold.unwrap().owner, owner);
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
fn migrates_v19_without_losing_jobs_or_bvids() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("v19.db");
    let old = Database::open(&path).unwrap();
    let id = old
        .create_job(NewJob {
            channel_id: None,
            video_id: "v19-video",
            url: "https://youtu.be/v19-video",
            title: Some("v19 保留稿件"),
            published: None,
            updated: None,
            transfer_mode: TransferMode::Direct,
        })
        .unwrap()
        .unwrap();
    old.set_job_bvid(&id, "BV1uxE16ZE7e").unwrap();
    old.conn()
        .execute_batch(
            "DROP TABLE subtitle_attempts; \
                 DROP INDEX idx_jobs_bvid_unique; \
                 DELETE FROM schema_migrations WHERE version>=20;",
        )
        .unwrap();
    drop(old);

    let migrated = Database::open(&path).unwrap();
    assert_eq!(migrated.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
    let job = migrated.get_job(&id).unwrap().unwrap();
    assert_eq!(job.title.as_deref(), Some("v19 保留稿件"));
    assert_eq!(job.bvid.as_deref(), Some("BV1uxE16ZE7e"));
    let index_exists: bool = migrated
        .conn()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master \
                 WHERE type='index' AND name='idx_jobs_bvid_unique')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(index_exists);
    let subtitle_attempts_exists: bool = migrated
        .conn()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master \
                 WHERE type='table' AND name='subtitle_attempts')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(subtitle_attempts_exists);
}

#[test]
fn v22_migrates_legacy_permanent_live_hold_to_a_short_lease() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("v21-live-hold.db");
    let old = Database::open(&path).unwrap();
    let previous_deadline = format_timestamp(Utc::now() + chrono::Duration::minutes(20));
    old.set_setting(LEGACY_BILIBILI_UPLOAD_HOLD_OWNER, "LIVE_ONCE:legacy:owner")
        .unwrap();
    old.set_setting(
        LEGACY_BILIBILI_UPLOAD_HOLD_PREVIOUS,
        &serde_json::to_string(&Some(&previous_deadline)).unwrap(),
    )
    .unwrap();
    old.set_setting(NEXT_BILIBILI_SUBMIT_AT, LEGACY_BILIBILI_UPLOAD_HOLD_UNTIL)
        .unwrap();
    old.conn()
        .execute_batch(
            "DROP INDEX idx_maintenance_hold_events_time; \
                 DROP TABLE maintenance_hold_events; \
                 DROP TABLE maintenance_hold; \
                 DELETE FROM schema_migrations WHERE version=22;",
        )
        .unwrap();
    drop(old);

    let migrated = Database::open(&path).unwrap();
    assert_eq!(
        migrated.get_setting(NEXT_BILIBILI_SUBMIT_AT).unwrap(),
        Some(previous_deadline)
    );
    assert!(
        migrated
            .get_setting(LEGACY_BILIBILI_UPLOAD_HOLD_OWNER)
            .unwrap()
            .is_none()
    );
    assert!(
        migrated
            .get_setting(LEGACY_BILIBILI_UPLOAD_HOLD_PREVIOUS)
            .unwrap()
            .is_none()
    );
    let hold = migrated.maintenance_hold().unwrap().unwrap();
    assert_eq!(hold.owner, "LIVE_ONCE:legacy:owner");
    assert!(hold.expires_at > Utc::now());
    assert!(hold.expires_at <= Utc::now() + chrono::Duration::minutes(5));
    let event = &migrated.maintenance_hold_events(1).unwrap()[0];
    assert_eq!(event.action, "acquired");
    assert_eq!(event.owner, hold.owner);
    assert!(event.reason.contains("v21"));
}

#[test]
fn v20_migration_reports_duplicate_bvid_before_creating_index() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("v19-duplicate-bvid.db");
    let old = Database::open(&path).unwrap();
    let mut ids = Vec::new();
    for video_id in ["duplicate-one", "duplicate-two"] {
        ids.push(
            old.create_job(NewJob {
                channel_id: None,
                video_id,
                url: &format!("https://youtu.be/{video_id}"),
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap(),
        );
    }
    old.conn()
        .execute_batch(
            "DROP TABLE subtitle_attempts; \
                 DROP INDEX idx_jobs_bvid_unique; \
                 DELETE FROM schema_migrations WHERE version>=20;",
        )
        .unwrap();
    old.conn()
        .execute(
            "UPDATE jobs SET bvid='BV1uxE16ZE7e' WHERE id IN (?,?)",
            params![&ids[0], &ids[1]],
        )
        .unwrap();
    drop(old);

    let error = match Database::open(&path) {
        Ok(_) => panic!("重复 BVID 不应迁移成功"),
        Err(error) => error,
    };
    let detail = error.to_string();
    assert!(detail.contains("迁移 v20 失败"), "{detail}");
    assert!(detail.contains("BV1uxE16ZE7e"), "{detail}");
    assert!(detail.contains("2 个任务重复占用"), "{detail}");
}

#[test]
fn jobs_bvid_is_unique_but_allows_multiple_nulls() {
    let t = tempfile::tempdir().unwrap();
    let db = Database::open(&t.path().join("unique-bvid.db")).unwrap();
    let mut ids = Vec::new();
    for video_id in ["null-bvid-one", "null-bvid-two"] {
        ids.push(
            db.create_job(NewJob {
                channel_id: None,
                video_id,
                url: &format!("https://youtu.be/{video_id}"),
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Direct,
            })
            .unwrap()
            .unwrap(),
        );
    }
    assert!(
        ids.iter()
            .all(|id| db.get_job(id).unwrap().unwrap().bvid.is_none())
    );

    db.set_job_bvid(&ids[0], "BV1uxE16ZE7e").unwrap();
    let error = db.set_job_bvid(&ids[1], "BV1uxE16ZE7e").unwrap_err();
    assert!(error.to_string().contains("UNIQUE constraint failed"));
    assert!(db.get_job(&ids[1]).unwrap().unwrap().bvid.is_none());
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
fn corrupted_job_and_channel_fields_return_errors() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("state.db")).unwrap();
    let channel_id = db
        .add_channel(
            "UC-corrupted",
            "corrupted",
            "https://youtube.com/@corrupted",
            "https://youtube.com/feeds/videos.xml?channel_id=UC-corrupted",
            TransferMode::Direct,
        )
        .unwrap();
    let job_id = db
        .create_job(NewJob {
            channel_id: Some(channel_id),
            video_id: "corrupted-job",
            url: "https://youtu.be/corrupted-job",
            title: None,
            published: None,
            updated: None,
            transfer_mode: TransferMode::Direct,
        })
        .unwrap()
        .unwrap();

    db.conn()
        .execute(
            "UPDATE channels SET transfer_mode='unknown-mode' WHERE id=?",
            [channel_id],
        )
        .unwrap();
    assert!(db.channel(channel_id).is_err());
    db.conn()
        .execute(
            "UPDATE channels SET transfer_mode='direct',priority='urgent' WHERE id=?",
            [channel_id],
        )
        .unwrap();
    assert!(db.channel(channel_id).is_err());
    db.conn()
        .execute(
            "UPDATE channels SET priority='normal' WHERE id=?",
            [channel_id],
        )
        .unwrap();

    db.conn()
        .execute("UPDATE jobs SET status='mystery' WHERE id=?", [&job_id])
        .unwrap();
    assert!(db.get_job(&job_id).is_err());
    db.conn()
        .execute(
            "UPDATE jobs SET status='queued',transfer_mode='unknown-mode' WHERE id=?",
            [&job_id],
        )
        .unwrap();
    assert!(db.get_job(&job_id).is_err());
    db.conn()
        .execute(
            "UPDATE jobs SET transfer_mode='direct',discovered_at='not-a-timestamp' WHERE id=?",
            [&job_id],
        )
        .unwrap();
    assert!(db.get_job(&job_id).is_err());
}

#[test]
fn corrupted_discovery_quota_state_returns_errors_without_resetting_values() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("state.db")).unwrap();
    let now = Utc::now();
    let next_reset = now + chrono::Duration::days(1);

    db.set_discovery_state("quota_used_today", "7").unwrap();
    db.set_discovery_state("quota_reset_at", "not-a-timestamp")
        .unwrap();
    let error = db
        .consume_discovery_quota(1, 100, now, next_reset)
        .unwrap_err();
    assert!(error.to_string().contains("quota_reset_at 无效"));
    assert_eq!(
        db.get_discovery_state("quota_used_today").unwrap(),
        Some("7".into())
    );

    db.set_discovery_state("quota_reset_at", &format_timestamp(next_reset))
        .unwrap();
    db.set_discovery_state("quota_used_today", "seven").unwrap();
    let error = db
        .consume_discovery_quota(1, 100, now, next_reset)
        .unwrap_err();
    assert!(error.to_string().contains("quota_used_today 无效"));
    assert_eq!(
        db.get_discovery_state("quota_used_today").unwrap(),
        Some("seven".into())
    );
}

#[test]
fn exhausting_discovery_quota_rolls_back_as_one_transaction() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("state.db")).unwrap();
    let now = Utc::now();
    let reset_at = now + chrono::Duration::hours(1);
    db.set_discovery_state("quota_used_today", "3").unwrap();
    db.set_discovery_state("quota_reset_at", &format_timestamp(reset_at))
        .unwrap();
    db.conn()
        .execute_batch(
            r#"
                CREATE TRIGGER reject_reset_after_exhaust
                BEFORE UPDATE ON discovery_state
                WHEN OLD.key='quota_reset_at'
                  AND (SELECT value FROM discovery_state
                       WHERE key='quota_used_today')='10'
                BEGIN
                  SELECT RAISE(ABORT, '模拟提交前失败');
                END;
                "#,
        )
        .unwrap();

    assert!(
        db.exhaust_discovery_quota(10, now, now + chrono::Duration::days(1))
            .is_err()
    );
    assert_eq!(
        db.get_discovery_state("quota_used_today").unwrap(),
        Some("3".into())
    );
}

#[tokio::test]
async fn disk_probe_failure_stops_job_preparation() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("state.db")).unwrap();
    let job_id = db
        .create_job(NewJob {
            channel_id: None,
            video_id: "disk-probe-failure",
            url: "https://youtu.be/disk-probe-failure",
            title: None,
            published: None,
            updated: None,
            transfer_mode: TransferMode::Direct,
        })
        .unwrap()
        .unwrap();
    let job = db.claim_prepare_job(&job_id).unwrap().unwrap();
    let mut config = crate::config::Config::default();
    config.runtime.data_dir = temp.path().join("不存在的目录");
    let pipeline = crate::pipeline::Pipeline::new(config, db);

    let error = pipeline.prepare_job(job).await.unwrap_err();
    assert!(error.to_string().contains("读取剩余磁盘空间失败"));
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
fn set_channel_enabled_reports_missing_channel_and_updates_existing() {
    let t = tempfile::tempdir().unwrap();
    let db = Database::open(&t.path().join("x.db")).unwrap();

    let error = db.set_channel_enabled(999, false).unwrap_err().to_string();
    assert!(error.contains("频道不存在"));

    let id = db
        .add_channel(
            "UC-enable",
            "enable",
            "https://youtube.com/@enable/videos",
            "https://youtube.com/feeds/videos.xml?channel_id=UC-enable",
            TransferMode::Direct,
        )
        .unwrap();
    db.set_channel_enabled(id, false).unwrap();
    assert!(!db.channel(id).unwrap().enabled);
    db.set_channel_enabled(id, true).unwrap();
    assert!(db.channel(id).unwrap().enabled);
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
            .update_claimed_job_status(&id, PREPARE_CLAIM_KIND, JobStatus::Processing, None, false,)
            .is_err()
    );
}

#[test]
fn maintenance_hold_refuses_prepare_upload_and_subtitle_claims_until_expiry() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("maintenance-claims.db")).unwrap();
    let prepare_id = maintenance_test_job(&db, "hold-prepare");
    let upload_id = maintenance_test_job(&db, "hold-upload");
    db.queue_prepared_upload(
        &upload_id,
        &PreparedUpload::Submission {
            video_path: "/tmp/hold-upload.mp4".into(),
            cover_path: "/tmp/hold-upload.jpg".into(),
            mode: TransferMode::Direct,
            completion_status: JobStatus::Completed,
        },
    )
    .unwrap();
    let subtitle_id = maintenance_test_job(&db, "hold-subtitle");
    db.set_job_bvid(&subtitle_id, "BV1holdtest1").unwrap();
    db.queue_pending_subtitle(&subtitle_id, -1).unwrap();

    assert!(
        db.acquire_maintenance_hold("deploy:claims", "验证队列暂停", 300)
            .unwrap()
    );
    assert!(db.next_queued_job().unwrap().is_none());
    assert!(db.claim_next_prepare_job().unwrap().is_none());
    assert!(db.claim_prepare_job(&prepare_id).unwrap().is_none());
    assert!(db.next_ready_to_upload_job().unwrap().is_none());
    assert!(!db.bilibili_submission_due(Utc::now()).unwrap());
    assert!(db.begin_prepared_upload(&upload_id).unwrap().is_none());
    assert!(db.next_pending_subtitle_job(16).unwrap().is_none());
    assert!(db.claim_next_pending_subtitle_job(16).unwrap().is_none());
    assert!(db.claim_subtitle_job_now(&subtitle_id).unwrap().is_none());

    db.conn()
        .execute(
            "UPDATE maintenance_hold SET expires_at=? WHERE singleton=1",
            [format_timestamp(Utc::now() - chrono::Duration::seconds(1))],
        )
        .unwrap();
    assert_eq!(db.claim_next_prepare_job().unwrap().unwrap().id, prepare_id);
    assert!(db.begin_prepared_upload(&upload_id).unwrap().is_some());
    assert_eq!(
        db.claim_next_pending_subtitle_job(16).unwrap().unwrap().id,
        subtitle_id
    );
}

#[test]
fn maintenance_hold_does_not_interrupt_already_claimed_work() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&temp.path().join("maintenance-inflight.db")).unwrap();
    let prepare_id = maintenance_test_job(&db, "inflight-prepare");
    db.claim_prepare_job(&prepare_id).unwrap().unwrap();

    let upload_id = maintenance_test_job(&db, "inflight-upload");
    db.queue_prepared_upload(
        &upload_id,
        &PreparedUpload::Submission {
            video_path: "/tmp/inflight-upload.mp4".into(),
            cover_path: "/tmp/inflight-upload.jpg".into(),
            mode: TransferMode::Direct,
            completion_status: JobStatus::Completed,
        },
    )
    .unwrap();
    let upload_attempt = db.begin_prepared_upload(&upload_id).unwrap().unwrap();

    let subtitle_id = maintenance_test_job(&db, "inflight-subtitle");
    db.set_job_bvid(&subtitle_id, "BV1holdtest2").unwrap();
    db.queue_pending_subtitle(&subtitle_id, -1).unwrap();
    db.claim_next_pending_subtitle_job(16).unwrap().unwrap();

    assert!(
        db.acquire_maintenance_hold("deploy:inflight", "等待存量任务", 300)
            .unwrap()
    );
    assert!(db.renew_job_claim(&prepare_id, PREPARE_CLAIM_KIND).unwrap());
    assert!(db.renew_job_claim(&upload_id, UPLOAD_CLAIM_KIND).unwrap());
    assert!(
        db.renew_job_claim(&subtitle_id, SUBTITLE_CLAIM_KIND)
            .unwrap()
    );
    db.update_claimed_job_status(
        &prepare_id,
        PREPARE_CLAIM_KIND,
        JobStatus::Processing,
        None,
        false,
    )
    .unwrap();
    assert!(
        !db.finish_upload_attempt(
            &upload_id,
            &upload_attempt,
            "BV1holdtest3",
            JobStatus::Completed,
            TransferMode::Direct,
            UploadCompletionTiming {
                subtitle_delay_seconds: 90,
                next_submit_at: None,
            },
        )
        .unwrap()
    );
    db.defer_claimed_pending_subtitle(&subtitle_id, "存量任务正常收尾", 60)
        .unwrap();

    assert_eq!(
        db.get_job(&prepare_id).unwrap().unwrap().status,
        JobStatus::Processing
    );
    assert_eq!(
        db.get_job(&upload_id).unwrap().unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(
        db.get_job(&subtitle_id).unwrap().unwrap().status,
        JobStatus::UploadedOriginalPendingSubtitle
    );
    assert!(db.claim_next_pending_subtitle_job(16).unwrap().is_none());
}

fn claimed_subtitle_job(db: &Database, video_id: &str, bvid: &str) -> (String, Job) {
    let id = db
        .create_job(NewJob {
            channel_id: None,
            video_id,
            url: &format!("https://youtu.be/{video_id}"),
            title: None,
            published: None,
            updated: None,
            transfer_mode: TransferMode::Translated,
        })
        .unwrap()
        .unwrap();
    db.set_job_bvid(&id, bvid).unwrap();
    db.queue_pending_subtitle(&id, -1).unwrap();
    let job = db.claim_next_pending_subtitle_job(16).unwrap().unwrap();
    (id, job)
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
fn uncertain_subtitle_attempt_allows_only_query_and_reconciliation() {
    let t = tempfile::tempdir().unwrap();
    let db = Database::open(&t.path().join("subtitle-uncertain.db")).unwrap();
    let (id, _) = claimed_subtitle_job(&db, "subtitle-response-lost", "BV1subtitle1");
    let attempt_id = match db.begin_subtitle_attempt(&id, "BV1subtitle1").unwrap() {
        SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
        decision => panic!("首次字幕提交未创建 attempt: {decision:?}"),
    };
    // 故障注入：平台已经接受，但客户端响应丢失。
    db.mark_subtitle_attempt_uncertain(&id, &attempt_id, "平台响应丢失")
        .unwrap();
    db.defer_claimed_pending_subtitle(&id, "仅查询平台", -1)
        .unwrap();
    db.claim_next_pending_subtitle_job(16).unwrap().unwrap();

    let blocking = db.begin_subtitle_attempt(&id, "BV1subtitle1").unwrap();
    assert_eq!(
        blocking,
        SubtitleAttemptDecision::QueryOnly(SubtitleAttempt {
            id: attempt_id.clone(),
            bvid: "BV1subtitle1".into(),
            status: "uncertain".into(),
        })
    );
    let attempt_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM subtitle_attempts WHERE job_id=?",
            [&id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempt_count, 1, "不确定结果后又创建了第二次 submit");

    // 只读查询一旦看到平台已有 zh，才把 attempt 核对成功。
    db.reconcile_subtitle_attempt(&id, &attempt_id, true)
        .unwrap();
    assert_eq!(
        db.get_job(&id).unwrap().unwrap().status,
        JobStatus::Completed
    );
    let status: String = db
        .conn()
        .query_row(
            "SELECT status FROM subtitle_attempts WHERE id=?",
            [&attempt_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "reconciled");
}

#[test]
fn exhausted_uncertain_subtitle_attempt_stays_manual_and_query_only() {
    let t = tempfile::tempdir().unwrap();
    let db = Database::open(&t.path().join("subtitle-manual.db")).unwrap();
    let (id, _) = claimed_subtitle_job(&db, "subtitle-manual", "BV1subtitle6");
    let attempt_id = match db.begin_subtitle_attempt(&id, "BV1subtitle6").unwrap() {
        SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
        decision => panic!("首次字幕提交未创建 attempt: {decision:?}"),
    };
    db.mark_subtitle_attempt_uncertain(&id, &attempt_id, "长期无法确认")
        .unwrap();
    db.exhaust_claimed_pending_subtitle(&id, 16, "投稿结果无法确认，已转人工")
        .unwrap();

    assert!(db.next_pending_subtitle_job(16).unwrap().is_none());
    let job = db.get_job(&id).unwrap().unwrap();
    assert_eq!(job.subtitle_attempt, 16);
    assert!(job.error.as_deref().unwrap().contains("已转人工"));
    db.claim_subtitle_job_now(&id).unwrap().unwrap();
    assert!(matches!(
        db.begin_subtitle_attempt(&id, "BV1subtitle6").unwrap(),
        SubtitleAttemptDecision::QueryOnly(_)
    ));
}

#[test]
fn rejected_subtitle_attempt_allows_policy_retry() {
    let t = tempfile::tempdir().unwrap();
    let db = Database::open(&t.path().join("subtitle-rejected.db")).unwrap();
    let (id, _) = claimed_subtitle_job(&db, "subtitle-rejected", "BV1subtitle2");
    let first_attempt = match db.begin_subtitle_attempt(&id, "BV1subtitle2").unwrap() {
        SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
        decision => panic!("首次字幕提交未创建 attempt: {decision:?}"),
    };
    db.reject_subtitle_attempt(&id, &first_attempt, "code=1001 平台明确拒绝")
        .unwrap();

    let second_attempt = match db.begin_subtitle_attempt(&id, "BV1subtitle2").unwrap() {
        SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
        decision => panic!("rejected 后未允许策略重试: {decision:?}"),
    };
    assert_ne!(first_attempt, second_attempt);
    let statuses: Vec<String> = db
        .conn()
        .prepare("SELECT status FROM subtitle_attempts WHERE job_id=? ORDER BY started_at,id")
        .unwrap()
        .query_map([&id], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(statuses, ["rejected", "started"]);
}

#[test]
fn interrupted_subtitle_attempt_becomes_uncertain_after_restart() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("subtitle-interrupted.db");
    let first = Database::open(&path).unwrap();
    let (id, _) = claimed_subtitle_job(&first, "subtitle-interrupted", "BV1subtitle3");
    let attempt_id = match first.begin_subtitle_attempt(&id, "BV1subtitle3").unwrap() {
        SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
        decision => panic!("首次字幕提交未创建 attempt: {decision:?}"),
    };
    first
        .conn()
        .execute(
            "UPDATE jobs SET claim_expires_at=? WHERE id=?",
            params![
                (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
                &id
            ],
        )
        .unwrap();

    let restarted = Database::open(&path).unwrap();
    restarted.recover_incomplete_jobs().unwrap();
    let attempt = restarted.blocking_subtitle_attempt(&id).unwrap().unwrap();
    assert_eq!(attempt.id, attempt_id);
    assert_eq!(attempt.status, "uncertain");
    restarted
        .claim_next_pending_subtitle_job(16)
        .unwrap()
        .unwrap();
    assert!(matches!(
        restarted
            .begin_subtitle_attempt(&id, "BV1subtitle3")
            .unwrap(),
        SubtitleAttemptDecision::QueryOnly(_)
    ));
    let attempt_count: i64 = restarted
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM subtitle_attempts WHERE job_id=?",
            [&id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempt_count, 1);
}

#[test]
fn subtitle_success_with_local_commit_failure_blocks_resubmit() {
    let t = tempfile::tempdir().unwrap();
    let db = Database::open(&t.path().join("subtitle-commit-failure.db")).unwrap();
    let (id, _) = claimed_subtitle_job(&db, "subtitle-commit-failure", "BV1subtitle5");
    let attempt_id = match db.begin_subtitle_attempt(&id, "BV1subtitle5").unwrap() {
        SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
        decision => panic!("首次字幕提交未创建 attempt: {decision:?}"),
    };
    db.conn()
        .execute_batch(
            r#"
                CREATE TRIGGER fail_subtitle_job_finish
                BEFORE UPDATE ON jobs
                WHEN OLD.id=NEW.id AND NEW.claim_kind IS NULL
                BEGIN
                  SELECT RAISE(ABORT, '模拟字幕平台成功后的本地提交失败');
                END;
                "#,
        )
        .unwrap();

    let error = db
        .finish_subtitle_attempt(&id, &attempt_id, true)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("模拟字幕平台成功后的本地提交失败")
    );
    let status: String = db
        .conn()
        .query_row(
            "SELECT status FROM subtitle_attempts WHERE id=?",
            [&attempt_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "started", "事务失败后 attempt 被写成了半完成状态");
    db.mark_subtitle_attempt_uncertain(&id, &attempt_id, "本地确认事务失败")
        .unwrap();
    db.conn()
        .execute_batch("DROP TRIGGER fail_subtitle_job_finish")
        .unwrap();
    db.defer_claimed_pending_subtitle(&id, "仅查询平台", -1)
        .unwrap();
    db.claim_next_pending_subtitle_job(16).unwrap().unwrap();
    assert!(matches!(
        db.begin_subtitle_attempt(&id, "BV1subtitle5").unwrap(),
        SubtitleAttemptDecision::QueryOnly(_)
    ));
}

#[test]
fn confirmed_subtitle_attempt_finishes_with_job_atomically() {
    let t = tempfile::tempdir().unwrap();
    let db = Database::open(&t.path().join("subtitle-confirmed.db")).unwrap();
    let (id, _) = claimed_subtitle_job(&db, "subtitle-confirmed", "BV1subtitle4");
    let attempt_id = match db.begin_subtitle_attempt(&id, "BV1subtitle4").unwrap() {
        SubtitleAttemptDecision::Submit(attempt_id) => attempt_id,
        decision => panic!("首次字幕提交未创建 attempt: {decision:?}"),
    };
    db.finish_subtitle_attempt(&id, &attempt_id, true).unwrap();

    assert_eq!(
        db.get_job(&id).unwrap().unwrap().status,
        JobStatus::Completed
    );
    let status: String = db
        .conn()
        .query_row(
            "SELECT status FROM subtitle_attempts WHERE id=?",
            [&attempt_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "confirmed");
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
            UploadCompletionTiming {
                subtitle_delay_seconds: 90,
                next_submit_at: None,
            },
        )
        .is_err()
    );
    assert_eq!(
        db.get_job(&id).unwrap().unwrap().status,
        JobStatus::Uploading
    );
    assert!(db.prepared_upload(&id).unwrap().is_some());

    let next_submit_at = Utc::now() + chrono::Duration::minutes(30);
    assert!(
        db.finish_upload_attempt(
            &id,
            &attempt_id,
            "BV1uxE16ZE7e",
            JobStatus::Completed,
            TransferMode::Translated,
            UploadCompletionTiming {
                subtitle_delay_seconds: 90,
                next_submit_at: Some(next_submit_at),
            },
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
    assert_eq!(
        db.get_setting(NEXT_BILIBILI_SUBMIT_AT).unwrap(),
        Some(next_submit_at.to_rfc3339())
    );
}

#[test]
fn upload_completion_commit_failure_never_reopens_submission() {
    let t = tempfile::tempdir().unwrap();
    let path = t.path().join("upload-commit-failure.db");
    let db = Database::open(&path).unwrap();
    let id = db
        .create_job(NewJob {
            channel_id: None,
            video_id: "upload-commit-failure",
            url: "https://youtu.be/upload-commit-failure",
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
            video_path: "/tmp/upload-commit-failure.mp4".into(),
            cover_path: "/tmp/upload-commit-failure.jpg".into(),
            mode: TransferMode::Direct,
            completion_status: JobStatus::Completed,
        },
    )
    .unwrap();
    let attempt_id = db.begin_prepared_upload(&id).unwrap().unwrap();
    db.conn()
        .execute_batch(
            r#"
                CREATE TRIGGER fail_upload_cooldown
                BEFORE INSERT ON settings
                WHEN NEW.key='bilibili.next_submit_at'
                BEGIN
                  SELECT RAISE(ABORT, '模拟平台成功后的本地提交失败');
                END;
                "#,
        )
        .unwrap();

    let error = db
        .finish_upload_attempt(
            &id,
            &attempt_id,
            "BV1uxE16ZE7e",
            JobStatus::Completed,
            TransferMode::Direct,
            UploadCompletionTiming {
                subtitle_delay_seconds: 90,
                next_submit_at: Some(Utc::now() + chrono::Duration::minutes(30)),
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("模拟平台成功后的本地提交失败"));
    // 整个完成事务已回滚；调用方随后必须把唯一 attempt 固定在不确定态。
    db.mark_upload_attempt_uncertain(&id, &attempt_id, "平台已成功，本地提交失败")
        .unwrap();
    assert_eq!(
        db.get_job(&id).unwrap().unwrap().status,
        JobStatus::UploadUncertain
    );
    assert!(db.get_setting(NEXT_BILIBILI_SUBMIT_AT).unwrap().is_none());
    drop(db);

    let reopened = Database::open(&path).unwrap();
    assert!(reopened.next_ready_to_upload_job().unwrap().is_none());
    assert!(reopened.begin_prepared_upload(&id).unwrap().is_none());
    let attempts: i64 = reopened
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM upload_attempts WHERE job_id=?",
            [&id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempts, 1);
}

#[test]
fn upload_claim_atomically_observes_deadline_and_maintenance_hold() {
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
    assert!(
        db.acquire_maintenance_hold("LIVE_ONCE:test", "一次性直播旁路", 300)
            .unwrap()
    );
    assert!(db.begin_prepared_upload(&id).unwrap().is_none());
    assert_eq!(
        db.get_job(&id).unwrap().unwrap().status,
        JobStatus::ReadyToUpload
    );

    assert!(db.release_maintenance_hold("LIVE_ONCE:test").unwrap());
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
            UploadCompletionTiming {
                subtitle_delay_seconds: 90,
                next_submit_at: None,
            },
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
            transfer_mode: TransferMode::Translated,
        })
        .unwrap()
        .unwrap();
    db.queue_prepared_upload(
        &id,
        &PreparedUpload::Submission {
            video_path: "/tmp/confirmed.mp4".into(),
            cover_path: "/tmp/confirmed.jpg".into(),
            mode: TransferMode::Translated,
            completion_status: JobStatus::Completed,
        },
    )
    .unwrap();
    let attempt_id = db.begin_prepared_upload(&id).unwrap().unwrap();
    db.mark_upload_attempt_uncertain(&id, &attempt_id, "lost response")
        .unwrap();
    let next_submit_at = Utc::now() + chrono::Duration::minutes(30);
    assert!(
        db.confirm_uncertain_upload(
            &id,
            "BV17x411w7KC",
            JobStatus::Completed,
            TransferMode::Translated,
            UploadCompletionTiming {
                subtitle_delay_seconds: 90,
                next_submit_at: Some(next_submit_at),
            },
        )
        .unwrap()
    );

    let job = db.get_job(&id).unwrap().unwrap();
    assert_eq!(job.status, JobStatus::UploadedOriginalPendingSubtitle);
    assert_eq!(job.bvid.as_deref(), Some("BV17x411w7KC"));
    assert!(db.prepared_upload(&id).unwrap().is_none());
    let (status, subtitle_retry_at): (String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT upload_attempts.status,jobs.subtitle_retry_at \
                 FROM upload_attempts JOIN jobs ON jobs.id=upload_attempts.job_id \
                 WHERE upload_attempts.id=?",
            [&attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "reconciled");
    assert!(subtitle_retry_at.is_some(), "核对成功后没有排入字幕队列");
    assert_eq!(
        db.get_setting(NEXT_BILIBILI_SUBMIT_AT).unwrap(),
        Some(next_submit_at.to_rfc3339())
    );
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
        db.update_claimed_job_status(&id, PREPARE_CLAIM_KIND, JobStatus::Processing, None, false,)
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
        // 恢复 SQL 不需要先把旧状态映射成枚举；直接读取则必须拒绝未知值。
        assert!(db.get_job(&id).is_err());
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
