use y2b_rs::model::{JobStatus, TransferMode};
use y2b_rs::monitor::Monitor;
use y2b_rs::{Config, Database};

#[tokio::test]
async fn external_crate_keeps_terminal_transfer_mode_and_rss_probe() {
    assert!(JobStatus::Completed.is_terminal());
    assert!(JobStatus::DeadLetter.is_terminal());
    assert!(JobStatus::Failed.is_terminal());
    assert!(!JobStatus::Queued.is_terminal());
    assert!(!JobStatus::UploadUncertain.is_terminal());

    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(&directory.path().join("api.db")).unwrap();
    let id = db
        .add_channel(
            "UC-public-api",
            "public",
            "https://www.youtube.com/@public",
            "https://example.invalid/feed",
            TransferMode::Translated,
        )
        .unwrap();
    assert_eq!(
        db.channel_transfer_mode(id).unwrap(),
        TransferMode::Translated
    );
    db.defer_channel_poll_until(id, chrono::Utc::now() + chrono::Duration::hours(2))
        .unwrap();
    let monitor = Monitor::new(Config::default(), db).unwrap();
    assert_eq!(monitor.poll_rss_probes(3).await.unwrap(), 0);
}
