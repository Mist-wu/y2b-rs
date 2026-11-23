use std::{path::Path, process::Command};

fn y2b(config: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_y2b"));
    cmd.arg("--config").arg(config);
    cmd
}

fn write_config(path: &Path, database: &Path, extra: &str) -> String {
    let raw = format!(
        "# keep this comment\n[runtime]\ndata_dir = \"{}\"\ndatabase = \"{}\"\ndownload_dir = \"{}\"\nbackup_dir = \"{}\"\n\n[ai]\npi = \"/usr/bin/old-pi\"\nextension = \"/opt/old-extension.ts\"\n\n[monitor]\npoll_seconds = 0\n{extra}",
        path.parent().unwrap().join("data").display(),
        database.display(),
        path.parent().unwrap().join("downloads").display(),
        path.parent().unwrap().join("backups").display(),
    );
    std::fs::write(path, &raw).unwrap();
    raw
}

#[test]
fn model_commands_and_init_do_not_rewrite_config_or_create_a_database() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let database = directory.path().join("missing").join("state.db");
    let original = write_config(&config_path, &database, "");

    for (args, expected) in [
        (
            &["model", "list"][..],
            &["deepseek/deepseek-flash", "thinking=off"][..],
        ),
        (&["model", "set", "deepseek-flash"], &["AI profile 已确认"]),
        (&["config-check"], &["AI profile:"]),
    ] {
        let output = y2b(&config_path).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        for message in expected {
            assert!(stdout.contains(message), "{stdout}");
        }
        assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);
        assert!(!database.exists());
        assert!(!directory.path().join("data").exists());
    }
    let rejected = y2b(&config_path)
        .args(["model", "set", "other-model"])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);

    let init = y2b(&config_path).arg("init").output().unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);
    assert!(directory.path().join("downloads").is_dir());
    assert!(!database.exists());

    let typo = directory.path().join("typo.toml");
    write_config(&typo, &database, "poll_second = 1\n");
    let typo_run = y2b(&typo).args(["model", "list"]).output().unwrap();
    assert!(!typo_run.status.success());

    let drifted = directory.path().join("drift.toml");
    let drifted_raw = write_config(&drifted, &database, "").replace(
        "extension = \"/opt/old-extension.ts\"",
        "extension = \"/opt/old-extension.ts\"\nmodel = \"other-model\"",
    );
    std::fs::write(&drifted, &drifted_raw).unwrap();
    let drift_run = y2b(&drifted).args(["model", "list"]).output().unwrap();
    assert!(!drift_run.status.success(), "偏离固定模型的配置必须拒绝");
    assert_eq!(std::fs::read_to_string(&drifted).unwrap(), drifted_raw);

    let home = directory.path().join("fresh-home");
    let fresh = directory.path().join("fresh.toml");
    let created = y2b(&fresh)
        .env("Y2B_HOME", &home)
        .arg("init")
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let generated = std::fs::read_to_string(&fresh).unwrap();
    assert!(generated.contains("deepseek-flash"), "{generated}");
    assert!(generated.contains("poll_seconds = 60"), "{generated}");
    assert!(home.join("downloads").is_dir());
    assert!(!home.join("state.db").exists());
}

#[test]
fn migrate_and_schema_version_report_the_binary_schema_without_config() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("missing.toml");
    let database = dir.path().join("explicit.db");
    let migrate = y2b(&config)
        .args(["migrate", "--database"])
        .arg(&database)
        .output()
        .unwrap();
    assert!(
        migrate.status.success(),
        "{}",
        String::from_utf8_lossy(&migrate.stderr)
    );
    let schema = y2b(&config).arg("schema-version").output().unwrap();
    assert!(schema.status.success());
    assert_eq!(migrate.stdout, schema.stdout);
    assert_eq!(
        y2b_rs::Database::open_existing(&database)
            .unwrap()
            .schema_version()
            .unwrap()
            .to_string(),
        String::from_utf8(schema.stdout).unwrap().trim()
    );
}
