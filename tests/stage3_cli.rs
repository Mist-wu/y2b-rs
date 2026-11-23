use std::process::Command;

fn y2b() -> Command {
    Command::new(env!("CARGO_BIN_EXE_y2b"))
}

fn write_config(path: &std::path::Path, database: &std::path::Path, extra: &str) -> String {
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

    let list = y2b()
        .args(["--config", config_path.to_str().unwrap(), "model", "list"])
        .output()
        .unwrap();
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8_lossy(&list.stdout);
    assert!(stdout.contains("deepseek/deepseek-flash"), "{stdout}");
    assert!(stdout.contains("thinking=off"), "{stdout}");
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);
    assert!(!database.exists());
    assert!(!directory.path().join("data").exists());

    let set = y2b()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "model",
            "set",
            "deepseek-flash",
        ])
        .output()
        .unwrap();
    assert!(
        set.status.success(),
        "{}",
        String::from_utf8_lossy(&set.stderr)
    );
    assert!(String::from_utf8_lossy(&set.stdout).contains("AI profile 已确认"));
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);
    assert!(!database.exists());

    let rejected = y2b()
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "model",
            "set",
            "other-model",
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);

    let checked = y2b()
        .args(["--config", config_path.to_str().unwrap(), "config-check"])
        .output()
        .unwrap();
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    assert_eq!(std::fs::read_to_string(&config_path).unwrap(), original);

    let init = y2b()
        .args(["--config", config_path.to_str().unwrap(), "init"])
        .output()
        .unwrap();
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
    let typo_run = y2b()
        .args(["--config", typo.to_str().unwrap(), "model", "list"])
        .output()
        .unwrap();
    assert!(!typo_run.status.success());

    let drifted = directory.path().join("drift.toml");
    let drifted_raw = write_config(&drifted, &database, "").replace(
        "extension = \"/opt/old-extension.ts\"",
        "extension = \"/opt/old-extension.ts\"\nmodel = \"other-model\"",
    );
    std::fs::write(&drifted, &drifted_raw).unwrap();
    let drift_run = y2b()
        .args(["--config", drifted.to_str().unwrap(), "model", "list"])
        .output()
        .unwrap();
    assert!(!drift_run.status.success(), "偏离固定模型的配置必须拒绝");
    assert_eq!(std::fs::read_to_string(&drifted).unwrap(), drifted_raw);

    let home = directory.path().join("fresh-home");
    let fresh = directory.path().join("fresh.toml");
    let created = y2b()
        .env("Y2B_HOME", &home)
        .args(["--config", fresh.to_str().unwrap(), "init"])
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
