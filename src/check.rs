use crate::{
    config::Config,
    db::{CURRENT_SCHEMA_VERSION, Database},
    process::run_monitored,
};
use anyhow::{Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckItem {
    pub name: String,
    pub ok: bool,
    pub required: bool,
    pub detail: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Baseline {
    pub generated_at: String,
    pub os: String,
    pub arch: String,
    pub items: Vec<BaselineItem>,
    #[serde(default)]
    pub details: std::collections::BTreeMap<String, String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineItem {
    pub name: String,
    pub path: String,
    pub version: String,
    pub sha256: Option<String>,
}

/// 依赖工具清单：名称、可执行路径、版本探测参数。
/// `run` 与 `write_baseline` 共用，保证两边检测一致。
fn tool_checks(config: &Config) -> [(&'static str, PathBuf, Vec<&'static str>); 5] {
    [
        ("pi", PathBuf::from(&config.ai.pi), vec!["--version"]),
        (
            "yt-dlp",
            PathBuf::from(&config.youtube.yt_dlp),
            vec!["--version"],
        ),
        (
            "ffmpeg",
            PathBuf::from(&config.render.ffmpeg),
            vec!["-version"],
        ),
        (
            "ffprobe",
            PathBuf::from(&config.render.ffprobe),
            vec!["-version"],
        ),
        (
            "biliup",
            PathBuf::from(&config.bilibili.biliup),
            vec!["--version"],
        ),
    ]
}

fn baseline_tools(
    config: &Config,
) -> impl Iterator<Item = (&'static str, PathBuf, Vec<&'static str>)> {
    // 保持现有基线格式：ffprobe 检查可运行性，但不记录摘要。
    tool_checks(config)
        .into_iter()
        .filter(|(name, _, _)| *name != "ffprobe")
}

fn policy_resources(config: &Config) -> [(&'static str, &'static str, PathBuf); 4] {
    [
        ("pi-extension", "Pi extension", config.ai.extension.clone()),
        ("pi-policy", "Pi policy", config.ai.policy.clone()),
        (
            "pi-audit-policy",
            "Pi audit policy",
            config.ai.policy.with_file_name("audit-policy.json"),
        ),
        (
            "brawl-stars-glossary",
            "Brawl Stars glossary",
            config.ai.policy.with_file_name("brawl-stars-glossary.json"),
        ),
    ]
}

fn baseline_check(config: &Config, baseline: &Baseline) -> CheckItem {
    let mut expected = baseline_tools(config)
        .map(|(name, path, _)| (name, path))
        .chain(policy_resources(config).map(|(name, _, path)| (name, path)))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut external_drift = Vec::new();
    let mut y2b_seen = false;
    let mut y2b_drift = false;
    for item in &baseline.items {
        if item.name == "y2b" {
            // 应用更新/旧 release 清理是正常部署行为，仅作告警。
            y2b_drift |= y2b_seen || !baseline_hash_matches(item, Path::new(&item.path));
            y2b_seen = true;
        } else if let Some(path) = expected.remove(item.name.as_str()) {
            if Path::new(&item.path) != path || !baseline_hash_matches(item, &path) {
                external_drift.push(item.name.clone());
            }
        } else {
            external_drift.push(format!("{}（重复或未知条目）", item.name));
        }
    }
    external_drift.extend(expected.keys().map(|name| format!("{name}（缺少条目）")));
    y2b_drift |= !y2b_seen;
    let mut details = Vec::new();
    if !external_drift.is_empty() {
        details.push(format!("漂移或基线不完整: {}", external_drift.join(", ")));
    }
    if y2b_drift {
        details.push("y2b 自身与基线不一致（部署预期变更，仅告警）".into());
    }
    CheckItem {
        name: "dependency baseline".into(),
        ok: details.is_empty(),
        required: !external_drift.is_empty() || !y2b_drift,
        detail: if details.is_empty() {
            format!("无漂移，基线 {}", baseline.generated_at)
        } else {
            details.join("；")
        },
    }
}

fn baseline_hash_matches(item: &BaselineItem, path: &Path) -> bool {
    item.sha256.as_ref().is_some_and(|expected| {
        hash_file(path)
            .map(|actual| actual == *expected)
            .unwrap_or(false)
    })
}

fn schema_check(schema: i64) -> CheckItem {
    CheckItem {
        name: "database schema".into(),
        ok: schema == CURRENT_SCHEMA_VERSION,
        required: true,
        detail: format!("v{schema}，期望 v{CURRENT_SCHEMA_VERSION}"),
    }
}

pub async fn run(config: &Config, db: &Database) -> Vec<CheckItem> {
    let mut out = Vec::new();
    for (name, path, args) in tool_checks(config) {
        if !path.exists() {
            out.push(CheckItem {
                name: name.into(),
                ok: false,
                required: true,
                detail: format!("未找到 {}", path.display()),
            });
            continue;
        }
        let mut c = Command::new(&path);
        c.args(args);
        match run_monitored(c, Duration::from_secs(20)).await {
            Ok(r) => out.push(CheckItem {
                name: name.into(),
                ok: true,
                required: true,
                detail: first_line(&(r.stdout + r.stderr.as_str())),
            }),
            Err(e) => out.push(CheckItem {
                name: name.into(),
                ok: false,
                required: true,
                detail: e.to_string(),
            }),
        }
    }
    let swap = fs::read_to_string("/proc/swaps")
        .map(|x| x.lines().count() > 1)
        .unwrap_or(false);
    out.push(CheckItem {
        name: "swap".into(),
        ok: swap,
        required: false,
        detail: if swap {
            "已启用".into()
        } else {
            "未启用".into()
        },
    });
    out.push(match fs2::available_space(&config.runtime.data_dir) {
        Ok(bytes) => {
            let free = bytes / (1024 * 1024 * 1024);
            CheckItem {
                name: "disk".into(),
                ok: free >= config.storage.stop_free_gib,
                required: true,
                detail: format!("剩余 {free} GiB"),
            }
        }
        Err(error) => CheckItem {
            name: "disk".into(),
            ok: false,
            required: true,
            detail: format!("读取磁盘空间失败: {error}"),
        },
    });
    let integrity = db.integrity_check().unwrap_or_else(|e| e.to_string());
    out.push(CheckItem {
        name: "database".into(),
        ok: integrity == "ok",
        required: true,
        detail: integrity,
    });
    out.push(match db.schema_version() {
        Ok(schema) => schema_check(schema),
        Err(error) => CheckItem {
            name: "database schema".into(),
            ok: false,
            required: true,
            detail: format!("读取版本失败: {error}"),
        },
    });
    for (name, p) in [
        ("YouTube cookies", config.youtube.cookies.clone()),
        ("Bilibili cookies", config.bilibili.cookies.clone()),
    ]
    .into_iter()
    .chain(policy_resources(config).map(|(_, label, path)| (label, path)))
    {
        out.push(CheckItem {
            name: name.into(),
            ok: p.exists(),
            required: true,
            detail: p.display().to_string(),
        });
    }
    let baseline_path = config.runtime.data_dir.join("dependency-baseline.json");
    if baseline_path.exists() {
        match fs::read(&baseline_path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Baseline>(&raw).ok())
        {
            Some(baseline) => out.push(baseline_check(config, &baseline)),
            None => out.push(CheckItem {
                name: "dependency baseline".into(),
                ok: false,
                required: true,
                detail: "基线 JSON 无效".into(),
            }),
        }
    } else {
        out.push(CheckItem {
            name: "dependency baseline".into(),
            ok: false,
            required: true,
            detail: format!("未找到 {}", baseline_path.display()),
        });
    }
    for (key, label) in [
        ("auth.youtube", "YouTube auth"),
        ("auth.bilibili", "Bilibili auth"),
    ] {
        out.push(match db.get_setting(key) {
            Ok(Some(value)) => CheckItem {
                name: label.into(),
                ok: value.starts_with("ok "),
                required: true,
                detail: value,
            },
            Ok(None) => CheckItem {
                name: label.into(),
                ok: false,
                required: true,
                detail: "未检查".into(),
            },
            Err(error) => CheckItem {
                name: label.into(),
                ok: false,
                required: true,
                detail: format!("读取状态失败: {error}"),
            },
        });
    }
    out
}

pub async fn write_baseline(
    config: &Config,
    dest: &Path,
    checks: &[CheckItem],
) -> Result<Baseline> {
    let mut items = Vec::new();
    // 版本必须复用本轮检查结果，不能再次执行命令后把另一份结果写入基线。
    for (name, path, _) in baseline_tools(config) {
        if !path.exists() {
            anyhow::bail!("生成依赖基线失败，缺少必选工具 {name}: {}", path.display());
        }
        let probe = checks
            .iter()
            .find(|item| item.name == name)
            .with_context(|| format!("生成依赖基线失败，缺少必选工具 {name} 的检查结果"))?;
        if !probe.ok {
            anyhow::bail!(
                "生成依赖基线失败，必选工具 {name} 检查未通过: {}",
                probe.detail
            );
        }
        items.push(BaselineItem {
            name: name.into(),
            path: path.display().to_string(),
            version: probe.detail.clone(),
            sha256: Some(hash_file(&path)?),
        });
    }
    if let Ok(path) = std::env::current_exe() {
        items.push(BaselineItem {
            name: "y2b".into(),
            path: path.display().to_string(),
            version: env!("CARGO_PKG_VERSION").into(),
            sha256: Some(hash_file(&path)?),
        });
    }
    for (name, _, path) in policy_resources(config) {
        items.push(BaselineItem {
            name: name.into(),
            path: path.display().to_string(),
            version: String::new(),
            sha256: Some(hash_file(&path)?),
        });
    }
    let mut details = std::collections::BTreeMap::new();
    details.insert(
        "kernel".into(),
        fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_default()
            .trim()
            .into(),
    );
    details.insert(
        "cargo_lock_sha256".into(),
        hash_file(Path::new("/opt/y2b/Cargo.lock")).unwrap_or_else(|_| "missing".into()),
    );
    let b = Baseline {
        generated_at: Utc::now().to_rfc3339(),
        os: fs::read_to_string("/etc/os-release").unwrap_or_else(|_| std::env::consts::OS.into()),
        arch: std::env::consts::ARCH.into(),
        items,
        details,
    };
    if let Some(p) = dest.parent() {
        fs::create_dir_all(p)?;
    }
    fs::write(dest, serde_json::to_vec_pretty(&b)?)?;
    Ok(b)
}
fn hash_file(path: &Path) -> Result<String> {
    let mut h = Sha256::new();
    h.update(fs::read(path).with_context(|| format!("读取 {}", path.display()))?);
    Ok(hex::encode(h.finalize()))
}
fn first_line(s: &str) -> String {
    s.lines()
        .find(|x| !x.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_check_rejects_versions_on_both_sides() {
        assert!(schema_check(CURRENT_SCHEMA_VERSION).ok);
        assert!(!schema_check(CURRENT_SCHEMA_VERSION - 1).ok);
        assert!(!schema_check(CURRENT_SCHEMA_VERSION + 1).ok);
    }

    #[tokio::test]
    async fn missing_baseline_and_auth_states_are_required_failures() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.runtime.data_dir = temp.path().to_path_buf();
        let missing = temp.path().join("missing-tool").display().to_string();
        config.ai.pi = missing.clone();
        config.youtube.yt_dlp = missing.clone();
        config.render.ffmpeg = missing.clone();
        config.render.ffprobe = missing.clone();
        config.bilibili.biliup = missing;
        let db = Database::open(&temp.path().join("state.db")).unwrap();

        let checks = run(&config, &db).await;
        for name in ["dependency baseline", "YouTube auth", "Bilibili auth"] {
            let item = checks.iter().find(|item| item.name == name).unwrap();
            assert!(item.required);
            assert!(!item.ok);
        }
    }

    #[tokio::test]
    async fn missing_required_tool_does_not_create_partial_baseline() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.ai.pi = temp.path().join("missing-pi").display().to_string();
        let destination = temp.path().join("dependency-baseline.json");
        let checks = vec![CheckItem {
            name: "pi".into(),
            ok: false,
            required: true,
            detail: "未找到 pi".into(),
        }];

        let error = write_baseline(&config, &destination, &checks)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("缺少必选工具 pi"));
        assert!(!destination.exists());
    }

    async fn baseline_fixture(dir: &Path) -> (Config, Database, Baseline) {
        use std::os::unix::fs::PermissionsExt;

        let mut config = Config::default();
        config.runtime.data_dir = dir.to_path_buf();
        let tool = dir.join("tool");
        fs::write(&tool, "#!/bin/sh\nprintf 'fixture-version\\n'\n").unwrap();
        fs::set_permissions(&tool, fs::Permissions::from_mode(0o700)).unwrap();
        let tool = tool.display().to_string();
        config.ai.pi = tool.clone();
        config.youtube.yt_dlp = tool.clone();
        config.render.ffmpeg = tool.clone();
        config.render.ffprobe = tool.clone();
        config.bilibili.biliup = tool;
        config.ai.extension = dir.join("extension.ts");
        config.ai.policy = dir.join("policy.json");
        config.youtube.cookies = dir.join("youtube.cookies");
        config.bilibili.cookies = dir.join("bilibili.cookies");
        for (_, _, path) in policy_resources(&config) {
            fs::write(path, "fixture-resource").unwrap();
        }
        let db = Database::open(&dir.join("state.db")).unwrap();
        let checks = run(&config, &db).await;
        let baseline = write_baseline(&config, &dir.join("dependency-baseline.json"), &checks)
            .await
            .unwrap();
        (config, db, baseline)
    }

    fn save_test_baseline(dir: &Path, baseline: &Baseline) {
        fs::write(
            dir.join("dependency-baseline.json"),
            serde_json::to_vec_pretty(baseline).unwrap(),
        )
        .unwrap();
    }

    fn baseline_result(checks: &[CheckItem]) -> &CheckItem {
        checks
            .iter()
            .find(|item| item.name == "dependency baseline")
            .unwrap()
    }

    #[tokio::test]
    async fn generated_baseline_matches_current_dependencies() {
        let temp = tempfile::tempdir().unwrap();
        let (config, db, baseline) = baseline_fixture(temp.path()).await;
        let checks = run(&config, &db).await;
        assert!(baseline_result(&checks).ok);
        // Existing baselines deliberately omit ffprobe, which is probed separately.
        assert!(!baseline.items.iter().any(|item| item.name == "ffprobe"));
    }

    #[tokio::test]
    async fn external_dependency_drift_is_a_required_failure() {
        let temp = tempfile::tempdir().unwrap();
        let (config, db, _) = baseline_fixture(temp.path()).await;
        fs::write(&config.ai.policy, "changed-resource").unwrap();
        let checks = run(&config, &db).await;
        let item = baseline_result(&checks);
        assert!(item.required);
        assert!(!item.ok);
        assert!(item.detail.contains("pi-policy"), "{}", item.detail);
    }

    #[tokio::test]
    async fn changed_dependency_path_cannot_keep_checking_the_old_file() {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, db, _) = baseline_fixture(temp.path()).await;
        let old_tool = config.youtube.yt_dlp.clone();
        let new_tool = temp.path().join("new-tool");
        fs::copy(&old_tool, &new_tool).unwrap();
        // Even identical bytes at a different configured path require explicit rebaselining.
        config.youtube.yt_dlp = new_tool.display().to_string();
        let checks = run(&config, &db).await;
        assert!(Path::new(&old_tool).exists());
        assert!(checks.iter().find(|item| item.name == "yt-dlp").unwrap().ok);
        let item = baseline_result(&checks);
        assert!(item.required && !item.ok, "{item:?}");
        assert!(item.detail.contains("yt-dlp"), "{}", item.detail);

        let old_policy = config.ai.policy.clone();
        config.youtube.yt_dlp = old_tool;
        config.ai.policy = temp.path().join("new-policy.json");
        fs::copy(old_policy, &config.ai.policy).unwrap();
        let checks = run(&config, &db).await;
        let item = baseline_result(&checks);
        assert!(item.required && !item.ok, "{item:?}");
        assert!(item.detail.contains("pi-policy"), "{}", item.detail);
    }

    #[tokio::test]
    async fn incomplete_or_ambiguous_baselines_are_required_failures() {
        let temp = tempfile::tempdir().unwrap();
        let (config, db, baseline) = baseline_fixture(temp.path()).await;
        let mut empty = baseline.clone();
        empty.items.clear();
        let mut missing = baseline.clone();
        missing.items.retain(|item| item.name != "pi-policy");
        let mut no_hash = baseline.clone();
        no_hash
            .items
            .iter_mut()
            .find(|item| item.name == "yt-dlp")
            .unwrap()
            .sha256 = None;
        let mut duplicate = baseline.clone();
        duplicate.items.push(duplicate.items[0].clone());
        for (case, invalid) in [
            ("empty", empty),
            ("missing", missing),
            ("null hash", no_hash),
            ("duplicate", duplicate),
        ] {
            save_test_baseline(temp.path(), &invalid);
            let checks = run(&config, &db).await;
            let item = baseline_result(&checks);
            assert!(item.required && !item.ok, "{case}: {item:?}");
        }
    }

    #[tokio::test]
    async fn y2b_own_drift_is_only_a_warning() {
        let temp = tempfile::tempdir().unwrap();
        let (config, db, mut baseline) = baseline_fixture(temp.path()).await;
        baseline
            .items
            .iter_mut()
            .find(|item| item.name == "y2b")
            .unwrap()
            .sha256 = Some("f".repeat(64));
        save_test_baseline(temp.path(), &baseline);
        let checks = run(&config, &db).await;
        let item = baseline_result(&checks);
        assert!(!item.required, "y2b 自身漂移不应构成必选失败");
        assert!(!item.ok);
        assert!(item.detail.contains("仅告警"), "{}", item.detail);
    }

    #[tokio::test]
    async fn missing_resource_cannot_overwrite_a_complete_baseline() {
        let temp = tempfile::tempdir().unwrap();
        let (config, db, _) = baseline_fixture(temp.path()).await;
        let path = temp.path().join("dependency-baseline.json");
        let original = fs::read(&path).unwrap();
        fs::remove_file(&config.ai.policy).unwrap();
        let checks = run(&config, &db).await;
        assert!(write_baseline(&config, &path, &checks).await.is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
    }
}
