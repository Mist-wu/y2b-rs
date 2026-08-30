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
    let glossary_path = config.ai.policy.with_file_name("brawl-stars-glossary.json");
    let audit_policy_path = config.ai.policy.with_file_name("audit-policy.json");
    for (name, p) in [
        ("YouTube cookies", &config.youtube.cookies),
        ("Bilibili cookies", &config.bilibili.cookies),
        ("Pi extension", &config.ai.extension),
        ("Pi policy", &config.ai.policy),
        ("Pi audit policy", &audit_policy_path),
        ("Brawl Stars glossary", &glossary_path),
    ] {
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
            Some(baseline) => {
                let drift = baseline
                    .items
                    .iter()
                    .filter(|item| {
                        item.sha256.as_ref().is_some_and(|expected| {
                            hash_file(Path::new(&item.path))
                                .map(|actual| actual != *expected)
                                .unwrap_or(true)
                        })
                    })
                    .map(|x| x.name.clone())
                    .collect::<Vec<_>>();
                out.push(CheckItem {
                    name: "dependency baseline".into(),
                    ok: drift.is_empty(),
                    required: true,
                    detail: if drift.is_empty() {
                        format!("无漂移，基线 {}", baseline.generated_at)
                    } else {
                        format!("漂移: {}", drift.join(", "))
                    },
                });
            }
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
    // 基线只记录会被二进制更新影响的工具，ffprobe 由 run() 检查但不入基线。
    // 版本必须复用本轮检查结果，不能再次执行命令后把另一份结果写入基线。
    for (name, path, _) in tool_checks(config)
        .into_iter()
        .filter(|(name, _, _)| *name != "ffprobe")
    {
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
    let glossary_path = config.ai.policy.with_file_name("brawl-stars-glossary.json");
    let audit_policy_path = config.ai.policy.with_file_name("audit-policy.json");
    for (name, path) in [
        ("pi-extension", config.ai.extension.as_path()),
        ("pi-policy", config.ai.policy.as_path()),
        ("pi-audit-policy", audit_policy_path.as_path()),
        ("brawl-stars-glossary", glossary_path.as_path()),
    ] {
        if path.exists() {
            items.push(BaselineItem {
                name: name.into(),
                path: path.display().to_string(),
                version: String::new(),
                sha256: Some(hash_file(path)?),
            });
        }
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
}
