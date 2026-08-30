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
                // 基线条目分两类：外部依赖（pi、yt-dlp、ffmpeg、biliup、Pi 资源文件）
                // 被偷换是意外，必须作为必选失败；y2b 自身的变化是部署的预期结果，
                // 部署脚本本身就是变更来源，不应由它拦住部署，因此只降级为告警。
                let mut external_drift = Vec::new();
                let mut y2b_drift = false;
                for item in &baseline.items {
                    let drifted = item.sha256.as_ref().is_some_and(|expected| {
                        hash_file(Path::new(&item.path))
                            .map(|actual| actual != *expected)
                            .unwrap_or(true)
                    });
                    if !drifted {
                        continue;
                    }
                    if item.name == "y2b" {
                        y2b_drift = true;
                    } else {
                        external_drift.push(item.name.clone());
                    }
                }
                let y2b_only_drift = y2b_drift && external_drift.is_empty();
                out.push(CheckItem {
                    name: "dependency baseline".into(),
                    ok: external_drift.is_empty() && !y2b_drift,
                    required: !y2b_only_drift,
                    detail: if external_drift.is_empty() && !y2b_drift {
                        format!("无漂移，基线 {}", baseline.generated_at)
                    } else {
                        let mut parts = Vec::new();
                        if !external_drift.is_empty() {
                            parts.push(format!("漂移: {}", external_drift.join(", ")));
                        }
                        if y2b_drift {
                            parts.push("y2b 自身与基线不一致（部署预期变更，仅告警）".into());
                        }
                        parts.join("；")
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

    fn config_with_missing_tools(data_dir: &Path) -> Config {
        let mut config = Config::default();
        config.runtime.data_dir = data_dir.to_path_buf();
        let missing = data_dir.join("missing-tool").display().to_string();
        config.ai.pi = missing.clone();
        config.youtube.yt_dlp = missing.clone();
        config.render.ffmpeg = missing.clone();
        config.render.ffprobe = missing.clone();
        config.bilibili.biliup = missing;
        config
    }

    fn write_baseline_json(dir: &Path, items: Vec<BaselineItem>) {
        let baseline = Baseline {
            generated_at: "test".into(),
            os: "test".into(),
            arch: "test".into(),
            items,
            details: std::collections::BTreeMap::new(),
        };
        fs::write(
            dir.join("dependency-baseline.json"),
            serde_json::to_vec_pretty(&baseline).unwrap(),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn external_dependency_drift_is_a_required_failure() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_with_missing_tools(temp.path());
        let ytdlp = temp.path().join("yt-dlp");
        fs::write(&ytdlp, "2026.01.01\n").unwrap();
        let y2b = temp.path().join("y2b");
        fs::write(&y2b, "current-binary\n").unwrap();
        write_baseline_json(
            temp.path(),
            vec![
                BaselineItem {
                    name: "yt-dlp".into(),
                    path: ytdlp.display().to_string(),
                    version: "2026.01.01".into(),
                    // 与磁盘上的 yt-dlp 不一致，模拟外部依赖被偷换。
                    sha256: Some("0".repeat(64)),
                },
                BaselineItem {
                    name: "y2b".into(),
                    path: y2b.display().to_string(),
                    version: "test".into(),
                    sha256: Some(hash_file(&y2b).unwrap()),
                },
            ],
        );
        let db = Database::open(&temp.path().join("state.db")).unwrap();

        let checks = run(&config, &db).await;
        let item = checks
            .iter()
            .find(|item| item.name == "dependency baseline")
            .unwrap();
        assert!(item.required, "外部依赖漂移必须保持必选失败");
        assert!(!item.ok);
        assert!(item.detail.contains("yt-dlp"), "{}", item.detail);
    }

    #[tokio::test]
    async fn y2b_own_drift_is_only_a_warning() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_with_missing_tools(temp.path());
        let ytdlp = temp.path().join("yt-dlp");
        fs::write(&ytdlp, "2026.01.01\n").unwrap();
        let y2b = temp.path().join("y2b");
        fs::write(&y2b, "new-binary\n").unwrap();
        write_baseline_json(
            temp.path(),
            vec![
                BaselineItem {
                    name: "yt-dlp".into(),
                    path: ytdlp.display().to_string(),
                    version: "2026.01.01".into(),
                    sha256: Some(hash_file(&ytdlp).unwrap()),
                },
                BaselineItem {
                    name: "y2b".into(),
                    path: y2b.display().to_string(),
                    version: "test".into(),
                    // 基线记录的是上一版二进制，当前二进制已经更新。
                    sha256: Some("f".repeat(64)),
                },
            ],
        );
        let db = Database::open(&temp.path().join("state.db")).unwrap();

        let checks = run(&config, &db).await;
        let item = checks
            .iter()
            .find(|item| item.name == "dependency baseline")
            .unwrap();
        assert!(!item.required, "y2b 自身漂移不应构成必选失败");
        assert!(!item.ok);
        assert!(item.detail.contains("y2b"), "{}", item.detail);
        assert!(item.detail.contains("仅告警"), "{}", item.detail);
    }
}
