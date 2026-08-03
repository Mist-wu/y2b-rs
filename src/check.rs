use crate::{config::Config, db::Database, process::run_monitored};
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

pub async fn run(config: &Config, db: &Database) -> Vec<CheckItem> {
    let mut out = Vec::new();
    for (name, path, args) in [
        ("pi", &config.ai.pi, vec!["--version"]),
        ("yt-dlp", &config.youtube.yt_dlp, vec!["--version"]),
        ("ffmpeg", &config.render.ffmpeg, vec!["-version"]),
        ("ffprobe", &config.render.ffprobe, vec!["-version"]),
        ("biliup", &config.bilibili.biliup, vec!["--version"]),
    ] {
        let p = Path::new(path);
        if !p.exists() {
            out.push(CheckItem {
                name: name.into(),
                ok: false,
                detail: format!("未找到 {path}"),
            });
            continue;
        }
        let mut c = Command::new(path);
        c.args(args);
        match run_monitored(c, Duration::from_secs(20)).await {
            Ok(r) => out.push(CheckItem {
                name: name.into(),
                ok: true,
                detail: first_line(&(r.stdout + r.stderr.as_str())),
            }),
            Err(e) => out.push(CheckItem {
                name: name.into(),
                ok: false,
                detail: e.to_string(),
            }),
        }
    }
    let mut c = Command::new(&config.render.ffmpeg);
    c.args(["-hide_banner", "-filters"]);
    match run_monitored(c, Duration::from_secs(20)).await {
        Ok(r) => {
            let ok = r.stdout.contains(" ass ") && r.stdout.contains(" subtitles ");
            out.push(CheckItem {
                name: "ffmpeg filters".into(),
                ok,
                detail: if ok {
                    "ass/subtitles 可用".into()
                } else {
                    "缺少 ass 或 subtitles".into()
                },
            })
        }
        Err(e) => out.push(CheckItem {
            name: "ffmpeg filters".into(),
            ok: false,
            detail: e.to_string(),
        }),
    }
    let swap = fs::read_to_string("/proc/swaps")
        .map(|x| x.lines().count() > 1)
        .unwrap_or(false);
    out.push(CheckItem {
        name: "swap".into(),
        ok: swap,
        detail: if swap {
            "已启用".into()
        } else {
            "未启用".into()
        },
    });
    let free = fs2::available_space(&config.runtime.data_dir).unwrap_or(0) / (1024 * 1024 * 1024);
    out.push(CheckItem {
        name: "disk".into(),
        ok: free >= config.storage.stop_free_gib,
        detail: format!("剩余 {free} GiB"),
    });
    let integrity = db.integrity_check().unwrap_or_else(|e| e.to_string());
    out.push(CheckItem {
        name: "database".into(),
        ok: integrity == "ok",
        detail: integrity,
    });
    let schema = db.schema_version().unwrap_or(0);
    out.push(CheckItem {
        name: "database schema".into(),
        ok: schema >= 2,
        detail: format!("v{schema}"),
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
        ("fonts", &config.render.fonts_dir),
    ] {
        out.push(CheckItem {
            name: name.into(),
            ok: p.exists(),
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
                detail: "基线 JSON 无效".into(),
            }),
        }
    }
    let smoke = config.runtime.data_dir.join("checks/ass-smoke.mp4");
    out.push(CheckItem {
        name: "ASS smoke".into(),
        ok: smoke.exists(),
        detail: smoke.display().to_string(),
    });
    for (key, label) in [
        ("auth.youtube", "YouTube auth"),
        ("auth.bilibili", "Bilibili auth"),
    ] {
        if let Ok(Some(value)) = db.get_setting(key) {
            out.push(CheckItem {
                name: label.into(),
                ok: value.starts_with("ok "),
                detail: value,
            });
        }
    }
    out
}

pub async fn write_baseline(config: &Config, dest: &Path) -> Result<Baseline> {
    let mut items = Vec::new();
    for (name, path, args) in [
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
            "biliup",
            PathBuf::from(&config.bilibili.biliup),
            vec!["--version"],
        ),
    ] {
        if !path.exists() {
            continue;
        }
        let mut c = Command::new(&path);
        c.args(args);
        let r = run_monitored(c, Duration::from_secs(20)).await?;
        items.push(BaselineItem {
            name: name.into(),
            path: path.display().to_string(),
            version: first_line(&(r.stdout + r.stderr.as_str())),
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
    for entry in fs::read_dir(&config.render.fonts_dir)
        .into_iter()
        .flatten()
        .flatten()
    {
        let path = entry.path();
        let is_font = path
            .extension()
            .and_then(|x| x.to_str())
            .is_some_and(|x| matches!(x.to_ascii_lowercase().as_str(), "ttf" | "otf" | "ttc"));
        if path.is_file() && is_font {
            items.push(BaselineItem {
                name: format!("font:{}", entry.file_name().to_string_lossy()),
                path: path.display().to_string(),
                version: String::new(),
                sha256: Some(hash_file(&path)?),
            });
        }
    }
    let mut details = std::collections::BTreeMap::new();
    let mut build = Command::new(&config.render.ffmpeg);
    build.arg("-buildconf");
    if let Ok(r) = run_monitored(build, Duration::from_secs(20)).await {
        details.insert("ffmpeg_buildconf".into(), r.stdout + r.stderr.as_str());
    }
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
