mod watch;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

/// 单批闸门处理的候选数。
///
/// 取 50 是为了对齐 `videos.list` 的批量上限：闸门每轮最多发一次 videos.list，
/// 一次 1 个配额单位。取小于 50（此前是 25）会让同样的候选量多花一倍配额。
const GATE_BATCH: usize = 50;
#[cfg(feature = "tui")]
use y2b_rs::tui;
use y2b_rs::{
    Database, check,
    config::{AI_MODEL, AI_PROVIDER, AI_THINKING, AI_TRANSLATION_MODEL, Config},
    cookies,
    db::CURRENT_SCHEMA_VERSION,
    model::{ChannelPriority, JobStatus, TransferMode},
    monitor::Monitor,
    pipeline::Pipeline,
    process::run_monitored,
    websub,
};

#[derive(Parser)]
#[command(
    name = "y2b",
    version,
    about = "自动监控 YouTube，翻译 CC 字幕并上传 Bilibili"
)]
struct Cli {
    #[arg(long, global = true, default_value = "/etc/y2b/config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Cmd,
}
#[derive(Subcommand)]
enum Cmd {
    Init,
    /// 只读取并校验配置，不打开数据库或启动外部进程。
    ConfigCheck,
    /// 输出当前二进制支持的数据库 schema 版本，不读取配置或数据库。
    SchemaVersion,
    /// 显式打开指定数据库并执行全部待应用迁移。
    Migrate {
        #[arg(long, value_name = "PATH")]
        database: PathBuf,
    },
    /// 获取维护锁或查询部署前空闲状态；所有操作都显式指定数据库。
    #[command(subcommand)]
    Maintenance(MaintenanceCmd),
    Check {
        #[arg(long)]
        write_baseline: bool,
    },
    Watch,
    Run {
        url: String,
        #[arg(long, value_enum, default_value_t = TransferMode::Translated)]
        mode: TransferMode,
    },
    #[cfg(feature = "tui")]
    Tui,
    Backup,
    AuthCheck,
    #[command(subcommand)]
    Channels(ChannelCmd),
    #[command(subcommand)]
    Jobs(JobCmd),
    #[command(subcommand)]
    Subtitle(SubtitleCmd),
    #[command(subcommand)]
    Model(ModelCmd),
    #[command(subcommand)]
    Login(LoginCmd),
    #[command(subcommand)]
    Websub(WebSubCmd),
}
#[derive(Subcommand)]
enum MaintenanceCmd {
    /// 原子获取带租约的维护锁。
    Acquire {
        #[arg(long, value_name = "PATH")]
        database: PathBuf,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        reason: String,
        #[arg(long, default_value_t = 300)]
        lease_seconds: i64,
    },
    /// 为当前持有者续租。
    Renew {
        #[arg(long, value_name = "PATH")]
        database: PathBuf,
        #[arg(long)]
        owner: String,
        #[arg(long, default_value_t = 300)]
        lease_seconds: i64,
    },
    /// 释放当前持有者自己的维护锁。
    Release {
        #[arg(long, value_name = "PATH")]
        database: PathBuf,
        #[arg(long)]
        owner: String,
    },
    /// 输出完整空闲状态；`owner` 用于排除调用方自己持有的锁。
    Status {
        #[arg(long, value_name = "PATH")]
        database: PathBuf,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ChannelCmd {
    SetLang {
        id: i64,
        #[arg(conflicts_with = "clear", value_parser = y2b_rs::config::normalize_source_lang)]
        source_lang: Option<String>,
        #[arg(long)]
        clear: bool,
    },
    Add {
        #[arg(long, value_parser = y2b_rs::config::normalize_source_lang)]
        source_lang: Option<String>,
        url: String,
        #[arg(long, value_enum)]
        mode: TransferMode,
    },
    List,
    SetMode {
        id: i64,
        #[arg(value_enum)]
        mode: TransferMode,
    },
    SetPriority {
        id: i64,
        #[arg(value_enum)]
        priority: ChannelPriority,
    },
    Enable {
        id: i64,
    },
    Disable {
        id: i64,
    },
    Sync {
        id: Option<i64>,
    },
}
#[derive(Subcommand)]
enum JobCmd {
    Add {
        url: String,
        #[arg(long, value_enum)]
        mode: TransferMode,
    },
    List {
        #[arg(default_value_t = 20)]
        limit: usize,
    },
    Show {
        id: String,
    },
    Retry {
        id: String,
    },
    /// 从 Bilibili 创作中心核对 upload_uncertain 任务；只接受唯一同名稿件。
    ReconcileUpload {
        id: String,
        /// 人工判定这次投稿从未落地：结算 attempt 并把任务退回重投。
        /// 仍会先查创作中心，存在同名稿件时拒绝执行。
        #[arg(long)]
        not_published: bool,
    },
}
#[derive(Subcommand)]
enum SubtitleCmd {
    /// 给指定 BVID 的已投稿视频补中文 CC 字幕
    Add { bvid: String },
    /// 给所有已投稿视频补中文 CC 字幕（已有中文字幕的自动跳过）
    All,
}
#[derive(Subcommand)]
enum ModelCmd {
    List,
    // 旧脚本用此命令确认固定模型。
    #[command(hide = true)]
    Set {
        model: String,
        #[arg(long)]
        provider: Option<String>,
    },
}
#[derive(Subcommand)]
enum LoginCmd {
    Bilibili,
    Youtube { cookies_file: PathBuf },
}

#[derive(Subcommand)]
enum WebSubCmd {
    /// 列出每个频道的订阅、租约和最近推送状态。
    Status,
    /// 手动强制向 hub 提交订阅请求，供首次启用和排障使用。
    Subscribe {
        #[arg(long, conflicts_with = "channel", required_unless_present = "channel")]
        all: bool,
        /// y2b 内部数字 id 或 YouTube UC... channel id。
        #[arg(long, value_name = "ID", conflicts_with = "all")]
        channel: Option<String>,
    },
}

fn run_maintenance_command(command: &MaintenanceCmd) -> Result<()> {
    match command {
        MaintenanceCmd::Acquire {
            database,
            owner,
            reason,
            lease_seconds,
        } => {
            let db = Database::open_existing(database)?;
            if !db.acquire_maintenance_hold(owner, reason, *lease_seconds)? {
                let holder = db
                    .maintenance_hold()?
                    .map(|hold| format!("{}（到期 {}）", hold.owner, hold.expires_at))
                    .unwrap_or_else(|| "未知持有者".into());
                anyhow::bail!("维护锁获取失败，当前持有者: {holder}")
            }
            println!("维护锁已获取: owner={owner} lease_seconds={lease_seconds}");
        }
        MaintenanceCmd::Renew {
            database,
            owner,
            lease_seconds,
        } => {
            let db = Database::open_existing(database)?;
            anyhow::ensure!(
                db.renew_maintenance_hold(owner, *lease_seconds)?,
                "维护锁续租失败：owner 不匹配或租约已经到期"
            );
            println!("维护锁已续租: owner={owner} lease_seconds={lease_seconds}");
        }
        MaintenanceCmd::Release { database, owner } => {
            let db = Database::open_existing(database)?;
            anyhow::ensure!(
                db.release_maintenance_hold(owner)?,
                "维护锁释放失败：owner 不匹配"
            );
            println!("维护锁已释放: owner={owner}");
        }
        MaintenanceCmd::Status {
            database,
            owner,
            json,
        } => {
            let db = Database::open_existing(database)?;
            let status = db.maintenance_status(owner.as_deref())?;
            if *json {
                println!("{}", serde_json::to_string(&status)?);
            } else {
                println!("idle={}", status.idle);
                if let Some(hold) = &status.hold {
                    println!(
                        "hold\towner={}\texpires_at={}\treason={}",
                        hold.owner, hold.expires_at, hold.reason
                    );
                }
                for blocker in &status.blockers {
                    println!(
                        "{}\t{}\t{}",
                        blocker.kind,
                        blocker.count,
                        blocker.details.join(",")
                    );
                }
            }
        }
    }
    Ok(())
}

async fn run_subtitle_all(pipeline: &Pipeline) -> Result<()> {
    let jobs = pipeline.db.jobs_awaiting_subtitle()?;
    if jobs.is_empty() {
        println!("没有待补字幕的已投稿视频");
        return Ok(());
    }
    let mut failed = 0;
    let mut skipped = 0;
    let mut submitted = 0;
    for job in jobs {
        let bvid = job.bvid.as_deref().unwrap_or_default();
        match pipeline.backfill_cc_subtitle(bvid).await {
            Ok(message) => {
                println!("{message}");
                if message.contains("跳过") {
                    skipped += 1;
                } else {
                    submitted += 1;
                }
            }
            Err(error) => {
                failed += 1;
                eprintln!("{bvid} 补字幕失败: {error:#}");
            }
        }
    }
    let summary = format!("完成: 提交 {submitted}，跳过 {skipped}，失败 {failed}");
    println!("{summary}");
    if failed > 0 {
        anyhow::bail!("{summary}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("y2b_rs=info".parse()?),
        )
        .init();
    let cli = Cli::parse();
    match &cli.command {
        Cmd::SchemaVersion => {
            println!("{CURRENT_SCHEMA_VERSION}");
            return Ok(());
        }
        Cmd::Migrate { database } => {
            println!("{}", Database::open(database)?.schema_version()?);
            return Ok(());
        }
        Cmd::Maintenance(command) => {
            run_maintenance_command(command)?;
            return Ok(());
        }
        _ => {}
    }
    let is_init = matches!(&cli.command, Cmd::Init);
    let config = if is_init {
        Config::load_or_default(&cli.config)?
    } else {
        Config::load(&cli.config)?
    };
    if matches!(cli.command, Cmd::ConfigCheck) {
        println!(
            "AI profile: {AI_PROVIDER}/{AI_MODEL} translate={AI_TRANSLATION_MODEL} thinking={AI_THINKING}"
        );
        return Ok(());
    }
    if let Cmd::Model(command) = &cli.command {
        match command {
            ModelCmd::List => {
                println!(
                    "{}/{}\ttranslate={}\tthinking={}",
                    config.ai.provider,
                    config.ai.model,
                    config.ai.translation_model,
                    config.ai.thinking
                );
            }
            ModelCmd::Set { model, provider } => {
                anyhow::ensure!(
                    provider.as_deref().unwrap_or(AI_PROVIDER) == AI_PROVIDER && model == AI_MODEL,
                    "AI profile 已固定为 {AI_PROVIDER}/{AI_MODEL} thinking={AI_THINKING}"
                );
                println!("AI profile 已确认");
            }
        }
        return Ok(());
    }
    if is_init {
        config.ensure_dirs()?;
        if !cli.config.exists() {
            config.save(&cli.config)?;
        }
        println!("已初始化 {}", cli.config.display());
        return Ok(());
    }
    config.ensure_dirs()?;
    let db = Database::open(&config.runtime.database)?;
    match cli.command {
        Cmd::Init => unreachable!(),
        Cmd::ConfigCheck | Cmd::SchemaVersion | Cmd::Migrate { .. } | Cmd::Maintenance(_) => {
            unreachable!()
        }
        Cmd::Check { write_baseline } => {
            let checks = check::run(&config, &db).await;
            finish_check(&config, &checks, write_baseline).await?;
        }
        Cmd::Watch => watch::watch(&cli.config, config, db).await?,
        Cmd::Run { url, mode } => {
            let monitor = Monitor::new(config.clone(), db.clone())?;
            let outcome = monitor.enqueue_video(&url, mode).await?;
            if !outcome.created {
                anyhow::bail!(
                    "视频已存在: job={} status={} mode={}",
                    outcome.job.id,
                    outcome.job.status,
                    outcome.job.transfer_mode
                )
            }
            Pipeline::new(config, db).run_job(outcome.job).await?;
        }
        #[cfg(feature = "tui")]
        Cmd::Tui => tui::run(&cli.config, config, db)?,
        Cmd::Backup => {
            println!("backup: {}", backup(&config, &db)?.display());
        }
        Cmd::AuthCheck => {
            check_auth(&config, &db).await?;
            println!(
                "YouTube: {}",
                db.get_setting("auth.youtube")?
                    .unwrap_or_else(|| "未检查".into())
            );
            println!(
                "Bilibili: {}",
                db.get_setting("auth.bilibili")?
                    .unwrap_or_else(|| "未检查".into())
            );
        }
        Cmd::Channels(c) => match c {
            ChannelCmd::Add {
                url,
                mode,
                source_lang,
            } => {
                let id = Monitor::new(config, db)?
                    .add_channel(&url, mode, source_lang.as_deref())
                    .await?;
                println!("已添加频道 id={id} mode={mode}，当前视频已作为基线");
            }
            ChannelCmd::SetLang {
                id,
                source_lang,
                clear,
            } => {
                if clear || source_lang.is_some() {
                    db.set_channel_language(id, source_lang.as_deref())?;
                }
                let channel = db.channel(id)?;
                println!(
                    "频道 {id} source_lang={}，生效语言={}",
                    channel.source_lang.as_deref().unwrap_or("NULL"),
                    channel
                        .source_lang
                        .as_deref()
                        .unwrap_or(&config.translation.source_lang)
                );
            }
            ChannelCmd::List => {
                for c in db.list_channels()? {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        c.id,
                        if c.enabled { "on" } else { "off" },
                        c.transfer_mode,
                        c.priority,
                        c.name,
                        c.youtube_channel_id,
                        c.source_lang.as_deref().unwrap_or("-")
                    )
                }
            }
            ChannelCmd::SetMode { id, mode } => {
                db.set_channel_transfer_mode(id, mode)?;
                println!("频道 {id} 的新任务模式已更新为 {mode}");
            }
            ChannelCmd::SetPriority { id, priority } => {
                db.set_channel_priority(id, priority)?;
                println!("频道 {id} 的优先级已更新为 {priority}");
            }
            ChannelCmd::Enable { id } => db.set_channel_enabled(id, true)?,
            ChannelCmd::Disable { id } => db.set_channel_enabled(id, false)?,
            ChannelCmd::Sync { id } => {
                // 手动 sync 的语义是「立刻同步一次」，所以要走和 discovery_loop
                // 相同的源选择（Data API 优先），并把目标频道的调度提前到现在——
                // 否则 next_data_api_poll_at / next_poll_at 未到期时这条命令会静默
                // 什么都不做，排障时完全看不出发生了什么。指定单个频道时只同步该
                // 频道，不顺带触发其他频道。发现之后再跑一次闸门，让候选真正变成任务，
                // 而不是停在 video_candidates 里。
                let m = Monitor::new(config, db.clone())?;
                let now = Utc::now();
                let discovered = if m.data_api_primary_enabled()? {
                    if let Some(id) = id {
                        m.poll_data_api_channel(id).await?
                    } else {
                        for channel in db.list_channels()?.into_iter().filter(|c| c.enabled) {
                            db.schedule_data_api_poll(channel.id, now, None)?;
                        }
                        m.poll_data_api().await?
                    }
                } else if let Some(id) = id {
                    m.poll_channel(id, true).await?
                } else {
                    for channel in db.list_channels()?.into_iter().filter(|c| c.enabled) {
                        db.defer_channel_poll_until(channel.id, now)?;
                    }
                    m.poll_all().await?
                };
                let mut promoted = 0;
                loop {
                    let outcome = match id {
                        Some(channel_id) => {
                            m.gate_pending_candidates_for_channel(channel_id, GATE_BATCH)
                                .await?
                        }
                        None => m.gate_pending_candidates(GATE_BATCH).await?,
                    };
                    promoted += outcome.promoted;
                    // 按 processed 判断是否还有活，不能看 promoted——整批被拒时
                    // promoted 是 0，但候选确实处理了。
                    if outcome.processed == 0 {
                        break;
                    }
                }
                println!("发现 {discovered} 条候选，入队 {promoted} 条");
            }
        },
        Cmd::Jobs(c) => match c {
            JobCmd::Add { url, mode } => {
                let outcome = Monitor::new(config, db)?.enqueue_video(&url, mode).await?;
                if outcome.created {
                    println!(
                        "已加入队列 {}\t{}\t{}",
                        outcome.job.id, outcome.job.video_id, outcome.job.transfer_mode
                    );
                } else {
                    println!(
                        "视频已存在 {}\t{}\t{}\t{}",
                        outcome.job.id,
                        outcome.job.video_id,
                        outcome.job.status,
                        outcome.job.transfer_mode
                    );
                }
            }
            JobCmd::List { limit } => {
                for j in db.list_jobs(limit)? {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}",
                        j.id,
                        j.status,
                        j.transfer_mode,
                        j.video_id,
                        j.bvid.unwrap_or_default(),
                        j.title.unwrap_or_default()
                    )
                }
            }
            JobCmd::Show { id } => {
                println!("{}", serde_json::to_string_pretty(&db.get_job(&id)?)?);
                println!("{}", serde_json::to_string_pretty(&db.list_stages(&id)?)?)
            }
            JobCmd::Retry { id } => {
                let job = db
                    .get_job(&id)?
                    .with_context(|| format!("任务不存在: {id}"))?;
                if job.status == JobStatus::UploadedOriginalPendingSubtitle {
                    // 已投稿的任务不能重跑整条流水线（会重复投稿），
                    // 重试对它的含义是重新武装 CC 字幕队列。
                    db.rearm_pending_subtitle(&id)?;
                    println!("已重新排队 CC 字幕补交 {id}");
                    return Ok(());
                }
                db.retry_job(&id)?;
                println!("已重新排队 {id}");
            }
            JobCmd::ReconcileUpload { id, not_published } => {
                let pipeline = Pipeline::new(config, db);
                let message = if not_published {
                    pipeline.declare_uncertain_upload_not_published(&id).await?
                } else {
                    pipeline.reconcile_uncertain_upload(&id).await?
                };
                println!("{message}");
            }
        },
        Cmd::Subtitle(c) => match c {
            SubtitleCmd::Add { bvid } => {
                let message = Pipeline::new(config, db)
                    .backfill_cc_subtitle(&bvid)
                    .await?;
                println!("{message}");
            }
            SubtitleCmd::All => {
                let pipeline = Pipeline::new(config, db);
                run_subtitle_all(&pipeline).await?;
            }
        },
        Cmd::Model(_) => unreachable!("模型命令在创建目录和打开数据库前返回"),
        Cmd::Login(c) => match c {
            LoginCmd::Youtube { cookies_file } => {
                cookies::import_cookie(&cookies_file, &config.youtube.cookies)?;
                println!("已导入 {}", config.youtube.cookies.display());
            }
            LoginCmd::Bilibili => {
                let mut c = Command::new(&config.bilibili.biliup);
                c.arg("-u").arg(&config.bilibili.cookies).arg("login");
                run_monitored(c, Duration::from_secs(600)).await?;
            }
        },
        Cmd::Websub(c) => match c {
            WebSubCmd::Status => {
                println!(
                    "WebSub\t{}\t{}",
                    if config.websub.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    if config.websub.callback_base_url.is_empty() {
                        "-"
                    } else {
                        &config.websub.callback_base_url
                    }
                );
                println!(
                    "id\tyoutube_channel_id\tname\tstatus\tlease_expires_at\tlast_received_at"
                );
                let now = chrono::Utc::now();
                for channel in db.list_websub_channels()? {
                    let status = if !channel.enabled {
                        "channel_disabled"
                    } else {
                        match channel.lease_expires_at {
                            Some(expires_at) if expires_at > now => "active",
                            Some(_) => "expired",
                            None if channel.callback_path.is_some() => "pending_verification",
                            None => "not_subscribed",
                        }
                    };
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}",
                        channel.id,
                        channel.youtube_channel_id,
                        channel.name,
                        status,
                        channel
                            .lease_expires_at
                            .map(|value| value.to_rfc3339())
                            .unwrap_or_else(|| "-".into()),
                        channel
                            .last_received_at
                            .map(|value| value.to_rfc3339())
                            .unwrap_or_else(|| "-".into())
                    );
                }
            }
            WebSubCmd::Subscribe { all, channel } => {
                anyhow::ensure!(
                    config.websub.enabled,
                    "请先在配置中设置 [websub] enabled = true 和公网 callback_base_url"
                );
                let service = websub::WebSubService::new(config.websub.clone(), db)?;
                if all {
                    let accepted = service.subscribe_all().await?;
                    println!("已提交 {accepted} 个 WebSub 订阅请求，等待异步验证");
                } else {
                    let identifier = channel.context("缺少 --all 或 --channel <id>")?;
                    service.subscribe_identifier(&identifier).await?;
                    println!("频道 {identifier} 的 WebSub 订阅请求已提交，等待异步验证");
                }
            }
        },
    }
    Ok(())
}

async fn finish_check(
    config: &Config,
    checks: &[check::CheckItem],
    write_baseline: bool,
) -> Result<()> {
    for item in checks {
        let status = if item.ok {
            "OK"
        } else if item.required {
            "FAIL"
        } else {
            "WARN"
        };
        println!("{status} {:<20} {}", item.name, item.detail);
    }
    let failed = checks
        .iter()
        .filter(|item| item.required && !item.ok)
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>();
    if !failed.is_empty() {
        anyhow::bail!("必选检查失败: {}", failed.join(", "))
    }
    if write_baseline {
        let path = config.runtime.data_dir.join("dependency-baseline.json");
        check::write_baseline(config, &path, checks).await?;
        println!("baseline: {}", path.display());
    }
    Ok(())
}

async fn check_auth(config: &Config, db: &Database) -> Result<()> {
    let youtube = match Monitor::new(config.clone(), db.clone()) {
        Ok(monitor) => monitor
            .fetch_metadata(&config.youtube.probe_url)
            .await
            .map(|_| ()),
        Err(error) => Err(error),
    };
    let mut cmd = Command::new(&config.bilibili.biliup);
    cmd.arg("-u").arg(&config.bilibili.cookies).arg("renew");
    let bilibili = run_monitored(cmd, Duration::from_secs(180))
        .await
        .map(|_| ());
    record_auth_results(db, youtube, bilibili)
}

fn record_auth_results(db: &Database, youtube: Result<()>, bilibili: Result<()>) -> Result<()> {
    let checked_at = chrono::Utc::now().to_rfc3339();
    let youtube_status = match &youtube {
        Ok(()) => format!("ok {checked_at}"),
        Err(error) => format!("failed: {error:#}"),
    };
    let bilibili_status = match &bilibili {
        Ok(()) => format!("ok {checked_at}"),
        Err(error) => format!("failed: {error:#}"),
    };
    let youtube_write = db.set_setting("auth.youtube", &youtube_status);
    let bilibili_write = db.set_setting("auth.bilibili", &bilibili_status);
    youtube_write?;
    bilibili_write?;

    let mut failed = Vec::new();
    if let Err(error) = youtube {
        failed.push(format!("YouTube: {error:#}"));
    }
    if let Err(error) = bilibili {
        failed.push(format!("Bilibili: {error:#}"));
    }
    if !failed.is_empty() {
        anyhow::bail!("认证检查失败: {}", failed.join("; "))
    }
    Ok(())
}

/// 执行分层备份，返回本次写入的小时级备份路径。
///
/// 由调用方决定怎么汇报：CLI 打到 stdout，watch 走 tracing。
fn backup(config: &Config, db: &Database) -> Result<PathBuf> {
    let now = chrono::Utc::now();
    let hourly = config.runtime.backup_dir.join("hourly");
    let daily = config.runtime.backup_dir.join("daily");
    let weekly = config.runtime.backup_dir.join("weekly");
    let dest = hourly.join(format!("state-{}.db", now.format("%Y%m%d-%H%M%S")));
    db.backup(&dest)?;
    prune(&hourly, 4)?;
    let day = daily.join(format!("state-{}.db", now.format("%Y%m%d")));
    if !day.exists() {
        db.backup(&day)?;
    }
    prune(&daily, config.storage.daily_backups)?;
    let week = weekly.join(format!(
        "state-{}-W{}.db",
        now.format("%Y"),
        now.format("%V")
    ));
    if !week.exists() {
        db.backup(&week)?;
    }
    prune(&weekly, config.storage.weekly_backups)?;
    Ok(dest)
}
fn is_database_backup_name(name: &std::ffi::OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(stamp) = name
        .strip_prefix("state-")
        .and_then(|name| name.strip_suffix(".db"))
    else {
        return false;
    };
    let bytes = stamp.as_bytes();
    match bytes.len() {
        // 每日备份：state-YYYYMMDD.db；每周备份：state-YYYY-WNN.db。
        8 => {
            bytes.iter().all(u8::is_ascii_digit)
                || (bytes[0..4].iter().all(u8::is_ascii_digit)
                    && &bytes[4..6] == b"-W"
                    && bytes[6..8].iter().all(u8::is_ascii_digit))
        }
        // 小时备份：state-YYYYMMDD-HHMMSS.db。
        15 => {
            bytes[0..8].iter().all(u8::is_ascii_digit)
                && bytes[8] == b'-'
                && bytes[9..15].iter().all(u8::is_ascii_digit)
        }
        _ => false,
    }
}

fn prune(dir: &std::path::Path, keep: usize) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let mut files = std::fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            (entry.file_type().ok()?.is_file() && is_database_backup_name(&entry.file_name()))
                .then_some(entry)
        })
        .collect::<Vec<_>>();
    files.sort_by_key(|entry| entry.file_name());
    let expired = files.len().saturating_sub(keep);
    for entry in files.into_iter().take(expired) {
        std::fs::remove_file(entry.path())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[tokio::test]
    async fn required_check_failure_returns_error_without_overwriting_baseline() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.runtime.data_dir = temp.path().to_path_buf();
        let baseline = temp.path().join("dependency-baseline.json");
        fs::write(&baseline, "原基线").unwrap();
        let checks = vec![
            check::CheckItem {
                name: "pi".into(),
                ok: false,
                required: true,
                detail: "未找到 pi".into(),
            },
            check::CheckItem {
                name: "swap".into(),
                ok: false,
                required: false,
                detail: "未启用".into(),
            },
        ];

        let error = finish_check(&config, &checks, true).await.unwrap_err();
        assert!(error.to_string().contains("必选检查失败: pi"));
        assert_eq!(fs::read_to_string(baseline).unwrap(), "原基线");
    }

    #[tokio::test]
    async fn subtitle_all_returns_error_when_any_item_fails() {
        let temp = tempfile::tempdir().unwrap();
        let db = Database::open(&temp.path().join("state.db")).unwrap();
        let id = db
            .create_job(y2b_rs::NewJob {
                channel_id: None,
                video_id: "subtitle-fail",
                url: "https://youtu.be/subtitle-fail",
                title: None,
                published: None,
                updated: None,
                transfer_mode: TransferMode::Translated,
            })
            .unwrap()
            .unwrap();
        db.update_job_status(&id, JobStatus::Completed, None)
            .unwrap();
        db.set_job_bvid(&id, "BV1subtitlefail").unwrap();
        // 活跃维护锁会让 claim_subtitle_job_now 立即返回 None，使 backfill 在
        // 触网前失败，从而离线验证批量命令的非零退出行为。
        assert!(
            db.acquire_maintenance_hold("tester", "测试维护", 300)
                .unwrap()
        );
        let mut config = Config::default();
        config.runtime.data_dir = temp.path().to_path_buf();
        let pipeline = Pipeline::new(config, db);

        let error = run_subtitle_all(&pipeline).await.unwrap_err();
        assert!(error.to_string().contains("失败 1"));
    }

    #[test]
    fn auth_failure_records_both_results_before_returning_error() {
        for youtube_fails in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let db = Database::open(&temp.path().join("state.db")).unwrap();
            let youtube = if youtube_fails {
                Err(anyhow::anyhow!("YouTube cookie 失效"))
            } else {
                Ok(())
            };
            let bilibili = if youtube_fails {
                Ok(())
            } else {
                Err(anyhow::anyhow!("Bilibili cookie 失效"))
            };

            let error = record_auth_results(&db, youtube, bilibili).unwrap_err();
            assert!(error.to_string().contains("认证检查失败"));
            let youtube_status = db.get_setting("auth.youtube").unwrap().unwrap();
            let bilibili_status = db.get_setting("auth.bilibili").unwrap().unwrap();
            assert_eq!(youtube_status.starts_with("failed:"), youtube_fails);
            assert_eq!(bilibili_status.starts_with("failed:"), !youtube_fails);
        }
    }

    #[test]
    fn channel_language_commands_parse() {
        for args in [
            vec!["add", "url", "--mode", "translated", "--source-lang", "ja"],
            vec!["set-lang", "1", "de-DE"],
            vec!["set-lang", "1", "--clear"],
            vec!["set-lang", "1"],
        ] {
            Cli::try_parse_from(["y2b", "channels"].into_iter().chain(args)).unwrap();
        }
        assert!(
            Cli::try_parse_from(["y2b", "channels", "set-lang", "1", "ja", "--clear"]).is_err()
        );
    }
    #[test]
    fn maintenance_status_command_uses_an_explicit_database_and_json_output() {
        let cli = Cli::try_parse_from([
            "y2b",
            "maintenance",
            "status",
            "--database",
            "/tmp/state.db",
            "--owner",
            "deploy:test",
            "--json",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Cmd::Maintenance(MaintenanceCmd::Status {
                database,
                owner: Some(owner),
                json: true,
            }) if database == Path::new("/tmp/state.db") && owner == "deploy:test"
        ));
    }

    #[test]
    fn schema_version_command_and_restore_default_follow_the_rust_constant() {
        let cli = Cli::try_parse_from(["y2b", "schema-version"]).unwrap();
        assert!(matches!(cli.command, Cmd::SchemaVersion));

        let restore =
            fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/restore.sh"))
                .unwrap();
        assert!(restore.contains("\"$y2b_cmd\" schema-version"));
        assert!(!restore.contains("Y2B_SCHEMA_VERSION:-19"));
    }

    use std::os::unix::fs::symlink;

    #[test]
    fn prune_ignores_directories_symlinks_and_unrecognized_files() {
        let temporary = tempfile::tempdir().unwrap();
        let backups = temporary.path().join("hourly");
        std::fs::create_dir(&backups).unwrap();
        let old = backups.join("state-20260830-010000.db");
        let new = backups.join("state-20260830-020000.db");
        let manual = backups.join("state.db.before-restore.20260830");
        let directory = backups.join("state-20260829-000000.db");
        let target = temporary.path().join("symlink-target.db");
        let link = backups.join("state-20260828-000000.db");
        std::fs::write(&old, "old").unwrap();
        std::fs::write(&new, "new").unwrap();
        std::fs::write(&manual, "manual").unwrap();
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(&target, "target").unwrap();
        symlink(&target, &link).unwrap();

        prune(&backups, 1).unwrap();

        assert!(!old.exists());
        assert!(new.exists());
        assert!(manual.exists());
        assert!(directory.is_dir());
        assert!(link.is_symlink());
        assert_eq!(std::fs::read_to_string(target).unwrap(), "target");
    }

    #[test]
    fn backup_name_filter_accepts_only_the_three_generated_shapes() {
        for name in [
            "state-20260830-010203.db",
            "state-20260830.db",
            "state-2026-W35.db",
        ] {
            assert!(is_database_backup_name(std::ffi::OsStr::new(name)));
        }
        for name in [
            "state.db",
            "state.db.before-restore.20260830",
            "state-20260830-010203.db.tmp",
            "state-2026-W.db",
        ] {
            assert!(!is_database_backup_name(std::ffi::OsStr::new(name)));
        }
    }
}
