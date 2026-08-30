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
    pipeline::{self, AiCircuitBreaker, Pipeline},
    process::run_monitored,
    websub,
};

#[derive(Parser)]
#[command(
    name = "y2b",
    version,
    about = "自动监控 YouTube，翻译压制并上传 Bilibili"
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
    Add {
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

fn current_schema_version() -> i64 {
    CURRENT_SCHEMA_VERSION
}

fn migrate_database(path: &std::path::Path) -> Result<i64> {
    let db = Database::open(path)?;
    db.schema_version()
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
            println!("{}", current_schema_version());
            return Ok(());
        }
        Cmd::Migrate { database } => {
            println!("{}", migrate_database(database)?);
            return Ok(());
        }
        Cmd::Maintenance(command) => {
            run_maintenance_command(command)?;
            return Ok(());
        }
        _ => {}
    }
    let is_init = matches!(&cli.command, Cmd::Init);
    let mut config = if is_init {
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
    if is_init {
        config.ensure_dirs()?;
        config.save(&cli.config)?;
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
        Cmd::Watch => watch(cli.config.clone(), config, db).await?,
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
            ChannelCmd::Add { url, mode } => {
                let id = Monitor::new(config, db)?.add_channel(&url, mode).await?;
                println!("已添加频道 id={id} mode={mode}，当前视频已作为基线");
            }
            ChannelCmd::List => {
                for c in db.list_channels()? {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}",
                        c.id,
                        if c.enabled { "on" } else { "off" },
                        c.transfer_mode,
                        c.priority,
                        c.name,
                        c.youtube_channel_id
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
                // 否则 next_data_api_poll_at 未到期时这条命令会静默什么都不做，
                // 排障时完全看不出发生了什么。发现之后再跑一次闸门，让候选真正
                // 变成任务，而不是停在 video_candidates 里。
                let m = Monitor::new(config, db.clone())?;
                let now = Utc::now();
                let targets: Vec<i64> = match id {
                    Some(id) => vec![id],
                    None => db
                        .list_channels()?
                        .into_iter()
                        .filter(|channel| channel.enabled)
                        .map(|channel| channel.id)
                        .collect(),
                };
                for channel_id in &targets {
                    db.schedule_data_api_poll(*channel_id, now, None)?;
                }
                let discovered = if m.data_api_primary_enabled()? {
                    m.poll_data_api().await?
                } else if let Some(id) = id {
                    m.poll_channel(id, true).await?
                } else {
                    m.poll_all().await?
                };
                let mut promoted = 0;
                loop {
                    let outcome = m.gate_pending_candidates(GATE_BATCH).await?;
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
            JobCmd::ReconcileUpload { id } => {
                let message = Pipeline::new(config, db)
                    .reconcile_uncertain_upload(&id)
                    .await?;
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
        Cmd::Model(c) => match c {
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
                let provider = provider.unwrap_or_else(|| AI_PROVIDER.into());
                anyhow::ensure!(
                    provider == AI_PROVIDER && model == AI_MODEL,
                    "AI profile 已固定为 {AI_PROVIDER}/{AI_MODEL} thinking={AI_THINKING}"
                );
                config.ai.provider = provider;
                config.ai.model = model;
                config.ai.thinking = AI_THINKING.into();
                config.save(&cli.config)?;
                println!("AI profile 已确认");
            }
        },
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

/// `watch` 的发现、闸门、维护和队列调度循环彼此独立。
///
/// 此前发现（RSS 轮询、yt-dlp 校对）、维护（备份、认证）和队列调度共用一个
/// `select!`：`select!` 选中某个分支后，该分支 `await` 期间其他分支不会被轮询，
/// 于是一次几分钟的 `poll_all`（每条新条目都要跑一次 yt-dlp）会让调度分支完全
/// 停摆，准备/上传 worker 拉不起来。拆开后调度循环只做轻量 DB 查询，可以稳定
/// 保持 1 秒节奏。
type CriticalTaskResult = (&'static str, Result<()>);

async fn watch(config_path: PathBuf, config: Config, db: Database) -> Result<()> {
    let recovered = db.recover_incomplete_jobs()?;
    if recovered > 0 {
        tracing::warn!(count = recovered, "已恢复服务重启前的未完成任务");
    }
    let monitor = Monitor::new(config.clone(), db.clone())?;
    let mut tasks = tokio::task::JoinSet::<CriticalTaskResult>::new();
    tasks.spawn(async move {
        discovery_loop(
            monitor,
            config.monitor.poll_seconds,
            config.monitor.reconcile_hours,
        )
        .await;
        ("discovery", Ok(()))
    });
    tasks.spawn({
        let monitor = Monitor::new(config.clone(), db.clone())?;
        async move {
            priority_discovery_loop(monitor).await;
            ("priority discovery", Ok(()))
        }
    });
    tasks.spawn({
        let monitor = Monitor::new(config.clone(), db.clone())?;
        async move {
            gate_loop(monitor).await;
            ("gate", Ok(()))
        }
    });
    tasks.spawn({
        let maintenance_config = config.clone();
        let maintenance_db = db.clone();
        async move {
            maintenance_loop(maintenance_config, maintenance_db).await;
            ("maintenance", Ok(()))
        }
    });
    if config.websub.enabled {
        let websub_config = config.websub.clone();
        let websub_db = db.clone();
        tasks.spawn(async move { ("WebSub", websub::run(websub_config, websub_db).await) });
    }
    supervise_watch(tasks, schedule_loop(&config_path, &config, &db)).await
}

/// scheduler 在前台接收 Ctrl-C；其余关键循环进入 JoinSet。任何循环先结束（包括
/// panic）都会让 watch 返回错误，避免 systemd 眼中的“服务仍存活但功能已缺失”。
async fn supervise_watch(
    mut tasks: tokio::task::JoinSet<CriticalTaskResult>,
    scheduler: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    tokio::pin!(scheduler);
    let result = tokio::select! {
        result = &mut scheduler => result.context("关键后台任务 scheduler 失败"),
        result = tasks.join_next() => critical_task_result(result),
    };
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}

fn critical_task_result(
    result: Option<std::result::Result<CriticalTaskResult, tokio::task::JoinError>>,
) -> Result<()> {
    match result {
        Some(Ok((name, Ok(())))) => anyhow::bail!("关键后台任务 {name} 意外退出"),
        Some(Ok((name, Err(error)))) => {
            Err(error).with_context(|| format!("关键后台任务 {name} 失败"))
        }
        Some(Err(error)) => anyhow::bail!("关键后台任务异常结束: {error}"),
        None => anyhow::bail!("关键后台任务集合意外为空"),
    }
}

/// 候选闸门独立运行，逐条拉元数据不会再阻塞 RSS/Data API 等轻量发现源。
async fn gate_loop(monitor: Monitor) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        match monitor.gate_pending_candidates(GATE_BATCH).await {
            Ok(outcome) if outcome.promoted > 0 => {
                tracing::info!(
                    promoted = outcome.promoted,
                    processed = outcome.processed,
                    "候选闸门晋级完成"
                )
            }
            Ok(_) => {}
            Err(error) => tracing::error!(error = %error, "候选闸门处理失败"),
        }
    }
}

/// 每秒推进准备/上传队列，并在 Ctrl-C 时收尾。
async fn schedule_loop(
    config_path: &std::path::Path,
    config: &Config,
    db: &Database,
) -> Result<()> {
    let mut scheduler_tick = tokio::time::interval(Duration::from_secs(1));
    scheduler_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut prepare_worker: Option<tokio::task::JoinHandle<()>> = None;
    let mut upload_worker: Option<tokio::task::JoinHandle<()>> = None;
    let mut subtitle_worker: Option<tokio::task::JoinHandle<()>> = None;
    let ai_circuit_breaker = AiCircuitBreaker::default();
    let outcome = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break Ok(()),
            _ = scheduler_tick.tick() => {
                reap_worker(&mut prepare_worker, "准备工作线程").await;
                reap_worker(&mut upload_worker, "上传工作线程").await;
                reap_worker(&mut subtitle_worker, "字幕工作线程").await;
                if prepare_worker.is_none()
                    && !ai_circuit_breaker.is_open()
                    && let Some(job) = db.claim_next_prepare_job()?
                {
                    let fresh = reload_config(config_path, config);
                    let worker_db = db.clone();
                    let worker_breaker = ai_circuit_breaker.clone();
                    prepare_worker = Some(tokio::spawn(async move {
                        if let Err(e) = Pipeline::with_ai_circuit_breaker(
                            fresh,
                            worker_db,
                            worker_breaker,
                        )
                        .prepare_job(job)
                        .await
                        {
                            tracing::error!(error = %e, "任务准备失败");
                        }
                    }));
                }
                if upload_worker.is_none()
                    && db.bilibili_submission_due(Utc::now())?
                    && let Some(job) = db.next_ready_to_upload_job()?
                {
                    let fresh = reload_config(config_path, config);
                    let worker_db = db.clone();
                    upload_worker = Some(tokio::spawn(async move {
                        if let Err(e) = Pipeline::new(fresh, worker_db).upload_prepared_job(job).await {
                            tracing::error!(error = %e, "待上传任务失败");
                        }
                    }));
                }
                // CC 字幕补交独立于上传：失败已由 Pipeline 记录并安排退避重试，
                // 这里只负责不让它占住上传 worker。
                if subtitle_worker.is_none()
                    && let Some(job) = db.claim_next_pending_subtitle_job(pipeline::CC_MAX_ATTEMPTS)?
                {
                    let fresh = reload_config(config_path, config);
                    let worker_db = db.clone();
                    subtitle_worker = Some(tokio::spawn(async move {
                        let _ = Pipeline::new(fresh, worker_db)
                            .submit_pending_subtitle(job)
                            .await;
                    }));
                }
            }
        }
    };
    for worker in [prepare_worker, upload_worker, subtitle_worker]
        .into_iter()
        .flatten()
    {
        worker.abort();
        let _ = worker.await;
    }
    outcome
}

/// 优先频道拥有独立的 RSS 发现循环，不和普通频道共享每轮名额，也不运行
/// yt-dlp 回退。每秒检查持久化到期时间，真正的网络请求仍由频道的
/// `next_poll_at` 按 `monitor.poll_seconds` 限速，避免请求耗时导致错过整轮节拍。
async fn priority_discovery_loop(monitor: Monitor) {
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        poll.tick().await;
        match monitor.poll_priority_rss().await {
            Ok(discovered) if discovered > 0 => {
                tracing::info!(discovered, "优先频道 RSS 发现新候选")
            }
            Ok(_) => {}
            Err(error) => tracing::error!(error = %error, "优先频道 RSS 轮询失败"),
        }
    }
}

/// Data API 预测主发现、RSS 探针与每日深扫。
///
/// 两者共用一个任务而不是各占一个：它们都会拉起 yt-dlp，并发执行会在 2 GiB
/// 服务器上叠加内存压力。串行执行保持了原有的资源占用特征，同时不再阻塞调度。
async fn discovery_loop(monitor: Monitor, poll_seconds: u64, reconcile_hours: u64) {
    let data_api_enabled = monitor.has_data_api();
    // 每秒只做一次轻量到期查询；实际请求间隔由 channels.next_data_api_poll_at
    // 持久化控制，热窗 60 秒不会因为进程重启而变成无界满速。
    let mut data_api_tick = tokio::time::interval(Duration::from_secs(1));
    data_api_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut poll = tokio::time::interval(Duration::from_secs(poll_seconds.max(1)));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let reconcile_period = Duration::from_secs(reconcile_hours * 3600);
    let mut reconcile = tokio::time::interval_at(
        tokio::time::Instant::now() + reconcile_period,
        reconcile_period,
    );
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = data_api_tick.tick(), if data_api_enabled => {
                match monitor.poll_data_api().await {
                    Ok(discovered) if discovered > 0 => {
                        tracing::info!(discovered, "Data API 主发现发现新候选")
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(error = %error, "Data API 主发现失败，尝试 RSS/yt-dlp 降级");
                        if let Err(fallback_error) = monitor.poll_all_normal().await {
                            tracing::error!(error = %fallback_error, "Data API 降级发现失败");
                        }
                    }
                }
            }
            _ = poll.tick() => {
                let result = match monitor.data_api_primary_enabled() {
                    Ok(true) => monitor.poll_normal_rss_probes(2).await,
                    Ok(false) => monitor.poll_all_normal().await,
                    Err(error) => {
                        tracing::warn!(error = %error, "读取 Data API 配额状态失败，回落到 RSS/yt-dlp");
                        monitor.poll_all_normal().await
                    }
                };
                if let Err(e) = result {
                    tracing::error!(error = %e, "轮询失败");
                }
            }
            _ = reconcile.tick() => match monitor.deep_scan_all().await {
                Ok(n) => tracing::info!(added = n, "每日频道深扫完成"),
                Err(e) => tracing::error!(error = %e, "每日频道深扫失败"),
            },
        }
    }
}

/// 数据库备份、历史清理和认证续期。
///
/// 维护循环本身是关键任务；单次备份、清理和认证检查显式归为非关键操作，失败只
/// 记录并等待下一轮，不能让一次运维故障退出整个循环。
async fn maintenance_loop(config: Config, db: Database) {
    let mut backup_tick = tokio::time::interval(Duration::from_secs(6 * 3600));
    backup_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut auth_tick = tokio::time::interval(Duration::from_secs(24 * 3600));
    auth_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = backup_tick.tick() => run_backup_and_prune(&config, &db).await,
            _ = auth_tick.tick() => {
                if let Err(e) = check_auth(&config, &db).await {
                    tracing::error!(error = %e, "认证检查失败");
                }
            }
        }
    }
}

/// 每个任务开始前重新读一次配置，让 `y2b model set` 之类的改动立刻生效。
///
/// 读取失败时沿用启动时的配置继续跑（不能因为配置写坏就停摆），但必须留下
/// 日志——此前是静默回退，配置写错了完全没有迹象。
fn reload_config(path: &std::path::Path, fallback: &Config) -> Config {
    match Config::load(path) {
        Ok(config) => config,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "重新读取配置失败，本次任务沿用启动时的配置"
            );
            fallback.clone()
        }
    }
}

/// 整库备份和历史清理都是同步的重活（拷贝整个 state.db、一个大删除事务），
/// 放到阻塞线程池执行，避免占住 tokio worker 线程数秒。
async fn run_backup_and_prune(config: &Config, db: &Database) {
    let config = config.clone();
    let db = db.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        // 备份失败不该跳过清理，两者分别汇报。
        (
            backup(&config, &db),
            db.prune_history(HISTORY_RETENTION_DAYS),
        )
    })
    .await;
    match outcome {
        Ok((backup_result, prune_result)) => {
            match backup_result {
                Ok(dest) => tracing::info!(path = %dest.display(), "备份完成"),
                Err(e) => tracing::error!(error = %e, "备份失败"),
            }
            match prune_result {
                Ok((ai_calls, events, stages)) => {
                    tracing::info!(ai_calls, events, stages, "历史清理完成")
                }
                Err(e) => tracing::error!(error = %e, "历史清理失败"),
            }
        }
        Err(e) => tracing::error!(error = %e, "备份任务异常结束"),
    }
}

async fn reap_worker(worker: &mut Option<tokio::task::JoinHandle<()>>, name: &str) {
    if worker.as_ref().is_some_and(|handle| handle.is_finished())
        && let Some(done) = worker.take()
        && let Err(error) = done.await
    {
        tracing::error!(error = %error, worker = name, "工作线程异常");
    }
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

const HISTORY_RETENTION_DAYS: i64 = 30;

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
    while files.len() > keep {
        let entry = files.remove(0);
        std::fs::remove_file(entry.path())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::process::Output;

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
    fn deployment_scripts_install_and_preflight_sqlite() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bootstrap = fs::read_to_string(root.join("deploy/bootstrap-server.sh")).unwrap();
        let install = bootstrap
            .lines()
            .find(|line| line.starts_with("apt-get install -y "))
            .unwrap();
        assert!(
            install
                .split_whitespace()
                .any(|dependency| dependency == "sqlite3")
        );
        assert!(bootstrap.contains("install -d /opt/y2b/releases"));

        let deploy = fs::read_to_string(root.join("deploy/deploy-app.sh")).unwrap();
        let sqlite_check = deploy.find("command -v \"$sqlite3_cmd\"").unwrap();
        let mv_probe = deploy
            .find("\"$mv_cmd\" -Tf -- \"$mv_probe_dir/source\" \"$mv_probe_dir/target\"")
            .unwrap();
        let hold_table_check = deploy.find("maintenance_hold_tables=").unwrap();
        let acquire = deploy.find("maintenance acquire").unwrap();
        let idle_wait = deploy.find("\nwait_for_two_idle_checks\n").unwrap();
        assert!(sqlite_check < acquire);
        assert!(mv_probe < acquire);
        assert!(hold_table_check < acquire);
        assert!(acquire < idle_wait);
        assert!(!deploy.contains("wait_for_two_idle_checks_bootstrap"));
        assert!(!deploy.contains("legacy_capture_required"));
        assert!(deploy.contains(
            "maintenance status \\\n      --database \"$database\" --owner \"$owner\" --json"
        ));
        assert!(deploy.contains("\"$mv_cmd\" -Tf -- \"$current_temp\" \"$current_link\""));
        assert!(!deploy.contains("rm -f -- \"$current_link\""));

        let service = fs::read_to_string(root.join("deploy/y2b-watch.service")).unwrap();
        assert!(service.contains("ExecStart=/opt/y2b/current/y2b"));
        assert!(service.contains("/opt/y2b/current/pi/y2b-extension.ts"));
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
        assert_eq!(current_schema_version(), CURRENT_SCHEMA_VERSION);

        let restore =
            fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/restore.sh"))
                .unwrap();
        assert!(restore.contains("\"$y2b_cmd\" schema-version"));
        assert!(!restore.contains("Y2B_SCHEMA_VERSION:-19"));
    }

    #[test]
    fn migrate_command_opens_the_explicit_database() {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("state.db");
        assert_eq!(migrate_database(&database).unwrap(), CURRENT_SCHEMA_VERSION);
        assert!(database.exists());
    }

    fn write_executable(path: &Path, content: &str) {
        fs::write(path, content).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    const OLD_DEPLOY_REVISION: &str = "aaaaaaaaaaaa";
    const NEW_DEPLOY_REVISION: &str = "bbbbbbbbbbbb";

    struct DeployFixture {
        _temp: tempfile::TempDir,
        candidate: PathBuf,
        app_root: PathBuf,
        state_dir: PathBuf,
        database: PathBuf,
        config: PathBuf,
        env_file: PathBuf,
        unit_dir: PathBuf,
        local_bin: PathBuf,
        local_sbin: PathBuf,
        current: PathBuf,
        hold: PathBuf,
        claim_attempt: PathBuf,
        claim_blocked: PathBuf,
        claim_acquired: PathBuf,
        service_state: PathBuf,
        systemctl_log: PathBuf,
        events: PathBuf,
        scenario: &'static str,
        schema_version: i64,
        hold_table_exists: bool,
        credential_owner: String,
        path: String,
    }

    fn deploy_fixture(scenario: &'static str) -> DeployFixture {
        deploy_fixture_with_layout(scenario, false, CURRENT_SCHEMA_VERSION)
    }

    fn deploy_fixture_legacy(scenario: &'static str, schema_version: i64) -> DeployFixture {
        deploy_fixture_with_layout(scenario, true, schema_version)
    }

    fn deploy_fixture_with_layout(
        scenario: &'static str,
        legacy: bool,
        schema_version: i64,
    ) -> DeployFixture {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let bin_dir = root.join("stub-bin");
        let app_root = root.join("opt/y2b");
        let state_dir = root.join("var/lib/y2b");
        let unit_dir = root.join("etc/systemd/system");
        let local_bin = root.join("usr/local/bin");
        let local_sbin = root.join("usr/local/sbin");
        let old_release = app_root.join("releases").join(OLD_DEPLOY_REVISION);
        let mut directories = vec![
            bin_dir.clone(),
            state_dir.join("backups"),
            unit_dir.clone(),
            local_bin.clone(),
            local_sbin.clone(),
            root.join("etc/y2b"),
        ];
        if legacy {
            directories.push(app_root.join("pi"));
        } else {
            directories.push(old_release.join("pi"));
            directories.push(old_release.join("deploy"));
        }
        for directory in directories {
            fs::create_dir_all(directory).unwrap();
        }

        let y2b_stub = r#"#!/usr/bin/env bash
set -euo pipefail
line=" $* "
scenario=${DEPLOY_TEST_SCENARIO:-success}
target=$(readlink "$DEPLOY_TEST_CURRENT" 2>/dev/null || printf 'none')
printf 'target=%s args=%s\n' "$target" "$*" >>"$DEPLOY_TEST_Y2B_LOG"

value_for() {
  local wanted=$1
  shift
  while (( $# > 0 )); do
    if [[ $1 == "$wanted" ]]; then
      printf '%s\n' "$2"
      return
    fi
    shift
  done
  return 1
}

record_event() {
  printf '%s\n' "$1" >>"$DEPLOY_TEST_EVENTS"
}

if [[ "$line" == *" maintenance acquire "* ]]; then
  owner=$(value_for --owner "$@")
  [[ ! -e "$DEPLOY_TEST_HOLD" ]] || exit 1
  printf '%s\n' "$owner" >"$DEPLOY_TEST_HOLD"
  record_event "acquire:$owner"
  printf 'acquired\n'
elif [[ "$line" == *" maintenance renew "* ]]; then
  owner=$(value_for --owner "$@")
  [[ -f "$DEPLOY_TEST_HOLD" && $(<"$DEPLOY_TEST_HOLD") == "$owner" ]] || exit 1
  if [[ "$scenario" == race && -f "$DEPLOY_TEST_FIRST_IDLE" && ! -f "$DEPLOY_TEST_CLAIM_ATTEMPT" ]]; then
    printf 'attempted\n' >"$DEPLOY_TEST_CLAIM_ATTEMPT"
    if [[ -f "$DEPLOY_TEST_HOLD" ]]; then
      printf 'blocked\n' >"$DEPLOY_TEST_CLAIM_BLOCKED"
      record_event 'claim:blocked-by-maintenance'
    else
      printf 'acquired\n' >"$DEPLOY_TEST_CLAIM_ACQUIRED"
      record_event 'claim:acquired'
    fi
  fi
  printf 'renewed\n'
elif [[ "$line" == *" maintenance status "* ]]; then
  if [[ "$line" != *" --owner "* ]]; then
    # 健康检查探针：观察者视角不要求持锁，只证明进程能打开数据库并响应。
    record_event "health-probe:$target"
    printf '%s\n' '{"checked_at":"test","idle":true,"hold":null,"expired_hold":null,"blockers":[]}'
    exit 0
  fi
  owner=$(value_for --owner "$@")
  [[ -f "$DEPLOY_TEST_HOLD" && $(<"$DEPLOY_TEST_HOLD") == "$owner" ]] || exit 1
  count=0
  [[ ! -f "$DEPLOY_TEST_STATUS_COUNT" ]] || count=$(<"$DEPLOY_TEST_STATUS_COUNT")
  ((count += 1))
  printf '%s\n' "$count" >"$DEPLOY_TEST_STATUS_COUNT"
  case "$scenario" in
    blocker)
      printf '%s\n' '{"checked_at":"test","idle":false,"hold":null,"expired_hold":null,"blockers":[{"kind":"active_claims","count":1,"details":["job-42:upload:worker-7"]}]}'
      ;;
    live_once)
      printf '%s\n' '{"checked_at":"test","idle":false,"hold":null,"expired_hold":null,"blockers":[{"kind":"live_once_hold","count":1,"details":["owner=LIVE_ONCE:premiere expires_at=2099-01-01 reason=直播首发"]}]}'
      ;;
    *)
      printf '%s\n' '{"checked_at":"test","idle":true,"hold":null,"expired_hold":null,"blockers":[]}'
      if (( count == 1 )); then
        printf 'returned\n' >"$DEPLOY_TEST_FIRST_IDLE"
      fi
      ;;
  esac
elif [[ "$line" == *" maintenance release "* ]]; then
  owner=$(value_for --owner "$@")
  [[ -f "$DEPLOY_TEST_HOLD" && $(<"$DEPLOY_TEST_HOLD") == "$owner" ]] || exit 1
  rm -f "$DEPLOY_TEST_HOLD"
  record_event "release:$target"
  printf 'released\n'
elif [[ "$line" == *" config-check "* ]]; then
  record_event 'preflight:config-check'
elif [[ "$line" == *" migrate --database "* ]]; then
  printf 'new-database\n' >"$DEPLOY_TEST_DATABASE"
  record_event "migrate:$target"
  printf '__SCHEMA__\n'
elif [[ "$line" == *" schema-version "* ]]; then
  printf '__SCHEMA__\n'
elif [[ "$line" == *" check "* ]]; then
  content=$(<"$DEPLOY_TEST_DATABASE")
  record_event "check:$target:$content"
  if [[ "$line" == *" --write-baseline "* ]]; then
    baseline="$DEPLOY_TEST_BASELINE"
    previous_y2b=
    previous_external=
    if [[ -f "$baseline" ]]; then
      previous_y2b=$(sed -n 's/^y2b=//p' "$baseline")
      previous_external=$(sed -n 's/^external=//p' "$baseline")
    fi
    # 模拟真实 check 的两类基线条目：外部依赖漂移是必选失败，y2b 自身漂移仅告警。
    if [[ -n "$previous_external" && "$previous_external" != "external-stable" ]]; then
      echo "FAIL dependency baseline 漂移: external" >&2
      exit 1
    fi
    if [[ -n "$previous_y2b" && "$previous_y2b" != "$target" ]]; then
      record_event 'baseline:y2b-drift-tolerated'
    fi
    printf 'y2b=%s\nexternal=external-stable\n' "$target" >"$baseline"
  fi
else
  echo "未预期的 y2b 参数: $*" >&2
  exit 2
fi
"#
        .replace("__SCHEMA__", &CURRENT_SCHEMA_VERSION.to_string());
        let old_y2b_stub = "#!/usr/bin/env bash\nexit 2\n";
        let candidate = bin_dir.join("y2b");
        write_executable(&candidate, &y2b_stub);

        let current = app_root.join("current");
        if legacy {
            write_executable(&local_bin.join("y2b"), old_y2b_stub);
            for resource in [
                "y2b-extension.ts",
                "policy.json",
                "audit-policy.json",
                "brawl-stars-glossary.json",
            ] {
                fs::write(app_root.join("pi").join(resource), resource).unwrap();
            }
            fs::write(app_root.join("Cargo.lock"), "old lock\n").unwrap();
        } else {
            write_executable(&old_release.join("y2b"), &y2b_stub);
            for resource in [
                "y2b-extension.ts",
                "policy.json",
                "audit-policy.json",
                "brawl-stars-glossary.json",
            ] {
                fs::write(old_release.join("pi").join(resource), resource).unwrap();
            }
            fs::write(old_release.join("Cargo.lock"), "old lock\n").unwrap();
            write_executable(
                &old_release.join("deploy/y2b-set-deepseek-key.py"),
                "#!/usr/bin/env python3\n",
            );
            symlink(format!("releases/{OLD_DEPLOY_REVISION}"), &current).unwrap();
            symlink("current/pi", app_root.join("pi")).unwrap();
            symlink("current/Cargo.lock", app_root.join("Cargo.lock")).unwrap();
            symlink("current/deploy", app_root.join("deploy")).unwrap();
            symlink(current.join("y2b"), local_bin.join("y2b")).unwrap();
        }

        let database = state_dir.join("state.db");
        fs::write(&database, "old-database\n").unwrap();
        let config = root.join("etc/y2b/config.toml");
        fs::write(
            &config,
            format!(
                "[runtime]\ndatabase = \"{}\"\n[ai]\nextension = \"{}/pi/y2b-extension.ts\"\npolicy = \"{}/pi/policy.json\"\n",
                database.display(),
                app_root.display(),
                app_root.display()
            ),
        )
        .unwrap();
        let env_file = root.join("etc/y2b/y2b.env");
        fs::write(&env_file, "YOUTUBE_API_KEY=test-only\n").unwrap();
        let mut env_permissions = fs::metadata(&env_file).unwrap().permissions();
        env_permissions.set_mode(0o600);
        fs::set_permissions(&env_file, env_permissions).unwrap();

        let service_state = root.join("service-state");
        fs::write(&service_state, "active\n").unwrap();
        let systemctl_log = root.join("systemctl.log");
        let events = root.join("events.log");
        write_executable(
            &bin_dir.join("systemctl"),
            r#"#!/usr/bin/env bash
set -euo pipefail
target=$(readlink "$DEPLOY_TEST_CURRENT" 2>/dev/null || printf 'none')
content=$(<"$DEPLOY_TEST_DATABASE")
printf '%s target=%s database=%s\n' "$*" "$target" "$content" >>"$DEPLOY_TEST_SYSTEMCTL_LOG"
line=" $* "
if [[ ${1:-} == stop ]]; then
  printf 'inactive\n' >"$DEPLOY_TEST_SERVICE_STATE"
  printf 'stop:%s:%s\n' "$target" "$content" >>"$DEPLOY_TEST_EVENTS"
elif [[ ${1:-} == start ]]; then
  if [[ "$DEPLOY_TEST_SCENARIO" == health_failure && "$target" == "releases/bbbbbbbbbbbb" ]]; then
    printf 'failed\n' >"$DEPLOY_TEST_SERVICE_STATE"
  else
    printf 'active\n' >"$DEPLOY_TEST_SERVICE_STATE"
  fi
  printf 'start:%s:%s\n' "$target" "$content" >>"$DEPLOY_TEST_EVENTS"
elif [[ ${1:-} == is-active ]]; then
  if [[ "$DEPLOY_TEST_SCENARIO" == health_exit && "$target" == "releases/bbbbbbbbbbbb" ]]; then
    count=0
    [[ ! -f "$DEPLOY_TEST_ISACTIVE_COUNT" ]] || count=$(<"$DEPLOY_TEST_ISACTIVE_COUNT")
    ((count += 1))
    printf '%s\n' "$count" >"$DEPLOY_TEST_ISACTIVE_COUNT"
    if (( count >= 3 )); then
      exit 3
    fi
  fi
  [[ $(<"$DEPLOY_TEST_SERVICE_STATE") == active ]] && exit 0
  exit 3
elif [[ ${1:-} == show ]]; then
  pid=${DEPLOY_TEST_MAINPID:-4242}
  restarts=${DEPLOY_TEST_NRESTARTS:-0}
  if [[ "$target" == "releases/bbbbbbbbbbbb" ]]; then
    case "$DEPLOY_TEST_SCENARIO" in
      health_pid_change)
        if [[ -f "$DEPLOY_TEST_HEALTH_SAMPLE" ]]; then
          pid=4243
        else
          printf 'sampled\n' >"$DEPLOY_TEST_HEALTH_SAMPLE"
        fi
        ;;
      health_restart)
        if [[ -f "$DEPLOY_TEST_HEALTH_SAMPLE" ]]; then
          restarts=1
        else
          printf 'sampled\n' >"$DEPLOY_TEST_HEALTH_SAMPLE"
        fi
        ;;
    esac
  fi
  printf '%s\n' "$pid"
  printf '%s\n' "$restarts"
elif [[ "$line" == *" status "* ]]; then
  [[ $(<"$DEPLOY_TEST_SERVICE_STATE") == active ]] && exit 0
  exit 3
elif [[ ${1:-} == daemon-reload || ${1:-} == enable ]]; then
  :
else
  echo "未预期的 systemctl 参数: $*" >&2
  exit 2
fi
"#,
        );

        let sqlite3_stub = r#"#!/usr/bin/env bash
set -euo pipefail
database=$1
query=$2
if [[ "$query" == .backup\ * ]]; then
  target=${query#.backup \'}
  target=${target%\'}
  printf 'backup:%s\n' "$target" >>"$DEPLOY_TEST_EVENTS"
  case "$DEPLOY_TEST_SCENARIO" in
    backup_missing) : ;;
    backup_corrupt) printf 'damaged-backup\n' >"$target" ;;
    *) cp "$database" "$target" ;;
  esac
elif [[ "$query" == *integrity_check* ]]; then
  if grep -q '^damaged' "$database"; then
    printf 'row 1 broken\nrow 2 missing\n'
  else
    printf 'ok\n'
  fi
elif [[ "$query" == *sqlite_master* ]]; then
  content=$(<"$database")
  if [[ "$content" == new-database* ]]; then
    printf '1\n'
  else
    printf '%s\n' "${DEPLOY_TEST_MAINTENANCE_HOLD_TABLE:-0}"
  fi
elif [[ "$query" == *schema_migrations* ]]; then
  content=$(<"$database")
  if [[ "$content" == new-database* ]]; then
    printf '__SCHEMA__\n'
  else
    printf '%s\n' "${DEPLOY_TEST_SCHEMA_VERSION:-__SCHEMA__}"
  fi
else
  echo "未预期的 sqlite3 查询: $query" >&2
  exit 2
fi
"#
        .replace("__SCHEMA__", &CURRENT_SCHEMA_VERSION.to_string());
        write_executable(&bin_dir.join("sqlite3"), &sqlite3_stub);

        let python = std::process::Command::new("python3")
            .args(["-c", "import sys; print(sys.executable)"])
            .output()
            .unwrap();
        assert!(python.status.success());
        let python = String::from_utf8(python.stdout).unwrap().trim().to_owned();
        write_executable(
            &bin_dir.join("python3"),
            &format!(
                r#"#!/usr/bin/env bash
set -euo pipefail
if [[ ${{1:-}} == *y2b-set-deepseek-key.py ]]; then
  exit 0
fi
exec "{python}" "$@"
"#
            ),
        );
        write_executable(&bin_dir.join("node"), "#!/usr/bin/env bash\nexit 0\n");
        write_executable(
            &bin_dir.join("mv"),
            &format!(
                r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "$DEPLOY_TEST_SCENARIO" == mv_without_t ]]; then
  for argument in "$@"; do
    case "$argument" in
      -*T*)
        echo 'mv: illegal option -- T' >&2
        exit 64
        ;;
    esac
  done
fi
operands=()
for argument in "$@"; do
  case "$argument" in
    --|-f|-T|-Tf|-fT) ;;
    -*) ;;
    *) operands+=("$argument") ;;
  esac
done
(( ${{#operands[@]}} == 2 )) || {{ echo "mv 测试桩参数错误: $*" >&2; exit 2; }}
if [[ ${{operands[1]}} == "$DEPLOY_TEST_CURRENT" ]]; then
  if [[ "$DEPLOY_TEST_SCENARIO" == non_atomic ]]; then
    rm -f -- "${{operands[1]}}"
  fi
  sleep 0.02
fi
exec "{python}" - "${{operands[0]}}" "${{operands[1]}}" <<'PY'
import os
import sys
os.replace(sys.argv[1], sys.argv[2])
PY
"#
            ),
        );

        let user = std::process::Command::new("id")
            .arg("-un")
            .output()
            .unwrap();
        let group = std::process::Command::new("id")
            .arg("-gn")
            .output()
            .unwrap();
        assert!(user.status.success() && group.status.success());
        let credential_owner = format!(
            "{}:{}",
            String::from_utf8(user.stdout).unwrap().trim(),
            String::from_utf8(group.stdout).unwrap().trim()
        );
        let path = format!(
            "{}:{}",
            bin_dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );

        DeployFixture {
            _temp: temp,
            candidate,
            app_root,
            state_dir,
            database,
            config,
            env_file,
            unit_dir,
            local_bin,
            local_sbin,
            current,
            hold: root.join("maintenance-hold"),
            claim_attempt: root.join("claim-attempt"),
            claim_blocked: root.join("claim-blocked"),
            claim_acquired: root.join("claim-acquired"),
            service_state,
            systemctl_log,
            events,
            scenario,
            schema_version,
            hold_table_exists: schema_version >= 22,
            credential_owner,
            path,
        }
    }

    impl DeployFixture {
        fn command(&self, idle_max_checks: usize) -> std::process::Command {
            let mut command = std::process::Command::new("bash");
            command
                .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/deploy-app.sh"))
                .arg(&self.candidate)
                .env("PATH", &self.path)
                .env("Y2B_APP_ROOT", &self.app_root)
                .env("Y2B_STATE_DIR", &self.state_dir)
                .env("Y2B_DATABASE", &self.database)
                .env("Y2B_CONFIG", &self.config)
                .env("Y2B_ENV_FILE", &self.env_file)
                .env("Y2B_SYSTEMD_UNIT_DIR", &self.unit_dir)
                .env("Y2B_BIN_LINK", self.local_bin.join("y2b"))
                .env(
                    "Y2B_KEY_TOOL_LINK",
                    self.local_sbin.join("y2b-set-deepseek-key"),
                )
                .env("Y2B_SERVICE", "y2b-test.service")
                .env("Y2B_CREDENTIAL_OWNER", &self.credential_owner)
                .env("Y2B_REVISION", NEW_DEPLOY_REVISION)
                .env("Y2B_DEPLOY_TIMESTAMP", "20260901T010203Z")
                .env("Y2B_IDLE_INTERVAL_SECONDS", "0")
                .env("Y2B_IDLE_MAX_CHECKS", idle_max_checks.to_string())
                .env("Y2B_HEALTH_INTERVAL_SECONDS", "0")
                .env("Y2B_HEALTH_MAX_CHECKS", "2")
                .env("Y2B_HEALTH_WINDOW_SECONDS", "0")
                .env(
                    "DEPLOY_TEST_HEALTH_SAMPLE",
                    self.state_dir.join("health-sample"),
                )
                .env(
                    "DEPLOY_TEST_ISACTIVE_COUNT",
                    self.state_dir.join("isactive-count"),
                )
                .env("Y2B_HOLD_LEASE_SECONDS", "60")
                .env("Y2B_RELEASE_KEEP", "5")
                .env("DEPLOY_TEST_SCENARIO", self.scenario)
                .env(
                    "DEPLOY_TEST_SCHEMA_VERSION",
                    self.schema_version.to_string(),
                )
                .env(
                    "DEPLOY_TEST_MAINTENANCE_HOLD_TABLE",
                    if self.hold_table_exists { "1" } else { "0" },
                )
                .env("DEPLOY_TEST_CURRENT", &self.current)
                .env("DEPLOY_TEST_DATABASE", &self.database)
                .env("DEPLOY_TEST_HOLD", &self.hold)
                .env("DEPLOY_TEST_FIRST_IDLE", self.state_dir.join("first-idle"))
                .env(
                    "DEPLOY_TEST_STATUS_COUNT",
                    self.state_dir.join("status-count"),
                )
                .env("DEPLOY_TEST_CLAIM_ATTEMPT", &self.claim_attempt)
                .env("DEPLOY_TEST_CLAIM_BLOCKED", &self.claim_blocked)
                .env("DEPLOY_TEST_CLAIM_ACQUIRED", &self.claim_acquired)
                .env("DEPLOY_TEST_SERVICE_STATE", &self.service_state)
                .env("DEPLOY_TEST_SYSTEMCTL_LOG", &self.systemctl_log)
                .env("DEPLOY_TEST_EVENTS", &self.events)
                .env(
                    "DEPLOY_TEST_BASELINE",
                    self.state_dir.join("dependency-baseline"),
                )
                .env("DEPLOY_TEST_Y2B_LOG", self.state_dir.join("y2b.log"));
            command
        }

        fn run(&self, idle_max_checks: usize) -> Output {
            self.command(idle_max_checks).output().unwrap()
        }
    }

    fn output_detail(output: &Output) -> String {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    #[test]
    fn deploy_hold_blocks_claim_between_two_idle_checks() {
        let fixture = deploy_fixture("race");
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(fixture.claim_attempt.exists());
        assert!(fixture.claim_blocked.exists());
        assert!(!fixture.claim_acquired.exists());
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{NEW_DEPLOY_REVISION}"))
        );
    }

    #[test]
    fn deploy_rejects_mv_without_t_before_acquiring_hold_or_stopping_service() {
        let fixture = deploy_fixture("mv_without_t");
        let mut command = fixture.command(3);
        command.env("TMPDIR", &fixture.state_dir);
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(detail.contains("mv 不支持 -T"), "{detail}");
        assert!(!detail.contains("mv 命令不可用"), "{detail}");
        assert!(!fixture.hold.exists());
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!fixture.systemctl_log.exists());
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(
            !fixture
                .app_root
                .join("releases")
                .join(NEW_DEPLOY_REVISION)
                .exists()
        );
        assert!(fs::read_dir(&fixture.state_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".y2b-mv-probe.")
        }));
    }

    #[test]
    fn deploy_distinguishes_an_unavailable_mv_command() {
        let fixture = deploy_fixture("success");
        let mut command = fixture.command(3);
        command.env("Y2B_MV", fixture.state_dir.join("missing-mv"));
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(
            detail.contains("mv 命令不可用，请检查 Y2B_MV 或安装 mv"),
            "{detail}"
        );
        assert!(!detail.contains("mv 不支持 -T"), "{detail}");
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn deploy_reports_blocker_and_refuses_before_install() {
        let fixture = deploy_fixture("blocker");
        let output = fixture.run(1);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(detail.contains("kind=active_claims count=1"), "{detail}");
        assert!(detail.contains("job-42:upload:worker-7"), "{detail}");
        assert!(!fixture.systemctl_log.exists());
        assert!(
            !fixture
                .app_root
                .join("releases")
                .join(NEW_DEPLOY_REVISION)
                .exists()
        );
    }

    #[test]
    fn deploy_reports_live_once_lease_and_refuses_to_continue() {
        let fixture = deploy_fixture("live_once");
        let output = fixture.run(1);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(detail.contains("kind=live_once_hold count=1"), "{detail}");
        assert!(detail.contains("owner=LIVE_ONCE:premiere"), "{detail}");
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn deploy_rejects_missing_or_corrupt_backup_before_stopping_service() {
        for scenario in ["backup_missing", "backup_corrupt"] {
            let fixture = deploy_fixture(scenario);
            let output = fixture.run(3);
            assert!(!output.status.success(), "{scenario}");
            let detail = output_detail(&output);
            assert!(
                detail.contains("迁移前备份缺失或为空")
                    || detail.contains("SQLite 完整性检查未返回唯一一行 ok"),
                "{scenario}: {detail}"
            );
            assert!(!fixture.systemctl_log.exists(), "{scenario}");
            assert_eq!(
                fs::read_to_string(&fixture.database).unwrap(),
                "old-database\n"
            );
        }
    }

    #[test]
    fn deploy_health_failure_rolls_back_release_and_database_as_a_pair() {
        let fixture = deploy_fixture("health_failure");
        let output = fixture.run(3);
        assert!(!output.status.success());
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());

        let events = fs::read_to_string(&fixture.events).unwrap();
        let new_start = events
            .find(&format!(
                "start:releases/{NEW_DEPLOY_REVISION}:new-database"
            ))
            .unwrap();
        let old_start = events
            .find(&format!(
                "start:releases/{OLD_DEPLOY_REVISION}:old-database"
            ))
            .unwrap();
        let release = events
            .find(&format!("release:releases/{OLD_DEPLOY_REVISION}"))
            .unwrap();
        assert!(new_start < old_start && old_start < release, "{events}");
    }

    fn assert_health_window_failure_rolls_back(scenario: &'static str) {
        let fixture = deploy_fixture(scenario);
        let output = fixture.run(3);
        assert!(!output.status.success(), "{}", output_detail(&output));
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());
        let events = fs::read_to_string(&fixture.events).unwrap();
        let new_start = events
            .find(&format!(
                "start:releases/{NEW_DEPLOY_REVISION}:new-database"
            ))
            .unwrap();
        let old_start = events
            .find(&format!(
                "start:releases/{OLD_DEPLOY_REVISION}:old-database"
            ))
            .unwrap();
        assert!(new_start < old_start, "{events}");
    }

    #[test]
    fn deploy_health_check_fails_when_process_restarts_in_window() {
        assert_health_window_failure_rolls_back("health_restart");
    }

    #[test]
    fn deploy_health_check_fails_when_process_exits_in_window() {
        assert_health_window_failure_rolls_back("health_exit");
    }

    #[test]
    fn deploy_health_check_fails_when_mainpid_changes() {
        assert_health_window_failure_rolls_back("health_pid_change");
    }

    #[test]
    fn deploy_rejects_when_external_dependency_drifts() {
        let fixture = deploy_fixture("success");
        fs::write(
            fixture.state_dir.join("dependency-baseline"),
            "y2b=releases/000000000000\nexternal=external-drifted\n",
        )
        .unwrap();
        let output = fixture.run(3);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(
            detail.contains("FAIL dependency baseline 漂移: external"),
            "{detail}"
        );
        assert!(detail.contains("自动回滚未完整成功"), "{detail}");
        // 外部依赖被偷换必须拦住部署：current 回滚到旧 release，数据库恢复，服务保持停止。
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "inactive\n"
        );
    }

    #[test]
    fn deploy_continues_when_only_the_deployed_binary_drifts() {
        let fixture = deploy_fixture("success");
        fs::write(
            fixture.state_dir.join("dependency-baseline"),
            "y2b=releases/000000000000\nexternal=external-stable\n",
        )
        .unwrap();
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(events.contains("baseline:y2b-drift-tolerated"), "{events}");
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{NEW_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "new-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());
    }

    #[test]
    fn deploy_rollback_reports_success_when_only_y2b_drifts_from_baseline() {
        let fixture = deploy_fixture("health_failure");
        let output = fixture.run(3);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(
            detail.contains("成对回滚完成，旧 release 已通过健康检查"),
            "{detail}"
        );
        assert!(!detail.contains("自动回滚未完整成功"), "{detail}");
        // 回滚切回旧 release 后，其 y2b 身份与部署写入的新基线不一致；门禁必须只当告警，
        // 否则旧 release 的 check 会失败，脚本会误报“自动回滚未完整成功”。
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(events.contains("baseline:y2b-drift-tolerated"), "{events}");
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());
    }

    fn observe_current_switch(
        child: &mut std::process::Child,
        current: &Path,
        releases_dir: &Path,
    ) -> Option<String> {
        let mut broken_observation = None;
        loop {
            // 触发瞬间一次性捕获完整状态：先 lstat 判断存在性与符号链接类型，
            // 再在同一轮内读取目标并校验，避免诊断字符串在窗口关闭后才二次读盘。
            let metadata = fs::symlink_metadata(current);
            let mut problem = match &metadata {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Some("current 缺失（lstat 返回 ENOENT）".to_string())
                }
                Err(error) => Some(format!("current 无法 lstat: {error}")),
                Ok(metadata) if !metadata.file_type().is_symlink() => Some(format!(
                    "current 不是符号链接: file_type={:?}",
                    metadata.file_type()
                )),
                Ok(_) => None,
            };
            if problem.is_none() {
                // 悬空判定不通过 current 做跟随 stat：macOS 在 rename 符号链接的
                // 瞬间会让跟随 stat 偶发 EINVAL，Path::exists 会把它误判成悬空。
                // 改为读取目标后直接校验 release 目录（切换期间该目录始终稳定）。
                if let Ok(target) = fs::read_link(current) {
                    let resolved = current.parent().unwrap().join(&target);
                    if fs::metadata(&resolved).is_err() {
                        problem = Some(format!("current 指向不存在的目录: target={target:?}"));
                    }
                }
            }
            if let Some(problem) = problem
                && broken_observation.is_none()
            {
                let releases = fs::read_dir(releases_dir)
                    .map(|entries| {
                        entries
                            .map(|entry| entry.unwrap().file_name())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                broken_observation = Some(format!("{problem} releases={releases:?}"));
            }
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::yield_now();
        }
        broken_observation
    }

    #[test]
    fn deploy_switch_never_exposes_a_missing_or_dangling_current() {
        let fixture = deploy_fixture("success");
        let mut command = fixture.command(3);
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().unwrap();
        let broken_observation = observe_current_switch(
            &mut child,
            &fixture.current,
            &fixture.app_root.join("releases"),
        );
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(
            broken_observation.is_none(),
            "current 在切换期间曾缺失或指向不存在的目录: {broken_observation:?}"
        );
    }

    #[test]
    fn deploy_switch_observation_catches_a_non_atomic_switch() {
        let fixture = deploy_fixture("non_atomic");
        let mut command = fixture.command(3);
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().unwrap();
        let broken_observation = observe_current_switch(
            &mut child,
            &fixture.current,
            &fixture.app_root.join("releases"),
        );
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(
            broken_observation.is_some(),
            "非原子切换应被观察者捕获，但未观察到缺失/悬空窗口"
        );
    }

    #[test]
    fn successful_deploy_releases_hold_after_new_service_is_healthy() {
        let fixture = deploy_fixture("success");
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(!fixture.hold.exists());
        let events = fs::read_to_string(&fixture.events).unwrap();
        let start = events
            .find(&format!(
                "start:releases/{NEW_DEPLOY_REVISION}:new-database"
            ))
            .unwrap();
        let release = events
            .find(&format!("release:releases/{NEW_DEPLOY_REVISION}"))
            .unwrap();
        assert!(start < release, "{events}");
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
    }

    #[test]
    fn deploy_rejects_legacy_flat_layout_without_touching_runtime() {
        let fixture = deploy_fixture_legacy("success", CURRENT_SCHEMA_VERSION);
        let output = fixture.run(3);
        assert!(!output.status.success(), "{}", output_detail(&output));
        let detail = output_detail(&output);
        assert!(detail.contains("不再支持旧扁平布局"), "{detail}");
        assert!(detail.contains("一次性布局迁移"), "{detail}");
        assert!(!fixture.hold.exists());
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!events.contains("stop:"), "{events}");
    }

    #[test]
    fn deploy_rejects_database_without_maintenance_hold() {
        let fixture = deploy_fixture_with_layout("success", false, 21);
        let output = fixture.run(3);
        assert!(!output.status.success(), "{}", output_detail(&output));
        let detail = output_detail(&output);
        assert!(detail.contains("schema v21"), "{detail}");
        assert!(detail.contains("不再执行无锁自举"), "{detail}");
        assert!(detail.contains("restore.sh"), "{detail}");
        assert!(!fixture.hold.exists());
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!events.contains("stop:"), "{events}");
    }

    #[test]
    fn deploy_uses_full_hold_when_hold_table_exists_but_schema_is_older() {
        let mut fixture = deploy_fixture_with_layout("success", false, 21);
        fixture.hold_table_exists = true;
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(events.contains("acquire:"), "{events}");
        assert!(
            events.contains(&format!("release:releases/{NEW_DEPLOY_REVISION}")),
            "{events}"
        );
        assert!(!fixture.hold.exists());
    }

    struct RestoreFixture {
        _temp: tempfile::TempDir,
        state_dir: PathBuf,
        backup: PathBuf,
        sqlite3: PathBuf,
        systemctl: PathBuf,
        y2b: PathBuf,
        service_state: PathBuf,
        systemctl_log: PathBuf,
        y2b_log: PathBuf,
        fail_start_once: PathBuf,
    }

    fn restore_fixture(active: bool) -> RestoreFixture {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("data");
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&bin_dir).unwrap();
        let backup = temp.path().join("backup.db");
        fs::write(&backup, format!("new-v{CURRENT_SCHEMA_VERSION}")).unwrap();

        let sqlite3 = bin_dir.join("sqlite3");
        write_executable(
            &sqlite3,
            r#"#!/usr/bin/env bash
set -euo pipefail
database=$1
query=$2
case "$query" in
  *integrity_check*)
    if grep -q '^damaged' "$database"; then
      printf 'row 1 missing\nrow 2 broken\n'
    else
      printf 'ok\n'
    fi
    ;;
  *sqlite_master*) printf '4\n' ;;
  *MAX\(version\)*)
    content=$(<"$database")
    if [[ "$content" =~ v([0-9]+) ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
    else
      echo "无法读取测试 schema" >&2
      exit 2
    fi
    ;;
  *) echo "未预期的 sqlite 查询: $query" >&2; exit 2 ;;
esac
"#,
        );

        let systemctl = bin_dir.join("systemctl");
        write_executable(
            &systemctl,
            r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1" >>"$RESTORE_TEST_SYSTEMCTL_LOG"
case "$1" in
  is-active)
    [[ $(<"$RESTORE_TEST_SERVICE_STATE") == active ]] && exit 0
    exit 3
    ;;
  stop) printf 'inactive\n' >"$RESTORE_TEST_SERVICE_STATE" ;;
  start)
    if [[ -f "$RESTORE_TEST_FAIL_START_ONCE" ]]; then
      rm -f "$RESTORE_TEST_FAIL_START_ONCE"
      exit 1
    fi
    printf 'active\n' >"$RESTORE_TEST_SERVICE_STATE"
    ;;
  show) printf '4242\n0\n' ;;
  *) exit 2 ;;
esac
"#,
        );
        let y2b = bin_dir.join("y2b");
        let y2b_script = r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$RESTORE_TEST_Y2B_LOG"
case "${1:-}" in
  schema-version) printf '__CURRENT_SCHEMA__\n' ;;
  migrate)
    if [[ ${2:-} == --help ]]; then exit 0; fi
    [[ ${2:-} == --database && -n ${3:-} ]] || exit 2
    database=$3
    content=$(<"$database")
    prefix=${content%v*}
    printf '%sv__CURRENT_SCHEMA__' "$prefix" >"$database"
    printf '__CURRENT_SCHEMA__\n'
    ;;
  maintenance) exit 0 ;;
  *) exit 2 ;;
esac
"#
        .replace("__CURRENT_SCHEMA__", &CURRENT_SCHEMA_VERSION.to_string());
        write_executable(&y2b, &y2b_script);

        let service_state = temp.path().join("service-state");
        fs::write(
            &service_state,
            if active { "active\n" } else { "inactive\n" },
        )
        .unwrap();

        RestoreFixture {
            _temp: temp,
            state_dir,
            backup,
            sqlite3,
            systemctl,
            y2b,
            service_state,
            systemctl_log: PathBuf::new(),
            y2b_log: PathBuf::new(),
            fail_start_once: PathBuf::new(),
        }
        .with_runtime_paths()
    }

    impl RestoreFixture {
        fn with_runtime_paths(mut self) -> Self {
            let root = self.service_state.parent().unwrap();
            self.systemctl_log = root.join("systemctl.log");
            self.y2b_log = root.join("y2b.log");
            self.fail_start_once = root.join("fail-start-once");
            self
        }

        fn run_with_sqlite(&self, sqlite3: &Path) -> Output {
            std::process::Command::new("bash")
                .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/restore.sh"))
                .arg(&self.backup)
                .env("Y2B_STATE_DIR", &self.state_dir)
                .env_remove("Y2B_DATABASE")
                .env_remove("Y2B_SCHEMA_VERSION")
                .env("Y2B_SQLITE3", sqlite3)
                .env("Y2B_SYSTEMCTL", &self.systemctl)
                .env("Y2B_BIN", &self.y2b)
                .env("Y2B_HEALTH_INTERVAL_SECONDS", "0")
                .env("Y2B_HEALTH_WINDOW_SECONDS", "0")
                .env("Y2B_HEALTH_MAX_CHECKS", "2")
                .env("RESTORE_TEST_SERVICE_STATE", &self.service_state)
                .env("RESTORE_TEST_SYSTEMCTL_LOG", &self.systemctl_log)
                .env("RESTORE_TEST_Y2B_LOG", &self.y2b_log)
                .env("RESTORE_TEST_FAIL_START_ONCE", &self.fail_start_once)
                .output()
                .unwrap()
        }

        fn run(&self) -> Output {
            self.run_with_sqlite(&self.sqlite3)
        }
    }

    #[test]
    fn restore_succeeds_without_an_existing_database() {
        let fixture = restore_fixture(false);
        let output = fixture.run();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(fixture.state_dir.join("state.db")).unwrap(),
            format!("new-v{CURRENT_SCHEMA_VERSION}")
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "inactive\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.y2b_log).unwrap(),
            "schema-version\n"
        );
    }

    #[test]
    fn restore_rejects_corrupt_backup_before_stopping_service() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(
            &fixture.backup,
            format!("damaged-v{CURRENT_SCHEMA_VERSION}"),
        )
        .unwrap();

        let output = fixture.run();
        assert!(!output.status.success());
        assert_eq!(fs::read_to_string(database).unwrap(), old);
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn restore_rejects_newer_backup_before_touching_current_database() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(
            &fixture.backup,
            format!("future-v{}", CURRENT_SCHEMA_VERSION + 1),
        )
        .unwrap();

        let output = fixture.run();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("无法降级恢复"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_to_string(database).unwrap(), old);
        assert!(!fixture.systemctl_log.exists());
        assert_eq!(
            fs::read_to_string(&fixture.y2b_log).unwrap(),
            "schema-version\n"
        );
    }

    #[test]
    fn restore_migrates_v17_backup_while_service_remains_inactive() {
        let fixture = restore_fixture(false);
        let database = fixture.state_dir.join("state.db");
        fs::write(&database, format!("old-v{CURRENT_SCHEMA_VERSION}")).unwrap();
        fs::write(&fixture.backup, "weekly-v17").unwrap();

        let output = fixture.run();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(database).unwrap(),
            format!("weekly-v{CURRENT_SCHEMA_VERSION}")
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "inactive\n"
        );
        let y2b_calls = fs::read_to_string(&fixture.y2b_log).unwrap();
        assert!(y2b_calls.contains("schema-version\n"));
        assert!(y2b_calls.contains("migrate --help\n"));
        assert!(y2b_calls.contains("migrate --database "));
    }

    #[test]
    fn restore_rejects_missing_sqlite_before_stopping_service() {
        let fixture = restore_fixture(true);
        let missing = fixture.state_dir.join("missing-sqlite3");
        let output = fixture.run_with_sqlite(&missing);

        assert!(!output.status.success());
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn restore_rolls_back_database_when_service_start_fails() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(&fixture.fail_start_once, "fail\n").unwrap();

        let output = fixture.run();
        assert!(!output.status.success());
        assert_eq!(fs::read_to_string(database).unwrap(), old);
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        let calls = fs::read_to_string(&fixture.systemctl_log).unwrap();
        assert!(calls.matches("start\n").count() >= 2);
    }

    #[test]
    fn restore_requires_stable_window_before_declaring_success() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(&fixture.backup, format!("new-v{CURRENT_SCHEMA_VERSION}")).unwrap();

        let output = fixture.run();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(database).unwrap(),
            format!("new-v{CURRENT_SCHEMA_VERSION}")
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        let calls = fs::read_to_string(&fixture.systemctl_log).unwrap();
        assert!(
            calls.matches("show\n").count() >= 2,
            "稳定窗口未采样: {calls}"
        );
        let y2b_calls = fs::read_to_string(&fixture.y2b_log).unwrap();
        assert!(
            y2b_calls.contains("maintenance status --database "),
            "{y2b_calls}"
        );
    }

    #[tokio::test]
    #[ignore = "由父测试进程单独启动，验证退出码"]
    async fn critical_task_panic_child_process() {
        let mut tasks = tokio::task::JoinSet::<CriticalTaskResult>::new();
        tasks.spawn(async {
            panic!("测试强制关键后台任务 panic");
        });
        supervise_watch(tasks, std::future::pending::<Result<()>>())
            .await
            .unwrap();
    }

    #[test]
    fn critical_task_panic_makes_process_exit_nonzero() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::critical_task_panic_child_process",
                "--ignored",
                "--nocapture",
            ])
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        let detail = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.status.success(), "关键任务 panic 后进程仍成功退出");
        assert!(
            detail.contains("关键后台任务异常结束"),
            "进程没有报告关键任务失败: {detail}"
        );
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
