use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

/// 单批闸门处理的候选数。
///
/// 取 50 是为了对齐 `videos.list` 的批量上限：闸门每轮最多发一次 videos.list，
/// 一次 1 个配额单位。取小于 50（此前是 25）会让同样的候选量多花一倍配额。
const GATE_BATCH: usize = 50;
use y2b_rs::{
    Database, check,
    config::{AI_MODEL, AI_PROVIDER, AI_THINKING, AI_TRANSLATION_MODEL, Config},
    db::CURRENT_SCHEMA_VERSION,
    model::{ChannelPriority, JobStatus, TransferMode},
    monitor::Monitor,
    pipeline::{self, AiCircuitBreaker, Pipeline},
    process::run_monitored,
    tui, websub,
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
        Cmd::ConfigCheck | Cmd::SchemaVersion | Cmd::Migrate { .. } => unreachable!(),
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
                let jobs = pipeline.db.jobs_awaiting_subtitle()?;
                if jobs.is_empty() {
                    println!("没有待补字幕的已投稿视频");
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
                            println!("{bvid} 补字幕失败: {error:#}");
                        }
                    }
                }
                println!("完成: 提交 {submitted}，跳过 {skipped}，失败 {failed}");
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
                std::fs::copy(cookies_file, &config.youtube.cookies)?;
                std::fs::set_permissions(
                    &config.youtube.cookies,
                    std::fs::Permissions::from_mode(0o600),
                )?;
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
async fn watch(config_path: PathBuf, config: Config, db: Database) -> Result<()> {
    let recovered = db.recover_incomplete_jobs()?;
    if recovered > 0 {
        tracing::warn!(count = recovered, "已恢复服务重启前的未完成任务");
    }
    let monitor = Monitor::new(config.clone(), db.clone())?;
    let discovery = tokio::spawn(discovery_loop(
        monitor,
        config.monitor.poll_seconds,
        config.monitor.reconcile_hours,
    ));
    let priority_discovery = tokio::spawn(priority_discovery_loop(Monitor::new(
        config.clone(),
        db.clone(),
    )?));
    let gate = tokio::spawn(gate_loop(Monitor::new(config.clone(), db.clone())?));
    let maintenance = tokio::spawn(maintenance_loop(config.clone(), db.clone()));
    let mut tasks = vec![discovery, priority_discovery, gate, maintenance];
    if config.websub.enabled {
        let websub_config = config.websub.clone();
        let websub_db = db.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(error) = websub::run(websub_config, websub_db).await {
                tracing::error!(error = %error, "WebSub 服务退出");
            }
        }));
    }
    let result = schedule_loop(&config_path, &config, &db).await;
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    result
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
fn prune(dir: &std::path::Path, keep: usize) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let mut files = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .collect::<Vec<_>>();
    files.sort_by_key(|e| e.file_name());
    while files.len() > keep {
        let e = files.remove(0);
        std::fs::remove_file(e.path())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
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

        let deploy = fs::read_to_string(root.join("deploy/deploy-app.sh")).unwrap();
        let sqlite_check = deploy.find("command -v sqlite3").unwrap();
        let first_idle_check = deploy.find("\nassert_idle\n").unwrap();
        assert!(sqlite_check < first_idle_check);
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
}
