use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use y2b_rs::{
    Database, check,
    config::Config,
    model::{JobStatus, TransferMode},
    monitor::Monitor,
    pipeline::{self, AiCircuitBreaker, Pipeline},
    process::run_monitored,
    tui,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("y2b_rs=info".parse()?),
        )
        .init();
    let cli = Cli::parse();
    let mut config = Config::load(&cli.config)?;
    if matches!(cli.command, Cmd::Init) {
        config.ensure_dirs()?;
        config.save(&cli.config)?;
        println!("已初始化 {}", cli.config.display());
        return Ok(());
    }
    config.ensure_dirs()?;
    let db = Database::open(&config.runtime.database)?;
    match cli.command {
        Cmd::Init => unreachable!(),
        Cmd::Check { write_baseline } => {
            for i in check::run(&config, &db).await {
                println!(
                    "{} {:<20} {}",
                    if i.ok { "OK" } else { "FAIL" },
                    i.name,
                    i.detail
                );
            }
            if write_baseline {
                let p = config.runtime.data_dir.join("dependency-baseline.json");
                check::write_baseline(&config, &p).await?;
                println!("baseline: {}", p.display());
            }
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
                        "{}\t{}\t{}\t{}\t{}",
                        c.id,
                        if c.enabled { "on" } else { "off" },
                        c.transfer_mode,
                        c.name,
                        c.youtube_channel_id
                    )
                }
            }
            ChannelCmd::SetMode { id, mode } => {
                db.set_channel_transfer_mode(id, mode)?;
                println!("频道 {id} 的新任务模式已更新为 {mode}");
            }
            ChannelCmd::Enable { id } => db.set_channel_enabled(id, true)?,
            ChannelCmd::Disable { id } => db.set_channel_enabled(id, false)?,
            ChannelCmd::Sync { id } => {
                let m = Monitor::new(config, db)?;
                if let Some(id) = id {
                    println!("新增 {} 条", m.poll_channel(id, true).await?)
                } else {
                    println!("新增 {} 条", m.poll_all().await?)
                }
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
                for m in &config.ai.allowed_models {
                    println!("{}/{}\t{}", m.provider, m.model, m.label)
                }
            }
            ModelCmd::Set { model, provider } => {
                let provider = provider.unwrap_or_else(|| "openai-codex".into());
                if !config
                    .ai
                    .allowed_models
                    .iter()
                    .any(|m| m.provider == provider && m.model == model)
                {
                    anyhow::bail!("模型不在 allowed_models: {provider}/{model}")
                }
                config.ai.provider = provider;
                config.ai.model = model;
                config.save(&cli.config)?;
                println!("默认模型已更新");
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
    }
    Ok(())
}

/// `watch` 的三条循环各自独立成任务。
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
    let maintenance = tokio::spawn(maintenance_loop(config.clone(), db.clone()));
    let result = schedule_loop(&config_path, &config, &db).await;
    for task in [discovery, maintenance] {
        task.abort();
        let _ = task.await;
    }
    result
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
                    && let Some(job) = db.next_queued_job()?
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
                    && let Some(job) = db.next_pending_subtitle_job(pipeline::CC_MAX_ATTEMPTS)?
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

/// RSS 轮询与 yt-dlp 校对。
///
/// 两者共用一个任务而不是各占一个：它们都会拉起 yt-dlp，并发执行会在 2 GiB
/// 服务器上叠加内存压力。串行执行保持了原有的资源占用特征，同时不再阻塞调度。
async fn discovery_loop(monitor: Monitor, poll_seconds: u64, reconcile_hours: u64) {
    let mut poll = tokio::time::interval(Duration::from_secs(poll_seconds));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let reconcile_period = Duration::from_secs(reconcile_hours * 3600);
    let mut reconcile = tokio::time::interval_at(
        tokio::time::Instant::now() + reconcile_period,
        reconcile_period,
    );
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = poll.tick() => {
                if let Err(e) = monitor.poll_all().await {
                    tracing::error!(error = %e, "轮询失败");
                }
            }
            _ = reconcile.tick() => match monitor.reconcile_all().await {
                Ok(n) => tracing::info!(added = n, "yt-dlp 校对完成"),
                Err(e) => tracing::error!(error = %e, "yt-dlp 校对失败"),
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

async fn check_auth(config: &Config, db: &Database) -> Result<()> {
    let monitor = Monitor::new(config.clone(), db.clone())?;
    match monitor.fetch_metadata(&config.youtube.probe_url).await {
        Ok(_) => db.set_setting(
            "auth.youtube",
            &format!("ok {}", chrono::Utc::now().to_rfc3339()),
        )?,
        Err(e) => db.set_setting("auth.youtube", &format!("failed: {e}"))?,
    }
    let mut cmd = Command::new(&config.bilibili.biliup);
    cmd.arg("-u").arg(&config.bilibili.cookies).arg("renew");
    match run_monitored(cmd, Duration::from_secs(180)).await {
        Ok(_) => db.set_setting(
            "auth.bilibili",
            &format!("ok {}", chrono::Utc::now().to_rfc3339()),
        )?,
        Err(e) => db.set_setting("auth.bilibili", &format!("failed: {e}"))?,
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
