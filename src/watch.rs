use super::{GATE_BATCH, backup, check_auth};
use anyhow::{Context, Result};
use chrono::Utc;
use std::{path::Path, time::Duration};
use y2b_rs::{
    Config, Database,
    monitor::Monitor,
    pipeline::{self, AiCircuitBreaker, Pipeline},
    websub,
};

const HISTORY_RETENTION_DAYS: i64 = 30;
type CriticalTaskResult = (&'static str, Result<()>);

pub(super) async fn watch(config_path: &Path, config: Config, db: Database) -> Result<()> {
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
    supervise_watch(tasks, schedule_loop(config_path, &config, &db)).await
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
    tasks.shutdown().await;
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
async fn schedule_loop(config_path: &Path, config: &Config, db: &Database) -> Result<()> {
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
                        let job_id = job.id.clone();
                        if let Err(error) = Pipeline::new(fresh, worker_db).submit_pending_subtitle(job).await { tracing::error!(job_id, %error, "CC 字幕补交失败"); }
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

/// 每个任务开始前读取最新配置；读取失败时沿用启动配置。
fn reload_config(path: &Path, fallback: &Config) -> Config {
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
            config.storage.auto_backup.then(|| backup(&config, &db)),
            db.prune_history(HISTORY_RETENTION_DAYS),
        )
    })
    .await;
    match outcome {
        Ok((backup_result, prune_result)) => {
            match backup_result {
                Some(Ok(dest)) => tracing::info!(path = %dest.display(), "备份完成"),
                Some(Err(e)) => tracing::error!(error = %e, "备份失败"),
                None => {}
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_automatic_backup_does_not_write_files_or_disable_manual_backup() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.runtime.data_dir = temp.path().to_path_buf();
        config.runtime.database = temp.path().join("state.db");
        config.runtime.backup_dir = temp.path().join("backups");
        config.storage.auto_backup = false;
        let db = Database::open(&config.runtime.database).unwrap();
        let conn = rusqlite::Connection::open(&config.runtime.database).unwrap();
        conn.execute(
            "INSERT INTO events(level,message,created_at) VALUES('info','old','2000-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
        db.event(None, "info", "recent").unwrap();

        run_backup_and_prune(&config, &db).await;
        assert!(!config.runtime.backup_dir.exists());
        let remaining: String = conn
            .query_row("SELECT message FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, "recent");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let manual = backup(&config, &db).unwrap();
        assert!(manual.is_file());
    }

    #[test]
    fn automatic_backup_defaults_on_and_can_be_disabled_in_config() {
        let default: y2b_rs::config::StorageConfig = toml::from_str("").unwrap();
        assert!(default.auto_backup);
        let disabled: y2b_rs::config::StorageConfig =
            toml::from_str("auto_backup = false").unwrap();
        assert!(!disabled.auto_backup);
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
                "watch::tests::critical_task_panic_child_process",
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
}
