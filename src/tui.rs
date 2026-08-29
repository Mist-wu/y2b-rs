use crate::{
    config::Config,
    db::Database,
    model::{JobStatus, TransferMode},
    monitor::{EnqueueOutcome, Monitor},
};
use anyhow::{Context, Result};
use chrono_tz::Tz;
use crossterm::event::{self, Event, KeyCode};
use ratatui::{
    DefaultTerminal,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
};
use std::{
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc::{Receiver, TryRecvError, channel},
    time::{Duration, Instant},
};
use sysinfo::System;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ManualInput {
    Idle,
    Url(String),
    Mode { url: String, mode: TransferMode },
    Submitting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ManualAction {
    None,
    Submit { url: String, mode: TransferMode },
    Cancelled,
}

fn handle_manual_key(input: &mut ManualInput, key: KeyCode) -> ManualAction {
    match input {
        ManualInput::Idle | ManualInput::Submitting => ManualAction::None,
        ManualInput::Url(value) => match key {
            KeyCode::Esc => {
                *input = ManualInput::Idle;
                ManualAction::Cancelled
            }
            KeyCode::Enter if !value.trim().is_empty() => {
                *input = ManualInput::Mode {
                    url: value.trim().to_string(),
                    mode: TransferMode::Translated,
                };
                ManualAction::None
            }
            KeyCode::Backspace => {
                value.pop();
                ManualAction::None
            }
            KeyCode::Char(c) => {
                value.push(c);
                ManualAction::None
            }
            _ => ManualAction::None,
        },
        ManualInput::Mode { url, mode } => match key {
            KeyCode::Esc => {
                *input = ManualInput::Idle;
                ManualAction::Cancelled
            }
            KeyCode::Char('1') => {
                *mode = TransferMode::Direct;
                ManualAction::None
            }
            KeyCode::Char('2') => {
                *mode = TransferMode::Translated;
                ManualAction::None
            }
            KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down => {
                *mode = match *mode {
                    TransferMode::Direct => TransferMode::Translated,
                    TransferMode::Translated => TransferMode::Direct,
                };
                ManualAction::None
            }
            KeyCode::Enter => {
                let action = ManualAction::Submit {
                    url: url.clone(),
                    mode: *mode,
                };
                *input = ManualInput::Submitting;
                action
            }
            _ => ManualAction::None,
        },
    }
}

pub fn run(config_path: &Path, mut config: Config, db: Database) -> Result<()> {
    let mut terminal = ratatui::init();
    let result = app(&mut terminal, config_path, &mut config, &db);
    ratatui::restore();
    result
}

fn app(
    terminal: &mut DefaultTerminal,
    config_path: &Path,
    config: &mut Config,
    db: &Database,
) -> Result<()> {
    let tz: Tz = config
        .runtime
        .timezone
        .parse()
        .unwrap_or(chrono_tz::Asia::Shanghai);
    let mut state = TableState::default().with_selected(Some(0));
    let mut channel_state = TableState::default().with_selected(Some(0));
    let mut show_channels = false;
    let mut notice = String::new();
    let mut import_target: Option<bool> = None;
    let mut import_path = String::new();
    let mut manual_input = ManualInput::Idle;
    let mut manual_result: Option<Receiver<Result<EnqueueOutcome>>> = None;
    let mut select_job_id: Option<String> = None;
    let mut host = HostStats::new(&config.runtime.data_dir);
    let mut auth_check = AuthCheckProcess::default();
    loop {
        if let Some(status) = auth_check.poll()? {
            notice = if status.success() {
                "认证检查已完成".into()
            } else {
                format!("认证检查失败: {status}")
            };
        }
        let received = manual_result.as_ref().map(Receiver::try_recv);
        match received {
            Some(Ok(Ok(outcome))) => {
                select_job_id = Some(outcome.job.id.clone());
                notice = if outcome.created {
                    format!(
                        "已加入队列 {} ({})",
                        outcome.job.video_id, outcome.job.transfer_mode
                    )
                } else {
                    format!(
                        "视频已存在 {} / {} / {}",
                        outcome.job.video_id, outcome.job.transfer_mode, outcome.job.status
                    )
                };
                manual_input = ManualInput::Idle;
                manual_result = None;
            }
            Some(Ok(Err(e))) => {
                notice = format!("手动入队失败: {e}");
                manual_input = ManualInput::Idle;
                manual_result = None;
            }
            Some(Err(TryRecvError::Disconnected)) => {
                notice = "手动入队任务异常结束".into();
                manual_input = ManualInput::Idle;
                manual_result = None;
            }
            Some(Err(TryRecvError::Empty)) | None => {}
        }
        let jobs = db.list_jobs(50)?;
        if let Some(id) = select_job_id.take()
            && let Some(index) = jobs.iter().position(|job| job.id == id)
        {
            state.select(Some(index));
        }
        if jobs.is_empty() {
            state.select(None);
        } else if state.selected().unwrap_or(0) >= jobs.len() {
            state.select(Some(jobs.len() - 1));
        }
        let channels = db.list_channels()?;
        if channels.is_empty() {
            channel_state.select(None);
        } else if channel_state.selected().unwrap_or(0) >= channels.len() {
            channel_state.select(Some(channels.len() - 1));
        }
        let direct_channels = channels
            .iter()
            .filter(|channel| channel.transfer_mode == TransferMode::Direct)
            .count();
        let translated_channels = channels.len().saturating_sub(direct_channels);
        let usage = db.ai_totals()?;
        let selected_job = state.selected().and_then(|i| jobs.get(i));
        let job_usage = selected_job
            .map(|j| db.ai_totals_for_job(&j.id))
            .transpose()?
            .map(|u| u.total)
            .unwrap_or(0);
        let channel_usage = selected_job
            .and_then(|j| j.channel_id)
            .map(|id| db.ai_totals_for_channel(id))
            .transpose()?
            .map(|u| u.total)
            .unwrap_or(0);
        // 主机指标按固定节奏刷新，而不是每次重绘都刷。`System::new_all()` +
        // `refresh_all()` 会扫描全部进程、网卡和温度传感器，在 2 GiB 服务器上
        // 每秒来一遍是实打实的 CPU 开销；这里只取内存/swap 和磁盘可用量。
        if host.needs_refresh() {
            host.refresh();
        }
        let free = host.free_gib;
        let yt_auth = db
            .get_setting("auth.youtube")
            .unwrap_or(None)
            .unwrap_or_else(|| "未检查".into());
        let bili_auth = db
            .get_setting("auth.bilibili")
            .unwrap_or(None)
            .unwrap_or_else(|| "未检查".into());
        terminal.draw(|f| {
            let areas = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(7),
                    Constraint::Min(8),
                    Constraint::Length(3),
                ])
                .split(f.area());
            let title = format!(
                " y2b-rs  model: {}/{} thinking: {}  jobs: {} channels: {} (direct: {} translated: {}) ",
                config.ai.provider,
                config.ai.model,
                config.ai.thinking,
                jobs.len(),
                channels.len(),
                direct_channels,
                translated_channels
            );
            f.render_widget(
                Paragraph::new(title)
                    .style(
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    )
                    .block(Block::default().borders(Borders::ALL)),
                areas[0],
            );
            let stats = vec![
                Line::from(format!(
                    "内存: {}/{} MiB   Swap: {}/{} MiB   磁盘剩余: {} GiB",
                    host.used_memory_mib,
                    host.total_memory_mib,
                    host.used_swap_mib,
                    host.total_swap_mib,
                    free
                )),
                Line::from(format!(
                    "Tokens: {} (in {} / out {} / reasoning {} / cache read {})",
                    usage.total, usage.input, usage.output, usage.reasoning, usage.cache_read
                )),
                Line::from(format!(
                    "选中任务 Tokens: {}   所属频道 Tokens: {}   今日 Tokens: {}",
                    job_usage,
                    channel_usage,
                    db.ai_tokens_today().unwrap_or(0)
                )),
                Line::from(format!("认证: YouTube={}  Bilibili={}", yt_auth, bili_auth)),
            ];
            f.render_widget(
                Paragraph::new(stats).block(
                    Block::default()
                        .title("服务器资源 / 认证")
                        .borders(Borders::ALL),
                ),
                areas[1],
            );
            if show_channels {
                let header = Row::new([
                    "ID",
                    "状态",
                    "模式",
                    "优先级",
                    "频道名称",
                    "YouTube Channel ID",
                    "最后检查/错误",
                ])
                .style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                );
                let rows = channels.iter().map(|channel| {
                    let checked = channel
                        .last_checked_at
                        .map(|value| {
                            value
                                .with_timezone(&tz)
                                .format("%m-%d %H:%M:%S")
                                .to_string()
                        })
                        .unwrap_or_default();
                    Row::new(vec![
                        Cell::from(channel.id.to_string()),
                        Cell::from(if channel.enabled { "enabled" } else { "disabled" }),
                        Cell::from(channel.transfer_mode.to_string()),
                        Cell::from(channel.priority.to_string()),
                        Cell::from(channel.name.clone()),
                        Cell::from(channel.youtube_channel_id.clone()),
                        Cell::from(channel.last_error.clone().unwrap_or(checked)),
                    ])
                    .style(Style::default().fg(if channel.enabled {
                        Color::White
                    } else {
                        Color::DarkGray
                    }))
                });
                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(6),
                        Constraint::Length(10),
                        Constraint::Length(10),
                        Constraint::Length(10),
                        Constraint::Percentage(25),
                        Constraint::Percentage(30),
                        Constraint::Percentage(20),
                    ],
                )
                .header(header)
                .block(
                    Block::default()
                        .title("频道（CLI 管理）")
                        .borders(Borders::ALL),
                )
                .column_spacing(1)
                .row_highlight_style(
                    Style::default()
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )
                .highlight_symbol("▶ ");
                f.render_stateful_widget(table, areas[2], &mut channel_state);
            } else {
                let header =
                    Row::new(["状态", "模式", "视频", "标题", "模型", "发现时间", "BV/错误"])
                        .style(
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        );
                let rows = jobs.iter().map(|j| {
                    let color = match j.status {
                        JobStatus::Completed => Color::Green,
                        JobStatus::Failed | JobStatus::DeadLetter => Color::Red,
                        JobStatus::ReadyToUpload => Color::Cyan,
                        JobStatus::UploadRetryWait => Color::Yellow,
                        JobStatus::UploadUncertain => Color::LightRed,
                        JobStatus::UploadedOriginalPendingSubtitle => Color::Magenta,
                        JobStatus::Paused => Color::DarkGray,
                        _ => Color::White,
                    };
                    Row::new(vec![
                        Cell::from(j.status.to_string()),
                        Cell::from(j.transfer_mode.to_string()),
                        Cell::from(j.video_id.clone()),
                        Cell::from(j.title.clone().unwrap_or_default()),
                        Cell::from(j.ai_model.clone().unwrap_or_default()),
                        Cell::from(
                            j.discovered_at
                                .with_timezone(&tz)
                                .format("%m-%d %H:%M:%S")
                                .to_string(),
                        ),
                        Cell::from(j.bvid.clone().or(j.error.clone()).unwrap_or_default()),
                    ])
                    .style(Style::default().fg(color))
                });
                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(26),
                        Constraint::Length(10),
                        Constraint::Length(12),
                        Constraint::Percentage(30),
                        Constraint::Length(16),
                        Constraint::Length(15),
                        Constraint::Percentage(20),
                    ],
                )
                .header(header)
                .block(Block::default().title("任务").borders(Borders::ALL))
                .column_spacing(1)
                .row_highlight_style(
                    Style::default()
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )
                .highlight_symbol("▶ ");
                f.render_stateful_widget(table, areas[2], &mut state);
            }
            let footer = if import_target.is_some() {
                format!(
                    "输入 {} cookies 的绝对路径，Enter 导入，Esc 取消: {}",
                    if import_target == Some(true) {
                        "YouTube"
                    } else {
                        "Bilibili"
                    },
                    import_path
                )
            } else {
                match &manual_input {
                    ManualInput::Url(url) => {
                        format!("输入单个 YouTube 视频 URL，Enter 下一步，Esc 取消: {url}")
                    }
                    ManualInput::Mode { url, mode } => format!(
                        "选择模式 [1 direct无字幕] [2 translated翻译字幕]，方向键切换，Enter入队，Esc取消: {mode}  {url}"
                    ),
                    ManualInput::Submitting => "正在解析视频并加入后台队列…".into(),
                    ManualInput::Idle => format!(
                        "Tab任务/频道 ↑↓ | n新视频 r重试 p补字幕 Space暂停 m换模型 | a认证 y/YT导入 b/B站导入 | q退出   {}",
                        notice
                    ),
                }
            };
            f.render_widget(
                Paragraph::new(footer)
                    .style(Style::default().fg(Color::Gray))
                    .block(Block::default().borders(Borders::TOP)),
                areas[3],
            );
        })?;
        if event::poll(Duration::from_millis(1000))?
            && let Event::Key(k) = event::read()?
        {
            if let Some(youtube) = import_target {
                match k.code {
                    KeyCode::Esc => {
                        import_target = None;
                        import_path.clear();
                        notice = "已取消导入".into();
                    }
                    KeyCode::Enter => {
                        let source = std::path::PathBuf::from(import_path.trim());
                        let dest = if youtube {
                            &config.youtube.cookies
                        } else {
                            &config.bilibili.cookies
                        };
                        match import_cookie(&source, dest) {
                            Ok(()) => notice = format!("已导入 {}，按 a 重检认证", dest.display()),
                            Err(e) => notice = format!("导入失败: {e}"),
                        };
                        import_target = None;
                        import_path.clear();
                    }
                    KeyCode::Backspace => {
                        import_path.pop();
                    }
                    KeyCode::Char(c) => import_path.push(c),
                    _ => {}
                }
                continue;
            }
            if manual_input != ManualInput::Idle {
                match handle_manual_key(&mut manual_input, k.code) {
                    ManualAction::None => {}
                    ManualAction::Cancelled => notice = "已取消手动入队".into(),
                    ManualAction::Submit { url, mode } => {
                        match Monitor::new(config.clone(), db.clone()) {
                            Ok(monitor) => {
                                let (sender, receiver) = channel();
                                tokio::spawn(async move {
                                    let _ = sender.send(monitor.enqueue_video(&url, mode).await);
                                });
                                manual_result = Some(receiver);
                            }
                            Err(e) => {
                                manual_input = ManualInput::Idle;
                                notice = format!("无法启动手动入队: {e}");
                            }
                        }
                    }
                }
                continue;
            }
            match k.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Tab => show_channels = !show_channels,
                KeyCode::Up => {
                    if show_channels && !channels.is_empty() {
                        channel_state.select(Some(
                            channel_state.selected().unwrap_or(0).saturating_sub(1),
                        ));
                    } else if !show_channels && !jobs.is_empty() {
                        state.select(Some(state.selected().unwrap_or(0).saturating_sub(1)))
                    }
                }
                KeyCode::Down => {
                    if show_channels && !channels.is_empty() {
                        channel_state.select(Some(
                            (channel_state.selected().unwrap_or(0) + 1).min(channels.len() - 1),
                        ));
                    } else if !show_channels && !jobs.is_empty() {
                        state.select(Some(
                            (state.selected().unwrap_or(0) + 1).min(jobs.len() - 1),
                        ))
                    }
                }
                KeyCode::Char('r') => {
                    if !show_channels
                        && let Some(i) = state.selected()
                        && let Some(j) = jobs.get(i)
                    {
                        if j.status == JobStatus::UploadedOriginalPendingSubtitle {
                            // 已投稿：重试的含义是重新武装 CC 字幕队列，
                            // 而不是重跑整条流水线（那会重复投稿）。
                            notice = match db.rearm_pending_subtitle(&j.id) {
                                Ok(()) => format!("已重新排队 CC 字幕补交 {}", j.video_id),
                                Err(e) => format!("重新排队失败: {e}"),
                            };
                        } else {
                            notice = match db.retry_job(&j.id) {
                                Ok(()) => format!("已重新排队 {}", j.video_id),
                                Err(e) => format!("重新排队失败: {e}"),
                            };
                        }
                    }
                }
                KeyCode::Char('p') => {
                    if !show_channels
                        && let Some(i) = state.selected()
                        && let Some(j) = jobs.get(i)
                    {
                        notice = format!(
                            "{} 请用 y2b subtitle add {} 补 CC 字幕",
                            j.video_id,
                            j.bvid.as_deref().unwrap_or("<bvid>")
                        );
                    }
                }
                KeyCode::Char(' ') => {
                    if !show_channels
                        && let Some(i) = state.selected()
                        && let Some(j) = jobs.get(i)
                    {
                        notice = match db.pause_job(&j.id) {
                            Ok(()) => format!("已暂停 {}", j.video_id),
                            Err(e) => format!("暂停失败: {e}"),
                        };
                    }
                }
                KeyCode::Char('a') => match std::env::current_exe() {
                    Ok(exe) => {
                        let mut command = Command::new(exe);
                        command
                            .arg("--config")
                            .arg(config_path)
                            .arg("auth-check")
                            .stdout(Stdio::null())
                            .stderr(Stdio::null());
                        match auth_check.start(&mut command) {
                            Ok(true) => notice = "已启动认证检查".into(),
                            Ok(false) => notice = "认证检查仍在运行，请勿重复启动".into(),
                            Err(e) => notice = format!("认证检查启动失败: {e}"),
                        }
                    }
                    Err(e) => notice = format!("无法定位当前程序: {e}"),
                },
                KeyCode::Char('y') => {
                    import_target = Some(true);
                    import_path.clear();
                }
                KeyCode::Char('b') => {
                    import_target = Some(false);
                    import_path.clear();
                }
                KeyCode::Char('n') => {
                    manual_input = ManualInput::Url(String::new());
                    notice.clear();
                }
                _ => {}
            }
        }
    }
    auth_check.shutdown()?;
    Ok(())
}

#[derive(Default)]
struct AuthCheckProcess {
    child: Option<Child>,
}

impl AuthCheckProcess {
    fn poll(&mut self) -> Result<Option<ExitStatus>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let status = child.try_wait()?;
        if status.is_some() {
            self.child.take();
        }
        Ok(status)
    }

    fn start(&mut self, command: &mut Command) -> Result<bool> {
        self.poll()?;
        if self.child.is_some() {
            return Ok(false);
        }
        self.child = Some(command.spawn()?);
        Ok(true)
    }

    #[cfg(test)]
    fn is_running(&self) -> bool {
        self.child.is_some()
    }

    fn shutdown(&mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if child.try_wait()?.is_none() {
            child.kill()?;
            child.wait()?;
        }
        Ok(())
    }
}

impl Drop for AuthCheckProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// 主机内存/swap/磁盘快照，按 `REFRESH_INTERVAL` 节流刷新。
///
/// 复用同一个 `System`（只刷内存那一类），而不是每次重绘都 `System::new_all()`
/// 重新扫描全部进程。
struct HostStats {
    system: System,
    data_dir: PathBuf,
    refreshed_at: Option<Instant>,
    used_memory_mib: u64,
    total_memory_mib: u64,
    used_swap_mib: u64,
    total_swap_mib: u64,
    free_gib: u64,
}

impl HostStats {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

    fn new(data_dir: &Path) -> Self {
        Self {
            system: System::new(),
            data_dir: data_dir.to_path_buf(),
            refreshed_at: None,
            used_memory_mib: 0,
            total_memory_mib: 0,
            used_swap_mib: 0,
            total_swap_mib: 0,
            free_gib: 0,
        }
    }

    fn needs_refresh(&self) -> bool {
        self.refreshed_at
            .is_none_or(|at| at.elapsed() >= Self::REFRESH_INTERVAL)
    }

    fn refresh(&mut self) {
        self.system.refresh_memory();
        self.used_memory_mib = self.system.used_memory() / 1024 / 1024;
        self.total_memory_mib = self.system.total_memory() / 1024 / 1024;
        self.used_swap_mib = self.system.used_swap() / 1024 / 1024;
        self.total_swap_mib = self.system.total_swap() / 1024 / 1024;
        self.free_gib = fs2::available_space(&self.data_dir).unwrap_or(0) / (1024 * 1024 * 1024);
        self.refreshed_at = Some(Instant::now());
    }
}

pub fn import_cookie(source: &Path, dest: &Path) -> Result<()> {
    let parent = dest.parent().context("cookies 目标路径没有父目录")?;
    let name = dest
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cookies");
    let temporary = parent.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut input = std::fs::File::open(source)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        output.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        std::io::copy(&mut input, &mut output)?;
        output.sync_all()?;
        drop(output);
        std::fs::rename(&temporary, dest)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_url_requires_value_and_can_select_direct() {
        let mut input = ManualInput::Url(String::new());
        assert_eq!(
            handle_manual_key(&mut input, KeyCode::Enter),
            ManualAction::None
        );
        for character in "https://youtu.be/test".chars() {
            handle_manual_key(&mut input, KeyCode::Char(character));
        }
        handle_manual_key(&mut input, KeyCode::Enter);
        assert!(matches!(
            input,
            ManualInput::Mode {
                mode: TransferMode::Translated,
                ..
            }
        ));
        handle_manual_key(&mut input, KeyCode::Char('1'));
        let action = handle_manual_key(&mut input, KeyCode::Enter);
        assert_eq!(
            action,
            ManualAction::Submit {
                url: "https://youtu.be/test".into(),
                mode: TransferMode::Direct,
            }
        );
        assert_eq!(input, ManualInput::Submitting);
    }

    #[test]
    fn host_stats_populate_once_then_throttle() {
        let directory = tempfile::tempdir().unwrap();
        let mut host = HostStats::new(directory.path());
        assert!(host.needs_refresh(), "首次绘制必须刷新");
        host.refresh();
        assert!(host.total_memory_mib > 0, "刷新后应读到总内存");
        assert_eq!(
            host.free_gib,
            fs2::available_space(directory.path()).unwrap() / (1024 * 1024 * 1024),
            "磁盘值必须来自 data_dir 所在文件系统"
        );
        // 刷新后进入节流窗口，后续重绘直接复用快照。
        assert!(!host.needs_refresh());
    }

    #[test]
    fn auth_check_process_blocks_duplicates_then_reaps_child() {
        let mut auth = AuthCheckProcess::default();
        let mut running = Command::new("sleep");
        running.arg("2");
        assert!(auth.start(&mut running).unwrap());
        assert!(auth.is_running());

        let mut duplicate = Command::new("true");
        assert!(!auth.start(&mut duplicate).unwrap());
        auth.shutdown().unwrap();
        assert!(!auth.is_running());

        let mut completed = Command::new("true");
        assert!(auth.start(&mut completed).unwrap());
        let deadline = Instant::now() + Duration::from_secs(2);
        while auth.poll().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!auth.is_running(), "退出的认证子进程必须被回收");
    }

    #[test]
    fn cookie_import_atomically_replaces_with_private_file() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.txt");
        let dest = directory.path().join("cookies.txt");
        std::fs::write(&source, "new-cookie").unwrap();
        std::fs::write(&dest, "old-cookie").unwrap();
        assert!(import_cookie(&directory.path().join("missing"), &dest).is_err());
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "old-cookie");
        import_cookie(&source, &dest).unwrap();

        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "new-cookie");
        assert_eq!(
            std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(
            std::fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
        );
    }

    #[test]
    fn manual_entry_can_be_cancelled() {
        let mut input = ManualInput::Url("https://youtu.be/test".into());
        assert_eq!(
            handle_manual_key(&mut input, KeyCode::Esc),
            ManualAction::Cancelled
        );
        assert_eq!(input, ManualInput::Idle);
    }
}
