use crate::{
    config::Config,
    db::Database,
    model::{JobStatus, TransferMode},
    monitor::{EnqueueOutcome, Monitor},
};
use anyhow::Result;
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
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Stdio},
    sync::mpsc::{Receiver, TryRecvError, channel},
    time::Duration,
};
use sysinfo::{Disks, System};

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
    loop {
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
        let mut sys = System::new_all();
        sys.refresh_all();
        let disks = Disks::new_with_refreshed_list();
        let free = disks
            .list()
            .iter()
            .map(|d| d.available_space())
            .sum::<u64>()
            / (1024 * 1024 * 1024);
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
                    sys.used_memory() / 1024 / 1024,
                    sys.total_memory() / 1024 / 1024,
                    sys.used_swap() / 1024 / 1024,
                    sys.total_swap() / 1024 / 1024,
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
                        Constraint::Percentage(25),
                        Constraint::Percentage(30),
                        Constraint::Percentage(25),
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
                            db.retry_job(&j.id)?;
                            notice = format!("已重新排队 {}", j.video_id);
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
                        db.update_job_status(&j.id, JobStatus::Paused, None)?;
                        notice = format!("已暂停 {}", j.video_id);
                    }
                }
                KeyCode::Char('m') => {
                    if let Some(pos) = config.ai.allowed_models.iter().position(|m| {
                        m.provider == config.ai.provider && m.model == config.ai.model
                    }) {
                        let m =
                            &config.ai.allowed_models[(pos + 1) % config.ai.allowed_models.len()];
                        config.ai.provider = m.provider.clone();
                        config.ai.model = m.model.clone();
                        config.save(config_path)?;
                        notice = format!("新任务模型: {}/{}", config.ai.provider, config.ai.model);
                    }
                }
                KeyCode::Char('a') => {
                    match std::env::current_exe().and_then(|exe| {
                        Command::new(exe)
                            .arg("--config")
                            .arg(config_path)
                            .arg("auth-check")
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .spawn()
                            .map(|_| ())
                    }) {
                        Ok(()) => notice = "已启动认证检查".into(),
                        Err(e) => notice = format!("认证检查启动失败: {e}"),
                    };
                }
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
    Ok(())
}

fn import_cookie(source: &Path, dest: &Path) -> Result<()> {
    std::fs::copy(source, dest)?;
    std::fs::set_permissions(dest, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
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
    fn manual_entry_can_be_cancelled() {
        let mut input = ManualInput::Url("https://youtu.be/test".into());
        assert_eq!(
            handle_manual_key(&mut input, KeyCode::Esc),
            ManualAction::Cancelled
        );
        assert_eq!(input, ManualInput::Idle);
    }
}
