use crate::{config::Config, db::Database, model::JobStatus};
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
    time::Duration,
};
use sysinfo::{Disks, System};

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
    let mut notice = String::new();
    let mut import_target: Option<bool> = None;
    let mut import_path = String::new();
    loop {
        let jobs = db.list_jobs(50)?;
        if jobs.is_empty() {
            state.select(None);
        } else if state.selected().unwrap_or(0) >= jobs.len() {
            state.select(Some(jobs.len() - 1));
        }
        let channels = db.list_channels()?;
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
                " y2b-rs  model: {}/{} thinking: {}  jobs: {} channels: {} ",
                config.ai.provider,
                config.ai.model,
                config.ai.thinking,
                jobs.len(),
                channels.len()
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
            let header = Row::new(["状态", "视频", "标题", "模型", "发现时间", "BV/错误"]).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            );
            let rows = jobs.iter().map(|j| {
                let color = match j.status {
                    JobStatus::Completed => Color::Green,
                    JobStatus::Failed | JobStatus::DeadLetter => Color::Red,
                    JobStatus::UploadedOriginalPendingSubtitle => Color::Magenta,
                    JobStatus::Paused => Color::DarkGray,
                    _ => Color::White,
                };
                Row::new(vec![
                    Cell::from(j.status.to_string()),
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
                    Constraint::Length(28),
                    Constraint::Length(12),
                    Constraint::Percentage(35),
                    Constraint::Length(16),
                    Constraint::Length(15),
                    Constraint::Percentage(25),
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
                format!(
                    "↑↓ | r重试 p补字幕 Space暂停 m换模型 | a认证 y/YT导入 b/B站导入 | q退出   {}",
                    notice
                )
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
            match k.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Up => {
                    if !jobs.is_empty() {
                        state.select(Some(state.selected().unwrap_or(0).saturating_sub(1)))
                    }
                }
                KeyCode::Down => {
                    if !jobs.is_empty() {
                        state.select(Some(
                            (state.selected().unwrap_or(0) + 1).min(jobs.len() - 1),
                        ))
                    }
                }
                KeyCode::Char('r') => {
                    if let Some(i) = state.selected()
                        && let Some(j) = jobs.get(i)
                    {
                        if j.status == JobStatus::UploadedOriginalPendingSubtitle
                            || j.append_to_bvid.is_some()
                        {
                            match db.queue_subtitle_recheck(&j.id) {
                                Ok(()) => notice = format!("已排队补字幕并追加原稿 {}", j.video_id),
                                Err(e) => notice = format!("补字幕排队失败: {e}"),
                            }
                        } else {
                            db.update_job_status(&j.id, JobStatus::Queued, None)?;
                            notice = format!("已重新排队 {}", j.video_id);
                        }
                    }
                }
                KeyCode::Char('p') => {
                    if let Some(i) = state.selected()
                        && let Some(j) = jobs.get(i)
                    {
                        match db.queue_subtitle_recheck(&j.id) {
                            Ok(()) => notice = format!("已排队补字幕并追加原稿 {}", j.video_id),
                            Err(e) => notice = format!("补字幕排队失败: {e}"),
                        }
                    }
                }
                KeyCode::Char(' ') => {
                    if let Some(i) = state.selected()
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
