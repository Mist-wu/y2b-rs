use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

/// 读取子进程输出并限制捕获大小：超过上限时只保留尾部。
///
/// pi 的 JSONL 事件流可能输出数百 MB（deepseek 的 thinking 全量流式），
/// 无限收集会撑爆低配服务器内存；关键信息（agent_end 事件、biliup 的 BV
/// 号、错误尾部）都位于输出末尾，保留尾部即可。
const MAX_CAPTURE_BYTES: usize = 32 * 1024 * 1024;

async fn read_capped<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_CAPTURE_BYTES * 2 {
            let drop = buf.len() - MAX_CAPTURE_BYTES;
            buf.drain(..drop);
        }
    }
    if buf.len() > MAX_CAPTURE_BYTES {
        let drop = buf.len() - MAX_CAPTURE_BYTES;
        buf.drain(..drop);
    }
    Ok(buf)
}

#[derive(Debug, Clone)]
pub struct ProcessOutput {
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: i64,
    pub peak_rss_kib: u64,
}

/// 子进程已经退出、且 stdout/stderr 已完整回收时的失败。
///
/// 调用方通常只需要把它当作普通错误处理；Pi 审计则会读取其中的 JSONL 尾部，
/// 尽可能回收供应商已经返回的 token/费用，避免“请求已计费但本地只看到退出码”。
#[derive(Debug, Error)]
#[error("子进程退出码 {code:?}: {detail}")]
pub struct ProcessFailure {
    code: Option<i32>,
    detail: String,
    output: ProcessOutput,
}

impl ProcessFailure {
    pub fn output(&self) -> &ProcessOutput {
        &self.output
    }
}

/// `run_monitored` future 被 `try_join!` 等调用方取消时，Tokio 的
/// `kill_on_drop` 只保证终止直接子进程，无法覆盖 yt-dlp PyInstaller 启动器 fork
/// 出来的后代。用独立进程组的 RAII guard 补齐取消路径，避免孤儿下载继续写
/// `.part` 并占用内存/带宽。
struct ProcessGroupGuard {
    pid: Option<u32>,
}

impl ProcessGroupGuard {
    fn new(pid: u32) -> Self {
        Self { pid: Some(pid) }
    }

    fn kill(&mut self) {
        if let Some(pid) = self.pid.take() {
            kill_process_group(pid);
        }
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

pub async fn run_monitored(mut command: Command, timeout: Duration) -> Result<ProcessOutput> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // 每条外部命令独占进程组。服务器上的 yt-dlp 是 PyInstaller onefile：启动器
    // 会再 fork 真正的 Python 进程，只 kill 启动器会留下 PPID=1 的 yt-dlp/Node，
    // 继续占用数百 MiB。独立进程组允许超时时一次清掉完整后代树。
    command.as_std_mut().process_group(0);
    let mut child = command.spawn().context("启动子进程失败")?;
    let pid = child.id().context("子进程没有 PID")?;
    let mut process_group = ProcessGroupGuard::new(pid);
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let stdout_task = tokio::spawn(async move { read_capped(&mut stdout).await });
    let stderr_task = tokio::spawn(async move { read_capped(&mut stderr).await });
    let started = Instant::now();
    let mut sys = System::new();
    let (status, peak) = match tokio::time::timeout(timeout, async {
        let mut peak = 0u64;
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = ticker.tick() => { peak = peak.max(process_tree_rss(pid, &mut sys)); }
                status = child.wait() => return Ok::<(std::process::ExitStatus, u64), anyhow::Error>((status?, peak)),
            }
        }
    })
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            process_group.kill();
            // `start_kill` 是 killpg 失败时对直接子进程的兜底。先显式终止两个
            // 读取任务，不能让它们继续等待 PyInstaller 后代继承的输出管道。
            let _ = child.start_kill();
            stdout_task.abort();
            stderr_task.abort();
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            let _ = child.wait().await;
            bail!("子进程超时: {}s", timeout.as_secs());
        }
    };
    let out = stdout_task.await??;
    let err = stderr_task.await??;
    // 直接子进程已退出且输出管道已全部 EOF，此时确认整组正常收尾；之后即使
    // 构造返回值失败，也不应再向可能复用的 PID 发信号。
    process_group.disarm();
    let stdout = String::from_utf8_lossy(&out).to_string();
    let stderr = String::from_utf8_lossy(&err).to_string();
    let output = ProcessOutput {
        stdout,
        stderr,
        duration_ms: started.elapsed().as_millis() as i64,
        peak_rss_kib: peak,
    };
    if !status.success() {
        return Err(ProcessFailure {
            code: status.code(),
            detail: tail(&(output.stdout.clone() + "\n" + &output.stderr), 80),
            output,
        }
        .into());
    }
    Ok(output)
}

/// 向整组发 SIGKILL。进程组 leader 即使已经退出，组内 PyInstaller/Node 后代
/// 仍保留同一个 pgid，`killpg` 依然可以清理它们。
fn kill_process_group(pid: u32) {
    if let Ok(pgid) = libc::pid_t::try_from(pid) {
        // SAFETY: killpg 只接收整数 pgid/signal；该 pgid 由刚 spawn 的子进程创建，
        // 不会命中 y2b 自身所在的进程组。
        unsafe {
            libc::killpg(pgid, libc::SIGKILL);
        }
    }
}

fn process_tree_rss(root: u32, sys: &mut System) -> u64 {
    let root = Pid::from_u32(root);
    sys.refresh_processes(ProcessesToUpdate::All, true);
    let mut wanted = HashSet::from([root]);
    let mut changed = true;
    while changed {
        changed = false;
        for (p, proc_) in sys.processes() {
            if let Some(parent) = proc_.parent()
                && wanted.contains(&parent)
                && wanted.insert(*p)
            {
                changed = true;
            }
        }
    }
    wanted
        .into_iter()
        .filter_map(|p| sys.process(p))
        .map(|p| p.memory() / 1024)
        .sum()
}

pub fn tail(s: &str, lines: usize) -> String {
    let v: Vec<_> = s.lines().collect();
    v[v.len().saturating_sub(lines)..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nonzero_exit_preserves_captured_output_for_audit() {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "printf '{\"type\":\"agent_end\"}'; printf 'provider error' >&2; exit 7",
        ]);
        let error = run_monitored(command, Duration::from_secs(2))
            .await
            .unwrap_err();
        let failure = error.downcast_ref::<ProcessFailure>().unwrap();
        assert_eq!(failure.output().stdout, r#"{"type":"agent_end"}"#);
        assert_eq!(failure.output().stderr, "provider error");
        assert!(error.to_string().contains("子进程退出码 Some(7)"));
    }

    #[tokio::test]
    async fn timeout_kills_the_whole_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("grandchild.pid");
        let script = format!(
            "sh -c 'echo $$ > {}; exec sleep 30' & wait",
            pid_file.display()
        );
        let mut command = Command::new("sh");
        command.args(["-c", &script]);

        let error = run_monitored(command, Duration::from_millis(500))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("子进程超时"));

        let grandchild: libc::pid_t = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            // SAFETY: signal 0 only checks whether this test-owned PID still exists.
            let alive = unsafe { libc::kill(grandchild, 0) } == 0;
            if !alive {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "超时后代进程仍存活: {grandchild}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn cancellation_kills_the_whole_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("cancelled-grandchild.pid");
        let script = format!(
            "sh -c 'echo $$ > {}; exec sleep 30' & wait",
            pid_file.display()
        );
        let mut command = Command::new("sh");
        command.args(["-c", &script]);
        let task = tokio::spawn(run_monitored(command, Duration::from_secs(30)));

        let created_deadline = Instant::now() + Duration::from_secs(2);
        while !pid_file.exists() {
            assert!(
                Instant::now() < created_deadline,
                "取消测试的后代进程未启动"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let grandchild: libc::pid_t = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        task.abort();
        let _ = task.await;
        let killed_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            // SAFETY: signal 0 only checks whether this test-owned PID still exists.
            let alive = unsafe { libc::kill(grandchild, 0) } == 0;
            if !alive {
                break;
            }
            assert!(
                Instant::now() < killed_deadline,
                "取消后代进程仍存活: {grandchild}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
