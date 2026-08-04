use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::process::Stdio;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct ProcessOutput {
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: i64,
    pub peak_rss_kib: u64,
}

pub async fn run_monitored(mut command: Command, timeout: Duration) -> Result<ProcessOutput> {
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().context("启动子进程失败")?;
    let pid = child.id().context("子进程没有 PID")?;
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let stdout_task = tokio::spawn(async move {
        let mut b = Vec::new();
        stdout.read_to_end(&mut b).await.map(|_| b)
    });
    let stderr_task = tokio::spawn(async move {
        let mut b = Vec::new();
        stderr.read_to_end(&mut b).await.map(|_| b)
    });
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
            let _ = child.kill().await;
            bail!("子进程超时: {}s", timeout.as_secs());
        }
    };
    let out = stdout_task.await??;
    let err = stderr_task.await??;
    let stdout = String::from_utf8_lossy(&out).to_string();
    let stderr = String::from_utf8_lossy(&err).to_string();
    if !status.success() {
        bail!(
            "子进程退出码 {:?}: {}",
            status.code(),
            tail(&(stdout.clone() + "\n" + &stderr), 80)
        );
    }
    Ok(ProcessOutput {
        stdout,
        stderr,
        duration_ms: started.elapsed().as_millis() as i64,
        peak_rss_kib: peak,
    })
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
