# y2b-rs

Rust CLI/TUI 工具：监控 YouTube 频道更新，下载视频与字幕，调用 Pi Agent 分句和翻译，生成双语 ASS、用 FFmpeg 压制，并通过 biliup 投稿 Bilibili。

## 流程

- RSS 每 60 秒发现更新，每 6 小时用 yt-dlp 校对最近 30 条。
- 全局串行处理。下载限制到 60fps、约 2,073,600 像素，优先 AVC/AAC。
- 有英文字幕：Pi 分句 → Pi 翻译 → 双语 ASS → H.264/AAC 压制 → 投稿。
- 无字幕：先投稿原片，状态设为 `uploaded_original_pending_subtitle`；之后重查字幕并以 `biliup append --vid` 追加双语分P。
- Pi 默认 `openai-codex/gpt-5.6-luna`，thinking `high`；每次调用使用 `--no-session --no-tools`，只加载 `pi/y2b-extension.ts`。
- Pi 批处理支持 `adaptive` 和 `whole_video`。默认按 256k 上下文、200k 安全阈值估算输入与输出；阈值内整条视频只调用一次分句和一次翻译，超限时按 token 拆批。自适应分句携带前后 12 条上下文，并在 Pi 返回的自然分句边界衔接批次。
- SQLite 持久化频道、任务、阶段、峰值 RSS、Pi token/cost 和认证状态。连续失败 5 次进入 `dead_letter` 并删除大型视频。

## CLI

```bash
y2b init
y2b check --write-baseline
y2b channels add 'https://www.youtube.com/@channel/videos'
y2b channels list
y2b channels sync
y2b watch
y2b tui
y2b run 'https://www.youtube.com/watch?v=VIDEO_ID'
y2b jobs list 50
y2b jobs show JOB_ID
y2b jobs retry JOB_ID
y2b jobs recheck-subtitle JOB_ID
y2b model list
y2b model set gpt-5.6-sol
y2b login youtube /path/to/cookies.txt
y2b login bilibili
y2b backup
y2b auth-check
```

TUI：`↑/↓` 选择，`r` 重试或恢复 dead-letter，`p` 重查字幕，`Space` 暂停，`m` 在 Luna/Sol/Terra 间切换，`a` 重做认证检查，`y`/`b` 导入 YouTube/Bilibili cookies，`q` 退出。服务会在下一条任务前重新读取配置。

## 新服务器部署

目标：Ubuntu 22.04 x86_64，`root@157.230.241.109`。服务器不编译 Rust 或 FFmpeg。

```bash
# 1. 服务器：2 GiB swap 和预编译依赖
scp deploy/bootstrap-server.sh root@157.230.241.109:/tmp/
ssh root@157.230.241.109 'bash /tmp/bootstrap-server.sh'

# 2. Mac：静态交叉编译
brew install zig
cargo install cargo-zigbuild --locked
rustup target add x86_64-unknown-linux-musl
cargo zigbuild --release --target x86_64-unknown-linux-musl

# 3. 上传二进制和运行资源
scp target/x86_64-unknown-linux-musl/release/y2b root@157.230.241.109:/tmp/y2b
scp -r pi config.example.toml deploy root@157.230.241.109:/tmp/y2b-release/
ssh root@157.230.241.109 'bash /tmp/y2b-release/deploy/deploy-app.sh /tmp/y2b'
```

需要另行放置且权限为 `0600`：

- `/root/.pi/agent/auth.json`
- `/var/lib/y2b/youtube_cookies.txt`
- `/var/lib/y2b/bilibili_cookies.json`

字体位于 `/opt/y2b/fonts`，默认使用 `SourceHanSansCN-Medium.otf` 和 `Inter-SemiBold.ttf`。执行 `deploy/verify-ass.sh` 会生成 `/var/lib/y2b/checks/ass-smoke.mp4` 和预览图。

systemd 资源限制：`MemoryHigh=1200M`、`MemoryMax=1600M`、`MemorySwapMax=1G`、`TasksMax=256`。查看状态：

```bash
systemctl status y2b-watch
journalctl -u y2b-watch -f
systemctl show y2b-watch -p MemoryCurrent -p MemoryPeak -p MemorySwapCurrent
```

## 恢复

1. 在空服务器运行 `bootstrap-server.sh`。
2. 恢复 `/etc/y2b/config.toml`、三份认证文件、`/opt/y2b/fonts` 和 Pi extension/policy。
3. 从 `/var/lib/y2b/backups/daily` 或 `weekly` 选择数据库，执行 `deploy/restore.sh BACKUP.db`。
4. 部署静态 `y2b`，执行 `y2b check`，再启动 `y2b-watch.service`。
5. SQLite 保存完整任务队列；`queued`/`retry_wait` 自动继续，`dead_letter` 从 TUI 或 CLI 恢复后会重新下载。

在线备份每 6 小时执行一次：保留 4 个小时备份、7 个日备份和 4 个周备份。数据库迁移前应先执行 `y2b backup`。

## 上线验收

```bash
y2b check --write-baseline
bash /opt/y2b/deploy/verify-ass.sh
y2b run '用户提供的未搬运 YouTube URL'
y2b jobs show JOB_ID
```

真实追加分P和投稿会改变 Bilibili 外部状态，只在提供测试 BV 和未搬运视频后执行。追加前程序尝试读取现有分P，达到配置的 `max_parts = 199` 时拒绝操作。
