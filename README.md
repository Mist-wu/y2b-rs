# y2b-rs

Rust CLI/TUI 工具：监控 YouTube 频道更新，按频道选择原片直传或 Pi 分句/翻译字幕后压制，并通过 biliup 投稿 Bilibili。

## 流程

- RSS 每 60 秒发现更新，每 6 小时用 yt-dlp 校对最近 30 条。
- 全局串行处理。单个 `translated` 任务内部并行下载视频和处理字幕。下载限制到 60fps、约 2,073,600 像素，优先 AVC/AAC。
- `direct`：并行下载视频和调用 Pi 一次生成中文标题、动态文案和标签；不下载字幕、不分句、不压制。
- `translated`：英文字幕 → Pi 分句 → Pi 翻译 → 双语 ASS → H.264/AAC 压制 → 投稿。
- `translated` 无字幕时自动直传原片，状态设为 `uploaded_original_pending_subtitle`；之后重查字幕并以 `biliup append --vid` 追加双语分P。
- 普通投稿按每个视频一次无状态 `publish_metadata` Pi 请求生成中文标题、动态文案和标签。字幕模式在预算内传入完整双语字幕，超限时保留首尾并均匀采样；结果持久化后，任务重试或服务重启不会重复调用 Pi。标题或动态不合格会重试，不会用英文原标题或固定动态投稿。
- 投稿固定为手机游戏分区 `tid=172`、转载 `copyright=2` 并填写 YouTube 来源；标签始终以“荒野乱斗”开头。简介由程序按原标题、来源、原作者、原发布日期、处理方式和工具地址确定性生成。
- Pi 默认 `openai-codex/gpt-5.6-luna`，thinking `high`；每次调用使用 `--no-session --no-tools`，只加载 `pi/y2b-extension.ts`。
- Pi 批处理支持 `adaptive` 和 `whole_video`。默认按 256k 上下文、200k 安全阈值估算输入与输出；阈值内整条视频只调用一次分句和一次翻译，超限时按 token 拆批。自适应分句携带前后 12 条上下文，并在 Pi 返回的自然分句边界衔接批次。
- SQLite 持久化频道、任务、阶段、峰值 RSS、Pi token/cost 和认证状态。连续失败 5 次进入 `dead_letter` 并删除大型视频。

## CLI

```bash
y2b init
y2b check --write-baseline
y2b channels add 'https://www.youtube.com/@channel/videos' --mode direct
y2b channels add 'https://www.youtube.com/@another/videos' --mode translated
y2b channels list
y2b channels set-mode 1 translated
y2b channels enable 1
y2b channels disable 1
y2b channels sync
y2b watch
y2b tui
y2b jobs add 'https://www.youtube.com/watch?v=VIDEO_ID' --mode direct
y2b jobs add 'https://www.youtube.com/watch?v=VIDEO_ID' --mode translated
y2b run 'https://www.youtube.com/watch?v=VIDEO_ID' --mode translated
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

TUI：`Tab` 切换任务/频道列表，`↑/↓` 选择，`n` 输入单个 YouTube URL 并选择 `direct` 或 `translated`，`r` 重试或恢复 dead-letter，`p` 重查字幕，`Space` 暂停，`m` 在 Luna/Sol/Terra 间切换，`a` 重做认证检查，`y`/`b` 导入 YouTube/Bilibili cookies，`q` 退出。手动 URL 在后台解析并入队，重复 URL 会定位已有任务；频道增删、模式切换和启停仅由 CLI 管理。

频道模式只是新任务的默认值。任务入队后会固化当时的模式，后续 `channels set-mode` 不会改写旧任务。`video_id` 全局唯一，同一视频不会二次入队或二次投稿。`y2b run` 的 `--mode` 默认为 `translated`；`channels add` 和 `jobs add` 要求显式指定 `--mode`。

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
4. 部署静态 `y2b`，执行 `y2b check --write-baseline`，再启动 `y2b-watch.service`。打开数据库时会自动升级到 v5，旧频道和任务的模式均为 `translated`；v5 会持久化已验证的投稿元数据。
5. SQLite 保存完整任务队列；`queued`/`retry_wait`/`processing` 会在重启后恢复，任务模式和追加目标 BV 不丢失，`dead_letter` 从 TUI 或 CLI 恢复后会重新下载。

在线备份每 6 小时执行一次：保留 4 个小时备份、7 个日备份和 4 个周备份。数据库迁移前应先执行 `y2b backup`。

## 上线验收

```bash
y2b check --write-baseline
bash /opt/y2b/deploy/verify-ass.sh
y2b run '用户提供的 direct 验收 URL' --mode direct
y2b run '用户提供的带英文字幕 URL' --mode translated
y2b jobs show JOB_ID
```

真实追加分P和投稿会改变 Bilibili 外部状态，只在提供测试 BV 和未搬运视频后执行。追加前程序尝试读取现有分P，达到配置的 `max_parts = 199` 时拒绝操作。
