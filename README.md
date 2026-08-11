# y2b-rs

Rust CLI/TUI 工具：监控 YouTube 频道更新，按频道选择原片直传或 Pi 分句/翻译后投稿，并通过 biliup 投稿 Bilibili，已投稿视频自动补中文 CC 字幕。

## 流程

### 发现与筛选

- RSS 每 60 秒发现更新，每 6 小时用 yt-dlp 校对最近 30 条。RSS 请求失败会先短退避重试 3 次；yt-dlp 回退同时受单频道冷却和全局 10 分钟最多 3 次的熔断限制，避免 YouTube 暂态故障演变成请求风暴。
- 直播回放（`was_live`）按普通视频搬运。直播中（`is_live`）、预约（`is_upcoming`）和回放生成中（`post_live`）暂不入队，每 30 分钟复查一次，回放就绪后自动搬运。
- 超过 `youtube.max_duration_seconds`（默认 2 小时）的视频直接跳过，不入队，并持久化判定以免每 30 分钟重复请求；放宽上限后会自动重新检查。已入队的任务若发现超时长会直接进 `dead_letter`，不消耗重试次数。
- 只自动搬运策略生效之后开播的回放。首次运行时把 `live_replay.enqueue_after` 游标设为当时时间，早于该时间开播的历史回放不会被 RSS 或 yt-dlp 校对扫进队列；手动 `y2b jobs add` 不受此限制。

### 处理

- 单个 `translated` 任务内部并行下载视频和处理字幕。下载限制到 60fps、约 2,073,600 像素，优先 AVC/AAC。
- `direct`：并行下载视频和调用 Pi 一次生成中文标题、动态文案和标签；不下载字幕、不分句。
- `translated`：英文字幕 → Pi 分句 → Pi 翻译 → 上传原片 → 自动提交中文 CC 字幕（B站软字幕，观众可开关，不走压制）。投稿成功后任务转入 `uploaded_original_pending_subtitle`，字幕 worker 在 90 秒后开始尝试提交；稿件仍在 B站处理中（`-404`）按 60 秒重试，其余失败按 `min(90 × 2^n, 1h)` 退避，最多 8 次。素材本就缺失时不重试，留给 `y2b subtitle add/all` 手动补。
- 普通投稿按每个视频一次无状态 `publish_metadata` Pi 请求生成中文标题、动态文案和标签。字幕模式在预算内传入完整双语字幕，超限时保留首尾并均匀采样；结果持久化后，任务重试或服务重启不会重复调用 Pi。标题或动态不合格会重试，不会用英文原标题或固定动态投稿。

### 投稿

- 投稿固定为手机游戏分区 `tid=172`、自制 `copyright=1` 并允许转载；不使用 Bilibili 转载来源字段。标签始终以“荒野乱斗”开头，简介按清理 hashtag 后的原标题、YouTube 来源、原作者和工具地址确定性生成。
- 所有新投稿都下载 yt-dlp 选定的 YouTube 原封面，转为 JPEG 后通过 biliup `--cover` 上传；封面失败时任务重试而不会无封面投稿。

### Pi

- 所有 Pi 调用固定为 `deepseek/deepseek-v4-flash`、`thinking=off`；投稿元数据、分句、翻译和词库审计共用同一配置。每次调用使用 `--no-session --no-tools`，只加载 `pi/y2b-extension.ts`。配置加载和部署预检都会拒绝其他 provider、model 或 thinking，避免任务间漂移和大 thinking 流式输出导致的额外成本/OOM。
- `pi/brawl-stars-glossary.json` 来自国际服客户端英文/简中本地化资源。当前审计脚本只测试固定的 DeepSeek V4 Flash 配置；JSON 内的 `audit` 字段保留词库生成时的历史模型溯源，不参与运行时模型选择。extension 每次仅注入当前输入实际出现的词条，避免全量词库占用上下文。
- Pi 批处理支持 `adaptive` 和 `whole_video`。默认按 256k 上下文、200k 安全阈值估算输入与输出；阈值内整条视频只调用一次分句和一次翻译，超限时按 token 拆批。自适应分句携带前后 12 条上下文，并在 Pi 返回的自然分句边界衔接批次。

### 队列与容错

- SQLite 持久化频道、任务、阶段、峰值 RSS、Pi token/cost 和认证状态。连续失败 5 次进入 `dead_letter` 并删除大型视频；失败之间按 `min(5min × 2^n, 1h)` 退避，首次重试仍是 10 分钟。
- `watch` 分别使用单个准备 worker、单个上传 worker 和单个字幕 worker；任务准备完成后持久化为 `ready_to_upload`，投稿冷却期间仍可继续下载和翻译后续任务，实际上传保持严格串行。CC 字幕补交独立成队列，不占用上传 worker。
- `watch` 的 RSS 轮询/yt-dlp 校对和备份/认证各跑一个独立任务，长时间的 yt-dlp 调用不会阻塞队列调度。裸频道 URL 会规范化到内容标签页，校对结果中的频道/播放列表条目不会被误当视频。
- 所有外部命令独占 Unix 进程组；超时会清理完整后代树和输出读取任务，避免 PyInstaller yt-dlp/Node 变成孤儿进程。
- 新投稿默认至少间隔 30 分钟；B站返回 `21566` 时全局冷却 6 小时并自动等待后重试，避免积压任务集中撞风控。

## CLI

```bash
y2b init
y2b config-check
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
y2b subtitle add BV1xxxxx
y2b subtitle all
y2b model list
y2b model set deepseek-v4-flash
y2b login youtube /path/to/cookies.txt
y2b login bilibili
y2b backup
y2b auth-check
```

TUI：`Tab` 切换任务/频道列表，`↑/↓` 选择，`n` 输入单个 YouTube URL 并选择 `direct` 或 `translated`，`r` 重试或恢复 dead-letter（对已投稿待补字幕的任务则是重新排队 CC 字幕补交），`p` 提示补 CC 字幕，`Space` 暂停，`a` 重做认证检查，`y`/`b` 导入 YouTube/Bilibili cookies，`q` 退出。手动 URL 在后台解析并入队，重复 URL 会定位已有任务；频道增删、模式切换和启停仅由 CLI 管理。

`y2b subtitle add <bvid>` 给指定已投稿视频补中文 CC 字幕；`y2b subtitle all` 遍历所有已投稿视频补字幕，已有中文字幕的自动跳过。字幕素材优先复用 `downloads/<video_id>/*.en-zh-CN.translated.json` 缓存，缺失时重新下载英文字幕、分句并调 Pi 翻译；提交走 B站审核（非即时生效）。

频道模式只是新任务的默认值。任务入队后会固化当时的模式，后续 `channels set-mode` 不会改写旧任务。`video_id` 全局唯一，同一视频不会二次入队或二次投稿。`y2b run` 的 `--mode` 默认为 `translated`；`channels add` 和 `jobs add` 要求显式指定 `--mode`。

## 荒野乱斗词库审计

`scripts/audit_brawl_glossary.py` 从国际服客户端镜像的 `localization/texts`、`localization/cn`、`localization/texts_patch` 以及游戏逻辑 TID 引用生成英文/简中术语集。完整说明句、占位模板、纯数字、一词多译项目、商店/通知/教程 UI 不会进入强制词库。提取器按引用行的 `Disabled` 字段把术语分为当前 `active` 和历史 `legacy`，不依靠名称或发布时间猜测状态。

```bash
python3 scripts/audit_brawl_glossary.py \
  --server root@157.230.241.109 \
  --models deepseek-v4-flash \
  --output /tmp/y2b-brawl-glossary-audit.json

# 使用已有模型错误并集重建分层生产词库
python3 scripts/audit_brawl_glossary.py \
  --models '' \
  --output /tmp/y2b-brawl-glossary-extract.json \
  --production-from pi/brawl-stars-glossary.json \
  --production-output pi/brawl-stars-glossary.json
```

脚本固定使用 `deepseek/deepseek-v4-flash` 和 `thinking=off`，通过服务器的 `/etc/y2b/y2b.env` 注入凭据，默认使用不含任何答案的 `pi/audit-policy.json`；extension 在此模式下不会加载生产词库，避免污染模型能力测试。脚本支持 `--terms-file` 重用提取结果、`--resume` 断点续跑和 `--shard-index/--shard-count` 分片；单词调用超时或失败会按错误计入并继续。

生产运行时的四层优先级为：`policy.json` 人工 `curated` > 动态数值 `patterns` > 当前 `active` > 历史 `legacy`。Pi 每次只接收输入中精确命中的词；`legacy` 不会常驻上下文，但视频明确提到旧地图时仍使用当年的游戏内官译。被规则折叠或来源排除的模型错误保存在 `omitted` 供下次重建，不参与运行时注入。

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

# 3. 通过安全通道放置 DeepSeek 凭据；文件只包含 DEEPSEEK_API_KEY=<value>
ssh root@157.230.241.109 'install -d -o root -g root -m 700 /etc/y2b'
scp /path/to/y2b.env root@157.230.241.109:/tmp/y2b.env
ssh root@157.230.241.109 'install -o root -g root -m 600 /tmp/y2b.env /etc/y2b/y2b.env && rm /tmp/y2b.env'

# 4. 上传二进制和运行资源
scp target/x86_64-unknown-linux-musl/release/y2b root@157.230.241.109:/tmp/y2b
scp -r pi config.example.toml deploy Cargo.lock root@157.230.241.109:/tmp/y2b-release/
ssh root@157.230.241.109 'bash /tmp/y2b-release/deploy/deploy-app.sh /tmp/y2b'
```

`deploy-app.sh` 会先用新二进制执行只读 `config-check`，再停服务、跑迁移和基线，最后 `restart`——迁移需要独占数据库，
且 `enable --now` 对已在运行的服务是空操作，会出现「装上了但跑的还是旧二进制」。
部署前确认没有投稿在途（`y2b jobs list` 无 `uploading`），避免中断真实上传。

需要另行放置且权限为 `0600`：

- `/etc/y2b/y2b.env`（`root:root`；仓库和 systemd unit 只引用路径，不保存 Key）
- `/root/.pi/agent/auth.json`
- `/var/lib/y2b/youtube_cookies.txt`
- `/var/lib/y2b/bilibili_cookies.json`

systemd 资源限制：`MemoryHigh=1200M`、`MemoryMax=1600M`、`MemorySwapMax=1G`、`TasksMax=256`。查看状态：

```bash
systemctl status y2b-watch
journalctl -u y2b-watch -f
systemctl show y2b-watch -p MemoryCurrent -p MemoryPeak -p MemorySwapCurrent
```

## 恢复

1. 在空服务器运行 `bootstrap-server.sh`。
2. 恢复 `/etc/y2b/config.toml`、三份认证文件、`/opt/y2b/fonts`、Pi extension/policy、`audit-policy.json` 和 `brawl-stars-glossary.json`。
3. 从 `/var/lib/y2b/backups/daily` 或 `weekly` 选择数据库，执行 `deploy/restore.sh BACKUP.db`。
4. 部署静态 `y2b`，执行 `y2b check --write-baseline`，再启动 `y2b-watch.service`。打开数据库时会自动升级到 v10（新增超时长视频判定表，幂等）。旧频道和任务的模式均为 `translated`。升级前就停在待补字幕状态的任务会各获得一次自动补交机会；升级前的 `retry_wait` 行沿用固定 10 分钟退避。
5. SQLite 保存完整任务队列；`queued`/`retry_wait`/`processing` 会在重启后恢复，任务模式和追加目标 BV 不丢失，`dead_letter` 从 TUI 或 CLI 恢复后会重新下载。

在线备份每 6 小时执行一次：保留 4 个小时备份、7 个日备份和 4 个周备份。数据库迁移前应先执行 `y2b backup`。

## 上线验收

```bash
y2b check --write-baseline
y2b run '用户提供的 direct 验收 URL' --mode direct
y2b run '用户提供的带英文字幕 URL' --mode translated
y2b jobs show JOB_ID
```

真实投稿会改变 Bilibili 外部状态，只在提供测试 BV 和未搬运视频后执行。
