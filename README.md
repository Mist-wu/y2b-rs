<div align="center">

# y2b-rs

**监控 YouTube 频道 → 下载 → Pi 分句翻译 → 投稿 Bilibili → 自动补中文 CC 字幕**

单二进制 Rust CLI/TUI，SQLite 持久化队列，全流程无人值守。

[![Rust](https://img.shields.io/badge/Rust-2024_edition-000?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Target](https://img.shields.io/badge/deploy-Ubuntu%2022.04%20musl-E95420?logo=ubuntu&logoColor=white)](#部署)

</div>

---

## 两种搬运模式

| 模式 | 流程 | 字幕 |
| --- | --- | --- |
| `direct` | 并行下载视频 + 调用 Pi 生成中文标题／动态／标签 | 不下载、不分句 |
| `translated` | 英文字幕 → Pi 分句 → Pi 翻译 → 上传原片 → 提交中文 CC | B 站软字幕，观众可开关，不走压制 |

频道模式只是**新任务的默认值**：任务入队即固化模式，之后 `channels set-mode` 不改写旧任务。`video_id` 全局唯一，同一视频不会二次入队或二次投稿。

`translated` 在 CLI 中表示“翻译后补交软 CC”，不是“翻译压制”：项目已移除生成硬字幕成片的路径，上传的始终是原片。

频道优先级分为 `normal` 和 `priority`。优先频道拥有独立的 60 秒 RSS 轮询和 60 秒 Data API 调度，并在候选闸门、准备队列和上传队列中排在全部普通频道之前；同一优先级内部仍按发现时间 FIFO。已经开始执行的任务不会被中断。

## 快速开始

```bash
y2b init                 # 生成配置
y2b config-check         # 校验配置
y2b login youtube /path/to/cookies.txt
y2b login bilibili
y2b channels add 'https://www.youtube.com/@channel/videos' --mode translated
y2b check --write-baseline
y2b watch                # 常驻；或 y2b tui 交互查看
```

## CLI

```bash
# 频道
y2b channels add <URL> --mode direct|translated   # 必须显式指定 --mode
y2b channels list | set-mode <ID> <MODE> | set-priority <ID> normal|priority
y2b channels enable <ID> | disable <ID> | sync

# 任务
y2b jobs add <URL> --mode direct|translated       # 必须显式指定 --mode
y2b run <URL> [--mode translated]                 # 单次跑完，默认上传原片并补中文 CC
y2b jobs list [N] | show <JOB_ID> | retry <JOB_ID> | reconcile-upload <JOB_ID>

# 字幕 / 模型 / 运维
y2b subtitle add <BVID>   # 给指定已投稿视频补中文 CC
y2b subtitle all          # 遍历所有已投稿视频，已有中文字幕自动跳过
y2b model list | set deepseek-v4-flash
y2b backup | auth-check | check --write-baseline
```

`subtitle` 优先复用 `downloads/<video_id>/*.en-zh-CN.translated.json` 缓存，缺失时重新下载英文字幕、分句并调 Pi 翻译；提交走 B 站审核，非即时生效。

### TUI

| 键 | 作用 | 键 | 作用 |
| --- | --- | --- | --- |
| `Tab` | 切换任务／频道列表 | `p` | 提示补 CC 字幕 |
| `↑` `↓` | 选择 | `Space` | 暂停 |
| `n` | 输入单个 URL 并选模式 | `a` | 重做认证检查 |
| `r` | 重试／恢复 dead-letter<br>（待补字幕任务则重排字幕队列） | `y` `b` | 导入 YouTube／Bilibili cookies |
| `q` | 退出 | | |

手动 URL 后台解析入队，重复 URL 会定位到已有任务。频道增删、模式切换和启停仅由 CLI 管理。

## 工作流程

<details>
<summary><b>发现与筛选</b></summary>

- 优先频道每 60 秒分别检查 RSS 和 Data API；独立 RSS 循环每秒检查到期时间，不与普通频道争抢探测名额。普通频道继续使用预测 Data API 与限额 RSS 探针。此保证从视频出现在 YouTube RSS/API 时开始计算，YouTube 自身的数据传播延迟不在服务控制范围内。
- RSS 失败先短退避重试 3 次；yt-dlp 回退受单频道冷却和「全局 10 分钟最多 3 次」熔断限制，避免暂态故障演变成请求风暴。回退名额优先给从未尝试或最久未尝试的普通频道，RSS 全面异常时排在后面的频道不会被饿死。
- 直播回放（`was_live`）按普通视频搬运。直播中（`is_live`）、预约（`is_upcoming`）、回放生成中（`post_live`）不入队，每 30 分钟复查，回放就绪后自动搬运。
- 超过 `youtube.max_duration_seconds`（默认 2 小时）直接跳过并持久化判定，不重复请求；放宽上限后自动重查。已入队任务若发现超时长直接进 `dead_letter`，不消耗重试次数。
- 只自动搬运策略生效后开播的回放：首次运行把 `live_replay.enqueue_after` 游标设为当时时间，更早的历史回放不会被扫进队列；手动 `jobs add` 不受限。

</details>

<details>
<summary><b>处理与元数据</b></summary>

- 单个 `translated` 任务内部并行下载视频和处理字幕。下载限制 60fps、约 2,073,600 像素，优先 AVC/AAC；遇到不可用 HLS 分片立即失败并清理残片，成片时长与源元数据相差超过 3 秒时拒绝投稿。
- 元数据按每视频一次无状态 `publish_metadata` Pi 请求生成；字幕模式在预算内传入完整双语字幕，超限时保留首尾并均匀采样。结果持久化，重试或重启不重复调用 Pi。
- 标题和动态里的 hashtag、链接、emoji 在解析时确定性剥掉（YouTube 原标题常带 `#bs #brawlstars`，AI 会照抄）；整条标题都是话题时（如原标题就叫 `#sync`）退让为保词去标记，只有链接这类无词可留的输入才交回 AI 重写。落库旧元数据校验不过时先清洗再复用，清洗后仍不合格才重新生成，不会拿同一份坏元数据失败到死信。其余不合格情况带原因反馈重试，绝不用英文原标题或固定动态投稿。
- CC 字幕：投稿 attempt 成功、任务转入 `uploaded_original_pending_subtitle` 和首次字幕检查时间在同一事务写入；正常翻译稿及“原视频暂缺字幕”的直传稿都统一等待 90 秒后再检查。稿件仍在 B 站处理中（`-404`）按较短基数退避，其余失败按 `min(90 × 2^n, 1h)` 退避，最多 16 次。每次先识别 B 站已有的 `zh`/`zh-CN` 等中文变体；本地素材暂缺时继续等待平台自动字幕，达上限才留给 `subtitle add/all` 手动补。提交前按标点拆分超过 B 站单条 100 字符／300 字节限制的 cue，按字符比例保持原时间轴和全文内容。

</details>

<details>
<summary><b>投稿参数</b></summary>

- 固定手机游戏分区 `tid=172`、自制 `copyright=1` 并允许转载，不使用 Bilibili 转载来源字段。
- 标签始终以「荒野乱斗」开头；简介按清理 hashtag 后的原标题、YouTube 来源、原作者和工具地址确定性生成。
- 所有新投稿都下载 yt-dlp 选定的 YouTube 原封面，转 JPEG 后经 biliup `--cover` 上传；封面失败时任务重试，不会无封面投稿。

</details>

<details>
<summary><b>队列与容错</b></summary>

- SQLite 持久化频道、任务、阶段、峰值 RSS、Pi token/cost 和认证状态。普通故障连续失败 5 次进 `dead_letter` 并删除大型视频；失败间按 `min(5min × 2^n, 1h)` 退避，首次重试仍是 10 分钟。直播／预约／回放生成中不消耗失败次数。
- `watch` 使用单个准备 worker + 单个上传 worker + 单个字幕 worker。任务准备完成后持久化为 `ready_to_upload`，投稿冷却期间仍可继续下载和翻译后续任务，实际上传严格串行。最终领取任务的写事务会同时复核投稿冷却和 `live_once` 独占 hold，避免旁路暂停与普通投稿抢跑；hold 只允许 owner 自己释放，并保留期间写入的更晚平台冷却。CC 字幕补交独立成队列，不占用上传 worker。
- 每次真正投稿先持久化 attempt；中断且无法确认结果时进入 `upload_uncertain`，禁止自动重投。`jobs reconcile-upload` 会查询创作中心辅助人工核对。

> [!CAUTION]
> “创作中心唯一同名即确认 BVID”只是**弱证据**：标题可能重复，也可能命中另一条投稿，误关联后会污染字幕和任务状态。这是已知的数据一致性风险；在更强的投稿关联校验完成前，只能在人工同时核对标题、时间和稿件内容后执行 `jobs reconcile-upload`，不能把唯一同名当作自动确认依据。

- RSS 轮询／yt-dlp 校对与备份／认证各跑独立任务，长时间 yt-dlp 调用不阻塞队列调度。裸频道 URL 规范化到内容标签页，校对结果中的频道／播放列表条目不会被误当视频。
- 所有外部命令独占 Unix 进程组；超时或并行分支提前取消都会清理完整后代树，避免 PyInstaller yt-dlp／Node 变成孤儿进程继续写临时分片。
- 新投稿默认至少间隔 30 分钟；B 站返回 `21566` 时全局冷却 6 小时并自动等待后重试。

</details>

## Pi 集成

Pi 调用固定为 `deepseek` + `thinking=off`：分句、投稿元数据和词库审计使用 `deepseek-v4-flash`，长列表逐条翻译使用对齐更稳定的 `deepseek-v4-pro`。每次调用 `--no-session --no-tools`，只加载 `pi/y2b-extension.ts`。配置加载和部署预检会拒绝其他 provider／model／thinking，避免任务间漂移和大 thinking 流式输出带来的成本与 OOM。

批处理支持 `adaptive` 和 `whole_video`：按 256k 上下文、200k 安全阈值估算输入输出，阈值内整条视频只调用一次分句和一次翻译，超限按 token 拆批。自适应分句携带前后 12 条上下文，并在 Pi 返回的自然分句边界衔接批次。

### 荒野乱斗词库

`pi/brawl-stars-glossary.json` 来自国际服客户端英文／简中本地化资源，extension 每次只注入输入中实际出现的词条。运行时四层优先级：

> `policy.json` 人工 `curated` › 动态数值 `patterns` › 当前 `active` › 历史 `legacy`

`legacy` 不常驻上下文，但视频明确提到旧地图时仍使用当年的游戏内官译。被规则折叠或来源排除的模型错误存在 `omitted` 供下次重建，不参与运行时注入。JSON 内的 `audit` 字段只是词库生成时的历史模型溯源，不参与运行时模型选择。

<details>
<summary><b>词库审计脚本</b></summary>

`scripts/audit_brawl_glossary.py` 从客户端镜像的 `localization/texts`、`localization/cn`、`localization/texts_patch` 及游戏逻辑 TID 引用生成术语集。完整说明句、占位模板、纯数字、一词多译、商店／通知／教程 UI 不进入强制词库；按引用行的 `Disabled` 字段区分 `active` 与 `legacy`，不靠名称或时间猜测。

```bash
# 审计模型能力
python3 scripts/audit_brawl_glossary.py \
  --server azureuser@20.89.60.23 \
  --models deepseek-v4-flash \
  --output /tmp/y2b-brawl-glossary-audit.json

# 用已有模型错误并集重建分层生产词库
python3 scripts/audit_brawl_glossary.py \
  --models '' \
  --output /tmp/y2b-brawl-glossary-extract.json \
  --production-from pi/brawl-stars-glossary.json \
  --production-output pi/brawl-stars-glossary.json
```

脚本固定 `deepseek/deepseek-v4-flash` + `thinking=off`，与生产服务一样只从服务器 `/var/lib/y2b/pi-agent/auth.json` 读取 DeepSeek 凭据，默认使用不含答案的 `pi/audit-policy.json`；`--server` 不是 `root@` 时远程命令自动加 `sudo -n`。审计模式下 extension 不加载生产词库，避免污染能力测试。支持 `--terms-file` 复用提取结果、`--resume` 断点续跑、`--shard-index/--shard-count` 分片；单词超时或失败计入错误并继续。

</details>

## 强失败质量门禁

提交前统一运行 `npm run check`；它同时执行 TypeScript 类型检查和 `pi/y2b-extension.ts` 的真实 import 解析。完整的本地门禁与 CI 一致：

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
npm ci
npm run check
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 -m unittest discover -s deploy/tests -p 'test_*.py'
python3 -m unittest discover -s .github/workflows/tests -p 'test_*.py'
python3 -m compileall -q scripts deploy
shellcheck deploy/*.sh
bash -n deploy/*.sh
npm audit
cargo audit
gitleaks git --gitleaks-ignore-path .github/workflows/gitleaksignore .
```

这些检查都是**强失败门禁**：任一命令非零退出就阻断合并和发布，不得用 `|| true`、跳过测试或宽泛 allowlist 降级。Gitleaks 扫描完整历史，仓库中的测试假 Key 只按唯一 fingerprint 精确放行。

## 部署

目标：Ubuntu 22.04 x86_64，`azureuser@20.89.60.23`。Azure 镜像禁止 root 直接 SSH，特权操作走 `azureuser` 免密 `sudo`。服务器不编译 Rust 或 FFmpeg。

```bash
# 1. 服务器：2 GiB swap、预编译依赖和自动 PO Token Provider
scp deploy/bootstrap-server.sh deploy/install-ytdlp-pot-provider.sh azureuser@20.89.60.23:/tmp/
ssh azureuser@20.89.60.23 'sudo bash /tmp/bootstrap-server.sh'

# 2. Mac：静态交叉编译
brew install zig
cargo install cargo-zigbuild --locked
rustup target add x86_64-unknown-linux-musl
cargo zigbuild --release --target x86_64-unknown-linux-musl

# 3. 按 commit 建独立传输目录，上传二进制、运行资源和安全换钥工具
release_id=$(git rev-parse --short=12 HEAD)
scp target/x86_64-unknown-linux-musl/release/y2b azureuser@20.89.60.23:/tmp/y2b-$release_id
ssh azureuser@20.89.60.23 "install -d /tmp/y2b-release-$release_id"
scp -r pi config.example.toml deploy Cargo.lock azureuser@20.89.60.23:/tmp/y2b-release-$release_id/
ssh azureuser@20.89.60.23 "sudo install -o root -g root -m 755 /tmp/y2b-release-$release_id/deploy/y2b-set-deepseek-key.py /usr/local/sbin/y2b-set-deepseek-key"

# 4. 在 Mac 终端输入新 Key；输入不回显，Key 只经 stdin 发送且不会出现在命令历史
(read -r -s 'Y2B_DEEPSEEK_KEY?请输入新的 DeepSeek API Key: '; printf '\n'; printf '%s' "$Y2B_DEEPSEEK_KEY" | ssh azureuser@20.89.60.23 'sudo /usr/local/sbin/y2b-set-deepseek-key')

# 5. 部署应用
ssh azureuser@20.89.60.23 "sudo bash /tmp/y2b-release-$release_id/deploy/deploy-app.sh /tmp/y2b-$release_id"
```

换钥工具会原子写入专用认证文件，并删除 `/etc/y2b/y2b.env` 和全局 Pi 认证中的旧 DeepSeek 条目；它不会打印明文 Key。部署前可用 `sudo y2b-set-deepseek-key --check` 只读检查单一路径约束。

> [!IMPORTANT]
> 第 3 步不能只拷二进制。`/opt/y2b/pi/` 下的 `y2b-extension.ts`、`policy.json`、`audit-policy.json`、`brawl-stars-glossary.json` 由 `deploy-app.sh` 一并安装，缺任何一个都要等第一次真正调用 Pi 才暴露（`config-check` 只校验配置本身）。换机或手工搬运后必须跑一次 `deploy-app.sh` 补齐。

> [!WARNING]
> 部署前确认没有投稿在途（`y2b jobs list` 无 `uploading`），避免中断真实上传。

### maintenance hold 与 release 边界

这里的 **maintenance hold** 是运维窗口约定：确认没有 `uploading`／运行中的 stage，并保证同一时间只有一个部署或恢复进程后才能动生产文件。它不是 `live_once` 的投稿 hold；后者只挡上传 worker，准备和字幕 worker 仍会运行，不能代替完整维护窗口。

按 commit 隔离的 `/tmp/y2b-release-$release_id` 是不可混用的 release 输入，但应用 release 当前不是原子切换：`deploy-app.sh` 仍把二进制和 Pi 资源安装到固定路径。因此必须保留上一版的完整文件集，任何安装失败都停止后续迁移／重启，不能只回退二进制而留下新旧资源混搭。脚本的 `config-check`、extension 解析、凭据检查、空闲检查和 SQLite 依赖检查均为强失败门禁。

真正使用**原子 release** 的是 PO Token Provider：安装器先写入版本化 `releases/<version>`，校验完整后再以临时符号链接和单次 `mv` 切换 `current`。失败时旧 `current` 仍可用，不会暴露半份 provider。

需另行放置且权限 `0600` 的文件：

| 路径 | 说明 |
| --- | --- |
| `/var/lib/y2b/pi-agent/auth.json` | `root:root`、`0600`；y2b 唯一的 DeepSeek Key 路径，只含 `deepseek` provider |
| `/etc/y2b/y2b.env` | `root:root`、`0600`；可保存 YouTube 等环境变量，禁止保存 `DEEPSEEK_API_KEY` |
| `/root/.pi/agent/auth.json` | 全局 Pi 认证；可保留其他 provider，禁止保存 `deepseek` 条目 |
| `/var/lib/y2b/youtube_cookies.txt` | YouTube cookies |
| `/var/lib/y2b/bilibili_cookies.json` | Bilibili cookies |

systemd 资源限制：`MemoryHigh=1200M`、`MemoryMax=1600M`、`MemorySwapMax=1G`、`TasksMax=256`。

```bash
systemctl status y2b-watch
journalctl -u y2b-watch -f
systemctl show y2b-watch -p MemoryCurrent -p MemoryPeak -p MemorySwapCurrent

# YouTube 自动字幕受 PO Token 限制时，确认 provider 已由 yt-dlp 发现
yt-dlp -v --simulate 'https://www.youtube.com/watch?v=VIDEO_ID' 2>&1 \
  | grep 'PO Token Providers'
```

`deploy/install-ytdlp-pot-provider.sh` 固定并校验 `bgutil-ytdlp-pot-provider`
的 provider 源码与插件版本，使用版本化目录和原子 `current` 切换，并以按需启动的
`script-node` 模式运行；它不监听网络端口，也不需要重启 `y2b-watch.service`。y2b
的 systemd 单元设置 `HOME=/root`，因此每次新启动的 yt-dlp 子进程会从
`/root/bgutil-ytdlp-pot-provider` 自动发现 provider。

## 备份与恢复

在线备份每 6 小时一次，保留 4 个小时备份、7 个日备份、4 个周备份。数据库迁移前先执行 `y2b backup`。恢复必须使用与备份兼容的完整 release，不能只替换数据库或二进制。

1. 记录备份时间、来源 schema 和对应 release；进入 maintenance hold，确认没有 `uploading`／运行中的 stage，并保留当前数据库与整套应用文件作为回退点。
2. 空服务器先运行 `bootstrap-server.sh`，再恢复 `/etc/y2b/config.toml`、`/etc/y2b/y2b.env` 和两个 cookies 文件，并用 `y2b-set-deepseek-key` 重新注入 DeepSeek Key。Pi 资源随应用 release 安装，无需单独备份。
3. 先用 `deploy-app.sh` 安装与当前代码匹配的完整 release，再从 `backups/daily` 或 `weekly` 选择数据库并执行 `deploy/restore.sh BACKUP.db`；不要手工覆盖在线 `state.db`，也不要并行运行部署和恢复。
4. `restore.sh` 在停服务前完成强预检：把备份复制到数据库所在文件系统的暂存路径，要求 SQLite `integrity_check` 精确返回单独一行 `ok`，同时检查关键表和可读 schema。预检通过后才记录原 service 状态、停服务、保存旧库，并以同文件系统 `mv` 原子替换数据库和清理旧 WAL/SHM。
5. 原服务先前为 active 时，脚本启动它并等待幂等迁移到 schema v19，再复查数据库完整性和 service 状态。任一步骤失败，EXIT trap 都尝试恢复旧数据库及原 service 状态并返回非零；成功后仍要核对 schema v19、队列数量、最近备份和 `upload_uncertain`。不确定投稿只能人工核对，不能因恢复而自动重投。
6. SQLite 保存完整队列：准备和 CC 字幕任务通过原子领取、租约与心跳避免多进程重复执行；过期租约在重启后恢复。任务模式和追加目标 BV 不丢失，`dead_letter` 可从 TUI 或 CLI 安全恢复。旧频道和任务模式均为 `translated`；升级前停在待补字幕的任务各获一次自动补交机会，旧 `retry_wait` 行沿用固定 10 分钟退避。

## 上线验收

```bash
y2b check --write-baseline
y2b run '用户提供的 direct 验收 URL' --mode direct
y2b run '用户提供的带英文字幕 URL' --mode translated
y2b jobs show JOB_ID
```

> [!CAUTION]
> 真实投稿会改变 Bilibili 外部状态，只在提供测试 BV 和未搬运视频后执行。
