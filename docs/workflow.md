# 工作流程与 Pi 集成

[← 返回 README](../README.md)

## 发现与筛选

- 优先频道每 60 秒分别检查 RSS 和 Data API；独立 RSS 循环每秒检查到期时间，不与普通频道争抢探测名额。普通频道继续使用预测 Data API 与限额 RSS 探针。此保证从视频出现在 YouTube RSS/API 时开始计算，YouTube 自身的数据传播延迟不在服务控制范围内。
- 启用 WebSub 后（见 `DISCOVERY_REARCHITECTURE.md`），新视频由 YouTube hub 主动推送到 `callback_base_url`；租约有效的频道（含优先频道）Data API 与 RSS 都退到每 `websub.data_api_poll_minutes`（默认 30 分钟）兜底一次，租约过期自动恢复原调度。
- RSS 失败先短退避重试 3 次；yt-dlp 回退受单频道冷却和「全局 10 分钟最多 3 次」熔断限制，避免暂态故障演变成请求风暴。回退名额优先给从未尝试或最久未尝试的普通频道，RSS 全面异常时排在后面的频道不会被饿死。
- 直播回放（`was_live`）按普通视频搬运。直播中（`is_live`）、预约（`is_upcoming`）、回放生成中（`post_live`）不入队，每 30 分钟复查，回放就绪后自动搬运。
- 超过 `youtube.max_duration_seconds`（默认 2 小时）直接跳过并持久化判定，不重复请求；放宽上限后自动重查。已入队任务若发现超时长直接进 `dead_letter`，不消耗重试次数。
- 只自动搬运策略生效后开播的回放：首次运行把 `live_replay.enqueue_after` 游标设为当时时间，更早的历史回放不会被扫进队列；手动 `jobs add` 不受限。

## 处理与元数据

- 单个 `translated` 任务内部并行下载视频和处理字幕。下载限制 60fps、约 2,073,600 像素，优先 AVC/AAC；遇到不可用 HLS 分片立即失败并清理残片，成片时长与源元数据相差超过 3 秒时拒绝投稿。
- 元数据按每视频一次无状态 `publish_metadata` Pi 请求生成；字幕模式在预算内传入完整双语字幕，超限时保留首尾并均匀采样。结果持久化，重试或重启不重复调用 Pi。
- 标题和动态里的 hashtag、链接、emoji 在解析时确定性剥掉（YouTube 原标题常带 `#bs #brawlstars`，AI 会照抄）；整条标题都是话题时（如原标题就叫 `#sync`）退让为保词去标记，只有链接这类无词可留的输入才交回 AI 重写。落库旧元数据校验不过时先清洗再复用，清洗后仍不合格才重新生成，不会拿同一份坏元数据失败到死信。其余不合格情况带原因反馈重试，绝不用英文原标题或固定动态投稿。
- CC 字幕：投稿 attempt 成功、任务转入 `uploaded_original_pending_subtitle` 和首次字幕检查时间在同一事务写入；正常翻译稿及“原视频暂缺字幕”的直传稿都统一等待 90 秒后再检查。每次调用字幕提交接口前先持久化 `subtitle_attempts`；只有平台明确拒绝才允许新 attempt，响应丢失、超时或进程中断一律转为 `uncertain`，后续只查询平台已有 `zh` 字幕，确认存在后记为 `reconciled`，长期无法确认则转人工。稿件仍在 B 站处理中（`-404`）按较短基数退避，其余提交前失败按 `min(90 × 2^n, 1h)` 退避，最多 16 次。上游暂无英文字幕轨时走独立的稀疏计划：最多 8 次探测、间隔 5 分钟逐步拉长到 8 小时（累计约 16 小时，覆盖 YouTube ASR 延迟和直播回放次日出字幕），耗尽后按无字幕完成而不报异常；之后仍可用 `y2b subtitle add` 手动补交。提交前按标点拆分超过 B 站单条 100 字符／300 字节限制的 cue，按字符比例保持原时间轴和全文内容。

## 投稿参数

- 固定手机游戏分区 `tid=172`、自制 `copyright=1` 并允许转载，不使用 Bilibili 转载来源字段。
- 标签始终以「荒野乱斗」开头；简介按清理 hashtag 后的原标题、YouTube 来源、原作者和工具地址确定性生成。
- 所有新投稿都下载 yt-dlp 选定的 YouTube 原封面，转 JPEG 后经 biliup `--cover` 上传；封面失败时任务重试，不会无封面投稿。

## 队列与容错

- SQLite 持久化频道、任务、阶段、峰值 RSS、Pi token/cost 和认证状态。普通故障连续失败 5 次进 `dead_letter` 并删除大型视频；失败间按 `min(5min × 2^n, 1h)` 退避，首次重试仍是 10 分钟。直播／预约／回放生成中不消耗失败次数。
- `watch` 使用单个准备 worker + 单个上传 worker + 单个字幕 worker。任务准备完成后持久化为 `ready_to_upload`，投稿冷却期间仍可继续下载和翻译后续任务，实际上传严格串行。最终领取任务的写事务会同时复核投稿冷却和维护锁。CC 字幕补交独立成队列，不占用上传 worker。
- 每次真正投稿先持久化 attempt；中断且无法确认结果时进入 `upload_uncertain`，禁止自动重投。`jobs reconcile-upload` 查询创作中心后，只有标题匹配、稿件发布时间晚于本次 attempt 开始时间且 BVID 未被其他任务占用时才会确认；缺少任一证据都会保持不确定态并要求人工提供 BVID。投稿确实没落地时（例如 biliup 传封面时网络中断）创作中心永远查不到同名稿件，而 `upload_attempts` 里的 `uncertain` 行是维护窗口的永久 blocker，会一直挡住部署；此时用 `jobs reconcile-upload <JOB_ID> --not-published` 显式声明未落地，它仍会先查创作中心，只要存在任何同名稿件就拒绝执行，确认没有才结算 attempt 并把任务退回 `ready_to_upload` 重投。数据库同时用部分唯一索引保证一个非空 BVID 只能归属一个任务。

- RSS 轮询／yt-dlp 校对与备份／认证各跑独立任务，长时间 yt-dlp 调用不阻塞队列调度。裸频道 URL 规范化到内容标签页，校对结果中的频道／播放列表条目不会被误当视频。
- 所有外部命令独占 Unix 进程组；超时或并行分支提前取消都会清理完整后代树，避免 PyInstaller yt-dlp／Node 变成孤儿进程继续写临时分片。
- 新投稿默认至少间隔 30 分钟；B 站返回 `21566` 时全局冷却 6 小时并自动等待后重试。

## Pi 集成

Pi 调用固定为 `deepseek` + `thinking=off`：分句、投稿元数据、长列表翻译和词库审计统一使用 `deepseek-flash`（V4.1 Flash）；旧的 `deepseek-v4-flash` 已下线，`deepseek-v4-pro` 自 2026-09-14 12:00 起也会被路由到 V4.1 Flash，`translation_model` 字段保留以便 V4.1 Pro 上线后单独切换翻译模型。每次调用 `--no-session --no-tools`，只加载 `pi/y2b-extension.ts`。配置加载和部署预检会拒绝其他 provider／model／thinking，避免任务间漂移和大 thinking 流式输出带来的成本与 OOM。

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
  --server azureuser@<server-ip> \
  --models deepseek-flash \
  --output /tmp/y2b-brawl-glossary-audit.json

# 用已有模型错误并集重建分层生产词库
python3 scripts/audit_brawl_glossary.py \
  --models '' \
  --output /tmp/y2b-brawl-glossary-extract.json \
  --production-from pi/brawl-stars-glossary.json \
  --production-output pi/brawl-stars-glossary.json
```

脚本固定 `deepseek/deepseek-flash` + `thinking=off`，与生产服务一样只从服务器 `/var/lib/y2b/pi-agent/auth.json` 读取 DeepSeek 凭据，默认使用不含答案的 `pi/audit-policy.json`；`--server` 不是 `root@` 时远程命令自动加 `sudo -n`。审计模式下 extension 不加载生产词库，避免污染能力测试。支持 `--terms-file` 复用提取结果、`--resume` 断点续跑、`--shard-index/--shard-count` 分片；单词超时或失败计入错误并继续。

</details>
