# y2b-rs 频道发现架构重建（历史设计文档）

> [!NOTE]
> 本文归档已经合入主线的 Phase 0–4 发现架构设计。下文的 v11–v15 是各阶段当时的迁移快照，不代表当前数据库终点；当前 schema 为 **v22**，v16–v22 的 AI 审计、频道优先级、队列租约、投稿 attempt、BVID 唯一性、CC 字幕 attempt 状态机和全局维护锁改造不在本文范围内。部署、恢复和质量门禁以 [README](README.md) 为准。

所有迁移都是幂等、向前追加式迁移；本文保留原阶段说明用于追溯，不再作为独立 feature 分支的上线手册。

## Phase 0：止血与持久化调度

- 修复频道状态早返回漏洞。每次 `poll_channel` 只有一个状态提交点；RSS 成功、RSS 解析失败、RSS 失败但 yt-dlp 回退成功、RSS 与回退都失败，都会更新 `last_checked_at`，并正确清除或写入 `last_error`。
- RSS 请求按错误类型处理：
  - `403`、`404` 不重试；
  - `429` 读取 `Retry-After`，并把精确时间写入下次调度；
  - `5xx`、超时和连接错误最多重试 2 次，使用指数退避和 `0.5–1.5` 随机抖动；
  - 其他错误不做无意义重试。
- 轮询只读取已经到期的启用频道，失败退避跨进程重启保留。
- RSS 全局熔断使用整个频道集合的 10 分钟滑动窗口；样本不少于 8 且失败率大于 60% 时熔断 10 分钟，期间最多放行 2 个探针。

数据库迁移 v11：

- `channels.next_poll_at TEXT`
- `channels.consecutive_failures INTEGER NOT NULL DEFAULT 0`
- 新表 `discovery_state(key TEXT PRIMARY KEY, value TEXT NOT NULL)`
- `discovery_state.rss_circuit_open_until` 保存 RSS 熔断截止时间

## Phase 1：统一候选闸门

- RSS、yt-dlp 校对以及后续数据源只负责发现视频并写候选，不再在发现路径逐条拉取视频元数据。
- `watch` 增加独立 gate worker，处理到期候选并统一执行：查重、元数据获取、直播状态、时长上限、历史直播回放、频道 baseline 和任务创建。
- 直播或暂时性元数据错误进入 `deferred`，默认 30 分钟后复查。
- 超长视频和历史回放进入持久化 `rejected`；合格候选与任务创建在同一事务内完成并进入 `promoted`。
- 删除纯内存 `deferred_videos`；服务重启不会丢失复查时间。

数据库迁移 v12 新增：

```sql
video_candidates(
  video_id TEXT PRIMARY KEY,
  channel_id INTEGER REFERENCES channels(id),
  url TEXT NOT NULL,
  title TEXT,
  published_at TEXT,
  source TEXT NOT NULL,
  discovered_at TEXT NOT NULL,
  gate_state TEXT NOT NULL,
  gate_attempts INTEGER NOT NULL DEFAULT 0,
  next_gate_at TEXT,
  last_error TEXT
)
```

`source` 可取 `websub | data_api | rss | ytdlp`；`gate_state` 可取 `pending | deferred | rejected | promoted`。

## Phase 2：YouTube Data API 主发现源

- API key 只从环境变量 `YOUTUBE_API_KEY` 读取，并通过 `X-goog-api-key` 请求头发送；不会写入配置文件或 query string。
- 没有 key 时只警告一次，不 panic，并继续使用 RSS 与 yt-dlp。
- `channels.list(part=contentDetails)` 每批最多 50 个频道，在进程启动时及之后每 24 小时刷新 uploads 播放列表。
- `playlistItems.list(part=snippet,contentDetails,maxResults=50)` 成为默认主发现源；Phase 4 改为按频道历史发布时间动态调度。
- `videos.list(part=snippet,contentDetails,liveStreamingDetails,status)` 每批最多 50 个视频，一次拿齐 gate 所需 part；API 不可用时才降级到 yt-dlp。
- 不使用高配额的 `search.list`。
- 支持 ISO-8601 duration、直播/预约/回放状态和直播实际开始时间。
- 日预算为 10,000 单位；配额在美国太平洋时间午夜重置。Phase 4 使用四级有序降级，收到 `403 quotaExceeded` 后将本地预算标记为耗尽。
- Data API 正常时，RSS 只做低频探针；周期深扫改为 Data API，每 24 小时一次。yt-dlp 校对只在 API 深扫失败或 API 整体不可用时兜底。

数据库迁移 v13：

- `channels.uploads_playlist_id TEXT`
- `discovery_state.quota_used_today`
- `discovery_state.quota_reset_at`
- `discovery_state.uploads_playlist_refreshed_at`
- `discovery_state.quota_warned_for_reset`

## Phase 3：WebSub 推送

- 功能默认关闭。关闭时不绑定端口、不启动订阅或续租任务。
- 启用后，回调服务与 `y2b watch` 同进程运行，并复用同一个 `Database` 的 `Arc<Mutex<Connection>>` 和 WAL 连接。
- 每个频道使用独立的随机回调路径，以及具有 32 字节随机熵、十六进制保存的 HMAC secret。
- 订阅目标固定为 `https://www.youtube.com/xml/feeds/videos.xml?channel_id=UC...`，hub 固定为 `https://pubsubhubbub.appspot.com/subscribe`，请求异步验证和 432,000 秒租约。
- GET 验证会校验 callback 与 topic，原样返回 `hub.challenge`，并记录租约截止时间。
- POST 通知限制为 128 KB；按 `X-Hub-Signature` 声明的算法校验签名，目前支持 hub 使用的 `sha1`。缺少、错误或不支持的签名会被拒绝。
- 通知只接受已知频道的 self topic；有效 Atom entry 只写入 `video_candidates` 并立即返回。`at:deleted-entry` 不会生成候选。
- 租约运行到 80% 时进入续租窗口；进程启动时立即扫描缺失、过期或待续租频道。
- WebSub 启用后，Data API 默认改为每 30 分钟兜底轮询，可独立配置。

数据库迁移 v14：

- `channels.websub_lease_expires_at TEXT`
- `channels.websub_secret TEXT`
- `channels.websub_callback_path TEXT`

## Phase 4：以最低发现延迟为目标

### 预测性 Data API 调度

- 调度粒度从“所有频道共用一个固定周期”改为“每频道独立、跨重启持久化”。每次成功或失败后写入 `channels.next_data_api_poll_at`。
- 每次计算都重新读取该频道已有任务的 `jobs.published_at`，按 `runtime.timezone` 转为本地时间，再按“星期几 + 小时”构建 7×24 分布；不会只在进程启动时计算一次。
- 历史样本达到默认 5 条后，每个星期几选择该日出现次数最多的小时桶；默认以桶中点为中心建立 120 分钟热窗：热窗内每 60 秒轮询，窗外每 30 分钟轮询。
- 冷区调度会在下一个热窗起点前提前唤醒，不能让一次 30/60 分钟睡眠跨过热窗开头。
- 历史少于 5 条的新频道固定每 5 分钟轮询，避免用不可靠分布制造假精度。
- 有效 WebSub 租约优先于预测模型，也优先于 `priority` 频道的固定 60 秒调度：该频道的 Data API 和 RSS 探针都变为每 `websub.data_api_poll_minutes` 分钟纯兜底；租约过期后自动恢复原调度。优先频道 60 秒 Data API 曾占日配额八成以上，这正是 WebSub 替代的部分。

`playlistItems.list` 按调用而不是返回条数计费，因此默认 `maxResults=50`。列表响应的 ETag 保存在频道行；后续发送 `If-None-Match`，HTTP 304 跳过 body 解析。配额在请求发出前记 1 单位，**304 同样计费**，预算不把它当成配额节省。

### 配额预算与四级降级

按 32 个频道、每频道每天 2 小时热窗估算：

| 项目 | 计算 | 单位/天 |
|---|---:|---:|
| 热窗 | `2h × 60 次/h × 32` | 3,840 |
| 冷区 | `22h × 2 次/h × 32` | 1,408 |
| 每频道主发现合计 | `164 × 32` | 5,248 |
| 每日 API 深扫 | `1 × 32` | 32 |
| `videos.list` gate | 约 30 个批次 | 约 30 |
| **预计总计** |  | **约 5,310 / 10,000** |
| **预留** | 重试、突发、playlist 刷新等 | **约 4,690** |

本地计数达到以下阈值时按顺序升级；每跨一级只记录一次 `warn`，状态按配额重置时间持久化：

1. `8,000`：停止当天的每日 API 深扫；
2. `8,500`：冷区间隔从 30 分钟延长为 60 分钟；
3. `9,000`：热窗宽度从 120 分钟收窄为 60 分钟，热窗内仍保持 60 秒；
4. `9,500`：Data API 主发现暂停到下次配额重置，整体回落到 RSS/yt-dlp。

### API 深扫、元数据和源语言

- 每 24 小时对每个启用频道执行一次 `playlistItems.list(maxResults=50)` 深扫；成功路径不再启动 yt-dlp。
- 无 key、配额完全不可用、403、网络错误或单频道 API 深扫失败时，才调用原有 yt-dlp reconcile；代码路径与 `reconcile_limit=30` 保留。
- `videos.list` 一次请求 `snippet,contentDetails,liveStreamingDetails,status`。
- `snippet.defaultAudioLanguage` 与 `translation.source_lang` 按基础语言标签比较，例如 `en-GB` 与 `en` 匹配。已知不匹配时写入候选标记并告警；默认不拦截。开启硬闸门后也只拒绝“已知不匹配”，字段缺失始终放行。
- 不使用 `contentDetails.caption`：它不能表示自动字幕是否存在；也不调用每次 50 单位的 `captions.list`。

数据库迁移 v15：

- `channels.next_data_api_poll_at TEXT`
- `channels.data_api_etag TEXT`
- `channels.websub_last_received_at TEXT`
- `video_candidates.source_language TEXT`
- `video_candidates.source_language_mismatch INTEGER NOT NULL DEFAULT 0`
- 索引 `idx_channels_next_data_api_poll(enabled, next_data_api_poll_at)`
- `discovery_state.quota_degradation_warned_for_reset` 保存当日已告警的最高降级级别

## 发布到上线的延迟分解

生产库典型 translated 任务在没有等待投稿窗口、且不走罕见 render 路径时，入队到投稿完成约 365 秒：

| 环节 | 平均耗时 | 说明 |
|---|---:|---|
| 发现：预测热窗 | 约 30 秒 | 60 秒轮询的平均等待；最坏约 60 秒 |
| 发现：预测冷区 | 约 15 分钟 | 30 分钟轮询的平均等待；最坏约 30 分钟 |
| 发现：WebSub | 秒级 | 取决于 YouTube hub 与网络，Data API 仅作 30 分钟兜底 |
| metadata | 6 秒 | gate 元数据 |
| subtitle_download | 9 秒 | 字幕下载 |
| video_download | 78 秒 | 视频下载，历史最大 761 秒 |
| segmentation | 48 秒 | 分句 |
| translation | 184 秒 | 历史最大 1,195 秒 |
| publish_metadata | 16 秒 | 投稿元数据 |
| upload | 24 秒 | Bilibili 上传 |
| **入队后典型合计** | **365 秒，约 6 分钟** | 不含发现等待 |
| render（罕见） | 平均 4,002 秒 | 仅 4 个历史样本，触发时额外增加 |
| Bilibili 投稿窗口 | 额外 0–30 分钟 | 若上一条刚投稿；`submit_interval_seconds=1800` 保持不变 |

因此在没有前序投稿占用窗口时：WebSub 通常约发布后 6 分钟上线；预测热窗约 6.5 分钟；冷区平均约 21 分钟。连续投稿仍必须服从 30 分钟保护，不能用提高流水线并发或缩短投稿间隔换发现速度。

## Phase 4 相关配置项

| 配置项 | 默认值 | 含义 |
|---|---:|---|
| `runtime.timezone` | `Asia/Shanghai` | 历史发布时间聚合使用的 IANA 本地时区 |
| `monitor.data_api_max_results` | `50` | 每次 `playlistItems.list` 返回上限，范围 1–50 |
| `monitor.prediction_window_minutes` | `120` | 预测热窗总宽度 |
| `monitor.prediction_hot_poll_seconds` | `60` | 热窗内轮询间隔 |
| `monitor.prediction_cold_poll_minutes` | `30` | 热窗外轮询间隔 |
| `monitor.prediction_fallback_poll_minutes` | `5` | 历史样本不足时的固定间隔 |
| `monitor.prediction_min_samples` | `5` | 启用预测所需最少历史样本 |
| `monitor.reconcile_hours` | `6` | API 深扫周期；API 不可用时也是 yt-dlp 兜底周期 |
| `monitor.reconcile_limit` | `30` | 保留的 yt-dlp reconcile 兜底读取上限 |
| `translation.source_lang` | `en` | 期望源语言；与地区变体按基础标签匹配 |
| `translation.enforce_source_lang` | `false` | 已知语言不匹配时是否硬拒绝；缺失值仍放行 |
| `websub.enabled` | `false` | 是否启动 WebSub 回调、订阅和续租；默认绝不监听端口 |
| `websub.bind_addr` | `127.0.0.1:8787` | 本地 HTTP 监听地址，通常只供 HTTPS 反向代理转发 |
| `websub.callback_base_url` | `""` | 公网 HTTPS 根地址；启用 WebSub 时必须设置为 `https://...` |
| `websub.data_api_poll_minutes` | `30` | WebSub 启用后的 Data API 兜底轮询周期（分钟） |

`YOUTUBE_API_KEY` 不是配置项，不能写进 `config.toml`。

## 如何启用 WebSub

WebSub 是秒级发现的最终通道。程序侧只需配置并运行 `y2b watch`；公网 HTTPS 与 Cloudflare 账户相关步骤必须由用户完成。Cloudflare 当前推荐从 Dashboard 创建 remotely-managed tunnel；官方步骤见 [Create a tunnel (dashboard)](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/)。

### 用户需要准备

- 一个已接入 Cloudflare DNS 的域名，以及一个专用主机名，例如 `push.example.com`；
- Cloudflare Dashboard 中创建 Tunnel、DNS route 和读取一次性 tunnel token 的权限；
- 在运行 y2b 的同一台主机安装并长期运行 `cloudflared` 的权限；
- 主机能出站连接 Cloudflare（受限网络需确认 TCP/UDP 7844）；不需要向公网开放 8787；
- YouTube Data API key 仍只放服务环境变量，不放 TOML、命令行或仓库。

### Cloudflare Tunnel 具体步骤

1. 在 Cloudflare Dashboard 打开 **Networking → Tunnels → Create a tunnel**，命名为 `y2b-websub`。
2. 选择目标主机的操作系统，按 Dashboard 给出的命令安装 connector。Linux 常见形式是：

   ```bash
   sudo cloudflared service install <TUNNEL_TOKEN>
   ```

   `<TUNNEL_TOKEN>` 是敏感凭据，只在目标主机执行，不写入仓库或日志。
3. Tunnel 显示 `Healthy` 后，在其 **Routes** 中选择 **Add route → Published application**：
   - Hostname：`push.example.com`；
   - Service URL：`http://127.0.0.1:8787`；
   - 如界面提供 Path，可限制为 `/websub/*`；否则应用自身对其他路径返回 404。
4. 不要在该主机名或 `/websub/*` 前加需要浏览器登录的 Cloudflare Access 身份验证，否则 YouTube hub 无法完成 GET challenge 和 POST 通知。
5. 等 DNS 与 Tunnel 生效，并完成下一节配置、启动 `y2b watch` 后，用不存在的随机路径验证转发；预期是应用返回 4xx（不带 `hub.*` 参数时为 400，带参数但回调未知时为 404），而不是 Cloudflare 502/530：

   ```bash
   curl -i https://push.example.com/websub/not-a-real-callback
   ```

   Cloudflare 的 published application 会把公网主机名映射到本地 HTTP 服务；官方配置说明见 [Configuration file / ingress rules](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/local-management/configuration-file/)。

### 用 API 代替 Dashboard（生产环境实际做法）

生产环境的 `push.mistwu.com` 是用账户 API token 通过 REST 完成的，不依赖浏览器；需要账户级 `Cloudflare Tunnel Write` 与 zone 级 `DNS Write`：

1. `POST /accounts/{account}/cfd_tunnel` `{"name":"y2b-websub","config_src":"cloudflare"}`，得到 tunnel id；
2. `PUT /accounts/{account}/cfd_tunnel/{id}/configurations`，ingress 只放行 `hostname=push.mistwu.com` 且 `path=^/websub/` 到 `http://127.0.0.1:8787`，兜底 `http_status:404`；
3. `POST /zones/{zone}/dns_records` 写 CNAME `push → {id}.cfargotunnel.com`，`proxied=true`；
4. `GET /accounts/{account}/cfd_tunnel/{id}/token` 取 connector token，只在目标主机执行 `sudo cloudflared service install <TOKEN>`（Ubuntu 用 `pkg.cloudflare.com` apt 源安装 `cloudflared`）。

若 token 只有账户级权限（没有 zone DNS），可先用 `Account API Tokens Write` 创建一个仅含目标 zone `DNS Write`、带过期时间的临时 token 写 CNAME，用完删除。

### y2b 配置与验证

1. 写入配置：

   ```toml
   [websub]
   enabled = true
   bind_addr = "127.0.0.1:8787"
   callback_base_url = "https://push.example.com"
   data_api_poll_minutes = 30
   ```

2. 先运行 `config-check`，再启动或重启 `y2b watch`。监听器绑定成功后，进程启动时会立即为所有缺失/待续租频道提交订阅请求。
3. 查看每个频道的状态、租约和最近推送：

   ```bash
   y2b --config /etc/y2b/config.toml websub status
   ```

   状态从 `not_subscribed` / `pending_verification` 变为 `active`，并出现租约截止时间，才算真正生效。
4. 首次启用或排障时可强制重提订阅：

   ```bash
   y2b --config /etc/y2b/config.toml websub subscribe --all
   y2b --config /etc/y2b/config.toml websub subscribe --channel <内部数字ID或UC频道ID>
   ```

5. 收到第一条有效通知后，`websub status` 的最近推送时间会更新；该频道的预测轮询自动切成 30 分钟兜底。若一直停在 `pending_verification`，依次检查 Tunnel 是否 `Healthy`、公网路径是否到达本地 8787、是否错误启用了 Access 登录、GET/POST 与 `X-Hub-Signature` 是否被代理保留。

## 用户必须完成的其他外部准备

1. 自行申请可调用 YouTube Data API v3 的 API key。
2. 将 key 写入 `/etc/y2b/y2b.env`：

   ```text
   YOUTUBE_API_KEY=<your-key>
   ```

   环境文件应只允许服务账户读取，并确认运行 `y2b watch` 的进程实际加载了该文件。不要把 key 提交到 Git。
3. WebSub 代理必须保留 GET query、POST body 和 `X-Hub-Signature`，且 body 上限不得低于 128 KB。

## 当前部署前检查清单

- 记录待部署的主线 commit ID，并使用该提交生成的完整 release 输入；不要误用本地未提交文件。
- 保存当前整套应用文件、配置文件、环境文件和 SQLite 数据库的可恢复备份，并进入 maintenance hold。
- 在数据库副本上执行迁移，确认 schema version 为 21，并执行 SQLite integrity/quick check。
- 执行：

  ```text
  cargo fmt --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test
  npm run check
  ```

- 使用实际部署配置运行 `config-check`；WebSub 未准备好时必须保持 `enabled = false`。
- 确认 `/etc/y2b/y2b.env` 已由服务加载，但不要在日志或命令输出中打印 key。
- 首次上线先保持 WebSub 关闭，确认：uploads playlist 已缓存、每频道 `next_data_api_poll_at` 推进、Data API 配额开始记账、候选能从 `pending` 晋级、RSS 仅作为探针、API 深扫失败才调用 yt-dlp。
- 准备 WebSub 后再单独开启，确认 challenge 成功、租约截止时间已写入、签名错误请求被拒绝、有效推送只新增候选。
- 观察现有准备、上传和字幕 worker，确认发现任务没有阻塞原流水线。

## 回滚

1. 进入 maintenance hold 并停止产生新写入后，保留故障现场数据库副本、当前 v21 应用文件和日志。
2. 完整切回部署前的二进制与配套资源，不要只替换二进制；旧版本可能不理解 v16–v21 的 AI 审计、租约、BVID 唯一性和投稿／字幕 attempt 状态。
3. 只有经兼容性确认的 release 才能继续使用 v21 数据库；否则通过 `restore.sh` 恢复与旧 release 匹配的完整数据库备份。
4. 恢复数据库前要明确接受丢失部署后新增任务、候选、配额、租约和投稿／字幕 attempt 状态，并单独记录 `upload_uncertain` 和 `subtitle_attempts` 中的 `uncertain` 供人工核对。
5. 恢复后重新执行配置检查、数据库完整性检查、schema v21／目标版本检查、队列状态检查和发现健康检查，全部通过后再释放 hold。

不要通过删除迁移记录或手工删除新增列来回滚；SQLite 的列删除会扩大风险，使用完整应用文件回滚或数据库备份恢复。
