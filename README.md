<div align="center">

# y2b-rs

**监控 YouTube 频道 → 下载 → Pi 分句翻译 → 投稿 Bilibili → 自动补中文 CC 字幕**

单二进制 Rust CLI，可选 TUI，SQLite 持久化队列，部署在云服务器上全程无人值守。

[![Rust](https://img.shields.io/badge/Rust-2024_edition-000?logo=rust)](https://www.rust-lang.org)
[![SQLite](https://img.shields.io/badge/SQLite-queue-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org)
[![Target](https://img.shields.io/badge/deploy-Ubuntu%2022.04%20musl-E95420?logo=ubuntu&logoColor=white)](docs/deploy.md)
[![License](https://img.shields.io/badge/license-MIT-22c55e)](LICENSE)

</div>

---

## 亮点

- 投稿防重：每次投稿和字幕提交前，先把 attempt 写进数据库。进程中断、响应丢失时，任务进入 `uncertain` 状态，禁止自动重投。之后查询 B 站创作中心，标题、发布时间和 BVID 三项证据都对上才确认。
- 可回滚的原子发布：每个版本发布到不可变的 `releases/<commit>/`，用一次 `mv -T` 切换 `current`。部署前拿 SQLite 里的维护锁并做在线快照；任一步失败，二进制和数据库成对回滚。
- 调度与限流：RSS、Data API 和 WebSub 三路发现新视频。yt-dlp 回退有单频道冷却和全局熔断（10 分钟最多 3 次）。B 站返回限流码 `21566` 时，全局冷却 6 小时后自动重试。
- LLM 翻译带游戏词库：从游戏客户端本地化资源提取官方译名，按“人工校订 > 数值模板 > 现行 > 历史”四层优先级注入。每次只注入字幕里实际出现的词条，控制 token 用量。
- 进程与内存限制：外部命令独占 Unix 进程组，超时时清理整棵进程树，不留孤儿进程。systemd 限制 `MemoryMax=1600M`，准备、上传、字幕三个 worker 分开排队。
- CI 门禁：`cargo fmt`、`clippy -D warnings`、测试、TypeScript 类型检查、依赖漏洞审计和 Gitleaks 全历史扫描，任一失败就阻断合并。

## 工作原理

```
YouTube 频道 ──RSS / Data API / WebSub──> 发现新视频 ──> SQLite 任务队列
                                                          │
                ┌─────────────────────────────────────────┘
                ▼
   准备 worker：下载原片 ∥ 下载英文字幕 → Pi 分句 → Pi 翻译 → 生成中文标题、简介、标签
                │
                ▼
   上传 worker：biliup 投稿原片（严格串行，默认间隔 30 分钟）
                │
                ▼
   字幕 worker：等 B 站转码完成 → 提交中文 CC 字幕（失败按指数退避重试）
```

每个视频有两种搬运模式：

| 模式 | 流程 | 字幕 |
| --- | --- | --- |
| `direct` | 下载视频，用 Pi 生成中文标题、简介和标签 | 不处理字幕 |
| `translated` | 英文字幕 → Pi 分句 → Pi 翻译 → 上传原片 → 提交中文 CC | B 站软字幕，观众可开关 |

`video_id` 全局唯一，同一个视频不会重复入队或重复投稿。优先频道单独按 60 秒轮询，在各个队列里都排在普通频道前面。

## 快速开始

```bash
cargo build --release    # 需要交互界面时加 --features tui

y2b init                 # 生成配置
y2b config-check         # 校验配置
y2b login youtube /path/to/cookies.txt
y2b login bilibili
y2b channels add 'https://www.youtube.com/@channel/videos' --mode translated
y2b check --write-baseline
y2b watch                # 常驻运行
```

运行时依赖 `yt-dlp`、`ffmpeg`、`biliup` 和 [pi](https://pi.dev)，`y2b check` 会逐项检查。

## 常用命令

```bash
# 频道
y2b channels add <URL> --mode direct|translated
y2b channels list | set-mode <ID> <MODE> | set-priority <ID> normal|priority
y2b channels enable <ID> | disable <ID> | sync

# 任务
y2b run <URL> [--mode translated]   # 单个视频跑完整流程
y2b jobs list [N] | show <JOB_ID> | retry <JOB_ID>
y2b jobs reconcile-upload <JOB_ID> [--not-published]

# 字幕与运维
y2b subtitle add <BVID>             # 给已投稿视频补中文 CC
y2b subtitle all                    # 遍历全部已投稿视频，已有中文字幕的跳过
y2b backup | auth-check | check --write-baseline
```

TUI 用 `Tab` 切换任务和频道列表，`n` 输入单个 URL，`r` 重试失败任务，`q` 退出。

## 项目结构

```text
y2b-rs/
├── src/          # Rust 主程序：发现、队列、下载、投稿、字幕、TUI
├── pi/           # Pi extension、翻译策略和荒野乱斗词库
├── scripts/      # 词库审计脚本（Python）
├── deploy/       # 服务器初始化、原子发布、恢复脚本和 systemd 单元
└── tests/
```

## 文档

- [工作流程与 Pi 集成](docs/workflow.md)：发现、筛选、投稿参数、队列容错和词库的细节
- [部署与运维](docs/deploy.md)：质量门禁、交叉编译部署、维护锁、依赖基线、备份与恢复
- [发现机制重构](DISCOVERY_REARCHITECTURE.md)：WebSub 推送方案

## License

[MIT](LICENSE)
