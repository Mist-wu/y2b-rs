<div align="center">

# y2b-rs

**监控 YouTube 频道 → 下载 → DeepSeek API 分句翻译 → 投稿 Bilibili → 自动补中文 CC 字幕**

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
- 门禁：push 前本地 hook 跑 Gitleaks、`cargo fmt`、`clippy -D warnings` 和测试；PR 上 CI 再跑 clippy 和测试；依赖漏洞审计在 `Cargo.lock` 变动时和每周跑。详见 `docs/deploy.md`。

## 搬运模式

| 模式 | 流程 | 字幕 |
| --- | --- | --- |
| `direct` | 下载视频，用 DeepSeek 生成中文标题、简介和标签 | 不处理字幕 |
| `translated` | 源语言字幕 → DeepSeek 分句 → DeepSeek 翻译 → 上传原片 → 提交中文 CC | B 站软字幕，观众可开关 |

频道默认使用 `translation.source_lang`。添加频道时可传 `--source-lang ja`，已有频道使用 `y2b channels set-lang <id> ja` 设置。省略语言可查看当前值，传 `--clear` 则恢复全局配置。

`video_id` 全局唯一，同一个视频不会重复入队或重复投稿。优先频道在队列中优先处理，发现频率和 WebSub 租约的关系见[发现与筛选](docs/workflow.md#发现与筛选)。

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

运行时依赖 `yt-dlp`、`ffmpeg` 和 `biliup`；AI 由 Rust 直接请求 DeepSeek API。运行前按[部署说明](docs/deploy.md#部署)配置专用 DeepSeek 认证文件，`y2b check` 会检查依赖与凭据。

## 常用命令

`y2b run <URL>` 手动搬运单个视频，`y2b subtitle add <BVID>` 给已有稿件补交中文字幕。用 `y2b --help` 查看所有命令，用 `y2b channels --help`、`y2b jobs --help` 或 `y2b subtitle --help` 查看对应操作。启用 `tui` 特性后，运行 `y2b tui` 可查看队列和当前可用快捷键。

## 项目结构

```text
y2b-rs/
├── src/          # Rust 主程序：发现、队列、下载、投稿、字幕、TUI
├── pi/           # AI 提示词、翻译策略和荒野乱斗词库
├── scripts/      # 词库审计与性能诊断脚本（Python）
└── deploy/       # 服务器初始化、原子发布、恢复脚本和 systemd 单元
```

## 文档

- [工作流程与 DeepSeek API](docs/workflow.md)：发现、筛选、投稿参数、队列容错和词库的细节
- [部署与运维](docs/deploy.md)：质量门禁、交叉编译部署、维护锁、依赖基线、备份与恢复

## License

[MIT](LICENSE)
