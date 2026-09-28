# 部署与运维

[← 返回 README](../README.md)

## 强失败质量门禁

本地按改动范围跑对应的那几条就行，剩下的交给 CI：

```bash
# 改 Rust
cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test
# 改 pi/ 下的 extension 或 policy
npm ci && npm run check   # TypeScript 类型检查 + y2b-extension.ts 的真实 import 解析
# 改 scripts/
python3 -m unittest discover -s scripts -p 'test_*.py'
# 改 deploy/
python3 -m unittest discover -s deploy/tests -p 'test_*.py' && shellcheck deploy/*.sh
```

CI 只有一个工作流 `.github/workflows/ci.yml`，四个并行 job：Rust、脚本与配置、依赖审计、Gitleaks。格式、类型、测试、脚本语法、Gitleaks 以及真正的 RustSec vulnerability 都是**强失败门禁**：任一命令非零退出就阻断合并和发布。Gitleaks 扫描完整历史，仓库中的测试假 Key 只按唯一 fingerprint 精确放行。

依赖审计刻意采用不同阈值，因为 advisory 会在上游发布后异步改变，与当前提交未必相关：npm 只让 high／critical 发现阻断（`npm audit --audit-level=high`），low／moderate 仍显示在报告中；`cargo audit` 默认只对 vulnerability 非零退出，unmaintained、yanked、unsound 等 warning 会打印在日志里但不阻断。这样既不隐藏漏洞和维护风险，也不会因低级噪音让主门禁长期失去可信度。

## 部署

目标：Ubuntu 22.04 x86_64。下文的 `<server>` 换成你的 SSH 目标（如 `azureuser@1.2.3.4`）。Azure 镜像禁止 root 直接 SSH，特权操作走 `azureuser` 免密 `sudo`。服务器不编译 Rust 或 FFmpeg。

```bash
# 1. 服务器：2 GiB swap、预编译依赖和自动 PO Token Provider
scp deploy/bootstrap-server.sh deploy/install-ytdlp-pot-provider.sh <server>:/tmp/
ssh <server> 'sudo bash /tmp/bootstrap-server.sh'

# 2. Mac：静态交叉编译
brew install zig
cargo install cargo-zigbuild --locked
rustup target add x86_64-unknown-linux-musl
cargo zigbuild --release --target x86_64-unknown-linux-musl

# 3. 按 commit 建独立传输目录，上传二进制、运行资源和安全换钥工具
release_id=$(git rev-parse --short=12 HEAD)
scp target/x86_64-unknown-linux-musl/release/y2b <server>:/tmp/y2b-$release_id
ssh <server> "install -d /tmp/y2b-release-$release_id"
scp -r pi config.example.toml deploy Cargo.lock <server>:/tmp/y2b-release-$release_id/
ssh <server> "sudo install -o root -g root -m 755 /tmp/y2b-release-$release_id/deploy/y2b-set-deepseek-key.py /usr/local/sbin/y2b-set-deepseek-key"

# 4. 在 Mac 终端输入新 Key；输入不回显，Key 只经 stdin 发送且不会出现在命令历史
(read -r -s 'Y2B_DEEPSEEK_KEY?请输入新的 DeepSeek API Key: '; printf '\n'; printf '%s' "$Y2B_DEEPSEEK_KEY" | ssh <server> 'sudo /usr/local/sbin/y2b-set-deepseek-key')

# 5. 部署应用
ssh <server> "sudo bash /tmp/y2b-release-$release_id/deploy/deploy-app.sh /tmp/y2b-$release_id"
```

换钥工具会原子写入专用认证文件，并删除 `/etc/y2b/y2b.env` 和全局 Pi 认证中的旧 DeepSeek 条目；它不会打印明文 Key。部署前可用 `sudo y2b-set-deepseek-key --check` 只读检查单一路径约束。

> [!IMPORTANT]
> 第 3 步不能只拷二进制。`y2b-extension.ts`、`policy.json`、`audit-policy.json`、`brawl-stars-glossary.json` 与二进制必须来自同一份输入；`deploy-app.sh` 会把它们一起放入 `/opt/y2b/releases/$release_id/`。`/opt/y2b/pi` 只是指向 `current/pi` 的兼容链接，禁止再向这个固定路径单独覆盖文件。

### maintenance hold 与原子 release 边界

**maintenance hold** 是 SQLite 中带 owner、原因和租期的真实写锁，不再只是运维约定。它会阻止 watch、手动 `y2b run` 和字幕流程领取新工作；部署获取锁后仍要等待已经领取的任务结束。`deploy-app.sh` 以 `deploy:<revision>:<UTC 时间>:<PID>` 作为唯一 owner，每轮等待都会续租，并用 `status --json --owner <本次 owner>` 排除自己的锁。`active_claims`、`upload_attempts`、`subtitle_attempts` 等 blocker 的 kind、数量和 details 都会原样打印。

手工维护用同一组命令，`--owner` 取一个唯一值、`--database` 显式写出：

```bash
owner="manual:$(date -u +%Y%m%dT%H%M%SZ):$$"
y2b maintenance acquire --database /var/lib/y2b/state.db \
  --owner "$owner" --reason '人工维护' --lease-seconds 900
y2b maintenance status --database /var/lib/y2b/state.db --owner "$owner" --json
y2b maintenance renew --database /var/lib/y2b/state.db \
  --owner "$owner" --lease-seconds 900
y2b maintenance release --database /var/lib/y2b/state.db --owner "$owner"
```

`--owner` 只在 status 中排除调用方自己的 hold；省略它可从旁观者视角确认当前维护者。租约到期后锁可被接管。`maintenance status` 对不存在的数据库会报错，部署也会拒绝继续，不会因路径写错而新建空库。

应用 release 已按 commit 原子化。旧说明“应用 release 当前不是原子切换”已经失效；当前契约是：

1. 在获取 hold 之前完成 `config-check`、Pi extension 解析、凭据、Python、SQLite 等静态预检。
2. 获取 hold，间隔等待两次连续 idle；hold 挡住新领取，两次检查只需排空存量工作。
3. 把二进制、全部 Pi 资源、`Cargo.lock` 和 deploy 脚本（含 watch systemd 单元）写入隐藏 staging，完整后发布为不可变的 `/opt/y2b/releases/<revision>/`，不改动运行中的 `current`。
4. 在 maintenance hold 保护下生成迁移前在线快照，并做严格完整性校验（`integrity_check` 只返回一行 `ok`）。
5. 以临时符号链接和单次 `mv -T` 原子切换 `/opt/y2b/current`，再依次执行显式迁移、`y2b check --write-baseline`、启动和稳定窗口健康检查。systemd 直接执行 `/opt/y2b/current/y2b`，Pi 兼容路径也经 `current/pi` 解析。
6. 稳定窗口健康检查成功后才释放 hold。脚本保留当前版、上一版和若干旧 release；超出上限时只按修改时间清理名称符合十六进制 revision 规则的真实目录，不跟随符号链接。

`deploy-app.sh` 只支持已经建立 `/opt/y2b/current -> releases/<revision>`、且数据库包含唯一 `maintenance_hold` 表的 release 布局。旧数据库必须先通过 `restore.sh` 显式迁移；旧扁平布局或首次安装必须先执行一次性布局迁移。部署脚本不会再退化到无维护锁的自举路径，也不会在常规发布中捕获旧扁平目录。

回滚的最小单位是 **release + 数据库**，绝不能只切回 symlink。停服务之后任一步失败，EXIT trap 都会先确保新服务退出，再把 `current` 原子切回上一 release、从迁移前快照原子恢复数据库并清理 WAL/SHM，随后用旧二进制重新执行 `check`、启动旧服务并通过稳定窗口健康检查，最后释放 hold。只有这套成对回滚能满足精确 schema 匹配；若旧服务健康检查也失败，脚本保持非零退出并保留迁移前备份供人工处理。旧布局、缺失维护锁表或不完整的 current release 都会在获取 hold、停止服务之前失败。

### 依赖基线（dependency-baseline.json）

`y2b check --write-baseline` 把必选工具和资源文件的 sha256 写入
`/var/lib/y2b/dependency-baseline.json`，后续每次 `y2b check` 都与基线比对。基线
条目分两类，处理不同：

| 类别 | 条目 | 漂移处理 |
| --- | --- | --- |
| 外部依赖 | `pi`、`yt-dlp`、`ffmpeg`、`biliup`、`pi-extension`、`pi-policy`、`pi-audit-policy`、`brawl-stars-glossary` | 漂移是意外，作为必选失败（FAIL）拦住部署 |
| `y2b` 自身 | 部署的二进制 | 漂移是部署的预期结果，只降级为告警（WARN），不拦住部署 |

这样区分是因为：部署一个新二进制必然改变 `y2b` 的 sha256，若把它的漂移当作必选
失败，就会出现“部署要更新的那一项，恰恰是拦住部署的那一项”的死循环；回滚后的
健康检查也会因旧二进制与基线不一致而误判。外部依赖被偷换时仍然会以必选失败拒绝
部署，这是基线存在的意义。

`deploy-app.sh` 在切换 `current` 后执行 `check --write-baseline`，把新二进制的
sha 写回基线；回滚时用旧二进制再执行一次，把基线同步回旧值。因此只有 `y2b`
漂移的部署和回滚都能通过门禁，而任何外部依赖漂移仍会阻断。

PO Token Provider 使用独立的**原子 release**：安装器先写入版本化 `releases/<version>`，校验完整后再以临时符号链接和单次 `mv` 切换 `current`。失败时旧 `current` 仍可用，不会暴露半份 provider。

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

在线备份每 6 小时一次，保留 4 个小时备份、7 个日备份、4 个周备份。`deploy-app.sh` 每次迁移前还会在 `backups/deploy/` 自动生成并验证专用快照；手工迁移前先执行一次 `y2b backup`。恢复要用与备份兼容的完整 release，不能只替换数据库或二进制——schema 对不上服务起不来。

1. 记录备份时间、来源 schema 和对应 release；用唯一 owner 获取 maintenance hold，并通过带同一 `--owner` 的 status 确认全部 blockers 为空，而不只是查看 `uploading`／运行中的 stage。保留当前数据库与整套 release 作为成对回退点。
2. 空服务器先运行 `bootstrap-server.sh`，再恢复 `/etc/y2b/config.toml`、`/etc/y2b/y2b.env` 和两个 cookies 文件，并用 `y2b-set-deepseek-key` 重新注入 DeepSeek Key。Pi 资源随应用 release 安装，无需单独备份。
3. 先用 `deploy-app.sh` 安装与当前代码匹配的完整 release，再从 `backups/daily` 或 `weekly` 选择数据库并执行 `deploy/restore.sh BACKUP.db`。release 与数据库成对选择，把这一对记下来备查。
4. `restore.sh` 在停服务前完成强预检：把备份复制到数据库所在文件系统的暂存路径，要求 SQLite `integrity_check` 精确返回单独一行 `ok`，同时检查关键表和可读 schema。预检通过后才记录原 service 状态、停服务、保存旧库，并以同文件系统 `mv` 原子替换数据库和清理旧 WAL/SHM。
5. 原服务先前为 active 时，脚本启动它并等待幂等迁移到 schema v22，再复查数据库完整性，并通过稳定窗口健康检查确认 service 稳定。任一步骤失败，EXIT trap 都尝试恢复旧数据库及原 service 状态并返回非零；成功后仍要核对 schema v22、队列数量、最近备份、`upload_uncertain` 和 `subtitle_attempts` 中的 `uncertain`。不确定的投稿或字幕提交只能人工核对，不能因恢复而自动重试。
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
