#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "$0")/.." && pwd)
binary=${1:-"$root_dir/target/x86_64-unknown-linux-musl/release/y2b"}
[[ -x "$binary" ]] || { echo "missing executable: $binary" >&2; exit 1; }

config_file=/etc/y2b/config.toml
if [[ ! -f "$config_file" ]]; then
  install -d -m 0755 /etc/y2b
  install -m 0644 "$root_dir/config.example.toml" "$config_file"
fi
"$binary" --config "$config_file" config-check

# Pi extension 由 pi 子进程加载，config-check 只看文件在不在、不解析内容：一个
# 反引号写进模板字符串就能让所有 Pi 调用等到运行时才炸，并白白消耗任务重试次数。
# `node --check` 对 .ts 无效（不剥类型，恒为 0），这里用真正的 import 解析一遍。
node --input-type=module -e "await import('file://'+process.argv[1])" \
  "$root_dir/pi/y2b-extension.ts" || {
  echo "pi extension failed to parse: $root_dir/pi/y2b-extension.ts" >&2
  exit 1
}

env_file=/etc/y2b/y2b.env
[[ -f "$env_file" ]] || { echo "missing credential file: $env_file" >&2; exit 1; }
[[ $(stat -c '%U:%G' "$env_file") == root:root ]] || {
  echo "credential file must be owned by root:root: $env_file" >&2
  exit 1
}
[[ $(stat -c '%a' "$env_file") == 600 ]] || {
  echo "credential file must have mode 600: $env_file" >&2
  exit 1
}
if grep -Eq '^[[:space:]]*(export[[:space:]]+)?DEEPSEEK_API_KEY[[:space:]]*=' "$env_file"; then
  echo "DeepSeek key must not be stored in $env_file" >&2
  exit 1
fi

command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
python3 "$root_dir/deploy/y2b-set-deepseek-key.py" --check

database=/var/lib/y2b/state.db
assert_idle() {
  if systemctl is-active --quiet y2b-watch.service && [[ -f "$database" ]]; then
    active_jobs=$(sqlite3 "$database" "SELECT COUNT(*) FROM jobs WHERE status IN ('inspecting','processing','uploading');")
    running_stages=$(sqlite3 "$database" "SELECT COUNT(*) FROM stage_runs WHERE status='running';")
    if (( active_jobs > 0 || running_stages > 0 )); then
      echo "refusing to restart y2b-watch with active jobs=$active_jobs running stages=$running_stages" >&2
      exit 1
    fi
  fi
}

assert_idle

install -m 0755 "$binary" /usr/local/bin/y2b
install -m 0644 "$root_dir/pi/y2b-extension.ts" /opt/y2b/pi/y2b-extension.ts
install -m 0644 "$root_dir/pi/policy.json" /opt/y2b/pi/policy.json
install -m 0644 "$root_dir/pi/audit-policy.json" /opt/y2b/pi/audit-policy.json
install -m 0644 "$root_dir/pi/brawl-stars-glossary.json" /opt/y2b/pi/brawl-stars-glossary.json
install -m 0644 "$root_dir/Cargo.lock" /opt/y2b/Cargo.lock
install -d /opt/y2b/deploy
install -m 0755 "$root_dir/deploy/restore.sh" /opt/y2b/deploy/restore.sh
install -m 0755 "$root_dir/deploy/y2b-set-deepseek-key.py" /usr/local/sbin/y2b-set-deepseek-key
install -m 0755 "$root_dir/deploy/y2b-set-deepseek-key.py" /opt/y2b/deploy/y2b-set-deepseek-key.py
install -m 0644 "$root_dir/deploy/y2b-watch.service" /opt/y2b/deploy/y2b-watch.service
install -m 0644 "$root_dir/deploy/y2b-watch.service" /etc/systemd/system/y2b-watch.service
systemctl daemon-reload

# 迁移和基线写入要独占数据库，且新二进制必须真正接管：`enable --now` 对已在
# 运行的服务是空操作，会出现「装上了但跑的还是旧二进制」。这里显式停 → 迁移
# → 起。停止前先确认没有投稿在途，避免 SIGKILL 掉一半的上传。
if systemctl is-active --quiet y2b-watch.service; then
  assert_idle
  echo "==> 停止 y2b-watch 以独占数据库"
  systemctl stop y2b-watch.service
fi

/usr/local/bin/y2b --config "$config_file" check --write-baseline

systemctl enable y2b-watch.service
systemctl restart y2b-watch.service
systemctl --no-pager --full status y2b-watch.service
