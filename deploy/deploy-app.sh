#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "$0")/.." && pwd)
binary=${1:-"$root_dir/target/x86_64-unknown-linux-musl/release/y2b"}
[[ -x "$binary" ]] || { echo "missing executable: $binary" >&2; exit 1; }

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
grep -Eq '^DEEPSEEK_API_KEY=sk-[A-Za-z0-9_-]+$' "$env_file" || {
  echo "credential file has invalid DEEPSEEK_API_KEY syntax: $env_file" >&2
  exit 1
}

install -m 0755 "$binary" /usr/local/bin/y2b
install -m 0644 "$root_dir/pi/y2b-extension.ts" /opt/y2b/pi/y2b-extension.ts
install -m 0644 "$root_dir/pi/policy.json" /opt/y2b/pi/policy.json
install -m 0644 "$root_dir/pi/audit-policy.json" /opt/y2b/pi/audit-policy.json
install -m 0644 "$root_dir/pi/brawl-stars-glossary.json" /opt/y2b/pi/brawl-stars-glossary.json
install -m 0644 "$root_dir/Cargo.lock" /opt/y2b/Cargo.lock
install -d /opt/y2b/deploy
install -m 0755 "$root_dir/deploy/restore.sh" /opt/y2b/deploy/restore.sh
[[ -f /etc/y2b/config.toml ]] || install -m 0644 "$root_dir/config.example.toml" /etc/y2b/config.toml
install -m 0644 "$root_dir/deploy/y2b-watch.service" /etc/systemd/system/y2b-watch.service
systemctl daemon-reload

# 迁移和基线写入要独占数据库，且新二进制必须真正接管：`enable --now` 对已在
# 运行的服务是空操作，会出现「装上了但跑的还是旧二进制」。这里显式停 → 迁移
# → 起。停止前先确认没有投稿在途，避免 SIGKILL 掉一半的上传。
if systemctl is-active --quiet y2b-watch.service; then
  echo "==> 停止 y2b-watch 以独占数据库"
  systemctl stop y2b-watch.service
fi

/usr/local/bin/y2b --config /etc/y2b/config.toml check --write-baseline

systemctl enable y2b-watch.service
systemctl restart y2b-watch.service
systemctl --no-pager --full status y2b-watch.service
