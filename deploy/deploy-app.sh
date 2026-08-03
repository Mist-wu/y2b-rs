#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "$0")/.." && pwd)
binary=${1:-"$root_dir/target/x86_64-unknown-linux-musl/release/y2b"}
[[ -x "$binary" ]] || { echo "missing executable: $binary" >&2; exit 1; }

install -m 0755 "$binary" /usr/local/bin/y2b
install -m 0644 "$root_dir/pi/y2b-extension.ts" /opt/y2b/pi/y2b-extension.ts
install -m 0644 "$root_dir/pi/policy.json" /opt/y2b/pi/policy.json
install -m 0644 "$root_dir/pi/audit-policy.json" /opt/y2b/pi/audit-policy.json
install -m 0644 "$root_dir/pi/brawl-stars-glossary.json" /opt/y2b/pi/brawl-stars-glossary.json
install -m 0644 "$root_dir/Cargo.lock" /opt/y2b/Cargo.lock
install -d /opt/y2b/deploy
install -m 0755 "$root_dir/deploy/verify-ass.sh" /opt/y2b/deploy/verify-ass.sh
install -m 0755 "$root_dir/deploy/restore.sh" /opt/y2b/deploy/restore.sh
[[ -f /etc/y2b/config.toml ]] || install -m 0644 "$root_dir/config.example.toml" /etc/y2b/config.toml
install -m 0644 "$root_dir/deploy/y2b-watch.service" /etc/systemd/system/y2b-watch.service
systemctl daemon-reload
/usr/local/bin/y2b --config /etc/y2b/config.toml check --write-baseline
systemctl enable --now y2b-watch.service
systemctl --no-pager --full status y2b-watch.service
