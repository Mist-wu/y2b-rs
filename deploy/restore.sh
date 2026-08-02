#!/usr/bin/env bash
set -euo pipefail
backup=${1:?usage: restore.sh /path/to/state-backup.db}
systemctl stop y2b-watch.service 2>/dev/null || true
[[ -f /var/lib/y2b/state.db ]] && cp -a /var/lib/y2b/state.db "/var/lib/y2b/state.db.before-restore.$(date +%s)"
install -m 0600 "$backup" /var/lib/y2b/state.db
rm -f /var/lib/y2b/state.db-wal /var/lib/y2b/state.db-shm
sqlite3 /var/lib/y2b/state.db 'PRAGMA integrity_check;'
systemctl start y2b-watch.service
