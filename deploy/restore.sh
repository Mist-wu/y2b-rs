#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
  echo "用法: restore.sh /path/to/state-backup.db" >&2
  exit 2
fi

backup=$1
state_dir=${Y2B_STATE_DIR:-/var/lib/y2b}
database=${Y2B_DATABASE:-"$state_dir/state.db"}
service=${Y2B_SERVICE:-y2b-watch.service}
sqlite3_cmd=${Y2B_SQLITE3:-sqlite3}
systemctl_cmd=${Y2B_SYSTEMCTL:-systemctl}
y2b_cmd=${Y2B_BIN:-y2b}
health_interval=${Y2B_HEALTH_INTERVAL_SECONDS:-1}
health_window_seconds=${Y2B_HEALTH_WINDOW_SECONDS:-10}
health_max_checks=${Y2B_HEALTH_MAX_CHECKS:-30}
schema_override=${Y2B_SCHEMA_VERSION:-}
expected_schema=

temp_db=
check_output=
old_backup=
had_old=false
replacement_done=false
service_state_recorded=false
was_active=false

cleanup_temporary_files() {
  [[ -z "$check_output" ]] || rm -f -- "$check_output"
  [[ -z "$temp_db" || ! -e "$temp_db" ]] || rm -f -- "$temp_db"
  if [[ -n "$old_backup" && "$had_old" != true ]]; then
    rm -f -- "$old_backup"
  fi
}

rollback_on_error() {
  local status=$?
  local rollback_tmp=
  trap - EXIT INT TERM
  if (( status == 0 )); then
    cleanup_temporary_files
    return 0
  fi

  set +e
  if [[ "$service_state_recorded" == true ]]; then
    # 替换后的进程可能仍持有新库；回滚前先确保它已经退出。
    "$systemctl_cmd" stop "$service" >/dev/null 2>&1
  fi
  if [[ "$replacement_done" == true ]]; then
    if [[ "$had_old" == true ]]; then
      rollback_tmp=$(mktemp "$state_dir/.state.db.rollback.XXXXXXXX")
      if cp -a -- "$old_backup" "$rollback_tmp" &&
        chmod 0600 "$rollback_tmp" &&
        mv -f -- "$rollback_tmp" "$database"; then
        rollback_tmp=
      else
        echo "恢复旧数据库失败，请使用 $old_backup 手工恢复" >&2
      fi
    else
      rm -f -- "$database"
    fi
    rm -f -- "$database-wal" "$database-shm"
  fi
  [[ -z "$rollback_tmp" ]] || rm -f -- "$rollback_tmp"

  if [[ "$service_state_recorded" == true ]]; then
    if [[ "$was_active" == true ]]; then
      "$systemctl_cmd" start "$service" >/dev/null 2>&1 ||
        echo "恢复服务原 active 状态失败" >&2
    else
      "$systemctl_cmd" stop "$service" >/dev/null 2>&1 ||
        echo "恢复服务原 inactive 状态失败" >&2
    fi
  fi
  cleanup_temporary_files
  echo "恢复失败，已尝试回滚数据库和服务状态" >&2
  exit "$status"
}
trap rollback_on_error EXIT
trap 'exit 130' INT TERM

check_integrity() {
  local target=$1
  check_output=$(mktemp "$state_dir/.state.db.integrity.XXXXXXXX")
  if ! "$sqlite3_cmd" "$target" 'PRAGMA integrity_check;' >"$check_output"; then
    echo "SQLite 完整性检查执行失败: $target" >&2
    return 1
  fi
  # sqlite3 即使发现损坏也可能以 0 退出，必须严格匹配唯一一行 `ok`。
  if ! cmp -s <(printf 'ok\n') "$check_output"; then
    echo "SQLite 完整性检查未返回唯一一行 ok: $target" >&2
    return 1
  fi
  rm -f -- "$check_output"
  check_output=
}

read_schema() {
  "$sqlite3_cmd" "$1" 'SELECT MAX(version) FROM schema_migrations;'
}

wait_for_stable_service() {
  local attempt
  local active_seen=false
  local stable_samples
  local required_stable_samples=2
  local window_samples
  local first_pid=
  local current_pid=
  local current_restarts=
  local health_output

  if (( health_interval > 0 )); then
    window_samples=$(( health_window_seconds / health_interval + 1 ))
    (( window_samples > required_stable_samples )) && required_stable_samples=$window_samples
  fi

  for ((attempt = 1; attempt <= health_max_checks; attempt++)); do
    if "$systemctl_cmd" is-active --quiet "$service"; then
      active_seen=true
      break
    fi
    if (( attempt < health_max_checks )); then
      sleep "$health_interval"
    fi
  done
  if [[ "$active_seen" != true ]]; then
    echo "$service 恢复后健康检查失败" >&2
    return 1
  fi

  for ((stable_samples = 1; stable_samples <= required_stable_samples; stable_samples++)); do
    if ! "$systemctl_cmd" is-active --quiet "$service"; then
      echo "$service 在稳定窗口内不再 active（第 $stable_samples/$required_stable_samples 次采样）" >&2
      return 1
    fi
    if ! health_output=$("$systemctl_cmd" show -p MainPID -p NRestarts --value "$service"); then
      echo "无法读取 $service 的 MainPID/NRestarts" >&2
      return 1
    fi
    current_pid=${health_output%%$'\n'*}
    current_restarts=${health_output#*$'\n'}
    if [[ ! "$current_pid" =~ ^[1-9][0-9]*$ ]]; then
      echo "无法读取 $service 的有效 MainPID: $current_pid" >&2
      return 1
    fi
    if [[ "$stable_samples" == 1 ]]; then
      first_pid=$current_pid
    elif [[ "$current_pid" != "$first_pid" ]]; then
      echo "$service 的 MainPID 在稳定窗口内变化: $first_pid -> $current_pid" >&2
      return 1
    fi
    if [[ ! "$current_restarts" =~ ^[0-9]+$ ]] || (( current_restarts != 0 )); then
      echo "$service 在稳定窗口内发生重启: NRestarts=$current_restarts" >&2
      return 1
    fi
    if (( stable_samples < required_stable_samples )); then
      sleep "$health_interval"
    fi
  done

  if ! "$y2b_cmd" maintenance status --database "$database" --json >/dev/null; then
    echo "$service 应用级探针失败: maintenance status 无法正常返回" >&2
    return 1
  fi
}

# 所有可能拒绝恢复的预检都必须发生在停服务之前。
[[ -f "$backup" ]] || { echo "备份文件不存在: $backup" >&2; exit 1; }
[[ -d "$state_dir" ]] || { echo "数据目录不存在: $state_dir" >&2; exit 1; }
command -v "$sqlite3_cmd" >/dev/null 2>&1 || {
  echo "sqlite3 是所有 restore 的必需依赖: $sqlite3_cmd" >&2
  exit 1
}
command -v "$systemctl_cmd" >/dev/null 2>&1 || {
  echo "systemctl 不可用: $systemctl_cmd" >&2
  exit 1
}
if [[ -n "$schema_override" ]]; then
  expected_schema=$schema_override
else
  command -v "$y2b_cmd" >/dev/null 2>&1 || {
    echo "y2b 不可用，无法读取当前 schema 版本: $y2b_cmd" >&2
    exit 1
  }
  if ! expected_schema=$("$y2b_cmd" schema-version); then
    echo "无法从 y2b 读取当前 schema 版本" >&2
    exit 1
  fi
fi
if [[ ! "$expected_schema" =~ ^[0-9]+$ ]] || (( expected_schema <= 0 )); then
  echo "期望 schema 版本无效: $expected_schema" >&2
  exit 1
fi

temp_db=$(mktemp "$state_dir/.state.db.restore.XXXXXXXX")
cp -- "$backup" "$temp_db"
chmod 0600 "$temp_db"
check_integrity "$temp_db"
key_table_count=$(
  "$sqlite3_cmd" "$temp_db" \
    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('schema_migrations','channels','jobs','settings');"
)
[[ "$key_table_count" == 4 ]] || {
  echo "备份缺少关键表" >&2
  exit 1
}
backup_schema=$(read_schema "$temp_db")
if [[ ! "$backup_schema" =~ ^[0-9]+$ ]] || (( backup_schema <= 0 )); then
  echo "备份 schema 版本不可读" >&2
  exit 1
fi
if (( backup_schema > expected_schema )); then
  echo "备份 schema v${backup_schema} 高于当前二进制 v${expected_schema}，无法降级恢复" >&2
  exit 1
fi
if (( backup_schema < expected_schema )); then
  command -v "$y2b_cmd" >/dev/null 2>&1 || {
    echo "y2b 不可用，无法把 schema v$backup_schema 迁移到 v$expected_schema: $y2b_cmd" >&2
    exit 1
  }
  "$y2b_cmd" migrate --help >/dev/null 2>&1 || {
    echo "当前 y2b 不支持显式数据库迁移" >&2
    exit 1
  }
fi

set +e
"$systemctl_cmd" is-active --quiet "$service"
service_status=$?
set -e
case "$service_status" in
  0) was_active=true ;;
  3) was_active=false ;;
  *)
    echo "无法确定 $service 的当前状态（systemctl=$service_status）" >&2
    exit 1
    ;;
esac
service_state_recorded=true

"$systemctl_cmd" stop "$service"
if [[ -f "$database" ]]; then
  old_backup=$(mktemp "$state_dir/state.db.before-restore.XXXXXXXX")
  cp -a -- "$database" "$old_backup"
  chmod 0600 "$old_backup"
  had_old=true
fi

# 先武装回滚标记，消除 mv 成功与 trap 得知已替换之间的信号窗口。
replacement_done=true
mv -f -- "$temp_db" "$database"
temp_db=
rm -f -- "$database-wal" "$database-shm"

if (( backup_schema < expected_schema )); then
  if ! migration_schema=$("$y2b_cmd" migrate --database "$database"); then
    echo "数据库从 schema v$backup_schema 迁移到 v$expected_schema 失败" >&2
    exit 1
  fi
  [[ "$migration_schema" == "$expected_schema" ]] || {
    echo "y2b 迁移后报告 schema v${migration_schema}，期望 v$expected_schema" >&2
    exit 1
  }
fi

if [[ "$was_active" == true ]]; then
  "$systemctl_cmd" start "$service"
fi

# active 服务启动时可能短暂占用数据库；迁移本身已经在恢复服务状态前显式完成。
migrated_schema=
for _ in {1..30}; do
  if migrated_schema=$(read_schema "$database" 2>/dev/null) &&
    [[ "$migrated_schema" == "$expected_schema" ]]; then
    break
  fi
  [[ "$was_active" == true ]] || break
  sleep 1
done
[[ "$migrated_schema" == "$expected_schema" ]] || {
  echo "恢复后 schema 版本为 ${migrated_schema:-不可读}，期望 $expected_schema" >&2
  exit 1
}
check_integrity "$database"

if [[ "$was_active" == true ]]; then
  wait_for_stable_service
else
  set +e
  "$systemctl_cmd" is-active --quiet "$service"
  final_service_status=$?
  set -e
  [[ "$final_service_status" == 3 ]] || {
    echo "恢复后服务未保持 inactive 状态" >&2
    exit 1
  }
fi

echo "恢复完成: schema v${migrated_schema}，旧库副本 ${old_backup:-无}"
