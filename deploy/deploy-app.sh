#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "$0")/.." && pwd)
binary=${1:-"$root_dir/target/x86_64-unknown-linux-musl/release/y2b"}
[[ -x "$binary" ]] || { echo "missing executable: $binary" >&2; exit 1; }

app_root=${Y2B_APP_ROOT:-/opt/y2b}
state_dir=${Y2B_STATE_DIR:-/var/lib/y2b}
database=${Y2B_DATABASE:-"$state_dir/state.db"}
config_file=${Y2B_CONFIG:-/etc/y2b/config.toml}
env_file=${Y2B_ENV_FILE:-/etc/y2b/y2b.env}
unit_dir=${Y2B_SYSTEMD_UNIT_DIR:-/etc/systemd/system}
bin_link=${Y2B_BIN_LINK:-/usr/local/bin/y2b}
key_tool_link=${Y2B_KEY_TOOL_LINK:-/usr/local/sbin/y2b-set-deepseek-key}
service=${Y2B_SERVICE:-y2b-watch.service}
sqlite3_cmd=${Y2B_SQLITE3:-sqlite3}
systemctl_cmd=${Y2B_SYSTEMCTL:-systemctl}
mv_cmd=${Y2B_MV:-mv}
credential_owner=${Y2B_CREDENTIAL_OWNER:-root:root}
idle_interval=${Y2B_IDLE_INTERVAL_SECONDS:-5}
idle_max_checks=${Y2B_IDLE_MAX_CHECKS:-60}
health_interval=${Y2B_HEALTH_INTERVAL_SECONDS:-1}
health_max_checks=${Y2B_HEALTH_MAX_CHECKS:-30}
hold_lease_seconds=${Y2B_HOLD_LEASE_SECONDS:-3600}
release_keep=${Y2B_RELEASE_KEEP:-5}
releases_dir="$app_root/releases"
current_link="$app_root/current"
backup_dir="$state_dir/backups/deploy"

require_positive_integer() {
  local name=$1
  local value=$2
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name 必须是正整数: $value" >&2
    exit 2
  fi
}

require_nonnegative_number() {
  local name=$1
  local value=$2
  if [[ ! "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "$name 必须是非负数: $value" >&2
    exit 2
  fi
}

require_positive_integer Y2B_IDLE_MAX_CHECKS "$idle_max_checks"
require_positive_integer Y2B_HEALTH_MAX_CHECKS "$health_max_checks"
require_positive_integer Y2B_HOLD_LEASE_SECONDS "$hold_lease_seconds"
require_positive_integer Y2B_RELEASE_KEEP "$release_keep"
require_nonnegative_number Y2B_IDLE_INTERVAL_SECONDS "$idle_interval"
require_nonnegative_number Y2B_HEALTH_INTERVAL_SECONDS "$health_interval"

revision=${Y2B_REVISION:-}
if [[ -z "$revision" ]]; then
  root_name=$(basename "$root_dir")
  binary_name=$(basename "$binary")
  if [[ "$root_name" =~ ^y2b-release-([0-9a-f]{7,40})$ ]]; then
    revision=${BASH_REMATCH[1]}
  elif [[ "$binary_name" =~ ^y2b-([0-9a-f]{7,40})$ ]]; then
    revision=${BASH_REMATCH[1]}
  else
    command -v sha256sum >/dev/null 2>&1 || {
      echo "无法推导 release revision，且 sha256sum 不可用" >&2
      exit 1
    }
    binary_digest=$(sha256sum "$binary")
    revision=${binary_digest%% *}
    revision=${revision:0:12}
  fi
fi
[[ "$revision" =~ ^[0-9a-f]{7,40}$ ]] || {
  echo "release revision 只能是 7 到 40 位小写十六进制: $revision" >&2
  exit 2
}

deployment_timestamp=${Y2B_DEPLOY_TIMESTAMP:-$(date -u +%Y%m%dT%H%M%SZ)}
[[ "$deployment_timestamp" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || {
  echo "部署时间标识格式无效: $deployment_timestamp" >&2
  exit 2
}
owner="deploy:${revision}:${deployment_timestamp}:$$"
owner_tag="${revision}-${deployment_timestamp}-$$"
release_dir="$releases_dir/$revision"
staging_dir="$releases_dir/.${revision}.staging-${deployment_timestamp}-$$"
unit_path="$unit_dir/$service"
unit_temp="${unit_path}.release-${owner_tag}"
current_temp="$app_root/.current-${owner_tag}"

# 所有会拒绝部署的静态检查都放在 maintenance hold 之前。
for resource in \
  "$root_dir/pi/y2b-extension.ts" \
  "$root_dir/pi/policy.json" \
  "$root_dir/pi/audit-policy.json" \
  "$root_dir/pi/brawl-stars-glossary.json" \
  "$root_dir/Cargo.lock" \
  "$root_dir/deploy/y2b-watch.service" \
  "$root_dir/deploy/restore.sh" \
  "$root_dir/deploy/install-ytdlp-pot-provider.sh" \
  "$root_dir/deploy/y2b-set-deepseek-key.py"; do
  [[ -f "$resource" ]] || { echo "missing release resource: $resource" >&2; exit 1; }
done

if [[ ! -f "$config_file" ]]; then
  install -d -m 0755 "$(dirname "$config_file")"
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

[[ -f "$env_file" ]] || { echo "missing credential file: $env_file" >&2; exit 1; }
if env_owner=$(stat -c '%U:%G' "$env_file" 2>/dev/null); then
  :
else
  env_owner=$(stat -f '%Su:%Sg' "$env_file")
fi
[[ "$env_owner" == "$credential_owner" ]] || {
  echo "credential file must be owned by $credential_owner: $env_file" >&2
  exit 1
}
if env_mode=$(stat -c '%a' "$env_file" 2>/dev/null); then
  :
else
  env_mode=$(stat -f '%Lp' "$env_file")
fi
[[ "$env_mode" == 600 ]] || {
  echo "credential file must have mode 600: $env_file" >&2
  exit 1
}
if grep -Eq '^[[:space:]]*(export[[:space:]]+)?DEEPSEEK_API_KEY[[:space:]]*=' "$env_file"; then
  echo "DeepSeek key must not be stored in $env_file" >&2
  exit 1
fi

command -v python3 >/dev/null 2>&1 || { echo "python3 is required" >&2; exit 1; }
python3 "$root_dir/deploy/y2b-set-deepseek-key.py" --check
command -v "$sqlite3_cmd" >/dev/null 2>&1 || { echo "sqlite3 is required" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 1; }
command -v "$systemctl_cmd" >/dev/null 2>&1 || { echo "systemctl is required" >&2; exit 1; }
# command -v 只能确认命令存在；停服务前必须实际验证原子切换依赖的 -T。
command -v "$mv_cmd" >/dev/null 2>&1 || {
  echo "mv 命令不可用，请检查 Y2B_MV 或安装 mv: $mv_cmd" >&2
  exit 1
}
mv_probe_dir=$(mktemp -d "${TMPDIR:-/tmp}/.y2b-mv-probe.XXXXXXXX")
if ! (
  trap 'rm -rf -- "$mv_probe_dir"' EXIT
  printf 'source\n' >"$mv_probe_dir/source" &&
    printf 'target\n' >"$mv_probe_dir/target" &&
    "$mv_cmd" -Tf -- "$mv_probe_dir/source" "$mv_probe_dir/target"
); then
  echo "mv 不支持 -T，请安装 GNU coreutils 并将 Y2B_MV 指向 GNU mv: $mv_cmd" >&2
  exit 1
fi
[[ -f "$database" ]] || {
  echo "数据库不存在，maintenance status 不会隐式创建数据库: $database" >&2
  exit 1
}
[[ ! -e "$release_dir" && ! -L "$release_dir" ]] || {
  echo "release 已存在，拒绝覆盖不可变目录: $release_dir" >&2
  exit 1
}

previous_target=
maintenance_y2b=
legacy_capture_required=false
if [[ -L "$current_link" ]]; then
  previous_target=$(readlink "$current_link")
  case "$previous_target" in
    releases/*) previous_name=${previous_target#releases/} ;;
    "$releases_dir"/*) previous_name=${previous_target#"$releases_dir"/} ;;
    *)
      echo "current 必须指向 releases 下的版本目录: $current_link -> $previous_target" >&2
      exit 1
      ;;
  esac
  [[ "$previous_name" =~ ^[0-9a-f]{7,40}$ ]] || {
    echo "current 的 release 名称不符合规则: $previous_name" >&2
    exit 1
  }
  [[ -d "$current_link" && -x "$current_link/y2b" ]] || {
    echo "current 没有指向完整 release: $current_link -> $previous_target" >&2
    exit 1
  }
  maintenance_y2b="$current_link/y2b"
elif [[ -e "$current_link" ]]; then
  echo "current 必须是符号链接: $current_link" >&2
  exit 1
else
  # 从旧的固定路径升级时，先只读捕获完整旧版；真正切换仍只有第 7 步的一次 mv -T。
  [[ -x "$bin_link" && -d "$app_root/pi" && ! -L "$app_root/pi" ]] || {
    echo "缺少可成对回滚的上一版 release，拒绝部署" >&2
    exit 1
  }
  for resource in y2b-extension.ts policy.json audit-policy.json brawl-stars-glossary.json; do
    [[ -f "$app_root/pi/$resource" ]] || {
      echo "旧版 Pi 资源不完整，无法建立回滚 release: $app_root/pi/$resource" >&2
      exit 1
    }
  done
  [[ -f "$app_root/Cargo.lock" ]] || {
    echo "旧版 Cargo.lock 缺失，无法建立回滚 release: $app_root/Cargo.lock" >&2
    exit 1
  }
  command -v sha256sum >/dev/null 2>&1 || {
    echo "捕获旧 release 需要 sha256sum" >&2
    exit 1
  }
  maintenance_y2b="$bin_link"
  legacy_capture_required=true
fi

hold_acquired=false
deployment_complete=false
rollback_required=false
database_restore_required=false
release_created=false
migration_backup=
backup_temp=
integrity_output=
rollback_temp=
legacy_staging=
alias_temporaries=()
quarantines=()

cleanup_temporary_files() {
  [[ -z "$backup_temp" || ! -e "$backup_temp" ]] || rm -f -- "$backup_temp"
  [[ -z "$integrity_output" || ! -e "$integrity_output" ]] || rm -f -- "$integrity_output"
  [[ -z "$rollback_temp" || ! -e "$rollback_temp" ]] || rm -f -- "$rollback_temp"
  [[ -z "$legacy_staging" || ! -e "$legacy_staging" ]] || rm -rf -- "$legacy_staging"
  [[ ! -e "$staging_dir" ]] || rm -rf -- "$staging_dir"
  rm -f -- "$current_temp" "$unit_temp"
  local temporary
  for temporary in "${alias_temporaries[@]}"; do
    [[ ! -e "$temporary" && ! -L "$temporary" ]] || rm -f -- "$temporary"
  done
}

check_integrity() {
  local target=$1
  integrity_output=$(mktemp "$state_dir/.deploy-integrity.XXXXXXXX")
  if ! "$sqlite3_cmd" "$target" 'PRAGMA integrity_check;' >"$integrity_output"; then
    echo "SQLite 完整性检查执行失败: $target" >&2
    return 1
  fi
  # sqlite3 发现损坏时也可能返回 0，必须严格匹配唯一一行 `ok`。
  if ! cmp -s <(printf 'ok\n') "$integrity_output"; then
    echo "SQLite 完整性检查未返回唯一一行 ok: $target" >&2
    return 1
  fi
  rm -f -- "$integrity_output"
  integrity_output=
}

atomic_set_current() {
  local target=$1
  rm -f -- "$current_temp"
  ln -s -- "$target" "$current_temp"
  if [[ ! -d "$current_temp" || ! -x "$current_temp/y2b" ]]; then
    echo "拒绝把 current 指向不完整 release: $target" >&2
    rm -f -- "$current_temp"
    return 1
  fi
  # 不得 rm current 再 ln；同文件系统的单次 rename 保证观察者始终看到完整旧版或新版。
  "$mv_cmd" -Tf -- "$current_temp" "$current_link"
}

install_release_alias() {
  local path=$1
  local target=$2
  local temporary="${path}.release-${owner_tag}"
  local quarantine="${path}.before-release-${owner_tag}"

  if [[ -L "$path" && $(readlink "$path") == "$target" ]]; then
    return
  fi
  alias_temporaries+=("$temporary")
  rm -f -- "$temporary"
  if [[ -e "$path" || -L "$path" ]]; then
    [[ ! -e "$quarantine" && ! -L "$quarantine" ]] || {
      echo "兼容路径隔离目录已存在: $quarantine" >&2
      return 1
    }
    "$mv_cmd" -- "$path" "$quarantine"
    quarantines+=("$quarantine")
  fi
  ln -s -- "$target" "$temporary"
  "$mv_cmd" -Tf -- "$temporary" "$path"
}

restore_database() {
  [[ -n "$migration_backup" && -f "$migration_backup" ]] || {
    echo "迁移前备份不可用，无法成对回滚数据库" >&2
    return 1
  }
  rollback_temp=$(mktemp "$state_dir/.state.db.deploy-rollback.XXXXXXXX")
  cp -a -- "$migration_backup" "$rollback_temp"
  chmod 0600 "$rollback_temp"
  check_integrity "$rollback_temp"
  "$mv_cmd" -f -- "$rollback_temp" "$database"
  rollback_temp=
  rm -f -- "$database-wal" "$database-shm"
}

wait_for_service() {
  local attempt
  for ((attempt = 1; attempt <= health_max_checks; attempt++)); do
    if "$systemctl_cmd" is-active --quiet "$service"; then
      "$systemctl_cmd" --no-pager --full status "$service"
      return
    fi
    if (( attempt < health_max_checks )); then
      sleep "$health_interval"
    fi
  done
  echo "$service 健康检查失败" >&2
  set +e
  "$systemctl_cmd" --no-pager --full status "$service" >&2
  set -e
  return 1
}

release_hold() {
  local candidate
  local seen=':'
  for candidate in "$current_link/y2b" "$maintenance_y2b" "$binary"; do
    [[ -x "$candidate" ]] || continue
    if [[ "$seen" == *":$candidate:"* ]]; then
      continue
    fi
    seen+="$candidate:"
    if "$candidate" maintenance release --database "$database" --owner "$owner"; then
      hold_acquired=false
      return
    fi
  done
  echo "maintenance hold 释放失败: owner=$owner" >&2
  return 1
}

remove_quarantines() {
  local quarantine
  for quarantine in "${quarantines[@]}"; do
    [[ ! -e "$quarantine" && ! -L "$quarantine" ]] || rm -rf -- "$quarantine"
  done
  quarantines=()
}

rollback_on_error() {
  local status=$?
  local rollback_ok=true
  trap - EXIT INT TERM
  if [[ "$deployment_complete" == true ]]; then
    cleanup_temporary_files
    return
  fi
  if (( status == 0 )); then
    status=1
  fi

  set +e
  if [[ "$rollback_required" == true ]]; then
    echo "部署失败，开始成对回滚 release 与数据库" >&2
    "$systemctl_cmd" stop "$service" >/dev/null 2>&1
    if ! atomic_set_current "$previous_target"; then
      echo "current 回滚失败，目标应为: $previous_target" >&2
      rollback_ok=false
    fi
    if [[ "$database_restore_required" == true ]] && ! restore_database; then
      rollback_ok=false
    fi
    if [[ "$rollback_ok" == true ]]; then
      if ! "$current_link/y2b" --config "$config_file" check --write-baseline; then
        echo "旧 release 的 y2b check 失败" >&2
        rollback_ok=false
      elif ! "$systemctl_cmd" start "$service"; then
        echo "旧 release 启动失败" >&2
        rollback_ok=false
      elif ! wait_for_service; then
        echo "旧 release 启动后未通过健康检查" >&2
        rollback_ok=false
      fi
    fi
  fi

  # 回滚健康检查结束后才释放锁；数据库已恢复时 current 中的旧二进制与 schema 精确匹配。
  if [[ "$hold_acquired" == true ]] && ! release_hold; then
    rollback_ok=false
  fi
  if [[ "$rollback_ok" == true ]]; then
    remove_quarantines
  fi
  cleanup_temporary_files
  if [[ "$release_created" == true && -d "$release_dir" ]]; then
    if [[ ! -L "$current_link" || $(readlink "$current_link") != "releases/$revision" ]]; then
      rm -rf -- "$release_dir"
    fi
  fi
  if [[ "$rollback_ok" != true ]]; then
    echo "自动回滚未完整成功，请保持服务停止并按迁移前备份手工恢复: ${migration_backup:-尚未生成}" >&2
    status=1
  elif [[ "$rollback_required" == true ]]; then
    echo "成对回滚完成，旧 release 已通过健康检查，maintenance hold 已释放" >&2
  fi
  exit "$status"
}
trap rollback_on_error EXIT
trap 'exit 130' INT TERM

# 先获取维护锁，之后所有 status 都带 owner，避免被自己的锁误判为 blocker。
hold_acquired=true
if ! "$maintenance_y2b" maintenance acquire \
  --database "$database" \
  --owner "$owner" \
  --reason "部署 release $revision" \
  --lease-seconds "$hold_lease_seconds"; then
  hold_acquired=false
  echo "无法获取 maintenance hold" >&2
  exit 1
fi

print_blockers() {
  local status_json=$1
  printf '%s\n' "$status_json" | jq -r \
    '.blockers[] | "阻塞项 kind=\(.kind) count=\(.count) details=\(.details | join("; "))"' >&2
}

wait_for_two_idle_checks() {
  local checks=0
  local consecutive=0
  local status_json

  while (( checks < idle_max_checks )); do
    ((checks += 1))
    "$maintenance_y2b" maintenance renew \
      --database "$database" --owner "$owner" --lease-seconds "$hold_lease_seconds" >/dev/null
    if ! status_json=$("$maintenance_y2b" maintenance status \
      --database "$database" --owner "$owner" --json); then
      echo "无法读取 maintenance blockers" >&2
      return 1
    fi
    if ! printf '%s\n' "$status_json" | jq -e \
      '(.idle | type == "boolean") and (.blockers | type == "array")' >/dev/null; then
      echo "maintenance status 返回无效 JSON: $status_json" >&2
      return 1
    fi

    if printf '%s\n' "$status_json" | jq -e '.idle == true' >/dev/null; then
      ((consecutive += 1))
      echo "maintenance idle 连续检查: $consecutive/2"
      if (( consecutive == 2 )); then
        return
      fi
    else
      consecutive=0
      echo "数据库仍有存量工作，继续等待（${checks}/${idle_max_checks}）" >&2
      print_blockers "$status_json"
    fi
    if (( checks < idle_max_checks )); then
      sleep "$idle_interval"
    fi
  done

  echo "等待两次连续 idle 超时，拒绝继续部署" >&2
  return 1
}

wait_for_two_idle_checks

install -d -m 0755 "$releases_dir"

if [[ "$legacy_capture_required" == true ]]; then
  legacy_digest=$(sha256sum "$bin_link")
  legacy_revision=${legacy_digest%% *}
  legacy_revision=${legacy_revision:0:12}
  if [[ "$legacy_revision" == "$revision" ]]; then
    legacy_revision="0$legacy_revision"
  fi
  legacy_release="$releases_dir/$legacy_revision"
  if [[ ! -d "$legacy_release" ]]; then
    legacy_staging="$releases_dir/.${legacy_revision}.legacy-${deployment_timestamp}-$$"
    install -d -m 0755 "$legacy_staging/pi" "$legacy_staging/deploy"
    install -m 0755 "$bin_link" "$legacy_staging/y2b"
    install -m 0644 "$app_root/pi/y2b-extension.ts" "$legacy_staging/pi/y2b-extension.ts"
    install -m 0644 "$app_root/pi/policy.json" "$legacy_staging/pi/policy.json"
    install -m 0644 "$app_root/pi/audit-policy.json" "$legacy_staging/pi/audit-policy.json"
    install -m 0644 "$app_root/pi/brawl-stars-glossary.json" "$legacy_staging/pi/brawl-stars-glossary.json"
    install -m 0644 "$app_root/Cargo.lock" "$legacy_staging/Cargo.lock"
    install -m 0644 "$root_dir/deploy/y2b-watch.service" "$legacy_staging/deploy/y2b-watch.service"
    "$mv_cmd" -- "$legacy_staging" "$legacy_release"
    legacy_staging=
  fi
  [[ -x "$legacy_release/y2b" && -f "$legacy_release/Cargo.lock" ]] || {
    echo "旧 release 捕获失败: $legacy_release" >&2
    exit 1
  }
  for resource in y2b-extension.ts policy.json audit-policy.json brawl-stars-glossary.json; do
    [[ -f "$legacy_release/pi/$resource" ]] || {
      echo "旧 release 捕获后缺少 Pi 资源: $legacy_release/pi/$resource" >&2
      exit 1
    }
  done
  previous_target="releases/$legacy_revision"
fi

# 新资源先写入隐藏 staging，完整后才发布为不可变 releases/<revision>；此时不碰 current。
install -d -m 0755 "$staging_dir/pi" "$staging_dir/deploy"
install -m 0755 "$binary" "$staging_dir/y2b"
install -m 0644 "$root_dir/pi/y2b-extension.ts" "$staging_dir/pi/y2b-extension.ts"
install -m 0644 "$root_dir/pi/policy.json" "$staging_dir/pi/policy.json"
install -m 0644 "$root_dir/pi/audit-policy.json" "$staging_dir/pi/audit-policy.json"
install -m 0644 "$root_dir/pi/brawl-stars-glossary.json" "$staging_dir/pi/brawl-stars-glossary.json"
install -m 0644 "$root_dir/Cargo.lock" "$staging_dir/Cargo.lock"
for script in bootstrap-server.sh deploy-app.sh install-ytdlp-pot-provider.sh restore.sh; do
  install -m 0755 "$root_dir/deploy/$script" "$staging_dir/deploy/$script"
done
install -m 0755 "$root_dir/deploy/y2b-set-deepseek-key.py" "$staging_dir/deploy/y2b-set-deepseek-key.py"
install -m 0644 "$root_dir/deploy/y2b-watch.service" "$staging_dir/deploy/y2b-watch.service"
"$mv_cmd" -- "$staging_dir" "$release_dir"
release_created=true

# 服务停止前完成迁移前快照及严格完整性校验；缺文件或非单行 ok 都立即拒绝。
"$maintenance_y2b" maintenance renew \
  --database "$database" --owner "$owner" --lease-seconds "$hold_lease_seconds" >/dev/null
install -d -m 0700 "$backup_dir"
backup_temp=$(mktemp "$backup_dir/.state-before-${revision}-${deployment_timestamp}.XXXXXXXX")
rm -f -- "$backup_temp"
[[ "$backup_temp" != *"'"* ]] || { echo "备份路径不能包含单引号" >&2; exit 1; }
"$sqlite3_cmd" "$database" ".backup '$backup_temp'"
[[ -s "$backup_temp" ]] || {
  echo "迁移前备份缺失或为空: $backup_temp" >&2
  exit 1
}
chmod 0600 "$backup_temp"
check_integrity "$backup_temp"
migration_backup="$backup_dir/state-before-${revision}-${deployment_timestamp}.db"
[[ ! -e "$migration_backup" ]] || {
  echo "迁移前备份已存在，拒绝覆盖: $migration_backup" >&2
  exit 1
}
"$mv_cmd" -- "$backup_temp" "$migration_backup"
backup_temp=

database_restore_required=true
rollback_required=true
"$systemctl_cmd" stop "$service"

# 固定兼容路径只间接指向 current；以后 binary、Pi、Cargo.lock 与运维脚本同步切换。
install_release_alias "$app_root/pi" 'current/pi'
install_release_alias "$app_root/Cargo.lock" 'current/Cargo.lock'
install_release_alias "$app_root/deploy" 'current/deploy'
install_release_alias "$bin_link" "$current_link/y2b"
install_release_alias "$key_tool_link" "$current_link/deploy/y2b-set-deepseek-key.py"

atomic_set_current "releases/$revision"

# current 和数据库必须视为一对：从这里开始任何错误都由 EXIT trap 同时恢复。
"$current_link/y2b" migrate --database "$database"
"$current_link/y2b" maintenance renew \
  --database "$database" --owner "$owner" --lease-seconds "$hold_lease_seconds" >/dev/null
"$current_link/y2b" --config "$config_file" check --write-baseline
"$current_link/y2b" maintenance renew \
  --database "$database" --owner "$owner" --lease-seconds "$hold_lease_seconds" >/dev/null

install -m 0644 "$current_link/deploy/y2b-watch.service" "$unit_temp"
"$mv_cmd" -Tf -- "$unit_temp" "$unit_path"
"$systemctl_cmd" daemon-reload
"$systemctl_cmd" enable "$service"
"$systemctl_cmd" start "$service"
wait_for_service

prune_old_releases() {
  local candidates=()
  local directory name current_target protected oldest oldest_mtime mtime index
  shopt -s nullglob
  for directory in "$releases_dir"/*; do
    name=$(basename "$directory")
    if [[ -d "$directory" && ! -L "$directory" && "$name" =~ ^[0-9a-f]{7,40}$ ]]; then
      candidates+=("$directory")
    fi
  done
  shopt -u nullglob
  current_target=$(readlink "$current_link")
  protected="$releases_dir/${current_target##*/}"

  while (( ${#candidates[@]} > release_keep )); do
    oldest=
    oldest_mtime=
    for index in "${!candidates[@]}"; do
      directory=${candidates[$index]}
      if [[ "$directory" == "$protected" || "$directory" == "$releases_dir/${previous_target##*/}" ]]; then
        continue
      fi
      if mtime=$(stat -c '%Y' "$directory" 2>/dev/null); then
        :
      else
        mtime=$(stat -f '%m' "$directory")
      fi
      if [[ -z "$oldest" || "$mtime" -lt "$oldest_mtime" ]]; then
        oldest=$directory
        oldest_mtime=$mtime
      fi
    done
    [[ -n "$oldest" ]] || break
    rm -rf -- "$oldest"
    for index in "${!candidates[@]}"; do
      if [[ "${candidates[$index]}" == "$oldest" ]]; then
        unset 'candidates[index]'
        candidates=("${candidates[@]}")
        break
      fi
    done
  done
}

prune_old_releases
remove_quarantines
cleanup_temporary_files
# 健康检查后的最终窗口不再接受中断：先释放 hold，紧接着解除 EXIT 回滚 trap。
trap '' INT TERM
release_hold
deployment_complete=true
trap - EXIT INT TERM

echo "部署完成: release=$revision backup=$migration_backup"
