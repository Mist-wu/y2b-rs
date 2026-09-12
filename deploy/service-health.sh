#!/usr/bin/env bash
# 部署和恢复共用的参数预检及服务稳定性采样。
health_interval=${Y2B_HEALTH_INTERVAL_SECONDS:-1}
health_window_seconds=${Y2B_HEALTH_WINDOW_SECONDS:-10}
health_max_checks=${Y2B_HEALTH_MAX_CHECKS:-30}

prepare_service_health_check() {
  if [[ ! "$health_max_checks" =~ ^[1-9][0-9]*$ ]]; then
    echo "Y2B_HEALTH_MAX_CHECKS 必须是正整数: $health_max_checks" >&2
    return 2
  fi
  if [[ ! "$health_interval" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "Y2B_HEALTH_INTERVAL_SECONDS 必须是非负数: $health_interval" >&2
    return 2
  fi
  if [[ ! "$health_window_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "Y2B_HEALTH_WINDOW_SECONDS 必须是非负数: $health_window_seconds" >&2
    return 2
  fi
  command -v python3 >/dev/null 2>&1 || {
    echo "python3 是健康窗口预检的必需依赖" >&2
    return 2
  }
  # Fraction 精确处理小数；向上取整保证最后一次采样覆盖完整窗口。
  # 0/0 保留测试所需的两次立即采样，零间隔不能跳过正数稳定窗口。
  required_stable_samples=$(python3 - "$health_interval" "$health_window_seconds" "$health_max_checks" <<'PYTHON'
from fractions import Fraction
import sys

try:
    interval, window = map(Fraction, sys.argv[1:3])
    if interval == 0:
        if window != 0:
            raise ValueError("零健康间隔要求 Y2B_HEALTH_WINDOW_SECONDS=0")
        samples = 2
    else:
        periods = window / interval
        samples = max(2, -(-periods.numerator // periods.denominator) + 1)
    if max(samples, int(sys.argv[3])) >= sys.maxsize:
        raise ValueError("健康采样次数超出 shell 整数范围")
except (ValueError, OverflowError) as error:
    print(f"健康窗口配置无效: {error}", file=sys.stderr)
    sys.exit(2)
print(samples)
PYTHON
  ) || return 2
}

wait_for_stable_service() {
  local systemctl_cmd=$1
  local service=$2
  local attempt
  local active_seen=false
  local stable_samples
  local first_pid=
  local current_pid=
  local current_restarts=
  local health_output

  # 阶段一：等服务进入 active。
  for ((attempt = 1; attempt <= health_max_checks; attempt++)); do
    if "$systemctl_cmd" is-active --quiet "$service"; then
      active_seen=true
      break
    fi
    if (( attempt < health_max_checks )); then
      sleep "$health_interval" || return 1
    fi
  done
  if [[ "$active_seen" != true ]]; then
    echo "$service 健康检查失败" >&2
    return 1
  fi

  # 阶段二：稳定窗口内多次采样，全程 active、MainPID 不变、NRestarts 为 0。
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
      sleep "$health_interval" || return 1
    fi
  done
}
