    #[test]
    fn deployment_scripts_install_and_preflight_sqlite() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bootstrap = fs::read_to_string(root.join("deploy/bootstrap-server.sh")).unwrap();
        let install = bootstrap
            .lines()
            .find(|line| line.starts_with("apt-get install -y "))
            .unwrap();
        assert!(
            install
                .split_whitespace()
                .any(|dependency| dependency == "sqlite3")
        );
        assert!(bootstrap.contains("install -d /opt/y2b/releases"));

        let deploy = fs::read_to_string(root.join("deploy/deploy-app.sh")).unwrap();
        let sqlite_check = deploy.find("command -v \"$sqlite3_cmd\"").unwrap();
        let mv_probe = deploy
            .find("\"$mv_cmd\" -Tf -- \"$mv_probe_dir/source\" \"$mv_probe_dir/target\"")
            .unwrap();
        let hold_table_check = deploy.find("maintenance_hold_tables=").unwrap();
        let acquire = deploy.find("maintenance acquire").unwrap();
        let idle_wait = deploy.find("\nwait_for_two_idle_checks\n").unwrap();
        assert!(sqlite_check < acquire);
        assert!(mv_probe < acquire);
        assert!(hold_table_check < acquire);
        assert!(acquire < idle_wait);
        assert!(!deploy.contains("wait_for_two_idle_checks_bootstrap"));
        assert!(!deploy.contains("legacy_capture_required"));
        assert!(deploy.contains(
            "maintenance status \\\n      --database \"$database\" --owner \"$owner\" --json"
        ));
        assert!(deploy.contains("\"$mv_cmd\" -Tf -- \"$current_temp\" \"$current_link\""));
        assert!(!deploy.contains("rm -f -- \"$current_link\""));

        let service = fs::read_to_string(root.join("deploy/y2b-watch.service")).unwrap();
        assert!(service.contains("ExecStart=/opt/y2b/current/y2b"));
        assert!(service.contains("/opt/y2b/current/pi/policy.json"));
    }

    fn write_executable(path: &Path, content: &str) {
        fs::write(path, content).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    const OLD_DEPLOY_REVISION: &str = "aaaaaaaaaaaa";
    const NEW_DEPLOY_REVISION: &str = "bbbbbbbbbbbb";

    struct DeployFixture {
        _temp: tempfile::TempDir,
        candidate: PathBuf,
        app_root: PathBuf,
        state_dir: PathBuf,
        database: PathBuf,
        config: PathBuf,
        env_file: PathBuf,
        unit_dir: PathBuf,
        local_bin: PathBuf,
        local_sbin: PathBuf,
        current: PathBuf,
        hold: PathBuf,
        claim_attempt: PathBuf,
        claim_blocked: PathBuf,
        claim_acquired: PathBuf,
        service_state: PathBuf,
        systemctl_log: PathBuf,
        events: PathBuf,
        scenario: &'static str,
        schema_version: i64,
        hold_table_exists: bool,
        credential_owner: String,
        path: String,
    }

    fn deploy_fixture(scenario: &'static str) -> DeployFixture {
        deploy_fixture_with_layout(scenario, false, CURRENT_SCHEMA_VERSION)
    }

    fn deploy_fixture_legacy(scenario: &'static str, schema_version: i64) -> DeployFixture {
        deploy_fixture_with_layout(scenario, true, schema_version)
    }

    fn deploy_fixture_with_layout(
        scenario: &'static str,
        legacy: bool,
        schema_version: i64,
    ) -> DeployFixture {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let bin_dir = root.join("stub-bin");
        let app_root = root.join("opt/y2b");
        let state_dir = root.join("var/lib/y2b");
        let unit_dir = root.join("etc/systemd/system");
        let local_bin = root.join("usr/local/bin");
        let local_sbin = root.join("usr/local/sbin");
        let old_release = app_root.join("releases").join(OLD_DEPLOY_REVISION);
        let mut directories = vec![
            bin_dir.clone(),
            state_dir.join("backups"),
            unit_dir.clone(),
            local_bin.clone(),
            local_sbin.clone(),
            root.join("etc/y2b"),
        ];
        if legacy {
            directories.push(app_root.join("pi"));
        } else {
            directories.push(old_release.join("pi"));
            directories.push(old_release.join("deploy"));
        }
        for directory in directories {
            fs::create_dir_all(directory).unwrap();
        }

        let y2b_stub = r#"#!/usr/bin/env bash
set -euo pipefail
line=" $* "
scenario=${DEPLOY_TEST_SCENARIO:-success}
target=$(readlink "$DEPLOY_TEST_CURRENT" 2>/dev/null || printf 'none')
printf 'target=%s args=%s\n' "$target" "$*" >>"$DEPLOY_TEST_Y2B_LOG"

value_for() {
  local wanted=$1
  shift
  while (( $# > 0 )); do
    if [[ $1 == "$wanted" ]]; then
      printf '%s\n' "$2"
      return
    fi
    shift
  done
  return 1
}

record_event() {
  printf '%s\n' "$1" >>"$DEPLOY_TEST_EVENTS"
}

if [[ "$line" == *" maintenance acquire "* ]]; then
  owner=$(value_for --owner "$@")
  [[ ! -e "$DEPLOY_TEST_HOLD" ]] || exit 1
  printf '%s\n' "$owner" >"$DEPLOY_TEST_HOLD"
  record_event "acquire:$owner"
  printf 'acquired\n'
elif [[ "$line" == *" maintenance renew "* ]]; then
  owner=$(value_for --owner "$@")
  [[ -f "$DEPLOY_TEST_HOLD" && $(<"$DEPLOY_TEST_HOLD") == "$owner" ]] || exit 1
  if [[ "$scenario" == race && -f "$DEPLOY_TEST_FIRST_IDLE" && ! -f "$DEPLOY_TEST_CLAIM_ATTEMPT" ]]; then
    printf 'attempted\n' >"$DEPLOY_TEST_CLAIM_ATTEMPT"
    if [[ -f "$DEPLOY_TEST_HOLD" ]]; then
      printf 'blocked\n' >"$DEPLOY_TEST_CLAIM_BLOCKED"
      record_event 'claim:blocked-by-maintenance'
    else
      printf 'acquired\n' >"$DEPLOY_TEST_CLAIM_ACQUIRED"
      record_event 'claim:acquired'
    fi
  fi
  printf 'renewed\n'
elif [[ "$line" == *" maintenance status "* ]]; then
  if [[ "$line" != *" --owner "* ]]; then
    # 健康检查探针：观察者视角不要求持锁，只证明进程能打开数据库并响应。
    record_event "health-probe:$target"
    printf '%s\n' '{"checked_at":"test","idle":true,"hold":null,"expired_hold":null,"blockers":[]}'
    exit 0
  fi
  owner=$(value_for --owner "$@")
  [[ -f "$DEPLOY_TEST_HOLD" && $(<"$DEPLOY_TEST_HOLD") == "$owner" ]] || exit 1
  count=0
  [[ ! -f "$DEPLOY_TEST_STATUS_COUNT" ]] || count=$(<"$DEPLOY_TEST_STATUS_COUNT")
  ((count += 1))
  printf '%s\n' "$count" >"$DEPLOY_TEST_STATUS_COUNT"
  case "$scenario" in
    blocker)
      printf '%s\n' '{"checked_at":"test","idle":false,"hold":null,"expired_hold":null,"blockers":[{"kind":"active_claims","count":1,"details":["job-42:upload:worker-7"]}]}'
      ;;
    live_once)
      printf '%s\n' '{"checked_at":"test","idle":false,"hold":null,"expired_hold":null,"blockers":[{"kind":"live_once_hold","count":1,"details":["owner=LIVE_ONCE:premiere expires_at=2099-01-01 reason=直播首发"]}]}'
      ;;
    *)
      printf '%s\n' '{"checked_at":"test","idle":true,"hold":null,"expired_hold":null,"blockers":[]}'
      if (( count == 1 )); then
        printf 'returned\n' >"$DEPLOY_TEST_FIRST_IDLE"
      fi
      ;;
  esac
elif [[ "$line" == *" maintenance release "* ]]; then
  owner=$(value_for --owner "$@")
  [[ -f "$DEPLOY_TEST_HOLD" && $(<"$DEPLOY_TEST_HOLD") == "$owner" ]] || exit 1
  rm -f "$DEPLOY_TEST_HOLD"
  record_event "release:$target"
  printf 'released\n'
elif [[ "$line" == *" config-check "* ]]; then
  record_event 'preflight:config-check'
elif [[ "$line" == *" migrate --database "* ]]; then
  printf 'new-database\n' >"$DEPLOY_TEST_DATABASE"
  record_event "migrate:$target"
  printf '__SCHEMA__\n'
elif [[ "$line" == *" schema-version "* ]]; then
  printf '__SCHEMA__\n'
elif [[ "$line" == *" check "* ]]; then
  content=$(<"$DEPLOY_TEST_DATABASE")
  record_event "check:$target:$content"
  baseline="$DEPLOY_TEST_BASELINE"
  if [[ -f "$baseline" ]]; then
    previous_y2b=$(jq -r '.y2b' "$baseline")
    previous_external=$(jq -r '.external' "$baseline")
    # 模拟真实 check 的两类基线条目：外部依赖漂移是必选失败，y2b 自身漂移仅告警。
    if [[ -n "$previous_external" && "$previous_external" != "external-stable" ]]; then
      echo "FAIL dependency baseline 漂移: external" >&2
      exit 1
    fi
    if [[ -n "$previous_y2b" && "$previous_y2b" != "$target" ]]; then
      record_event 'baseline:y2b-drift-tolerated'
    fi
    python3 - "$baseline" <<'PY'
import hashlib
import json
from pathlib import Path
import sys
for item in json.loads(Path(sys.argv[1]).read_text())["items"]:
    if hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() != item["sha256"]:
        raise SystemExit(f"FAIL dependency baseline 漂移: {item['name']}")
PY
  fi
  if [[ "$line" == *" --write-baseline "* ]]; then
    [[ -f "$baseline" ]] || printf '{"items":[]}\n' >"$baseline"
    jq --arg target "$target" '.y2b=$target | .external="external-stable"' \
      "$baseline" >"$baseline.new"
    mv "$baseline.new" "$baseline"
  fi
else
  echo "未预期的 y2b 参数: $*" >&2
  exit 2
fi
"#
        .replace("__SCHEMA__", &CURRENT_SCHEMA_VERSION.to_string());
        let old_y2b_stub = "#!/usr/bin/env bash\nexit 2\n";
        let candidate = bin_dir.join("y2b");
        write_executable(&candidate, &y2b_stub);

        let current = app_root.join("current");
        if legacy {
            write_executable(&local_bin.join("y2b"), old_y2b_stub);
            for resource in [
                "y2b-extension.ts",
                "policy.json",
                "audit-policy.json",
                "brawl-stars-glossary.json",
            ] {
                fs::write(app_root.join("pi").join(resource), resource).unwrap();
            }
            fs::write(app_root.join("Cargo.lock"), "old lock\n").unwrap();
        } else {
            write_executable(&old_release.join("y2b"), &y2b_stub);
            for resource in [
                "y2b-extension.ts",
                "policy.json",
                "audit-policy.json",
                "brawl-stars-glossary.json",
            ] {
                fs::write(old_release.join("pi").join(resource), resource).unwrap();
            }
            fs::write(old_release.join("Cargo.lock"), "old lock\n").unwrap();
            write_executable(
                &old_release.join("deploy/y2b-set-deepseek-key.py"),
                "#!/usr/bin/env python3\n",
            );
            symlink(format!("releases/{OLD_DEPLOY_REVISION}"), &current).unwrap();
            symlink("current/pi", app_root.join("pi")).unwrap();
            symlink("current/Cargo.lock", app_root.join("Cargo.lock")).unwrap();
            symlink("current/deploy", app_root.join("deploy")).unwrap();
            symlink(current.join("y2b"), local_bin.join("y2b")).unwrap();
        }

        let database = state_dir.join("state.db");
        fs::write(&database, "old-database\n").unwrap();
        let config = root.join("etc/y2b/config.toml");
        fs::write(
            &config,
            format!(
                "[runtime]\ndatabase = \"{}\"\n[ai]\nextension = \"{}/pi/y2b-extension.ts\"\npolicy = \"{}/pi/policy.json\"\n",
                database.display(),
                app_root.display(),
                app_root.display()
            ),
        )
        .unwrap();
        let env_file = root.join("etc/y2b/y2b.env");
        fs::write(&env_file, "YOUTUBE_API_KEY=test-only\n").unwrap();
        let mut env_permissions = fs::metadata(&env_file).unwrap().permissions();
        env_permissions.set_mode(0o600);
        fs::set_permissions(&env_file, env_permissions).unwrap();

        let service_state = root.join("service-state");
        fs::write(&service_state, "active\n").unwrap();
        let systemctl_log = root.join("systemctl.log");
        let events = root.join("events.log");
        write_executable(
            &bin_dir.join("systemctl"),
            r#"#!/usr/bin/env bash
set -euo pipefail
target=$(readlink "$DEPLOY_TEST_CURRENT" 2>/dev/null || printf 'none')
content=$(<"$DEPLOY_TEST_DATABASE")
printf '%s target=%s database=%s\n' "$*" "$target" "$content" >>"$DEPLOY_TEST_SYSTEMCTL_LOG"
line=" $* "
if [[ ${1:-} == stop ]]; then
  printf 'inactive\n' >"$DEPLOY_TEST_SERVICE_STATE"
  printf 'stop:%s:%s\n' "$target" "$content" >>"$DEPLOY_TEST_EVENTS"
elif [[ ${1:-} == start ]]; then
  if [[ "$DEPLOY_TEST_SCENARIO" == health_failure && "$target" == "releases/bbbbbbbbbbbb" ]]; then
    printf 'failed\n' >"$DEPLOY_TEST_SERVICE_STATE"
  else
    printf 'active\n' >"$DEPLOY_TEST_SERVICE_STATE"
  fi
  printf 'start:%s:%s\n' "$target" "$content" >>"$DEPLOY_TEST_EVENTS"
elif [[ ${1:-} == is-active ]]; then
  if [[ "$DEPLOY_TEST_SCENARIO" == health_exit && "$target" == "releases/bbbbbbbbbbbb" ]]; then
    count=0
    [[ ! -f "$DEPLOY_TEST_ISACTIVE_COUNT" ]] || count=$(<"$DEPLOY_TEST_ISACTIVE_COUNT")
    ((count += 1))
    printf '%s\n' "$count" >"$DEPLOY_TEST_ISACTIVE_COUNT"
    if (( count >= 3 )); then
      exit 3
    fi
  fi
  [[ $(<"$DEPLOY_TEST_SERVICE_STATE") == active ]] && exit 0
  exit 3
elif [[ ${1:-} == show ]]; then
  pid=${DEPLOY_TEST_MAINPID:-4242}
  restarts=${DEPLOY_TEST_NRESTARTS:-0}
  if [[ "$target" == "releases/bbbbbbbbbbbb" ]]; then
    case "$DEPLOY_TEST_SCENARIO" in
      health_pid_change)
        if [[ -f "$DEPLOY_TEST_HEALTH_SAMPLE" ]]; then
          pid=4243
        else
          printf 'sampled\n' >"$DEPLOY_TEST_HEALTH_SAMPLE"
        fi
        ;;
      health_restart)
        if [[ -f "$DEPLOY_TEST_HEALTH_SAMPLE" ]]; then
          restarts=1
        else
          printf 'sampled\n' >"$DEPLOY_TEST_HEALTH_SAMPLE"
        fi
        ;;
    esac
  fi
  printf '%s\n' "$pid"
  printf '%s\n' "$restarts"
elif [[ "$line" == *" status "* ]]; then
  [[ $(<"$DEPLOY_TEST_SERVICE_STATE") == active ]] && exit 0
  exit 3
elif [[ ${1:-} == daemon-reload || ${1:-} == enable ]]; then
  :
else
  echo "未预期的 systemctl 参数: $*" >&2
  exit 2
fi
"#,
        );

        let sqlite3_stub = r#"#!/usr/bin/env bash
set -euo pipefail
database=$1
query=$2
if [[ "$query" == .backup\ * ]]; then
  target=${query#.backup \'}
  target=${target%\'}
  printf 'backup:%s\n' "$target" >>"$DEPLOY_TEST_EVENTS"
  case "$DEPLOY_TEST_SCENARIO" in
    backup_missing) : ;;
    backup_corrupt) printf 'damaged-backup\n' >"$target" ;;
    *) cp "$database" "$target" ;;
  esac
elif [[ "$query" == *integrity_check* ]]; then
  if grep -q '^damaged' "$database"; then
    printf 'row 1 broken\nrow 2 missing\n'
  else
    printf 'ok\n'
  fi
elif [[ "$query" == *sqlite_master* ]]; then
  content=$(<"$database")
  if [[ "$content" == new-database* ]]; then
    printf '1\n'
  else
    printf '%s\n' "${DEPLOY_TEST_MAINTENANCE_HOLD_TABLE:-0}"
  fi
elif [[ "$query" == *schema_migrations* ]]; then
  content=$(<"$database")
  if [[ "$content" == new-database* ]]; then
    printf '__SCHEMA__\n'
  else
    printf '%s\n' "${DEPLOY_TEST_SCHEMA_VERSION:-__SCHEMA__}"
  fi
else
  echo "未预期的 sqlite3 查询: $query" >&2
  exit 2
fi
"#
        .replace("__SCHEMA__", &CURRENT_SCHEMA_VERSION.to_string());
        write_executable(&bin_dir.join("sqlite3"), &sqlite3_stub);

        let python = std::process::Command::new("python3")
            .args(["-c", "import sys; print(sys.executable)"])
            .output()
            .unwrap();
        assert!(python.status.success());
        let python = String::from_utf8(python.stdout).unwrap().trim().to_owned();
        write_executable(
            &bin_dir.join("python3"),
            &format!(
                r#"#!/usr/bin/env bash
set -euo pipefail
if [[ ${{1:-}} == *y2b-set-deepseek-key.py ]]; then
  exit 0
fi
exec "{python}" "$@"
"#
            ),
        );
        write_executable(
            &bin_dir.join("mv"),
            &format!(
                r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "$DEPLOY_TEST_SCENARIO" == mv_without_t ]]; then
  for argument in "$@"; do
    case "$argument" in
      -*T*)
        echo 'mv: illegal option -- T' >&2
        exit 64
        ;;
    esac
  done
fi
operands=()
for argument in "$@"; do
  case "$argument" in
    --|-f|-T|-Tf|-fT) ;;
    -*) ;;
    *) operands+=("$argument") ;;
  esac
done
(( ${{#operands[@]}} == 2 )) || {{ echo "mv 测试桩参数错误: $*" >&2; exit 2; }}
if [[ ${{operands[1]}} == "$DEPLOY_TEST_CURRENT" ]]; then
  if [[ "$DEPLOY_TEST_SCENARIO" == non_atomic ]]; then
    rm -f -- "${{operands[1]}}"
  fi
  sleep 0.02
fi
exec "{python}" - "${{operands[0]}}" "${{operands[1]}}" <<'PY'
import os
import sys
os.replace(sys.argv[1], sys.argv[2])
PY
"#
            ),
        );

        let user = std::process::Command::new("id")
            .arg("-un")
            .output()
            .unwrap();
        let group = std::process::Command::new("id")
            .arg("-gn")
            .output()
            .unwrap();
        assert!(user.status.success() && group.status.success());
        let credential_owner = format!(
            "{}:{}",
            String::from_utf8(user.stdout).unwrap().trim(),
            String::from_utf8(group.stdout).unwrap().trim()
        );
        let path = format!(
            "{}:{}",
            bin_dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );

        DeployFixture {
            _temp: temp,
            candidate,
            app_root,
            state_dir,
            database,
            config,
            env_file,
            unit_dir,
            local_bin,
            local_sbin,
            current,
            hold: root.join("maintenance-hold"),
            claim_attempt: root.join("claim-attempt"),
            claim_blocked: root.join("claim-blocked"),
            claim_acquired: root.join("claim-acquired"),
            service_state,
            systemctl_log,
            events,
            scenario,
            schema_version,
            hold_table_exists: schema_version >= 22,
            credential_owner,
            path,
        }
    }

    impl DeployFixture {
        fn write_baseline(&self, y2b: &str, external: &str) -> serde_json::Value {
            use sha2::{Digest, Sha256};
            let items: Vec<_> = [
                ("pi-policy", "policy.json"),
                ("pi-audit-policy", "audit-policy.json"),
                ("brawl-stars-glossary", "brawl-stars-glossary.json"),
            ]
            .into_iter()
            .map(|(name, file)| {
                let path = self.app_root.join("pi").join(file);
                let hash = format!("{:x}", Sha256::digest(fs::read(&path).unwrap()));
                serde_json::json!({"name": name, "path": path, "sha256": hash})
            })
            .collect();
            let baseline = serde_json::json!({"y2b": y2b, "external": external, "items": items});
            fs::write(
                self.state_dir.join("dependency-baseline.json"),
                serde_json::to_vec_pretty(&baseline).unwrap(),
            )
            .unwrap();
            baseline
        }

        fn command(&self, idle_max_checks: usize) -> std::process::Command {
            let mut command = std::process::Command::new("bash");
            command
                .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/deploy-app.sh"))
                .arg(&self.candidate)
                .env("PATH", &self.path)
                .env("Y2B_APP_ROOT", &self.app_root)
                .env("Y2B_STATE_DIR", &self.state_dir)
                .env("Y2B_DATABASE", &self.database)
                .env("Y2B_CONFIG", &self.config)
                .env("Y2B_ENV_FILE", &self.env_file)
                .env("Y2B_SYSTEMD_UNIT_DIR", &self.unit_dir)
                .env("Y2B_BIN_LINK", self.local_bin.join("y2b"))
                .env(
                    "Y2B_KEY_TOOL_LINK",
                    self.local_sbin.join("y2b-set-deepseek-key"),
                )
                .env("Y2B_SERVICE", "y2b-test.service")
                .env("Y2B_CREDENTIAL_OWNER", &self.credential_owner)
                .env("Y2B_REVISION", NEW_DEPLOY_REVISION)
                .env("Y2B_DEPLOY_TIMESTAMP", "20260901T010203Z")
                .env("Y2B_IDLE_INTERVAL_SECONDS", "0")
                .env("Y2B_IDLE_MAX_CHECKS", idle_max_checks.to_string())
                .env("Y2B_HEALTH_INTERVAL_SECONDS", "0")
                .env("Y2B_HEALTH_MAX_CHECKS", "2")
                .env("Y2B_HEALTH_WINDOW_SECONDS", "0")
                .env(
                    "DEPLOY_TEST_HEALTH_SAMPLE",
                    self.state_dir.join("health-sample"),
                )
                .env(
                    "DEPLOY_TEST_ISACTIVE_COUNT",
                    self.state_dir.join("isactive-count"),
                )
                .env("Y2B_HOLD_LEASE_SECONDS", "60")
                .env("Y2B_RELEASE_KEEP", "5")
                .env("DEPLOY_TEST_SCENARIO", self.scenario)
                .env(
                    "DEPLOY_TEST_SCHEMA_VERSION",
                    self.schema_version.to_string(),
                )
                .env(
                    "DEPLOY_TEST_MAINTENANCE_HOLD_TABLE",
                    if self.hold_table_exists { "1" } else { "0" },
                )
                .env("DEPLOY_TEST_CURRENT", &self.current)
                .env("DEPLOY_TEST_DATABASE", &self.database)
                .env("DEPLOY_TEST_HOLD", &self.hold)
                .env("DEPLOY_TEST_FIRST_IDLE", self.state_dir.join("first-idle"))
                .env(
                    "DEPLOY_TEST_STATUS_COUNT",
                    self.state_dir.join("status-count"),
                )
                .env("DEPLOY_TEST_CLAIM_ATTEMPT", &self.claim_attempt)
                .env("DEPLOY_TEST_CLAIM_BLOCKED", &self.claim_blocked)
                .env("DEPLOY_TEST_CLAIM_ACQUIRED", &self.claim_acquired)
                .env("DEPLOY_TEST_SERVICE_STATE", &self.service_state)
                .env("DEPLOY_TEST_SYSTEMCTL_LOG", &self.systemctl_log)
                .env("DEPLOY_TEST_EVENTS", &self.events)
                .env(
                    "DEPLOY_TEST_BASELINE",
                    self.state_dir.join("dependency-baseline.json"),
                )
                .env("DEPLOY_TEST_Y2B_LOG", self.state_dir.join("y2b.log"));
            command
        }

        fn run(&self, idle_max_checks: usize) -> Output {
            self.command(idle_max_checks).output().unwrap()
        }
    }

    fn output_detail(output: &Output) -> String {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    #[test]
    fn service_health_samples_cover_decimal_and_nondivisible_windows() {
        // Observe the shared sampler's probes and sleeps without wall-clock waits.
        let probe = r#"set -euo pipefail
source "$1"
prepare_service_health_check
systemctl_stub() {
  case "$1" in
    is-active) return 0 ;;
    show) printf '123\n0\n'; printf 'sample\n' >&2 ;;
    *) return 2 ;;
  esac
}
sleep() { printf 'sleep:%s\n' "$1"; }
wait_for_stable_service systemctl_stub test.service
"#;
        for (interval, window, samples) in [
            ("0.5", "10", 21),
            ("3", "10", 5),
            ("1", "0.5", 2),
            ("0.1", "0.3", 4),
            ("0", "0", 2),
        ] {
            let output = std::process::Command::new("bash")
                .args(["-c", probe, "service-health-test"])
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/deploy/service-health.sh"
                ))
                .env("Y2B_HEALTH_INTERVAL_SECONDS", interval)
                .env("Y2B_HEALTH_WINDOW_SECONDS", window)
                .env("Y2B_HEALTH_MAX_CHECKS", "2")
                .output()
                .unwrap();
            let detail = output_detail(&output);
            assert!(output.status.success(), "{interval}/{window}: {detail}");
            assert_eq!(
                String::from_utf8_lossy(&output.stderr)
                    .lines()
                    .filter(|line| *line == "sample")
                    .count(),
                samples,
                "{interval}/{window}: {detail}"
            );
            assert_eq!(
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .collect::<Vec<_>>(),
                vec![format!("sleep:{interval}"); samples - 1],
                "{interval}/{window}: {detail}"
            );
        }
    }

    #[test]
    fn deployment_scripts_reject_invalid_health_parameters_before_runtime_changes() {
        let deploy = deploy_fixture("success");
        let restore = restore_fixture(true);
        for (interval, window, max_checks) in [
            ("invalid", "10", "2"),
            ("1", "-1", "2"),
            ("1", "10", "0"),
            ("1", "10", "0.5"),
            ("0", "10", "2"),
        ] {
            for mut command in [
                deploy.command(3),
                restore.command_with_sqlite(&restore.sqlite3),
            ] {
                let output = command
                    .env("Y2B_HEALTH_INTERVAL_SECONDS", interval)
                    .env("Y2B_HEALTH_WINDOW_SECONDS", window)
                    .env("Y2B_HEALTH_MAX_CHECKS", max_checks)
                    .output()
                    .unwrap();
                assert_eq!(output.status.code(), Some(2), "{}", output_detail(&output));
                assert!(!deploy.hold.exists());
                assert!(!deploy.systemctl_log.exists());
                assert!(!restore.systemctl_log.exists());
                assert_eq!(
                    fs::read_to_string(&restore.service_state).unwrap(),
                    "active\n"
                );
            }
        }
    }

    #[test]
    fn deploy_hold_blocks_claim_between_two_idle_checks() {
        let fixture = deploy_fixture("race");
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(fixture.claim_attempt.exists());
        assert!(fixture.claim_blocked.exists());
        assert!(!fixture.claim_acquired.exists());
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{NEW_DEPLOY_REVISION}"))
        );
    }

    #[test]
    fn deploy_rejects_mv_without_t_before_acquiring_hold_or_stopping_service() {
        let fixture = deploy_fixture("mv_without_t");
        let mut command = fixture.command(3);
        command.env("TMPDIR", &fixture.state_dir);
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(detail.contains("mv 不支持 -T"), "{detail}");
        assert!(!detail.contains("mv 命令不可用"), "{detail}");
        assert!(!fixture.hold.exists());
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!fixture.systemctl_log.exists());
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(
            !fixture
                .app_root
                .join("releases")
                .join(NEW_DEPLOY_REVISION)
                .exists()
        );
        assert!(fs::read_dir(&fixture.state_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".y2b-mv-probe.")
        }));
    }

    #[test]
    fn deploy_distinguishes_an_unavailable_mv_command() {
        let fixture = deploy_fixture("success");
        let mut command = fixture.command(3);
        command.env("Y2B_MV", fixture.state_dir.join("missing-mv"));
        let output = command.output().unwrap();
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(
            detail.contains("mv 命令不可用，请检查 Y2B_MV 或安装 mv"),
            "{detail}"
        );
        assert!(!detail.contains("mv 不支持 -T"), "{detail}");
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn deploy_reports_blocker_and_refuses_before_install() {
        let fixture = deploy_fixture("blocker");
        let output = fixture.run(1);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(detail.contains("kind=active_claims count=1"), "{detail}");
        assert!(detail.contains("job-42:upload:worker-7"), "{detail}");
        assert!(!fixture.systemctl_log.exists());
        assert!(
            !fixture
                .app_root
                .join("releases")
                .join(NEW_DEPLOY_REVISION)
                .exists()
        );
    }

    #[test]
    fn deploy_reports_live_once_lease_and_refuses_to_continue() {
        let fixture = deploy_fixture("live_once");
        let output = fixture.run(1);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(detail.contains("kind=live_once_hold count=1"), "{detail}");
        assert!(detail.contains("owner=LIVE_ONCE:premiere"), "{detail}");
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn deploy_rejects_missing_or_corrupt_backup_before_stopping_service() {
        for scenario in ["backup_missing", "backup_corrupt"] {
            let fixture = deploy_fixture(scenario);
            let output = fixture.run(3);
            assert!(!output.status.success(), "{scenario}");
            let detail = output_detail(&output);
            assert!(
                detail.contains("迁移前备份缺失或为空")
                    || detail.contains("SQLite 完整性检查未返回唯一一行 ok"),
                "{scenario}: {detail}"
            );
            assert!(!fixture.systemctl_log.exists(), "{scenario}");
            assert_eq!(
                fs::read_to_string(&fixture.database).unwrap(),
                "old-database\n"
            );
        }
    }

    #[test]
    fn deploy_health_failure_rolls_back_release_and_database_as_a_pair() {
        let fixture = deploy_fixture("health_failure");
        let output = fixture.run(3);
        assert!(!output.status.success());
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());

        let events = fs::read_to_string(&fixture.events).unwrap();
        let new_start = events
            .find(&format!(
                "start:releases/{NEW_DEPLOY_REVISION}:new-database"
            ))
            .unwrap();
        let old_start = events
            .find(&format!(
                "start:releases/{OLD_DEPLOY_REVISION}:old-database"
            ))
            .unwrap();
        let release = events
            .find(&format!("release:releases/{OLD_DEPLOY_REVISION}"))
            .unwrap();
        assert!(new_start < old_start && old_start < release, "{events}");
    }

    fn assert_health_window_failure_rolls_back(scenario: &'static str) {
        let fixture = deploy_fixture(scenario);
        let output = fixture.run(3);
        assert!(!output.status.success(), "{}", output_detail(&output));
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());
        let events = fs::read_to_string(&fixture.events).unwrap();
        let new_start = events
            .find(&format!(
                "start:releases/{NEW_DEPLOY_REVISION}:new-database"
            ))
            .unwrap();
        let old_start = events
            .find(&format!(
                "start:releases/{OLD_DEPLOY_REVISION}:old-database"
            ))
            .unwrap();
        assert!(new_start < old_start, "{events}");
    }

    #[test]
    fn deploy_health_check_fails_when_process_restarts_in_window() {
        assert_health_window_failure_rolls_back("health_restart");
    }

    #[test]
    fn deploy_health_check_fails_when_process_exits_in_window() {
        assert_health_window_failure_rolls_back("health_exit");
    }

    #[test]
    fn deploy_health_check_fails_when_mainpid_changes() {
        assert_health_window_failure_rolls_back("health_pid_change");
    }

    #[test]
    fn deploy_rejects_when_external_dependency_drifts() {
        let fixture = deploy_fixture("success");
        fixture.write_baseline("releases/000000000000", "external-drifted");
        let output = fixture.run(3);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(
            detail.contains("FAIL dependency baseline 漂移: external"),
            "{detail}"
        );
        assert!(!detail.contains("自动回滚未完整成功"), "{detail}");
        // 外部依赖漂移在停服前被拦住，current、数据库和运行进程均未变更。
        assert!(!fixture.systemctl_log.exists());
        assert!(!fixture.hold.exists());
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
    }

    #[test]
    fn deploy_refreshes_bundled_resources_and_tolerates_binary_drift() {
        let fixture = deploy_fixture("success");
        let old_baseline = fixture.write_baseline("releases/000000000000", "external-stable");
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        let baseline: serde_json::Value = serde_json::from_slice(
            &fs::read(fixture.state_dir.join("dependency-baseline.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(baseline["external"], old_baseline["external"]);
        // 旧版内嵌提示词没有基线条目；首次升级必须补齐并接受新 release 的文件。
        let prompt = baseline["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["name"] == "ai-system-prompt")
            .unwrap();
        assert_eq!(
            prompt["path"],
            fixture
                .app_root
                .join("pi/system-prompt.txt")
                .to_str()
                .unwrap()
        );
        assert!(
            prompt["sha256"]
                .as_str()
                .is_some_and(|hash| hash.len() == 64)
        );
        for (old, new) in old_baseline["items"]
            .as_array()
            .unwrap()
            .iter()
            .zip(baseline["items"].as_array().unwrap())
        {
            assert_eq!(old["path"], new["path"]);
            assert_ne!(old["sha256"], new["sha256"]);
        }
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(events.contains("baseline:y2b-drift-tolerated"), "{events}");
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{NEW_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "new-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());
    }

    #[test]
    fn deploy_rollback_restores_dependency_baseline_with_release() {
        let fixture = deploy_fixture("health_failure");
        let baseline = fixture.write_baseline(
            &format!("releases/{OLD_DEPLOY_REVISION}"),
            "external-stable",
        );
        let output = fixture.run(3);
        assert!(!output.status.success());
        let detail = output_detail(&output);
        assert!(
            detail.contains("成对回滚完成，旧 release 已通过健康检查"),
            "{detail}"
        );
        assert!(!detail.contains("自动回滚未完整成功"), "{detail}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs::read(fixture.state_dir.join("dependency-baseline.json")).unwrap()
            )
            .unwrap(),
            baseline
        );
        // 新版检查允许自身二进制变更，回滚后恢复与旧 release 对应的依赖基线。
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(events.contains("baseline:y2b-drift-tolerated"), "{events}");
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        assert!(!fixture.hold.exists());
    }

    fn observe_current_switch(
        child: &mut std::process::Child,
        current: &Path,
        releases_dir: &Path,
    ) -> Option<String> {
        let mut broken_observation = None;
        loop {
            // 触发瞬间一次性捕获完整状态：先 lstat 判断存在性与符号链接类型，
            // 再在同一轮内读取目标并校验，避免诊断字符串在窗口关闭后才二次读盘。
            let metadata = fs::symlink_metadata(current);
            let mut problem = match &metadata {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Some("current 缺失（lstat 返回 ENOENT）".to_string())
                }
                Err(error) => Some(format!("current 无法 lstat: {error}")),
                Ok(metadata) if !metadata.file_type().is_symlink() => Some(format!(
                    "current 不是符号链接: file_type={:?}",
                    metadata.file_type()
                )),
                Ok(_) => None,
            };
            if problem.is_none() {
                // 悬空判定不通过 current 做跟随 stat：macOS 在 rename 符号链接的
                // 瞬间会让跟随 stat 偶发 EINVAL，Path::exists 会把它误判成悬空。
                // 改为读取目标后直接校验 release 目录（切换期间该目录始终稳定）。
                if let Ok(target) = fs::read_link(current) {
                    let resolved = current.parent().unwrap().join(&target);
                    if fs::metadata(&resolved).is_err() {
                        problem = Some(format!("current 指向不存在的目录: target={target:?}"));
                    }
                }
            }
            if let Some(problem) = problem
                && broken_observation.is_none()
            {
                let releases = fs::read_dir(releases_dir)
                    .map(|entries| {
                        entries
                            .map(|entry| entry.unwrap().file_name())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                broken_observation = Some(format!("{problem} releases={releases:?}"));
            }
            if child.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::yield_now();
        }
        broken_observation
    }

    #[test]
    fn deploy_switch_never_exposes_a_missing_or_dangling_current() {
        let fixture = deploy_fixture("success");
        let mut command = fixture.command(3);
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().unwrap();
        let broken_observation = observe_current_switch(
            &mut child,
            &fixture.current,
            &fixture.app_root.join("releases"),
        );
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(
            broken_observation.is_none(),
            "current 在切换期间曾缺失或指向不存在的目录: {broken_observation:?}"
        );
    }

    #[test]
    fn deploy_switch_observation_catches_a_non_atomic_switch() {
        let fixture = deploy_fixture("non_atomic");
        let mut command = fixture.command(3);
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().unwrap();
        let broken_observation = observe_current_switch(
            &mut child,
            &fixture.current,
            &fixture.app_root.join("releases"),
        );
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(
            broken_observation.is_some(),
            "非原子切换应被观察者捕获，但未观察到缺失/悬空窗口"
        );
    }

    #[test]
    fn successful_deploy_releases_hold_after_new_service_is_healthy() {
        let fixture = deploy_fixture("success");
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        assert!(!fixture.hold.exists());
        assert!(fixture.current.join("deploy/service-health.sh").is_file());
        let events = fs::read_to_string(&fixture.events).unwrap();
        let start = events
            .find(&format!(
                "start:releases/{NEW_DEPLOY_REVISION}:new-database"
            ))
            .unwrap();
        let release = events
            .find(&format!("release:releases/{NEW_DEPLOY_REVISION}"))
            .unwrap();
        assert!(start < release, "{events}");
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
    }

    #[test]
    fn deploy_rejects_legacy_flat_layout_without_touching_runtime() {
        let fixture = deploy_fixture_legacy("success", CURRENT_SCHEMA_VERSION);
        let output = fixture.run(3);
        assert!(!output.status.success(), "{}", output_detail(&output));
        let detail = output_detail(&output);
        assert!(detail.contains("不再支持旧扁平布局"), "{detail}");
        assert!(detail.contains("一次性布局迁移"), "{detail}");
        assert!(!fixture.hold.exists());
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!events.contains("stop:"), "{events}");
    }

    #[test]
    fn deploy_rejects_database_without_maintenance_hold() {
        let fixture = deploy_fixture_with_layout("success", false, 21);
        let output = fixture.run(3);
        assert!(!output.status.success(), "{}", output_detail(&output));
        let detail = output_detail(&output);
        assert!(detail.contains("schema v21"), "{detail}");
        assert!(detail.contains("不再执行无锁自举"), "{detail}");
        assert!(detail.contains("restore.sh"), "{detail}");
        assert!(!fixture.hold.exists());
        assert_eq!(
            fs::read_link(&fixture.current).unwrap(),
            PathBuf::from(format!("releases/{OLD_DEPLOY_REVISION}"))
        );
        assert_eq!(
            fs::read_to_string(&fixture.database).unwrap(),
            "old-database\n"
        );
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(!events.contains("acquire:"), "{events}");
        assert!(!events.contains("stop:"), "{events}");
    }

    #[test]
    fn deploy_uses_full_hold_when_hold_table_exists_but_schema_is_older() {
        let mut fixture = deploy_fixture_with_layout("success", false, 21);
        fixture.hold_table_exists = true;
        let output = fixture.run(3);
        assert!(output.status.success(), "{}", output_detail(&output));
        let events = fs::read_to_string(&fixture.events).unwrap();
        assert!(events.contains("acquire:"), "{events}");
        assert!(
            events.contains(&format!("release:releases/{NEW_DEPLOY_REVISION}")),
            "{events}"
        );
        assert!(!fixture.hold.exists());
    }

    struct RestoreFixture {
        _temp: tempfile::TempDir,
        state_dir: PathBuf,
        backup: PathBuf,
        sqlite3: PathBuf,
        systemctl: PathBuf,
        y2b: PathBuf,
        service_state: PathBuf,
        systemctl_log: PathBuf,
        y2b_log: PathBuf,
        fail_start_once: PathBuf,
    }

    fn restore_fixture(active: bool) -> RestoreFixture {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("data");
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&bin_dir).unwrap();
        let backup = temp.path().join("backup.db");
        fs::write(&backup, format!("new-v{CURRENT_SCHEMA_VERSION}")).unwrap();

        let sqlite3 = bin_dir.join("sqlite3");
        write_executable(
            &sqlite3,
            r#"#!/usr/bin/env bash
set -euo pipefail
database=$1
query=$2
case "$query" in
  *integrity_check*)
    if grep -q '^damaged' "$database"; then
      printf 'row 1 missing\nrow 2 broken\n'
    else
      printf 'ok\n'
    fi
    ;;
  *sqlite_master*) printf '4\n' ;;
  *MAX\(version\)*)
    content=$(<"$database")
    if [[ "$content" =~ v([0-9]+) ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
    else
      echo "无法读取测试 schema" >&2
      exit 2
    fi
    ;;
  *) echo "未预期的 sqlite 查询: $query" >&2; exit 2 ;;
esac
"#,
        );

        let systemctl = bin_dir.join("systemctl");
        write_executable(
            &systemctl,
            r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1" >>"$RESTORE_TEST_SYSTEMCTL_LOG"
case "$1" in
  is-active)
    [[ $(<"$RESTORE_TEST_SERVICE_STATE") == active ]] && exit 0
    exit 3
    ;;
  stop) printf 'inactive\n' >"$RESTORE_TEST_SERVICE_STATE" ;;
  start)
    if [[ -f "$RESTORE_TEST_FAIL_START_ONCE" ]]; then
      rm -f "$RESTORE_TEST_FAIL_START_ONCE"
      exit 1
    fi
    printf 'active\n' >"$RESTORE_TEST_SERVICE_STATE"
    ;;
  show) printf '4242\n0\n' ;;
  *) exit 2 ;;
esac
"#,
        );
        let y2b = bin_dir.join("y2b");
        let y2b_script = r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$RESTORE_TEST_Y2B_LOG"
case "${1:-}" in
  schema-version) printf '__CURRENT_SCHEMA__\n' ;;
  migrate)
    if [[ ${2:-} == --help ]]; then exit 0; fi
    [[ ${2:-} == --database && -n ${3:-} ]] || exit 2
    database=$3
    content=$(<"$database")
    prefix=${content%v*}
    printf '%sv__CURRENT_SCHEMA__' "$prefix" >"$database"
    printf '__CURRENT_SCHEMA__\n'
    ;;
  maintenance) exit 0 ;;
  *) exit 2 ;;
esac
"#
        .replace("__CURRENT_SCHEMA__", &CURRENT_SCHEMA_VERSION.to_string());
        write_executable(&y2b, &y2b_script);

        let service_state = temp.path().join("service-state");
        fs::write(
            &service_state,
            if active { "active\n" } else { "inactive\n" },
        )
        .unwrap();

        RestoreFixture {
            _temp: temp,
            state_dir,
            backup,
            sqlite3,
            systemctl,
            y2b,
            service_state,
            systemctl_log: PathBuf::new(),
            y2b_log: PathBuf::new(),
            fail_start_once: PathBuf::new(),
        }
        .with_runtime_paths()
    }

    impl RestoreFixture {
        fn with_runtime_paths(mut self) -> Self {
            let root = self.service_state.parent().unwrap();
            self.systemctl_log = root.join("systemctl.log");
            self.y2b_log = root.join("y2b.log");
            self.fail_start_once = root.join("fail-start-once");
            self
        }

        fn command_with_sqlite(&self, sqlite3: &Path) -> std::process::Command {
            let mut command = std::process::Command::new("bash");
            command
                .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/restore.sh"))
                .arg(&self.backup)
                .env("Y2B_STATE_DIR", &self.state_dir)
                .env_remove("Y2B_DATABASE")
                .env_remove("Y2B_SCHEMA_VERSION")
                .env("Y2B_SQLITE3", sqlite3)
                .env("Y2B_SYSTEMCTL", &self.systemctl)
                .env("Y2B_BIN", &self.y2b)
                .env("Y2B_HEALTH_INTERVAL_SECONDS", "0")
                .env("Y2B_HEALTH_WINDOW_SECONDS", "0")
                .env("Y2B_HEALTH_MAX_CHECKS", "2")
                .env("RESTORE_TEST_SERVICE_STATE", &self.service_state)
                .env("RESTORE_TEST_SYSTEMCTL_LOG", &self.systemctl_log)
                .env("RESTORE_TEST_Y2B_LOG", &self.y2b_log)
                .env("RESTORE_TEST_FAIL_START_ONCE", &self.fail_start_once);
            command
        }

        fn run_with_sqlite(&self, sqlite3: &Path) -> Output {
            self.command_with_sqlite(sqlite3).output().unwrap()
        }

        fn run(&self) -> Output {
            self.run_with_sqlite(&self.sqlite3)
        }
    }

    #[test]
    fn restore_succeeds_without_an_existing_database() {
        let fixture = restore_fixture(false);
        let output = fixture.run();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(fixture.state_dir.join("state.db")).unwrap(),
            format!("new-v{CURRENT_SCHEMA_VERSION}")
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "inactive\n"
        );
        assert_eq!(
            fs::read_to_string(&fixture.y2b_log).unwrap(),
            "schema-version\n"
        );
    }

    #[test]
    fn restore_rejects_corrupt_backup_before_stopping_service() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(
            &fixture.backup,
            format!("damaged-v{CURRENT_SCHEMA_VERSION}"),
        )
        .unwrap();

        let output = fixture.run();
        assert!(!output.status.success());
        assert_eq!(fs::read_to_string(database).unwrap(), old);
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn restore_rejects_newer_backup_before_touching_current_database() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(
            &fixture.backup,
            format!("future-v{}", CURRENT_SCHEMA_VERSION + 1),
        )
        .unwrap();

        let output = fixture.run();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("无法降级恢复"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_to_string(database).unwrap(), old);
        assert!(!fixture.systemctl_log.exists());
        assert_eq!(
            fs::read_to_string(&fixture.y2b_log).unwrap(),
            "schema-version\n"
        );
    }

    #[test]
    fn restore_migrates_v17_backup_while_service_remains_inactive() {
        let fixture = restore_fixture(false);
        let database = fixture.state_dir.join("state.db");
        fs::write(&database, format!("old-v{CURRENT_SCHEMA_VERSION}")).unwrap();
        fs::write(&fixture.backup, "weekly-v17").unwrap();

        let output = fixture.run();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(database).unwrap(),
            format!("weekly-v{CURRENT_SCHEMA_VERSION}")
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "inactive\n"
        );
        let y2b_calls = fs::read_to_string(&fixture.y2b_log).unwrap();
        assert!(y2b_calls.contains("schema-version\n"));
        assert!(y2b_calls.contains("migrate --help\n"));
        assert!(y2b_calls.contains("migrate --database "));
    }

    #[test]
    fn restore_rejects_missing_sqlite_before_stopping_service() {
        let fixture = restore_fixture(true);
        let missing = fixture.state_dir.join("missing-sqlite3");
        let output = fixture.run_with_sqlite(&missing);

        assert!(!output.status.success());
        assert!(!fixture.systemctl_log.exists());
    }

    #[test]
    fn restore_rolls_back_database_when_service_start_fails() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(&fixture.fail_start_once, "fail\n").unwrap();

        let output = fixture.run();
        assert!(!output.status.success());
        assert_eq!(fs::read_to_string(database).unwrap(), old);
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        let calls = fs::read_to_string(&fixture.systemctl_log).unwrap();
        assert!(calls.matches("start\n").count() >= 2);
    }

    #[test]
    fn restore_requires_stable_window_before_declaring_success() {
        let fixture = restore_fixture(true);
        let database = fixture.state_dir.join("state.db");
        let old = format!("old-v{CURRENT_SCHEMA_VERSION}");
        fs::write(&database, &old).unwrap();
        fs::write(&fixture.backup, format!("new-v{CURRENT_SCHEMA_VERSION}")).unwrap();

        let output = fixture.run();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(database).unwrap(),
            format!("new-v{CURRENT_SCHEMA_VERSION}")
        );
        assert_eq!(
            fs::read_to_string(&fixture.service_state).unwrap(),
            "active\n"
        );
        let calls = fs::read_to_string(&fixture.systemctl_log).unwrap();
        assert!(
            calls.matches("show\n").count() >= 2,
            "稳定窗口未采样: {calls}"
        );
        let y2b_calls = fs::read_to_string(&fixture.y2b_log).unwrap();
        assert!(
            y2b_calls.contains("maintenance status --database "),
            "{y2b_calls}"
        );
    }
