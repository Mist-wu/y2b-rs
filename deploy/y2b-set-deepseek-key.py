#!/usr/bin/env python3
"""Atomically install y2b's DeepSeek key and remove legacy copies."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any


AUTH_DIR = Path("/var/lib/y2b/pi-agent")
AUTH_FILE = AUTH_DIR / "auth.json"
GLOBAL_PI_AUTH = Path("/root/.pi/agent/auth.json")
ENV_FILE = Path("/etc/y2b/y2b.env")
LOCK_FILE = Path("/run/lock/y2b-deepseek-key.lock")
KEY_PATTERN = re.compile(r"sk-[A-Za-z0-9_-]{20,}")
ENV_KEY_PATTERN = re.compile(
    r"^[ \t]*(?:export[ \t]+)?DEEPSEEK_API_KEY[ \t]*=", re.MULTILINE
)


class TopologyError(RuntimeError):
    """Credential topology is incomplete or unsafe."""


def validate_key(value: Any) -> str:
    if not isinstance(value, str) or KEY_PATTERN.fullmatch(value) is None:
        raise TopologyError("invalid DeepSeek API key syntax")
    return value


def read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TopologyError(f"cannot read valid JSON object: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise TopologyError(f"JSON root must be an object: {path}")
    return value


def render_json(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()


def ensure_private_dir(path: Path, uid: int, gid: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.chown(path, uid, gid)
    path.chmod(0o700)


def atomic_write(path: Path, content: bytes, uid: int, gid: int) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, uid, gid)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise


def deepseek_document(key: str) -> dict[str, Any]:
    return {"deepseek": {"type": "api_key", "key": validate_key(key)}}


def without_deepseek_env(content: str) -> tuple[str, bool]:
    lines = content.splitlines(keepends=True)
    kept = [line for line in lines if ENV_KEY_PATTERN.match(line) is None]
    return "".join(kept), len(kept) != len(lines)


def without_global_deepseek(document: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if "deepseek" not in document:
        return document, False
    cleaned = dict(document)
    del cleaned["deepseek"]
    return cleaned, True


def assert_private(path: Path, expected_uid: int, expected_gid: int, mode: int) -> None:
    try:
        metadata = path.stat()
    except OSError as exc:
        raise TopologyError(f"missing credential path: {path}") from exc
    actual_mode = stat.S_IMODE(metadata.st_mode)
    if metadata.st_uid != expected_uid or metadata.st_gid != expected_gid:
        raise TopologyError(f"credential path must be owned by uid:gid {expected_uid}:{expected_gid}: {path}")
    if actual_mode != mode:
        raise TopologyError(f"credential path must have mode {mode:04o}: {path}")


def dedicated_key(path: Path) -> str:
    document = read_json_object(path)
    if set(document) != {"deepseek"}:
        raise TopologyError(f"dedicated auth must contain only the deepseek provider: {path}")
    credential = document["deepseek"]
    if not isinstance(credential, dict) or credential.get("type") != "api_key":
        raise TopologyError(f"deepseek credential must have type api_key: {path}")
    return validate_key(credential.get("key"))


def check_topology(
    auth_dir: Path = AUTH_DIR,
    auth_file: Path = AUTH_FILE,
    global_auth: Path = GLOBAL_PI_AUTH,
    env_file: Path = ENV_FILE,
    expected_uid: int = 0,
    expected_gid: int = 0,
) -> str:
    assert_private(auth_dir, expected_uid, expected_gid, 0o700)
    assert_private(auth_file, expected_uid, expected_gid, 0o600)
    key = dedicated_key(auth_file)

    assert_private(env_file, expected_uid, expected_gid, 0o600)
    if ENV_KEY_PATTERN.search(env_file.read_text(encoding="utf-8")):
        raise TopologyError(f"legacy DeepSeek key remains in environment file: {env_file}")

    if global_auth.exists():
        assert_private(global_auth, expected_uid, expected_gid, 0o600)
        if "deepseek" in read_json_object(global_auth):
            raise TopologyError(f"legacy DeepSeek credential remains in global Pi auth: {global_auth}")
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def purge_legacy(
    global_auth: Path = GLOBAL_PI_AUTH,
    env_file: Path = ENV_FILE,
    uid: int = 0,
    gid: int = 0,
) -> tuple[bool, bool]:
    assert_private(env_file, uid, gid, 0o600)
    global_document: dict[str, Any] | None = None
    cleaned_global: dict[str, Any] | None = None
    if global_auth.exists():
        assert_private(global_auth, uid, gid, 0o600)
        global_document = read_json_object(global_auth)
        cleaned_global, removed_global = without_global_deepseek(global_document)
    else:
        removed_global = False

    try:
        env_content = env_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise TopologyError(f"cannot read environment file: {env_file}: {exc}") from exc
    cleaned_env, removed_env = without_deepseek_env(env_content)

    if removed_global and cleaned_global is not None:
        atomic_write(global_auth, render_json(cleaned_global), uid, gid)
    if removed_env:
        atomic_write(env_file, cleaned_env.encode(), uid, gid)
    return removed_global, removed_env


def install_key(
    key: str,
    auth_dir: Path = AUTH_DIR,
    auth_file: Path = AUTH_FILE,
    global_auth: Path = GLOBAL_PI_AUTH,
    env_file: Path = ENV_FILE,
    uid: int = 0,
    gid: int = 0,
) -> str:
    validate_key(key)
    # Parse both legacy stores before changing anything, so malformed input cannot
    # leave the topology half-migrated.
    if global_auth.exists():
        assert_private(global_auth, uid, gid, 0o600)
        read_json_object(global_auth)
    assert_private(env_file, uid, gid, 0o600)
    env_file.read_text(encoding="utf-8")
    ensure_private_dir(auth_dir, uid, gid)
    atomic_write(auth_file, render_json(deepseek_document(key)), uid, gid)
    purge_legacy(global_auth, env_file, uid, gid)
    return check_topology(auth_dir, auth_file, global_auth, env_file, uid, gid)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Install y2b's sole DeepSeek credential without printing it"
    )
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--check", action="store_true", help="validate only")
    actions.add_argument(
        "--purge-legacy",
        action="store_true",
        help="remove old DeepSeek copies after validating the dedicated key",
    )
    args = parser.parse_args()

    if os.geteuid() != 0:
        parser.error("must run as root")

    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_FILE.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            if args.check:
                digest = check_topology()
            elif args.purge_legacy:
                assert_private(AUTH_DIR, 0, 0, 0o700)
                assert_private(AUTH_FILE, 0, 0, 0o600)
                dedicated_key(AUTH_FILE)
                removed_global, removed_env = purge_legacy()
                digest = check_topology()
                print(
                    "removed legacy DeepSeek entries: "
                    f"global_auth={str(removed_global).lower()} "
                    f"env_file={str(removed_env).lower()}"
                )
            else:
                raw_key = sys.stdin.read()
                key = raw_key.strip()
                if not key or any(character.isspace() for character in key):
                    raise TopologyError("read exactly one DeepSeek API key from stdin")
                digest = install_key(key)
        except (OSError, TopologyError) as exc:
            print(f"credential update failed: {exc}", file=sys.stderr)
            return 1

    print(f"DeepSeek credential topology OK (sha256={digest})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
