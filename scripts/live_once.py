#!/usr/bin/env python3
"""One-shot YouTube premiere capture, Bilibili upload, and delayed CC backfill.

This deliberately stays outside the long-running y2b scheduler.  It reserves one
candidate/job in SQLite, captures the live HLS stream into recoverable MPEG-TS
segments, uploads a verified MP4 without a dynamic, and then keeps invoking the
existing `y2b subtitle add` flow until an exact manual `zh` subtitle succeeds.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
NEXT_SUBMIT_KEY = "bilibili.next_submit_at"
MAINTENANCE_HOLD_LEASE_SECONDS = 180
MAINTENANCE_HOLD_RENEW_INTERVAL_SECONDS = 60
SIDECAR_SUBTITLE_RETRY_AT = "2099-12-31T23:59:59+00:00"
EXPECTED_SCHEMA_VERSION = 22
BVID_RE = re.compile(r"\bBV[0-9A-Za-z]{10}\b")
BILIUP_DEBUG_RESPONSE_RE = re.compile(r"ResponseData\s*\{\s*code:\s*(-?\d+)")
SEGMENT_RE = re.compile(r"segment-(\d{8}T\d{6}Z)\.ts$")
END_STATUSES = {"post_live", "was_live", "not_live"}


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def parse_time(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"time must include an offset: {value}")
    return parsed.astimezone(UTC)


def log(message: str) -> None:
    print(f"{iso(utc_now())} {message}", flush=True)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"state is not an object: {path}")
    return value


def run(
    command: list[str],
    *,
    timeout: float | None = None,
    check: bool = True,
    stdout: int | Any = subprocess.PIPE,
    stderr: int | Any = subprocess.PIPE,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        text=True,
        stdout=stdout,
        stderr=stderr,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[-2000:]
        raise RuntimeError(f"command failed ({result.returncode}): {command[0]}: {detail}")
    return result


def sqlite_connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path), timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def response_from_json_line(line: str) -> tuple[str, str | int | None] | None:
    decoder = json.JSONDecoder()
    for start, character in enumerate(line):
        if character != "{":
            continue
        try:
            value, _ = decoder.raw_decode(line[start:])
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict):
            continue
        code = value.get("code")
        if isinstance(code, bool) or not isinstance(code, int):
            continue
        if code != 0:
            return ("rejected", code)
        data = value.get("data")
        bvid = data.get("bvid") if isinstance(data, dict) else None
        if isinstance(bvid, str) and BVID_RE.fullmatch(bvid):
            return ("accepted", bvid)
        return ("accepted_without_bvid", None)
    return None


def response_from_debug_line(line: str) -> tuple[str, str | int | None] | None:
    match = BILIUP_DEBUG_RESPONSE_RE.search(line)
    if match is None:
        return None
    code = int(match.group(1))
    if code != 0:
        return ("rejected", code)
    bvid = BVID_RE.search(line)
    if bvid:
        return ("accepted", bvid.group(0))
    return ("accepted_without_bvid", None)


def parse_biliup_submission(output: str) -> tuple[str, str | int | None] | None:
    response = None
    for line in output.splitlines():
        parsed = response_from_json_line(line) or response_from_debug_line(line)
        if parsed is not None:
            response = parsed
    return response


class UploadUncertainError(RuntimeError):
    pass


class SchemaVersionError(RuntimeError):
    pass


class LiveOnce:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.work = Path(args.work_dir)
        self.work.mkdir(parents=True, exist_ok=True)
        self.state_path = self.work / "state.json"
        self.state = load_json(self.state_path)
        self.state_lock = threading.RLock()
        self.hold_stop = threading.Event()
        self.hold_thread: threading.Thread | None = None
        self.marker = f"LIVE_ONCE:{args.video_id}"
        self.main_start = parse_time(args.main_start)
        self.cut_at = self.main_start - dt.timedelta(seconds=args.keep_before_seconds)
        self.hold_at = parse_time(args.hold_at)
        self.last_status_check = 0.0
        self.metadata: dict[str, Any] = self.state.get("metadata") or {}

    def save(self, **changes: Any) -> None:
        with self.state_lock:
            self.state.update(changes)
            atomic_json(self.state_path, self.state)

    def acquire_lock(self) -> Any:
        lock_path = self.work / "live-once.lock"
        handle = lock_path.open("a+")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.close()
            raise RuntimeError(f"another live-once process owns {lock_path}") from error
        return handle

    def validate_schema_version(self) -> None:
        database = Path(self.args.database)
        if not database.is_file():
            raise SchemaVersionError(f"数据库不存在: {database}")
        try:
            with sqlite_connect(database) as connection:
                version = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(version),0) FROM schema_migrations"
                    ).fetchone()[0]
                )
        except (sqlite3.Error, TypeError, ValueError) as error:
            raise SchemaVersionError(
                f"无法读取数据库 schema_version，拒绝运行: {error}"
            ) from error
        if version != EXPECTED_SCHEMA_VERSION:
            raise SchemaVersionError(
                "数据库 schema 版本不兼容："
                f"当前 v{version}，live_once.py 仅支持 v{EXPECTED_SCHEMA_VERSION}；"
                "请同步脚本与 y2b 后再人工处理"
            )

    def backup_database(self) -> None:
        if self.state.get("database_backup"):
            return
        backup_dir = Path(self.args.backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
        destination = backup_dir / f"state.db.before-live-once-{self.args.video_id}-{stamp}"
        with sqlite_connect(Path(self.args.database)) as source:
            with sqlite3.connect(str(destination)) as target:
                source.backup(target)
        if destination.stat().st_size == 0:
            raise RuntimeError("database backup is empty")
        self.save(database_backup=str(destination))
        log(f"数据库备份完成: {destination}")

    def reserve_job(self) -> None:
        if self.state.get("job_reserved"):
            return
        job_id = self.state.get("job_id") or str(uuid.uuid4())
        self.save(job_id=job_id)
        now = iso(utc_now())
        database = Path(self.args.database)
        with sqlite_connect(database) as connection:
            connection.execute("BEGIN IMMEDIATE")
            candidate = connection.execute(
                "SELECT * FROM video_candidates WHERE video_id=?", (self.args.video_id,)
            ).fetchone()
            if candidate is None:
                raise RuntimeError(f"candidate does not exist: {self.args.video_id}")
            existing = connection.execute(
                "SELECT id,status,bvid,error FROM jobs WHERE video_id=?",
                (self.args.video_id,),
            ).fetchone()
            if existing is not None:
                if existing["id"] != job_id and self.marker not in (existing["error"] or ""):
                    raise RuntimeError(
                        f"video already has a non-live-once job: {existing['id']} {existing['status']}"
                    )
                job_id = existing["id"]
            else:
                connection.execute(
                    """
                    INSERT INTO jobs(
                      id,channel_id,video_id,url,title,status,transfer_mode,
                      published_at,discovered_at,error,created_at,updated_at
                    ) VALUES(?,?,?,?,?,'paused','translated',?,?,?,?,?)
                    """,
                    (
                        job_id,
                        candidate["channel_id"],
                        self.args.video_id,
                        self.args.url,
                        candidate["title"],
                        candidate["published_at"],
                        candidate["discovered_at"],
                        f"{self.marker} 一次性直播旁路正在处理",
                        now,
                        now,
                    ),
                )
            original_candidate = {key: candidate[key] for key in candidate.keys()}
            connection.execute(
                """
                UPDATE video_candidates
                SET gate_state='promoted',next_gate_at=NULL,
                    last_error=?,gate_attempts=gate_attempts+1
                WHERE video_id=?
                """,
                (f"{self.marker} reserved", self.args.video_id),
            )
            connection.execute(
                "INSERT INTO events(job_id,level,message,created_at) VALUES(?,?,?,?)",
                (job_id, "info", "一次性直播旁路已保留候选，等待直播开始", now),
            )
            connection.commit()
        self.save(job_id=job_id, original_candidate=original_candidate, job_reserved=True)
        log(f"已保留候选并创建 paused 任务: {job_id}")

    def active_ordinary_uploads(self) -> int:
        with sqlite_connect(Path(self.args.database)) as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='uploading' AND video_id<>?",
                (self.args.video_id,),
            ).fetchone()
        return int(row[0])

    def upload_hold_owner(self) -> str:
        owner = self.state.get("upload_hold_owner")
        if owner:
            return str(owner)
        owner = f"{self.marker}:{uuid.uuid4()}"
        self.save(upload_hold_owner=owner)
        return owner

    @staticmethod
    def record_hold_event(
        connection: sqlite3.Connection,
        action: str,
        owner: str,
        reason: str,
        occurred_at: str,
        expires_at: str | None,
        *,
        previous_owner: str | None = None,
        previous_reason: str | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO maintenance_hold_events(
              action,owner,previous_owner,reason,previous_reason,occurred_at,expires_at
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                action,
                owner,
                previous_owner,
                reason,
                previous_reason,
                occurred_at,
                expires_at,
            ),
        )

    def ensure_upload_hold(self) -> bool:
        now_time = utc_now()
        if now_time < self.hold_at:
            return False
        owner = self.upload_hold_owner()
        reason = "live_once 一次性直播旁路"
        now = iso(now_time)
        expires_at = iso(
            now_time + dt.timedelta(seconds=MAINTENANCE_HOLD_LEASE_SECONDS)
        )
        active = 0
        foreign_owner: str | None = None
        current: str | None = None
        acquired_new = False
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            # 写锁内先检查存量投稿，再写维护租约；它与 Rust 的投稿领取只能有一个赢家。
            active = int(
                connection.execute(
                    "SELECT COUNT(*) FROM jobs WHERE status='uploading' AND video_id<>?",
                    (self.args.video_id,),
                ).fetchone()[0]
            )
            if active:
                connection.rollback()
            else:
                hold = connection.execute(
                    """
                    SELECT owner,reason,acquired_at,heartbeat_at,expires_at
                    FROM maintenance_hold WHERE singleton=1
                    """
                ).fetchone()
                if hold is not None:
                    hold_expiry = parse_time(str(hold["expires_at"]))
                    foreign_owner = (
                        str(hold["owner"])
                        if hold["owner"] != owner and hold_expiry > now_time
                        else None
                    )
                if foreign_owner:
                    connection.rollback()
                else:
                    current_row = connection.execute(
                        "SELECT value FROM settings WHERE key=?", (NEXT_SUBMIT_KEY,)
                    ).fetchone()
                    current = str(current_row[0]) if current_row else None
                    if hold is None:
                        connection.execute(
                            """
                            INSERT INTO maintenance_hold(
                              singleton,owner,reason,acquired_at,heartbeat_at,expires_at
                            ) VALUES(1,?,?,?,?,?)
                            """,
                            (owner, reason, now, now, expires_at),
                        )
                        self.record_hold_event(
                            connection, "acquired", owner, reason, now, expires_at
                        )
                        acquired_new = True
                    elif parse_time(str(hold["expires_at"])) <= now_time:
                        previous_owner = str(hold["owner"])
                        previous_reason = str(hold["reason"])
                        connection.execute(
                            """
                            UPDATE maintenance_hold SET
                              owner=?,reason=?,acquired_at=?,heartbeat_at=?,expires_at=?
                            WHERE singleton=1
                            """,
                            (owner, reason, now, now, expires_at),
                        )
                        self.record_hold_event(
                            connection,
                            "taken_over",
                            owner,
                            reason,
                            now,
                            expires_at,
                            previous_owner=previous_owner,
                            previous_reason=previous_reason,
                        )
                        acquired_new = True
                    else:
                        renew_at = now_time + dt.timedelta(
                            seconds=MAINTENANCE_HOLD_LEASE_SECONDS
                            - MAINTENANCE_HOLD_RENEW_INTERVAL_SECONDS
                        )
                        if parse_time(str(hold["expires_at"])) <= renew_at:
                            connection.execute(
                                """
                                UPDATE maintenance_hold SET heartbeat_at=?,expires_at=?
                                WHERE singleton=1 AND owner=?
                                """,
                                (now, expires_at, owner),
                            )
                            self.record_hold_event(
                                connection,
                                "renewed",
                                owner,
                                str(hold["reason"]),
                                now,
                                expires_at,
                            )
                        else:
                            expires_at = str(hold["expires_at"])
                    connection.commit()

        if active:
            if not self.state.get("waiting_for_ordinary_upload"):
                log(f"22:00 已到，等待 {active} 个已开始的普通投稿完成")
                self.save(waiting_for_ordinary_upload=True)
            return False
        if foreign_owner:
            if self.state.get("waiting_for_upload_hold_owner") != foreign_owner:
                log(f"另一个维护进程 {foreign_owner} 持有租约，等待其释放或到期")
                self.save(waiting_for_upload_hold_owner=foreign_owner)
            return False

        not_before = self.state.get("sidecar_not_before")
        if current:
            try:
                current_time = parse_time(current)
                saved_time = parse_time(not_before) if not_before else None
                if current_time > utc_now() and (
                    saved_time is None or current_time > saved_time
                ):
                    not_before = iso(current_time)
            except ValueError:
                log(f"忽略无法解析的原投稿冷却时间: {current}")
        self.save(
            original_next_submit_at=current,
            sidecar_not_before=not_before,
            upload_hold=True,
            upload_hold_expires_at=expires_at,
            waiting_for_ordinary_upload=False,
            waiting_for_upload_hold_owner=None,
        )
        if acquired_new:
            log("全局维护租约已获取；只阻止新任务，已经执行的任务不受影响")
        return True

    def start_upload_hold_worker(self) -> None:
        if self.state.get("bvid") or self.hold_thread is not None:
            return

        def worker() -> None:
            remaining = max(0.0, (self.hold_at - utc_now()).total_seconds())
            if self.hold_stop.wait(remaining):
                return
            while not self.hold_stop.is_set():
                delay = 2
                try:
                    if self.ensure_upload_hold():
                        delay = MAINTENANCE_HOLD_RENEW_INTERVAL_SECONDS
                except Exception as error:
                    log(f"获取或续租全局维护锁失败，2 秒后重试: {error}")
                if self.hold_stop.wait(delay):
                    return

        self.hold_thread = threading.Thread(
            target=worker,
            name="live-once-upload-hold",
            daemon=True,
        )
        self.hold_thread.start()

    def stop_upload_hold_worker(self) -> None:
        self.hold_stop.set()
        if self.hold_thread is not None:
            self.hold_thread.join(timeout=10)

    def wait_until_sidecar_can_upload(self) -> None:
        while True:
            if not self.ensure_upload_hold():
                remaining = max(0.0, (self.hold_at - utc_now()).total_seconds())
                time.sleep(min(max(remaining, 1), 3))
                continue
            if self.active_ordinary_uploads():
                time.sleep(3)
                continue
            value = self.state.get("sidecar_not_before")
            if value:
                remaining = (parse_time(value) - utc_now()).total_seconds()
                if remaining > 0:
                    log(f"遵守上一普通投稿的冷却窗口，还需等待 {int(remaining)} 秒")
                    time.sleep(min(remaining, 30))
                    continue
            return

    def release_upload_hold_after_success(self) -> None:
        owner = self.upload_hold_owner()
        now_time = utc_now()
        next_submit_time = now_time + dt.timedelta(seconds=self.args.submit_interval_seconds)
        now = iso(now_time)
        released = False
        next_submit: str | None = None
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            hold = connection.execute(
                "SELECT owner,reason,expires_at FROM maintenance_hold WHERE singleton=1"
            ).fetchone()
            if hold is not None and hold["owner"] == owner:
                current_row = connection.execute(
                    "SELECT value FROM settings WHERE key=?", (NEXT_SUBMIT_KEY,)
                ).fetchone()
                current = str(current_row[0]) if current_row else None
                # 租约期间若平台限流写入了更晚的冷却时间，释放时必须保留它。
                if current:
                    current_time = parse_time(current)
                    if current_time > next_submit_time:
                        next_submit_time = current_time
                next_submit = iso(next_submit_time)
                connection.execute(
                    """
                    INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at
                    """,
                    (NEXT_SUBMIT_KEY, next_submit, now),
                )
                connection.execute(
                    "DELETE FROM maintenance_hold WHERE singleton=1 AND owner=?",
                    (owner,),
                )
                self.record_hold_event(
                    connection,
                    "released",
                    owner,
                    str(hold["reason"]),
                    now,
                    str(hold["expires_at"]),
                )
                connection.commit()
                released = True
            else:
                connection.rollback()
        if not released:
            raise RuntimeError("cannot release a maintenance hold owned by another process")
        self.save(upload_hold=False, released_next_submit_at=next_submit)
        log(f"全局维护租约已释放，下一次最早投稿时间: {next_submit}")

    def youtube_common_args(self) -> list[str]:
        result = [
            self.args.yt_dlp,
            "--no-playlist",
            "--js-runtimes",
            "node",
            "--extractor-args",
            "youtube:player_client=web_creator",
        ]
        cookies = Path(self.args.youtube_cookies)
        if cookies.exists():
            result.extend(["--cookies", str(cookies)])
        return result

    def fetch_metadata(self) -> dict[str, Any] | None:
        template = (
            "%(.{id,title,description,uploader,upload_date,channel,channel_id,"
            "timestamp,duration,width,height,fps,thumbnail,webpage_url,live_status,"
            "release_timestamp})j"
        )
        command = self.youtube_common_args() + [
            "--skip-download",
            "--ignore-no-formats-error",
            "--no-warnings",
            "--print",
            template,
            self.args.url,
        ]
        try:
            result = run(command, timeout=90, check=False)
        except subprocess.TimeoutExpired:
            log("YouTube 元数据查询超时")
            return None
        for line in reversed((result.stdout or "").splitlines()):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                self.metadata.update({key: item for key, item in value.items() if item is not None})
                self.save(metadata=self.metadata)
                return self.metadata
        detail = (result.stderr or "").strip()[-500:]
        if detail:
            log(f"YouTube 元数据暂不可用: {detail}")
        return None

    def resolve_manifest(self) -> str | None:
        command = self.youtube_common_args() + [
            "--no-warnings",
            "-f",
            "best[protocol^=m3u8][vcodec!=none][acodec!=none]/best[protocol^=m3u8]/best",
            "-g",
            self.args.url,
        ]
        try:
            result = run(command, timeout=90, check=False)
        except subprocess.TimeoutExpired:
            log("直播清单查询超时，稍后重试")
            return None
        if result.returncode != 0:
            return None
        for line in (result.stdout or "").splitlines():
            if line.startswith("http://") or line.startswith("https://"):
                return line.strip()
        return None

    def status_ended(self, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and now - self.last_status_check < 15:
            return False
        self.last_status_check = now
        metadata = self.fetch_metadata()
        status = (metadata or self.metadata).get("live_status")
        if status:
            log(f"YouTube live_status={status}")
        return status in END_STATUSES

    def capture_attempt(self, manifest: str) -> int:
        attempts_dir = self.work / "segments"
        attempts_dir.mkdir(parents=True, exist_ok=True)
        attempt = int(self.state.get("capture_attempt", 0)) + 1
        directory = attempts_dir / f"attempt-{attempt:03d}"
        directory.mkdir(parents=True, exist_ok=True)
        ffmpeg_log = directory / "ffmpeg.log"
        output = directory / "segment-%Y%m%dT%H%M%SZ.ts"
        command = [
            self.args.ffmpeg,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "warning",
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_on_network_error",
            "1",
            "-reconnect_on_http_error",
            "4xx,5xx",
            "-reconnect_delay_max",
            "5",
            "-i",
            manifest,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-c",
            "copy",
            "-f",
            "segment",
            "-segment_time",
            "2",
            "-segment_atclocktime",
            "1",
            "-reset_timestamps",
            "1",
            "-strftime",
            "1",
            str(output),
        ]
        self.save(capture_attempt=attempt, phase="capturing")
        log(f"直播抓取已开始，分段批次 {attempt:03d}")
        with ffmpeg_log.open("ab", buffering=0) as log_handle:
            process = subprocess.Popen(command, stdout=log_handle, stderr=log_handle)
            ended = False
            while process.poll() is None:
                self.ensure_upload_hold()
                if utc_now() >= self.main_start and self.status_ended():
                    ended = True
                    log("检测到首映结束，停止等待清单并准备封装")
                    process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                    break
                time.sleep(2)
            returncode = process.wait()
        count = len(list(directory.glob("segment-*.ts")))
        log(f"分段批次 {attempt:03d} 结束: exit={returncode}, segments={count}")
        if ended:
            return 0
        return returncode

    def segment_time(self, path: Path) -> dt.datetime | None:
        match = SEGMENT_RE.search(path.name)
        if not match:
            return None
        return dt.datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)

    def selected_segments(self) -> list[tuple[dt.datetime, Path]]:
        earliest = self.cut_at - dt.timedelta(seconds=10)
        choices: dict[dt.datetime, Path] = {}
        for path in (self.work / "segments").glob("attempt-*/segment-*.ts"):
            timestamp = self.segment_time(path)
            if timestamp is None or timestamp < earliest or path.stat().st_size == 0:
                continue
            previous = choices.get(timestamp)
            if previous is None or path.stat().st_size > previous.stat().st_size:
                choices[timestamp] = path
        return sorted(choices.items())

    @staticmethod
    def concat_line(path: Path) -> str:
        escaped = str(path.resolve()).replace("'", "'\\''")
        return f"file '{escaped}'\n"

    def finalize_video(self) -> Path:
        final = self.work / f"{self.args.video_id}.live.mp4"
        if final.exists():
            self.validate_video(final)
            self.save(final_video=str(final), phase="video_ready")
            return final
        segments = self.selected_segments()
        if not segments:
            raise RuntimeError(f"no captured segments near or after {iso(self.cut_at)}")
        first_time = segments[0][0]
        trim_seconds = max(0.0, (self.cut_at - first_time).total_seconds())
        concat_path = self.work / "segments.concat.txt"
        concat_path.write_text(
            "".join(self.concat_line(path) for _, path in segments), encoding="utf-8"
        )
        temporary = self.work / f"{self.args.video_id}.live.tmp.mp4"
        temporary.unlink(missing_ok=True)
        command = [
            self.args.ffmpeg,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "warning",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_path),
        ]
        if trim_seconds > 0:
            command.extend(["-ss", f"{trim_seconds:.3f}"])
        command.extend(
            [
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-c",
                "copy",
                "-avoid_negative_ts",
                "make_zero",
                "-movflags",
                "+faststart",
                "-y",
                str(temporary),
            ]
        )
        log(f"开始拼接 {len(segments)} 个直播分段，起点修剪 {trim_seconds:.3f} 秒")
        run(command, timeout=1800)
        os.replace(temporary, final)
        details = self.validate_video(final)
        self.save(final_video=str(final), video_probe=details, phase="video_ready")
        log(
            f"视频封装和解码验证通过: duration={details['duration']:.3f}s, "
            f"size={final.stat().st_size}"
        )
        return final

    def validate_video(self, path: Path) -> dict[str, Any]:
        result = run(
            [
                self.args.ffprobe,
                "-v",
                "error",
                "-show_streams",
                "-show_format",
                "-of",
                "json",
                str(path),
            ],
            timeout=120,
        )
        probe = json.loads(result.stdout)
        streams = probe.get("streams") or []
        if not any(item.get("codec_type") == "video" for item in streams):
            raise RuntimeError("final video has no video stream")
        if not any(item.get("codec_type") == "audio" for item in streams):
            raise RuntimeError("final video has no audio stream")
        duration = float((probe.get("format") or {}).get("duration") or 0)
        if duration <= 1:
            raise RuntimeError(f"final video duration is invalid: {duration}")
        for start in (0.0, max(0.0, duration - min(10.0, duration))):
            command = [self.args.ffmpeg, "-v", "error"]
            if start:
                command.extend(["-ss", f"{start:.3f}"])
            command.extend(
                [
                    "-i",
                    str(path),
                    "-t",
                    f"{min(10.0, duration):.3f}",
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0",
                    "-f",
                    "null",
                    "-",
                ]
            )
            run(command, timeout=180)
        video = next(item for item in streams if item.get("codec_type") == "video")
        return {
            "duration": duration,
            "width": video.get("width"),
            "height": video.get("height"),
            "fps": video.get("avg_frame_rate"),
        }

    def capture_until_end(self) -> Path:
        if self.state.get("final_video"):
            return self.finalize_video()
        no_manifest_logged = False
        while True:
            self.ensure_upload_hold()
            metadata = self.fetch_metadata()
            status = (metadata or self.metadata).get("live_status")
            if status in END_STATUSES and self.selected_segments():
                log(f"首映已结束（{status}），使用已抓取分段封装")
                return self.finalize_video()
            manifest = self.resolve_manifest()
            if manifest:
                no_manifest_logged = False
                self.capture_attempt(manifest)
                if utc_now() >= self.main_start and self.status_ended(force=True):
                    return self.finalize_video()
                log("直播清单中断但首映尚未结束，刷新清单后重连")
                time.sleep(2)
                continue
            if not no_manifest_logged:
                log(f"直播清单尚不可用，当前 live_status={status or 'unknown'}")
                no_manifest_logged = True
            if status in END_STATUSES:
                if self.selected_segments():
                    return self.finalize_video()
                raise RuntimeError("premiere ended before any usable main-program segment was captured")
            now = utc_now()
            delay = 10 if now >= self.main_start - dt.timedelta(minutes=30) else 60
            time.sleep(delay)

    def download_cover(self) -> Path | None:
        cover = self.work / "cover.jpg"
        if cover.exists() and cover.stat().st_size > 1000:
            return cover
        urls = [
            f"https://i.ytimg.com/vi/{self.args.video_id}/maxresdefault.jpg",
            str(self.metadata.get("thumbnail") or ""),
            f"https://i.ytimg.com/vi/{self.args.video_id}/hqdefault.jpg",
        ]
        request_headers = {"User-Agent": "Mozilla/5.0"}
        for index, url in enumerate(dict.fromkeys(item for item in urls if item)):
            source = self.work / f"cover-source-{index}"
            try:
                request = urllib.request.Request(url, headers=request_headers)
                with urllib.request.urlopen(request, timeout=30) as response:
                    source.write_bytes(response.read())
                run(
                    [
                        self.args.ffmpeg,
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-y",
                        "-i",
                        str(source),
                        "-frames:v",
                        "1",
                        str(cover),
                    ],
                    timeout=60,
                )
                if cover.stat().st_size > 1000:
                    log(f"封面已准备: {cover}")
                    return cover
            except (OSError, RuntimeError, urllib.error.URLError) as error:
                log(f"封面下载尝试失败: {error}")
        log("未取得封面，将让 biliup 使用默认封面")
        return None

    def publication_date(self) -> str:
        value = str(self.metadata.get("upload_date") or "")
        if len(value) == 8 and value.isdigit():
            return f"{value[:4]}-{value[4:6]}-{value[6:]}"
        timestamp = self.metadata.get("timestamp") or self.metadata.get("release_timestamp")
        if timestamp:
            return dt.datetime.fromtimestamp(float(timestamp), UTC).strftime("%Y-%m-%d")
        return "未知"

    def description(self) -> str:
        original = str(self.metadata.get("title") or "").strip()
        uploader = str(
            self.metadata.get("uploader") or self.metadata.get("channel") or "Brawl Stars"
        )
        lines = []
        if original:
            clean = " ".join(
                part for part in original.split() if not part.startswith(("#", "＃"))
            )
            if clean:
                lines.append(f"原标题：{clean}")
        lines.extend(
            [
                f"来源：{self.args.url}",
                f"原作者：{uploader}",
                f"原视频发布时间：{self.publication_date()}",
                "处理工具：https://github.com/Mist-wu/y2b-rs",
            ]
        )
        return "\n".join(lines)

    def upload_args(self, video: Path, cover: Path | None) -> list[str]:
        command = [
            self.args.biliup,
            "-u",
            self.args.bilibili_cookies,
            "upload",
            str(video),
            "--submit",
            "web",
            "--title",
            self.args.title,
            "--desc",
            self.description(),
            "--tag",
            self.args.tags,
            "--tid",
            str(self.args.tid),
            "--copyright",
            "1",
            "--no-reprint",
            "0",
            "--limit",
            "1",
        ]
        if cover is not None:
            command.extend(["--cover", str(cover)])
        return command

    def recover_bvid_from_logs(self) -> str | None:
        for path in sorted(self.work.glob("upload-attempt-*.log"), reverse=True):
            match = BVID_RE.search(path.read_text(encoding="utf-8", errors="replace"))
            if match:
                return match.group(0)
        return None

    def mark_bvid_conflict_uncertain(
        self,
        connection: sqlite3.Connection,
        bvid: str,
        attempt_id: str | None,
    ) -> str | None:
        connection.execute("BEGIN IMMEDIATE")
        owner = connection.execute(
            "SELECT id FROM jobs WHERE bvid=? AND id<>?",
            (bvid, str(self.state["job_id"])),
        ).fetchone()
        if owner is None:
            connection.rollback()
            return None
        attempt = f"（attempt={attempt_id}）" if attempt_id else ""
        detail = (
            f"平台结果 BVID {bvid} 已归属其他任务 {owner['id']}，"
            f"当前任务无法自动确认{attempt}；结果不确定，禁止自动重投，"
            "请人工核对 Bilibili 创作中心并处理 BVID 归属冲突"
        )
        now = iso(utc_now())
        if attempt_id:
            connection.execute(
                """
                UPDATE upload_attempts
                SET status='uncertain',bvid=?,detail=?,finished_at=?
                WHERE id=? AND job_id=?
                """,
                (bvid, detail, now, attempt_id, str(self.state["job_id"])),
            )
        changed = connection.execute(
            "UPDATE jobs SET status='upload_uncertain',error=?,updated_at=? WHERE id=?",
            (f"{self.marker} {detail}", now, str(self.state["job_id"])),
        ).rowcount
        if changed != 1:
            raise RuntimeError("BVID 归属冲突发生后无法标记当前任务为 uncertain")
        connection.commit()
        self.save(phase="upload_uncertain", last_upload_error=detail)
        return detail

    def update_job_bvid_or_mark_uncertain(
        self,
        connection: sqlite3.Connection,
        statement: str,
        parameters: tuple[Any, ...],
        bvid: str,
        attempt_id: str | None,
    ) -> sqlite3.Cursor:
        try:
            return connection.execute(statement, parameters)
        except sqlite3.IntegrityError as error:
            connection.rollback()
            detail = self.mark_bvid_conflict_uncertain(
                connection, bvid, attempt_id
            )
            if detail is None:
                raise
            raise UploadUncertainError(detail) from error

    def recover_persisted_upload(self) -> str | None:
        job_id = self.state.get("job_id")
        state_bvid = str(self.state["bvid"]) if self.state.get("bvid") else None
        if not job_id:
            if state_bvid:
                self.save(bvid=state_bvid, phase="uploaded")
                return state_bvid
            raise RuntimeError("一次性直播任务缺少已保留的 job_id")
        log_bvid = self.recover_bvid_from_logs()
        now = iso(utc_now())
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute(
                "SELECT id,status,bvid FROM jobs WHERE video_id=?", (self.args.video_id,)
            ).fetchone()
            if job is None or job["id"] != job_id:
                raise RuntimeError("一次性直播任务对应的数据库 job 不存在或已被替换")
            attempt = connection.execute(
                """
                SELECT id,status,bvid,detail FROM upload_attempts
                WHERE job_id=? ORDER BY started_at DESC,rowid DESC LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            attempt_status = attempt["status"] if attempt else None
            bvid = job["bvid"] or state_bvid
            if not bvid and attempt_status in {"succeeded", "reconciled"}:
                bvid = attempt["bvid"]
            if not bvid and attempt_status in {None, "running", "uncertain"}:
                bvid = log_bvid

            if attempt_status in {"running", "uncertain"} and bvid:
                connection.execute(
                    """
                    UPDATE upload_attempts
                    SET status='reconciled',bvid=?,detail='从本地持久化结果核对确认',finished_at=?
                    WHERE id=? AND status IN ('running','uncertain')
                    """,
                    (bvid, now, attempt["id"]),
                )
                self.update_job_bvid_or_mark_uncertain(
                    connection,
                    """
                    UPDATE jobs SET
                      status=CASE WHEN status IN ('uploading','upload_uncertain')
                                  THEN 'paused' ELSE status END,
                      bvid=?,error=NULL,updated_at=?
                    WHERE id=?
                    """,
                    (bvid, now, job_id),
                    str(bvid),
                    str(attempt["id"]),
                )
                connection.commit()
                self.save(bvid=bvid, phase="uploaded")
                log(f"从持久化投稿结果恢复 BVID，未重复投稿: {bvid}")
                return str(bvid)

            if attempt_status in {"running", "uncertain"}:
                detail = (
                    attempt["detail"]
                    if attempt_status == "uncertain" and attempt["detail"]
                    else f"投稿 attempt {attempt['id']} 在确认结果前中断"
                )
                if "禁止自动重投" not in detail:
                    detail = f"{detail}；禁止自动重投，请人工核对 Bilibili 创作中心"
                if attempt_status == "running":
                    connection.execute(
                        """
                        UPDATE upload_attempts
                        SET status='uncertain',detail=?,finished_at=?
                        WHERE id=? AND status='running'
                        """,
                        (detail, now, attempt["id"]),
                    )
                connection.execute(
                    "UPDATE jobs SET status='upload_uncertain',error=?,updated_at=? WHERE id=?",
                    (f"{self.marker} {detail}", now, job_id),
                )
                connection.commit()
                self.save(phase="upload_uncertain", last_upload_error=detail)
                raise UploadUncertainError(detail)

            if attempt_status in {"succeeded", "reconciled"} and not bvid:
                detail = (
                    f"投稿 attempt {attempt['id']} 状态为 {attempt_status}，但缺少 BVID；"
                    "禁止自动重投，请人工核对 Bilibili 创作中心"
                )
                connection.execute(
                    "UPDATE upload_attempts SET status='uncertain',detail=?,finished_at=? WHERE id=?",
                    (detail, now, attempt["id"]),
                )
                connection.execute(
                    "UPDATE jobs SET status='upload_uncertain',error=?,updated_at=? WHERE id=?",
                    (f"{self.marker} {detail}", now, job_id),
                )
                connection.commit()
                self.save(phase="upload_uncertain", last_upload_error=detail)
                raise UploadUncertainError(detail)

            if bvid:
                attempt_id = (
                    str(attempt["id"])
                    if attempt_status in {"running", "uncertain", "succeeded", "reconciled"}
                    else None
                )
                self.update_job_bvid_or_mark_uncertain(
                    connection,
                    """
                    UPDATE jobs SET
                      status=CASE WHEN status IN ('uploading','upload_uncertain')
                                  THEN 'paused' ELSE status END,
                      bvid=?,error=NULL,updated_at=?
                    WHERE id=?
                    """,
                    (bvid, now, job_id),
                    str(bvid),
                    attempt_id,
                )
                connection.commit()
                self.save(bvid=bvid, phase="uploaded")
                return str(bvid)
            connection.commit()
        return None

    def begin_upload_attempt(self) -> str:
        attempt_id = str(uuid.uuid4())
        now = iso(utc_now())
        job_id = str(self.state["job_id"])
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO upload_attempts(id,job_id,status,started_at)
                VALUES(?,?,'running',?)
                """,
                (attempt_id, job_id, now),
            )
            changed = connection.execute(
                "UPDATE jobs SET status='uploading',error=NULL,updated_at=? WHERE id=? AND bvid IS NULL",
                (now, job_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError("投稿前持久化 attempt 时任务已失效或已有 BVID")
            connection.commit()
        return attempt_id

    def finish_upload_attempt(self, attempt_id: str, bvid: str) -> None:
        now = iso(utc_now())
        job_id = str(self.state["job_id"])
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE upload_attempts
                SET status='succeeded',bvid=?,detail=NULL,finished_at=?
                WHERE id=? AND job_id=? AND status='running'
                """,
                (bvid, now, attempt_id, job_id),
            ).rowcount
            job_changed = self.update_job_bvid_or_mark_uncertain(
                connection,
                "UPDATE jobs SET status='paused',bvid=?,error=NULL,updated_at=? WHERE id=?",
                (bvid, now, job_id),
                bvid,
                attempt_id,
            ).rowcount
            if changed != 1 or job_changed != 1:
                raise RuntimeError(f"投稿 attempt {attempt_id} 已失效")
            connection.commit()

    def fail_upload_attempt(self, attempt_id: str, detail: str) -> None:
        now = iso(utc_now())
        job_id = str(self.state["job_id"])
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE upload_attempts SET status='failed',detail=?,finished_at=?
                WHERE id=? AND job_id=? AND status='running'
                """,
                (detail, now, attempt_id, job_id),
            ).rowcount
            job_changed = connection.execute(
                "UPDATE jobs SET status='paused',error=?,updated_at=? WHERE id=?",
                (f"{self.marker} {detail}", now, job_id),
            ).rowcount
            if changed != 1 or job_changed != 1:
                raise RuntimeError(f"投稿 attempt {attempt_id} 已失效")
            connection.commit()

    def mark_upload_attempt_uncertain(self, attempt_id: str, detail: str) -> None:
        now = iso(utc_now())
        job_id = str(self.state["job_id"])
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE upload_attempts SET status='uncertain',detail=?,finished_at=?
                WHERE id=? AND job_id=? AND status='running'
                """,
                (detail, now, attempt_id, job_id),
            ).rowcount
            job_changed = connection.execute(
                """
                UPDATE jobs SET status='upload_uncertain',error=?,updated_at=?
                WHERE id=?
                """,
                (f"{self.marker} {detail}", now, job_id),
            ).rowcount
            if changed != 1 or job_changed != 1:
                raise RuntimeError(f"投稿 attempt {attempt_id} 已失效")
            connection.commit()

    def upload_video(self, video: Path) -> str:
        bvid = self.recover_persisted_upload()
        if bvid:
            return bvid
        cover = self.download_cover()
        attempt = int(self.state.get("upload_attempt", 0))
        while True:
            self.wait_until_sidecar_can_upload()
            attempt += 1
            upload_log = self.work / f"upload-attempt-{attempt:03d}.log"
            attempt_id = self.begin_upload_attempt()
            self.save(
                upload_attempt=attempt,
                upload_attempt_id=attempt_id,
                phase="uploading",
            )
            log(
                f"开始一次性直传，投稿尝试 {attempt}，attempt={attempt_id}；"
                f"标题={self.args.title}"
            )
            command = self.upload_args(video, cover)
            launch_error: OSError | None = None
            try:
                handle = upload_log.open("wb", buffering=0)
            except OSError as error:
                launch_error = error
            else:
                with handle:
                    try:
                        process = subprocess.Popen(
                            command, stdout=handle, stderr=subprocess.STDOUT
                        )
                    except OSError as error:
                        launch_error = error
                    else:
                        timed_out = False
                        try:
                            returncode = process.wait(timeout=4 * 3600)
                        except subprocess.TimeoutExpired:
                            timed_out = True
                            with contextlib.suppress(OSError):
                                process.terminate()
                            try:
                                returncode = process.wait(timeout=20)
                            except subprocess.TimeoutExpired:
                                with contextlib.suppress(OSError):
                                    process.kill()
                                returncode = -1
                        except KeyboardInterrupt as error:
                            with contextlib.suppress(OSError):
                                process.terminate()
                            detail = (
                                f"biliup 执行被中断（attempt={attempt_id}），无法确认平台结果；"
                                "禁止自动重投，请人工核对 Bilibili 创作中心"
                            )
                            self.mark_upload_attempt_uncertain(attempt_id, detail)
                            self.save(phase="upload_uncertain", last_upload_error=detail)
                            raise UploadUncertainError(detail) from error
                        except Exception as error:
                            with contextlib.suppress(OSError):
                                process.terminate()
                            detail = (
                                f"等待 biliup 结果异常（attempt={attempt_id}）: {error}；"
                                "禁止自动重投，请人工核对 Bilibili 创作中心"
                            )
                            self.mark_upload_attempt_uncertain(attempt_id, detail)
                            self.save(phase="upload_uncertain", last_upload_error=detail)
                            raise UploadUncertainError(detail) from error
            if launch_error is not None:
                detail = f"biliup 未启动，确定没有产生投稿: {launch_error}"
                self.fail_upload_attempt(attempt_id, detail)
                delay = min(900, 60 * attempt)
                self.save(
                    last_upload_error=detail,
                    next_upload_retry_at=iso(
                        utc_now() + dt.timedelta(seconds=delay)
                    ),
                )
                log(f"{detail}；{delay} 秒后重试")
                time.sleep(delay)
                continue

            try:
                content = upload_log.read_text(encoding="utf-8", errors="replace")
            except OSError as error:
                detail = (
                    f"无法读取 biliup 完整输出（attempt={attempt_id}）: {error}；"
                    "禁止自动重投，请人工核对 Bilibili 创作中心"
                )
                self.mark_upload_attempt_uncertain(attempt_id, detail)
                self.save(phase="upload_uncertain", last_upload_error=detail)
                raise UploadUncertainError(detail) from error
            response = parse_biliup_submission(content)
            if response and response[0] == "accepted":
                bvid = str(response[1])
            elif response is None:
                match = BVID_RE.search(content)
                bvid = match.group(0) if match else None
            else:
                bvid = None
            if bvid:
                try:
                    self.finish_upload_attempt(attempt_id, bvid)
                except UploadUncertainError:
                    raise
                except Exception as error:
                    detail = (
                        f"Bilibili 已返回成功 {bvid}，但本地确认失败（attempt={attempt_id}）: "
                        f"{error}；禁止自动重投，请人工核对"
                    )
                    with contextlib.suppress(Exception):
                        self.mark_upload_attempt_uncertain(attempt_id, detail)
                    self.save(phase="upload_uncertain", last_upload_error=detail)
                    raise UploadUncertainError(detail) from error
                self.save(bvid=bvid, phase="uploaded")
                log(f"B站视频投稿成功: {bvid}")
                return bvid

            tail = content[-1500:].replace(self.args.bilibili_cookies, "[cookies]")
            tail = tail or "（无可用输出）"
            if (
                returncode != 0
                and response is not None
                and response[0] == "rejected"
            ):
                code = int(response[1])
                detail = f"biliup 投稿被平台明确拒绝: code={code}, exit={returncode}: {tail}"
                self.fail_upload_attempt(attempt_id, detail)
                delay = (
                    self.args.rate_limit_cooldown_seconds
                    if code == 21566
                    else min(900, 60 * attempt)
                )
                self.save(
                    last_upload_error=detail,
                    next_upload_retry_at=iso(
                        utc_now() + dt.timedelta(seconds=delay)
                    ),
                )
                log(f"{detail}；{delay} 秒后按策略重试")
                time.sleep(delay)
                continue

            if timed_out:
                reason = "biliup 投稿超时并被终止"
            elif returncode < 0:
                reason = f"biliup 被信号终止（exit={returncode}）"
            elif response and response[0] == "accepted_without_bvid":
                reason = "biliup 返回成功响应，但响应中没有合法 BVID"
            elif returncode == 0:
                reason = "biliup 退出成功，但没有可验证的结构化投稿响应或 BVID"
            else:
                reason = f"biliup 异常退出且没有明确的平台拒绝响应（exit={returncode}）"
            detail = (
                f"{reason}（attempt={attempt_id}）: {tail}；"
                "结果不确定，禁止自动重投，请人工核对 Bilibili 创作中心"
            )
            self.mark_upload_attempt_uncertain(attempt_id, detail)
            self.save(phase="upload_uncertain", last_upload_error=detail)
            log(detail)
            raise UploadUncertainError(detail)

    def source_metadata(self, video: Path) -> dict[str, Any]:
        probe = self.state.get("video_probe") or self.validate_video(video)
        timestamp = self.metadata.get("timestamp") or self.metadata.get("release_timestamp")
        return {
            "id": self.args.video_id,
            "url": self.args.url,
            "title": str(self.metadata.get("title") or ""),
            "description": self.metadata.get("description"),
            "uploader": self.metadata.get("uploader") or "Brawl Stars",
            "upload_date": self.metadata.get("upload_date"),
            "channel": self.metadata.get("channel") or "Brawl Stars",
            "channel_id": self.metadata.get("channel_id"),
            "timestamp": int(timestamp) if timestamp else None,
            "duration": float(probe.get("duration") or 0),
            "width": probe.get("width"),
            "height": probe.get("height"),
            "fps": self.metadata.get("fps"),
            "thumbnail_url": self.metadata.get("thumbnail"),
            "webpage_url": self.args.url,
            "live_status": self.metadata.get("live_status") or "was_live",
            "default_audio_language": self.metadata.get("default_audio_language"),
        }

    def record_upload(self, video: Path, bvid: str) -> None:
        if self.state.get("upload_recorded"):
            return
        now = iso(utc_now())
        metadata = self.source_metadata(video)
        probe = self.state.get("video_probe") or {}
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute(
                "SELECT id,error FROM jobs WHERE video_id=?", (self.args.video_id,)
            ).fetchone()
            if job is None or job["id"] != self.state["job_id"]:
                raise RuntimeError("reserved live-once job disappeared before upload recording")
            self.update_job_bvid_or_mark_uncertain(
                connection,
                """
                UPDATE jobs SET
                  title=?,status='uploaded_original_pending_subtitle',bvid=?,error=NULL,
                  raw_video_path=?,duration_seconds=?,width=?,height=?,fps=?,
                  source_metadata_json=?,subtitle_attempt=0,subtitle_retry_at=?,updated_at=?
                WHERE id=?
                """,
                (
                    self.args.title,
                    bvid,
                    str(video),
                    float(probe.get("duration") or 0),
                    probe.get("width"),
                    probe.get("height"),
                    self.metadata.get("fps"),
                    json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
                    SIDECAR_SUBTITLE_RETRY_AT,
                    now,
                    self.state["job_id"],
                ),
                bvid,
                str(self.state["upload_attempt_id"])
                if self.state.get("upload_attempt_id")
                else None,
            )
            connection.execute(
                """
                UPDATE video_candidates SET
                  title=COALESCE(?,title),published_at=COALESCE(?,published_at),
                  gate_state='promoted',next_gate_at=NULL,last_error=NULL
                WHERE video_id=?
                """,
                (
                    self.metadata.get("title"),
                    iso(dt.datetime.fromtimestamp(metadata["timestamp"], UTC))
                    if metadata.get("timestamp")
                    else None,
                    self.args.video_id,
                ),
            )
            connection.execute(
                "INSERT INTO events(job_id,level,message,created_at) VALUES(?,?,?,?)",
                (
                    self.state["job_id"],
                    "info",
                    f"一次性直播旁路投稿成功: {bvid}；由旁路持续等待官方字幕",
                    now,
                ),
            )
            connection.commit()
        self.save(upload_recorded=True, phase="waiting_subtitle")
        log("投稿结果已原子写回 y2b 数据库")

    def wait_for_subtitle(self, bvid: str) -> None:
        attempt = int(self.state.get("subtitle_attempt", 0))
        delays = [30, 60, 120, 300, 600, 1800, 3600]
        while True:
            attempt += 1
            log(f"检查官方字幕并尝试现有翻译/CC 流程，第 {attempt} 次")
            result = run(
                [
                    self.args.y2b,
                    "--config",
                    self.args.config,
                    "subtitle",
                    "add",
                    bvid,
                ],
                timeout=self.args.subtitle_command_timeout_seconds,
                check=False,
            )
            merged = (result.stdout or "") + "\n" + (result.stderr or "")
            if result.returncode == 0:
                self.save(
                    subtitle_attempt=attempt,
                    subtitle_completed_at=iso(utc_now()),
                    phase="completed",
                )
                log(f"官方字幕翻译并上传为手动 zh CC 成功: {merged.strip()[-1000:]}")
                return
            tail = merged.strip()[-1500:]
            delay = 60 if "code=-404" in merged or "稿件" in merged else delays[min(attempt - 1, len(delays) - 1)]
            self.save(
                subtitle_attempt=attempt,
                last_subtitle_error=tail,
                next_subtitle_retry_at=iso(utc_now() + dt.timedelta(seconds=delay)),
            )
            log(f"官方字幕尚不可用或 CC 暂未就绪，{delay} 秒后继续: {tail}")
            time.sleep(delay)

    def rollback(self) -> None:
        if self.state.get("bvid"):
            raise RuntimeError("cannot roll back after Bilibili upload succeeded")
        original = self.state.get("original_candidate")
        owner = self.state.get("upload_hold_owner")
        hold_released = False
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute(
                "SELECT id,status,bvid,error FROM jobs WHERE video_id=?",
                (self.args.video_id,),
            ).fetchone()
            if job and (
                job["bvid"] is not None
                or job["status"] in {"uploading", "upload_uncertain"}
            ):
                raise RuntimeError("投稿已成功或结果不确定，禁止回滚；请先人工核对")
            if job and self.marker in (job["error"] or ""):
                connection.execute("DELETE FROM jobs WHERE id=?", (job["id"],))
            if original:
                columns = [
                    "channel_id",
                    "url",
                    "title",
                    "published_at",
                    "source",
                    "discovered_at",
                    "gate_state",
                    "gate_attempts",
                    "next_gate_at",
                    "last_error",
                ]
                assignments = ",".join(f"{column}=?" for column in columns)
                connection.execute(
                    f"UPDATE video_candidates SET {assignments} WHERE video_id=?",
                    tuple(original.get(column) for column in columns) + (self.args.video_id,),
                )
            hold = connection.execute(
                "SELECT owner,reason,expires_at FROM maintenance_hold WHERE singleton=1"
            ).fetchone()
            if owner and hold is not None and hold["owner"] == owner:
                connection.execute(
                    "DELETE FROM maintenance_hold WHERE singleton=1 AND owner=?",
                    (owner,),
                )
                self.record_hold_event(
                    connection,
                    "released",
                    str(owner),
                    str(hold["reason"]),
                    iso(utc_now()),
                    str(hold["expires_at"]),
                )
                hold_released = True
            connection.commit()
        self.save(phase="rolled_back", upload_hold=False)
        if hold_released:
            log("一次性任务已回滚；候选已恢复并释放自己的维护租约")
        else:
            log("一次性任务已回滚；未改动其他进程持有的维护租约")

    def execute(self) -> None:
        with self.acquire_lock():
            self.validate_schema_version()
            if self.args.rollback:
                self.rollback()
                return
            if self.state.get("phase") == "completed":
                log("一次性直播任务已经完成")
                return
            self.backup_database()
            self.reserve_job()
            self.start_upload_hold_worker()
            self.fetch_metadata()
            video = self.capture_until_end()
            bvid = self.upload_video(video)
            self.stop_upload_hold_worker()
            self.record_upload(video, bvid)
            self.release_upload_hold_after_success()
            self.wait_for_subtitle(bvid)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--video-id", required=True)
    value.add_argument("--url", required=True)
    value.add_argument("--title", required=True)
    value.add_argument("--main-start", required=True)
    value.add_argument("--hold-at", required=True)
    value.add_argument("--keep-before-seconds", type=int, default=10)
    value.add_argument("--work-dir", required=True)
    value.add_argument("--database", default="/var/lib/y2b/state.db")
    value.add_argument("--backup-dir", default="/var/lib/y2b/backups")
    value.add_argument("--config", default="/etc/y2b/config.toml")
    value.add_argument("--yt-dlp", default="/usr/local/bin/yt-dlp")
    value.add_argument("--youtube-cookies", default="/var/lib/y2b/youtube_cookies.txt")
    value.add_argument("--ffmpeg", default="/usr/local/bin/ffmpeg")
    value.add_argument("--ffprobe", default="/usr/local/bin/ffprobe")
    value.add_argument("--biliup", default="/usr/local/bin/biliup")
    value.add_argument("--bilibili-cookies", default="/var/lib/y2b/bilibili_cookies.json")
    value.add_argument("--y2b", default="/usr/local/bin/y2b")
    value.add_argument("--tags", default="荒野乱斗")
    value.add_argument("--tid", type=int, default=172)
    value.add_argument("--submit-interval-seconds", type=int, default=1800)
    value.add_argument("--rate-limit-cooldown-seconds", type=int, default=21600)
    value.add_argument("--subtitle-command-timeout-seconds", type=int, default=7200)
    value.add_argument("--rollback", action="store_true")
    return value


def main() -> int:
    args = parser().parse_args()
    os.environ["TZ"] = "UTC"
    with contextlib.suppress(AttributeError):
        time.tzset()
    try:
        LiveOnce(args).execute()
    except UploadUncertainError as error:
        log(
            "投稿结果已记录为 uncertain，已停止自动重投并正常退出；"
            f"请人工核对 Bilibili 创作中心: {error}"
        )
        return 0
    except SchemaVersionError as error:
        log(f"数据库 schema 不兼容，已拒绝运行；请同步 y2b 与脚本后人工处理: {error}")
        return 0
    except KeyboardInterrupt:
        log("收到中断信号；已落盘分段和状态可由 systemd 续跑")
        return 130
    except Exception as error:  # systemd will restart and resume from durable state
        log(f"一次性直播任务失败，将由 systemd 重试: {type(error).__name__}: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
