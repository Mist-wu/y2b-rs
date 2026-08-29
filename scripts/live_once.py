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
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
NEXT_SUBMIT_KEY = "bilibili.next_submit_at"
HOLD_UNTIL = "2099-12-31T23:59:59+00:00"
BVID_RE = re.compile(r"\bBV[0-9A-Za-z]+\b")
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


class LiveOnce:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.work = Path(args.work_dir)
        self.work.mkdir(parents=True, exist_ok=True)
        self.state_path = self.work / "state.json"
        self.state = load_json(self.state_path)
        self.marker = f"LIVE_ONCE:{args.video_id}"
        self.main_start = parse_time(args.main_start)
        self.cut_at = self.main_start - dt.timedelta(seconds=args.keep_before_seconds)
        self.hold_at = parse_time(args.hold_at)
        self.last_status_check = 0.0
        self.metadata: dict[str, Any] = self.state.get("metadata") or {}

    def save(self, **changes: Any) -> None:
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

    def ensure_upload_hold(self) -> bool:
        if utc_now() < self.hold_at:
            return False
        active = self.active_ordinary_uploads()
        if active:
            if not self.state.get("waiting_for_ordinary_upload"):
                log(f"22:00 已到，等待 {active} 个已开始的普通投稿完成")
                self.save(waiting_for_ordinary_upload=True)
            return False
        now = iso(utc_now())
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT value FROM settings WHERE key=?", (NEXT_SUBMIT_KEY,)
            ).fetchone()
            current = row[0] if row else None
            if current != HOLD_UNTIL:
                original = self.state.get("original_next_submit_at")
                if "original_next_submit_at" not in self.state:
                    original = current
                not_before = self.state.get("sidecar_not_before")
                if current and current != HOLD_UNTIL:
                    try:
                        current_time = parse_time(current)
                        previous = parse_time(not_before) if not_before else None
                        if current_time > utc_now() and (previous is None or current_time > previous):
                            not_before = iso(current_time)
                    except ValueError:
                        log(f"忽略无法解析的原投稿冷却时间: {current}")
                connection.execute(
                    """
                    INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at
                    """,
                    (NEXT_SUBMIT_KEY, HOLD_UNTIL, now),
                )
                connection.commit()
                self.save(
                    original_next_submit_at=original,
                    sidecar_not_before=not_before,
                    upload_hold=True,
                    waiting_for_ordinary_upload=False,
                )
                log("普通视频投稿已暂停；准备、下载和字幕 worker 不受影响")
        return True

    def wait_until_sidecar_can_upload(self) -> None:
        while True:
            self.ensure_upload_hold()
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
        next_submit = iso(utc_now() + dt.timedelta(seconds=self.args.submit_interval_seconds))
        now = iso(utc_now())
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute(
                """
                INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at
                """,
                (NEXT_SUBMIT_KEY, next_submit, now),
            )
            connection.commit()
        self.save(upload_hold=False, released_next_submit_at=next_submit)
        log(f"普通投稿恢复，下一次最早投稿时间: {next_submit}")

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

    def upload_video(self, video: Path) -> str:
        bvid = self.state.get("bvid") or self.recover_bvid_from_logs()
        if bvid:
            self.save(bvid=bvid, phase="uploaded")
            return str(bvid)
        cover = self.download_cover()
        attempt = int(self.state.get("upload_attempt", 0))
        while True:
            self.wait_until_sidecar_can_upload()
            attempt += 1
            upload_log = self.work / f"upload-attempt-{attempt:03d}.log"
            self.save(upload_attempt=attempt, phase="uploading")
            log(f"开始一次性直传，投稿尝试 {attempt}；标题={self.args.title}")
            command = self.upload_args(video, cover)
            with upload_log.open("wb", buffering=0) as handle:
                process = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT)
                try:
                    returncode = process.wait(timeout=4 * 3600)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    returncode = -1
            content = upload_log.read_text(encoding="utf-8", errors="replace")
            match = BVID_RE.search(content)
            if match:
                bvid = match.group(0)
                self.save(bvid=bvid, phase="uploaded")
                log(f"B站视频投稿成功: {bvid}")
                return bvid
            tail = content[-1500:].replace(self.args.bilibili_cookies, "[cookies]")
            log(f"投稿失败 exit={returncode}: {tail}")
            delay = self.args.rate_limit_cooldown_seconds if "21566" in content else min(900, 60 * attempt)
            self.save(last_upload_error=tail, next_upload_retry_at=iso(utc_now() + dt.timedelta(seconds=delay)))
            time.sleep(delay)

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
            connection.execute(
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
                    HOLD_UNTIL,
                    now,
                    self.state["job_id"],
                ),
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
                    f"一次性直播旁路投稿成功: {bvid}；开始无限等待官方字幕",
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
        with sqlite_connect(Path(self.args.database)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute(
                "SELECT id,bvid,error FROM jobs WHERE video_id=?", (self.args.video_id,)
            ).fetchone()
            if job and job["bvid"] is None and self.marker in (job["error"] or ""):
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
            if self.state.get("upload_hold"):
                original_submit = self.state.get("original_next_submit_at")
                if original_submit is None:
                    connection.execute("DELETE FROM settings WHERE key=?", (NEXT_SUBMIT_KEY,))
                else:
                    connection.execute(
                        "UPDATE settings SET value=?,updated_at=? WHERE key=?",
                        (original_submit, iso(utc_now()), NEXT_SUBMIT_KEY),
                    )
            connection.commit()
        self.save(phase="rolled_back", upload_hold=False)
        log("一次性任务已回滚；候选和普通投稿窗口已恢复")

    def execute(self) -> None:
        with self.acquire_lock():
            if self.args.rollback:
                self.rollback()
                return
            if self.state.get("phase") == "completed":
                log("一次性直播任务已经完成")
                return
            self.backup_database()
            self.reserve_job()
            self.fetch_metadata()
            video = self.capture_until_end()
            bvid = self.upload_video(video)
            self.record_upload(video, bvid)
            if self.state.get("upload_hold"):
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
    except KeyboardInterrupt:
        log("收到中断信号；已落盘分段和状态可由 systemd 续跑")
        return 130
    except Exception as error:  # systemd will restart and resume from durable state
        log(f"一次性直播任务失败，将由 systemd 重试: {type(error).__name__}: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
