#!/usr/bin/env python3
"""Record y2b job, process, cgroup, and disk performance snapshots as JSONL."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path


PROCESS_NAMES = {"y2b", "yt-dlp", "pi", "ffmpeg", "ffprobe", "biliup"}


def read_int(path: Path) -> int | None:
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def read_cpu_stat(path: Path) -> dict[str, int]:
    try:
        return {
            key: int(value)
            for key, value in (
                line.split(maxsplit=1) for line in path.read_text().splitlines()
            )
        }
    except (OSError, ValueError):
        return {}


def read_text(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def read_keyed_ints(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for line in path.read_text().splitlines():
            key, value = line.split(":", maxsplit=1)
            values[key] = int(value.strip())
    except (OSError, ValueError):
        return {}
    return values


def process_snapshots() -> list[dict[str, int | str]]:
    snapshots: list[dict[str, int | str]] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            name = (entry / "comm").read_text().strip()
            if name not in PROCESS_NAMES:
                continue
            status = (entry / "status").read_text().splitlines()
            rss_kib = next(
                int(line.split()[1]) for line in status if line.startswith("VmRSS:")
            )
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", errors="replace"
            )
            stat = (entry / "stat").read_text().split()
            io = read_keyed_ints(entry / "io")
            if name == "pi":
                cmdline = "pi [prompt omitted]"
            snapshots.append(
                {
                    "pid": int(entry.name),
                    "name": name,
                    "rss_kib": rss_kib,
                    "cpu_user_ticks": int(stat[13]),
                    "cpu_system_ticks": int(stat[14]),
                    "read_bytes": io.get("read_bytes", 0),
                    "write_bytes": io.get("write_bytes", 0),
                    "cmdline": cmdline[:512],
                }
            )
        except (IndexError, OSError, StopIteration, ValueError):
            continue
    return sorted(snapshots, key=lambda item: int(item["pid"]))


def job_snapshots(db_path: Path, video_ids: list[str]) -> list[dict[str, object]]:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        snapshots: list[dict[str, object]] = []
        for video_id in video_ids:
            job = connection.execute(
                """
                SELECT id,video_id,title,status,transfer_mode,attempt,bvid,error,updated_at
                FROM jobs WHERE video_id=?
                """,
                (video_id,),
            ).fetchone()
            if job is None:
                snapshots.append({"video_id": video_id, "missing": True})
                continue
            item = dict(job)
            stage = connection.execute(
                """
                SELECT id,stage,status,started_at,finished_at,duration_ms,peak_rss_kib,detail
                FROM stage_runs WHERE job_id=? ORDER BY id DESC LIMIT 1
                """,
                (job["id"],),
            ).fetchone()
            item["latest_stage"] = dict(stage) if stage is not None else None
            item["ai_tokens"] = connection.execute(
                "SELECT coalesce(sum(total_tokens),0) FROM ai_calls WHERE job_id=?",
                (job["id"],),
            ).fetchone()[0]
            snapshots.append(item)
        return snapshots
    finally:
        connection.close()


def snapshot(args: argparse.Namespace) -> dict[str, object]:
    cgroup = Path(args.cgroup)
    disk = shutil.disk_usage(args.data_dir)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "jobs": job_snapshots(Path(args.db), args.video_ids),
        "processes": process_snapshots(),
        "cgroup": {
            "memory_current": read_int(cgroup / "memory.current"),
            "memory_peak": read_int(cgroup / "memory.peak"),
            "memory_swap_current": read_int(cgroup / "memory.swap.current"),
            "memory_events": read_text(cgroup / "memory.events"),
            "cpu": read_cpu_stat(cgroup / "cpu.stat"),
        },
        "disk": {"total": disk.total, "used": disk.used, "free": disk.free},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--video-ids", nargs="+", required=True)
    parser.add_argument("--data-dir", default="/var/lib/y2b")
    parser.add_argument(
        "--cgroup", default="/sys/fs/cgroup/system.slice/y2b-watch.service"
    )
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--duration", type=float, default=36_000.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration
    with output.open("a", encoding="utf-8") as stream:
        while True:
            started = time.monotonic()
            try:
                value = snapshot(args)
            except Exception as error:  # keep the ten-hour monitor alive
                value = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "monitor_error": repr(error),
                }
            stream.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
            stream.write("\n")
            stream.flush()
            if time.monotonic() >= deadline:
                break
            time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
