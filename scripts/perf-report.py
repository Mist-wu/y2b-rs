#!/usr/bin/env python3
"""Summarize one or more y2b performance-monitor JSONL files."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_memory_events(value: str | None) -> dict[str, int]:
    if not value:
        return {}
    result: dict[str, int] = {}
    for line in value.splitlines():
        try:
            key, count = line.split(maxsplit=1)
            result[key] = int(count)
        except ValueError:
            continue
    return result


def load_records(path: Path) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    invalid = 0
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                invalid += 1
                continue
            if isinstance(value, dict):
                records.append(value)
            else:
                invalid += 1
    return records, invalid


def summarize(path: Path, clock_ticks: int) -> dict[str, Any]:
    records, invalid = load_records(path)
    complete = [record for record in records if "jobs" in record]
    process_max_rss: dict[str, int] = defaultdict(int)
    process_max_count: dict[str, int] = defaultdict(int)
    rate_max: dict[str, dict[str, float]] = defaultdict(
        lambda: {"cpu_percent": 0.0, "read_mib_s": 0.0, "write_mib_s": 0.0}
    )
    previous: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
    event_max: dict[str, int] = defaultdict(int)
    transitions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    last_status: dict[str, str] = {}
    max_memory = 0
    max_swap = 0
    min_disk_free: int | None = None

    for record in complete:
        timestamp = record.get("timestamp")
        try:
            epoch = __import__("datetime").datetime.fromisoformat(timestamp).timestamp()
        except (AttributeError, TypeError, ValueError):
            epoch = 0.0
        counts: dict[str, int] = defaultdict(int)
        for process in record.get("processes", []):
            name = str(process.get("name", "unknown"))
            pid = int(process.get("pid", 0))
            counts[name] += 1
            process_max_rss[name] = max(
                process_max_rss[name], int(process.get("rss_kib", 0))
            )
            key = (name, pid)
            old = previous.get(key)
            if old and epoch > old[0]:
                seconds = epoch - old[0]
                prior = old[1]
                cpu_delta = (
                    int(process.get("cpu_user_ticks", 0))
                    + int(process.get("cpu_system_ticks", 0))
                    - int(prior.get("cpu_user_ticks", 0))
                    - int(prior.get("cpu_system_ticks", 0))
                )
                read_delta = int(process.get("read_bytes", 0)) - int(
                    prior.get("read_bytes", 0)
                )
                write_delta = int(process.get("write_bytes", 0)) - int(
                    prior.get("write_bytes", 0)
                )
                if cpu_delta >= 0:
                    rate_max[name]["cpu_percent"] = max(
                        rate_max[name]["cpu_percent"],
                        cpu_delta / clock_ticks / seconds * 100.0,
                    )
                if read_delta >= 0:
                    rate_max[name]["read_mib_s"] = max(
                        rate_max[name]["read_mib_s"], read_delta / seconds / 1048576
                    )
                if write_delta >= 0:
                    rate_max[name]["write_mib_s"] = max(
                        rate_max[name]["write_mib_s"], write_delta / seconds / 1048576
                    )
            previous[key] = (epoch, process)
        for name, count in counts.items():
            process_max_count[name] = max(process_max_count[name], count)

        cgroup = record.get("cgroup", {})
        max_memory = max(max_memory, int(cgroup.get("memory_current") or 0))
        max_swap = max(max_swap, int(cgroup.get("memory_swap_current") or 0))
        for key, count in parse_memory_events(cgroup.get("memory_events")).items():
            event_max[key] = max(event_max[key], count)
        disk_free = record.get("disk", {}).get("free")
        if isinstance(disk_free, int):
            min_disk_free = (
                disk_free if min_disk_free is None else min(min_disk_free, disk_free)
            )

        for job in record.get("jobs", []):
            video_id = str(job.get("video_id", "unknown"))
            status = str(job.get("status", "unknown"))
            if last_status.get(video_id) != status:
                transitions[video_id].append(
                    {"timestamp": timestamp, "status": status, "bvid": job.get("bvid")}
                )
                last_status[video_id] = status

    return {
        "path": str(path),
        "records": len(records),
        "complete_records": len(complete),
        "monitor_errors": sum("monitor_error" in record for record in records),
        "invalid_json_lines": invalid,
        "first_timestamp": records[0].get("timestamp") if records else None,
        "last_timestamp": records[-1].get("timestamp") if records else None,
        "max_memory_bytes": max_memory,
        "max_swap_bytes": max_swap,
        "min_disk_free_bytes": min_disk_free,
        "memory_event_max": dict(sorted(event_max.items())),
        "process_max_rss_kib": dict(sorted(process_max_rss.items())),
        "process_max_count": dict(sorted(process_max_count.items())),
        "process_max_rates": {
            name: {key: round(value, 3) for key, value in rates.items()}
            for name, rates in sorted(rate_max.items())
        },
        "job_transitions": dict(sorted(transitions.items())),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--clock-ticks", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.clock_ticks <= 0:
        raise SystemExit("--clock-ticks must be positive")
    reports = [summarize(path, args.clock_ticks) for path in args.logs]
    print(json.dumps({"reports": reports}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
