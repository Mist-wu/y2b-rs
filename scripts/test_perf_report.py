import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("perf-report.py")
SPEC = importlib.util.spec_from_file_location("perf_report", SCRIPT)
assert SPEC and SPEC.loader
PERF_REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PERF_REPORT)


class PerfReportTests(unittest.TestCase):
    def test_summarizes_resources_rates_errors_and_transitions(self):
        records = [
            {
                "timestamp": "2026-08-04T00:00:00+00:00",
                "jobs": [{"video_id": "video", "status": "rendering", "bvid": None}],
                "processes": [
                    {
                        "pid": 7,
                        "name": "ffmpeg",
                        "rss_kib": 100,
                        "cpu_user_ticks": 100,
                        "cpu_system_ticks": 0,
                        "read_bytes": 0,
                        "write_bytes": 0,
                    }
                ],
                "cgroup": {
                    "memory_current": 1000,
                    "memory_swap_current": 0,
                    "memory_events": "high 1\noom 0",
                },
                "disk": {"free": 5000},
            },
            {
                "timestamp": "2026-08-04T00:00:10+00:00",
                "jobs": [{"video_id": "video", "status": "completed", "bvid": "BV1"}],
                "processes": [
                    {
                        "pid": 7,
                        "name": "ffmpeg",
                        "rss_kib": 200,
                        "cpu_user_ticks": 200,
                        "cpu_system_ticks": 0,
                        "read_bytes": 1048576,
                        "write_bytes": 2097152,
                    }
                ],
                "cgroup": {
                    "memory_current": 2000,
                    "memory_swap_current": 100,
                    "memory_events": "high 3\noom 0",
                },
                "disk": {"free": 4000},
            },
            {"timestamp": "2026-08-04T00:00:20+00:00", "monitor_error": "test"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.jsonl"
            path.write_text("\n".join(json.dumps(item) for item in records) + "\n")
            report = PERF_REPORT.summarize(path, 100)

        self.assertEqual(report["records"], 3)
        self.assertEqual(report["complete_records"], 2)
        self.assertEqual(report["monitor_errors"], 1)
        self.assertEqual(report["max_memory_bytes"], 2000)
        self.assertEqual(report["max_swap_bytes"], 100)
        self.assertEqual(report["memory_event_max"]["high"], 3)
        self.assertEqual(report["process_max_rss_kib"]["ffmpeg"], 200)
        self.assertEqual(report["process_max_rates"]["ffmpeg"]["cpu_percent"], 10.0)
        self.assertEqual(report["process_max_rates"]["ffmpeg"]["read_mib_s"], 0.1)
        self.assertEqual(report["process_max_rates"]["ffmpeg"]["write_mib_s"], 0.2)
        self.assertEqual(
            [item["status"] for item in report["job_transitions"]["video"]],
            ["rendering", "completed"],
        )


if __name__ == "__main__":
    unittest.main()
