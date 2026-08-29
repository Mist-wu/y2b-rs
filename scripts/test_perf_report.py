import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("perf-report.py")
SPEC = importlib.util.spec_from_file_location("perf_report", SCRIPT)
assert SPEC and SPEC.loader
PERF_REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PERF_REPORT)


def load_script(name: str, filename: str):
    script = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(name, script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PERF_MONITOR = load_script("perf_monitor", "perf-monitor.py")
GLOSSARY_AUDIT = load_script("audit_brawl_glossary", "audit_brawl_glossary.py")


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


class OperationalArgumentTests(unittest.TestCase):
    def assert_parse_error(self, parse_args, argv):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                parse_args(argv)
        self.assertEqual(raised.exception.code, 2)

    def test_perf_tools_reject_nonpositive_and_nonfinite_rates(self):
        common = ["--db", "state.db", "--output", "out.jsonl", "--video-ids", "vid"]
        for option, value in [
            ("--interval", "0"),
            ("--interval", "nan"),
            ("--duration", "-1"),
            ("--duration", "inf"),
        ]:
            with self.subTest(option=option, value=value):
                self.assert_parse_error(PERF_MONITOR.parse_args, common + [option, value])
        self.assert_parse_error(
            PERF_REPORT.parse_args, ["monitor.jsonl", "--clock-ticks", "0"]
        )

    def test_glossary_audit_rejects_invalid_batch_worker_timeout_and_shard(self):
        for option in ["--batch-size", "--workers", "--timeout", "--shard-count"]:
            with self.subTest(option=option):
                self.assert_parse_error(GLOSSARY_AUDIT.parse_args, [option, "0"])
        self.assert_parse_error(GLOSSARY_AUDIT.parse_args, ["--shard-index", "-1"])
        self.assert_parse_error(
            GLOSSARY_AUDIT.parse_args,
            ["--shard-index", "1", "--shard-count", "1"],
        )
        self.assert_parse_error(GLOSSARY_AUDIT.parse_args, ["--timeout"])


if __name__ == "__main__":
    unittest.main()
