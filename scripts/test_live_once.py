import argparse
import datetime as dt
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from live_once import (
    HOLD_OWNER_KEY,
    HOLD_PREVIOUS_KEY,
    HOLD_UNTIL,
    NEXT_SUBMIT_KEY,
    LiveOnce,
    iso,
    parse_time,
    utc_now,
)


UTC = dt.timezone.utc


def arguments(root: Path) -> argparse.Namespace:
    return argparse.Namespace(
        video_id="example123",
        url="https://www.youtube.com/watch?v=example123",
        title="乱斗抢先看",
        main_start="2026-08-29T23:00:00+08:00",
        hold_at="2026-08-29T22:00:00+08:00",
        keep_before_seconds=10,
        work_dir=str(root / "work"),
        database=str(root / "state.db"),
        backup_dir=str(root / "backups"),
        config="/etc/y2b/config.toml",
        yt_dlp="yt-dlp",
        youtube_cookies=str(root / "youtube.txt"),
        ffmpeg="ffmpeg",
        ffprobe="ffprobe",
        biliup="biliup",
        bilibili_cookies=str(root / "bilibili.json"),
        y2b="y2b",
        tags="荒野乱斗",
        tid=172,
        submit_interval_seconds=1800,
        rate_limit_cooldown_seconds=21600,
        subtitle_command_timeout_seconds=7200,
        rollback=False,
    )


def make_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE channels(id INTEGER PRIMARY KEY, transfer_mode TEXT);
        INSERT INTO channels VALUES(2, 'translated');
        CREATE TABLE video_candidates(
          video_id TEXT PRIMARY KEY, channel_id INTEGER, url TEXT, title TEXT,
          published_at TEXT, source TEXT, discovered_at TEXT, gate_state TEXT,
          gate_attempts INTEGER, next_gate_at TEXT, last_error TEXT
        );
        INSERT INTO video_candidates VALUES(
          'example123',2,'https://www.youtube.com/watch?v=example123','Original',
          '2026-08-29T15:00:00+00:00','rss','2026-08-29T10:00:00+00:00',
          'deferred',4,'2026-08-29T14:00:00+00:00','upcoming'
        );
        CREATE TABLE jobs(
          id TEXT PRIMARY KEY, channel_id INTEGER, video_id TEXT UNIQUE, url TEXT,
          title TEXT, status TEXT, transfer_mode TEXT, published_at TEXT,
          discovered_at TEXT, error TEXT, created_at TEXT, updated_at TEXT, bvid TEXT
        );
        CREATE TABLE events(
          id INTEGER PRIMARY KEY, job_id TEXT, level TEXT, message TEXT, created_at TEXT
        );
        CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
        """
    )
    connection.commit()
    connection.close()


class LiveOnceTests(unittest.TestCase):
    def test_time_and_trim_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            item = LiveOnce(arguments(Path(directory)))
            self.assertEqual(item.main_start, parse_time("2026-08-29T15:00:00Z"))
            self.assertEqual(item.cut_at, parse_time("2026-08-29T14:59:50Z"))

    def test_upload_arguments_have_no_dynamic_or_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            item = LiveOnce(arguments(Path(directory)))
            item.metadata = {
                "title": "Official Talk #BrawlTalk",
                "uploader": "Brawl Stars",
                "upload_date": "20260829",
            }
            command = item.upload_args(Path("video.mp4"), Path("cover.jpg"))
            self.assertEqual(command[command.index("--title") + 1], "乱斗抢先看")
            self.assertEqual(command[command.index("--copyright") + 1], "1")
            self.assertEqual(command[command.index("--no-reprint") + 1], "0")
            self.assertNotIn("--dynamic", command)
            self.assertNotIn("--source", command)
            self.assertIn("原标题：Official Talk", command[command.index("--desc") + 1])

    def test_upload_hold_worker_runs_independently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory))
            args.hold_at = "2020-01-01T00:00:00Z"
            item = LiveOnce(args)
            called = threading.Event()

            def hold() -> bool:
                called.set()
                return True

            item.ensure_upload_hold = hold  # type: ignore[method-assign]
            item.start_upload_hold_worker()
            self.assertTrue(called.wait(1))
            item.stop_upload_hold_worker()

    def test_upload_hold_waits_for_uploading_job_before_claiming(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            args.hold_at = "2020-01-01T00:00:00Z"
            make_database(Path(args.database))
            connection = sqlite3.connect(args.database)
            connection.execute(
                """
                INSERT INTO jobs(id,video_id,url,status,transfer_mode,created_at,updated_at)
                VALUES('ordinary','ordinary','https://youtu.be/ordinary','uploading','direct','now','now')
                """
            )
            connection.commit()
            connection.close()

            item = LiveOnce(args)
            self.assertFalse(item.ensure_upload_hold())
            connection = sqlite3.connect(args.database)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM settings").fetchone()[0], 0
            )
            connection.execute("UPDATE jobs SET status='completed' WHERE id='ordinary'")
            connection.commit()
            connection.close()

            self.assertTrue(item.ensure_upload_hold())
            connection = sqlite3.connect(args.database)
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            connection.close()
            self.assertEqual(settings[NEXT_SUBMIT_KEY], HOLD_UNTIL)
            self.assertEqual(settings[HOLD_OWNER_KEY], item.state["upload_hold_owner"])
            self.assertEqual(json.loads(settings[HOLD_PREVIOUS_KEY]), None)

    def test_upload_hold_release_preserves_newer_platform_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            args.hold_at = "2020-01-01T00:00:00Z"
            make_database(Path(args.database))
            item = LiveOnce(args)
            self.assertTrue(item.ensure_upload_hold())
            later = iso(utc_now() + dt.timedelta(hours=12))
            connection = sqlite3.connect(args.database)
            connection.execute(
                "UPDATE settings SET value=? WHERE key=?", (later, NEXT_SUBMIT_KEY)
            )
            connection.commit()
            connection.close()

            item.release_upload_hold_after_success()
            connection = sqlite3.connect(args.database)
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            connection.close()
            self.assertEqual(settings[NEXT_SUBMIT_KEY], later)
            self.assertNotIn(HOLD_OWNER_KEY, settings)
            self.assertNotIn(HOLD_PREVIOUS_KEY, settings)

    def test_upload_hold_rollback_restores_only_its_own_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            args.hold_at = "2020-01-01T00:00:00Z"
            make_database(Path(args.database))
            original = iso(utc_now() + dt.timedelta(minutes=10))
            connection = sqlite3.connect(args.database)
            connection.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (NEXT_SUBMIT_KEY, original, "now"),
            )
            connection.commit()
            connection.close()
            item = LiveOnce(args)
            self.assertTrue(item.ensure_upload_hold())

            resumed = LiveOnce(args)
            self.assertTrue(resumed.ensure_upload_hold())
            resumed.rollback()
            connection = sqlite3.connect(args.database)
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            connection.close()
            self.assertEqual(settings[NEXT_SUBMIT_KEY], original)
            self.assertNotIn(HOLD_OWNER_KEY, settings)
            self.assertNotIn(HOLD_PREVIOUS_KEY, settings)

    def test_upload_hold_rollback_does_not_clobber_foreign_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            make_database(Path(args.database))
            item = LiveOnce(args)
            item.save(upload_hold=True, upload_hold_owner="ours")
            deadline = iso(utc_now() + dt.timedelta(hours=6))
            connection = sqlite3.connect(args.database)
            connection.executemany(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)",
                [
                    (NEXT_SUBMIT_KEY, deadline, "now"),
                    (HOLD_OWNER_KEY, "foreign", "now"),
                    (HOLD_PREVIOUS_KEY, json.dumps(None), "now"),
                ],
            )
            connection.commit()
            connection.close()

            with self.assertRaisesRegex(RuntimeError, "owned by another process"):
                item.release_upload_hold_after_success()
            item.rollback()
            connection = sqlite3.connect(args.database)
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            connection.close()
            self.assertEqual(settings[NEXT_SUBMIT_KEY], deadline)
            self.assertEqual(settings[HOLD_OWNER_KEY], "foreign")

    def test_segments_are_deduplicated_and_keep_pre_roll(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            item = LiveOnce(arguments(root))
            first = item.work / "segments" / "attempt-001"
            second = item.work / "segments" / "attempt-002"
            first.mkdir(parents=True)
            second.mkdir(parents=True)
            (first / "segment-20260829T145939Z.ts").write_bytes(b"old")
            (first / "segment-20260829T145940Z.ts").write_bytes(b"small")
            (second / "segment-20260829T145940Z.ts").write_bytes(b"larger-copy")
            (second / "segment-20260829T145950Z.ts").write_bytes(b"main")
            selected = item.selected_segments()
            self.assertEqual([value[0].second for value in selected], [40, 50])
            self.assertEqual(selected[0][1].parent.name, "attempt-002")

    def test_reserve_and_rollback_are_transactional(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            make_database(Path(args.database))
            item = LiveOnce(args)
            item.backup_database()
            item.reserve_job()
            connection = sqlite3.connect(args.database)
            status, error = connection.execute(
                "SELECT status,error FROM jobs WHERE video_id='example123'"
            ).fetchone()
            gate = connection.execute(
                "SELECT gate_state FROM video_candidates WHERE video_id='example123'"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(status, "paused")
            self.assertIn("LIVE_ONCE:example123", error)
            self.assertEqual(gate, "promoted")
            item.rollback()
            connection = sqlite3.connect(args.database)
            count = connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            candidate = connection.execute(
                "SELECT gate_state,gate_attempts,next_gate_at,last_error FROM video_candidates"
            ).fetchone()
            connection.close()
            self.assertEqual(count, 0)
            self.assertEqual(
                candidate,
                ("deferred", 4, "2026-08-29T14:00:00+00:00", "upcoming"),
            )

    def test_source_metadata_matches_rust_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            item = LiveOnce(arguments(Path(directory)))
            item.metadata = {"title": "Talk", "timestamp": 1788015600}
            item.state["video_probe"] = {
                "duration": 771.2,
                "width": 1920,
                "height": 1080,
            }
            value = item.source_metadata(Path("unused.mp4"))
            expected = {
                "id",
                "url",
                "title",
                "description",
                "uploader",
                "upload_date",
                "channel",
                "channel_id",
                "timestamp",
                "duration",
                "width",
                "height",
                "fps",
                "thumbnail_url",
                "webpage_url",
                "live_status",
                "default_audio_language",
            }
            self.assertEqual(set(value), expected)
            json.dumps(value)


if __name__ == "__main__":
    unittest.main()
