import argparse
import datetime as dt
import json
import re
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from live_once import (
    EXPECTED_SCHEMA_VERSION,
    MAINTENANCE_HOLD_LEASE_SECONDS,
    MAINTENANCE_HOLD_RENEW_INTERVAL_SECONDS,
    NEXT_SUBMIT_KEY,
    LiveOnce,
    SchemaVersionError,
    UploadUncertainError,
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


def make_database(path: Path, schema_version: int = EXPECTED_SCHEMA_VERSION) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        f"""
        CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT);
        INSERT INTO schema_migrations VALUES({schema_version}, 'now');
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
          discovered_at TEXT, error TEXT, created_at TEXT, updated_at TEXT, bvid TEXT,
          raw_video_path TEXT, duration_seconds REAL, width INTEGER, height INTEGER,
          fps REAL, source_metadata_json TEXT, subtitle_attempt INTEGER,
          subtitle_retry_at TEXT
        );
        CREATE UNIQUE INDEX idx_jobs_bvid_unique
          ON jobs(bvid) WHERE bvid IS NOT NULL;
        CREATE TABLE events(
          id INTEGER PRIMARY KEY, job_id TEXT, level TEXT, message TEXT, created_at TEXT
        );
        CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
        CREATE TABLE maintenance_hold(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),
          owner TEXT NOT NULL CHECK(owner<>''),
          reason TEXT NOT NULL,
          acquired_at TEXT NOT NULL,
          heartbeat_at TEXT NOT NULL,
          expires_at TEXT NOT NULL
        );
        CREATE TABLE maintenance_hold_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          action TEXT NOT NULL CHECK(action IN('acquired','taken_over','renewed','released')),
          owner TEXT NOT NULL,
          previous_owner TEXT,
          reason TEXT NOT NULL,
          previous_reason TEXT,
          occurred_at TEXT NOT NULL,
          expires_at TEXT
        );
        CREATE TABLE upload_attempts(
          id TEXT PRIMARY KEY,
          job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
          status TEXT NOT NULL,
          bvid TEXT,
          detail TEXT,
          started_at TEXT NOT NULL,
          finished_at TEXT
        );
        """
    )
    connection.commit()
    connection.close()


def fake_biliup(responses: list[tuple[int, str]]) -> mock.Mock:
    remaining = iter(responses)

    def start(_command: list[str], *, stdout: object, stderr: object) -> mock.Mock:
        del stderr
        returncode, output = next(remaining)
        stdout.write(output.encode("utf-8"))
        process = mock.Mock()
        process.wait.return_value = returncode
        return process

    return mock.Mock(side_effect=start)


class LiveOnceTests(unittest.TestCase):
    def prepare_upload(self, root: Path) -> tuple[LiveOnce, Path]:
        args = arguments(root)
        make_database(Path(args.database))
        item = LiveOnce(args)
        item.reserve_job()
        video = root / "video.mp4"
        video.write_bytes(b"video")
        item.download_cover = mock.Mock(return_value=None)  # type: ignore[method-assign]
        item.wait_until_sidecar_can_upload = mock.Mock()  # type: ignore[method-assign]
        return item, video

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

    def test_upload_hold_worker_keeps_renewing_until_stopped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(Path(directory))
            args.hold_at = "2020-01-01T00:00:00Z"
            item = LiveOnce(args)
            calls = 0

            def hold() -> bool:
                nonlocal calls
                calls += 1
                if calls == 2:
                    item.hold_stop.set()
                return True

            item.ensure_upload_hold = hold  # type: ignore[method-assign]
            with mock.patch.object(item.hold_stop, "wait", return_value=False):
                item.start_upload_hold_worker()
                assert item.hold_thread is not None
                item.hold_thread.join(timeout=1)
            self.assertEqual(calls, 2)

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
                connection.execute("SELECT COUNT(*) FROM maintenance_hold").fetchone()[0],
                0,
            )
            connection.execute("UPDATE jobs SET status='completed' WHERE id='ordinary'")
            connection.commit()
            connection.close()

            before = utc_now()
            self.assertTrue(item.ensure_upload_hold())
            connection = sqlite3.connect(args.database)
            hold = connection.execute(
                "SELECT owner,expires_at FROM maintenance_hold WHERE singleton=1"
            ).fetchone()
            events = list(
                connection.execute(
                    "SELECT action,owner FROM maintenance_hold_events ORDER BY id"
                )
            )
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            connection.close()
            self.assertEqual(hold[0], item.state["upload_hold_owner"])
            self.assertGreater(parse_time(hold[1]), before)
            self.assertLessEqual(
                parse_time(hold[1]),
                before
                + dt.timedelta(seconds=MAINTENANCE_HOLD_LEASE_SECONDS + 2),
            )
            self.assertEqual(events, [("acquired", hold[0])])
            self.assertNotIn(NEXT_SUBMIT_KEY, settings)

    def test_upload_hold_is_renewed_before_its_short_lease_expires(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            args.hold_at = "2020-01-01T00:00:00Z"
            make_database(Path(args.database))
            item = LiveOnce(args)
            self.assertTrue(item.ensure_upload_hold())
            almost_expired = iso(
                utc_now()
                + dt.timedelta(seconds=MAINTENANCE_HOLD_RENEW_INTERVAL_SECONDS - 1)
            )
            connection = sqlite3.connect(args.database)
            connection.execute(
                "UPDATE maintenance_hold SET expires_at=? WHERE singleton=1",
                (almost_expired,),
            )
            connection.commit()
            connection.close()

            self.assertTrue(item.ensure_upload_hold())
            connection = sqlite3.connect(args.database)
            expires_at = connection.execute(
                "SELECT expires_at FROM maintenance_hold WHERE singleton=1"
            ).fetchone()[0]
            actions = [
                row[0]
                for row in connection.execute(
                    "SELECT action FROM maintenance_hold_events ORDER BY id"
                )
            ]
            connection.close()
            self.assertGreater(
                parse_time(expires_at),
                utc_now()
                + dt.timedelta(seconds=MAINTENANCE_HOLD_LEASE_SECONDS - 5),
            )
            self.assertEqual(actions, ["acquired", "renewed"])

    def test_expired_live_once_lease_no_longer_blocks_automatically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            args.hold_at = "2020-01-01T00:00:00Z"
            make_database(Path(args.database))
            item = LiveOnce(args)
            self.assertTrue(item.ensure_upload_hold())
            connection = sqlite3.connect(args.database)
            connection.execute(
                "UPDATE maintenance_hold SET expires_at=? WHERE singleton=1",
                (iso(utc_now() - dt.timedelta(seconds=1)),),
            )
            connection.commit()
            active = connection.execute(
                "SELECT COUNT(*) FROM maintenance_hold WHERE expires_at>?",
                (iso(utc_now()),),
            ).fetchone()[0]
            retained = connection.execute(
                "SELECT owner FROM maintenance_hold WHERE singleton=1"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(active, 0)
            self.assertEqual(retained, item.state["upload_hold_owner"])

    def test_crashed_process_lease_can_be_taken_over_by_a_later_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_args = arguments(root)
            first_args.hold_at = "2020-01-01T00:00:00Z"
            make_database(Path(first_args.database))
            first = LiveOnce(first_args)
            self.assertTrue(first.ensure_upload_hold())
            first_owner = first.state["upload_hold_owner"]
            connection = sqlite3.connect(first_args.database)
            connection.execute(
                "UPDATE maintenance_hold SET expires_at=? WHERE singleton=1",
                (iso(utc_now() - dt.timedelta(seconds=1)),),
            )
            connection.commit()
            connection.close()

            second_args = arguments(root)
            second_args.hold_at = "2020-01-01T00:00:00Z"
            second_args.work_dir = str(root / "second-work")
            second = LiveOnce(second_args)
            self.assertTrue(second.ensure_upload_hold())
            second_owner = second.state["upload_hold_owner"]
            connection = sqlite3.connect(second_args.database)
            hold_owner = connection.execute(
                "SELECT owner FROM maintenance_hold WHERE singleton=1"
            ).fetchone()[0]
            takeover = connection.execute(
                """
                SELECT action,owner,previous_owner
                FROM maintenance_hold_events ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
            connection.close()
            self.assertNotEqual(first_owner, second_owner)
            self.assertEqual(hold_owner, second_owner)
            self.assertEqual(takeover, ("taken_over", second_owner, first_owner))

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
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (NEXT_SUBMIT_KEY, later, "now"),
            )
            connection.commit()
            connection.close()

            item.release_upload_hold_after_success()
            connection = sqlite3.connect(args.database)
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            hold_count = connection.execute(
                "SELECT COUNT(*) FROM maintenance_hold"
            ).fetchone()[0]
            action = connection.execute(
                "SELECT action FROM maintenance_hold_events ORDER BY id DESC LIMIT 1"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(settings[NEXT_SUBMIT_KEY], later)
            self.assertEqual(hold_count, 0)
            self.assertEqual(action, "released")

    def test_upload_hold_rollback_releases_only_its_own_lease(self) -> None:
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
            hold_count = connection.execute(
                "SELECT COUNT(*) FROM maintenance_hold"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(settings[NEXT_SUBMIT_KEY], original)
            self.assertEqual(hold_count, 0)

    def test_upload_hold_rollback_does_not_clobber_foreign_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            make_database(Path(args.database))
            item = LiveOnce(args)
            item.save(upload_hold=True, upload_hold_owner="ours")
            deadline = iso(utc_now() + dt.timedelta(hours=6))
            now = iso(utc_now())
            expires_at = iso(utc_now() + dt.timedelta(minutes=5))
            connection = sqlite3.connect(args.database)
            connection.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)",
                (NEXT_SUBMIT_KEY, deadline, now),
            )
            connection.execute(
                """
                INSERT INTO maintenance_hold(
                  singleton,owner,reason,acquired_at,heartbeat_at,expires_at
                ) VALUES(1,'foreign','部署维护',?,?,?)
                """,
                (now, now, expires_at),
            )
            connection.commit()
            connection.close()

            with self.assertRaisesRegex(RuntimeError, "owned by another process"):
                item.release_upload_hold_after_success()
            item.rollback()
            connection = sqlite3.connect(args.database)
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            hold_owner = connection.execute(
                "SELECT owner FROM maintenance_hold WHERE singleton=1"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(settings[NEXT_SUBMIT_KEY], deadline)
            self.assertEqual(hold_owner, "foreign")

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

    def test_uncertain_upload_is_persisted_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            item, video = self.prepare_upload(Path(directory))
            popen = fake_biliup([(0, "biliup exited without a response\n")])
            with mock.patch("live_once.subprocess.Popen", popen):
                with self.assertRaisesRegex(UploadUncertainError, "人工核对"):
                    item.upload_video(video)
                resumed = LiveOnce(item.args)
                with self.assertRaisesRegex(UploadUncertainError, "人工核对"):
                    resumed.upload_video(video)

            self.assertEqual(popen.call_count, 1)
            connection = sqlite3.connect(item.args.database)
            attempt = connection.execute(
                "SELECT status,detail FROM upload_attempts"
            ).fetchone()
            job = connection.execute(
                "SELECT status FROM jobs WHERE id=?", (item.state["job_id"],)
            ).fetchone()
            connection.close()
            self.assertEqual(attempt[0], "uncertain")
            self.assertIn("禁止自动重投", attempt[1])
            self.assertEqual(job[0], "upload_uncertain")
            self.assertEqual(item.state["phase"], "upload_uncertain")
            with self.assertRaisesRegex(RuntimeError, "禁止回滚"):
                item.rollback()

    def test_explicit_rejection_can_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            item, video = self.prepare_upload(Path(directory))
            bvid = "BV1uxE16ZE7e"
            popen = fake_biliup(
                [
                    (1, '{"code":21566,"message":"rate limited"}\n'),
                    (0, f'{{"code":0,"data":{{"bvid":"{bvid}"}}}}\n'),
                ]
            )
            with (
                mock.patch("live_once.subprocess.Popen", popen),
                mock.patch("live_once.time.sleep") as sleep,
            ):
                self.assertEqual(item.upload_video(video), bvid)

            self.assertEqual(popen.call_count, 2)
            sleep.assert_called_once_with(item.args.rate_limit_cooldown_seconds)
            connection = sqlite3.connect(item.args.database)
            statuses = [
                row[0]
                for row in connection.execute(
                    "SELECT status FROM upload_attempts ORDER BY rowid"
                )
            ]
            stored_bvid = connection.execute(
                "SELECT bvid FROM jobs WHERE id=?", (item.state["job_id"],)
            ).fetchone()[0]
            connection.close()
            self.assertEqual(statuses, ["failed", "succeeded"])
            self.assertEqual(stored_bvid, bvid)

    def test_python_schema_version_matches_rust(self) -> None:
        rust_database = Path(__file__).resolve().parents[1] / "src" / "db.rs"
        versions = re.findall(
            r"^pub const CURRENT_SCHEMA_VERSION: i64 = (\d+);$",
            rust_database.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        self.assertEqual(versions, [str(EXPECTED_SCHEMA_VERSION)])

    def test_schema_version_mismatch_refuses_to_run(self) -> None:
        for version in (EXPECTED_SCHEMA_VERSION - 1, EXPECTED_SCHEMA_VERSION + 1):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                args = arguments(root)
                make_database(Path(args.database), version)
                item = LiveOnce(args)
                with mock.patch.object(item, "backup_database") as backup:
                    with self.assertRaisesRegex(
                        SchemaVersionError, "schema 版本不兼容"
                    ):
                        item.execute()
                backup.assert_not_called()
                connection = sqlite3.connect(args.database)
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0
                )
                connection.close()

    def test_duplicate_bvid_is_marked_uncertain_for_manual_review(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            item, video = self.prepare_upload(Path(directory))
            bvid = "BV1uxE16ZE7e"
            connection = sqlite3.connect(item.args.database)
            connection.execute(
                """
                INSERT INTO jobs(
                  id,video_id,url,status,transfer_mode,created_at,updated_at,bvid
                ) VALUES('other','other','https://youtu.be/other','completed','direct','now','now',?)
                """,
                (bvid,),
            )
            connection.commit()
            connection.close()
            popen = fake_biliup(
                [(0, f'{{"code":0,"data":{{"bvid":"{bvid}"}}}}\n')]
            )

            with mock.patch("live_once.subprocess.Popen", popen):
                with self.assertRaisesRegex(
                    UploadUncertainError, "已归属其他任务.*人工核对"
                ):
                    item.upload_video(video)

            connection = sqlite3.connect(item.args.database)
            attempt = connection.execute(
                "SELECT status,bvid,detail FROM upload_attempts WHERE job_id=?",
                (item.state["job_id"],),
            ).fetchone()
            job = connection.execute(
                "SELECT status,bvid,error FROM jobs WHERE id=?",
                (item.state["job_id"],),
            ).fetchone()
            connection.close()
            self.assertEqual(attempt[0], "uncertain")
            self.assertEqual(attempt[1], bvid)
            self.assertIn("处理 BVID 归属冲突", attempt[2])
            self.assertEqual(job[0], "upload_uncertain")
            self.assertIsNone(job[1])
            self.assertIn("禁止自动重投", job[2])
            self.assertEqual(item.state["phase"], "upload_uncertain")

    def test_existing_bvid_skips_duplicate_upload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            item, video = self.prepare_upload(Path(directory))
            bvid = "BV1uxE16ZE7e"
            connection = sqlite3.connect(item.args.database)
            connection.execute(
                """
                UPDATE jobs SET status='uploaded_original_pending_subtitle',bvid=?
                WHERE id=?
                """,
                (bvid, item.state["job_id"]),
            )
            connection.execute(
                """
                INSERT INTO upload_attempts(
                  id,job_id,status,bvid,started_at,finished_at
                ) VALUES('existing',?,'succeeded',?,'now','now')
                """,
                (item.state["job_id"], bvid),
            )
            connection.commit()
            connection.close()

            with mock.patch("live_once.subprocess.Popen") as popen:
                self.assertEqual(item.upload_video(video), bvid)
            popen.assert_not_called()
            item.download_cover.assert_not_called()
            connection = sqlite3.connect(item.args.database)
            status = connection.execute(
                "SELECT status FROM jobs WHERE id=?", (item.state["job_id"],)
            ).fetchone()[0]
            connection.close()
            self.assertEqual(status, "uploaded_original_pending_subtitle")

    def test_upload_hold_worker_resumes_until_handoff_is_committed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            args.hold_at = "2020-01-01T00:00:00Z"
            item = LiveOnce(args)
            item.save(bvid="BV1uxE16ZE7e")

            def hold() -> bool:
                return True

            item.ensure_upload_hold = hold  # type: ignore[method-assign]
            item.start_upload_hold_worker()
            self.assertIsNotNone(item.hold_thread)
            item.stop_upload_hold_worker()

            # 交接提交后，即使 state 里已有 BVID 也不再恢复续租。
            item.save(upload_recorded=True, upload_hold=False)
            resumed = LiveOnce(args)
            resumed.ensure_upload_hold = mock.Mock(return_value=True)
            resumed.start_upload_hold_worker()
            self.assertIsNone(resumed.hold_thread)
            resumed.ensure_upload_hold.assert_not_called()

    def test_execute_stops_hold_worker_only_after_handoff_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = arguments(root)
            args.hold_at = "2020-01-01T00:00:00Z"
            item = LiveOnce(args)
            item.acquire_lock = mock.MagicMock()
            item.validate_schema_version = mock.Mock()
            item.backup_database = mock.Mock()
            item.reserve_job = mock.Mock()
            item.fetch_metadata = mock.Mock()
            video = root / "video.mp4"
            bvid = "BV1uxE16ZE7e"
            item.capture_until_end = mock.Mock(return_value=video)
            item.upload_video = mock.Mock(return_value=bvid)
            item.wait_for_subtitle = mock.Mock()

            def hold() -> bool:
                return True

            item.ensure_upload_hold = hold  # type: ignore[method-assign]
            item.commit_upload_handoff = mock.Mock(
                side_effect=RuntimeError("handoff failed")
            )
            with self.assertRaisesRegex(RuntimeError, "handoff failed"):
                item.execute()
            self.assertIsNotNone(item.hold_thread)
            self.assertFalse(item.hold_stop.is_set())
            item.stop_upload_hold_worker()

            item.commit_upload_handoff = mock.Mock(return_value=None)
            item.execute()
            item.commit_upload_handoff.assert_called_once_with(video, bvid)
            self.assertTrue(item.hold_stop.is_set())
            item.wait_for_subtitle.assert_called_once_with(bvid)

    def _handoff_item(self, root: Path) -> tuple[LiveOnce, str, Path]:
        args = arguments(root)
        args.hold_at = "2020-01-01T00:00:00Z"
        make_database(Path(args.database))
        item = LiveOnce(args)
        item.reserve_job()
        self.assertTrue(item.ensure_upload_hold())
        bvid = "BV1uxE16ZE7e"
        connection = sqlite3.connect(args.database)
        connection.execute(
            "UPDATE jobs SET bvid=? WHERE video_id='example123'", (bvid,)
        )
        connection.commit()
        connection.close()
        item.save(
            bvid=bvid,
            video_probe={"duration": 10.0, "width": 1920, "height": 1080},
        )
        video = root / "video.mp4"
        video.write_bytes(b"video")
        return item, bvid, video

    def test_upload_handoff_persists_final_state_and_releases_hold_atomically(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            item, bvid, video = self._handoff_item(root)

            item.commit_upload_handoff(video, bvid)

            connection = sqlite3.connect(item.args.database)
            status, stored_bvid = connection.execute(
                "SELECT status,bvid FROM jobs WHERE video_id='example123'"
            ).fetchone()
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            hold_count = connection.execute(
                "SELECT COUNT(*) FROM maintenance_hold"
            ).fetchone()[0]
            actions = [
                row[0]
                for row in connection.execute(
                    "SELECT action FROM maintenance_hold_events ORDER BY id"
                )
            ]
            event_count = connection.execute(
                "SELECT COUNT(*) FROM events WHERE job_id=?", (item.state["job_id"],)
            ).fetchone()[0]
            connection.close()
            self.assertEqual(status, "uploaded_original_pending_subtitle")
            self.assertEqual(stored_bvid, bvid)
            self.assertIn(NEXT_SUBMIT_KEY, settings)
            self.assertEqual(hold_count, 0)
            self.assertEqual(actions, ["acquired", "released"])
            self.assertEqual(event_count, 2)
            self.assertTrue(item.state.get("upload_recorded"))
            self.assertFalse(item.state.get("upload_hold"))

    def test_upload_handoff_rolls_back_cooldown_and_hold_when_commit_fails(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            item, bvid, video = self._handoff_item(root)

            with mock.patch.object(
                item, "record_hold_event", side_effect=RuntimeError("boom")
            ):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    item.commit_upload_handoff(video, bvid)

            connection = sqlite3.connect(item.args.database)
            status, stored_bvid = connection.execute(
                "SELECT status,bvid FROM jobs WHERE video_id='example123'"
            ).fetchone()
            settings = dict(connection.execute("SELECT key,value FROM settings"))
            hold_count = connection.execute(
                "SELECT COUNT(*) FROM maintenance_hold"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(status, "paused")
            self.assertEqual(stored_bvid, bvid)
            self.assertNotIn(NEXT_SUBMIT_KEY, settings)
            self.assertEqual(hold_count, 1)
            self.assertFalse(item.state.get("upload_recorded"))


if __name__ == "__main__":
    unittest.main()
