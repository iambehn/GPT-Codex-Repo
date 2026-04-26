from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    from pipeline.distribution_queue import (
        classify_distribution_error,
        distribution_status,
        mark_manual_posted,
        run_distribution_queue,
        schedule_distribution_tasks,
    )
    from pipeline.review import app as review_app
finally:
    os.chdir(_ORIGINAL_CWD)


class DistributionQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = {
            "games": {"marvel_rivals": {"display_name": "Marvel Rivals"}},
            "paths": {
                "accepted": str(self.root / "accepted"),
                "inbox": str(self.root / "inbox"),
                "templates": str(self.root / "templates"),
            },
            "distribution": {
                "queue_db_path": str(self.root / "data" / "distribution.sqlite3"),
                "manual_pack_dir": str(self.root / "manual_packs"),
                "schedule": {
                    "default_daily_cap": 3,
                    "default_min_spacing_minutes": 60,
                    "jitter_minutes": 0,
                    "retry_base_minutes": 5,
                    "retry_jitter_minutes": 0,
                    "retry_max_attempts": 3,
                    "circuit_breaker_failure_threshold": 3,
                    "circuit_breaker_window_minutes": 60,
                },
                "platforms": {
                    "youtube_shorts": {"enabled": True},
                    "tiktok": {"enabled": True},
                    "reddit": {"enabled": True, "subreddits": {"marvel_rivals": "MarvelRivals"}},
                },
                "accounts": [
                    {
                        "account_id": "yt-main",
                        "platform": "youtube_shorts",
                        "channel": "main",
                        "enabled": True,
                        "policy_mode": "official_api",
                        "require_credentials": False,
                        "daily_cap": 3,
                        "min_spacing_minutes": 60,
                    },
                    {
                        "account_id": "tt-main",
                        "platform": "tiktok",
                        "channel": "main",
                        "enabled": True,
                        "policy_mode": "human_assisted",
                        "daily_cap": 3,
                        "min_spacing_minutes": 60,
                    },
                ],
            },
            "analytics": {"enabled": False},
        }
        self.now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
        self._write_clip("clip-1", "Great title")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_clip(self, clip_id: str, title: str) -> Path:
        accepted = self.root / "accepted" / "marvel_rivals"
        inbox = self.root / "inbox" / "marvel_rivals"
        accepted.mkdir(parents=True, exist_ok=True)
        inbox.mkdir(parents=True, exist_ok=True)
        clip = accepted / f"marvel_rivals_20260423_{clip_id}.mp4"
        clip.write_bytes(b"fake video")
        meta = {
            "clip_id": clip_id,
            "game": "marvel_rivals",
            "review_status": "accepted",
            "final_path": str(clip),
            "title_engine": {"title": title, "caption": "Caption", "hashtags": ["#MarvelRivals"]},
            "decision": {"hook_gate_passed": True, "composite_score": 0.8},
            "context": {"player_entity": "hero_one", "detected_event": "multi_kill"},
        }
        (inbox / f"{clip_id}.meta.json").write_text(json.dumps(meta))
        return clip

    def _rows(self, table: str) -> list[dict]:
        conn = sqlite3.connect(self.config["distribution"]["queue_db_path"])
        conn.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in conn.execute(f"SELECT * FROM {table}")]
        finally:
            conn.close()

    def test_schedule_creates_api_and_manual_tasks_with_pack(self) -> None:
        result = schedule_distribution_tasks(self.config, now=self.now)

        self.assertTrue(result["ok"])
        self.assertEqual(result["created"], 2)
        self.assertEqual(result["manual"], 1)
        tasks = self._rows("distribution_tasks")
        self.assertEqual({task["platform"] for task in tasks}, {"youtube_shorts", "tiktok"})
        self.assertEqual({task["status"] for task in tasks}, {"ready", "needs_human_publish"})
        manual_task = next(task for task in tasks if task["status"] == "needs_human_publish")
        self.assertTrue(Path(manual_task["manual_pack_path"]).exists())
        pack = json.loads(Path(manual_task["manual_pack_path"]).read_text())
        self.assertEqual(pack["title"], "Great title")
        self.assertIn("checklist", pack)

    def test_schedule_is_idempotent_and_enforces_daily_cap(self) -> None:
        self.config["distribution"]["accounts"][0]["daily_cap"] = 1
        first = schedule_distribution_tasks(self.config, now=self.now)
        self._write_clip("clip-2", "Another title")
        second = schedule_distribution_tasks(self.config, now=self.now)

        self.assertEqual(first["created"], 2)
        self.assertEqual(second["created"], 2)
        yt_tasks = [task for task in self._rows("distribution_tasks") if task["platform"] == "youtube_shorts"]
        self.assertEqual(len(yt_tasks), 2)
        scheduled_days = {task["scheduled_at"][:10] for task in yt_tasks}
        self.assertEqual(len(scheduled_days), 2)

    def test_missing_credentials_pause_official_api_task(self) -> None:
        self.config["distribution"]["accounts"][0]["require_credentials"] = True
        result = schedule_distribution_tasks(self.config, now=self.now)

        self.assertEqual(result["paused"], 1)
        yt_task = next(task for task in self._rows("distribution_tasks") if task["platform"] == "youtube_shorts")
        self.assertEqual(yt_task["status"], "paused_compliance")
        self.assertEqual(yt_task["compliance_reason"], "missing_credentials")

    def test_runner_posts_due_task_and_records_attempt(self) -> None:
        schedule_distribution_tasks(self.config, now=self.now)
        with patch(
            "pipeline.distribution_queue.upload_to_platform",
            return_value={"success": True, "url": "https://youtu.be/abc"},
        ) as mocked:
            result = run_distribution_queue(self.config, now=self.now)

        self.assertEqual(result["posted"], 1)
        mocked.assert_called_once()
        task = next(task for task in self._rows("distribution_tasks") if task["platform"] == "youtube_shorts")
        self.assertEqual(task["status"], "posted")
        self.assertEqual(task["published_url"], "https://youtu.be/abc")
        self.assertEqual(len(self._rows("distribution_attempts")), 1)
        meta = json.loads((self.root / "inbox" / "marvel_rivals" / "clip-1.meta.json").read_text())
        self.assertEqual(meta["distribution"]["youtube_shorts"]["url"], "https://youtu.be/abc")

    def test_retryable_error_is_requeued(self) -> None:
        schedule_distribution_tasks(self.config, now=self.now)
        with patch(
            "pipeline.distribution_queue.upload_to_platform",
            return_value={"success": False, "error": "rate limit exceeded"},
        ):
            result = run_distribution_queue(self.config, now=self.now)

        self.assertEqual(result["retryable"], 1)
        task = next(task for task in self._rows("distribution_tasks") if task["platform"] == "youtube_shorts")
        self.assertEqual(task["status"], "failed_retryable")
        self.assertEqual(task["last_error_class"], "rate_limit")
        self.assertTrue(task["next_attempt_at"])

    def test_manual_mark_posted_updates_task_and_meta(self) -> None:
        schedule_distribution_tasks(self.config, now=self.now)
        manual_task = next(task for task in self._rows("distribution_tasks") if task["platform"] == "tiktok")
        result = mark_manual_posted(manual_task["task_id"], "https://tiktok.com/@acct/video/1", self.config, now=self.now)

        self.assertTrue(result["ok"])
        updated = next(task for task in self._rows("distribution_tasks") if task["task_id"] == manual_task["task_id"])
        self.assertEqual(updated["status"], "posted")
        meta = json.loads((self.root / "inbox" / "marvel_rivals" / "clip-1.meta.json").read_text())
        self.assertEqual(meta["distribution"]["tiktok"]["url"], "https://tiktok.com/@acct/video/1")

    def test_error_classifier(self) -> None:
        self.assertEqual(classify_distribution_error({"success": False, "error": "token expired"}), "auth_error")
        self.assertEqual(classify_distribution_error({"success": False, "error": "rate limit"}), "rate_limit")
        self.assertEqual(classify_distribution_error({"success": False, "error": "network timeout"}), "transient_network")
        self.assertEqual(classify_distribution_error({"success": False, "error": "policy rejected"}), "content_rejected")
        self.assertEqual(classify_distribution_error({"success": False, "error": "invalid media codec"}), "invalid_media")

    def test_status_summary(self) -> None:
        schedule_distribution_tasks(self.config, now=self.now)
        status = distribution_status(self.config)

        self.assertTrue(status["ok"])
        self.assertEqual(status["counts"]["ready"], 1)
        self.assertEqual(status["counts"]["needs_human_publish"], 1)


class DistributionRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        review_app.CONFIG = {"paths": {"inbox": "inbox", "processing": "processing", "quarantine": "quarantine"}}
        review_app.app.config["TESTING"] = True
        self.client = review_app.app.test_client()

    def test_distribution_dashboard_loads(self) -> None:
        state = {
            "configured": True,
            "db_path": "/tmp/distribution.sqlite3",
            "filters": {"status": "", "platform": "", "account_id": "", "game": ""},
            "tasks": [],
            "counts": {},
            "attempts": [],
            "states": ["ready", "posted"],
            "platforms": ["tiktok"],
            "accounts": ["tt-main"],
        }
        with patch.object(review_app, "get_distribution_dashboard", return_value=state):
            self.assertEqual(self.client.get("/distribution").status_code, 200)


if __name__ == "__main__":
    unittest.main()
