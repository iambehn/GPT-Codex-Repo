from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    from utils.analytics import (
        build_dashboard_state,
        build_post_rows,
        compute_decision_rows,
        normalize_metric_import,
    )
    from pipeline.review import app as review_app
finally:
    os.chdir(_ORIGINAL_CWD)


class AnalyticsUtilityTests(unittest.TestCase):
    def test_build_post_rows_creates_one_post_per_platform(self) -> None:
        metadata = {
            "clip_id": "clip-1",
            "game": "marvel_rivals",
            "selected_template_id": "fast_hype",
            "title_engine": {"title": "Great title", "caption": "Great caption", "category": "performance_hype"},
            "decision": {"hook_gate_passed": True, "composite_score": 0.82, "hook_alignment": {"mode": "early"}},
            "context": {"player_entity": "hero_one", "detected_event": "multi_kill"},
            "meta_path": "/tmp/clip-1.meta.json",
        }
        distribution = {
            "tiktok": {"success": True, "publish_id": "tt-123"},
            "youtube_shorts": {"success": True, "url": "https://youtu.be/abc"},
            "reddit": {"success": False, "error": "not configured"},
        }

        rows = build_post_rows(metadata, distribution, now="2026-04-23T00:00:00+00:00")

        self.assertEqual(len(rows), 2)
        self.assertEqual({row["platform"] for row in rows}, {"tiktok", "youtube_shorts"})
        self.assertEqual(len({row["post_id"] for row in rows}), 2)
        self.assertTrue(all(row["clip_id"] == "clip-1" for row in rows))
        self.assertTrue(all(row["hook_gate_passed"] is True for row in rows))

    def test_metric_import_normalizes_csv_json_and_skips_duplicates(self) -> None:
        csv_payload = (
            "post_id,views,likes,comments,shares,saves,retention,follows,paid_spend\n"
            "post-a,1000,80,10,20,12,72%,15,0\n"
        )
        first = normalize_metric_import(
            csv_payload,
            "TikTok",
            imported_at="2026-04-23T00:00:00+00:00",
            import_batch_id="batch-1",
        )
        self.assertTrue(first["ok"])
        self.assertEqual(first["rows"][0]["platform"], "tiktok")
        self.assertEqual(first["rows"][0]["retention"], 0.72)
        self.assertEqual(first["rows"][0]["views"], 1000)

        duplicate = normalize_metric_import(
            csv_payload,
            "tiktok",
            existing_snapshot_ids={first["rows"][0]["snapshot_id"]},
            imported_at="2026-04-23T00:00:00+00:00",
            import_batch_id="batch-1",
        )
        self.assertEqual(duplicate["rows"], [])
        self.assertIn("duplicate", duplicate["warnings"][0])

        json_payload = json.dumps([{"post_id": "post-b", "views": 500, "retention": 0.55}])
        parsed_json = normalize_metric_import(json_payload, "youtube")
        self.assertTrue(parsed_json["ok"])
        self.assertEqual(parsed_json["rows"][0]["platform"], "youtube_shorts")

        unsupported = normalize_metric_import(json_payload, "myspace")
        self.assertFalse(unsupported["ok"])

    def test_url_only_metric_import_matches_distribution_post_id(self) -> None:
        metadata = {"clip_id": "clip-1", "game": "marvel_rivals"}
        url = "https://youtu.be/abc"
        post = build_post_rows(
            metadata,
            {"youtube_shorts": {"success": True, "url": url}},
            now="2026-04-23T00:00:00+00:00",
        )[0]

        imported = normalize_metric_import(
            f"url,views,retention\n{url},1000,70%\n",
            "youtube_shorts",
            imported_at="2026-04-23T00:00:00+00:00",
        )

        self.assertEqual(imported["rows"][0]["post_id"], post["post_id"])

    def test_decision_rules_do_not_mix_paid_and_organic_boost_logic(self) -> None:
        posts = [
            {"post_id": "organic", "posted_at": "2026-04-22T00:00:00+00:00"},
            {"post_id": "paid", "posted_at": "2026-04-22T00:00:00+00:00"},
            {"post_id": "weak", "posted_at": "2026-04-22T00:00:00+00:00"},
        ]
        metrics = [
            {"post_id": "organic", "snapshot_at": "2026-04-23T00:00:00+00:00", "views": 1000, "likes": 60, "shares": 20, "saves": 10, "retention": 0.72, "follows": 15},
            {"post_id": "paid", "snapshot_at": "2026-04-23T00:00:00+00:00", "views": 1000, "likes": 60, "shares": 20, "saves": 10, "retention": 0.72, "follows": 15, "paid_spend": 20},
            {"post_id": "weak", "snapshot_at": "2026-04-23T00:00:00+00:00", "views": 1000, "likes": 5, "shares": 0, "saves": 0, "retention": 0.20, "follows": 0},
        ]

        decisions = {row["post_id"]: row for row in compute_decision_rows(posts, metrics, {"analytics": {"decision_rules": {}}})}

        self.assertTrue(decisions["organic"]["boost_candidate"])
        self.assertFalse(decisions["paid"]["boost_candidate"])
        self.assertEqual(decisions["paid"]["paid_status"], "paid")
        self.assertTrue(decisions["weak"]["underperforming"])

    def test_dashboard_state_joins_posts_metrics_and_decisions(self) -> None:
        tables = {
            "configured": True,
            "error": None,
            "posts": [
                {"post_id": "post-a", "clip_id": "clip-a", "platform": "tiktok", "game": "marvel_rivals", "template_id": "fast_hype", "hook_type": "early"},
                {"post_id": "post-b", "clip_id": "clip-a", "platform": "reddit", "game": "marvel_rivals", "template_id": "fast_hype", "hook_type": "early"},
            ],
            "metrics": [
                {"post_id": "post-a", "snapshot_at": "2026-04-23T00:00:00+00:00", "views": 1000, "retention": 0.7},
                {"post_id": "post-b", "snapshot_at": "2026-04-23T00:00:00+00:00", "views": 200, "retention": 0.5},
            ],
            "decisions": [
                {"post_id": "post-a", "computed_at": "2026-04-23T00:00:00+00:00", "boost_candidate": "TRUE", "score": 0.9},
            ],
        }
        with patch("utils.analytics.read_analytics_tables", return_value=tables):
            state = build_dashboard_state({"analytics": {"enabled": True}}, {"platform": "tiktok"})

        self.assertEqual(state["overview"]["posts_tracked"], 1)
        self.assertEqual(state["posts"][0]["post_id"], "post-a")
        self.assertEqual(len(state["boost_candidates"]), 1)


class AnalyticsRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        review_app.CONFIG = {"analytics": {"enabled": True}, "paths": {"inbox": "inbox", "processing": "processing", "quarantine": "quarantine"}}
        review_app.app.config["TESTING"] = True
        self.client = review_app.app.test_client()

    def _state(self) -> dict:
        return {
            "configured": True,
            "error": None,
            "filters": {"platform": "", "game": "", "template_id": "", "hook_type": "", "paid": ""},
            "overview": {"posts_tracked": 1, "total_views": 1000, "avg_retention": 0.7, "follower_conversion": 0.01, "top_template": "fast_hype"},
            "posts": [],
            "top_performers": [
                {
                    "post_id": "post-a",
                    "clip_id": "clip-a",
                    "title": "Title",
                    "platform": "tiktok",
                    "game": "marvel_rivals",
                    "metrics": {"views": 1000, "retention": 0.7},
                    "decision": {"boost_candidate": True, "score": 0.9},
                    "score": 0.9,
                }
            ],
            "boost_candidates": [],
            "recycle_candidates": [],
            "underperformers": [],
            "platforms": ["tiktok"],
            "games": ["marvel_rivals"],
            "templates": ["fast_hype"],
            "hook_types": ["early"],
        }

    def test_analytics_dashboard_and_detail_routes_load(self) -> None:
        detail = {
            "configured": True,
            "error": None,
            "post": {"post_id": "post-a", "clip_id": "clip-a", "title": "Title", "platform": "tiktok", "game": "marvel_rivals"},
            "metrics": [{"snapshot_at": "now", "views": 1000, "likes": 1, "shares": 1, "saves": 1, "retention": 0.7, "follows": 1, "paid_spend": 0}],
            "decisions": [{"computed_at": "now", "boost_candidate": True, "recycle_candidate": False, "underperforming": False, "score": 0.9, "decision_reason": "good"}],
        }
        with patch.object(review_app, "build_dashboard_state", return_value=self._state()):
            self.assertEqual(self.client.get("/analytics").status_code, 200)
        with patch.object(review_app, "build_post_detail", return_value=detail):
            self.assertEqual(self.client.get("/analytics/post/post-a").status_code, 200)
        with patch.object(review_app, "build_post_detail", return_value=None):
            self.assertEqual(self.client.get("/analytics/post/missing").status_code, 404)

    def test_analytics_import_api(self) -> None:
        result = {"ok": True, "imported": 1, "decisions": 1, "errors": [], "warnings": []}
        with patch.object(review_app, "import_metric_payload", return_value=result) as mocked:
            response = self.client.post(
                "/api/analytics/import",
                json={"source_platform": "tiktok", "payload": [{"post_id": "post-a", "views": 1000}]},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["imported"], 1)
        mocked.assert_called_once()


if __name__ == "__main__":
    unittest.main()
