"""Tests for the Agent Mode JSON API routes added to pipeline/review/app.py.

Covers all 8 endpoints:
  GET  /api/queue
  GET  /api/stats
  GET  /api/clip/<game>/<stem>/inspect
  GET  /api/clip/<game>/<stem>/signals
  POST /api/clip/<game>/<stem>/approve
  POST /api/clip/<game>/<stem>/reject
  POST /api/clip/<game>/<stem>/rescore
  GET  /api/quarantine
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    from pipeline.review import app as review_app
finally:
    os.chdir(_ORIGINAL_CWD)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_RICH_META = {
    "clip_id": "clip_a",
    "game": "test_game",
    "duration_seconds": 15.0,
    "motion_level": "high",
    "audio_energy": "high",
    "keywords": ["headshot", "clutch"],
    "quality_tag": "hd",
    "selected_template_id": "fast_hype",
    "scoring": {
        "highlight_score": 87,
        "clip_type": "clutch_play",
        "suggested_title": "Insane clutch",
        "suggested_caption": "#gaming",
        "score_reasoning": "Strong hook.",
    },
    "decision": {
        "status": "accept",
        "composite_score": 0.81,
        "hook_gate_passed": True,
    },
    "audio_events": {
        "events": [{"type": "multi_kill", "timestamp": 2.1, "spike_count": 3}],
    },
    "kill_feed": {
        "events": [
            {"kind": "kill", "timestamp": 3.0, "confidence": 0.9, "method": "color_mask"},
            {"kind": "headshot", "timestamp": 3.5, "confidence": 0.85, "method": "color_mask"},
        ],
    },
    "weapon_detection": {
        "weapon_id": "hero_one",
        "display_name": "Hero One",
        "confidence": 0.93,
        "frame_time": 4.2,
    },
    "niceshot_detection": {
        "moments": [{"timestamp": 3.1, "kind": "headshot", "confidence": 0.88, "hook_candidate": True}],
    },
    "yolo_detection": {
        "event_candidates": [{"timestamp": 3.0, "event_id": "team_wipe", "confidence": 0.75}],
        "detections": [],
    },
    "hook_enforcer": {
        "early_hook_passed": True,
        "hook_score": 0.82,
        "window_seconds": 1.5,
        "anchor_moment": {"timestamp": 3.1, "kind": "headshot", "confidence": 0.88},
        "trim_plan": {"strategy": "none"},
    },
}


class AgentApiTests(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"

        self.processing_dir = self.root / "processing" / self.game
        self.inbox_dir = self.root / "inbox" / self.game
        self.accepted_dir = self.root / "accepted" / self.game
        self.rejected_dir = self.root / "rejected" / self.game
        self.quarantine_dir = self.root / "quarantine" / self.game
        self.assets_dir = self.root / "assets"
        self.game_pack_dir = self.assets_dir / "games" / self.game

        for path in (
            self.processing_dir,
            self.inbox_dir,
            self.accepted_dir,
            self.rejected_dir,
            self.quarantine_dir,
            self.game_pack_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        self.config = {
            "paths": {
                "assets": str(self.assets_dir),
                "inbox": str(self.root / "inbox"),
                "processing": str(self.root / "processing"),
                "accepted": str(self.root / "accepted"),
                "rejected": str(self.root / "rejected"),
                "quarantine": str(self.root / "quarantine"),
                "templates": str(self.root / "templates"),
                "logs": str(self.root / "logs"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "weapon_detector": {
                "enabled": True,
                "confidence_threshold": 0.8,
                "frame_sample": "middle",
                "icon_dir": str(self.assets_dir / "weapon_icons"),
            },
            "kill_feed": {"enabled": False, "games": {}},
            "scoring": {"model": "claude-haiku-4-5-20251001", "max_tokens": 512},
        }

        self._write_game_pack()

        review_app.PROJECT_ROOT = self.root
        review_app.CONFIG = self.config
        review_app.app.config["TESTING"] = True
        self.client = review_app.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # ── fixtures ──────────────────────────────────────────────────────────────

    def _write_game_pack(self) -> None:
        files = {
            "game.yaml": {
                "game_id": self.game,
                "display_name": "Test Game",
                "genre": "hero_shooter",
                "ui_version": "test",
                "detectors": {"weapon_detector": {"enabled": True}},
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {"hero_one": {"display_name": "Hero One"}},
                "aliases": {},
            },
            "hud.yaml": {
                "ui_version": "test",
                "rois": {"weapon_detector": {"x": 0, "y": 960, "w": 160, "h": 100}},
                "detectors": {"weapon_detector": {"roi_ref": "weapon_detector"}},
            },
            "moments.yaml": {"moments": {}, "hook_targets": {"window_seconds": 1.5}},
            "weights.yaml": {
                "clip_judge": {
                    "thresholds": {"accept": 0.7, "quarantine": 0.45, "reject": 0.25},
                    "quarantine_reasons": ["missing_context"],
                }
            },
        }
        for filename, payload in files.items():
            (self.game_pack_dir / filename).write_text(yaml.safe_dump(payload))

    def _make_pending_clip(
        self,
        stem: str = "clip_a",
        score: int = 87,
        meta_override: dict | None = None,
    ) -> tuple[Path, Path]:
        """Create a processing clip + inbox meta and return (clip_path, meta_path)."""
        clip_path = self.processing_dir / f"{stem}.mp4"
        clip_path.write_bytes(b"fake-video")

        meta = {**_RICH_META, "clip_id": stem, "processed_path": str(clip_path)}
        meta["scoring"] = {**meta.get("scoring", {}), "highlight_score": score}
        if meta_override:
            meta.update(meta_override)

        meta_path = self.inbox_dir / f"{stem}.meta.json"
        meta_path.write_text(json.dumps(meta, indent=2))
        return clip_path, meta_path

    def _make_quarantine_clip(self, stem: str = "q_clip") -> tuple[Path, Path]:
        """Create a quarantine clip + meta sidecar."""
        clip_path = self.quarantine_dir / f"{stem}.mp4"
        clip_path.write_bytes(b"quarantine-video")
        meta_path = clip_path.with_suffix(".meta.json")
        meta_path.write_text(json.dumps({
            "clip_id": stem,
            "game": self.game,
            "clip_path": str(clip_path),
            "status": "quarantine",
            "quarantine": {"reason": "low_confidence"},
            "decision": {"status": "quarantine", "composite_score": 0.3},
        }, indent=2))
        return clip_path, meta_path

    # ── /api/queue ────────────────────────────────────────────────────────────

    def test_queue_empty(self) -> None:
        resp = self.client.get("/api/queue")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["total"], 0)
        self.assertEqual(body["clips"], [])

    def test_queue_returns_pending_clips(self) -> None:
        self._make_pending_clip("clip_a", score=87)
        self._make_pending_clip("clip_b", score=55)

        resp = self.client.get("/api/queue")
        body = resp.get_json()
        self.assertEqual(body["total"], 2)
        scores = [c["score"] for c in body["clips"]]
        self.assertEqual(scores, sorted(scores, reverse=True), "clips must be sorted by score desc")

    def test_queue_excludes_reviewed_clips(self) -> None:
        self._make_pending_clip("clip_a")
        self._make_pending_clip("clip_b", meta_override={"review_status": "accepted"})

        body = self.client.get("/api/queue").get_json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["clips"][0]["stem"], "clip_a")

    def test_queue_clips_omit_raw_meta_blob(self) -> None:
        self._make_pending_clip("clip_a")
        body = self.client.get("/api/queue").get_json()
        self.assertNotIn("meta", body["clips"][0])

    def test_queue_filter_by_game(self) -> None:
        self._make_pending_clip("clip_a")
        resp_match = self.client.get(f"/api/queue?game={self.game}")
        resp_no_match = self.client.get("/api/queue?game=other_game")
        self.assertEqual(resp_match.get_json()["total"], 1)
        self.assertEqual(resp_no_match.get_json()["total"], 0)

    def test_queue_filter_by_min_score(self) -> None:
        self._make_pending_clip("clip_a", score=90)
        self._make_pending_clip("clip_b", score=40)

        body_high = self.client.get("/api/queue?min_score=80").get_json()
        body_all = self.client.get("/api/queue?min_score=0").get_json()
        self.assertEqual(body_high["total"], 1)
        self.assertEqual(body_all["total"], 2)

    # ── /api/stats ────────────────────────────────────────────────────────────

    def test_stats_schema(self) -> None:
        resp = self.client.get("/api/stats")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        for key in ("pending", "accepted", "rejected", "quarantined"):
            self.assertIn(key, body["totals"])
        self.assertIn(self.game, body["by_game"])

    def test_stats_counts_pending_clips(self) -> None:
        self._make_pending_clip("clip_a")
        self._make_pending_clip("clip_b")
        body = self.client.get("/api/stats").get_json()
        self.assertEqual(body["totals"]["pending"], 2)
        self.assertEqual(body["by_game"][self.game]["pending"], 2)

    def test_stats_counts_accepted_clips(self) -> None:
        (self.accepted_dir / "finished.mp4").write_bytes(b"done")
        body = self.client.get("/api/stats").get_json()
        self.assertEqual(body["totals"]["accepted"], 1)
        self.assertEqual(body["by_game"][self.game]["accepted"], 1)

    def test_stats_counts_quarantined_clips(self) -> None:
        self._make_quarantine_clip("q1")
        body = self.client.get("/api/stats").get_json()
        self.assertEqual(body["totals"]["quarantined"], 1)

    def test_stats_all_zeroes_when_empty(self) -> None:
        body = self.client.get("/api/stats").get_json()
        self.assertEqual(body["totals"]["pending"], 0)
        self.assertEqual(body["totals"]["accepted"], 0)
        self.assertEqual(body["totals"]["rejected"], 0)
        self.assertEqual(body["totals"]["quarantined"], 0)

    # ── /api/clip/<game>/<stem>/inspect ───────────────────────────────────────

    def test_inspect_returns_meta(self) -> None:
        self._make_pending_clip("clip_a")
        resp = self.client.get(f"/api/clip/{self.game}/clip_a/inspect")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["clip_id"], "clip_a")
        self.assertEqual(body["game"], self.game)
        self.assertIn("scoring", body["meta"])

    def test_inspect_404_for_unknown_stem(self) -> None:
        resp = self.client.get(f"/api/clip/{self.game}/nonexistent/inspect")
        self.assertEqual(resp.status_code, 404)
        self.assertFalse(resp.get_json()["ok"])

    def test_inspect_prefers_inbox_meta(self) -> None:
        clip_path, meta_path = self._make_pending_clip("clip_a")
        # Also plant a processing-side meta with different clip_type to confirm inbox wins
        processing_meta = clip_path.with_suffix(".meta.json")
        processing_meta.write_text(json.dumps({"clip_id": "clip_a", "source": "processing_side"}))

        body = self.client.get(f"/api/clip/{self.game}/clip_a/inspect").get_json()
        self.assertIn("scoring", body["meta"], "inbox meta (with scoring) should be returned")

    # ── /api/clip/<game>/<stem>/signals ───────────────────────────────────────

    def test_signals_returns_timeline(self) -> None:
        self._make_pending_clip("clip_a")
        resp = self.client.get(f"/api/clip/{self.game}/clip_a/signals")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["clip_id"], "clip_a")
        self.assertIsInstance(body["signals"], list)

    def test_signals_contain_expected_keys(self) -> None:
        self._make_pending_clip("clip_a")
        body = self.client.get(f"/api/clip/{self.game}/clip_a/signals").get_json()
        for sig in body["signals"]:
            for key in ("timestamp", "source", "kind", "label"):
                self.assertIn(key, sig, f"signal missing key '{key}': {sig}")

    def test_signals_are_sorted_by_timestamp(self) -> None:
        self._make_pending_clip("clip_a")
        body = self.client.get(f"/api/clip/{self.game}/clip_a/signals").get_json()
        timestamps = [s["timestamp"] for s in body["signals"]]
        self.assertEqual(timestamps, sorted(timestamps))

    def test_signals_include_all_detector_sources(self) -> None:
        self._make_pending_clip("clip_a")
        body = self.client.get(f"/api/clip/{self.game}/clip_a/signals").get_json()
        sources = {s["source"] for s in body["signals"]}
        self.assertIn("audio_detector", sources)
        self.assertIn("kill_feed", sources)
        self.assertIn("weapon_detector", sources)
        self.assertIn("niceshot", sources)
        self.assertIn("yolo_detector", sources)
        self.assertIn("hook_enforcer", sources)

    def test_signals_404_for_unknown_stem(self) -> None:
        resp = self.client.get(f"/api/clip/{self.game}/ghost/signals")
        self.assertEqual(resp.status_code, 404)

    # ── /api/clip/<game>/<stem>/approve ───────────────────────────────────────

    def test_approve_moves_clip_to_accepted(self) -> None:
        clip_path, meta_path = self._make_pending_clip("clip_a")
        resp = self.client.post(f"/api/clip/{self.game}/clip_a/approve")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["review_status"], "accepted")
        self.assertFalse(clip_path.exists(), "source file should be moved out of processing/")
        self.assertTrue((self.accepted_dir / "clip_a.mp4").exists())

    def test_approve_stamps_meta(self) -> None:
        _, meta_path = self._make_pending_clip("clip_a")
        self.client.post(f"/api/clip/{self.game}/clip_a/approve")
        meta = json.loads(meta_path.read_text())
        self.assertEqual(meta["review_status"], "accepted")
        self.assertIn("reviewed_at", meta)
        self.assertIn("final_path", meta)

    def test_approve_404_for_unknown_stem(self) -> None:
        resp = self.client.post(f"/api/clip/{self.game}/ghost/approve")
        self.assertEqual(resp.status_code, 404)

    def test_approve_does_not_affect_other_clips(self) -> None:
        clip_a, _ = self._make_pending_clip("clip_a")
        clip_b, _ = self._make_pending_clip("clip_b")
        self.client.post(f"/api/clip/{self.game}/clip_a/approve")
        self.assertTrue(clip_b.exists(), "clip_b must be untouched")

    # ── /api/clip/<game>/<stem>/reject ────────────────────────────────────────

    def test_reject_moves_clip_to_rejected(self) -> None:
        clip_path, _ = self._make_pending_clip("clip_a")
        resp = self.client.post(f"/api/clip/{self.game}/clip_a/reject")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertEqual(body["review_status"], "rejected")
        self.assertFalse(clip_path.exists())
        self.assertTrue((self.rejected_dir / "clip_a.mp4").exists())

    def test_reject_stores_reason_in_meta(self) -> None:
        _, meta_path = self._make_pending_clip("clip_a")
        self.client.post(
            f"/api/clip/{self.game}/clip_a/reject",
            json={"reason": "too short"},
        )
        meta = json.loads(meta_path.read_text())
        self.assertEqual(meta["review_reason"], "too short")

    def test_reject_without_reason_does_not_write_review_reason(self) -> None:
        _, meta_path = self._make_pending_clip("clip_a")
        self.client.post(f"/api/clip/{self.game}/clip_a/reject", json={})
        meta = json.loads(meta_path.read_text())
        self.assertNotIn("review_reason", meta)

    def test_reject_404_for_unknown_stem(self) -> None:
        resp = self.client.post(f"/api/clip/{self.game}/ghost/reject")
        self.assertEqual(resp.status_code, 404)

    # ── /api/clip/<game>/<stem>/rescore ───────────────────────────────────────

    def test_rescore_clears_stale_scoring_and_returns_new_result(self) -> None:
        _, meta_path = self._make_pending_clip("clip_a")

        fresh_score = {
            "highlight_score": 72,
            "clip_type": "kill_streak",
            "suggested_title": "Fresh title",
            "suggested_caption": "#new",
            "score_reasoning": "Recomputed.",
        }
        with patch("pipeline.scoring.run_scoring", return_value=fresh_score) as mock_score:
            resp = self.client.post(f"/api/clip/{self.game}/clip_a/rescore")

        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["scoring"]["highlight_score"], 72)
        mock_score.assert_called_once()

    def test_rescore_clears_existing_scoring_from_meta_before_calling(self) -> None:
        _, meta_path = self._make_pending_clip("clip_a")
        original_meta = json.loads(meta_path.read_text())
        self.assertIn("scoring", original_meta)

        captured_meta: list[dict] = []

        def _capture_run_scoring(clip_path, meta, config):
            captured_meta.append(dict(meta))
            return {"highlight_score": 50, "clip_type": "other",
                    "suggested_title": "", "suggested_caption": "", "score_reasoning": ""}

        with patch("pipeline.scoring.run_scoring", side_effect=_capture_run_scoring):
            self.client.post(f"/api/clip/{self.game}/clip_a/rescore")

        self.assertNotIn("scoring", captured_meta[0], "scoring must be cleared before run_scoring is called")

    def test_rescore_404_for_unknown_stem(self) -> None:
        resp = self.client.post(f"/api/clip/{self.game}/ghost/rescore")
        self.assertEqual(resp.status_code, 404)

    # ── /api/quarantine ───────────────────────────────────────────────────────

    def test_quarantine_empty(self) -> None:
        resp = self.client.get("/api/quarantine")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["total"], 0)
        self.assertEqual(body["clips"], [])

    def test_quarantine_returns_quarantined_clips(self) -> None:
        self._make_quarantine_clip("q_clip")
        body = self.client.get("/api/quarantine").get_json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["clips"][0]["clip_id"], "q_clip")

    def test_quarantine_clips_omit_filesystem_path(self) -> None:
        self._make_quarantine_clip("q_clip")
        body = self.client.get("/api/quarantine").get_json()
        self.assertNotIn("clip_path", body["clips"][0])

    def test_quarantine_filter_by_game(self) -> None:
        self._make_quarantine_clip("q_clip")
        resp_match = self.client.get(f"/api/quarantine?game={self.game}")
        resp_no_match = self.client.get("/api/quarantine?game=other_game")
        self.assertEqual(resp_match.get_json()["total"], 1)
        self.assertEqual(resp_no_match.get_json()["total"], 0)

    def test_quarantine_includes_reason(self) -> None:
        self._make_quarantine_clip("q_clip")
        body = self.client.get("/api/quarantine").get_json()
        self.assertEqual(body["clips"][0]["reason"], "low_confidence")

    def test_quarantine_does_not_include_pending_clips(self) -> None:
        self._make_pending_clip("clip_a")
        self._make_quarantine_clip("q_clip")
        body = self.client.get("/api/quarantine").get_json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["clips"][0]["clip_id"], "q_clip")


if __name__ == "__main__":
    unittest.main()
