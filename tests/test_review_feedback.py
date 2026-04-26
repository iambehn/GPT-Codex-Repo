from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    from pipeline.review import app as review_app
    from pipeline.review_feedback import apply_feedback_updates, load_feedback_entries
finally:
    os.chdir(_ORIGINAL_CWD)


class ReviewFeedbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"

        self.assets_dir = self.root / "assets"
        self.game_pack_dir = self.assets_dir / "games" / self.game
        self.inbox_dir = self.root / "inbox" / self.game
        self.processing_dir = self.root / "processing" / self.game
        self.quarantine_dir = self.root / "quarantine" / self.game / "missing_context"
        for path in (self.game_pack_dir, self.inbox_dir, self.processing_dir, self.quarantine_dir):
            path.mkdir(parents=True, exist_ok=True)

        self.queue_clip = self.processing_dir / "queue_clip.mp4"
        self.queue_clip.write_bytes(b"queue")
        self.queue_meta = self.inbox_dir / "queue_clip.meta.json"
        self.queue_meta.write_text(json.dumps({
            "clip_id": "queue_clip",
            "game": self.game,
            "processed_path": str(self.queue_clip),
            "clip_path": str(self.inbox_dir / "queue_clip.mp4"),
            "decision": {"status": "accept"},
            "context": {"player_entity": "hero_one", "detected_event": "precision_pick"},
            "niceshot_detection": {"profile": "hero_shooter_default"},
            "yolo_detection": {"top_entity": {"entity_id": "hero_one"}},
        }))

        self.quarantine_clip = self.quarantine_dir / "quarantine_clip.mp4"
        self.quarantine_clip.write_bytes(b"quarantine")
        self.quarantine_meta = self.quarantine_dir / "quarantine_clip.meta.json"
        self.quarantine_meta.write_text(json.dumps({
            "clip_id": "quarantine_clip",
            "game": self.game,
            "clip_path": str(self.quarantine_clip),
            "decision": {"status": "quarantine"},
            "quarantine": {"reason": "missing_context"},
            "context": {"player_entity": None, "detected_event": "precision_pick"},
            "yolo_detection": {"top_entity": {"entity_id": None}},
        }))

        self._write_game_pack()
        self.config = {
            "paths": {
                "assets": str(self.assets_dir),
                "inbox": str(self.root / "inbox"),
                "processing": str(self.root / "processing"),
                "quarantine": str(self.root / "quarantine"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "weapon_detector": {"enabled": True},
            "scout": {"thresholds": {}},
        }

        review_app.PROJECT_ROOT = self.root
        review_app.CONFIG = self.config
        review_app.app.config["TESTING"] = True
        self.client = review_app.app.test_client()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_game_pack(self) -> None:
        files = {
            "game.yaml": {
                "game_id": self.game,
                "display_name": "Test Game",
                "genre": "hero_shooter",
                "ui_version": "test",
                "detectors": {
                    "weapon_detector": {"enabled": True},
                    "niceshot": {"enabled": True, "profile": "hero_shooter_default"},
                },
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {"hero_one": {"display_name": "Hero One"}},
                "aliases": {},
            },
            "moments.yaml": {"moments": [{"id": "precision_pick", "labels": ["headshot"]}]},
            "hud.yaml": {
                "ui_version": "test",
                "rois": {},
                "detectors": {
                    "weapon_detector": {"icon_dir": str(self.assets_dir / "weapon_icons" / self.game)},
                    "yolo": {"labels": {}},
                },
            },
            "weights.yaml": {
                "clip_judge": {
                    "thresholds": {"accept": 0.7, "quarantine": 0.45, "reject": 0.25},
                    "composite_weights": {
                        "ai_clip_score": 0.35,
                        "ai_hook_score": 0.25,
                        "kill_feed_score": 0.2,
                        "audio_score": 0.1,
                        "context_score": 0.1,
                    },
                    "quarantine_reasons": ["missing_context", "needs_roi_template", "hook_not_resolved"],
                    "feedback": {
                        "enabled": True,
                        "threshold_step": 0.01,
                        "weight_step": 0.05,
                        "retrain_threshold": 2,
                    },
                }
            },
        }
        for filename, payload in files.items():
            (self.game_pack_dir / filename).write_text(yaml.safe_dump(payload, sort_keys=False))

    def test_feedback_route_records_queue_and_quarantine_events(self) -> None:
        self.assertEqual(self.client.get("/feedback").status_code, 200)

        queue_response = self.client.post("/api/feedback/record", json={
            "game": self.game,
            "clip_stem": "queue_clip",
            "source_stage": "queue",
            "feedback_type": "false_positive",
            "detector": "clip_judge",
            "note": "This was scored too generously.",
        })
        self.assertEqual(queue_response.status_code, 200)
        self.assertEqual(queue_response.get_json()["feedback"]["feedback_type"], "false_positive")

        quarantine_response = self.client.post("/api/feedback/record", json={
            "game": self.game,
            "clip_stem": "quarantine_clip",
            "source_stage": "quarantine",
            "feedback_type": "false_negative",
            "detector": "yolo_detector",
            "note": "This should have passed and needs better visual context.",
        })
        self.assertEqual(quarantine_response.status_code, 200)
        self.assertEqual(quarantine_response.get_json()["summary"]["counts"]["false_negative"], 1)

        entries = load_feedback_entries(self.game, self.config)
        self.assertEqual(len(entries), 2)

        queue_meta = json.loads(self.queue_meta.read_text())
        quarantine_meta = json.loads(self.quarantine_meta.read_text())
        self.assertEqual(queue_meta["review_feedback"][-1]["feedback_type"], "false_positive")
        self.assertEqual(quarantine_meta["review_feedback"][-1]["feedback_type"], "false_negative")

    def test_apply_feedback_updates_changes_weights_and_sets_retrain_pressure(self) -> None:
        for payload in (
            {
                "game": self.game,
                "clip_stem": "quarantine_clip",
                "source_stage": "quarantine",
                "feedback_type": "false_negative",
                "detector": "yolo_detector",
            },
            {
                "game": self.game,
                "clip_stem": "quarantine_clip",
                "source_stage": "quarantine",
                "feedback_type": "false_negative",
                "detector": "yolo_detector",
            },
            {
                "game": self.game,
                "clip_stem": "quarantine_clip",
                "source_stage": "quarantine",
                "feedback_type": "needs_roi_template",
                "detector": "weapon_detector",
            },
        ):
            response = self.client.post("/api/feedback/record", json=payload)
            self.assertEqual(response.status_code, 200)

        response = self.client.post(f"/api/feedback/apply/{self.game}", json={"dry_run": False})
        self.assertEqual(response.status_code, 200)
        result = response.get_json()["result"]
        self.assertTrue(result["applied"])
        self.assertTrue(result["retrain_recommendation"]["recommended"])

        weights = yaml.safe_load((self.game_pack_dir / "weights.yaml").read_text())
        self.assertLess(weights["clip_judge"]["thresholds"]["accept"], 0.7)
        self.assertIn("last_feedback_summary", weights["clip_judge"]["feedback"])

        self.assertEqual(self.client.get(f"/feedback?game={self.game}").status_code, 200)

        apply_result = apply_feedback_updates(self.game, self.config, dry_run=True)
        self.assertTrue(apply_result["weight_update"]["composite_weights"]["context_score"] >= 0.1)


if __name__ == "__main__":
    unittest.main()
