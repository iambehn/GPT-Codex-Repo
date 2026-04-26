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

from pipeline.game_pack_evaluator import evaluate_game_pack, scaffold_gold_set


class GamePackEvaluatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.gold_dir = self.pack_dir / "examples" / "gold_set"
        self.gold_dir.mkdir(parents=True)
        self.config = {
            "paths": {
                "assets": str(self.assets),
                "quarantine": str(self.root / "quarantine"),
                "inbox": str(self.root / "inbox"),
                "processing": str(self.root / "processing"),
                "rejected": str(self.root / "rejected"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "clip_judge": {"enabled": False},
            "niceshot_detector": {"enabled": False},
            "yolo_detector": {"enabled": False},
        }
        self._write_pack()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_pack(self) -> None:
        files = {
            "game.yaml": {
                "game_id": self.game,
                "display_name": "Test Game",
                "genre": "hero_shooter",
                "ui_version": "test",
                "detectors": {
                    "audio_detector": {"enabled": False},
                    "kill_feed": {"enabled": False},
                    "weapon_detector": {"enabled": False},
                    "niceshot": {"enabled": False},
                },
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {"hero_one": {"display_name": "Hero One"}},
                "aliases": {},
            },
            "moments.yaml": {
                "moments": [{"id": "precision_pick", "labels": ["headshot"]}],
                "hook_targets": {"window_seconds": 1.5},
            },
            "hud.yaml": {"ui_version": "test", "rois": {}, "detectors": {}},
            "weights.yaml": {
                "clip_judge": {
                    "thresholds": {"accept": 0.65, "quarantine": 0.45, "reject": 0.25},
                    "hard_gates": {"hook_window_seconds": 1.5, "require_context_fields": ["player_entity", "detected_event"]},
                    "hook_enforcer": {
                        "window_seconds": 1.5,
                        "acceptance_threshold": 0.5,
                        "pre_event_padding_seconds": 0.5,
                        "minimum_remaining_seconds": 6.0,
                    },
                    "composite_weights": {
                        "ai_clip_score": 0.35,
                        "ai_hook_score": 0.25,
                        "kill_feed_score": 0.20,
                        "context_score": 0.20,
                    },
                    "quarantine_reasons": [
                        "missing_context",
                        "hook_not_resolved",
                        "low_confidence",
                        "ui_drift",
                        "needs_roi_template",
                    ],
                }
            },
        }
        for filename, payload in files.items():
            (self.pack_dir / filename).write_text(yaml.safe_dump(payload))

    def test_scaffold_gold_set_creates_manifest_and_folders(self) -> None:
        result = scaffold_gold_set(self.game, self.config)
        self.assertTrue((self.gold_dir / "manifest.yaml").exists())
        self.assertTrue((self.gold_dir / "clips" / ".gitkeep").exists())
        self.assertTrue((self.gold_dir / "sidecars" / ".gitkeep").exists())
        self.assertTrue(result["gold_set_dir"].endswith("gold_set"))

    def test_evaluate_game_pack_reports_pass_and_failure(self) -> None:
        clips_dir = self.gold_dir / "clips"
        sidecars_dir = self.gold_dir / "sidecars"
        clips_dir.mkdir()
        sidecars_dir.mkdir()

        accept_clip = clips_dir / "accept.mp4"
        accept_clip.write_bytes(b"fake")
        (sidecars_dir / "accept.meta.json").write_text(json.dumps({
            "clip_id": "accept",
            "game": self.game,
            "duration_seconds": 12,
            "kill_feed": {
                "passed": True,
                "sweat_score": 80,
                "kill_count": 2,
                "headshot_count": 1,
                "kill_timestamps": [0.8],
                "headshot_timestamps": [0.8],
                "method": "fixture",
            },
            "weapon_detection": {"weapon_id": "hero_one", "confidence": 0.9, "method": "fixture"},
        }))

        quarantine_clip = clips_dir / "quarantine.mp4"
        quarantine_clip.write_bytes(b"fake")
        (sidecars_dir / "quarantine.meta.json").write_text(json.dumps({
            "clip_id": "quarantine",
            "game": self.game,
            "duration_seconds": 12,
            "weapon_detection": {"weapon_id": None, "confidence": 0.0, "method": "no_match"},
        }))

        manifest = {
            "game": self.game,
            "clips": [
                {
                    "id": "accept",
                    "clip": "clips/accept.mp4",
                    "meta": "sidecars/accept.meta.json",
                    "expected": {
                        "status": "accept",
                        "hook_gate_passed": True,
                        "player_entity": "hero_one",
                        "detected_event": "multi_kill",
                    },
                },
                {
                    "id": "quarantine",
                    "clip": "clips/quarantine.mp4",
                    "meta": "sidecars/quarantine.meta.json",
                    "expected": {"status": "accept"},
                },
            ],
        }
        (self.gold_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest))

        result = evaluate_game_pack(self.game, self.config, run_detectors=False, force=True)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["summary"]["total"], 2)
        self.assertEqual(result["summary"]["passed"], 1)
        self.assertEqual(result["summary"]["failures"], ["quarantine"])
        self.assertTrue(Path(result["report_path"]).exists())
        self.assertTrue((self.pack_dir / "reports" / "evaluation" / "latest.json").exists())

    def test_missing_manifest_returns_failed_report(self) -> None:
        result = evaluate_game_pack(self.game, self.config, run_detectors=False, force=True)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["errors"])


if __name__ == "__main__":
    unittest.main()
