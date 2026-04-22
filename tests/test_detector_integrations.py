from __future__ import annotations

import importlib
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
    from pipeline.clip_judge import evaluate as evaluate_clip
    from pipeline.game_pack import validate_game_pack
    from pipeline.hook_enforcer import run_hook_enforcer
    from pipeline.niceshot_detector import run_niceshot_detector
    from pipeline.yolo_detector import run_yolo_detector

    run_module = importlib.import_module("run")
finally:
    os.chdir(_ORIGINAL_CWD)


class DetectorIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.pack_dir.mkdir(parents=True, exist_ok=True)
        self.clip = self.root / "clip.mp4"
        self.clip.write_bytes(b"not-a-real-video")
        self.meta_path = self.clip.with_suffix(".meta.json")
        self.meta_path.write_text(json.dumps({
            "clip_id": "clip",
            "game": self.game,
            "clip_path": str(self.clip),
            "weapon_detection": {"weapon_id": None, "confidence": 0.0, "method": "no_match"},
        }))

        self.weights_path = self.root / "models" / "yolo" / self.game / "weights" / "best.pt"
        self.config = {
            "paths": {
                "assets": str(self.assets),
                "quarantine": str(self.root / "quarantine"),
                "inbox": str(self.root / "inbox"),
                "processing": str(self.root / "processing"),
                "rejected": str(self.root / "rejected"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "audio_detector": {"enabled": True},
            "kill_feed": {"enabled": True},
            "weapon_detector": {"enabled": True},
            "niceshot_detector": {
                "enabled": True,
                "mode": "stub",
                "provider": "niceshot_ai",
                "profile": "cod_like_default",
                "stub": {
                    "action_score": 0.8,
                    "hook_score": 0.7,
                    "confidence": 0.6,
                    "moments": [
                        {"timestamp": 1.2, "kind": "flick", "confidence": 0.77, "hook_candidate": True}
                    ],
                },
            },
            "yolo_detector": {"enabled": True, "confidence_threshold": 0.6},
            "clip_judge": {"enabled": False},
        }
        self._write_pack(yolo_enabled=False)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_pack(self, yolo_enabled: bool = False, niceshot_enabled: bool = True) -> dict:
        files = {
            "game.yaml": {
                "game_id": self.game,
                "display_name": "Test Game",
                "genre": "hero_shooter",
                "ui_version": "test",
                "detectors": {
                    "audio_detector": {"enabled": True},
                    "kill_feed": {"enabled": True},
                    "weapon_detector": {"enabled": True},
                    "niceshot": {"enabled": niceshot_enabled, "provider": "niceshot_ai"},
                },
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {
                    "hero_one": {"display_name": "Hero One"},
                    "hero_two": {"display_name": "Hero Two"},
                },
                "aliases": {},
            },
            "moments.yaml": {
                "moments": [
                    {"id": "precision_pick", "labels": ["headshot"]},
                    {"id": "multi_kill_swing", "labels": ["multi-kill"]},
                ],
                "hook_targets": {"window_seconds": 1.5},
            },
            "hud.yaml": {
                "ui_version": "test",
                "rois": {},
                "detectors": {
                    "yolo": {
                        "enabled": yolo_enabled,
                        "weights_path": str(self.weights_path),
                        "labels": {
                            "hero_one_label": {"kind": "entity", "maps_to": "hero_one"},
                            "medal_headshot": {"kind": "event", "maps_to": "precision_pick"},
                        },
                    }
                },
            },
            "weights.yaml": {
                "clip_judge": {
                    "thresholds": {"accept": 0.7, "quarantine": 0.45, "reject": 0.25},
                    "hard_gates": {"require_context_fields": ["player_entity", "detected_event"]},
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
        from pipeline.game_pack import load_game_pack

        return load_game_pack(self.game, self.config)

    def _meta(self) -> dict:
        return json.loads(self.meta_path.read_text())

    def test_niceshot_disabled_stub_fixture_and_idempotency(self) -> None:
        pack = self._write_pack(niceshot_enabled=False)
        disabled = run_niceshot_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(disabled["status"], "disabled")

        pack = self._write_pack(niceshot_enabled=True)
        stub = run_niceshot_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(stub["status"], "ok")
        self.assertEqual(stub["moments"][0]["kind"], "flick")

        self.config["niceshot_detector"]["stub"]["moments"] = []
        cached = run_niceshot_detector(self.clip, self.game, self.config, pack, force=False)
        self.assertEqual(cached["moments"][0]["kind"], "flick")

        fixture_dir = self.root / "fixtures"
        fixture_dir.mkdir()
        (fixture_dir / "clip.json").write_text(json.dumps({
            "action_score": 0.9,
            "hook_score": 0.85,
            "confidence": 0.8,
            "moments": [{"timestamp": 0.5, "kind": "killstreak", "confidence": 0.88}],
        }))
        self.config["niceshot_detector"].update({"mode": "fixture_json", "fixture_dir": str(fixture_dir)})
        fixture = run_niceshot_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(fixture["moments"][0]["kind"], "killstreak")

        (fixture_dir / "clip.json").write_text("{bad json")
        malformed = run_niceshot_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(malformed["status"], "error")

    def test_yolo_disabled_missing_weights_missing_dependency_and_mapping(self) -> None:
        pack = self._write_pack(yolo_enabled=False)
        disabled = run_yolo_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(disabled["status"], "disabled")

        pack = self._write_pack(yolo_enabled=True)
        missing_weights = run_yolo_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(missing_weights["status"], "missing_weights")

        self.weights_path.parent.mkdir(parents=True, exist_ok=True)
        self.weights_path.write_text("fake weights")
        with patch("pipeline.yolo_detector._run_model_inference", side_effect=ImportError("missing ultralytics")):
            missing_dependency = run_yolo_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(missing_dependency["status"], "missing_dependency")

        raw = [
            {"label": "hero_one_label", "class_id": 0, "confidence": 0.91, "box": [1, 2, 3, 4], "timestamp": 0.0},
            {"label": "medal_headshot", "class_id": 1, "confidence": 0.82, "box": [5, 6, 7, 8], "timestamp": 1.1},
        ]
        with patch("pipeline.yolo_detector._run_model_inference", return_value=raw):
            detected = run_yolo_detector(self.clip, self.game, self.config, pack, force=True)
        self.assertEqual(detected["status"], "ok")
        self.assertEqual(detected["top_entity"]["entity_id"], "hero_one")
        self.assertEqual(detected["event_candidates"][0]["event_id"], "precision_pick")

        with patch("pipeline.yolo_detector._run_model_inference", return_value=[]):
            cached = run_yolo_detector(self.clip, self.game, self.config, pack, force=False)
        self.assertEqual(cached["top_entity"]["entity_id"], "hero_one")

    def test_clip_judge_uses_niceshot_moments_and_yolo_context(self) -> None:
        pack = self._write_pack(yolo_enabled=True)
        meta = self._meta()
        meta["niceshot_detection"] = {
            "status": "ok",
            "action_score": 0.8,
            "hook_score": 0.7,
            "confidence": 0.9,
            "moments": [{"timestamp": 0.4, "kind": "entry_frag", "confidence": 0.86, "hook_candidate": True}],
        }
        meta["yolo_detection"] = {
            "status": "ok",
            "top_entity": {"entity_id": "hero_two", "confidence": 0.93},
            "event_candidates": [{"event_id": "precision_pick", "confidence": 0.88, "timestamp": 0.4}],
            "context_confidence": 0.905,
        }
        self.meta_path.write_text(json.dumps(meta))

        result = evaluate_clip(self.clip, pack, self.config, force=True)
        self.assertIn("niceshot", {moment["source"] for moment in result["candidate_moments"]})
        self.assertEqual(result["context"]["player_entity"], "hero_two")
        self.assertEqual(result["context"]["detected_event"], "precision_pick")
        self.assertEqual(result["detector_outputs"]["yolo"]["top_entity"]["entity_id"], "hero_two")

    def test_game_pack_validation_rejects_bad_yolo_mapping(self) -> None:
        pack = yaml.safe_load((self.pack_dir / "hud.yaml").read_text())
        pack["detectors"]["yolo"]["labels"]["bad_label"] = {"kind": "entity", "maps_to": "missing_hero"}
        (self.pack_dir / "hud.yaml").write_text(yaml.safe_dump(pack))

        result = validate_game_pack(self.game, self.config)
        self.assertFalse(result["valid"])
        self.assertTrue(any("missing_hero" in error for error in result["errors"]))

    def test_run_process_clip_orders_niceshot_and_yolo_before_judge(self) -> None:
        clip = self.root / "orchestration.mp4"
        clip.write_bytes(b"clip")
        clip.with_suffix(".meta.json").write_text(json.dumps({"clip_id": "orchestration", "game": self.game}))
        pack = self._write_pack(yolo_enabled=False, niceshot_enabled=True)
        calls: list[str] = []

        def record(name: str, return_value=None):
            def inner(*args, **kwargs):
                calls.append(name)
                return return_value or {}
            return inner

        with patch.object(run_module, "load_game_pack", return_value=pack), \
            patch.object(run_module, "run_audio_detector", side_effect=record("audio")), \
            patch.object(run_module, "run_kill_feed_parser", side_effect=record("kill_feed", {"passed": True, "sweat_score": 0})), \
            patch.object(run_module, "run_weapon_detector", side_effect=record("weapon")), \
            patch.object(run_module, "run_niceshot_detector", side_effect=record("niceshot")), \
            patch.object(run_module, "run_yolo_detector", side_effect=record("yolo")), \
            patch.object(run_module, "run_hook_enforcer", side_effect=record("hook_enforcer")), \
            patch.object(run_module, "evaluate_clip", side_effect=record("judge", {"decision": {"status": "quarantine"}, "quarantine": {"reason": "low_confidence"}})), \
            patch.object(run_module, "move_to_quarantine", side_effect=record("quarantine")):
            run_module._process_clip(clip, self.game, self.config)

        self.assertEqual(calls[:7], ["audio", "kill_feed", "weapon", "niceshot", "yolo", "hook_enforcer", "judge"])


if __name__ == "__main__":
    unittest.main()
