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
    import pipeline.weapon_detector_audit as audit_module
    from pipeline.weapon_detector_audit import audit_weapon_detector

    run_module = importlib.import_module("run")
finally:
    os.chdir(_ORIGINAL_CWD)


class WeaponDetectorAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.pack_dir.mkdir(parents=True, exist_ok=True)

        self.config = {
            "paths": {
                "assets": str(self.assets),
                "inbox": str(self.root / "inbox"),
                "quarantine": str(self.root / "quarantine"),
                "processing": str(self.root / "processing"),
                "accepted": str(self.root / "accepted"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "weapon_detector": {
                "enabled": True,
                "audit": {
                    "top_k": 2,
                    "min_confidence": 0.45,
                    "export_crops": True,
                },
            },
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
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {
                    "hero_one": {"display_name": "Hero One"},
                    "hero_two": {"display_name": "Hero Two"},
                },
            },
            "moments.yaml": {"moments": []},
            "hud.yaml": {
                "rois": {
                    "weapon_detector": {"x": 58, "y": 895, "w": 125, "h": 135},
                },
                "detectors": {
                    "weapon_detector": {
                        "roi_ref": "weapon_detector",
                    }
                },
            },
            "weights.yaml": {"clip_judge": {}},
        }
        for filename, payload in files.items():
            (self.pack_dir / filename).write_text(yaml.safe_dump(payload, sort_keys=False))

    def _write_clip_meta(
        self,
        stage: str,
        stem: str,
        *,
        reason: str | None = None,
        weapon_detection: dict | None = None,
        clip_exists: bool = True,
    ) -> tuple[Path, Path]:
        stage_root = Path(self.config["paths"][stage]) / self.game
        if reason:
            stage_root = stage_root / reason
        stage_root.mkdir(parents=True, exist_ok=True)
        clip_path = stage_root / f"{stem}.mp4"
        if clip_exists:
            clip_path.write_bytes(b"clip")
        meta_path = stage_root / f"{stem}.meta.json"
        payload = {
            "clip_id": stem,
            "game": self.game,
            "clip_path": str(clip_path),
        }
        if weapon_detection is not None:
            payload["weapon_detection"] = weapon_detection
        meta_path.write_text(json.dumps(payload, indent=2))
        return clip_path, meta_path

    def test_audit_weapon_detector_writes_report_and_ranks_targets(self) -> None:
        roi = {"x": 58, "y": 895, "w": 125, "h": 135, "base_width": 1920, "base_height": 1080}
        match_box = {"x": 60, "y": 900, "w": 64, "h": 64, "base_width": 1920, "base_height": 1080}
        no_match_payload = {
            "weapon_id": None,
            "display_name": None,
            "confidence": 0.68,
            "method": "no_match",
            "frame_time": 12.4,
            "roi": roi,
            "best_match_box": match_box,
            "top_candidates": [
                {
                    "weapon_id": "hero_one",
                    "display_name": "Hero One",
                    "confidence": 0.68,
                    "match_box": match_box,
                    "match_variant": "grayscale",
                    "match_scale": 0.9,
                }
            ],
            "frame_observations": [
                {
                    "timestamp": 12.4,
                    "weapon_id": "hero_one",
                    "display_name": "Hero One",
                    "confidence": 0.68,
                    "match_box": match_box,
                    "match_variant": "grayscale",
                    "match_scale": 0.9,
                }
            ],
        }
        template_match_payload = {
            "weapon_id": "hero_two",
            "display_name": "Hero Two",
            "confidence": 0.83,
            "method": "template_match",
            "frame_time": 8.1,
            "roi": roi,
            "best_match_box": match_box,
            "top_candidates": [
                {
                    "weapon_id": "hero_two",
                    "display_name": "Hero Two",
                    "confidence": 0.83,
                    "match_box": match_box,
                }
            ],
            "frame_observations": [],
        }

        self._write_clip_meta("inbox", "clip_one", weapon_detection=no_match_payload)
        self._write_clip_meta("quarantine", "clip_two", reason="missing_context", weapon_detection={
            **no_match_payload,
            "confidence": 0.66,
            "frame_time": 10.0,
            "frame_observations": [
                {
                    "timestamp": 10.0,
                    "weapon_id": "hero_one",
                    "display_name": "Hero One",
                    "confidence": 0.66,
                    "match_box": match_box,
                }
            ],
        })
        self._write_clip_meta("accepted", "clip_three", weapon_detection=template_match_payload)

        def fake_export(item: dict, crop_dir: Path, index: int) -> dict[str, str]:
            roi_path = crop_dir / f"{index:02d}_{item['clip_stem']}_roi.png"
            roi_path.write_bytes(b"roi")
            candidate_path = crop_dir / f"{index:02d}_{item['clip_stem']}_candidate.png"
            candidate_path.write_bytes(b"candidate")
            return {
                "roi_crop_path": str(roi_path),
                "candidate_crop_path": str(candidate_path),
            }

        with patch.object(audit_module, "_CV2_AVAILABLE", True), \
            patch.object(audit_module, "_export_candidate_assets", side_effect=fake_export) as mocked_export:
            result = audit_weapon_detector(self.game, self.config)

        self.assertEqual(result["audited_clips"], 3)
        self.assertEqual(result["stage_counts"]["inbox"], 1)
        self.assertEqual(result["stage_counts"]["quarantine"], 1)
        self.assertEqual(result["method_counts"]["no_match"], 2)
        self.assertEqual(result["method_counts"]["template_match"], 1)
        self.assertEqual(result["exported_crop_count"], 2)
        self.assertEqual(mocked_export.call_count, 2)
        self.assertEqual(result["recommended_targets"][0]["weapon_id"], "hero_one")
        self.assertEqual(result["recommended_targets"][0]["count"], 2)
        self.assertEqual(len(result["ranked_candidates"]), 2)
        self.assertEqual(result["ranked_candidates"][0]["candidate_weapon_id"], "hero_one")
        self.assertEqual(result["ranked_candidates"][1]["quarantine_reason"], "missing_context")
        self.assertTrue(Path(result["report_path"]).exists())
        self.assertTrue(Path(result["ranked_candidates"][0]["exported_assets"]["roi_crop_path"]).exists())

        written = json.loads(Path(result["report_path"]).read_text())
        self.assertEqual(written["recommended_targets"][0]["weapon_id"], "hero_one")
        self.assertEqual(len(written["ranked_candidates"]), 2)

    def test_audit_weapon_detector_skips_missing_and_missing_detection(self) -> None:
        self._write_clip_meta(
            "inbox",
            "missing_clip",
            clip_exists=False,
            weapon_detection={
                "weapon_id": None,
                "display_name": None,
                "confidence": 0.51,
                "method": "no_match",
                "frame_time": 3.0,
                "top_candidates": [{"weapon_id": "hero_one", "confidence": 0.51}],
            },
        )
        self._write_clip_meta("processing", "no_detection", weapon_detection=None)

        with patch.object(audit_module, "_CV2_AVAILABLE", True), \
            patch.object(audit_module, "_export_candidate_assets", return_value={}) as mocked_export:
            result = audit_weapon_detector(self.game, self.config)

        self.assertEqual(result["audited_clips"], 0)
        self.assertEqual(result["skipped_missing"], 1)
        self.assertEqual(result["skipped_no_detection"], 1)
        self.assertEqual(result["ranked_candidates"], [])
        mocked_export.assert_not_called()

    def test_run_cli_routes_to_weapon_detector_audit(self) -> None:
        config_path = self.root / "config.yaml"
        config_path.write_text(yaml.safe_dump({
            "paths": {
                "assets": str(self.assets),
                "inbox": str(self.root / "inbox"),
                "quarantine": str(self.root / "quarantine"),
                "processing": str(self.root / "processing"),
                "accepted": str(self.root / "accepted"),
                "rejected": str(self.root / "rejected"),
                "templates": str(self.root / "templates"),
                "logs": str(self.root / "logs"),
            }
        }))

        with patch.object(run_module, "audit_weapon_detector", return_value={"ok": True}) as mocked:
            with patch.object(sys, "argv", ["run.py", "--audit-weapon-detector", self.game, "--config", str(config_path)]):
                run_module.main()

        mocked.assert_called_once()
        self.assertEqual(mocked.call_args[0][0], self.game)


if __name__ == "__main__":
    unittest.main()
