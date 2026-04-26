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
    from pipeline.weapon_icon_promotion import promote_weapon_audit_crop

    run_module = importlib.import_module("run")
finally:
    os.chdir(_ORIGINAL_CWD)


class WeaponIconPromotionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.report_dir = self.pack_dir / "reports" / "weapon_detector"
        self.icon_dir = self.assets / "weapon_icons" / self.game
        self.pack_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.icon_dir.mkdir(parents=True, exist_ok=True)

        self.config = {
            "paths": {
                "assets": str(self.assets),
                "inbox": str(self.root / "inbox"),
                "quarantine": str(self.root / "quarantine"),
                "processing": str(self.root / "processing"),
                "accepted": str(self.root / "accepted"),
                "rejected": str(self.root / "rejected"),
                "templates": str(self.root / "templates"),
                "logs": str(self.root / "logs"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "weapon_detector": {"enabled": True},
        }
        self._write_pack()

        self.candidate_path = self.report_dir / "candidate.png"
        self.candidate_path.write_bytes(b"candidate")
        self.roi_path = self.report_dir / "roi.png"
        self.roi_path.write_bytes(b"roi")

        self.report_path = self.report_dir / "20260425-000000.json"
        self.report_path.write_text(json.dumps({
            "game": self.game,
            "ranked_candidates": [
                {
                    "candidate_weapon_id": "hero_one",
                    "candidate_display_name": "Hero One",
                    "candidate_confidence": 0.72,
                    "clip_stem": "clip_one",
                    "exported_assets": {
                        "candidate_crop_path": str(self.candidate_path),
                        "roi_crop_path": str(self.roi_path),
                    },
                }
            ],
        }, indent=2))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_pack(self) -> None:
        files = {
            "game.yaml": {
                "game_id": self.game,
                "display_name": "Test Game",
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {"hero_one": {"display_name": "Hero One"}},
            },
            "moments.yaml": {"moments": []},
            "hud.yaml": {
                "rois": {"weapon_detector": {"x": 58, "y": 895, "w": 125, "h": 135}},
                "detectors": {
                    "weapon_detector": {
                        "roi_ref": "weapon_detector",
                        "icon_dir": str(self.icon_dir),
                    }
                },
            },
            "weights.yaml": {"clip_judge": {}},
        }
        for filename, payload in files.items():
            (self.pack_dir / filename).write_text(yaml.safe_dump(payload, sort_keys=False))

    def test_promote_weapon_audit_crop_requires_overwrite(self) -> None:
        asset_path = self.icon_dir / "hero_one.png"
        asset_path.write_bytes(b"old-icon")

        result = promote_weapon_audit_crop(
            self.game,
            self.config,
            report_path=self.report_path,
            overwrite=False,
        )

        self.assertFalse(result["ok"])
        self.assertIn("asset already exists", result["error"])
        self.assertEqual(asset_path.read_bytes(), b"old-icon")

    def test_promote_weapon_audit_crop_copies_asset_and_creates_backup(self) -> None:
        asset_path = self.icon_dir / "hero_one.png"
        asset_path.write_bytes(b"old-icon")

        result = promote_weapon_audit_crop(
            self.game,
            self.config,
            report_path=self.report_path,
            overwrite=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["entity_id"], "hero_one")
        self.assertEqual(asset_path.read_bytes(), b"candidate")
        backup_path = Path(result["backup_path"])
        self.assertTrue(backup_path.exists())
        self.assertEqual(backup_path.read_bytes(), b"old-icon")
        log_path = self.report_dir / "promotion_log.jsonl"
        self.assertTrue(log_path.exists())
        self.assertIn("hero_one", log_path.read_text())

    def test_promote_weapon_audit_crop_prefers_roi_when_requested(self) -> None:
        result = promote_weapon_audit_crop(
            self.game,
            self.config,
            report_path=self.report_path,
            source="roi",
            overwrite=False,
            dry_run=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["source_type"], "roi")
        self.assertEqual(Path(result["source_path"]).read_bytes(), b"roi")

    def test_run_cli_routes_to_weapon_icon_promotion(self) -> None:
        config_path = self.root / "config.yaml"
        config_path.write_text(yaml.safe_dump(self.config))

        with patch.object(run_module, "promote_weapon_audit_crop", return_value={"ok": True}) as mocked:
            with patch.object(
                sys,
                "argv",
                [
                    "run.py",
                    "--promote-weapon-audit-crop",
                    self.game,
                    "--config",
                    str(config_path),
                    "--report",
                    str(self.report_path),
                    "--rank",
                    "2",
                    "--crop-source",
                    "roi",
                    "--overwrite",
                    "--dry-run",
                ],
            ):
                run_module.main()

        mocked.assert_called_once()
        self.assertEqual(mocked.call_args[0][0], self.game)
        self.assertEqual(mocked.call_args[1]["rank"], 2)
        self.assertEqual(mocked.call_args[1]["source"], "roi")
        self.assertTrue(mocked.call_args[1]["overwrite"])
        self.assertTrue(mocked.call_args[1]["dry_run"])


if __name__ == "__main__":
    unittest.main()
