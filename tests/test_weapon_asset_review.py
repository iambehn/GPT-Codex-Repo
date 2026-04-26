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
    import pipeline.weapon_asset_review as review_module
    from pipeline.weapon_asset_review import render_weapon_audit_review

    run_module = importlib.import_module("run")
finally:
    os.chdir(_ORIGINAL_CWD)


@unittest.skipUnless(getattr(review_module, "_CV2_AVAILABLE", False), "opencv required")
class WeaponAssetReviewTests(unittest.TestCase):
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
        self._write_image(self.icon_dir / "hero_one.png", (0, 255, 0))
        self._write_image(self.report_dir / "candidate.png", (0, 0, 255))
        self._write_image(self.report_dir / "roi.png", (255, 0, 0), size=(125, 135))

        self.report_path = self.report_dir / "20260425-000000.json"
        self.report_path.write_text(json.dumps({
            "game": self.game,
            "generated_at": "2026-04-25T00:00:00+00:00",
            "ranked_candidates": [
                {
                    "candidate_weapon_id": "hero_one",
                    "candidate_display_name": "Hero One",
                    "candidate_confidence": 0.71,
                    "clip_stem": "clip_one",
                    "exported_assets": {
                        "candidate_crop_path": str(self.report_dir / "candidate.png"),
                        "roi_crop_path": str(self.report_dir / "roi.png"),
                    },
                },
                {
                    "candidate_weapon_id": "hero_two",
                    "candidate_display_name": "Hero Two",
                    "candidate_confidence": 0.63,
                    "clip_stem": "clip_two",
                    "exported_assets": {
                        "roi_crop_path": str(self.report_dir / "roi.png"),
                    },
                },
            ],
        }, indent=2))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_pack(self) -> None:
        files = {
            "game.yaml": {"game_id": self.game, "display_name": "Test Game"},
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {
                    "hero_one": {"display_name": "Hero One"},
                    "hero_two": {"display_name": "Hero Two"},
                },
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

    def _write_image(self, path: Path, color: tuple[int, int, int], size: tuple[int, int] = (64, 64)) -> None:
        import cv2
        import numpy as np

        width, height = size
        image = np.full((height, width, 3), color, dtype=np.uint8)
        cv2.imwrite(str(path), image)

    def test_render_weapon_audit_review_outputs_manifest_and_sheet(self) -> None:
        result = render_weapon_audit_review(
            self.game,
            self.config,
            report_path=self.report_path,
            top_k=2,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["rendered_items"], 2)
        self.assertTrue(Path(result["sheet_path"]).exists())
        self.assertTrue(Path(result["manifest_path"]).exists())
        self.assertTrue(Path(result["html_path"]).exists())

        manifest = json.loads(Path(result["manifest_path"]).read_text())
        self.assertEqual(len(manifest["items"]), 2)
        self.assertTrue(Path(manifest["items"][0]["comparison_path"]).exists())
        self.assertIsNotNone(manifest["items"][0]["current_asset_path"])
        self.assertIsNone(manifest["items"][1]["current_asset_path"])
        html = Path(result["html_path"]).read_text()
        self.assertIn("Weapon Audit Review", html)
        self.assertIn("Hero One", html)
        self.assertIn("review_sheet.png", html)

    def test_run_cli_routes_to_weapon_asset_review(self) -> None:
        config_path = self.root / "config.yaml"
        config_path.write_text(yaml.safe_dump(self.config))

        with patch.object(run_module, "render_weapon_audit_review", return_value={"ok": True}) as mocked:
            with patch.object(
                sys,
                "argv",
                [
                    "run.py",
                    "--render-weapon-audit-review",
                    self.game,
                    "--config",
                    str(config_path),
                    "--report",
                    str(self.report_path),
                    "--top-k",
                    "4",
                ],
            ):
                run_module.main()

        mocked.assert_called_once()
        self.assertEqual(mocked.call_args[0][0], self.game)
        self.assertEqual(mocked.call_args[1]["top_k"], 4)


if __name__ == "__main__":
    unittest.main()
