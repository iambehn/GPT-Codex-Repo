from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    from pipeline.yolo_dataset import build_yolo_dataset
    from pipeline.yolo_training import train_yolo_model

    run_module = importlib.import_module("run")
finally:
    os.chdir(_ORIGINAL_CWD)


class YoloTrainingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.pack_dir.mkdir(parents=True, exist_ok=True)
        self.model_dir = self.root / "models" / "yolo" / self.game
        (self.model_dir / "weights").mkdir(parents=True, exist_ok=True)
        self.config = {
            "paths": {
                "assets": str(self.assets),
                "inbox": str(self.root / "inbox"),
                "quarantine": str(self.root / "quarantine"),
                "processing": str(self.root / "processing"),
                "accepted": str(self.root / "accepted"),
                "rejected": str(self.root / "rejected"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "yolo_detector": {"enabled": True},
            "weapon_detector": {"enabled": True},
            "yolo_training": {
                "base_model": "yolov8s.pt",
                "epochs": 5,
                "imgsz": 320,
                "batch": 2,
                "patience": 3,
                "device": "cpu",
                "workers": 1,
                "minimum_examples": 2,
            },
        }
        self._write_pack()
        build_yolo_dataset(self.game, self.config)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_pack(self) -> None:
        icon_dir = self.assets / "weapon_icons" / self.game
        icon_dir.mkdir(parents=True, exist_ok=True)
        icon_dir.joinpath("hero_one.png").write_bytes(_fake_png())
        icon_dir.joinpath("hero_two.png").write_bytes(_fake_png())

        files = {
            "game.yaml": {
                "game_id": self.game,
                "display_name": "Test Game",
                "genre": "hero_shooter",
                "ui_version": "test",
                "detectors": {"niceshot": {"enabled": False}},
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
                "moments": [{"id": "precision_pick"}],
                "hook_targets": {"window_seconds": 1.5},
            },
            "hud.yaml": {
                "ui_version": "test",
                "rois": {"weapon_detector": {"x": 10, "y": 10, "w": 50, "h": 50}},
                "detectors": {
                    "weapon_detector": {
                        "roi_ref": "weapon_detector",
                        "icon_dir": str(icon_dir),
                        "entities_kind": "heroes",
                    },
                    "yolo": {
                        "enabled": True,
                        "inference_mode": "roi_crop",
                        "roi_ref": "weapon_detector",
                        "weights_path": str(self.model_dir / "weights" / "best.pt"),
                        "labels": {
                            "hero_one_label": {"kind": "entity", "maps_to": "hero_one"},
                            "hero_two_label": {"kind": "entity", "maps_to": "hero_two"},
                        },
                    },
                },
            },
            "weights.yaml": {
                "clip_judge": {
                    "thresholds": {"accept": 0.7, "quarantine": 0.45, "reject": 0.25},
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
            (self.pack_dir / filename).write_text(yaml.safe_dump(payload, sort_keys=False))

    def test_train_yolo_dry_run_reports_expected_run_dir(self) -> None:
        result = train_yolo_model(self.game, self.config, dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "dry_run")
        self.assertIn("/runs/", result["expected_run_dir"])

    def test_train_yolo_missing_dependency_is_structured(self) -> None:
        with patch("pipeline.yolo_training._run_training", side_effect=ImportError("missing ultralytics")):
            result = train_yolo_model(self.game, self.config)

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "missing_dependency")
        self.assertIn("missing ultralytics", result["errors"][0])

    def test_train_yolo_promotes_best_weights_and_writes_manifest(self) -> None:
        run_dir = self.model_dir / "runs" / "mock-run"
        weights_dir = run_dir / "weights"
        weights_dir.mkdir(parents=True, exist_ok=True)
        (weights_dir / "best.pt").write_text("best")
        (weights_dir / "last.pt").write_text("last")
        results = SimpleNamespace(
            save_dir=str(run_dir),
            results_dict={"metrics/mAP50(B)": 0.91, "metrics/precision(B)": 0.87},
        )

        with patch("pipeline.yolo_training._run_training", return_value=results):
            result = train_yolo_model(self.game, self.config)

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "ok")
        self.assertEqual((self.model_dir / "weights" / "best.pt").read_text(), "best")
        self.assertEqual((self.model_dir / "weights" / "last.pt").read_text(), "last")

        manifest = json.loads((self.model_dir / "training_manifest.json").read_text())
        self.assertEqual(manifest["metrics"]["metrics/mAP50(B)"], 0.91)
        self.assertEqual(manifest["promoted_best_weights"], str(self.model_dir / "weights" / "best.pt"))

    def test_run_cli_routes_to_yolo_training(self) -> None:
        config_path = self.root / "config.yaml"
        config_path.write_text(yaml.safe_dump({"paths": {"assets": str(self.assets)}}))

        with patch.object(run_module, "train_yolo_model", return_value={"ok": True}) as mocked:
            with patch.object(sys, "argv", ["run.py", "--train-yolo", self.game, "--config", str(config_path)]):
                run_module.main()

        mocked.assert_called_once()
        self.assertEqual(mocked.call_args[0][0], self.game)


def _fake_png(width: int = 16, height: int = 16) -> bytes:
    import binascii
    import struct
    import zlib

    def chunk(kind: bytes, data: bytes) -> bytes:
        crc = binascii.crc32(kind + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    rows = b"".join(b"\x00" + (b"\x00\x00\x00\x00" * width) for _ in range(height))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )


if __name__ == "__main__":
    unittest.main()
