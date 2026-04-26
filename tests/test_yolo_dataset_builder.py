from __future__ import annotations

import binascii
import importlib
import json
import os
import struct
import sys
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    from pipeline.yolo_dataset import build_yolo_dataset
    import pipeline.yolo_dataset as yolo_dataset_module

    run_module = importlib.import_module("run")
finally:
    os.chdir(_ORIGINAL_CWD)


def _fake_png(width: int = 16, height: int = 16) -> bytes:
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


class YoloDatasetBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.pack_dir.mkdir(parents=True, exist_ok=True)
        self.model_dir = self.root / "models" / "yolo" / self.game
        (self.model_dir / "weights").mkdir(parents=True, exist_ok=True)
        (self.model_dir / "weights" / "best.pt").write_text("fake weights")
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
        }
        self._write_pack(with_labels=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_pack(self, *, with_labels: bool) -> None:
        icon_dir = self.assets / "weapon_icons" / self.game
        icon_dir.mkdir(parents=True, exist_ok=True)
        (icon_dir / "hero_one.png").write_bytes(_fake_png())

        roi_dir = self.pack_dir / "roi_templates"
        roi_dir.mkdir(parents=True, exist_ok=True)
        (roi_dir / "medal_headshot.png").write_bytes(_fake_png())

        ref_dir = self.pack_dir / "examples" / "reference_frames"
        ref_dir.mkdir(parents=True, exist_ok=True)
        (ref_dir / "frame_001.png").write_bytes(_fake_png())

        labels = {
            "hero_one_label": {"kind": "entity", "maps_to": "hero_one"},
            "medal_headshot": {"kind": "event", "maps_to": "precision_pick"},
        } if with_labels else {}

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
                "heroes": {"hero_one": {"display_name": "Hero One"}},
                "aliases": {},
            },
            "moments.yaml": {
                "moments": [{"id": "precision_pick"}],
                "hook_targets": {"window_seconds": 1.5},
            },
            "hud.yaml": {
                "ui_version": "test",
                "rois": {
                    "weapon_detector": {"x": 10, "y": 10, "w": 50, "h": 50},
                    "kill_feed": {"x": 20, "y": 20, "w": 60, "h": 60},
                },
                "roi_templates": [
                    {
                        "id": "medal_headshot",
                        "semantic_type": "medal",
                        "roi_ref": "kill_feed",
                    }
                ],
                "detectors": {
                    "weapon_detector": {
                        "roi_ref": "weapon_detector",
                        "icon_dir": str(icon_dir),
                        "entities_kind": "heroes",
                    },
                    "yolo": {
                        "enabled": True,
                        "weights_path": str(self.model_dir / "weights" / "best.pt"),
                        "labels": labels,
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

    def test_builder_writes_dataset_registry_and_exported_manifest(self) -> None:
        result = build_yolo_dataset(self.game, self.config)

        self.assertTrue(result["ok"])
        dataset = yaml.safe_load((self.model_dir / "dataset.yaml").read_text())
        self.assertEqual(dataset["names"], ["hero_one_label", "medal_headshot"])
        labels_txt = (self.model_dir / "labels.txt").read_text().strip().splitlines()
        self.assertEqual(labels_txt, ["hero_one_label", "medal_headshot"])

        label_map = json.loads((self.model_dir / "label_map.json").read_text())
        self.assertEqual(label_map["classes"][0]["maps_to"], "hero_one")
        self.assertEqual(label_map["classes"][1]["maps_to"], "precision_pick")

        seed_manifest = json.loads((self.model_dir / "seed_manifest.json").read_text())
        self.assertEqual(seed_manifest["summary"]["icons"], 1)
        self.assertEqual(seed_manifest["summary"]["roi_templates"], 1)
        self.assertEqual(seed_manifest["summary"]["reference_frames"], 1)
        self.assertEqual(seed_manifest["sources"]["icons"][0]["suggested_labels"], ["hero_one_label"])
        self.assertEqual(seed_manifest["sources"]["roi_templates"][0]["suggested_labels"], ["medal_headshot"])

        dataset_manifest = json.loads((self.model_dir / "dataset_manifest.json").read_text())
        self.assertEqual(dataset_manifest["export_mode"], "roi_crop")
        self.assertEqual(dataset_manifest["summary"]["exported_examples"], 2)
        self.assertEqual(dataset_manifest["summary"]["icon_seed_examples"], 1)
        self.assertEqual(dataset_manifest["summary"]["roi_template_examples"], 1)
        self.assertEqual(dataset_manifest["summary"]["clip_asset_training_examples"], 0)
        self.assertEqual(dataset_manifest["summary"]["weapon_detector_pseudo_examples"], 0)
        self.assertIn("reference_frames are tracked", dataset_manifest["warnings"][0])

        exported = {sample["label"]: sample for sample in dataset_manifest["samples"]}
        self.assertEqual(set(exported), {"hero_one_label", "medal_headshot"})

        hero_sample = exported["hero_one_label"]
        event_sample = exported["medal_headshot"]
        self.assertTrue(Path(hero_sample["image_path"]).exists())
        self.assertTrue(Path(hero_sample["label_path"]).exists())
        self.assertTrue(Path(event_sample["image_path"]).exists())
        self.assertTrue(Path(event_sample["label_path"]).exists())
        self.assertIn(hero_sample["split"], {"train", "val"})
        self.assertIn(event_sample["split"], {"train", "val"})

        hero_label = Path(hero_sample["label_path"]).read_text().strip().split()
        event_label = Path(event_sample["label_path"]).read_text().strip().split()
        self.assertEqual(hero_label[0], "0")
        self.assertEqual(event_label[0], "1")
        self.assertEqual(result["exported_examples"], 2)

    def test_builder_fails_when_yolo_labels_are_missing(self) -> None:
        self._write_pack(with_labels=False)

        result = build_yolo_dataset(self.game, self.config)

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "failed")
        self.assertIn("labels is empty", result["errors"][0])

    def test_builder_expands_asset_training_entries_with_temporal_offsets(self) -> None:
        clip_dir = self.root / "inbox" / self.game
        clip_dir.mkdir(parents=True, exist_ok=True)
        clip_path = clip_dir / "clip.mp4"
        clip_path.write_bytes(b"not-a-real-video")
        (clip_dir / "clip.meta.json").write_text(json.dumps({
            "clip_id": "clip",
            "game": self.game,
            "clip_path": str(clip_path),
            "asset_training": [
                {
                    "entity_id": "hero_one",
                    "crop_box": {"x": 20, "y": 20, "w": 20, "h": 20},
                    "frame_time_seconds": 1.0,
                    "source_clip": str(clip_path),
                }
            ],
        }))
        self.config["yolo_detector"]["asset_training_time_offsets_seconds"] = [0.0, -0.2, 0.2]

        with patch.object(yolo_dataset_module, "_read_video_frame") as mock_read:
            import numpy as np

            mock_read.return_value = np.zeros((1080, 1920, 3), dtype=np.uint8)
            result = build_yolo_dataset(self.game, self.config)

        self.assertTrue(result["ok"])
        dataset_manifest = json.loads((self.model_dir / "dataset_manifest.json").read_text())
        self.assertEqual(dataset_manifest["asset_training_time_offsets_seconds"], [0.0, -0.2, 0.2])
        self.assertEqual(dataset_manifest["summary"]["clip_asset_training_examples"], 3)
        self.assertEqual(dataset_manifest["summary"]["weapon_detector_pseudo_examples"], 0)
        self.assertEqual(dataset_manifest["summary"]["exported_examples"], 5)
        clip_samples = [
            sample for sample in dataset_manifest["samples"]
            if sample["source_type"] == "clip_asset_training"
        ]
        self.assertEqual(len(clip_samples), 3)
        self.assertEqual(
            sorted(sample["frame_time_seconds"] for sample in clip_samples),
            [0.8, 1.0, 1.2],
        )
        self.assertEqual(
            sorted(sample["frame_offset_seconds"] for sample in clip_samples),
            [-0.2, 0.0, 0.2],
        )
        self.assertTrue(all(Path(sample["image_path"]).exists() for sample in clip_samples))

    def test_builder_bootstraps_pseudo_labels_from_weapon_detector(self) -> None:
        clip_dir = self.root / "inbox" / self.game
        clip_dir.mkdir(parents=True, exist_ok=True)
        clip_path = clip_dir / "pseudo.mp4"
        clip_path.write_bytes(b"not-a-real-video")
        (clip_dir / "pseudo.meta.json").write_text(json.dumps({
            "clip_id": "pseudo",
            "game": self.game,
            "clip_path": str(clip_path),
            "weapon_detection": {
                "weapon_id": "hero_one",
                "display_name": "Hero One",
                "confidence": 0.96,
                "method": "template_match",
                "frame_time": 2.5,
                "best_match_box": {
                    "x": 20,
                    "y": 20,
                    "w": 20,
                    "h": 20,
                    "base_width": 1920,
                    "base_height": 1080,
                },
                "frame_observations": [
                    {
                        "timestamp": 2.5,
                        "weapon_id": "hero_one",
                        "display_name": "Hero One",
                        "confidence": 0.96,
                        "match_box": {
                            "x": 20,
                            "y": 20,
                            "w": 20,
                            "h": 20,
                            "base_width": 1920,
                            "base_height": 1080,
                        },
                    }
                ],
            },
        }))
        self.config["yolo_detector"]["weapon_pseudo_label_min_confidence"] = 0.92

        with patch.object(yolo_dataset_module, "_read_video_frame") as mock_read:
            import numpy as np

            mock_read.return_value = np.zeros((1080, 1920, 3), dtype=np.uint8)
            result = build_yolo_dataset(self.game, self.config)

        self.assertTrue(result["ok"])
        dataset_manifest = json.loads((self.model_dir / "dataset_manifest.json").read_text())
        self.assertEqual(dataset_manifest["weapon_pseudo_label_min_confidence"], 0.92)
        self.assertEqual(dataset_manifest["summary"]["weapon_detector_pseudo_examples"], 1)
        self.assertEqual(dataset_manifest["summary"]["exported_examples"], 3)
        pseudo_samples = [
            sample for sample in dataset_manifest["samples"]
            if sample["source_type"] == "weapon_detector_pseudo"
        ]
        self.assertEqual(len(pseudo_samples), 1)
        self.assertEqual(pseudo_samples[0]["label"], "hero_one_label")
        self.assertEqual(pseudo_samples[0]["pseudo_confidence"], 0.96)
        self.assertTrue(Path(pseudo_samples[0]["image_path"]).exists())

    def test_run_cli_routes_to_yolo_dataset_builder(self) -> None:
        config_path = self.root / "config.yaml"
        config_path.write_text(yaml.safe_dump({"paths": {"assets": str(self.assets)}}))

        with patch.object(run_module, "build_yolo_dataset", return_value={"ok": True}) as mocked:
            with patch.object(sys, "argv", ["run.py", "--build-yolo-dataset", self.game, "--config", str(config_path)]):
                run_module.main()

        mocked.assert_called_once()
        self.assertEqual(mocked.call_args[0][0], self.game)


if __name__ == "__main__":
    unittest.main()
