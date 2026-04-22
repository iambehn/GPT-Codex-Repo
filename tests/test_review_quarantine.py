from __future__ import annotations

import base64
import binascii
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

# Importing the app initializes the project logger. Keep that log file out of
# the repo so route tests do not depend on local write permissions.
_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    from pipeline.review import app as review_app
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


class QuarantineReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"

        self.quarantine_dir = self.root / "quarantine" / self.game
        self.inbox_dir = self.root / "inbox" / self.game
        self.processing_dir = self.root / "processing" / self.game
        self.assets_dir = self.root / "assets"
        self.icon_dir = self.assets_dir / "weapon_icons" / self.game
        self.game_pack_dir = self.assets_dir / "games" / self.game

        for path in (
            self.quarantine_dir,
            self.inbox_dir,
            self.processing_dir,
            self.icon_dir,
            self.game_pack_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        self.clip_path = self.quarantine_dir / "clip.mp4"
        self.clip_path.write_bytes(b"not-a-real-video")
        self.meta_path = self.quarantine_dir / "clip.meta.json"
        self.meta_path.write_text(json.dumps({
            "clip_id": "clip",
            "game": self.game,
            "clip_path": str(self.clip_path),
            "status": "quarantine",
            "quarantine": {"reason": "missing_context"},
        }))

        self._write_game_pack()
        self.config = {
            "paths": {
                "assets": str(self.assets_dir),
                "quarantine": str(self.root / "quarantine"),
                "inbox": str(self.root / "inbox"),
                "processing": str(self.root / "processing"),
            },
            "games": {
                self.game: {"display_name": "Test Game"},
            },
            "weapon_detector": {
                "enabled": True,
                "confidence_threshold": 0.8,
                "frame_sample": "middle",
                "icon_dir": str(self.assets_dir / "weapon_icons"),
            },
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
                "detectors": {"weapon_detector": {"enabled": True}},
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {
                    "hero_one": {"display_name": "Hero One"},
                    "hero_two": {"display_name": "Hero Two"},
                },
                "aliases": {},
            },
            "hud.yaml": {
                "ui_version": "test",
                "rois": {"weapon_detector": {"x": 0, "y": 0, "w": 64, "h": 64}},
                "detectors": {
                    "weapon_detector": {
                        "roi_ref": "weapon_detector",
                        "icon_dir": str(self.icon_dir),
                    }
                },
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

    def _save_payload(self, entity_id: str = "hero_one", overwrite: bool = False) -> dict:
        return {
            "game": self.game,
            "clip_stem": "clip",
            "entity_id": entity_id,
            "image_b64": base64.b64encode(_fake_png()).decode("ascii"),
            "crop_box": {"x": 1, "y": 2, "w": 16, "h": 16},
            "frame_time_seconds": 3.5,
            "overwrite": overwrite,
        }

    def _mock_rescan_dependencies(self, status: str = "quarantine"):
        return patch.multiple(
            review_app,
            run_weapon_detector=unittest.mock.DEFAULT,
            evaluate_clip=unittest.mock.DEFAULT,
        )

    def test_quarantine_routes_and_roster_load(self) -> None:
        self.assertEqual(self.client.get("/quarantine").status_code, 200)
        self.assertEqual(self.client.get(f"/quarantine/{self.game}/clip").status_code, 200)

        video = self.client.get(f"/quarantine/video/{self.game}/clip.mp4")
        self.assertEqual(video.status_code, 200)
        self.assertEqual(self.client.get(f"/quarantine/video/{self.game}/../config.yaml").status_code, 404)

        roster = self.client.get(f"/api/quarantine/roster/{self.game}")
        self.assertEqual(roster.status_code, 200)
        body = roster.get_json()
        self.assertEqual(body["primary_kind"], "heroes")
        self.assertEqual({entity["entity_id"] for entity in body["entities"]}, {"hero_one", "hero_two"})

    def test_queue_and_scout_pages_still_load(self) -> None:
        self.assertEqual(self.client.get("/").status_code, 200)
        with patch("pipeline.scout.tracker.load_cache", return_value={"last_poll": None, "games": {}}):
            self.assertEqual(self.client.get("/scout").status_code, 200)

    def test_save_icon_rejects_unknown_entity(self) -> None:
        response = self.client.post("/api/quarantine/save-icon", json=self._save_payload(entity_id="missing"))
        self.assertEqual(response.status_code, 400)
        self.assertIn("Unknown entity_id", response.get_json()["error"])

    def test_save_icon_rejects_malformed_png_and_bad_crop(self) -> None:
        malformed = self._save_payload()
        malformed["image_b64"] = base64.b64encode(b"not-png").decode("ascii")
        self.assertEqual(self.client.post("/api/quarantine/save-icon", json=malformed).status_code, 400)

        tiny = self._save_payload()
        tiny["crop_box"]["w"] = 1
        self.assertEqual(self.client.post("/api/quarantine/save-icon", json=tiny).status_code, 400)

        mismatch = self._save_payload()
        mismatch["crop_box"]["w"] = 20
        self.assertEqual(self.client.post("/api/quarantine/save-icon", json=mismatch).status_code, 400)

    def test_save_icon_creates_asset_and_audit_entry(self) -> None:
        with self._mock_rescan_dependencies() as mocks:
            mocks["run_weapon_detector"].return_value = {"weapon_id": "hero_one", "confidence": 0.91}
            mocks["evaluate_clip"].return_value = {
                "decision": {"status": "quarantine", "composite_score": 0.4},
                "quarantine": {"reason": "hook_not_resolved"},
            }
            response = self.client.post("/api/quarantine/save-icon", json=self._save_payload())

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["asset_path"], f"assets/weapon_icons/{self.game}/hero_one.png")
        self.assertTrue((self.icon_dir / "hero_one.png").exists())

        meta = json.loads(self.meta_path.read_text())
        self.assertEqual(meta["asset_training"][-1]["entity_id"], "hero_one")
        self.assertEqual(meta["asset_training"][-1]["crop_box"]["w"], 16)

    def test_existing_icon_requires_overwrite_and_backup_is_recorded(self) -> None:
        asset_path = self.icon_dir / "hero_one.png"
        asset_path.write_bytes(b"old-icon")

        blocked = self.client.post("/api/quarantine/save-icon", json=self._save_payload())
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(asset_path.read_bytes(), b"old-icon")

        with self._mock_rescan_dependencies() as mocks:
            mocks["run_weapon_detector"].return_value = {"weapon_id": "hero_one", "confidence": 0.91}
            mocks["evaluate_clip"].return_value = {
                "decision": {"status": "quarantine", "composite_score": 0.4},
                "quarantine": {"reason": "hook_not_resolved"},
            }
            replaced = self.client.post(
                "/api/quarantine/save-icon",
                json=self._save_payload(overwrite=True),
            )

        self.assertEqual(replaced.status_code, 200)
        backup_path = self.root / replaced.get_json()["backup_path"]
        self.assertEqual(backup_path.read_bytes(), b"old-icon")

        meta = json.loads(self.meta_path.read_text())
        self.assertEqual(meta["asset_training"][-1]["backup_path"], replaced.get_json()["backup_path"])

    def test_rescan_accept_moves_clip_and_sidecar_to_inbox(self) -> None:
        with self._mock_rescan_dependencies() as mocks:
            mocks["run_weapon_detector"].return_value = {"weapon_id": "hero_one", "confidence": 0.91}
            mocks["evaluate_clip"].return_value = {
                "decision": {"status": "accept", "composite_score": 0.8},
                "quarantine": {},
            }
            response = self.client.post(
                "/api/quarantine/rescan",
                json={"game": self.game, "clip_stem": "clip"},
            )

        self.assertEqual(response.status_code, 200)
        rescan = response.get_json()["rescan"]
        self.assertTrue(rescan["moved_to_inbox"])
        self.assertTrue((self.inbox_dir / "clip.mp4").exists())
        self.assertTrue((self.inbox_dir / "clip.meta.json").exists())
        self.assertFalse(self.clip_path.exists())

        meta = json.loads((self.inbox_dir / "clip.meta.json").read_text())
        self.assertEqual(meta["status"], "recovered_from_quarantine")


if __name__ == "__main__":
    unittest.main()
