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
finally:
    os.chdir(_ORIGINAL_CWD)


class ReplayViewerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"

        self.processing_dir = self.root / "processing" / self.game
        self.inbox_dir = self.root / "inbox" / self.game
        self.quarantine_dir = self.root / "quarantine" / self.game
        self.assets_dir = self.root / "assets"
        self.game_pack_dir = self.assets_dir / "games" / self.game

        for path in (
            self.processing_dir,
            self.inbox_dir,
            self.quarantine_dir,
            self.game_pack_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        self.processing_clip = self.processing_dir / "queue_clip.mp4"
        self.processing_clip.write_bytes(b"queue-video")
        self.queue_meta = self.inbox_dir / "queue_clip.meta.json"
        self.queue_meta.write_text(json.dumps(self._meta_payload(str(self.processing_clip), "accept"), indent=2))

        self.quarantine_clip = self.quarantine_dir / "quarantine_clip.mp4"
        self.quarantine_clip.write_bytes(b"quarantine-video")
        self.quarantine_meta = self.quarantine_clip.with_suffix(".meta.json")
        self.quarantine_meta.write_text(json.dumps(self._meta_payload(str(self.quarantine_clip), "quarantine"), indent=2))

        self._write_game_pack()
        self.config = {
            "paths": {
                "assets": str(self.assets_dir),
                "processing": str(self.root / "processing"),
                "inbox": str(self.root / "inbox"),
                "quarantine": str(self.root / "quarantine"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "weapon_detector": {
                "enabled": True,
                "icon_dir": str(self.assets_dir / "weapon_icons"),
            },
            "kill_feed": {"enabled": True, "games": {self.game: {"roi": {"x": 1500, "y": 60, "w": 320, "h": 260}}}},
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
                    "kill_feed": {"enabled": True},
                    "yolo": {"enabled": True},
                },
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {
                    "hero_one": {"display_name": "Hero One"},
                },
                "aliases": {},
            },
            "hud.yaml": {
                "ui_version": "test",
                "rois": {
                    "weapon_detector": {"x": 0, "y": 960, "w": 160, "h": 100},
                    "kill_feed": {"x": 1500, "y": 60, "w": 320, "h": 260},
                },
                "detectors": {
                    "weapon_detector": {"roi_ref": "weapon_detector"},
                    "kill_feed": {"roi_ref": "kill_feed"},
                    "yolo": {"enabled": True, "labels": {"hero_label": "hero_one"}},
                },
            },
            "moments.yaml": {
                "moments": [{"id": "team_wipe_candidate", "display_name": "Team Wipe Candidate"}],
                "hook_targets": {"window_seconds": 1.5},
            },
            "weights.yaml": {
                "clip_judge": {
                    "thresholds": {"accept": 0.7, "quarantine": 0.45, "reject": 0.25},
                    "quarantine_reasons": ["missing_context", "hook_not_resolved", "low_confidence", "ui_drift", "needs_roi_template"],
                }
            },
        }
        for filename, payload in files.items():
            (self.game_pack_dir / filename).write_text(yaml.safe_dump(payload))

    def _meta_payload(self, clip_path: str, status: str) -> dict:
        return {
            "clip_id": Path(clip_path).stem,
            "game": self.game,
            "clip_path": clip_path,
            "processed_path": clip_path,
            "duration_seconds": 12.4,
            "audio_events": {
                "spike_timestamps": [1.2, 3.7],
                "events": [{"type": "multi_kill", "timestamp": 1.2, "spike_count": 3}],
            },
            "kill_feed": {
                "kill_timestamps": [2.0],
                "headshot_timestamps": [2.5],
                "kill_count": 1,
                "headshot_count": 1,
                "sweat_score": 60,
                "method": "color_mask",
            },
            "weapon_detection": {
                "weapon_id": "hero_one",
                "display_name": "Hero One",
                "confidence": 0.91,
                "method": "template_match",
                "frame_time": 4.2,
            },
            "niceshot_detection": {
                "status": "ok",
                "moments": [{"timestamp": 2.1, "kind": "ultimate_swing", "confidence": 0.82, "hook_candidate": True}],
                "normalized_scores": {"composite": 0.76},
            },
            "yolo_detection": {
                "status": "ok",
                "detections": [{
                    "label": "hero_label",
                    "kind": "entity",
                    "maps_to": "hero_one",
                    "confidence": 0.88,
                    "box": [100, 120, 220, 280],
                    "timestamp": 0.0,
                }],
                "event_candidates": [{
                    "event_id": "team_wipe_candidate",
                    "label": "team_wipe_candidate",
                    "confidence": 0.8,
                    "timestamp": 2.2,
                    "box": [300, 330, 430, 470],
                }],
                "context_confidence": 0.8,
            },
            "hook_enforcer": {
                "status": "ok",
                "early_hook_passed": False,
                "hook_score": 0.72,
                "window_seconds": 1.5,
                "anchor_moment": {"timestamp": 4.8, "source": "kill_feed", "kind": "headshot", "confidence": 0.92},
                "trim_plan": {
                    "strategy": "hard_trim",
                    "trim_start_seconds": 4.3,
                    "expected_hook_timestamp": 0.5,
                    "pre_event_padding_seconds": 0.5,
                },
                "retention_flags": {"max_gap_between_moments_seconds": 2.5, "dead_air_risk": False},
                "explanation": ["Late hook can be aligned with hard trim."],
            },
            "context": {
                "player_entity": "hero_one",
                "player_entity_name": "Hero One",
                "detected_event": "team_wipe_candidate",
                "context_confidence": 0.91,
            },
            "decision": {
                "status": status,
                "composite_score": 0.74,
                "hook_gate_passed": True,
                "original_hook_timestamp": 4.8,
                "expected_final_hook_timestamp": 0.5,
                "hook_alignment": {
                    "mode": "hard_trim",
                    "original_hook_timestamp": 4.8,
                    "expected_hook_timestamp": 0.5,
                    "trim_plan": {"strategy": "hard_trim", "trim_start_seconds": 4.3},
                },
                "explanation": ["Composite score 0.74 with good context."],
            },
            "quarantine": {"reason": "missing_context"} if status == "quarantine" else {},
            "title_engine": {
                "title": "Hero One Turns The Fight",
                "caption": "A tight recovery with the hook pulled into the first second.",
            },
        }

    def test_replay_queue_route_loads(self) -> None:
        response = self.client.get(f"/replay/queue/{self.game}/queue_clip")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Replay Viewer", html)
        self.assertIn("Hero One Turns The Fight", html)
        self.assertIn("HUD ROIs", html)
        self.assertIn("YOLO Boxes", html)
        self.assertIn("Hook And Trim", html)
        self.assertIn("Replay is read-only for queue clips", html)

    def test_replay_quarantine_route_loads(self) -> None:
        response = self.client.get(f"/replay/quarantine/{self.game}/quarantine_clip")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("missing_context", html)
        self.assertIn("Late hook can be aligned with hard trim.", html)
        self.assertIn("multi_kill", html)
        self.assertIn("Open ROI Repair", html)

    def test_replay_missing_clip_404s(self) -> None:
        self.assertEqual(self.client.get(f"/replay/queue/{self.game}/missing").status_code, 404)
        self.assertEqual(self.client.get(f"/replay/quarantine/{self.game}/missing").status_code, 404)
