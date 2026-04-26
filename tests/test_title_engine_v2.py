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
    from pipeline.publishing import publishing_caption, publishing_title
    from pipeline.title_engine import generate_title
finally:
    os.chdir(_ORIGINAL_CWD)


class TitleEngineV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.pack_dir.mkdir(parents=True)
        self.titles_path = self.assets / "titles.yaml"
        self.history_path = self.assets / "title_history.json"
        self.config = {
            "paths": {"assets": str(self.assets)},
            "games": {self.game: {"display_name": "Test Game"}},
            "title_engine": {
                "enabled": True,
                "titles_path": str(self.titles_path),
                "history_path": str(self.history_path),
                "history_window": 5,
            },
        }
        self._write_game_pack()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_game_pack(self) -> None:
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
            "moments.yaml": {"moments": [{"id": "multi_kill_swing"}, {"id": "ultimate_swing"}]},
            "hud.yaml": {"ui_version": "test", "rois": {}, "detectors": {}},
            "weights.yaml": {"clip_judge": {"thresholds": {"accept": 0.7}}},
        }
        for filename, payload in files.items():
            (self.pack_dir / filename).write_text(yaml.safe_dump(payload))

    def _clip_with_meta(self, name: str, meta: dict) -> Path:
        clip = self.root / f"{name}.mp4"
        clip.write_bytes(b"fake video")
        clip.with_suffix(".meta.json").write_text(json.dumps(meta))
        return clip

    def _write_titles(self, payload: dict) -> None:
        self.titles_path.write_text(yaml.safe_dump(payload))

    def test_opencv_entity_detection_drives_structured_title(self) -> None:
        self._write_titles({
            self.game: {
                "performance_hype": [
                    "{subject} {action} {stakes}",
                ]
            }
        })
        clip = self._clip_with_meta("opencv", {
            "weapon_detection": {
                "weapon_id": "hero_one",
                "display_name": "Hero One",
                "confidence": 0.91,
                "method": "template_match",
            },
            "kill_feed": {"kill_count": 3, "headshot_count": 1},
            "decision": {"hook_gate_passed": True, "top_hook_timestamp": 0.6, "composite_score": 0.82},
        })

        result = generate_title(clip, self.game, self.config)

        self.assertEqual(result["fallback_level"], "game_template")
        self.assertIn("Hero One", result["title"])
        self.assertIn("inside the first second", result["title"])
        self.assertEqual(result["variables"]["weapon"], "Hero One")
        self.assertIn("#HeroOne", result["hashtags"])
        self.assertIn(result["title"], result["caption"])

    def test_yolo_entity_fills_subject_when_opencv_is_unresolved(self) -> None:
        self._write_titles({
            self.game: {
                "reactionary_shock": ["{subject} {action} in {game}"],
            }
        })
        clip = self._clip_with_meta("yolo", {
            "weapon_detection": {"weapon_id": None, "confidence": 0.0, "method": "no_match"},
            "context": {"detected_event": "precision_pick", "confidence": 0.5},
            "decision": {"hook_gate_passed": True, "top_hook_timestamp": 1.2, "composite_score": 0.71},
            "yolo_detection": {
                "status": "ok",
                "top_entity": {"entity_id": "hero_two", "confidence": 0.93},
            },
        })

        result = generate_title(clip, self.game, self.config)

        self.assertEqual(result["fallback_level"], "game_template")
        self.assertIn("Hero Two", result["title"])
        self.assertEqual(result["variables"]["entity_id"], "hero_two")
        self.assertTrue(any("yolo" in item for item in result["explanation"]))

    def test_niceshot_and_candidate_moments_shape_action_phrase(self) -> None:
        self._write_titles({
            "generic": {
                "performance_hype": ["{subject} {action} from {moment}"],
            }
        })
        clip = self._clip_with_meta("niceshot", {
            "niceshot_detection": {
                "status": "ok",
                "action_score": 0.9,
                "hook_score": 0.8,
                "confidence": 0.85,
                "moments": [{"kind": "ultimate_swing", "confidence": 0.9, "source": "niceshot"}],
            },
            "candidate_moments": [{"kind": "ultimate_swing", "confidence": 0.75, "source": "clip_judge"}],
            "decision": {"hook_gate_passed": True, "top_hook_timestamp": 0.8, "composite_score": 0.78},
        })

        result = generate_title(clip, self.game, self.config)

        self.assertEqual(result["fallback_level"], "generic_template")
        self.assertIn("ultimate", result["title"].lower())
        self.assertEqual(result["variables"]["subject"], "this play")
        self.assertTrue(any("Top moment source" in item for item in result["explanation"]))

    def test_missing_entity_uses_safe_generated_fallback_when_templates_are_absent(self) -> None:
        self._write_titles({})
        clip = self._clip_with_meta("missing", {})

        result = generate_title(clip, self.game, self.config)

        self.assertEqual(result["fallback_level"], "generated")
        self.assertEqual(result["title"], "Test Game clip")
        self.assertEqual(result["variables"]["subject"], "this play")

    def test_history_avoids_reusing_recent_template_and_final_title(self) -> None:
        self._write_titles({
            self.game: {
                "performance_hype": [
                    "{subject} first title",
                    "{subject} second title",
                ]
            }
        })
        meta = {
            "weapon_detection": {
                "weapon_id": "hero_one",
                "display_name": "Hero One",
                "confidence": 0.9,
                "method": "template_match",
            },
            "kill_feed": {"kill_count": 3},
        }

        first = generate_title(self._clip_with_meta("history_one", meta), self.game, self.config)
        second = generate_title(self._clip_with_meta("history_two", meta), self.game, self.config)

        self.assertEqual(first["title"], "Hero One first title")
        self.assertEqual(second["title"], "Hero One second title")

    def test_publishing_helpers_prefer_title_engine_then_scoring_then_clip_id(self) -> None:
        metadata = {
            "clip_id": "clip-123",
            "title_engine": {
                "title": "Engine title",
                "caption": "Engine caption",
                "hashtags": ["#One"],
            },
            "scoring": {
                "suggested_title": "Scoring title",
                "suggested_caption": "Scoring caption",
            },
        }
        self.assertEqual(publishing_title(metadata), "Engine title")
        self.assertEqual(publishing_caption(metadata), "Engine caption")

        metadata["title_engine"] = {}
        self.assertEqual(publishing_title(metadata), "Scoring title")
        self.assertEqual(publishing_caption(metadata), "Scoring caption")

        metadata["scoring"] = {}
        self.assertEqual(publishing_title(metadata), "clip-123")
        self.assertIn("clip-123", publishing_caption(metadata))


if __name__ == "__main__":
    unittest.main()
