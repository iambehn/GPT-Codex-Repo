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
    from pipeline.clip_judge import evaluate as evaluate_clip
    from pipeline.game_pack import load_game_pack
    from pipeline.hook_enforcer import run_hook_enforcer
    from pipeline.processing import _build_ffmpeg_cmd, _effective_timing, _shift_srt_timestamps
finally:
    os.chdir(_ORIGINAL_CWD)


class HookEnforcerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.game = "test_game"
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / self.game
        self.pack_dir.mkdir(parents=True)
        self.clip = self.root / "clip.mp4"
        self.clip.write_bytes(b"fake video")
        self.meta_path = self.clip.with_suffix(".meta.json")
        self.config = {
            "paths": {
                "assets": str(self.assets),
                "processing": str(self.root / "processing"),
                "quarantine": str(self.root / "quarantine"),
                "inbox": str(self.root / "inbox"),
                "rejected": str(self.root / "rejected"),
            },
            "games": {self.game: {"display_name": "Test Game"}},
            "clip_judge": {"enabled": False},
        }
        self.pack = self._write_pack()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_pack(self, required_context_fields: list[str] | None = None) -> dict:
        if required_context_fields is None:
            required_context_fields = ["player_entity", "detected_event"]
        files = {
            "game.yaml": {
                "game_id": self.game,
                "display_name": "Test Game",
                "genre": "hero_shooter",
                "ui_version": "test",
            },
            "entities.yaml": {
                "primary_kind": "heroes",
                "heroes": {"hero_one": {"display_name": "Hero One"}},
            },
            "moments.yaml": {"moments": [{"id": "precision_pick"}]},
            "hud.yaml": {"ui_version": "test", "rois": {}, "detectors": {}},
            "weights.yaml": {
                "clip_judge": {
                    "thresholds": {"accept": 0.1, "quarantine": 0.45, "reject": 0.25},
                    "hard_gates": {
                        "hook_window_seconds": 1.5,
                        "require_context_fields": required_context_fields,
                    },
                    "hook_enforcer": {
                        "window_seconds": 1.5,
                        "acceptance_threshold": 0.5,
                        "pre_event_padding_seconds": 0.5,
                        "minimum_remaining_seconds": 6.0,
                        "signal_weights": {
                            "kill_feed": 0.45,
                            "niceshot": 0.25,
                            "yolo": 0.20,
                            "audio": 0.10,
                        },
                    },
                }
            },
        }
        for filename, payload in files.items():
            (self.pack_dir / filename).write_text(yaml.safe_dump(payload))
        return load_game_pack(self.game, self.config)

    def _write_meta(self, payload: dict) -> None:
        base = {
            "clip_id": "clip",
            "game": self.game,
            "clip_path": str(self.clip),
            "duration_seconds": 20.0,
            "weapon_detection": {"weapon_id": "hero_one", "confidence": 0.9, "method": "template_match"},
        }
        base.update(payload)
        self.meta_path.write_text(json.dumps(base))

    def test_early_kill_passes_without_trim(self) -> None:
        self._write_meta({"kill_feed": {"kill_timestamps": [1.0], "kill_count": 1}})

        result = run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        self.assertTrue(result["early_hook_passed"])
        self.assertEqual(result["trim_plan"]["strategy"], "none")
        self.assertEqual(result["anchor_moment"]["source"], "kill_feed")

    def test_late_headshot_creates_hard_trim_plan(self) -> None:
        self._write_meta({"kill_feed": {"headshot_timestamps": [4.8], "headshot_count": 1}})

        result = run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        self.assertFalse(result["early_hook_passed"])
        self.assertEqual(result["trim_plan"]["strategy"], "hard_trim")
        self.assertEqual(result["trim_plan"]["trim_start_seconds"], 4.3)
        self.assertEqual(result["trim_plan"]["expected_hook_timestamp"], 0.5)

    def test_audio_only_does_not_pass_default_hook_gate(self) -> None:
        self._write_meta({"audio_events": {"spike_timestamps": [0.2], "spike_count": 1}})

        result = run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        self.assertFalse(result["early_hook_passed"])
        self.assertEqual(result["hook_score"], 0.055)
        self.assertEqual(result["trim_plan"]["strategy"], "unresolved")

    def test_niceshot_and_yolo_can_anchor_late_trim(self) -> None:
        self._write_meta({
            "niceshot_detection": {
                "status": "ok",
                "confidence": 0.9,
                "moments": [{"timestamp": 3.2, "kind": "ultimate_swing", "confidence": 0.9}],
            },
            "yolo_detection": {
                "status": "ok",
                "event_candidates": [{"timestamp": 5.0, "event_id": "precision_pick", "confidence": 0.88}],
            },
        })

        result = run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        self.assertEqual(result["trim_plan"]["strategy"], "hard_trim")
        self.assertEqual(result["anchor_moment"]["source"], "niceshot")
        self.assertEqual(result["trim_plan"]["trim_start_seconds"], 2.7)

    def test_no_candidate_moments_are_unresolved(self) -> None:
        self._write_meta({})

        result = run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        self.assertFalse(result["early_hook_passed"])
        self.assertEqual(result["trim_plan"]["strategy"], "unresolved")
        self.assertIsNone(result["anchor_moment"])

    def test_existing_manifest_is_reused_unless_forced(self) -> None:
        self._write_meta({"kill_feed": {"headshot_timestamps": [4.8], "headshot_count": 1}})
        first = run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        meta = json.loads(self.meta_path.read_text())
        meta["kill_feed"] = {"headshot_timestamps": [1.0], "headshot_count": 1}
        self.meta_path.write_text(json.dumps(meta))
        cached = run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=False)

        self.assertEqual(cached["trim_plan"], first["trim_plan"])

    def test_clip_judge_accepts_valid_hard_trim_hook(self) -> None:
        self._write_meta({
            "kill_feed": {
                "headshot_timestamps": [4.8],
                "headshot_count": 1,
                "sweat_score": 80,
            },
        })
        run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        result = evaluate_clip(self.clip, self.pack, self.config, force=True)

        self.assertTrue(result["decision"]["hook_gate_passed"])
        self.assertEqual(result["decision"]["hook_alignment"]["mode"], "hard_trim")
        self.assertEqual(result["decision"]["original_hook_timestamp"], 4.8)
        self.assertEqual(result["decision"]["expected_final_hook_timestamp"], 0.5)
        self.assertNotEqual((result.get("quarantine") or {}).get("reason"), "hook_not_resolved")

    def test_clip_judge_quarantines_unresolved_hook(self) -> None:
        self.pack = self._write_pack(required_context_fields=[])
        self._write_meta({})
        run_hook_enforcer(self.clip, self.game, self.config, self.pack, force=True)

        result = evaluate_clip(self.clip, self.pack, self.config, force=True)

        self.assertFalse(result["decision"]["hook_gate_passed"])
        self.assertEqual(result["quarantine"]["reason"], "hook_not_resolved")


class ProcessingHookTrimTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _template(self) -> dict:
        return {
            "template_id": "fast_hype",
            "timeline": {"target_duration_seconds": 75.0, "target_duration_strategy": "trim_end"},
            "captions": {"enabled": False, "source": "none"},
            "visual_effects": {
                "vertical_fill": {"method": "center_crop"},
                "zoom": {"enabled": False, "type": "none", "intensity": 1.0, "duration_seconds": 0},
                "color_grade": {"enabled": False},
                "vignette": {"enabled": False},
                "film_grain": {"enabled": False},
                "chromatic_aberration": {"enabled": False},
            },
            "audio": {
                "background_music": {"enabled": False},
                "original_audio": {"enabled": True, "volume_db": 0.0, "normalize": False},
            },
            "output": {
                "resolution": {"width": 1080, "height": 1920},
                "fps": 30,
                "codec": "h264",
                "bitrate_kbps": 6000,
            },
        }

    def _metadata(self) -> dict:
        return {
            "duration_seconds": 20.0,
            "has_audio": True,
            "hook_enforcer": {
                "trim_plan": {
                    "strategy": "hard_trim",
                    "trim_start_seconds": 4.3,
                    "expected_hook_timestamp": 0.5,
                }
            },
        }

    def test_ffmpeg_command_applies_video_and_audio_source_trim(self) -> None:
        clip = self.root / "clip.mp4"
        output = self.root / "out.mp4"
        cmd = _build_ffmpeg_cmd(clip, output, self._template(), self._metadata(), str(self.root))
        filter_graph = cmd[cmd.index("-filter_complex") + 1]

        self.assertIn("[0:v]trim=start=4.300", filter_graph)
        self.assertIn("[0:a]atrim=start=4.300", filter_graph)
        self.assertNotIn("trim=end=75.0", filter_graph)

    def test_effective_timing_uses_remaining_duration_after_trim(self) -> None:
        trim_start, target_duration, remaining_duration = _effective_timing(self._metadata(), 75.0)

        self.assertEqual(trim_start, 4.3)
        self.assertAlmostEqual(target_duration, 15.7)
        self.assertAlmostEqual(remaining_duration, 15.7)

    def test_srt_timestamps_shift_and_drop_fully_trimmed_blocks(self) -> None:
        srt = (
            "1\n00:00:00,000 --> 00:00:02,000\nold intro\n\n"
            "2\n00:00:04,000 --> 00:00:05,000\npartial hook\n\n"
            "3\n00:00:06,000 --> 00:00:08,000\npost hook\n"
        )

        shifted = _shift_srt_timestamps(srt, 4.3)

        self.assertNotIn("old intro", shifted)
        self.assertIn("1\n00:00:00,000 --> 00:00:00,700\npartial hook", shifted)
        self.assertIn("2\n00:00:01,700 --> 00:00:03,700\npost hook", shifted)


if __name__ == "__main__":
    unittest.main()
