from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline import replay_viewer
from run import main as run_main
from run import run_render_replay_viewer


def _runtime_sidecar(*, source: Path, game: str = "marvel_rivals", schema_version: str = "runtime_analysis_v1") -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "analysis_id": "marvel_rivals-runtime-123abc",
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source.resolve()),
        "sidecar_path": "/tmp/runtime.runtime_analysis.json",
        "game_pack": {"game_id": game},
        "contract_summary": {"status": "canonical"},
        "matcher": {
            "status": "ok",
            "frame_count": 42,
            "sample_fps": 4.0,
            "template_count": 2,
            "summary": {"total_confirmed_detections": 1},
            "top_scores": {"hero_asset": 0.98},
            "unseen_templates": [],
            "confirmed_detections": [
                {
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.0,
                    "last_timestamp": 1.5,
                    "peak_score": 0.98,
                }
            ],
            "signals": [
                {
                    "signal_id": "signal-1",
                    "signal_type": "character_identity",
                    "start_timestamp": 1.0,
                    "end_timestamp": 1.5,
                    "confidence": 0.98,
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "entity_id": "punisher",
                }
            ],
        },
        "events": {
            "status": "ok",
            "signal_count": 1,
            "event_count": 1,
            "event_summary": {"event_types": {"pov_character_identified": 1}},
            "rows": [
                {
                    "event_id": "runtime-1",
                    "event_type": "pov_character_identified",
                    "start_timestamp": 1.0,
                    "end_timestamp": 1.5,
                    "confidence": 0.98,
                    "entity_id": "punisher",
                }
            ],
        },
        "runtime_review": {"review_status": "approved"},
    }


def _fused_sidecar(*, source: Path, game: str = "marvel_rivals", schema_version: str = "fused_analysis_v1") -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "fusion_id": "fusion-123abc",
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source.resolve()),
        "sidecar_path": "/tmp/fused.fused_analysis.json",
        "normalized_signals": [
            {
                "signal_id": "proxy-1",
                "signal_type": "chat_spike",
                "producer_family": "proxy",
                "start_timestamp": 1.8,
                "end_timestamp": 2.0,
            }
        ],
        "fused_events": [
            {
                "event_id": "fused-1",
                "event_type": "ability_plus_medal_combo",
                "start_timestamp": 1.0,
                "end_timestamp": 2.0,
                "confidence": 0.82,
                "final_score": 0.91,
                "gate_status": "confirmed",
                "synergy_applied": True,
                "synergy_multiplier": 1.15,
                "minimum_required_signals_met": True,
                "suggested_start_timestamp": 0.5,
                "suggested_end_timestamp": 3.0,
                "contributing_signals": ["signal-1", "proxy-1"],
                "metadata": {"entity_id": "punisher"},
            }
        ],
        "fusion_summary": {"event_count": 1},
        "rule_matches": [],
        "fused_review": {"events": {"fused-1": {"review_status": "approved"}}},
    }


class ReplayViewerTests(unittest.TestCase):
    def test_render_replay_viewer_writes_html_from_runtime_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")

            runtime_path = root / "runtime.runtime_analysis.json"
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_replay_viewer(runtime_path)

            self.assertTrue(result["ok"])
            self.assertTrue(result["media_embed_available"])
            viewer_path = Path(result["viewer_path"])
            self.assertTrue(viewer_path.is_file())
            html_text = viewer_path.read_text(encoding="utf-8")
            self.assertIn("Replay Viewer - marvel_rivals", html_text)
            self.assertIn("pov_character_identified", html_text)
            self.assertIn("character_identity", html_text)
            self.assertIn("Viewer Controls", html_text)
            self.assertIn("raw-detections", html_text)

    def test_render_replay_viewer_includes_fused_sidecar_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")

            runtime_path = root / "runtime.runtime_analysis.json"
            fused_path = root / "fused.fused_analysis.json"
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media), indent=2), encoding="utf-8")
            fused_path.write_text(json.dumps(_fused_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_replay_viewer(runtime_path, fused_sidecar=fused_path)

            self.assertTrue(result["ok"])
            self.assertEqual(result["fused_event_count"], 1)
            self.assertEqual(result["fused_group_count"], 1)
            self.assertGreaterEqual(result["linked_runtime_event_count"], 1)
            self.assertGreaterEqual(result["linked_detection_count"], 1)
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("ability_plus_medal_combo", html_text)
            self.assertIn("chat_spike", html_text)
            self.assertIn("confirmed", html_text)
            self.assertIn("Event Explorer", html_text)
            self.assertIn("Play Segment", html_text)
            self.assertIn("minimum_required_signals_met", html_text)
            self.assertIn("signal-1", html_text)
            self.assertIn("Jump To Event", html_text)
            self.assertIn("viewer-search", html_text)
            self.assertIn("data-jump-target", html_text)
            self.assertIn("Show JSON", html_text)

    def test_render_replay_viewer_rejects_invalid_runtime_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            runtime_path = root / "bad.runtime_analysis.json"
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media, schema_version="runtime_analysis_v0"), indent=2), encoding="utf-8")

            result = run_render_replay_viewer(runtime_path)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_runtime_sidecar")

    def test_render_replay_viewer_rejects_mismatched_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            runtime_path = root / "runtime.runtime_analysis.json"
            fused_path = root / "fused.fused_analysis.json"
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media, game="marvel_rivals"), indent=2), encoding="utf-8")
            fused_path.write_text(json.dumps(_fused_sidecar(source=media, game="overwatch"), indent=2), encoding="utf-8")

            result = run_render_replay_viewer(runtime_path, fused_sidecar=fused_path)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "mismatched_sidecars")

    def test_cli_routes_to_render_replay_viewer(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--render-replay-viewer", "/tmp/example.runtime_analysis.json"]
            with patch("run.run_render_replay_viewer", return_value={"ok": True, "viewer_path": "/tmp/viewer.html"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()
