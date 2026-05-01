from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.runtime_analysis import RuntimeAnalysisError, analyze_roi_runtime
from run import main as run_main
from run import run_analyze_roi_runtime


class RuntimeAnalysisTests(unittest.TestCase):
    def _write_published_pack(self, root: Path) -> None:
        game_root = root / "assets" / "games" / "marvel_rivals"
        (game_root / "manifests").mkdir(parents=True, exist_ok=True)
        (game_root / "game.yaml").write_text(
            "\n".join(
                [
                    "game_id: marvel_rivals",
                    'display_name: "Marvel Rivals"',
                    "resolution_profiles:",
                    '  normalize_to: "64x36"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "entities.yaml").write_text("heroes: []\nabilities: []\nevents: []\n", encoding="utf-8")
        (game_root / "hud.yaml").write_text(
            "\n".join(
                [
                    "rois:",
                    "  hero_portrait:",
                    "    x_pct: 0.0",
                    "    y_pct: 0.0",
                    "    w_pct: 0.5",
                    "    h_pct: 0.5",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "weights.yaml").write_text("weights: {}\nthresholds: {}\ngates: {}\n", encoding="utf-8")
        (game_root / "manifests" / "assets_manifest.json").write_text(
            json.dumps({"game_id": "marvel_rivals", "published_assets": []}, indent=2),
            encoding="utf-8",
        )
        (game_root / "manifests" / "cv_templates.yaml").write_text(
            "\n".join(
                [
                    "templates:",
                    "  - asset_id: marvel_rivals.punisher.hero_portrait",
                    "    asset_family: hero_portrait",
                    '    display_name: "Punisher"',
                    "    roi_ref: hero_portrait",
                    '    template_path: "templates/heroes/punisher.png"',
                    '    match_method: "TM_CCOEFF_NORMED"',
                    "    threshold: 0.9",
                    "    temporal_window: 3",
                    "    scale_set: [1.0]",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    def _matcher_result(self) -> dict[str, object]:
        return {
            "ok": True,
            "status": "ok",
            "game": "marvel_rivals",
            "source": "/tmp/example.mp4",
            "frame_count": 12,
            "sample_fps": 4.0,
            "template_count": 1,
            "summary": {
                "total_confirmed_detections": 1,
                "detections_by_roi": {"hero_portrait": 1},
                "detections_by_asset_family": {"hero_portrait": 1},
            },
            "top_scores": {"marvel_rivals.punisher.hero_portrait": 0.98},
            "unseen_templates": [],
            "confirmed_detections": [
                {
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.0,
                    "last_timestamp": 1.5,
                    "peak_score": 0.98,
                    "supporting_frames": 4,
                    "temporal_window": 3,
                }
            ],
        }

    def test_analyze_roi_runtime_writes_default_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            output_root = root / "outputs" / "runtime_analysis"
            self._write_published_pack(root)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.runtime_analysis.DEFAULT_OUTPUT_ROOT", output_root), patch(
                "pipeline.runtime_analysis.match_roi_templates",
                return_value=self._matcher_result(),
            ):
                result = analyze_roi_runtime("/tmp/example.mp4", "marvel_rivals")
            sidecar_path = Path(result["sidecar_path"])
            self.assertTrue(sidecar_path.exists())
            self.assertEqual(sidecar_path.parent, output_root / "marvel_rivals")
            payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "runtime_analysis_v1")
            self.assertIn("game_pack", payload)
            self.assertIn("matcher", payload)
            self.assertIn("events", payload)

    def test_analyze_roi_runtime_respects_explicit_output_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            output_path = root / "custom" / "analysis.json"
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch(
                "pipeline.runtime_analysis.match_roi_templates",
                return_value=self._matcher_result(),
            ):
                result = analyze_roi_runtime("/tmp/example.mp4", "marvel_rivals", output_path=output_path)
            self.assertEqual(Path(result["sidecar_path"]), output_path.resolve())
            self.assertTrue(output_path.exists())

    def test_analyze_roi_runtime_uses_matcher_report_without_rerunning_matcher(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            matcher_report = root / "matcher_report.json"
            matcher_report.write_text(json.dumps(self._matcher_result(), indent=2), encoding="utf-8")
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.runtime_analysis.match_roi_templates") as mock_match:
                result = analyze_roi_runtime("/tmp/example.mp4", "marvel_rivals", matcher_report=matcher_report)
            mock_match.assert_not_called()
            self.assertTrue(result["ok"])
            self.assertEqual(result["events"]["event_count"], 1)

    def test_run_analyze_roi_runtime_returns_structured_error(self) -> None:
        with patch("run.analyze_roi_runtime", side_effect=RuntimeAnalysisError("matcher_failed", "matcher failed")):
            result = run_analyze_roi_runtime("/tmp/example.mp4", "marvel_rivals")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "matcher_failed")

    def test_analyze_roi_runtime_failed_matcher_writes_no_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            output_root = root / "outputs" / "runtime_analysis"
            self._write_published_pack(root)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.runtime_analysis.DEFAULT_OUTPUT_ROOT", output_root), patch(
                "pipeline.runtime_analysis.match_roi_templates",
                side_effect=RoiMatcherFailure("decode_failed", "decode failed"),
            ):
                with self.assertRaises(RoiMatcherFailure):
                    analyze_roi_runtime("/tmp/example.mp4", "marvel_rivals")
            self.assertFalse(output_root.exists())

    def test_cli_routes_to_analyze_roi_runtime(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--analyze-roi-runtime", "/tmp/example.mp4", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_analyze_roi_runtime",
                return_value={"ok": True, "status": "ok", "sidecar_path": "/tmp/example.runtime_analysis.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv


class RoiMatcherFailure(Exception):
    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
