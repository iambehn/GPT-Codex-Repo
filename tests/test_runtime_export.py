from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline import runtime_export
from run import main as run_main
from run import run_export_runtime_analysis


def _runtime_sidecar(
    *,
    analysis_id: str,
    game: str,
    source: str,
    events: list[dict[str, object]],
    detections: list[dict[str, object]],
    ok: bool = True,
    schema_version: str = "runtime_analysis_v1",
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "analysis_id": analysis_id,
        "ok": ok,
        "status": "ok" if ok else "failed",
        "game": game,
        "source": source,
        "sidecar_path": f"/tmp/{analysis_id}.runtime_analysis.json",
        "game_pack": {"game_id": game, "pack_format": "published"},
        "matcher": {
            "status": "ok",
            "frame_count": 12,
            "sample_fps": 4.0,
            "template_count": 3,
            "summary": {
                "total_confirmed_detections": len(detections),
                "detections_by_roi": {},
                "detections_by_asset_family": {},
            },
            "top_scores": {},
            "unseen_templates": [],
            "confirmed_detections": detections,
        },
        "events": {
            "status": "ok",
            "event_count": len(events),
            "event_summary": {},
            "rows": events,
        },
    }


def _event(
    *,
    event_id: str,
    event_type: str,
    asset_id: str,
    roi_ref: str,
    confidence: float = 0.95,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": 1.25,
        "start_timestamp": 1.0,
        "end_timestamp": 1.5,
        "asset_id": asset_id,
        "roi_ref": roi_ref,
        "confidence": confidence,
        "evidence": {"peak_score": confidence},
        "source_detection_count": 3,
    }


def _detection(*, asset_id: str, roi_ref: str, peak_score: float = 0.98) -> dict[str, object]:
    return {
        "asset_id": asset_id,
        "roi_ref": roi_ref,
        "first_timestamp": 1.0,
        "last_timestamp": 1.5,
        "peak_score": peak_score,
        "supporting_frames": 4,
        "temporal_window": 3,
    }


class RuntimeExportTests(unittest.TestCase):
    def test_export_runtime_analysis_writes_clip_event_and_detection_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as export_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "marvel_rivals" / "alpha.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="analysis-a",
                    game="marvel_rivals",
                    source="fixture-a",
                    events=[_event(event_id="e1", event_type="medal_seen", asset_id="a1", roi_ref="medal_area")],
                    detections=[_detection(asset_id="a1", roi_ref="medal_area")],
                ),
            )
            self._write_sidecar(
                root / "bad" / "failed.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="analysis-b",
                    game="marvel_rivals",
                    source="fixture-b",
                    events=[],
                    detections=[],
                    ok=False,
                ),
            )
            malformed_path = root / "bad" / "malformed.runtime_analysis.json"
            malformed_path.parent.mkdir(parents=True, exist_ok=True)
            malformed_path.write_text("{not-json", encoding="utf-8")

            with patch.object(runtime_export, "DEFAULT_OUTPUT_ROOT", Path(export_root)):
                result = run_export_runtime_analysis(root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["clip_row_count"], 1)
            self.assertEqual(result["event_row_count"], 1)
            self.assertEqual(result["detection_row_count"], 1)
            self.assertEqual(result["scanned_sidecar_count"], 3)
            self.assertEqual(result["exported_sidecar_count"], 1)
            self.assertEqual(result["skipped_sidecar_count"], 2)

            clips_jsonl = Path(result["clips_jsonl_path"])
            events_jsonl = Path(result["events_jsonl_path"])
            detections_jsonl = Path(result["detections_jsonl_path"])
            manifest_path = Path(result["manifest_path"])
            self.assertTrue(clips_jsonl.is_file())
            self.assertTrue(events_jsonl.is_file())
            self.assertTrue(detections_jsonl.is_file())
            self.assertTrue(manifest_path.is_file())

            clip_rows = [json.loads(line) for line in clips_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(clip_rows), 1)
            self.assertIn("highlight_score", clip_rows[0])
            self.assertIn("recommended_action", clip_rows[0])
            self.assertIn("score_breakdown", clip_rows[0])
            self.assertIn("score_reasoning", clip_rows[0])

            event_rows = [json.loads(line) for line in events_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(event_rows), 1)
            self.assertEqual(event_rows[0]["recommended_action"], clip_rows[0]["recommended_action"])

            detection_rows = [
                json.loads(line) for line in detections_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()
            ]
            self.assertEqual(len(detection_rows), 1)
            self.assertEqual(detection_rows[0]["recommended_action"], clip_rows[0]["recommended_action"])

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["clip_row_count"], 1)
            self.assertEqual(manifest["event_row_count"], 1)
            self.assertEqual(manifest["detection_row_count"], 1)
            self.assertEqual(manifest["skipped_failed_analysis_count"], 1)
            self.assertEqual(manifest["skipped_malformed_count"], 1)

    def test_export_runtime_analysis_game_filter_limits_rows(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as export_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "marvel" / "marvel.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="analysis-marvel",
                    game="marvel_rivals",
                    source="fixture-a",
                    events=[],
                    detections=[],
                ),
            )
            self._write_sidecar(
                root / "cod" / "cod.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="analysis-cod",
                    game="call_of_duty",
                    source="fixture-b",
                    events=[],
                    detections=[],
                ),
            )

            with patch.object(runtime_export, "DEFAULT_OUTPUT_ROOT", Path(export_root)):
                result = run_export_runtime_analysis(root, game="marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["clip_row_count"], 1)
            self.assertEqual(result["event_row_count"], 0)
            self.assertEqual(result["detection_row_count"], 0)
            self.assertEqual(result["skipped_sidecar_count"], 1)

    def test_clip_rows_export_when_events_and_detections_are_empty(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as export_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "marvel_rivals" / "alpha.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="analysis-a",
                    game="marvel_rivals",
                    source="fixture-a",
                    events=[],
                    detections=[],
                ),
            )

            with patch.object(runtime_export, "DEFAULT_OUTPUT_ROOT", Path(export_root)):
                result = run_export_runtime_analysis(root)

            self.assertEqual(result["clip_row_count"], 1)
            self.assertEqual(result["event_row_count"], 0)
            self.assertEqual(result["detection_row_count"], 0)
            clip_row = json.loads(Path(result["clips_jsonl_path"]).read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(clip_row["recommended_action"], "skip")

    def test_medal_heavy_sidecar_outranks_identity_only_sidecar(self) -> None:
        medal_sidecar = _runtime_sidecar(
            analysis_id="analysis-medal",
            game="marvel_rivals",
            source="fixture-medal",
            events=[
                _event(event_id="m1", event_type="medal_seen", asset_id="m1", roi_ref="medal_area"),
                _event(event_id="m2", event_type="medal_seen", asset_id="m2", roi_ref="medal_area"),
            ],
            detections=[_detection(asset_id="m1", roi_ref="medal_area")],
        )
        identity_sidecar = _runtime_sidecar(
            analysis_id="analysis-identity",
            game="marvel_rivals",
            source="fixture-identity",
            events=[
                _event(
                    event_id="p1",
                    event_type="pov_character_identified",
                    asset_id="p1",
                    roi_ref="hero_portrait",
                )
            ],
            detections=[_detection(asset_id="p1", roi_ref="hero_portrait")],
        )

        medal_row, _, _ = runtime_export._build_rows_for_sidecar(Path("/tmp/a.runtime_analysis.json"), medal_sidecar, "ds", runtime_export.DEFAULT_SCORING_CONFIG)
        identity_row, _, _ = runtime_export._build_rows_for_sidecar(Path("/tmp/b.runtime_analysis.json"), identity_sidecar, "ds", runtime_export.DEFAULT_SCORING_CONFIG)
        self.assertGreater(medal_row["highlight_score"], identity_row["highlight_score"])
        self.assertEqual(medal_row["recommended_action"], "highlight_candidate")

    def test_repeated_low_value_events_are_capped(self) -> None:
        sidecar = _runtime_sidecar(
            analysis_id="analysis-ability",
            game="marvel_rivals",
            source="fixture-ability",
            events=[
                _event(event_id=f"a{i}", event_type="ability_seen", asset_id=f"a{i}", roi_ref="ability_hud")
                for i in range(6)
            ],
            detections=[],
        )
        clip_row, _, _ = runtime_export._build_rows_for_sidecar(Path("/tmp/c.runtime_analysis.json"), sidecar, "ds", runtime_export.DEFAULT_SCORING_CONFIG)
        self.assertLessEqual(clip_row["highlight_score"], 1.0)
        self.assertEqual(clip_row["score_breakdown"]["event_counts"]["ability_seen"], 6)
        self.assertAlmostEqual(clip_row["score_breakdown"]["event_contributions"]["ability_seen"], 0.54, places=2)

    def test_export_runtime_analysis_invalid_root_returns_error(self) -> None:
        result = run_export_runtime_analysis("/tmp/does-not-exist-runtime-export")
        self.assertFalse(result["ok"])
        self.assertIn("error", result)

    def test_cli_routes_to_export_runtime_analysis(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--export-runtime-analysis", "/tmp/runtime-sidecars", "--game", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_export_runtime_analysis",
                return_value={"ok": True, "clip_row_count": 1, "event_row_count": 2, "detection_row_count": 3},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/runtime-sidecars", game="marvel_rivals")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def _write_sidecar(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
