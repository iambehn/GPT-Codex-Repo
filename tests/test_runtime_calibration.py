from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline import runtime_calibration
from run import main as run_main
from run import run_calibrate_runtime_review


def _event(event_type: str, *, confidence: float = 0.95) -> dict[str, object]:
    return {
        "event_id": f"{event_type}-event",
        "event_type": event_type,
        "timestamp": 1.0,
        "start_timestamp": 1.0,
        "end_timestamp": 1.5,
        "asset_id": f"{event_type}-asset",
        "roi_ref": "hero_portrait",
        "confidence": confidence,
        "evidence": {"peak_score": confidence},
        "source_detection_count": 3,
    }


def _detection(*, roi_ref: str = "hero_portrait", asset_family: str = "hero_portrait") -> dict[str, object]:
    return {
        "asset_id": f"{asset_family}-asset",
        "roi_ref": roi_ref,
        "asset_family": asset_family,
        "first_timestamp": 1.0,
        "last_timestamp": 1.5,
        "peak_score": 0.98,
        "supporting_frames": 4,
        "temporal_window": 3,
    }


def _runtime_sidecar(
    *,
    analysis_id: str,
    game: str,
    source: str,
    events: list[dict[str, object]],
    detections: list[dict[str, object]],
    review_status: str | None = None,
    ok: bool = True,
    schema_version: str = "runtime_analysis_v1",
) -> dict[str, object]:
    payload: dict[str, object] = {
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
    if review_status is not None:
        payload["runtime_review"] = {"review_status": review_status}
    return payload


class RuntimeCalibrationTests(unittest.TestCase):
    def test_calibrate_runtime_review_reports_diagnostics_from_reviewed_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "approved.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="approved",
                    game="marvel_rivals",
                    source="clip-approved.mp4",
                    events=[_event("medal_seen"), _event("ability_seen")],
                    detections=[_detection(roi_ref="medal_area", asset_family="medal_icon")],
                    review_status="approved",
                ),
            )
            self._write_sidecar(
                root / "rejected.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="rejected",
                    game="marvel_rivals",
                    source="clip-rejected.mp4",
                    events=[_event("pov_character_identified")],
                    detections=[_detection()],
                    review_status="rejected",
                ),
            )
            self._write_sidecar(
                root / "unreviewed.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="unreviewed",
                    game="marvel_rivals",
                    source="clip-unreviewed.mp4",
                    events=[_event("ability_seen")],
                    detections=[_detection(asset_family="ability_icon")],
                ),
            )

            result = run_calibrate_runtime_review(root, game="marvel_rivals", min_reviewed=2)

            self.assertTrue(result["ok"])
            self.assertEqual(result["reviewed_sidecar_count"], 2)
            self.assertEqual(result["approved_count"], 1)
            self.assertEqual(result["rejected_count"], 1)
            self.assertEqual(result["diagnostics"]["action_outcomes"]["highlight_candidate"]["approved"], 1)
            self.assertEqual(result["diagnostics"]["action_outcomes"]["skip"]["rejected"], 1)
            self.assertIn("medal_seen", result["diagnostics"]["event_type_incidence"]["approved"])
            self.assertIn("threshold_observations", result["recommendations"])
            self.assertIn("weight_observations", result["recommendations"])
            self.assertEqual(result["release_gate_summary"]["status"], "pass")

    def test_calibrate_runtime_review_skips_invalid_sidecars_with_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "valid.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="valid",
                    game="marvel_rivals",
                    source="clip-valid.mp4",
                    events=[_event("medal_seen")],
                    detections=[_detection(asset_family="medal_icon")],
                    review_status="approved",
                ),
            )
            self._write_sidecar(
                root / "failed.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="failed",
                    game="marvel_rivals",
                    source="clip-failed.mp4",
                    events=[],
                    detections=[],
                    review_status="rejected",
                    ok=False,
                ),
            )
            self._write_sidecar(
                root / "wrong-schema.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="wrong-schema",
                    game="marvel_rivals",
                    source="clip-schema.mp4",
                    events=[],
                    detections=[],
                    review_status="approved",
                    schema_version="runtime_analysis_v0",
                ),
            )
            (root / "malformed.runtime_analysis.json").write_text("{bad-json", encoding="utf-8")

            result = run_calibrate_runtime_review(root, game="marvel_rivals", min_reviewed=1)

            self.assertTrue(result["ok"])
            reasons = {warning["reason"] for warning in result["warnings"]}
            self.assertIn("failed_analysis", reasons)
            self.assertIn("unsupported_schema_version", reasons)
            self.assertIn("malformed_json", reasons)

    def test_calibrate_runtime_review_returns_insufficient_data_when_below_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "approved.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="approved",
                    game="marvel_rivals",
                    source="clip-approved.mp4",
                    events=[_event("medal_seen")],
                    detections=[_detection(asset_family="medal_icon")],
                    review_status="approved",
                ),
            )

            result = run_calibrate_runtime_review(root, game="marvel_rivals", min_reviewed=2)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "insufficient_review_data")
            self.assertIn("data_quality_notes", result["recommendations"])
            self.assertEqual(result["release_gate_summary"]["status"], "fail")

    def test_calibrate_runtime_review_writes_output_and_debug_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as output_root:
            root = Path(sidecar_root)
            output = Path(output_root)
            self._write_sidecar(
                root / "approved.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="approved",
                    game="marvel_rivals",
                    source="clip-approved.mp4",
                    events=[_event("medal_seen"), _event("ability_seen")],
                    detections=[_detection(asset_family="medal_icon")],
                    review_status="approved",
                ),
            )
            self._write_sidecar(
                root / "rejected.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="rejected",
                    game="marvel_rivals",
                    source="clip-rejected.mp4",
                    events=[_event("pov_character_identified")],
                    detections=[_detection()],
                    review_status="rejected",
                ),
            )

            report_path = output / "report.json"
            debug_dir = output / "debug"
            result = run_calibrate_runtime_review(
                root,
                game="marvel_rivals",
                min_reviewed=2,
                output_path=report_path,
                debug_output_dir=debug_dir,
            )

            self.assertTrue(result["ok"])
            self.assertTrue(report_path.is_file())
            self.assertTrue((debug_dir / "runtime_calibration_report.json").is_file())
            self.assertTrue((debug_dir / "reviewed_clips.csv").is_file())
            self.assertTrue((debug_dir / "event_diagnostics.csv").is_file())
            self.assertTrue((debug_dir / "score_buckets.csv").is_file())
            self.assertTrue((debug_dir / "warnings.json").is_file())

    def test_cli_routes_to_runtime_calibration(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--calibrate-runtime-review", "/tmp/runtime-sidecars", "--game", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_calibrate_runtime_review",
                return_value={"ok": True, "reviewed_sidecar_count": 2, "approved_count": 1, "rejected_count": 1},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/runtime-sidecars",
                game="marvel_rivals",
                output_path=None,
                min_reviewed=3,
                include_unreviewed=False,
                debug_output_dir=None,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_invalid_root_returns_error(self) -> None:
        result = run_calibrate_runtime_review("/tmp/does-not-exist-runtime-calibration")
        self.assertFalse(result["ok"])
        self.assertIn("error", result)

    def _write_sidecar(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
