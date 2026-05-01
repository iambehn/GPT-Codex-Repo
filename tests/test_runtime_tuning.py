from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from run import main as run_main
from run import run_replay_runtime_scoring


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


class RuntimeTuningTests(unittest.TestCase):
    def test_replay_runtime_scoring_prefers_trial_when_approved_routing_improves(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as config_root:
            root = Path(sidecar_root)
            config_dir = Path(config_root)
            self._write_sidecar(
                root / "approved.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="approved",
                    game="marvel_rivals",
                    source="clip-approved.mp4",
                    events=[_event("medal_seen")],
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

            trial_config = config_dir / "trial.yaml"
            trial_config.write_text(
                "\n".join(
                    [
                        "action_thresholds:",
                        "  inspect: 0.25",
                        "  highlight_candidate: 0.45",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = run_replay_runtime_scoring(
                root,
                trial_config,
                game="marvel_rivals",
                min_reviewed=2,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["trial_name"], "trial")
            self.assertEqual(result["recommendation"]["decision"], "prefer_trial")
            self.assertEqual(result["comparison"]["action_quality"]["current"]["inspect"]["approved"], 1)
            self.assertEqual(result["comparison"]["action_quality"]["trial"]["highlight_candidate"]["approved"], 1)
            self.assertEqual(result["comparison"]["action_quality"]["trial"]["highlight_candidate"]["rejected"], 0)
            moved = result["comparison"]["clip_movements"]["moved_rows"]
            self.assertEqual(len(moved), 1)
            self.assertEqual(moved[0]["movement"], "inspect -> highlight_candidate")

    def test_replay_runtime_scoring_writes_output_and_debug_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as tempdir:
            root = Path(sidecar_root)
            out = Path(tempdir)
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

            trial_config = out / "trial.json"
            trial_config.write_text(json.dumps({"action_thresholds": {"highlight_candidate": 0.45}}, indent=2), encoding="utf-8")
            report_path = out / "report.json"
            debug_dir = out / "debug"

            result = run_replay_runtime_scoring(
                root,
                trial_config,
                game="marvel_rivals",
                min_reviewed=2,
                output_path=report_path,
                debug_output_dir=debug_dir,
                trial_name="test-trial",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["trial_name"], "test-trial")
            self.assertTrue(report_path.is_file())
            self.assertTrue((debug_dir / "runtime_tuning_report.json").is_file())
            self.assertTrue((debug_dir / "reviewed_comparisons.csv").is_file())
            self.assertTrue((debug_dir / "action_movements.csv").is_file())
            self.assertTrue((debug_dir / "bucket_outcomes.csv").is_file())
            self.assertTrue((debug_dir / "warnings.json").is_file())

    def test_replay_runtime_scoring_returns_insufficient_data_below_minimum(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as config_root:
            root = Path(sidecar_root)
            config_dir = Path(config_root)
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
            trial_config = config_dir / "trial.yaml"
            trial_config.write_text("action_thresholds:\n  highlight_candidate: 0.45\n", encoding="utf-8")

            result = run_replay_runtime_scoring(root, trial_config, game="marvel_rivals", min_reviewed=2)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "insufficient_review_data")
            self.assertEqual(result["recommendation"]["decision"], "inconclusive")

    def test_replay_runtime_scoring_skips_invalid_sidecars_and_bad_trial_config(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as config_root:
            root = Path(sidecar_root)
            config_dir = Path(config_root)
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
                root / "wrong-schema.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="wrong",
                    game="marvel_rivals",
                    source="clip-wrong.mp4",
                    events=[],
                    detections=[],
                    review_status="rejected",
                    schema_version="runtime_analysis_v0",
                ),
            )
            (root / "malformed.runtime_analysis.json").write_text("{bad-json", encoding="utf-8")

            good_trial = config_dir / "trial.yaml"
            good_trial.write_text("action_thresholds:\n  highlight_candidate: 0.45\n", encoding="utf-8")
            good_result = run_replay_runtime_scoring(root, good_trial, game="marvel_rivals", min_reviewed=1)
            reasons = {warning["reason"] for warning in good_result["warnings"]}
            self.assertIn("unsupported_schema_version", reasons)
            self.assertIn("malformed_json", reasons)

            bad_trial = config_dir / "bad.yaml"
            bad_trial.write_text("action_thresholds\n  broken", encoding="utf-8")
            bad_result = run_replay_runtime_scoring(root, bad_trial, game="marvel_rivals", min_reviewed=1)
            self.assertFalse(bad_result["ok"])
            self.assertIn("error", bad_result)

    def test_cli_routes_to_runtime_tuning(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--replay-runtime-scoring",
                "/tmp/runtime-sidecars",
                "--trial-config",
                "/tmp/trial.yaml",
                "--game",
                "marvel_rivals",
                "--trial-name",
                "trial-a",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_replay_runtime_scoring",
                return_value={"ok": True, "trial_name": "trial-a", "reviewed_sidecar_count": 2},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/runtime-sidecars",
                "/tmp/trial.yaml",
                game="marvel_rivals",
                output_path=None,
                min_reviewed=3,
                include_unreviewed=False,
                debug_output_dir=None,
                trial_name="trial-a",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def _write_sidecar(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

