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
from run import run_fixture_trial_batch


class FixtureTrialBatchTests(unittest.TestCase):
    def test_batch_runner_defaults_to_all_cheap_stage_trials(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)

            def _run_trial(*args, **kwargs):
                trial_name = str(kwargs["trial_name"])
                trial_root = root / "fixture_trial_batches" / "baseline-batch" / "runs" / trial_name
                manifest_path = trial_root / "fixture_trial_run_manifest.json"
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_text("{}", encoding="utf-8")
                return {
                    "ok": True,
                    "status": "ok",
                    "trial_root": str(trial_root),
                    "manifest_path": str(manifest_path),
                    "completed_fixture_count": 2,
                    "failed_fixture_count": 0,
                    "warnings": [],
                }

            def _compare_trials(*args, **kwargs):
                output_path = Path(kwargs["output_path"])
                output_path.parent.mkdir(parents=True, exist_ok=True)
                payload = {
                    "ok": True,
                    "status": "ok",
                    "report_path": str(output_path),
                    "comparison": {"fixture_rows": []},
                    "recommendation": {"decision": "inconclusive"},
                }
                output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                return payload

            with (
                patch("run.run_fixture_trial", side_effect=_run_trial) as trial_mock,
                patch("run.run_compare_fixture_trials", side_effect=_compare_trials) as compare_mock,
            ):
                result = run_fixture_trial_batch(
                    "assets/evaluation/fixture_manifest.json",
                    fixture_source_manifest="assets/evaluation/fixture_sources.json",
                    output_root=root,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(
                [call.kwargs["trial_name"] for call in trial_mock.call_args_list],
                ["baseline", "pyscenedetect", "distil-whisper", "cheap-stage-combined"],
            )
            self.assertEqual(compare_mock.call_count, 3)
            self.assertTrue(Path(result["manifest_path"]).is_file())
            self.assertTrue(Path(result["csv_path"]).is_file())
            self.assertTrue(Path(result["warnings_path"]).is_file())

    def test_batch_runner_restricts_trials_and_orders_baseline_first(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            seen_trials: list[str] = []

            def _run_trial(*args, **kwargs):
                trial_name = str(kwargs["trial_name"])
                seen_trials.append(trial_name)
                trial_root = root / "fixture_trial_batches" / "subset" / "runs" / trial_name
                manifest_path = trial_root / "fixture_trial_run_manifest.json"
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_text("{}", encoding="utf-8")
                return {
                    "ok": True,
                    "status": "ok",
                    "trial_root": str(trial_root),
                    "manifest_path": str(manifest_path),
                    "completed_fixture_count": 1,
                    "failed_fixture_count": 0,
                    "warnings": [],
                }

            def _compare_trials(*args, **kwargs):
                output_path = Path(kwargs["output_path"])
                output_path.parent.mkdir(parents=True, exist_ok=True)
                payload = {
                    "ok": True,
                    "status": "ok",
                    "report_path": str(output_path),
                    "comparison": {"fixture_rows": []},
                    "recommendation": {"decision": "inconclusive"},
                }
                output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                return payload

            with (
                patch("run.run_fixture_trial", side_effect=_run_trial),
                patch("run.run_compare_fixture_trials", side_effect=_compare_trials),
            ):
                run_fixture_trial_batch(
                    "assets/evaluation/fixture_manifest.json",
                    fixture_source_manifest="assets/evaluation/fixture_sources.json",
                    trial_names=["pyscenedetect"],
                    batch_name="subset",
                    output_root=root,
                )

            self.assertEqual(seen_trials, ["baseline", "pyscenedetect"])

    def test_batch_runner_continues_across_failed_trials_and_recommends_adopt_trial(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)

            def _run_trial(*args, **kwargs):
                trial_name = str(kwargs["trial_name"])
                trial_root = root / "fixture_trial_batches" / "batch" / "runs" / trial_name
                manifest_path = trial_root / "fixture_trial_run_manifest.json"
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_text("{}", encoding="utf-8")
                status = "partial_failure" if trial_name == "pyscenedetect" else "ok"
                return {
                    "ok": status == "ok",
                    "status": status,
                    "trial_root": str(trial_root),
                    "manifest_path": str(manifest_path),
                    "completed_fixture_count": 2 if status == "ok" else 1,
                    "failed_fixture_count": 0 if status == "ok" else 1,
                    "warnings": [],
                }

            def _compare_trials(*args, **kwargs):
                trial_root = str(kwargs["trial_run_root"])
                output_path = Path(kwargs["output_path"])
                output_path.parent.mkdir(parents=True, exist_ok=True)
                if trial_root.endswith("distil-whisper"):
                    recommendation = {
                        "decision": "prefer_trial",
                        "reason": "trial improved reviewed fixtures",
                        "supporting_metrics": {"prefer_trial_count": 2},
                        "data_quality_notes": ["balanced fixture coverage"],
                        "follow_up": "promote the winning trial into the next comparison batch",
                    }
                    rows = [
                        {"review_status": "approved", "coverage_status": "both", "recommendation_signal": "trial_better"},
                        {"review_status": "approved", "coverage_status": "both", "recommendation_signal": "trial_better"},
                    ]
                else:
                    recommendation = {"decision": "inconclusive"}
                    rows = []
                payload = {
                    "ok": True,
                    "status": "ok",
                    "report_path": str(output_path),
                    "comparison": {"fixture_rows": rows},
                    "recommendation": recommendation,
                }
                output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                return payload

            with (
                patch("run.run_fixture_trial", side_effect=_run_trial),
                patch("run.run_compare_fixture_trials", side_effect=_compare_trials),
            ):
                result = run_fixture_trial_batch(
                    "assets/evaluation/fixture_manifest.json",
                    fixture_source_manifest="assets/evaluation/fixture_sources.json",
                    trial_names=["pyscenedetect", "distil-whisper"],
                    batch_name="batch",
                    output_root=root,
                )

            self.assertTrue(result["ok"])
            skipped = next(row for row in result["trial_comparisons"] if row["trial_name"] == "pyscenedetect")
            adopted = next(row for row in result["trial_comparisons"] if row["trial_name"] == "distil-whisper")
            self.assertEqual(skipped["comparison_status"], "skipped_due_to_run_failure")
            self.assertEqual(adopted["recommendation"]["decision"], "prefer_trial")
            self.assertEqual(result["overall_recommendation"]["decision"], "adopt_trial")
            self.assertEqual(result["overall_recommendation"]["trial_name"], "distil-whisper")
            self.assertEqual(result["overall_recommendation"]["supporting_metrics"]["prefer_trial_count"], 2)
            self.assertIn("balanced fixture coverage", result["overall_recommendation"]["data_quality_notes"])
            self.assertIn("promote the winning trial", result["overall_recommendation"]["follow_up"])

    def test_batch_runner_reports_invalid_trial_selection(self) -> None:
        result = run_fixture_trial_batch(
            "assets/evaluation/fixture_manifest.json",
            fixture_source_manifest="assets/evaluation/fixture_sources.json",
            trial_names=["baseline", "not-real"],
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_trial_selection")

    def test_cli_routes_to_run_fixture_trial_batch(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--run-fixture-trial-batch",
                "/tmp/fixtures.json",
                "--fixture-source-manifest",
                "/tmp/fixture_sources.json",
                "--trial",
                "baseline",
                "--trial",
                "distil-whisper",
                "--batch-name",
                "nightly",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_fixture_trial_batch",
                return_value={"ok": True, "status": "ok"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/fixtures.json",
                fixture_source_manifest="/tmp/fixture_sources.json",
                trial_names=["baseline", "distil-whisper"],
                batch_name="nightly",
                output_root=None,
                game=None,
                pattern="*.mp4",
                limit=None,
                emit_runtime=False,
                emit_fused=False,
            )
            self.assertTrue(json.loads(stdout.getvalue())["ok"])
        finally:
            sys.argv = original_argv


if __name__ == "__main__":
    unittest.main()
