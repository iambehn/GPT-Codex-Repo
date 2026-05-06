from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.shadow_operator_workflow import run_shadow_operator_workflow


class ShadowOperatorWorkflowTests(unittest.TestCase):
    def test_invalid_mode_writes_failure_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = run_shadow_operator_workflow(
                mode="not-a-mode",
                output_path=root / "shadow" / "invalid.shadow_operator_run.json",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["schema_version"], "shadow_operator_run_v1")
            self.assertEqual(result["mode"], "not-a-mode")
            self.assertEqual(result["final_recommendation"]["decision"], "inconclusive")
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "shadow_operator_run_v1")
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["mode"], "not-a-mode")

    def test_missing_required_input_by_mode_writes_failure_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = run_shadow_operator_workflow(
                mode="benchmark",
                output_path=root / "shadow" / "missing-input.shadow_operator_run.json",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertIn("missing required input for mode benchmark: dataset_manifest", result["errors"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], result["errors"])

    def test_final_status_ok_when_all_steps_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = run_shadow_operator_workflow(
                mode="train",
                dataset_manifest="/tmp/dataset.json",
                output_path=root / "shadow" / "ok.shadow_operator_run.json",
                step_results=[
                    {
                        "step_name": "train_model",
                        "status": "ok",
                        "artifact_path": "/tmp/model.shadow_ranking_model.json",
                        "summary": {"row_count": 12},
                    },
                    {
                        "step_name": "evaluate_model",
                        "status": "ok",
                        "artifact_path": "/tmp/experiment.shadow_ranking_experiment.json",
                        "recommendation": {
                            "decision": "prefer_shadow",
                            "reason": "trial improved benchmark quality",
                        },
                    },
                ],
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["final_summary"]["successful_step_count"], 2)
            self.assertEqual(result["final_recommendation"]["decision"], "prefer_shadow")
            self.assertEqual(
                result["final_recommendation"]["supporting_artifacts"],
                ["/tmp/experiment.shadow_ranking_experiment.json"],
            )

    def test_final_status_partial_when_success_precedes_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = run_shadow_operator_workflow(
                mode="full",
                dataset_manifest="/tmp/dataset.json",
                output_path=root / "shadow" / "partial.shadow_operator_run.json",
                step_results=[
                    {
                        "step_name": "train_model",
                        "status": "ok",
                        "artifact_path": "/tmp/model.shadow_ranking_model.json",
                    },
                    {
                        "step_name": "benchmark_matrix",
                        "status": "failed",
                        "error": "benchmark review failed",
                    },
                ],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["final_summary"]["successful_step_count"], 1)
            self.assertEqual(result["final_summary"]["failed_step_count"], 1)
            self.assertEqual(result["final_recommendation"]["decision"], "inconclusive")

    def test_final_status_failed_when_no_steps_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = run_shadow_operator_workflow(
                mode="train",
                dataset_manifest="/tmp/dataset.json",
                output_path=root / "shadow" / "failed.shadow_operator_run.json",
                step_results=[
                    {
                        "step_name": "train_model",
                        "status": "failed",
                        "error": "invalid dataset manifest",
                    }
                ],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["final_summary"]["successful_step_count"], 0)
            self.assertEqual(result["final_summary"]["failed_step_count"], 1)

    def test_artifact_schema_fields_are_present(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = run_shadow_operator_workflow(
                mode="train",
                dataset_manifest="/tmp/dataset.json",
                model_family="linear_shadow_ranker",
                training_target="approved_or_selected_probability",
                target="candidate_approval_probability",
                output_path=root / "shadow" / "schema.shadow_operator_run.json",
                step_results=[
                    {
                        "step_name": "train_model",
                        "status": "ok",
                        "artifact_path": "/tmp/model.shadow_ranking_model.json",
                        "produced_artifacts": {"model_manifest": "/tmp/model.shadow_ranking_model.json"},
                    }
                ],
            )
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(
                set(manifest.keys()),
                {
                    "ok",
                    "status",
                    "schema_version",
                    "operator_run_id",
                    "created_at",
                    "mode",
                    "inputs",
                    "filters",
                    "step_results",
                    "produced_artifacts",
                    "final_summary",
                    "final_recommendation",
                },
            )
            self.assertEqual(manifest["mode"], "train")
            self.assertEqual(manifest["inputs"]["dataset_manifest"], "/tmp/dataset.json")
            self.assertEqual(manifest["produced_artifacts"]["model_manifest"], "/tmp/model.shadow_ranking_model.json")


if __name__ == "__main__":
    unittest.main()
