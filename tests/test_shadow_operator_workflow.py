from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.shadow_model_training import evaluate_shadow_ranking_model, train_shadow_ranking_model
from pipeline.shadow_operator_workflow import run_shadow_operator_workflow
from pipeline.shadow_evaluation_policy import write_shadow_evaluation_policy
from tests.test_shadow_model_training import _prepare_dataset, _write_minimal_v2_dataset


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

    def test_govern_requires_experiment_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = run_shadow_operator_workflow(
                mode="govern",
                output_path=root / "shadow" / "missing-govern.shadow_operator_run.json",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertIn("missing required input for mode govern: experiment_manifest", result["errors"])

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

    def test_train_mode_runs_real_workflow_and_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            result = run_shadow_operator_workflow(
                mode="train",
                dataset_manifest=dataset["manifest_path"],
                split_key="candidate_id",
                train_fraction=0.75,
                output_root=root / "shadow-operator",
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual([step["step_name"] for step in result["step_results"]], ["train_model", "evaluate_model"])
            self.assertEqual(result["step_results"][0]["status"], "ok")
            self.assertEqual(result["step_results"][1]["status"], "ok")
            self.assertIn("model_manifest_path", result["produced_artifacts"])
            self.assertIn("experiment_manifest_path", result["produced_artifacts"])
            self.assertIn("replay_manifest_path", result["produced_artifacts"])
            self.assertIn("comparison_report_path", result["produced_artifacts"])
            self.assertTrue(Path(result["manifest_path"]).exists())

    def test_benchmark_mode_runs_matrix_and_review(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")
            result = run_shadow_operator_workflow(
                mode="benchmark",
                dataset_manifest=dataset["manifest_path"],
                policy_path=policy["manifest_path"],
                output_root=root / "shadow-operator",
                game="marvel_rivals",
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual([step["step_name"] for step in result["step_results"]], ["run_benchmark_matrix", "review_benchmark_results"])
            self.assertEqual(result["step_results"][1]["status"], "ok")
            self.assertIn("benchmark_manifest_path", result["produced_artifacts"])
            self.assertIn("benchmark_review_manifest_path", result["produced_artifacts"])
            self.assertIn(result["final_recommendation"]["decision"], {"prefer_shadow", "keep_current", "inconclusive"})
            self.assertEqual(
                result["final_recommendation"]["supporting_artifacts"],
                [result["produced_artifacts"]["benchmark_review_manifest_path"]],
            )

    def test_train_mode_returns_partial_when_evaluation_fails_after_training(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            with patch(
                "pipeline.shadow_operator_workflow.evaluate_shadow_ranking_model",
                return_value={"ok": False, "status": "evaluation_failed", "error": "forced failure"},
            ):
                result = run_shadow_operator_workflow(
                    mode="train",
                    dataset_manifest=dataset["manifest_path"],
                    split_key="candidate_id",
                    train_fraction=0.75,
                    output_root=root / "shadow-operator",
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["step_results"][0]["status"], "ok")
            self.assertEqual(result["step_results"][1]["status"], "failed")

    def test_benchmark_mode_returns_partial_when_review_fails_after_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            with patch(
                "pipeline.shadow_operator_workflow.review_shadow_benchmark_results",
                return_value={"ok": False, "status": "review_failed", "error": "forced failure"},
            ):
                result = run_shadow_operator_workflow(
                    mode="benchmark",
                    dataset_manifest=dataset["manifest_path"],
                    output_root=root / "shadow-operator",
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["step_results"][0]["status"], "ok")
            self.assertEqual(result["step_results"][1]["status"], "failed")

    def test_govern_mode_runs_policy_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")
            model = train_shadow_ranking_model(
                dataset["manifest_path"],
                model_output_path=root / "models" / "ranker.shadow_ranking_model.json",
                split_key="candidate_id",
                train_fraction=0.75,
            )
            experiment = evaluate_shadow_ranking_model(
                model_path=model["manifest_path"],
                dataset_manifest=dataset["manifest_path"],
                output_path=root / "experiments" / "eval.shadow_ranking_experiment.json",
            )
            result = run_shadow_operator_workflow(
                mode="govern",
                experiment_manifest=experiment["manifest_path"],
                policy_path=policy["manifest_path"],
                target="candidate_approval_probability",
                output_root=root / "shadow-operator",
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual([step["step_name"] for step in result["step_results"]], ["evaluate_governance_policy"])
            self.assertEqual(result["step_results"][0]["status"], "ok")
            self.assertIn("governed_ledger_manifest_path", result["produced_artifacts"])
            self.assertIn(result["final_recommendation"]["decision"], {"prefer_shadow", "blocked_by_policy", "keep_current", "inconclusive"})

    def test_full_mode_runs_train_benchmark_and_govern(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")
            result = run_shadow_operator_workflow(
                mode="full",
                dataset_manifest=dataset["manifest_path"],
                policy_path=policy["manifest_path"],
                split_key="candidate_id",
                train_fraction=0.75,
                output_root=root / "shadow-operator",
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(
                [step["step_name"] for step in result["step_results"]],
                ["train_model", "evaluate_model", "run_benchmark_matrix", "review_benchmark_results", "evaluate_governance_policy"],
            )
            self.assertIn("governed_ledger_manifest_path", result["produced_artifacts"])
            self.assertEqual(
                result["final_recommendation"]["supporting_artifacts"],
                [result["produced_artifacts"]["governed_ledger_manifest_path"]],
            )

    def test_full_mode_stops_before_govern_after_benchmark_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            with patch(
                "pipeline.shadow_operator_workflow.review_shadow_benchmark_results",
                return_value={"ok": False, "status": "review_failed", "error": "forced failure"},
            ):
                result = run_shadow_operator_workflow(
                    mode="full",
                    dataset_manifest=dataset["manifest_path"],
                    split_key="candidate_id",
                    train_fraction=0.75,
                    output_root=root / "shadow-operator",
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "partial")
            self.assertEqual(
                [step["step_name"] for step in result["step_results"]],
                ["train_model", "evaluate_model", "run_benchmark_matrix", "review_benchmark_results"],
            )
            self.assertNotIn("governed_ledger_manifest_path", result["produced_artifacts"])

    def test_full_mode_approved_target_omits_irrelevant_post_sparsity_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")
            result = run_shadow_operator_workflow(
                mode="full",
                dataset_manifest=dataset["manifest_path"],
                policy_path=policy["manifest_path"],
                split_key="candidate_id",
                train_fraction=0.75,
                output_root=root / "shadow-operator",
                training_target="approved_or_selected_probability",
                target="candidate_approval_probability",
            )
            self.assertTrue(result["ok"])
            step_map = {step["step_name"]: step for step in result["step_results"]}
            self.assertEqual(step_map["train_model"]["warning_count"], 0)
            self.assertEqual(step_map["run_benchmark_matrix"]["warning_count"], 0)

    def test_full_mode_fails_early_on_insufficient_approval_label_balance(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset = _write_minimal_v2_dataset(
                root,
                candidate_rows=[
                    {
                        "candidate_id": "candidate-a",
                        "game": "marvel_rivals",
                        "source": "a.mp4",
                        "lifecycle_state": "posted",
                        "review_outcome": None,
                        "final_score": 0.9,
                        "export_present": True,
                        "post_present": True,
                        "metrics_present": False,
                    },
                    {
                        "candidate_id": "candidate-b",
                        "game": "marvel_rivals",
                        "source": "b.mp4",
                        "lifecycle_state": "posted",
                        "review_outcome": None,
                        "final_score": 0.8,
                        "export_present": True,
                        "post_present": True,
                        "metrics_present": False,
                    },
                ],
            )
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")
            result = run_shadow_operator_workflow(
                mode="full",
                dataset_manifest=dataset["manifest_path"],
                policy_path=policy["manifest_path"],
                split_key="candidate_id",
                train_fraction=0.75,
                output_root=root / "shadow-operator",
                training_target="approved_or_selected_probability",
                target="candidate_approval_probability",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(len(result["step_results"]), 1)
            self.assertEqual(result["step_results"][0]["step_name"], "train_model")
            self.assertEqual(result["step_results"][0]["status"], "failed")
            self.assertEqual(result["step_results"][0]["error"], "approved_or_selected_probability requires both positive and negative labels after target construction (positive_count=2, negative_count=0)")


if __name__ == "__main__":
    unittest.main()
