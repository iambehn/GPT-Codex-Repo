from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.shadow_benchmark_matrix import (
    run_shadow_benchmark_matrix,
    summarize_shadow_benchmark_matrix,
)
from pipeline.shadow_evaluation_policy import write_shadow_evaluation_policy
from tests.test_shadow_model_training import _prepare_dataset


class ShadowBenchmarkMatrixTests(unittest.TestCase):
    def test_run_shadow_benchmark_matrix_writes_manifest_and_registry_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, registry_path = _prepare_dataset(root)
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")

            benchmark = run_shadow_benchmark_matrix(
                dataset["manifest_path"],
                policy_path=policy["manifest_path"],
                output_path=root / "benchmarks" / "matrix.shadow_benchmark_matrix.json",
                game="marvel_rivals",
            )
            self.assertTrue(benchmark["ok"])
            self.assertEqual(benchmark["schema_version"], "shadow_benchmark_matrix_v1")
            self.assertEqual(benchmark["run_count"], 6)
            self.assertEqual({row["model_family"] for row in benchmark["runs"] if row["status"] == "ok"}, {
                "linear_shadow_ranker",
                "gradient_boosted_shadow_ranker",
            })
            self.assertEqual({row["training_target"] for row in benchmark["summary"]["best_family_per_target"]}, {
                "approved_or_selected_probability",
                "export_selection_probability",
                "post_performance_score",
            })
            self.assertTrue(all(row["evidence_mode"] == "real_only" for row in benchmark["runs"] if row["status"] == "ok"))

            refresh = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(refresh["ok"])
            self.assertEqual(refresh["shadow_benchmark_matrix_manifest_count"], 1)
            self.assertEqual(refresh["shadow_benchmark_run_row_count"], 6)

            matrices = query_clip_registry(mode="shadow-benchmark-matrices", registry_path=registry_path)
            boosted_runs = query_clip_registry(
                mode="shadow-benchmark-runs",
                registry_path=registry_path,
                model_family="gradient_boosted_shadow_ranker",
            )
            post_runs = query_clip_registry(
                mode="shadow-benchmark-runs",
                registry_path=registry_path,
                training_target="post_performance_score",
            )
            self.assertEqual(matrices["row_count"], 1)
            self.assertEqual(boosted_runs["row_count"], 3)
            self.assertEqual(post_runs["row_count"], 2)
            self.assertTrue(all(row["evidence_mode"] == "real_only" for row in post_runs["rows"]))

            summary = summarize_shadow_benchmark_matrix(benchmark["manifest_path"])
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["summary"]["best_family_per_target"][0]["training_target"], "approved_or_selected_probability")

    def test_run_shadow_benchmark_matrix_records_failed_cells_without_aborting(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)

            benchmark = run_shadow_benchmark_matrix(
                dataset["manifest_path"],
                model_families=["linear_shadow_ranker"],
                training_targets=["approved_or_selected_probability", "invalid_target"],
                output_path=root / "benchmarks" / "partial.shadow_benchmark_matrix.json",
            )
            self.assertTrue(benchmark["ok"])
            self.assertEqual(benchmark["run_count"], 2)
            failed = [row for row in benchmark["runs"] if row["status"] == "failed"]
            ok_rows = [row for row in benchmark["runs"] if row["status"] == "ok"]
            self.assertEqual(len(failed), 1)
            self.assertEqual(len(ok_rows), 1)
            self.assertEqual(failed[0]["failure_reason"], "unsupported_training_target")
