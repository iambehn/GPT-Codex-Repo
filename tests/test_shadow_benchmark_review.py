from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.shadow_benchmark_matrix import SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION, run_shadow_benchmark_matrix
from pipeline.shadow_benchmark_review import (
    review_shadow_benchmark_results,
    summarize_shadow_target_readiness,
)
from pipeline.shadow_evaluation_policy import write_shadow_evaluation_policy
from tests.test_shadow_model_training import _prepare_dataset


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class ShadowBenchmarkReviewTests(unittest.TestCase):
    def test_review_shadow_benchmark_results_writes_manifest_and_registry_rows(self) -> None:
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

            review = review_shadow_benchmark_results(
                [benchmark["manifest_path"]],
                output_path=root / "reviews" / "review.shadow_benchmark_review.json",
            )
            self.assertTrue(review["ok"])
            self.assertEqual(review["schema_version"], "shadow_benchmark_review_v1")
            self.assertEqual(
                {row["training_target"] for row in review["target_reviews"]},
                {
                    "approved_or_selected_probability",
                    "export_selection_probability",
                    "post_performance_score",
                },
            )
            self.assertTrue(all(row["current_best_evidence_mode"] == "real_only" for row in review["target_reviews"]))

            first = refresh_clip_registry(root, registry_path=registry_path)
            second = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["shadow_benchmark_review_manifest_count"], 1)
            self.assertEqual(first["shadow_target_readiness_row_count"], 3)

            review_rows = query_clip_registry(mode="shadow-benchmark-reviews", registry_path=registry_path)
            readiness_rows = query_clip_registry(mode="shadow-target-readiness", registry_path=registry_path)
            target_rows = query_clip_registry(
                mode="shadow-target-readiness",
                registry_path=registry_path,
                training_target="approved_or_selected_probability",
            )
            self.assertEqual(review_rows["row_count"], 1)
            self.assertEqual(readiness_rows["row_count"], 3)
            self.assertEqual(target_rows["row_count"], 1)

            summary = summarize_shadow_target_readiness(
                review["manifest_path"],
                training_target="approved_or_selected_probability",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["row_count"], 1)
            self.assertEqual(summary["rows"][0]["training_target"], "approved_or_selected_probability")
            self.assertEqual(summary["rows"][0]["current_best_evidence_mode"], "real_only")

    def test_review_classifies_sparse_target_as_not_ready_due_to_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark_path = root / "benchmarks" / "sparse.shadow_benchmark_matrix.json"
            _write_json(
                benchmark_path,
                {
                    "schema_version": SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION,
                    "benchmark_id": "benchmark-sparse",
                    "created_at": "2026-05-04T00:00:00+00:00",
                    "benchmark_config": {
                        "model_families": ["linear_shadow_ranker"],
                        "training_targets": ["post_performance_score"],
                        "split_key": "fixture_id",
                        "train_fraction": 0.8,
                        "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    },
                    "run_count": 1,
                    "runs": [
                        {
                            "run_id": "run-post-linear",
                            "status": "ok",
                            "model_family": "linear_shadow_ranker",
                            "training_target": "post_performance_score",
                            "evaluation_target": "post_performance_score",
                            "split_key": "fixture_id",
                            "train_fraction": 0.8,
                            "recommendation_decision": "inconclusive",
                            "recommendation_reason": "insufficient posted coverage",
                            "coverage_status": "sparse",
                            "primary_metric_name": "shadow_pearson_correlation",
                            "primary_metric_delta": 0.02,
                            "protected_regression_count": 0,
                            "blocking_reasons": [],
                        }
                    ],
                    "summary": {
                        "best_family_per_target": [],
                        "family_counts": {},
                        "blocked_run_count": 0,
                        "inconclusive_run_count": 1,
                        "failed_run_count": 0,
                        "unstable_slices": [],
                        "benchmark_recommendation": "inconclusive",
                    },
                    "warnings": [],
                },
            )

            review = review_shadow_benchmark_results([benchmark_path])
            self.assertTrue(review["ok"])
            self.assertEqual(review["target_reviews"][0]["readiness_classification"], "not_ready_due_to_coverage")
            self.assertEqual(review["target_reviews"][0]["recommended_next_action"], "defer_target")

            summary = summarize_shadow_target_readiness(review["manifest_path"])
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["row_count"], 1)
            self.assertEqual(summary["rows"][0]["readiness_classification"], "not_ready_due_to_coverage")

    def test_review_classifies_post_performance_keep_current_as_feature_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            benchmark_path = root / "benchmarks" / "post-quality.shadow_benchmark_matrix.json"
            _write_json(
                benchmark_path,
                {
                    "schema_version": SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION,
                    "benchmark_id": "benchmark-post-quality",
                    "created_at": "2026-05-04T00:00:00+00:00",
                    "benchmark_config": {
                        "model_families": ["gradient_boosted_shadow_ranker", "linear_shadow_ranker"],
                        "training_targets": ["post_performance_score"],
                        "split_key": "fixture_id",
                        "train_fraction": 0.8,
                        "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    },
                    "run_count": 2,
                    "runs": [
                        {
                            "run_id": "run-post-boosted",
                            "status": "ok",
                            "model_family": "gradient_boosted_shadow_ranker",
                            "training_target": "post_performance_score",
                            "evaluation_target": "post_performance_score",
                            "evidence_mode": "synthetic_augmented",
                            "split_key": "fixture_id",
                            "train_fraction": 0.8,
                            "recommendation_decision": "keep_current",
                            "recommendation_reason": "shadow model does not improve the primary metric enough to clear policy thresholds",
                            "coverage_status": "sufficient",
                            "primary_metric_name": "pearson_correlation",
                            "primary_metric_delta": -0.08,
                            "protected_regression_count": 0,
                            "blocking_reasons": [],
                        },
                        {
                            "run_id": "run-post-linear",
                            "status": "ok",
                            "model_family": "linear_shadow_ranker",
                            "training_target": "post_performance_score",
                            "evaluation_target": "post_performance_score",
                            "evidence_mode": "synthetic_augmented",
                            "split_key": "fixture_id",
                            "train_fraction": 0.8,
                            "recommendation_decision": "blocked_by_policy",
                            "recommendation_reason": "global improvement is blocked by protected-slice regression",
                            "coverage_status": "sufficient",
                            "primary_metric_name": "pearson_correlation",
                            "primary_metric_delta": -0.55,
                            "protected_regression_count": 3,
                            "blocking_reasons": ["game=marvel_rivals", "fixture_id=unassigned", "coverage_tier=posted_usable_metrics"],
                        },
                    ],
                    "summary": {
                        "best_family_per_target": [],
                        "family_counts": {},
                        "blocked_run_count": 1,
                        "inconclusive_run_count": 0,
                        "failed_run_count": 0,
                        "unstable_slices": [],
                        "benchmark_recommendation": "keep_current",
                    },
                    "warnings": [],
                },
            )

            review = review_shadow_benchmark_results([benchmark_path])
            self.assertTrue(review["ok"])
            self.assertEqual(review["target_reviews"][0]["readiness_classification"], "needs_feature_cleanup")
            self.assertEqual(review["target_reviews"][0]["recommended_next_action"], "prune_features")
            self.assertEqual(review["target_reviews"][0]["current_best_evidence_mode"], "synthetic_augmented")
            self.assertEqual(review["target_reviews"][0]["evidence_modes"], ["synthetic_augmented"])
