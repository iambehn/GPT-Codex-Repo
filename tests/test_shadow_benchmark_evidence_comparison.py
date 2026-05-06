from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.shadow_benchmark_matrix import SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION
from pipeline.shadow_benchmark_evidence_comparison import compare_shadow_benchmark_evidence_modes


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class ShadowBenchmarkEvidenceComparisonTests(unittest.TestCase):
    def test_compare_shadow_benchmark_evidence_modes_writes_rows_and_registry_query(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            real_review = root / "real.shadow_benchmark_review.json"
            synthetic_review = root / "synthetic.shadow_benchmark_review.json"
            _write_json(
                real_review,
                {
                    "schema_version": "shadow_benchmark_review_v1",
                    "review_id": "review-real",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "reviewed_targets": ["post_performance_score"],
                    "reviewed_families": ["linear_shadow_ranker"],
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "target_reviews": [
                        {
                            "training_target": "post_performance_score",
                            "current_best_family": "linear_shadow_ranker",
                            "best_recommendation_decision": "keep_current",
                            "current_best_evidence_mode": "real_only",
                            "evidence_modes": ["real_only"],
                            "synthetic_augmented_run_count": 0,
                            "real_only_run_count": 2,
                            "primary_metric_name": "pearson_correlation",
                            "primary_metric_delta": -0.03,
                            "run_count": 2,
                            "successful_run_count": 2,
                            "win_count": 0,
                            "keep_current_count": 2,
                            "blocked_count": 0,
                            "inconclusive_count": 0,
                            "failed_count": 0,
                            "dominant_failure_modes": [],
                            "confidence_level": "low",
                            "readiness_classification": "needs_feature_cleanup",
                            "recommended_next_action": "prune_features",
                            "game": "marvel_rivals",
                            "platform": "youtube",
                        }
                    ],
                },
            )
            _write_json(
                synthetic_review,
                {
                    "schema_version": "shadow_benchmark_review_v1",
                    "review_id": "review-synthetic",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "reviewed_targets": ["post_performance_score"],
                    "reviewed_families": ["gradient_boosted_shadow_ranker"],
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "target_reviews": [
                        {
                            "training_target": "post_performance_score",
                            "current_best_family": "gradient_boosted_shadow_ranker",
                            "best_recommendation_decision": "keep_current",
                            "current_best_evidence_mode": "synthetic_augmented",
                            "evidence_modes": ["synthetic_augmented"],
                            "synthetic_augmented_run_count": 2,
                            "real_only_run_count": 0,
                            "primary_metric_name": "pearson_correlation",
                            "primary_metric_delta": 0.07,
                            "run_count": 2,
                            "successful_run_count": 2,
                            "win_count": 0,
                            "keep_current_count": 2,
                            "blocked_count": 0,
                            "inconclusive_count": 0,
                            "failed_count": 0,
                            "dominant_failure_modes": [],
                            "confidence_level": "low",
                            "readiness_classification": "ready_for_next_iteration",
                            "recommended_next_action": "keep_target_as_is",
                            "game": "marvel_rivals",
                            "platform": "youtube",
                        }
                    ],
                },
            )

            comparison = compare_shadow_benchmark_evidence_modes(
                real_review,
                synthetic_review,
                output_path=root / "comparisons" / "compare.shadow_benchmark_evidence_comparison.json",
            )
            self.assertTrue(comparison["ok"])
            self.assertEqual(comparison["schema_version"], "shadow_benchmark_evidence_comparison_v1")
            self.assertEqual(comparison["row_count"], 1)
            row = comparison["rows"][0]
            self.assertTrue(row["family_winner_changed"])
            self.assertTrue(row["readiness_changed"])
            self.assertIn("ready_only_under_synthetic", row["disagreement_indicators"])

            registry_path = root / "registry.sqlite"
            refresh = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(refresh["ok"])

            rows = query_clip_registry(
                mode="shadow-benchmark-evidence-comparisons",
                registry_path=registry_path,
                training_target="post_performance_score",
            )
            self.assertEqual(rows["row_count"], 1)
            self.assertEqual(rows["rows"][0]["synthetic_current_best_family"], "gradient_boosted_shadow_ranker")

    def test_compare_shadow_benchmark_evidence_modes_preserves_real_review_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            real_review = root / "real.shadow_benchmark_review.json"
            synthetic_review = root / "synthetic.shadow_benchmark_review.json"
            _write_json(
                real_review,
                {
                    "schema_version": "shadow_benchmark_review_v1",
                    "review_id": "review-real",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "target_reviews": [
                        {
                            "training_target": "approved_or_selected_probability",
                            "current_best_family": "gradient_boosted_shadow_ranker",
                            "best_recommendation_decision": "inconclusive",
                            "current_best_evidence_mode": "real_only",
                            "primary_metric_name": "top_k_recall",
                            "primary_metric_delta": 0.0,
                            "run_count": 2,
                            "successful_run_count": 2,
                            "confidence_level": "low",
                            "readiness_classification": "not_ready_due_to_coverage",
                            "game": "marvel_rivals",
                            "platform": "youtube",
                        }
                    ],
                },
            )
            _write_json(
                synthetic_review,
                {
                    "schema_version": "shadow_benchmark_review_v1",
                    "review_id": "review-synthetic",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "target_reviews": [
                        {
                            "training_target": "approved_or_selected_probability",
                            "current_best_family": "gradient_boosted_shadow_ranker",
                            "best_recommendation_decision": "keep_current",
                            "current_best_evidence_mode": "synthetic_augmented",
                            "primary_metric_name": "top_k_recall",
                            "primary_metric_delta": 0.0,
                            "run_count": 2,
                            "successful_run_count": 2,
                            "confidence_level": "low",
                            "readiness_classification": "ready_for_next_iteration",
                            "game": "marvel_rivals",
                            "platform": "youtube",
                        }
                    ],
                },
            )

            comparison = compare_shadow_benchmark_evidence_modes(real_review, synthetic_review)
            self.assertTrue(comparison["ok"])
            row = comparison["rows"][0]
            self.assertEqual(row["real_current_best_family"], "gradient_boosted_shadow_ranker")
            self.assertEqual(row["real_best_recommendation_decision"], "inconclusive")
            self.assertEqual(row["real_current_best_evidence_mode"], "real_only")
            self.assertEqual(row["real_primary_metric_name"], "top_k_recall")
            self.assertEqual(row["real_primary_metric_delta"], 0.0)
            self.assertEqual(row["real_confidence_level"], "low")
            self.assertEqual(row["real_successful_run_count"], 2)
            self.assertEqual(row["real_run_count"], 2)
            self.assertEqual(row["game"], "marvel_rivals")
            self.assertEqual(row["platform"], "youtube")

    def test_compare_shadow_benchmark_evidence_modes_accepts_matrix_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            real_benchmark = root / "real.shadow_benchmark_matrix.json"
            synthetic_benchmark = root / "synthetic.shadow_benchmark_matrix.json"
            _write_json(
                real_benchmark,
                {
                    "schema_version": SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION,
                    "benchmark_id": "real-benchmark",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "benchmark_config": {"filters": {"game": "marvel_rivals", "platform": "youtube"}},
                    "runs": [
                        {
                            "run_id": "real-run",
                            "status": "ok",
                            "model_family": "linear_shadow_ranker",
                            "training_target": "post_performance_score",
                            "evidence_mode": "real_only",
                            "recommendation_decision": "inconclusive",
                            "primary_metric_name": "pearson_correlation",
                            "primary_metric_delta": -0.01,
                        }
                    ],
                },
            )
            _write_json(
                synthetic_benchmark,
                {
                    "schema_version": SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION,
                    "benchmark_id": "synthetic-benchmark",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "benchmark_config": {"filters": {"game": "marvel_rivals", "platform": "youtube"}},
                    "runs": [
                        {
                            "run_id": "synthetic-run",
                            "status": "ok",
                            "model_family": "gradient_boosted_shadow_ranker",
                            "training_target": "post_performance_score",
                            "evidence_mode": "synthetic_augmented",
                            "recommendation_decision": "keep_current",
                            "primary_metric_name": "pearson_correlation",
                            "primary_metric_delta": 0.07,
                        }
                    ],
                },
            )

            comparison = compare_shadow_benchmark_evidence_modes(real_benchmark, synthetic_benchmark)
            self.assertTrue(comparison["ok"])
            row = comparison["rows"][0]
            self.assertEqual(row["training_target"], "post_performance_score")
            self.assertEqual(row["real_current_best_family"], "linear_shadow_ranker")
            self.assertEqual(row["synthetic_current_best_family"], "gradient_boosted_shadow_ranker")
            self.assertEqual(row["real_current_best_evidence_mode"], "real_only")
            self.assertEqual(row["synthetic_current_best_evidence_mode"], "synthetic_augmented")
            self.assertEqual(row["game"], "marvel_rivals")
            self.assertEqual(row["platform"], "youtube")
