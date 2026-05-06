from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.shadow_evaluation_policy import (
    evaluate_shadow_experiment_policy,
    summarize_shadow_experiment_ledger,
    write_shadow_evaluation_policy,
)
from pipeline.shadow_model_training import evaluate_shadow_ranking_model, train_shadow_ranking_model
from tests.test_shadow_model_training import _prepare_dataset


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class ShadowEvaluationPolicyTests(unittest.TestCase):
    def test_policy_evaluation_ingests_ledgers_and_summaries_for_all_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, registry_path = _prepare_dataset(root)
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
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")

            candidate_ledger = evaluate_shadow_experiment_policy(
                experiment["manifest_path"],
                policy_path=policy["manifest_path"],
                target="candidate_approval_probability",
                output_path=root / "ledgers" / "candidate.shadow_experiment_ledger.json",
            )
            export_ledger = evaluate_shadow_experiment_policy(
                experiment["manifest_path"],
                policy_path=policy["manifest_path"],
                target="export_selection_probability",
                output_path=root / "ledgers" / "export.shadow_experiment_ledger.json",
            )
            post_ledger = evaluate_shadow_experiment_policy(
                experiment["manifest_path"],
                policy_path=policy["manifest_path"],
                target="post_performance_score",
                output_path=root / "ledgers" / "post.shadow_experiment_ledger.json",
                platform="youtube",
            )

            self.assertTrue(candidate_ledger["ok"])
            self.assertTrue(export_ledger["ok"])
            self.assertTrue(post_ledger["ok"])
            self.assertEqual(candidate_ledger["schema_version"], "shadow_experiment_ledger_v1")
            self.assertEqual(export_ledger["evaluation_target"], "export_selection_probability")
            self.assertEqual(post_ledger["evaluation_target"], "post_performance_score")

            refresh = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(refresh["ok"])
            self.assertEqual(refresh["shadow_evaluation_policy_manifest_count"], 1)
            self.assertEqual(refresh["shadow_ranking_experiment_ledger_manifest_count"], 3)
            self.assertGreater(refresh["shadow_ranking_experiment_slice_row_count"], 0)

            policies = query_clip_registry(mode="shadow-evaluation-policies", registry_path=registry_path)
            ledgers = query_clip_registry(
                mode="shadow-ranking-experiment-ledgers",
                registry_path=registry_path,
                training_target="approved_or_selected_probability",
            )
            slices = query_clip_registry(
                mode="shadow-ranking-experiment-slices",
                registry_path=registry_path,
                hook_mode="natural",
            )
            self.assertEqual(policies["row_count"], 1)
            self.assertEqual(ledgers["row_count"], 3)
            self.assertGreaterEqual(slices["row_count"], 1)
            self.assertEqual({row["evaluation_target"] for row in ledgers["rows"]}, {
                "candidate_approval_probability",
                "export_selection_probability",
                "post_performance_score",
            })

            summary = summarize_shadow_experiment_ledger(ledgers)
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["target_count"], 3)
            self.assertEqual({row["evaluation_target"] for row in summary["targets"]}, {
                "candidate_approval_probability",
                "export_selection_probability",
                "post_performance_score",
            })

    def test_policy_blocks_global_improvement_when_fixture_slice_regresses(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            policy = write_shadow_evaluation_policy(root / "policy" / "default.shadow_evaluation_policy.json")
            replay_path = root / "replays" / "blocked.shadow_ranking_replay.json"
            experiment_path = root / "experiments" / "blocked.shadow_ranking_experiment.json"

            replay_rows = [
                self._candidate_row("a-pos-1", "fixture-a", True, 1.0, 0.99, 0.70),
                self._candidate_row("a-pos-2", "fixture-a", True, 1.0, 0.98, 0.60),
                self._candidate_row("a-neg-1", "fixture-a", False, 0.0, 0.10, 0.20),
                self._candidate_row("b-pos-1", "fixture-b", True, 1.0, 0.10, 0.95),
                self._candidate_row("b-neg-1", "fixture-b", False, 0.0, 0.20, 0.30),
                self._candidate_row("c-pos-1", "fixture-c", True, 1.0, 0.97, 0.15),
                self._candidate_row("c-neg-1", "fixture-c", False, 0.0, 0.05, 0.90),
                self._candidate_row("d-neg-1", "fixture-d", False, 0.0, 0.01, 0.89),
            ]
            _write_json(
                replay_path,
                {
                    "schema_version": "shadow_ranking_replay_v1",
                    "replay_id": "replay-blocked",
                    "created_at": "2026-05-04T00:00:00+00:00",
                    "model_family": "linear_shadow_ranker",
                    "model_version": "v1",
                    "dataset_manifest_path": "/tmp/dataset.json",
                    "row_count": len(replay_rows),
                    "rows": replay_rows,
                },
            )
            _write_json(
                experiment_path,
                {
                    "schema_version": "shadow_ranking_experiment_v1",
                    "experiment_id": "exp-blocked",
                    "created_at": "2026-05-04T00:00:00+00:00",
                    "model_path": "/tmp/model.json",
                    "model_id": "shadow-model-blocked",
                    "model_family": "linear_shadow_ranker",
                    "model_version": "v1",
                    "dataset_manifest_path": "/tmp/dataset.json",
                    "dataset_export_id": "dataset-1",
                    "training_target": "approved_or_selected_probability",
                    "split_key": "fixture_id",
                    "train_fraction": 0.8,
                    "replay_manifest_path": str(replay_path),
                    "comparison_report_path": "/tmp/comparison.json",
                    "replay_row_count": len(replay_rows),
                    "comparison_row_count": len(replay_rows),
                    "comparison_recommendation": {"decision": "prefer_shadow", "reason": "global aggregate improved"},
                    "comparison_summary": {},
                    "training_metrics": {},
                    "evaluation_metrics": {},
                },
            )

            ledger = evaluate_shadow_experiment_policy(
                experiment_path,
                policy_path=policy["manifest_path"],
                target="candidate_approval_probability",
                output_path=root / "ledgers" / "blocked.shadow_experiment_ledger.json",
            )
            self.assertTrue(ledger["ok"])
            self.assertEqual(ledger["recommendation"]["decision"], "blocked_by_policy")
            self.assertIn("fixture_id=fixture-b", ledger["recommendation"]["blocking_reasons"])

    @staticmethod
    def _candidate_row(
        candidate_id: str,
        fixture_id: str,
        positive: bool,
        label_score: float,
        predicted_score: float,
        heuristic_score: float,
    ) -> dict:
        return {
            "candidate_id": candidate_id,
            "event_id": f"event-{candidate_id}",
            "game": "marvel_rivals",
            "fixture_id": fixture_id,
            "source": f"/tmp/{candidate_id}.mp4",
            "platform": None,
            "account_id": None,
            "heuristic_final_score": heuristic_score,
            "heuristic_recommended_action": "highlight" if positive else "discard",
            "heuristic_lifecycle_state": "approved" if positive else "rejected",
            "review_outcome": "approved" if positive else "rejected",
            "export_present": False,
            "post_present": False,
            "metrics_present": False,
            "latest_view_count": None,
            "latest_engagement_rate": None,
            "hook_mode": "natural" if positive else "reject",
            "hook_archetype": "clutch" if positive else "other",
            "packaging_strategy": "tight_crop",
            "label_positive": positive,
            "label_score": label_score,
            "predicted_candidate_score": predicted_score,
            "predicted_export_score": predicted_score,
            "predicted_post_performance_score": predicted_score,
            "predicted_rank": None,
            "heuristic_rank": None,
            "feature_values": {},
        }
