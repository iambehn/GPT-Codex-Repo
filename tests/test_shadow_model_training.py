from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.highlight_export_batch import (
    create_highlight_export_batch,
    record_post_ledger,
    record_posted_metrics_snapshot,
)
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from pipeline.shadow_model_training import (
    _target_value,
    compare_shadow_model_families,
    evaluate_shadow_ranking_model,
    train_shadow_ranking_model,
)
from pipeline.v2_training_export import export_v2_training_datasets
from pipeline.workflow_run_state import create_workflow_run


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fused_sidecar(
    path: Path,
    *,
    game: str,
    source: Path,
    fusion_id: str,
    event_id: str,
    final_score: float,
    review_status: str,
) -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_analysis_v1",
            "fusion_id": fusion_id,
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "normalized_signals": [
                {
                    "signal_id": f"{event_id}-signal",
                    "signal_type": "character_identity",
                    "producer_family": "runtime",
                }
            ],
            "fused_events": [
                {
                    "event_id": event_id,
                    "event_type": "ability_plus_medal_combo",
                    "confidence": 0.84,
                    "final_score": final_score,
                    "gate_status": "confirmed",
                    "synergy_applied": True,
                    "minimum_required_signals_met": True,
                    "suggested_start_timestamp": 0.5,
                    "suggested_end_timestamp": 3.2,
                    "contributing_signals": [f"{event_id}-signal"],
                    "metadata": {"entity_id": "punisher", "ability_id": "ult"},
                }
            ],
            "fused_review": {
                "session_id": f"{fusion_id}-review",
                "reviewed_event_count": 1,
                "events": {event_id: {"review_status": review_status}},
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _prepare_dataset(root: Path) -> tuple[dict, Path]:
    registry_path = root / "registry.sqlite"
    candidates = [
        ("alpha", 0.88, "approved"),
        ("beta", 0.79, "approved"),
        ("gamma", 0.77, "rejected"),
        ("delta", 0.68, "rejected"),
    ]
    fused_paths: dict[str, Path] = {}
    for name, final_score, review_status in candidates:
        media = root / "media" / f"{name}.mp4"
        media.parent.mkdir(parents=True, exist_ok=True)
        media.write_bytes(name.encode("utf-8"))
        fused_path = root / "fused" / f"{name}.fused_analysis.json"
        _fused_sidecar(
            fused_path,
            game="marvel_rivals",
            source=media,
            fusion_id=f"fusion-{name}",
            event_id=f"fused-{name}",
            final_score=final_score,
            review_status=review_status,
        )
        fused_paths[name] = fused_path

    refresh_clip_registry(root, registry_path=registry_path)

    export_highlight_selection(
        fused_sidecar=fused_paths["alpha"],
        output_path=root / "selection" / "alpha.highlight_selection.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    derive_hook_candidates(
        fused_paths["alpha"],
        registry_path=registry_path,
        output_path=root / "hooks" / "alpha.hook_candidates.json",
    )
    derive_hook_candidates(
        fused_paths["beta"],
        registry_path=registry_path,
        output_path=root / "hooks" / "beta.hook_candidates.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    workflow = create_workflow_run(
        "export_queue",
        registry_path=registry_path,
        output_path=root / "workflow" / "export.workflow_run.json",
    )
    export_batch = create_highlight_export_batch(
        registry_path=registry_path,
        workflow_run_id=workflow["workflow_run_id"],
        output_path=root / "exports" / "batch.highlight_export_batch.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    ledger = record_post_ledger(
        export_batch["manifest_path"],
        workflow_run_id=workflow["workflow_run_id"],
        platform="youtube",
        account_id="acct-1",
        output_path=root / "posted" / "ledger.posted_highlight_ledger.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    record_posted_metrics_snapshot(
        ledger["manifest_path"],
        workflow_run_id=workflow["workflow_run_id"],
        platform="youtube",
        account_id="acct-1",
        output_path=root / "metrics" / "snapshot.posted_highlight_metrics_snapshot.json",
        view_count=420,
        like_count=36,
        comment_count=8,
        share_count=5,
        save_count=4,
        watch_time_seconds=120.0,
        average_watch_time_seconds=8.0,
        completion_rate=0.56,
        engagement_rate=0.13,
    )
    refresh_clip_registry(root, registry_path=registry_path)

    dataset = export_v2_training_datasets(
        registry_path=registry_path,
        output_root=root / "dataset_exports",
        game="marvel_rivals",
    )
    return dataset, registry_path


class ShadowModelTrainingTests(unittest.TestCase):
    def test_approved_target_rejected_review_outweighs_posted_lifecycle(self) -> None:
        value = _target_value(
            {
                "lifecycle_state": "posted",
                "review_outcome": "rejected",
            },
            training_target="approved_or_selected_probability",
        )
        self.assertEqual(value, 0.0)

    def test_approved_target_approved_review_outweighs_lifecycle(self) -> None:
        value = _target_value(
            {
                "lifecycle_state": "rejected",
                "review_outcome": "approved",
            },
            training_target="approved_or_selected_probability",
        )
        self.assertEqual(value, 1.0)

    def test_train_and_evaluate_shadow_model_with_registry_ingest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, registry_path = _prepare_dataset(root)

            linear_model = train_shadow_ranking_model(
                dataset["manifest_path"],
                model_output_path=root / "models" / "ranker.shadow_ranking_model.json",
                model_family="linear_shadow_ranker",
                split_key="candidate_id",
                train_fraction=0.75,
            )
            self.assertTrue(linear_model["ok"])
            self.assertEqual(linear_model["schema_version"], "shadow_ranking_model_v1")
            self.assertEqual(linear_model["training_target"], "approved_or_selected_probability")
            self.assertEqual(linear_model["row_count"], 4)
            self.assertEqual(linear_model["label_positive_count"] + linear_model["label_negative_count"], linear_model["train_row_count"])
            self.assertIn("candidate_score_weights", linear_model)
            self.assertIn("fused_confidence", linear_model["feature_fields"])
            self.assertIn("preferred_hook_clarity_score", linear_model["feature_fields"])

            boosted_model = train_shadow_ranking_model(
                dataset["manifest_path"],
                model_output_path=root / "models" / "boosted.shadow_ranking_model.json",
                model_family="gradient_boosted_shadow_ranker",
                split_key="candidate_id",
                train_fraction=0.75,
            )
            self.assertTrue(boosted_model["ok"])
            self.assertEqual(boosted_model["model_family"], "gradient_boosted_shadow_ranker")
            self.assertEqual(boosted_model["scoring_backend"], "boosted_stumps")
            self.assertIn("candidate_score_model", boosted_model)
            self.assertIn("trees", boosted_model["candidate_score_model"])

            linear_experiment = evaluate_shadow_ranking_model(
                model_path=linear_model["manifest_path"],
                dataset_manifest=dataset["manifest_path"],
                output_path=root / "experiments" / "linear.shadow_ranking_experiment.json",
            )
            self.assertTrue(linear_experiment["ok"])
            self.assertEqual(linear_experiment["schema_version"], "shadow_ranking_experiment_v1")
            self.assertEqual(linear_experiment["model_id"], linear_model["model_id"])
            self.assertIn(linear_experiment["comparison_recommendation"]["decision"], {"prefer_shadow", "keep_current", "inconclusive"})

            boosted_experiment = evaluate_shadow_ranking_model(
                model_path=boosted_model["manifest_path"],
                dataset_manifest=dataset["manifest_path"],
                output_path=root / "experiments" / "boosted.shadow_ranking_experiment.json",
            )
            self.assertTrue(boosted_experiment["ok"])
            self.assertEqual(boosted_experiment["model_id"], boosted_model["model_id"])

            family_comparison = compare_shadow_model_families(
                [linear_experiment["manifest_path"], boosted_experiment["manifest_path"]],
                output_path=root / "comparisons" / "families.shadow_model_family_comparison.json",
                training_target="approved_or_selected_probability",
            )
            self.assertTrue(family_comparison["ok"])
            self.assertEqual(family_comparison["schema_version"], "shadow_model_family_comparison_v1")
            self.assertEqual(family_comparison["row_count"], 2)
            self.assertEqual({row["model_family"] for row in family_comparison["rows"]}, {"linear_shadow_ranker", "gradient_boosted_shadow_ranker"})

            first = refresh_clip_registry(root, registry_path=registry_path)
            second = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["shadow_ranking_model_manifest_count"], 2)
            self.assertEqual(first["shadow_ranking_experiment_manifest_count"], 2)
            self.assertEqual(first["shadow_ranking_replay_manifest_count"], 2)
            self.assertEqual(first["shadow_ranking_comparison_report_count"], 2)
            self.assertEqual(first["shadow_model_family_comparison_manifest_count"], 1)

            model_rows = query_clip_registry(mode="shadow-ranking-models", registry_path=registry_path)
            boosted_model_rows = query_clip_registry(
                mode="shadow-ranking-models",
                registry_path=registry_path,
                model_family="gradient_boosted_shadow_ranker",
            )
            experiment_rows = query_clip_registry(mode="shadow-ranking-experiments", registry_path=registry_path)
            family_rows = query_clip_registry(mode="shadow-model-family-comparisons", registry_path=registry_path)
            self.assertEqual(model_rows["row_count"], 2)
            self.assertEqual(boosted_model_rows["row_count"], 1)
            self.assertEqual(experiment_rows["row_count"], 2)
            self.assertEqual(family_rows["row_count"], 2)
            self.assertEqual(boosted_model_rows["rows"][0]["training_target"], "approved_or_selected_probability")
            self.assertEqual({row["model_id"] for row in experiment_rows["rows"]}, {linear_model["model_id"], boosted_model["model_id"]})

    def test_train_shadow_model_degrades_cleanly_for_single_class(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = root / "registry.sqlite"
            media = root / "media" / "solo.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"solo")
            fused_path = root / "fused" / "solo.fused_analysis.json"
            _fused_sidecar(
                fused_path,
                game="marvel_rivals",
                source=media,
                fusion_id="fusion-solo",
                event_id="fused-solo",
                final_score=0.91,
                review_status="approved",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            dataset = export_v2_training_datasets(
                registry_path=registry_path,
                output_root=root / "dataset_exports",
                game="marvel_rivals",
            )

            model = train_shadow_ranking_model(dataset["manifest_path"])
            self.assertTrue(model["ok"])
            self.assertTrue(any(warning["code"] == "single_class_training_data" for warning in model["warnings"]))

    def test_train_shadow_model_supports_post_performance_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)

            model = train_shadow_ranking_model(
                dataset["manifest_path"],
                model_family="gradient_boosted_shadow_ranker",
                training_target="post_performance_score",
                split_key="candidate_id",
                train_fraction=0.75,
            )
            self.assertTrue(model["ok"])
            self.assertEqual(model["training_target"], "post_performance_score")
            self.assertEqual(model["model_family"], "gradient_boosted_shadow_ranker")
            self.assertEqual(model["row_count"], 1)
            self.assertTrue(any(warning["code"] == "single_class_training_data" for warning in model["warnings"]))
            self.assertNotIn("latest_view_count_norm", model["feature_fields"])
            self.assertNotIn("latest_completion_rate", model["feature_fields"])
            self.assertNotIn("latest_engagement_rate", model["feature_fields"])
            self.assertNotIn("post_present", model["feature_fields"])
            self.assertNotIn("metrics_present", model["feature_fields"])
            self.assertNotIn("is_approved", model["feature_fields"])
            self.assertNotIn("hook_mode_natural", model["feature_fields"])
            self.assertNotIn("hook_mode_reject", model["feature_fields"])
            self.assertNotIn("hook_archetype_flex", model["feature_fields"])
            self.assertNotIn("account_context_present", model["feature_fields"])
            self.assertEqual(
                model["feature_fields"],
                model["feature_fields_by_head"]["post_performance"],
            )

    def test_train_shadow_model_approved_target_omits_irrelevant_post_sparsity_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, _registry_path = _prepare_dataset(root)

            model = train_shadow_ranking_model(
                dataset["manifest_path"],
                training_target="approved_or_selected_probability",
                split_key="candidate_id",
                train_fraction=0.75,
            )
            self.assertTrue(model["ok"])
            self.assertFalse(any(warning["code"] == "sparse_post_performance_target" for warning in model["warnings"]))
