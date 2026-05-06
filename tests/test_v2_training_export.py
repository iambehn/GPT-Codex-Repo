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
from pipeline.v2_training_export import export_v2_training_datasets
from pipeline.workflow_run_state import create_workflow_run


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fused_sidecar(path: Path, *, game: str, source: Path, fusion_id: str, event_id: str) -> None:
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
                    "final_score": 0.92,
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
                "events": {event_id: {"review_status": "approved"}},
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _clone_with_name(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.loads(source.read_text(encoding="utf-8"))
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _prepare_registry(root: Path) -> tuple[Path, str, str]:
    registry_path = root / "registry.sqlite"

    alpha_media = root / "media" / "alpha.mp4"
    beta_media = root / "media" / "beta.mp4"
    alpha_media.parent.mkdir(parents=True, exist_ok=True)
    alpha_media.write_bytes(b"alpha-video")
    beta_media.write_bytes(b"beta-video")

    alpha_fused = root / "fused" / "alpha.fused_analysis.json"
    beta_fused = root / "fused" / "beta.fused_analysis.json"
    _fused_sidecar(alpha_fused, game="marvel_rivals", source=alpha_media, fusion_id="fusion-alpha", event_id="fused-alpha")
    _fused_sidecar(beta_fused, game="marvel_rivals", source=beta_media, fusion_id="fusion-beta", event_id="fused-beta")

    refresh_clip_registry(root, registry_path=registry_path)

    export_highlight_selection(
        fused_sidecar=alpha_fused,
        output_path=root / "selection" / "alpha.highlight_selection.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    derive_hook_candidates(
        alpha_fused,
        registry_path=registry_path,
        output_path=root / "hooks" / "alpha.hook_candidates.json",
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
        output_path=root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
        view_count=120,
        like_count=12,
        comment_count=3,
        share_count=2,
        save_count=1,
        watch_time_seconds=48.0,
        average_watch_time_seconds=6.0,
        completion_rate=0.44,
        engagement_rate=0.14,
    )
    record_posted_metrics_snapshot(
        ledger["manifest_path"],
        workflow_run_id=workflow["workflow_run_id"],
        platform="youtube",
        account_id="acct-1",
        output_path=root / "metrics" / "snapshot-b.posted_highlight_metrics_snapshot.json",
        view_count=300,
        like_count=27,
        comment_count=4,
        share_count=5,
        save_count=3,
        watch_time_seconds=80.0,
        average_watch_time_seconds=7.0,
        completion_rate=0.52,
        engagement_rate=0.12,
    )
    refresh_clip_registry(root, registry_path=registry_path)

    lifecycle_rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)
    candidate_by_source = {
        Path(str(row["source"])).name: str(row["candidate_id"])
        for row in lifecycle_rows["rows"]
    }
    return registry_path, candidate_by_source["alpha.mp4"], candidate_by_source["beta.mp4"]


class V2TrainingExportTests(unittest.TestCase):
    def test_export_v2_training_datasets_writes_all_views(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, posted_candidate_id, sparse_candidate_id = _prepare_registry(root)

            result = export_v2_training_datasets(
                registry_path=registry_path,
                output_root=root / "dataset_exports",
                game="marvel_rivals",
            )

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "v2_training_dataset_export_v1")
            self.assertEqual(manifest["coverage_counts"]["candidate_count"], 2)
            self.assertEqual(manifest["coverage_counts"]["hook_count"], 1)
            self.assertEqual(manifest["coverage_counts"]["outcome_count"], 1)
            self.assertEqual(manifest["coverage_counts"]["performance_count"], 2)
            self.assertEqual(manifest["warning_count"], 0)

            candidate_rows = _read_jsonl(Path(result["dataset_views"]["candidates"]["jsonl_path"]))
            hook_rows = _read_jsonl(Path(result["dataset_views"]["hooks"]["jsonl_path"]))
            outcome_rows = _read_jsonl(Path(result["dataset_views"]["outcomes"]["jsonl_path"]))
            performance_rows = _read_jsonl(Path(result["dataset_views"]["performance"]["jsonl_path"]))

            self.assertEqual(len(candidate_rows), 2)
            self.assertEqual(len(hook_rows), 1)
            self.assertEqual(len(outcome_rows), 1)
            self.assertEqual(len(performance_rows), 2)

            posted_candidate = next(row for row in candidate_rows if row["candidate_id"] == posted_candidate_id)
            sparse_candidate = next(row for row in candidate_rows if row["candidate_id"] == sparse_candidate_id)

            self.assertEqual(posted_candidate["review_outcome"], "approved")
            self.assertTrue(posted_candidate["export_present"])
            self.assertTrue(posted_candidate["post_present"])
            self.assertTrue(posted_candidate["metrics_present"])
            self.assertEqual(posted_candidate["fused_event_type"], "ability_plus_medal_combo")
            self.assertTrue(posted_candidate["fused_synergy_applied"])
            self.assertEqual(posted_candidate["fused_contributing_signal_count"], 1)
            self.assertTrue(posted_candidate["fused_ability_present"])
            self.assertEqual(posted_candidate["preferred_hook_mode"], "natural")
            self.assertTrue(posted_candidate["preferred_hook_archetype"])
            self.assertIn("preferred_hook_clarity_score", posted_candidate)
            self.assertEqual(
                posted_candidate[f"hook_archetype_{posted_candidate['preferred_hook_archetype']}"],
                1.0,
            )
            self.assertEqual(posted_candidate["coverage_tier"], "posted_with_metrics")
            self.assertIn("latest_view_count_norm", posted_candidate)
            self.assertEqual(posted_candidate["latest_view_count"], 300)
            self.assertEqual(posted_candidate["latest_metrics_coverage_status"], "complete")
            self.assertEqual(posted_candidate["selected_highlight_fusion_id"], "fusion-alpha")
            self.assertEqual(posted_candidate["selected_highlight_event_type"], "ability_plus_medal_combo")
            self.assertEqual(posted_candidate["selected_highlight_gate_status"], "confirmed")
            self.assertEqual(posted_candidate["selected_highlight_contributing_producer_families"], ["runtime"])
            self.assertEqual(posted_candidate["latest_post_performance_coverage_tier"], "posted_usable_metrics")
            self.assertTrue(posted_candidate["latest_post_performance_label_eligible"])
            self.assertIsNotNone(posted_candidate["latest_post_performance_target_score"])
            self.assertIn(posted_candidate["latest_post_performance_target_bucket"], {"low", "medium", "high"})
            self.assertTrue(posted_candidate["latest_post_performance_recoverable"])
            self.assertEqual(posted_candidate["latest_post_performance_missing_fields"], [])

            self.assertEqual(sparse_candidate["lifecycle_state"], "approved")
            self.assertFalse(sparse_candidate["hook_candidate_present"])
            self.assertFalse(sparse_candidate["export_present"])
            self.assertFalse(sparse_candidate["post_present"])
            self.assertFalse(sparse_candidate["metrics_present"])
            self.assertEqual(sparse_candidate["coverage_tier"], "reviewed")
            self.assertEqual(sparse_candidate["preferred_hook_mode_natural"], 0.0)
            self.assertEqual(sparse_candidate["latest_post_performance_coverage_tier"], "no_post_record")
            self.assertFalse(sparse_candidate["latest_post_performance_label_eligible"])
            self.assertFalse(sparse_candidate["latest_post_performance_recoverable"])

            self.assertEqual(hook_rows[0]["candidate_id"], posted_candidate_id)
            self.assertEqual(hook_rows[0]["hook_mode"], "natural")
            self.assertEqual(hook_rows[0]["hook_mode_natural"], 1.0)
            self.assertEqual(
                hook_rows[0][f"hook_archetype_{hook_rows[0]['hook_archetype']}"],
                1.0,
            )
            self.assertTrue(hook_rows[0]["packaging_strategy_present"])
            self.assertTrue(hook_rows[0]["metadata_entity_present"])
            self.assertIn("hook_strength", hook_rows[0])

            self.assertEqual(outcome_rows[0]["candidate_id"], posted_candidate_id)
            self.assertEqual(outcome_rows[0]["platform"], "youtube")
            self.assertIsNotNone(outcome_rows[0]["export_id"])
            self.assertIsNotNone(outcome_rows[0]["post_record_id"])
            self.assertEqual(outcome_rows[0]["selected_highlight_fusion_id"], "fusion-alpha")
            self.assertEqual(outcome_rows[0]["selected_highlight_event_type"], "ability_plus_medal_combo")
            self.assertEqual(outcome_rows[0]["selected_highlight_contributing_producer_families"], ["runtime"])
            self.assertEqual(outcome_rows[0]["latest_post_performance_coverage_tier"], "posted_usable_metrics")
            self.assertTrue(outcome_rows[0]["latest_post_performance_label_eligible"])
            self.assertTrue(outcome_rows[0]["latest_post_performance_recoverable"])

            latest_performance = next(row for row in performance_rows if row["is_latest_snapshot"])
            self.assertEqual(latest_performance["candidate_id"], posted_candidate_id)
            self.assertEqual(latest_performance["view_count"], 300)
            self.assertEqual(latest_performance["hook_mode"], "natural")
            self.assertEqual(latest_performance["selected_highlight_fusion_id"], "fusion-alpha")
            self.assertEqual(latest_performance["selected_highlight_event_type"], "ability_plus_medal_combo")
            self.assertEqual(latest_performance["selected_highlight_contributing_producer_families"], ["runtime"])
            self.assertEqual(latest_performance["post_performance_coverage_tier"], "posted_usable_metrics")
            self.assertTrue(latest_performance["post_performance_label_eligible"])
            self.assertTrue(latest_performance["post_performance_recoverable"])
            self.assertEqual(latest_performance["post_performance_missing_fields"], [])

    def test_export_v2_training_datasets_filters_real_only_vs_synthetic_augmented(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, posted_candidate_id, _sparse_candidate_id = _prepare_registry(root)

            _clone_with_name(
                root / "posted" / "ledger.posted_highlight_ledger.json",
                root / "synthetic" / "synthetic.posted_highlight_ledger.json",
            )
            _clone_with_name(
                root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
                root / "synthetic" / "synthetic-a.posted_highlight_metrics_snapshot.json",
            )
            _clone_with_name(
                root / "metrics" / "snapshot-b.posted_highlight_metrics_snapshot.json",
                root / "synthetic" / "synthetic-b.posted_highlight_metrics_snapshot.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)

            real_only = export_v2_training_datasets(
                registry_path=registry_path,
                output_root=root / "dataset_exports" / "real",
                game="marvel_rivals",
                evidence_mode="real_only",
            )
            synthetic_only = export_v2_training_datasets(
                registry_path=registry_path,
                output_root=root / "dataset_exports" / "synthetic",
                game="marvel_rivals",
                evidence_mode="synthetic_augmented",
            )

            self.assertTrue(real_only["ok"])
            self.assertTrue(synthetic_only["ok"])

            real_manifest = json.loads(Path(real_only["manifest_path"]).read_text(encoding="utf-8"))
            synthetic_manifest = json.loads(Path(synthetic_only["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(real_manifest["filters"]["evidence_mode"], "real_only")
            self.assertEqual(synthetic_manifest["filters"]["evidence_mode"], "synthetic_augmented")

            real_candidate_rows = _read_jsonl(Path(real_only["dataset_views"]["candidates"]["jsonl_path"]))
            synthetic_candidate_rows = _read_jsonl(Path(synthetic_only["dataset_views"]["candidates"]["jsonl_path"]))
            real_performance_rows = _read_jsonl(Path(real_only["dataset_views"]["performance"]["jsonl_path"]))
            synthetic_performance_rows = _read_jsonl(Path(synthetic_only["dataset_views"]["performance"]["jsonl_path"]))

            posted_real = next(row for row in real_candidate_rows if row["candidate_id"] == posted_candidate_id)
            posted_synthetic = next(row for row in synthetic_candidate_rows if row["candidate_id"] == posted_candidate_id)
            self.assertEqual(posted_real["evidence_mode"], "real_only")
            self.assertEqual(posted_real["latest_post_performance_evidence_mode"], "real_only")
            self.assertEqual(posted_synthetic["evidence_mode"], "synthetic_augmented")
            self.assertEqual(posted_synthetic["latest_post_performance_evidence_mode"], "synthetic_augmented")
            self.assertTrue(all(row["evidence_mode"] == "real_only" for row in real_performance_rows))
            self.assertTrue(all(row["evidence_mode"] == "synthetic_augmented" for row in synthetic_performance_rows))

    def test_export_v2_training_datasets_filters_and_is_stable(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, posted_candidate_id, _ = _prepare_registry(root)

            first = export_v2_training_datasets(
                registry_path=registry_path,
                output_root=root / "dataset_exports",
                candidate_id=posted_candidate_id,
                hook_mode="natural",
                platform="youtube",
                account_id="acct-1",
            )
            second = export_v2_training_datasets(
                registry_path=registry_path,
                output_root=root / "dataset_exports",
                candidate_id=posted_candidate_id,
                hook_mode="natural",
                platform="youtube",
                account_id="acct-1",
            )

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["dataset_export_id"], second["dataset_export_id"])

            first_candidate_rows = _read_jsonl(Path(first["dataset_views"]["candidates"]["jsonl_path"]))
            second_candidate_rows = _read_jsonl(Path(second["dataset_views"]["candidates"]["jsonl_path"]))
            self.assertEqual(first_candidate_rows, second_candidate_rows)
            self.assertEqual(len(first_candidate_rows), 1)
            self.assertEqual(first_candidate_rows[0]["candidate_id"], posted_candidate_id)

            performance_rows = _read_jsonl(Path(first["dataset_views"]["performance"]["jsonl_path"]))
            self.assertEqual(len(performance_rows), 2)
            self.assertTrue(all(row["platform"] == "youtube" for row in performance_rows))
