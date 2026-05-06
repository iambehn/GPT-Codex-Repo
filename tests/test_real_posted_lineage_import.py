from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.real_posted_lineage_import import (
    advise_real_artifact_intake_dedup,
    bootstrap_real_artifact_intake_bundle,
    import_real_posted_lineage,
    materialize_real_artifact_intake_dedup_resolutions,
    preflight_real_artifact_intake_refresh,
    record_real_artifact_intake_preflight_history,
    record_real_artifact_intake_refresh_outcome_history,
    report_real_artifact_intake_coverage,
    report_real_artifact_intake_dashboard_summary_trends,
    report_real_artifact_intake_history_comparison,
    report_real_artifact_intake_preflight_trends,
    report_real_artifact_intake_refresh_outcome_trends,
    record_real_artifact_intake_dashboard_summary_history,
    render_real_artifact_intake_dashboard,
    refresh_real_artifact_intake,
    refresh_real_only_benchmark,
    summarize_real_artifact_intake,
    summarize_real_artifact_intake_comparison_targets,
    summarize_real_artifact_intake_dashboard_registry,
    summarize_real_artifact_intake_dashboard_summary_history,
    summarize_real_artifact_intake_dedup_resolutions,
    summarize_real_artifact_intake_preflight_history,
    summarize_real_artifact_intake_refresh_outcome_history,
    update_real_artifact_intake_dedup_resolution,
    validate_real_artifact_intake,
)
from pipeline.v2_training_export import export_v2_training_datasets
from tests.test_v2_training_export import _clone_with_name, _prepare_registry


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _stage_real_intake_bundle(source_root: Path, intake_root: Path, *, include_synthetic: bool = False) -> Path:
    bundle_root = intake_root / "bundles" / "session-001"
    _write_bundle_manifest(bundle_root, game="marvel_rivals", platform="youtube")
    _clone_with_name(source_root / "fused" / "alpha.fused_analysis.json", bundle_root / "fused" / "alpha.fused_analysis.json")
    _clone_with_name(source_root / "fused" / "beta.fused_analysis.json", bundle_root / "fused" / "beta.fused_analysis.json")
    _clone_with_name(source_root / "hooks" / "alpha.hook_candidates.json", bundle_root / "hooks" / "alpha.hook_candidates.json")
    _clone_with_name(source_root / "selection" / "alpha.highlight_selection.json", bundle_root / "selection" / "alpha.highlight_selection.json")
    _clone_with_name(source_root / "exports" / "batch.highlight_export_batch.json", bundle_root / "exports" / "batch.highlight_export_batch.json")
    _clone_with_name(source_root / "posted" / "ledger.posted_highlight_ledger.json", bundle_root / "posted" / "ledger.posted_highlight_ledger.json")
    _clone_with_name(
        source_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
        bundle_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
    )
    _clone_with_name(
        source_root / "metrics" / "snapshot-b.posted_highlight_metrics_snapshot.json",
        bundle_root / "metrics" / "snapshot-b.posted_highlight_metrics_snapshot.json",
    )
    if include_synthetic:
        _clone_with_name(
            source_root / "posted" / "ledger.posted_highlight_ledger.json",
            bundle_root / "synthetic" / "synthetic.posted_highlight_ledger.json",
        )
        _clone_with_name(
            source_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
            bundle_root / "synthetic" / "synthetic.posted_highlight_metrics_snapshot.json",
        )
    return bundle_root


def _stage_downstream_only_bundle(source_root: Path, intake_root: Path) -> Path:
    bundle_root = intake_root / "bundles" / "session-downstream"
    _write_bundle_manifest(bundle_root, game="marvel_rivals", platform="youtube")
    _clone_with_name(source_root / "exports" / "batch.highlight_export_batch.json", bundle_root / "exports" / "batch.highlight_export_batch.json")
    _clone_with_name(source_root / "posted" / "ledger.posted_highlight_ledger.json", bundle_root / "posted" / "ledger.posted_highlight_ledger.json")
    _clone_with_name(
        source_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
        bundle_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
    )
    return bundle_root


def _stage_duplicate_real_intake_bundle(source_root: Path, intake_root: Path, *, bundle_name: str) -> Path:
    bundle_root = intake_root / "bundles" / bundle_name
    _write_bundle_manifest(bundle_root, game="marvel_rivals", platform="youtube")
    _clone_with_name(source_root / "exports" / "batch.highlight_export_batch.json", bundle_root / "exports" / "batch.highlight_export_batch.json")
    _clone_with_name(source_root / "posted" / "ledger.posted_highlight_ledger.json", bundle_root / "posted" / "ledger.posted_highlight_ledger.json")
    _clone_with_name(
        source_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
        bundle_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
    )
    return bundle_root


def _write_bundle_manifest(
    bundle_root: Path,
    *,
    game: str,
    platform: str,
    date_start: str = "2026-05-01",
    date_end: str = "2026-05-02",
    expected_artifact_types: list[str] | None = None,
) -> None:
    bundle_root.mkdir(parents=True, exist_ok=True)
    (bundle_root / "bundle.manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "real_artifact_intake_bundle_manifest_v1",
                "bundle_name": bundle_root.name,
                "source": {
                    "label": "unit-test-drop",
                    "kind": "local_drop",
                    "description": "staged from fixture source",
                },
                "game": game,
                "platform": platform,
                "date_range": {
                    "start": date_start,
                    "end": date_end,
                },
                "operator_notes": "test bundle",
                "completeness_expectations": {
                    "expected_artifact_types": expected_artifact_types
                    or [
                        "fused_analysis",
                        "highlight_export_batch",
                        "posted_highlight_ledger",
                        "posted_highlight_metrics_snapshot",
                    ],
                    "notes": "fixture coverage",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )


class RealPostedLineageImportTests(unittest.TestCase):
    def test_bootstrap_real_artifact_intake_bundle_creates_expected_structure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            intake_root = Path(tempdir) / "intake"
            result = bootstrap_real_artifact_intake_bundle(
                "session 001",
                intake_root=intake_root,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["schema_version"], "real_artifact_intake_bundle_bootstrap_v1")
            self.assertEqual(result["bundle_name"], "session-001")
            bundle_root = Path(result["bundle_root"])
            self.assertTrue((bundle_root / "fused").is_dir())
            self.assertTrue((bundle_root / "hooks").is_dir())
            self.assertTrue((bundle_root / "selection").is_dir())
            self.assertTrue((bundle_root / "exports").is_dir())
            self.assertTrue((bundle_root / "posted").is_dir())
            self.assertTrue((bundle_root / "metrics").is_dir())
            manifest = json.loads(Path(result["bundle_manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "real_artifact_intake_bundle_manifest_v1")
            self.assertEqual(manifest["bundle_name"], "session-001")
            checklist = Path(result["checklist_path"]).read_text(encoding="utf-8")
            self.assertIn("Real Artifact Intake Bundle: session-001", checklist)
            self.assertIn("bundle.manifest.json", checklist)
            self.assertIn("validate-real-artifact-intake", checklist)

    def test_validate_real_artifact_intake_handles_empty_root(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            intake_root = Path(tempdir) / "intake"
            result = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["bundle_count"], 0)
            self.assertEqual(result["intake_status"], "empty_intake_root")
            self.assertEqual(result["coverage_inventory"]["imported_candidate_count"], 0)
            self.assertEqual(result["coverage_inventory"]["selected_event_type_counts"], {})
            self.assertEqual(result["coverage_inventory"]["selected_producer_family_counts"], {})
            self.assertEqual(result["bundle_readiness_rollups"]["benchmark_ready_bundle_count"], 0)
            self.assertTrue(Path(result["manifest_path"]).exists())

    def test_import_real_posted_lineage_excludes_synthetic_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            workspace_root = root / "workspace"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _clone_with_name(
                source_root / "posted" / "ledger.posted_highlight_ledger.json",
                source_root / "synthetic" / "synthetic.posted_highlight_ledger.json",
            )
            _clone_with_name(
                source_root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
                source_root / "synthetic" / "synthetic-a.posted_highlight_metrics_snapshot.json",
            )

            workspace_registry = workspace_root / "registry.sqlite"
            first = import_real_posted_lineage(
                source_roots=[source_root],
                registry_path=workspace_registry,
                game="marvel_rivals",
                output_path=workspace_root / "imports" / "real.real_posted_lineage_import.json",
            )
            second = import_real_posted_lineage(
                source_roots=[source_root],
                registry_path=workspace_registry,
                game="marvel_rivals",
                output_path=workspace_root / "imports" / "real.real_posted_lineage_import.json",
            )

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["imported_counts"]["post_ledger_manifest_count"], 1)
            self.assertEqual(first["imported_counts"]["posted_metrics_snapshot_manifest_count"], 2)
            self.assertEqual(second["imported_counts"]["post_ledger_manifest_count"], 1)

            imports = query_clip_registry(mode="real-posted-lineage-imports", registry_path=workspace_registry)
            metrics = query_clip_registry(mode="posted-metrics", registry_path=workspace_registry, evidence_mode="real_only")
            synthetic_metrics = query_clip_registry(mode="posted-metrics", registry_path=workspace_registry, evidence_mode="synthetic_augmented")

            self.assertEqual(imports["row_count"], 1)
            self.assertEqual(metrics["row_count"], 2)
            self.assertEqual(synthetic_metrics["row_count"], 0)

            payload = json.loads(Path(first["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "real_posted_lineage_import_v1")
            self.assertIn(str(source_root.resolve()), payload["source_roots"])
            self.assertEqual(payload["coverage_inventory"]["imported_candidate_count"], 2)
            self.assertEqual(payload["coverage_inventory"]["imported_hook_count"], 1)
            self.assertEqual(payload["coverage_inventory"]["eligible_real_post_performance_label_count"], 2)
            self.assertEqual(payload["coverage_inventory"]["selected_event_type_counts"], {"ability_plus_medal_combo": 1})
            self.assertEqual(payload["coverage_inventory"]["selected_producer_family_counts"], {"runtime": 1})

            exported = export_v2_training_datasets(
                registry_path=workspace_registry,
                output_root=workspace_root / "dataset_exports",
                game="marvel_rivals",
                evidence_mode="real_only",
            )
            self.assertTrue(exported["ok"])
            exported_manifest = json.loads(Path(exported["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(exported_manifest["coverage_counts"]["candidate_count"], 2)
            self.assertEqual(exported_manifest["coverage_counts"]["outcome_count"], 1)
            self.assertEqual(exported_manifest["coverage_counts"]["performance_count"], 2)

    def test_validate_real_artifact_intake_reports_lineage_complete_bundle_and_flags_synthetic(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root, include_synthetic=True)

            result = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "validation.json",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["bundle_count"], 1)
            self.assertEqual(result["intake_status"], "benchmark_ready")
            self.assertEqual(result["discovered_synthetic_artifact_count"], 2)
            self.assertEqual(result["coverage_inventory"]["imported_candidate_count"], 2)
            self.assertEqual(result["coverage_inventory"]["imported_post_count"], 1)
            self.assertEqual(result["coverage_inventory"]["eligible_real_post_performance_label_count"], 2)
            self.assertEqual(result["coverage_inventory"]["selected_event_type_counts"], {"ability_plus_medal_combo": 1})
            self.assertEqual(result["coverage_inventory"]["selected_producer_family_counts"], {"runtime": 1})
            self.assertEqual(result["bundle_summaries"][0]["status"], "lineage_complete")
            self.assertEqual(result["bundle_summaries"][0]["readiness_status"], "benchmark_ready")
            self.assertEqual(result["bundle_summaries"][0]["dominant_gap_reason"], "benchmark_ready")
            self.assertTrue(result["bundle_summaries"][0]["bundle_manifest_present"])
            self.assertTrue(result["bundle_summaries"][0]["bundle_manifest_valid"])
            self.assertEqual(result["bundle_summaries"][0]["manifest_game"], "marvel_rivals")
            self.assertEqual(result["bundle_summaries"][0]["eligible_post_performance_label_count"], 2)
            self.assertEqual(result["bundle_readiness_rollups"]["benchmark_ready_bundle_count"], 1)
            self.assertTrue(Path(result["manifest_path"]).exists())

    def test_validate_real_artifact_intake_reports_downstream_only_gap_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_downstream_only_bundle(source_root, intake_root)

            result = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["bundle_count"], 1)
            self.assertEqual(result["bundle_summaries"][0]["readiness_status"], "downstream_only")
            self.assertEqual(result["bundle_summaries"][0]["dominant_gap_reason"], "missing_fused_lineage")
            self.assertEqual(result["bundle_summaries"][0]["missing_required_artifact_types"][0], "fused_analysis")
            self.assertEqual(result["bundle_readiness_rollups"]["missing_fused_lineage_bundle_count"], 1)

    def test_validate_real_artifact_intake_requires_bundle_manifest_for_benchmark_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            bundle_root = _stage_real_intake_bundle(source_root, intake_root)
            (bundle_root / "bundle.manifest.json").unlink()

            result = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["intake_status"], "lineage_complete_without_required_metadata")
            self.assertEqual(result["bundle_summaries"][0]["readiness_status"], "partial_lineage")
            self.assertEqual(result["bundle_summaries"][0]["dominant_gap_reason"], "missing_bundle_manifest")
            self.assertIn("bundle_manifest", result["bundle_summaries"][0]["missing_required_artifact_types"])
            self.assertEqual(result["bundle_readiness_rollups"]["benchmark_ready_bundle_count"], 0)
            self.assertEqual(result["bundle_readiness_rollups"]["missing_bundle_manifest_bundle_count"], 1)

    def test_validate_real_artifact_intake_lints_manifest_dates_and_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            bundle_root = _stage_real_intake_bundle(source_root, intake_root)
            _write_bundle_manifest(
                bundle_root,
                game="valorant",
                platform="twitch",
                date_start="2026/05/02",
                date_end="2026-05-01",
            )

            result = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["bundle_summaries"][0]["readiness_status"], "partial_lineage")
            self.assertEqual(result["bundle_summaries"][0]["dominant_gap_reason"], "invalid_bundle_manifest")
            self.assertIn("bundle_manifest_metadata", result["bundle_summaries"][0]["missing_required_artifact_types"])
            self.assertIn("game must match the validation game filter", result["bundle_summaries"][0]["bundle_manifest_errors"])
            self.assertIn("platform must match the validation platform filter", result["bundle_summaries"][0]["bundle_manifest_errors"])
            self.assertIn("date_range.start must use YYYY-MM-DD", result["bundle_summaries"][0]["bundle_manifest_errors"])

    def test_validate_real_artifact_intake_lints_declared_expected_artifact_types(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            bundle_root = _stage_real_intake_bundle(source_root, intake_root)
            _write_bundle_manifest(
                bundle_root,
                game="marvel_rivals",
                platform="youtube",
                expected_artifact_types=[
                    "fused_analysis",
                    "highlight_export_batch",
                    "posted_highlight_ledger",
                    "posted_highlight_metrics_snapshot",
                    "workflow_run",
                ],
            )

            result = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["bundle_summaries"][0]["dominant_gap_reason"], "invalid_bundle_manifest")
            self.assertIn(
                "declared expected_artifact_types missing from bundle artifacts: workflow_run",
                result["bundle_summaries"][0]["bundle_manifest_errors"],
            )

    def test_summarize_real_artifact_intake_reads_validation_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "validation.json",
            )

            summary = summarize_real_artifact_intake(validation["manifest_path"])
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["schema_version"], "real_artifact_intake_summary_v1")
            self.assertEqual(summary["bundle_readiness_rollups"]["benchmark_ready_bundle_count"], 1)
            self.assertEqual(summary["bundle_summaries"][0]["readiness_status"], "benchmark_ready")
            self.assertEqual(summary["bundle_summaries"][0]["dominant_gap_reason"], "benchmark_ready")
            self.assertEqual(summary["bundle_summaries"][0]["manifest_source_label"], "unit-test-drop")

    def test_report_real_artifact_intake_coverage_prefers_validation_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            _stage_duplicate_real_intake_bundle(source_root, intake_root, bundle_name="session-002")
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "validation.json",
            )

            (intake_root / "bundles" / "session-002").rename(intake_root / "bundles" / "_moved-session-002")

            report = report_real_artifact_intake_coverage(validation["manifest_path"])
            self.assertTrue(report["ok"])
            self.assertEqual(report["schema_version"], "real_artifact_intake_coverage_report_v1")
            self.assertEqual(report["bundle_count"], 2)
            self.assertEqual(report["bundle_count_by_game"]["marvel_rivals"], 2)
            self.assertEqual(report["bundle_count_by_platform"]["youtube"], 2)
            self.assertGreater(report["duplicate_downstream_summary"]["duplicate_downstream_record_total"], 0)
            self.assertEqual(
                report["duplicate_downstream_summary"]["duplicate_downstream_record_count_by_artifact_type"][
                    "posted_highlight_ledger"
                ],
                1,
            )
            self.assertEqual(report["sufficiency_assessment"]["status"], "ready_for_real_only_refresh")

    def test_advise_real_artifact_intake_dedup_emits_cleanup_recommendations(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            _stage_duplicate_real_intake_bundle(source_root, intake_root, bundle_name="session-002")
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "validation.json",
            )

            advisory = advise_real_artifact_intake_dedup(validation["manifest_path"])
            self.assertTrue(advisory["ok"])
            self.assertEqual(advisory["schema_version"], "real_artifact_intake_dedup_advisory_v1")
            self.assertGreater(advisory["duplicate_group_count"], 0)
            self.assertGreater(advisory["recommended_cleanup_group_count"], 0)
            first = advisory["advisories"][0]
            self.assertEqual(first["canonical_bundle_name"], "session-001")
            self.assertIn("session-002", first["non_canonical_bundle_names"])
            self.assertIn("keep_canonical_bundle", first["recommended_actions"])
            self.assertIn("remove_duplicate_downstream_lineage", first["recommended_actions"])
            self.assertIn("review_before_cleanup", first["recommended_actions"])

    def test_materialize_and_summarize_real_artifact_intake_dedup_resolutions(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            _stage_duplicate_real_intake_bundle(source_root, intake_root, bundle_name="session-002")
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "validation.json",
            )
            advisory = advise_real_artifact_intake_dedup(validation["manifest_path"])

            materialized = materialize_real_artifact_intake_dedup_resolutions(advisory["manifest_path"], intake_root=intake_root)
            self.assertTrue(materialized["ok"])
            self.assertGreater(materialized["created_count"], 0)
            first_resolution_path = Path(materialized["resolutions"][0]["resolution_path"])
            self.assertTrue(first_resolution_path.exists())
            first_payload = json.loads(first_resolution_path.read_text(encoding="utf-8"))
            self.assertEqual(first_payload["schema_version"], "real_artifact_intake_dedup_resolution_v1")
            self.assertEqual(first_payload["status"], "pending")

            summary = summarize_real_artifact_intake_dedup_resolutions(advisory["manifest_path"], intake_root=intake_root)
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["schema_version"], "real_artifact_intake_dedup_resolution_summary_v1")
            self.assertGreater(summary["status_counts"]["pending"], 0)
            self.assertEqual(summary["unresolved_count"], summary["status_counts"]["pending"])

    def test_update_real_artifact_intake_dedup_resolution_updates_status_and_validation_rollup(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            _stage_duplicate_real_intake_bundle(source_root, intake_root, bundle_name="session-002")
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "validation.json",
            )
            advisory = advise_real_artifact_intake_dedup(validation["manifest_path"])
            materialize_real_artifact_intake_dedup_resolutions(advisory["manifest_path"], intake_root=intake_root)
            for advisory_row in advisory["advisories"]:
                updated = update_real_artifact_intake_dedup_resolution(
                    advisory["manifest_path"],
                    group_id=advisory_row["group_id"],
                    status="accepted",
                    reviewed_by="tj",
                    notes="canonical bundle confirmed",
                    intake_root=intake_root,
                )
                self.assertTrue(updated["ok"])
                self.assertEqual(updated["resolution_status"], "accepted")
                self.assertEqual(updated["reviewed_by"], "tj")

            summary = summarize_real_artifact_intake_dedup_resolutions(advisory["manifest_path"], intake_root=intake_root)
            self.assertEqual(summary["status_counts"]["accepted"], len(advisory["advisories"]))
            self.assertEqual(summary["unresolved_count"], 0)

            refreshed_validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertEqual(
                refreshed_validation["dedup_resolution_summary"]["status_counts"]["accepted"],
                len(advisory["advisories"]),
            )
            self.assertFalse(refreshed_validation["dedup_resolution_summary"]["has_unresolved_duplicate_groups"])

    def test_refresh_real_only_benchmark_runs_end_to_end_on_imported_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            workspace_root = root / "workspace"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            result = refresh_real_only_benchmark(
                source_roots=[source_root],
                registry_path=workspace_root / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                output_root=workspace_root / "refresh",
                import_output_path=workspace_root / "imports" / "real_only.real_posted_lineage_import.json",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["import_summary"]["candidate_lifecycle_row_count"], 2)
            self.assertEqual(result["dataset_coverage_counts"]["candidate_count"], 1)
            self.assertEqual(result["dataset_coverage_counts"]["outcome_count"], 1)
            self.assertEqual(result["dataset_coverage_counts"]["performance_count"], 2)
            self.assertTrue(Path(result["import_manifest_path"]).exists())
            self.assertTrue(Path(result["dataset_manifest_path"]).exists())
            self.assertTrue(Path(result["benchmark_manifest_path"]).exists())
            self.assertTrue(Path(result["review_manifest_path"]).exists())

    def test_refresh_real_artifact_intake_runs_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                output_root=root / "workspace" / "refresh",
                output_path=root / "workspace" / "imports" / "validation.json",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["validation_summary"]["bundle_count"], 1)
            self.assertEqual(result["validation_summary"]["intake_status"], "benchmark_ready")
            self.assertEqual(result["dataset_coverage_counts"]["candidate_count"], 1)
            self.assertTrue(Path(result["validation_manifest_path"]).exists())
            self.assertTrue(Path(result["import_manifest_path"]).exists())
            self.assertTrue(Path(result["dataset_manifest_path"]).exists())
            self.assertTrue(Path(result["benchmark_manifest_path"]).exists())
            self.assertTrue(Path(result["review_manifest_path"]).exists())

    def test_refresh_real_artifact_intake_can_record_dashboard_summary_history(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                record_dashboard_summary_history=True,
                output_root=root / "workspace" / "refresh",
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["dashboard_summary_history_recorded"])
            history_manifest_path = Path(result["dashboard_summary_history_manifest_path"])
            self.assertTrue(history_manifest_path.exists())
            history_payload = json.loads(history_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(history_payload["schema_version"], "real_artifact_intake_dashboard_summary_history_v1")
            summary = summarize_real_artifact_intake_dashboard_summary_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["entry_count"], 1)

    def test_refresh_real_artifact_intake_can_record_refresh_outcome_history(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                record_refresh_outcome_history=True,
                output_root=root / "workspace" / "refresh",
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["refresh_outcome_history_recorded"])
            history_manifest_path = Path(result["refresh_outcome_history_manifest_path"])
            self.assertTrue(history_manifest_path.exists())
            history_payload = json.loads(history_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(history_payload["schema_version"], "real_artifact_intake_refresh_outcome_history_v1")
            self.assertEqual(history_payload["refresh"]["status"], "ok")
            summary = summarize_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["entry_count"], 1)

    def test_refresh_real_artifact_intake_can_render_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            record_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            record_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                render_dashboard=True,
                output_root=root / "workspace" / "refresh",
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["dashboard_rendered"])
            dashboard_manifest_path = Path(result["dashboard_manifest_path"])
            self.assertTrue(dashboard_manifest_path.exists())
            dashboard_payload = json.loads(dashboard_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(dashboard_payload["schema_version"], "real_artifact_intake_dashboard_v1")

    def test_refresh_real_artifact_intake_can_create_evidence_comparison_and_feed_dashboard(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            record_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            record_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            synthetic_review_manifest = intake_root / "reports" / "synthetic.shadow_benchmark_review.json"
            _write_json(
                synthetic_review_manifest,
                {
                    "schema_version": "shadow_benchmark_review_v1",
                    "review_id": "review-synthetic",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "target_reviews": [
                        {
                            "training_target": "approved_or_selected_probability",
                            "current_best_family": "gradient_boosted_shadow_ranker",
                            "best_recommendation_decision": "prefer_shadow",
                            "current_best_evidence_mode": "synthetic_augmented",
                            "primary_metric_name": "top_k_recall",
                            "primary_metric_delta": 0.2,
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

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                comparison_manifest=synthetic_review_manifest,
                render_dashboard=True,
                output_root=root / "workspace" / "refresh",
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["evidence_comparison_created"])
            comparison_manifest_path = Path(result["evidence_comparison_manifest_path"])
            self.assertTrue(comparison_manifest_path.exists())
            comparison_payload = json.loads(comparison_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(comparison_payload["schema_version"], "shadow_benchmark_evidence_comparison_v1")
            self.assertEqual(comparison_payload["real_manifest_path"], result["review_manifest_path"])
            self.assertEqual(comparison_payload["synthetic_manifest_path"], str(synthetic_review_manifest.resolve()))
            self.assertTrue(result["dashboard_rendered"])
            dashboard_payload = json.loads(Path(result["dashboard_manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(
                dashboard_payload["history_comparison"]["comparison_manifest_path"],
                str(comparison_manifest_path),
            )

    def test_refresh_real_artifact_intake_can_refresh_artifact_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            record_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            record_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                render_dashboard=True,
                refresh_artifact_registry=True,
                output_root=root / "workspace" / "refresh",
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["artifact_registry_refreshed"])
            artifact_registry_path = Path(result["artifact_registry_path"])
            self.assertTrue(artifact_registry_path.exists())
            dashboard_rows = query_clip_registry(
                mode="real-artifact-intake-dashboards",
                registry_path=artifact_registry_path,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(dashboard_rows["ok"])
            self.assertGreaterEqual(dashboard_rows["row_count"], 1)

    def test_refresh_real_artifact_intake_registry_refresh_ingests_evidence_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            synthetic_review_manifest = intake_root / "reports" / "synthetic.shadow_benchmark_review.json"
            _write_json(
                synthetic_review_manifest,
                {
                    "schema_version": "shadow_benchmark_review_v1",
                    "review_id": "review-synthetic",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "target_reviews": [
                        {
                            "training_target": "approved_or_selected_probability",
                            "current_best_family": "gradient_boosted_shadow_ranker",
                            "best_recommendation_decision": "prefer_shadow",
                            "current_best_evidence_mode": "synthetic_augmented",
                            "primary_metric_name": "top_k_recall",
                            "primary_metric_delta": 0.2,
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

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                comparison_manifest=synthetic_review_manifest,
                refresh_artifact_registry=True,
                output_root=root / "workspace" / "refresh",
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["artifact_registry_refreshed"])
            comparison_rows = query_clip_registry(
                mode="shadow-benchmark-evidence-comparisons",
                registry_path=Path(result["artifact_registry_path"]),
                training_target="approved_or_selected_probability",
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(comparison_rows["ok"])
            self.assertEqual(comparison_rows["row_count"], 1)
            self.assertEqual(
                comparison_rows["rows"][0]["synthetic_current_best_family"],
                "gradient_boosted_shadow_ranker",
            )

    def test_refresh_real_artifact_intake_warns_on_unresolved_dedup_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            _stage_duplicate_real_intake_bundle(source_root, intake_root, bundle_name="session-002")

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                output_root=root / "workspace" / "refresh",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["validation_summary"]["unresolved_dedup_group_count"], 3)
            self.assertEqual(result["warnings"][0]["status"], "unresolved_dedup_groups")

    def test_refresh_real_artifact_intake_can_block_on_unresolved_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            _stage_duplicate_real_intake_bundle(source_root, intake_root, bundle_name="session-002")

            result = refresh_real_artifact_intake(
                intake_root=intake_root,
                registry_path=root / "workspace" / "registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
                output_root=root / "workspace" / "refresh",
            )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "blocked_by_unresolved_dedup_groups")
            self.assertEqual(result["warning"]["status"], "unresolved_dedup_groups")
            self.assertEqual(result["validation_summary"]["unresolved_dedup_group_count"], 3)

    def test_preflight_real_artifact_intake_refresh_reports_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)

            result = preflight_real_artifact_intake_refresh(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["preflight_status"], "ready")
            self.assertEqual(result["blocking_issue_count"], 0)
            self.assertEqual(result["warning_issue_count"], 0)
            self.assertEqual(result["summary"]["benchmark_ready_bundle_count"], 1)
            self.assertEqual(result["summary"]["eligible_real_post_performance_label_count"], 2)

    def test_preflight_real_artifact_intake_refresh_can_block_on_unresolved_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            _stage_duplicate_real_intake_bundle(source_root, intake_root, bundle_name="session-002")

            result = preflight_real_artifact_intake_refresh(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["preflight_status"], "blocked")
            self.assertEqual(result["blocking_issue_count"], 1)
            self.assertEqual(result["blockers"][0]["status"], "unresolved_dedup_groups")

    def test_record_and_summarize_real_artifact_intake_preflight_history(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)

            recorded = record_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(recorded["ok"])
            self.assertTrue(Path(recorded["manifest_path"]).exists())
            self.assertEqual(recorded["schema_version"], "real_artifact_intake_refresh_preflight_history_v1")
            self.assertEqual(recorded["preflight"]["preflight_status"], "ready")

            summary = summarize_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["schema_version"], "real_artifact_intake_refresh_preflight_history_summary_v1")
            self.assertEqual(summary["entry_count"], 1)
            self.assertEqual(summary["status_counts"]["ready"], 1)
            self.assertEqual(summary["latest_entry"]["preflight_status"], "ready")

    def test_report_real_artifact_intake_preflight_trends_detects_improvement(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            recorded_blocked = record_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
                output_path=intake_root / "reports" / "preflight_history" / "blocked.real_artifact_intake_refresh_preflight.json",
            )
            self.assertEqual(recorded_blocked["preflight"]["preflight_status"], "blocked")

            _stage_real_intake_bundle(source_root, intake_root)
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            recorded_ready = record_real_artifact_intake_preflight_history(
                validation["manifest_path"],
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "preflight_history" / "ready.real_artifact_intake_refresh_preflight.json",
            )
            self.assertEqual(recorded_ready["preflight"]["preflight_status"], "ready")

            trend_report = report_real_artifact_intake_preflight_trends(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(trend_report["ok"])
            self.assertEqual(trend_report["schema_version"], "real_artifact_intake_refresh_preflight_trend_report_v1")
            self.assertEqual(trend_report["entry_count"], 2)
            self.assertEqual(trend_report["trend_status"], "improving")
            self.assertEqual(trend_report["status_transition_counts"]["blocked->ready"], 1)
            self.assertEqual(trend_report["delta_summary"]["benchmark_ready_bundle_count_delta"], 1)
            self.assertEqual(trend_report["delta_summary"]["eligible_real_post_performance_label_count_delta"], 2)

    def test_record_summarize_and_report_real_artifact_intake_refresh_outcome_history(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            _stage_real_intake_bundle(source_root, intake_root)
            first = record_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "refresh_outcome_history" / "one.real_artifact_intake_refresh_outcome.json",
            )
            self.assertTrue(first["ok"])
            self.assertTrue(Path(first["manifest_path"]).exists())
            self.assertEqual(first["schema_version"], "real_artifact_intake_refresh_outcome_history_v1")
            self.assertGreaterEqual(len(first["target_reviews"]), 1)

            second = record_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "refresh_outcome_history" / "two.real_artifact_intake_refresh_outcome.json",
            )
            self.assertTrue(second["ok"])

            summary = summarize_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["schema_version"], "real_artifact_intake_refresh_outcome_history_summary_v1")
            self.assertEqual(summary["entry_count"], 2)
            self.assertIn(summary["latest_entry"]["benchmark_recommendation"], {"keep_current", "blocked_by_policy", "inconclusive", "prefer_shadow"})

            trend_report = report_real_artifact_intake_refresh_outcome_trends(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(trend_report["ok"])
            self.assertEqual(trend_report["schema_version"], "real_artifact_intake_refresh_outcome_trend_report_v1")
            self.assertEqual(trend_report["entry_count"], 2)
            self.assertEqual(trend_report["delta_summary"]["performance_count_delta"], 0)

    def test_report_real_artifact_intake_history_comparison_joins_trends_and_evidence_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            record_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
                output_path=intake_root / "reports" / "preflight_history" / "blocked.real_artifact_intake_refresh_preflight.json",
            )
            _stage_real_intake_bundle(source_root, intake_root)
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            record_real_artifact_intake_preflight_history(
                validation["manifest_path"],
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "preflight_history" / "ready.real_artifact_intake_refresh_preflight.json",
            )
            record_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "refresh_outcome_history" / "one.real_artifact_intake_refresh_outcome.json",
            )

            comparison_manifest = intake_root / "reports" / "real_vs_synthetic.shadow_benchmark_evidence_comparison.json"
            comparison_manifest.parent.mkdir(parents=True, exist_ok=True)
            comparison_manifest.write_text(
                json.dumps(
                    {
                        "schema_version": "shadow_benchmark_evidence_comparison_v1",
                        "summary": {
                            "target_count": 1,
                            "family_winner_changed_count": 1,
                            "readiness_changed_count": 1,
                            "recommendation_changed_count": 1,
                            "synthetic_only_ready_count": 1,
                            "real_only_ready_count": 0,
                        },
                        "rows": [
                            {
                                "training_target": "approved_or_selected_probability",
                                "real_readiness_classification": "not_ready_due_to_coverage",
                                "synthetic_readiness_classification": "ready_for_next_iteration",
                                "real_best_recommendation_decision": "inconclusive",
                                "synthetic_best_recommendation_decision": "prefer_shadow",
                                "real_current_best_family": "linear_shadow_ranker",
                                "synthetic_current_best_family": "gradient_boosted_shadow_ranker",
                                "real_primary_metric_delta": -0.1,
                                "synthetic_primary_metric_delta": 0.2,
                                "disagreement_indicators": ["ready_only_under_synthetic"],
                                "readiness_changed": True,
                                "recommendation_changed": True,
                                "family_winner_changed": True,
                                "game": "marvel_rivals",
                                "platform": "youtube",
                            }
                        ],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            report = report_real_artifact_intake_history_comparison(
                comparison_manifest,
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(report["ok"])
            self.assertEqual(report["schema_version"], "real_artifact_intake_history_comparison_report_v1")
            self.assertEqual(report["preflight_trends"]["trend_status"], "improving")
            self.assertEqual(report["history_alignment"]["real_vs_synthetic_status"], "real_lags_synthetic")
            self.assertEqual(report["history_alignment"]["next_focus"], "add_real_bundles_and_refresh")
            target_index = {row["training_target"]: row for row in report["target_rows"]}
            self.assertIn("approved_or_selected_probability", target_index)
            self.assertEqual(
                target_index["approved_or_selected_probability"]["synthetic_readiness_classification"],
                "ready_for_next_iteration",
            )

    def test_render_real_artifact_intake_dashboard_snapshots_current_status(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source_root = root / "source"
            intake_root = root / "intake"
            registry_path, _posted_candidate_id, _sparse_candidate_id = _prepare_registry(source_root)
            del registry_path

            record_real_artifact_intake_preflight_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
                output_path=intake_root / "reports" / "preflight_history" / "blocked.real_artifact_intake_refresh_preflight.json",
            )
            _stage_real_intake_bundle(source_root, intake_root)
            validation = validate_real_artifact_intake(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            record_real_artifact_intake_preflight_history(
                validation["manifest_path"],
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "preflight_history" / "ready.real_artifact_intake_refresh_preflight.json",
            )
            record_real_artifact_intake_refresh_outcome_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "refresh_outcome_history" / "one.real_artifact_intake_refresh_outcome.json",
            )
            comparison_manifest = intake_root / "reports" / "dashboard.shadow_benchmark_evidence_comparison.json"
            comparison_manifest.write_text(
                json.dumps(
                    {
                        "schema_version": "shadow_benchmark_evidence_comparison_v1",
                        "summary": {
                            "target_count": 1,
                            "family_winner_changed_count": 0,
                            "readiness_changed_count": 1,
                            "recommendation_changed_count": 1,
                            "synthetic_only_ready_count": 1,
                            "real_only_ready_count": 0,
                        },
                        "rows": [
                            {
                                "training_target": "approved_or_selected_probability",
                                "synthetic_readiness_classification": "ready_for_next_iteration",
                                "synthetic_best_recommendation_decision": "prefer_shadow",
                                "synthetic_current_best_family": "gradient_boosted_shadow_ranker",
                                "synthetic_primary_metric_delta": 0.2,
                                "disagreement_indicators": ["ready_only_under_synthetic"],
                                "readiness_changed": True,
                                "recommendation_changed": True,
                                "family_winner_changed": False,
                                "game": "marvel_rivals",
                                "platform": "youtube",
                            }
                        ],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            dashboard = render_real_artifact_intake_dashboard(
                comparison_manifest,
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(dashboard["ok"])
            self.assertEqual(dashboard["schema_version"], "real_artifact_intake_dashboard_v1")
            self.assertTrue(Path(dashboard["manifest_path"]).exists())
            self.assertIn(
                dashboard["headline_status"],
                {"real_lags_synthetic", "improving_end_to_end", "improving_real_only_outcomes", "improving_intake_health"},
            )
            self.assertEqual(dashboard["current_intake"]["bundle_count"], 1)
            self.assertEqual(dashboard["preflight_trends"]["trend_status"], "improving")
            self.assertIn("history_alignment", dashboard["history_comparison"])

    def test_summarize_real_artifact_intake_dashboard_registry_returns_latest_and_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            reports = root / "reports"
            registry_path = root / "registry.sqlite"
            reports.mkdir(parents=True, exist_ok=True)
            (reports / "older.real_artifact_intake.dashboard.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "status": "ok",
                        "schema_version": "real_artifact_intake_dashboard_v1",
                        "generated_at": "2026-05-05T09:00:00+00:00",
                        "intake_root": str((root / "intake").resolve()),
                        "filters": {"game": "marvel_rivals", "platform": "youtube"},
                        "headline_status": "warning",
                        "current_intake": {
                            "intake_status": "warning",
                            "bundle_count": 1,
                            "warning_count": 1,
                            "bundle_readiness_rollups": {"readiness_status_counts": {"benchmark_ready": 0}},
                            "coverage_inventory": {"eligible_real_post_performance_label_count": 1},
                        },
                        "preflight_trends": {"trend_status": "stable", "entry_count": 1},
                        "refresh_outcome_trends": {"trend_status": "stable", "entry_count": 1},
                        "history_comparison": {"history_alignment": {"preflight_to_refresh_status": "blocked", "real_vs_synthetic_status": "real_lags_synthetic", "next_focus": "expand_real_evidence"}},
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            (reports / "latest.real_artifact_intake.dashboard.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "status": "ok",
                        "schema_version": "real_artifact_intake_dashboard_v1",
                        "generated_at": "2026-05-05T10:00:00+00:00",
                        "intake_root": str((root / "intake").resolve()),
                        "filters": {"game": "marvel_rivals", "platform": "youtube"},
                        "headline_status": "ready",
                        "current_intake": {
                            "intake_status": "ready",
                            "bundle_count": 2,
                            "warning_count": 0,
                            "bundle_readiness_rollups": {"readiness_status_counts": {"benchmark_ready": 1}},
                            "coverage_inventory": {"eligible_real_post_performance_label_count": 3},
                        },
                        "preflight_trends": {"trend_status": "improving", "entry_count": 2},
                        "refresh_outcome_trends": {"trend_status": "improving", "entry_count": 2},
                        "history_comparison": {"history_alignment": {"preflight_to_refresh_status": "aligned", "real_vs_synthetic_status": "narrowing", "next_focus": "run_real_only_refresh"}},
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            refresh_result = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(refresh_result["ok"])

            summary = summarize_real_artifact_intake_dashboard_registry(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["schema_version"], "real_artifact_intake_dashboard_registry_summary_v1")
            self.assertEqual(summary["row_count"], 2)
            self.assertEqual(summary["headline_status_counts"], {"ready": 1, "warning": 1})
            self.assertEqual(summary["latest_dashboard"]["headline_status"], "ready")
            self.assertEqual(summary["latest_dashboard"]["next_focus"], "run_real_only_refresh")
            self.assertEqual(summary["scope_count"], 1)

    def test_summarize_real_artifact_intake_comparison_targets_aggregates_by_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            reports = root / "reports"
            registry_path = root / "registry.sqlite"
            reports.mkdir(parents=True, exist_ok=True)
            _write_json(
                reports / "older.shadow_benchmark_evidence_comparison.json",
                {
                    "schema_version": "shadow_benchmark_evidence_comparison_v1",
                    "comparison_id": "compare-older",
                    "created_at": "2026-05-05T09:00:00+00:00",
                    "real_manifest_path": "/tmp/real-older.json",
                    "synthetic_manifest_path": "/tmp/synth-older.json",
                    "row_count": 2,
                    "rows": [
                        {
                            "row_index": 0,
                            "training_target": "approved_or_selected_probability",
                            "real_manifest_path": "/tmp/real-older.json",
                            "synthetic_manifest_path": "/tmp/synth-older.json",
                            "real_current_best_family": "linear_shadow_ranker",
                            "synthetic_current_best_family": "gradient_boosted_shadow_ranker",
                            "real_best_recommendation_decision": "inconclusive",
                            "synthetic_best_recommendation_decision": "prefer_shadow",
                            "real_current_best_evidence_mode": "real_only",
                            "synthetic_current_best_evidence_mode": "synthetic_augmented",
                            "real_readiness_classification": "not_ready_due_to_coverage",
                            "synthetic_readiness_classification": "ready_for_next_iteration",
                            "real_primary_metric_name": "top_k_recall",
                            "synthetic_primary_metric_name": "top_k_recall",
                            "real_primary_metric_delta": -0.1,
                            "synthetic_primary_metric_delta": 0.2,
                            "primary_metric_delta_gap": -0.3,
                            "real_confidence_level": "low",
                            "synthetic_confidence_level": "low",
                            "real_successful_run_count": 1,
                            "synthetic_successful_run_count": 2,
                            "real_run_count": 1,
                            "synthetic_run_count": 2,
                            "family_winner_changed": True,
                            "readiness_changed": True,
                            "recommendation_changed": True,
                            "disagreement_indicators": ["ready_only_under_synthetic"],
                            "game": "marvel_rivals",
                            "platform": "youtube",
                        },
                        {
                            "row_index": 1,
                            "training_target": "post_performance_score",
                            "real_manifest_path": "/tmp/real-older.json",
                            "synthetic_manifest_path": "/tmp/synth-older.json",
                            "real_current_best_family": "linear_shadow_ranker",
                            "synthetic_current_best_family": "linear_shadow_ranker",
                            "real_best_recommendation_decision": "keep_current",
                            "synthetic_best_recommendation_decision": "keep_current",
                            "real_current_best_evidence_mode": "real_only",
                            "synthetic_current_best_evidence_mode": "synthetic_augmented",
                            "real_readiness_classification": "needs_feature_cleanup",
                            "synthetic_readiness_classification": "needs_feature_cleanup",
                            "real_primary_metric_name": "pearson_correlation",
                            "synthetic_primary_metric_name": "pearson_correlation",
                            "real_primary_metric_delta": -0.02,
                            "synthetic_primary_metric_delta": 0.01,
                            "primary_metric_delta_gap": -0.03,
                            "real_confidence_level": "low",
                            "synthetic_confidence_level": "low",
                            "real_successful_run_count": 2,
                            "synthetic_successful_run_count": 2,
                            "real_run_count": 2,
                            "synthetic_run_count": 2,
                            "family_winner_changed": False,
                            "readiness_changed": False,
                            "recommendation_changed": False,
                            "disagreement_indicators": [],
                            "game": "marvel_rivals",
                            "platform": "youtube",
                        },
                    ],
                    "summary": {},
                },
            )
            _write_json(
                reports / "latest.shadow_benchmark_evidence_comparison.json",
                {
                    "schema_version": "shadow_benchmark_evidence_comparison_v1",
                    "comparison_id": "compare-latest",
                    "created_at": "2026-05-05T10:00:00+00:00",
                    "real_manifest_path": "/tmp/real-latest.json",
                    "synthetic_manifest_path": "/tmp/synth-latest.json",
                    "row_count": 1,
                    "rows": [
                        {
                            "row_index": 0,
                            "training_target": "approved_or_selected_probability",
                            "real_manifest_path": "/tmp/real-latest.json",
                            "synthetic_manifest_path": "/tmp/synth-latest.json",
                            "real_current_best_family": "gradient_boosted_shadow_ranker",
                            "synthetic_current_best_family": "gradient_boosted_shadow_ranker",
                            "real_best_recommendation_decision": "keep_current",
                            "synthetic_best_recommendation_decision": "prefer_shadow",
                            "real_current_best_evidence_mode": "real_only",
                            "synthetic_current_best_evidence_mode": "synthetic_augmented",
                            "real_readiness_classification": "needs_feature_cleanup",
                            "synthetic_readiness_classification": "ready_for_next_iteration",
                            "real_primary_metric_name": "top_k_recall",
                            "synthetic_primary_metric_name": "top_k_recall",
                            "real_primary_metric_delta": 0.0,
                            "synthetic_primary_metric_delta": 0.25,
                            "primary_metric_delta_gap": -0.25,
                            "real_confidence_level": "medium",
                            "synthetic_confidence_level": "low",
                            "real_successful_run_count": 3,
                            "synthetic_successful_run_count": 2,
                            "real_run_count": 3,
                            "synthetic_run_count": 2,
                            "family_winner_changed": False,
                            "readiness_changed": True,
                            "recommendation_changed": True,
                            "disagreement_indicators": ["ready_only_under_synthetic"],
                            "game": "marvel_rivals",
                            "platform": "youtube",
                        }
                    ],
                    "summary": {},
                },
            )
            refresh_result = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(refresh_result["ok"])

            summary = summarize_real_artifact_intake_comparison_targets(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["schema_version"], "real_artifact_intake_comparison_target_summary_v1")
            self.assertEqual(summary["row_count"], 3)
            self.assertEqual(summary["target_count"], 2)
            self.assertEqual(summary["aggregate_counts"]["ready_only_under_synthetic_count"], 2)
            target_index = {row["training_target"]: row for row in summary["targets"]}
            self.assertEqual(target_index["approved_or_selected_probability"]["comparison_row_count"], 2)
            self.assertEqual(target_index["approved_or_selected_probability"]["readiness_changed_count"], 2)
            self.assertEqual(target_index["approved_or_selected_probability"]["recommendation_changed_count"], 2)
            self.assertEqual(
                target_index["approved_or_selected_probability"]["latest_real_current_best_family"],
                "gradient_boosted_shadow_ranker",
            )
            self.assertEqual(
                target_index["approved_or_selected_probability"]["latest_real_confidence_level"],
                "medium",
            )
            self.assertEqual(
                target_index["approved_or_selected_probability"]["disagreement_indicator_counts"]["ready_only_under_synthetic"],
                2,
            )
            self.assertEqual(target_index["post_performance_score"]["comparison_row_count"], 1)
            self.assertEqual(target_index["post_performance_score"]["readiness_changed_count"], 0)

    def test_record_summarize_and_report_real_artifact_intake_dashboard_summary_history(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            reports = root / "reports"
            registry_path = root / "registry.sqlite"
            intake_root = root / "intake"
            reports.mkdir(parents=True, exist_ok=True)

            def _write_dashboard(name: str, generated_at: str, headline_status: str, ready_bundles: int, eligible_labels: int, next_focus: str) -> None:
                (reports / name).write_text(
                    json.dumps(
                        {
                            "ok": True,
                            "status": "ok",
                            "schema_version": "real_artifact_intake_dashboard_v1",
                            "generated_at": generated_at,
                            "intake_root": str((root / "intake-source").resolve()),
                            "filters": {"game": "marvel_rivals", "platform": "youtube"},
                            "headline_status": headline_status,
                            "current_intake": {
                                "intake_status": headline_status,
                                "bundle_count": 2,
                                "warning_count": 0,
                                "bundle_readiness_rollups": {"readiness_status_counts": {"benchmark_ready": ready_bundles}},
                                "coverage_inventory": {"eligible_real_post_performance_label_count": eligible_labels},
                            },
                            "preflight_trends": {"trend_status": "improving", "entry_count": 2},
                            "refresh_outcome_trends": {"trend_status": "improving", "entry_count": 2},
                            "history_comparison": {
                                "history_alignment": {
                                    "preflight_to_refresh_status": "aligned",
                                    "real_vs_synthetic_status": "narrowing",
                                    "next_focus": next_focus,
                                }
                            },
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )

            _write_dashboard(
                "older.real_artifact_intake.dashboard.json",
                "2026-05-05T09:00:00+00:00",
                "warning",
                0,
                1,
                "expand_real_evidence",
            )
            refresh_result = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(refresh_result["ok"])
            first = record_real_artifact_intake_dashboard_summary_history(
                registry_path=registry_path,
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "dashboard_summary_history" / "one.real_artifact_intake_dashboard_registry_summary.json",
            )
            self.assertTrue(first["ok"])

            _write_dashboard(
                "latest.real_artifact_intake.dashboard.json",
                "2026-05-05T10:00:00+00:00",
                "ready",
                2,
                4,
                "run_real_only_refresh",
            )
            refresh_result = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(refresh_result["ok"])
            second = record_real_artifact_intake_dashboard_summary_history(
                registry_path=registry_path,
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
                output_path=intake_root / "reports" / "dashboard_summary_history" / "two.real_artifact_intake_dashboard_registry_summary.json",
            )
            self.assertTrue(second["ok"])

            summary = summarize_real_artifact_intake_dashboard_summary_history(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["schema_version"], "real_artifact_intake_dashboard_summary_history_summary_v1")
            self.assertEqual(summary["entry_count"], 2)
            self.assertEqual(summary["latest_entry"]["latest_headline_status"], "ready")

            trend_report = report_real_artifact_intake_dashboard_summary_trends(
                intake_root=intake_root,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertTrue(trend_report["ok"])
            self.assertEqual(trend_report["schema_version"], "real_artifact_intake_dashboard_summary_trend_report_v1")
            self.assertEqual(trend_report["entry_count"], 2)
            self.assertEqual(trend_report["trend_status"], "improving")
            self.assertEqual(trend_report["headline_transition_counts"], {"warning->ready": 1})
