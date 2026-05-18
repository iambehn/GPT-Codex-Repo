from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from pipeline.commands.export_posting import dispatch_export_posting_commands


class _ParserStub:
    def error(self, message: str) -> None:
        raise RuntimeError(message)


def _base_args() -> SimpleNamespace:
    return SimpleNamespace(
        export_v2_training_datasets=False,
        build_approval_target_dataset=False,
        adapt_approval_target_dataset=None,
        build_accepted_clip_inventory=False,
        build_accepted_clip_intake_manifest=False,
        adapt_accepted_clip_intake_to_source_manifest=False,
        run_accepted_fixture_trial_batch=False,
        prepare_accepted_proxy_review=False,
        export_runtime_analysis=None,
        export_fused_analysis=None,
        export_highlight_selection=None,
        derive_hook_candidates=None,
        compare_hook_candidates=None,
        report_hook_evaluation=None,
        create_workflow_run=False,
        create_highlight_export_batch=False,
        record_post_ledger=False,
        record_posted_metrics_snapshot=False,
        materialize_synthetic_post_coverage=False,
        import_real_posted_lineage=False,
        validate_real_artifact_intake=False,
        bootstrap_real_artifact_intake_bundle=False,
        summarize_real_artifact_intake=None,
        report_real_artifact_intake_coverage=None,
        preflight_real_artifact_intake_refresh=None,
        record_real_artifact_intake_preflight_history=None,
        summarize_real_artifact_intake_preflight_history=False,
        report_real_artifact_intake_preflight_trends=False,
        record_real_artifact_intake_refresh_outcome_history=False,
        summarize_real_artifact_intake_refresh_outcome_history=False,
        report_real_artifact_intake_refresh_outcome_trends=False,
        report_real_artifact_intake_history_comparison=None,
        render_real_artifact_intake_dashboard=None,
        summarize_real_artifact_intake_dashboard_registry=False,
        summarize_real_artifact_intake_comparison_targets=False,
        record_real_artifact_intake_dashboard_summary_history=False,
        summarize_real_artifact_intake_dashboard_summary_history=False,
        report_real_artifact_intake_dashboard_summary_trends=False,
        advise_real_artifact_intake_dedup=None,
        materialize_real_artifact_intake_dedup_resolutions=None,
        summarize_real_artifact_intake_dedup_resolutions=None,
        update_real_artifact_intake_dedup_resolution=None,
        refresh_real_only_benchmark=False,
        refresh_real_artifact_intake=False,
        report_posted_performance=False,
        registry_path=None,
        output_root=None,
        output_path=None,
        game="marvel_rivals",
        fixture_id=None,
        candidate_id=None,
        lifecycle_state=None,
        hook_archetype=None,
        hook_mode=None,
        platform=None,
        account_id=None,
        evidence_mode=None,
        full_json=False,
        source_root=None,
        accepted_inventory_manifest=None,
        accepted_clip_intake_manifest=None,
        fixture_source_manifest=None,
        pattern=None,
        limit=None,
        emit_runtime=False,
        emit_fused=False,
        accepted_fixture_trial_batch_manifest=None,
        gpt_repo=None,
        proxy_sidecar=None,
        fused_sidecar=None,
        baseline_sidecar_root=None,
        trial_sidecar_root=None,
        workflow_type=None,
        workflow_run_id=None,
        selection_manifest=None,
        export_manifest=None,
        post_ledger_manifest=None,
        view_count=None,
        like_count=None,
        comment_count=None,
        share_count=None,
        save_count=None,
        watch_time_seconds=None,
        average_watch_time_seconds=None,
        completion_rate=None,
        engagement_rate=None,
        synthetic_profile=None,
        include_rejected=False,
        intake_root=None,
        bundle_name=None,
        require_resolved_dedup=False,
        record_dashboard_summary_history_on_refresh=False,
        record_refresh_outcome_history_on_refresh=False,
        render_dashboard_on_refresh=False,
        refresh_artifact_registry_on_refresh=False,
        compare_evidence_on_refresh=None,
        group_id=None,
        resolution_status=None,
        reviewed_by=None,
        notes=None,
    )


def _dispatch(args: SimpleNamespace, *, phase: str = "all") -> int | None:
    return dispatch_export_posting_commands(
        args,
        parser=_ParserStub(),
        phase=phase,
        print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
        run_export_v2_training_datasets_fn=lambda **kwargs: {"ok": True, "status": "exported", "kwargs": kwargs},
        run_build_approval_target_dataset_fn=lambda **kwargs: {"ok": True, "status": "ok", "kwargs": kwargs},
        run_adapt_approval_target_dataset_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_build_accepted_clip_inventory_fn=lambda **kwargs: {"ok": True, "status": "ok", "kwargs": kwargs},
        run_build_accepted_clip_intake_manifest_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_adapt_accepted_clip_intake_to_source_manifest_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_accepted_fixture_trial_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_prepare_accepted_proxy_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_export_runtime_analysis_fn=lambda *a, **k: {"status": "exported"},
        run_export_fused_analysis_fn=lambda *a, **k: {"status": "exported"},
        run_export_highlight_selection_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_derive_hook_candidates_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_compare_hook_candidates_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_hook_evaluation_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_create_workflow_run_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_create_highlight_export_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_record_post_ledger_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_record_posted_metrics_snapshot_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_materialize_synthetic_post_coverage_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_import_real_posted_lineage_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_validate_real_artifact_intake_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_bootstrap_real_artifact_intake_bundle_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_real_artifact_intake_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_real_artifact_intake_coverage_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_preflight_real_artifact_intake_refresh_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_record_real_artifact_intake_preflight_history_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_real_artifact_intake_preflight_history_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_real_artifact_intake_preflight_trends_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_record_real_artifact_intake_refresh_outcome_history_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_real_artifact_intake_refresh_outcome_history_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_real_artifact_intake_refresh_outcome_trends_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_real_artifact_intake_history_comparison_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_render_real_artifact_intake_dashboard_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_real_artifact_intake_dashboard_registry_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_real_artifact_intake_comparison_targets_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_record_real_artifact_intake_dashboard_summary_history_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_real_artifact_intake_dashboard_summary_history_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_real_artifact_intake_dashboard_summary_trends_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_advise_real_artifact_intake_dedup_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_materialize_real_artifact_intake_dedup_resolutions_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_real_artifact_intake_dedup_resolutions_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_update_real_artifact_intake_dedup_resolution_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_refresh_real_only_benchmark_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_refresh_real_artifact_intake_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_report_posted_performance_fn=lambda *a, **k: {"ok": True, "status": "ok"},
    )


class ExportPostingCommandTests(unittest.TestCase):
    def test_pre_shadow_phase_routes_export_v2_training_datasets(self) -> None:
        args = _base_args()
        args.export_v2_training_datasets = True
        args.registry_path = "/tmp/registry.json"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args, phase="pre_shadow")
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["result"]["status"], "exported")
        self.assertEqual(payload["result"]["kwargs"]["registry_path"], "/tmp/registry.json")

    def test_post_shadow_phase_requires_registry_path_for_build_approval_target_dataset(self) -> None:
        args = _base_args()
        args.build_approval_target_dataset = True
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args, phase="post_shadow")
        self.assertEqual(str(ctx.exception), "--build-approval-target-dataset requires --registry-path")

    def test_post_shadow_phase_reports_real_artifact_intake_json_result(self) -> None:
        args = _base_args()
        args.validate_real_artifact_intake = True
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args, phase="post_shadow")
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])

    def test_returns_none_when_no_route_matches_phase(self) -> None:
        args = _base_args()
        self.assertIsNone(_dispatch(args, phase="post_shadow"))
