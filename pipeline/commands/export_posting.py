from __future__ import annotations

import argparse
import json
from typing import Any, Callable


def dispatch_export_posting_commands(
    args: argparse.Namespace,
    *,
    parser: argparse.ArgumentParser,
    phase: str = "all",
    print_cli_result_fn: Callable[..., None],
    run_export_v2_training_datasets_fn: Callable[..., dict[str, Any]],
    run_build_approval_target_dataset_fn: Callable[..., dict[str, Any]],
    run_adapt_approval_target_dataset_fn: Callable[..., dict[str, Any]],
    run_build_accepted_clip_inventory_fn: Callable[..., dict[str, Any]],
    run_build_accepted_clip_intake_manifest_fn: Callable[..., dict[str, Any]],
    run_adapt_accepted_clip_intake_to_source_manifest_fn: Callable[..., dict[str, Any]],
    run_accepted_fixture_trial_batch_fn: Callable[..., dict[str, Any]],
    run_prepare_accepted_proxy_review_fn: Callable[..., dict[str, Any]],
    run_export_runtime_analysis_fn: Callable[..., dict[str, Any]],
    run_export_fused_analysis_fn: Callable[..., dict[str, Any]],
    run_export_highlight_selection_fn: Callable[..., dict[str, Any]],
    run_derive_hook_candidates_fn: Callable[..., dict[str, Any]],
    run_compare_hook_candidates_fn: Callable[..., dict[str, Any]],
    run_report_hook_evaluation_fn: Callable[..., dict[str, Any]],
    run_create_workflow_run_fn: Callable[..., dict[str, Any]],
    run_create_highlight_export_batch_fn: Callable[..., dict[str, Any]],
    run_record_post_ledger_fn: Callable[..., dict[str, Any]],
    run_record_posted_metrics_snapshot_fn: Callable[..., dict[str, Any]],
    run_materialize_synthetic_post_coverage_fn: Callable[..., dict[str, Any]],
    run_import_real_posted_lineage_fn: Callable[..., dict[str, Any]],
    run_validate_real_artifact_intake_fn: Callable[..., dict[str, Any]],
    run_bootstrap_real_artifact_intake_bundle_fn: Callable[..., dict[str, Any]],
    run_summarize_real_artifact_intake_fn: Callable[..., dict[str, Any]],
    run_report_real_artifact_intake_coverage_fn: Callable[..., dict[str, Any]],
    run_preflight_real_artifact_intake_refresh_fn: Callable[..., dict[str, Any]],
    run_record_real_artifact_intake_preflight_history_fn: Callable[..., dict[str, Any]],
    run_summarize_real_artifact_intake_preflight_history_fn: Callable[..., dict[str, Any]],
    run_report_real_artifact_intake_preflight_trends_fn: Callable[..., dict[str, Any]],
    run_record_real_artifact_intake_refresh_outcome_history_fn: Callable[..., dict[str, Any]],
    run_summarize_real_artifact_intake_refresh_outcome_history_fn: Callable[..., dict[str, Any]],
    run_report_real_artifact_intake_refresh_outcome_trends_fn: Callable[..., dict[str, Any]],
    run_report_real_artifact_intake_history_comparison_fn: Callable[..., dict[str, Any]],
    run_render_real_artifact_intake_dashboard_fn: Callable[..., dict[str, Any]],
    run_summarize_real_artifact_intake_dashboard_registry_fn: Callable[..., dict[str, Any]],
    run_summarize_real_artifact_intake_comparison_targets_fn: Callable[..., dict[str, Any]],
    run_record_real_artifact_intake_dashboard_summary_history_fn: Callable[..., dict[str, Any]],
    run_summarize_real_artifact_intake_dashboard_summary_history_fn: Callable[..., dict[str, Any]],
    run_report_real_artifact_intake_dashboard_summary_trends_fn: Callable[..., dict[str, Any]],
    run_advise_real_artifact_intake_dedup_fn: Callable[..., dict[str, Any]],
    run_materialize_real_artifact_intake_dedup_resolutions_fn: Callable[..., dict[str, Any]],
    run_summarize_real_artifact_intake_dedup_resolutions_fn: Callable[..., dict[str, Any]],
    run_update_real_artifact_intake_dedup_resolution_fn: Callable[..., dict[str, Any]],
    run_refresh_real_only_benchmark_fn: Callable[..., dict[str, Any]],
    run_refresh_real_artifact_intake_fn: Callable[..., dict[str, Any]],
    run_report_posted_performance_fn: Callable[..., dict[str, Any]],
) -> int | None:
    if phase not in {"all", "pre_shadow", "post_shadow"}:
        raise ValueError(f"unsupported export posting dispatch phase: {phase}")

    if phase in {"all", "pre_shadow"} and args.export_v2_training_datasets:
        print_cli_result_fn(
            run_export_v2_training_datasets_fn(
                registry_path=args.registry_path,
                output_root=args.output_root,
                game=args.game,
                fixture_id=args.fixture_id,
                candidate_id=args.candidate_id,
                lifecycle_state=args.lifecycle_state,
                hook_archetype=args.hook_archetype,
                hook_mode=args.hook_mode,
                platform=args.platform,
                account_id=args.account_id,
                evidence_mode=args.evidence_mode,
            ),
            full_json=args.full_json,
        )
        return 0

    if phase not in {"all", "post_shadow"}:
        return None

    if args.build_approval_target_dataset:
        if not args.registry_path:
            parser.error("--build-approval-target-dataset requires --registry-path")
        if not args.game:
            parser.error("--build-approval-target-dataset requires --game")
        print_cli_result_fn(
            run_build_approval_target_dataset_fn(
                registry_path=args.registry_path,
                game=args.game,
                platform=args.platform,
                evidence_mode=args.evidence_mode,
                output_root=args.output_root,
                output_path=args.output_path,
            ),
            command_name="build_approval_target_dataset",
            full_json=args.full_json,
        )
        return 0

    if args.adapt_approval_target_dataset:
        print_cli_result_fn(
            run_adapt_approval_target_dataset_fn(
                args.adapt_approval_target_dataset,
                output_root=args.output_root,
                output_path=args.output_path,
            ),
            command_name="adapt_approval_target_dataset",
            full_json=args.full_json,
        )
        return 0

    if args.build_accepted_clip_inventory:
        if not args.source_root:
            parser.error("--build-accepted-clip-inventory requires --source-root")
        if len(args.source_root) != 1:
            parser.error("--build-accepted-clip-inventory accepts exactly one --source-root")
        if not args.game:
            parser.error("--build-accepted-clip-inventory requires --game")
        print_cli_result_fn(
            run_build_accepted_clip_inventory_fn(
                source_root=args.source_root[0],
                game=args.game,
                output_root=args.output_root,
                output_path=args.output_path,
            ),
            command_name="build_accepted_clip_inventory",
            full_json=args.full_json,
        )
        return 0

    if args.build_accepted_clip_intake_manifest:
        if not args.accepted_inventory_manifest:
            parser.error("--build-accepted-clip-intake-manifest requires --accepted-inventory-manifest")
        print_cli_result_fn(
            run_build_accepted_clip_intake_manifest_fn(
                args.accepted_inventory_manifest,
                output_root=args.output_root,
                output_path=args.output_path,
            ),
            command_name="build_accepted_clip_intake_manifest",
            full_json=args.full_json,
        )
        return 0

    if args.adapt_accepted_clip_intake_to_source_manifest:
        if not args.accepted_clip_intake_manifest:
            parser.error("--adapt-accepted-clip-intake-to-source-manifest requires --accepted-clip-intake-manifest")
        print_cli_result_fn(
            run_adapt_accepted_clip_intake_to_source_manifest_fn(
                args.accepted_clip_intake_manifest,
                output_root=args.output_root,
                output_path=args.output_path,
            ),
            command_name="adapt_accepted_clip_intake_to_source_manifest",
            full_json=args.full_json,
        )
        return 0

    if args.run_accepted_fixture_trial_batch:
        if not args.fixture_source_manifest:
            parser.error("--run-accepted-fixture-trial-batch requires --fixture-source-manifest")
        print_cli_result_fn(
            run_accepted_fixture_trial_batch_fn(
                args.fixture_source_manifest,
                output_root=args.output_root,
                output_path=args.output_path,
                game=args.game,
                pattern=args.pattern,
                limit=args.limit,
                emit_runtime=args.emit_runtime,
                emit_fused=args.emit_fused,
            ),
            command_name="run_accepted_fixture_trial_batch",
            full_json=args.full_json,
        )
        return 0

    if args.prepare_accepted_proxy_review:
        if not args.accepted_fixture_trial_batch_manifest:
            parser.error("--prepare-accepted-proxy-review requires --accepted-fixture-trial-batch-manifest")
        print_cli_result_fn(
            run_prepare_accepted_proxy_review_fn(
                args.accepted_fixture_trial_batch_manifest,
                output_root=args.output_root,
                output_path=args.output_path,
                gpt_repo=args.gpt_repo,
            ),
            command_name="prepare_accepted_proxy_review",
            full_json=args.full_json,
        )
        return 0

    if args.export_runtime_analysis:
        return _json_exit(run_export_runtime_analysis_fn(args.export_runtime_analysis, game=args.game), check_ok=False)

    if args.export_fused_analysis:
        return _json_exit(run_export_fused_analysis_fn(args.export_fused_analysis, game=args.game), check_ok=False)

    if args.export_highlight_selection is not None:
        result = run_export_highlight_selection_fn(
            args.proxy_sidecar or args.export_highlight_selection or None,
            fused_sidecar=args.fused_sidecar,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.derive_hook_candidates:
        result = run_derive_hook_candidates_fn(
            args.derive_hook_candidates,
            registry_path=args.registry_path,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.compare_hook_candidates:
        if not args.baseline_sidecar_root or not args.trial_sidecar_root:
            parser.error("--compare-hook-candidates requires --baseline-sidecar-root and --trial-sidecar-root")
        result = run_compare_hook_candidates_fn(
            args.compare_hook_candidates,
            baseline_sidecar_root=args.baseline_sidecar_root,
            trial_sidecar_root=args.trial_sidecar_root,
            game=args.game,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, command_name="compare_hook_candidates", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.report_hook_evaluation:
        if not args.baseline_sidecar_root or not args.trial_sidecar_root:
            parser.error("--report-hook-evaluation requires --baseline-sidecar-root and --trial-sidecar-root")
        if not args.registry_path:
            parser.error("--report-hook-evaluation requires --registry-path")
        result = run_report_hook_evaluation_fn(
            args.report_hook_evaluation,
            baseline_sidecar_root=args.baseline_sidecar_root,
            trial_sidecar_root=args.trial_sidecar_root,
            registry_path=args.registry_path,
            game=args.game,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, command_name="report_hook_evaluation", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.create_workflow_run:
        if not args.workflow_type:
            parser.error("--create-workflow-run requires --workflow-type")
        result = run_create_workflow_run_fn(
            args.workflow_type,
            registry_path=args.registry_path,
            output_path=args.output_path,
            game=args.game,
            fixture_id=args.fixture_id,
        )
        print_cli_result_fn(result, full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.create_highlight_export_batch:
        result = run_create_highlight_export_batch_fn(
            registry_path=args.registry_path,
            workflow_run_id=args.workflow_run_id,
            selection_manifest=args.selection_manifest,
            game=args.game,
            fixture_id=args.fixture_id,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.record_post_ledger:
        if not args.export_manifest:
            parser.error("--record-post-ledger requires --export-manifest")
        result = run_record_post_ledger_fn(
            args.export_manifest,
            workflow_run_id=args.workflow_run_id,
            platform=args.platform,
            account_id=args.account_id,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.record_posted_metrics_snapshot:
        if not args.post_ledger_manifest:
            parser.error("--record-posted-metrics-snapshot requires --post-ledger-manifest")
        result = run_record_posted_metrics_snapshot_fn(
            args.post_ledger_manifest,
            workflow_run_id=args.workflow_run_id,
            platform=args.platform,
            account_id=args.account_id,
            output_path=args.output_path,
            view_count=args.view_count,
            like_count=args.like_count,
            comment_count=args.comment_count,
            share_count=args.share_count,
            save_count=args.save_count,
            watch_time_seconds=args.watch_time_seconds,
            average_watch_time_seconds=args.average_watch_time_seconds,
            completion_rate=args.completion_rate,
            engagement_rate=args.engagement_rate,
        )
        print_cli_result_fn(result, full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.materialize_synthetic_post_coverage:
        if not args.registry_path:
            parser.error("--materialize-synthetic-post-coverage requires --registry-path")
        result = run_materialize_synthetic_post_coverage_fn(
            registry_path=args.registry_path,
            game=args.game,
            fixture_id=args.fixture_id,
            platform=args.platform,
            account_id=args.account_id,
            workflow_run_id=args.workflow_run_id,
            output_root=args.output_root,
            synthetic_profile=args.synthetic_profile or "balanced",
            include_rejected=bool(args.include_rejected),
        )
        print_cli_result_fn(result, command_name="materialize_synthetic_post_coverage", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.import_real_posted_lineage:
        if not args.registry_path:
            parser.error("--import-real-posted-lineage requires --registry-path")
        if not args.source_root:
            parser.error("--import-real-posted-lineage requires at least one --source-root")
        result = run_import_real_posted_lineage_fn(
            source_roots=args.source_root,
            registry_path=args.registry_path,
            game=args.game,
            platform=args.platform,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.validate_real_artifact_intake:
        result = run_validate_real_artifact_intake_fn(
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.bootstrap_real_artifact_intake_bundle:
        if not args.bundle_name:
            parser.error("--bootstrap-real-artifact-intake-bundle requires --bundle-name")
        result = run_bootstrap_real_artifact_intake_bundle_fn(
            args.bundle_name,
            intake_root=args.intake_root,
        )
        return _json_exit(result)

    if args.summarize_real_artifact_intake is not None:
        result = run_summarize_real_artifact_intake_fn(
            args.summarize_real_artifact_intake,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.report_real_artifact_intake_coverage is not None:
        result = run_report_real_artifact_intake_coverage_fn(
            args.report_real_artifact_intake_coverage,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.preflight_real_artifact_intake_refresh is not None:
        result = run_preflight_real_artifact_intake_refresh_fn(
            args.preflight_real_artifact_intake_refresh,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
            require_resolved_dedup=args.require_resolved_dedup,
        )
        return _json_exit(result)

    if args.record_real_artifact_intake_preflight_history is not None:
        result = run_record_real_artifact_intake_preflight_history_fn(
            args.record_real_artifact_intake_preflight_history,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
            require_resolved_dedup=args.require_resolved_dedup,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.summarize_real_artifact_intake_preflight_history:
        result = run_summarize_real_artifact_intake_preflight_history_fn(
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.report_real_artifact_intake_preflight_trends:
        result = run_report_real_artifact_intake_preflight_trends_fn(
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.record_real_artifact_intake_refresh_outcome_history:
        result = run_record_real_artifact_intake_refresh_outcome_history_fn(
            intake_root=args.intake_root,
            registry_path=args.registry_path,
            game=args.game,
            platform=args.platform,
            require_resolved_dedup=args.require_resolved_dedup,
            output_root=args.output_root,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.summarize_real_artifact_intake_refresh_outcome_history:
        result = run_summarize_real_artifact_intake_refresh_outcome_history_fn(
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.report_real_artifact_intake_refresh_outcome_trends:
        result = run_report_real_artifact_intake_refresh_outcome_trends_fn(
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.report_real_artifact_intake_history_comparison is not None:
        result = run_report_real_artifact_intake_history_comparison_fn(
            args.report_real_artifact_intake_history_comparison,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.render_real_artifact_intake_dashboard is not None:
        result = run_render_real_artifact_intake_dashboard_fn(
            args.render_real_artifact_intake_dashboard,
            validation_manifest=args.summarize_real_artifact_intake,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.summarize_real_artifact_intake_dashboard_registry:
        result = run_summarize_real_artifact_intake_dashboard_registry_fn(
            registry_path=args.registry_path,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.summarize_real_artifact_intake_comparison_targets:
        result = run_summarize_real_artifact_intake_comparison_targets_fn(
            registry_path=args.registry_path,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.record_real_artifact_intake_dashboard_summary_history:
        result = run_record_real_artifact_intake_dashboard_summary_history_fn(
            registry_path=args.registry_path,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.summarize_real_artifact_intake_dashboard_summary_history:
        result = run_summarize_real_artifact_intake_dashboard_summary_history_fn(
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.report_real_artifact_intake_dashboard_summary_trends:
        result = run_report_real_artifact_intake_dashboard_summary_trends_fn(
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.advise_real_artifact_intake_dedup is not None:
        result = run_advise_real_artifact_intake_dedup_fn(
            args.advise_real_artifact_intake_dedup,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.materialize_real_artifact_intake_dedup_resolutions is not None:
        result = run_materialize_real_artifact_intake_dedup_resolutions_fn(
            args.materialize_real_artifact_intake_dedup_resolutions,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.summarize_real_artifact_intake_dedup_resolutions is not None:
        result = run_summarize_real_artifact_intake_dedup_resolutions_fn(
            args.summarize_real_artifact_intake_dedup_resolutions,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.update_real_artifact_intake_dedup_resolution is not None:
        if not args.group_id:
            parser.error("--update-real-artifact-intake-dedup-resolution requires --group-id")
        if not args.resolution_status:
            parser.error("--update-real-artifact-intake-dedup-resolution requires --resolution-status")
        result = run_update_real_artifact_intake_dedup_resolution_fn(
            args.update_real_artifact_intake_dedup_resolution,
            group_id=args.group_id,
            status=args.resolution_status,
            reviewed_by=args.reviewed_by,
            notes=args.notes,
            intake_root=args.intake_root,
            game=args.game,
            platform=args.platform,
        )
        return _json_exit(result)

    if args.refresh_real_only_benchmark:
        if not args.registry_path:
            parser.error("--refresh-real-only-benchmark requires --registry-path")
        if not args.source_root:
            parser.error("--refresh-real-only-benchmark requires at least one --source-root")
        result = run_refresh_real_only_benchmark_fn(
            source_roots=args.source_root,
            registry_path=args.registry_path,
            game=args.game,
            platform=args.platform,
            output_root=args.output_root,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.refresh_real_artifact_intake:
        result = run_refresh_real_artifact_intake_fn(
            intake_root=args.intake_root,
            registry_path=args.registry_path,
            game=args.game,
            platform=args.platform,
            require_resolved_dedup=args.require_resolved_dedup,
            record_dashboard_summary_history=args.record_dashboard_summary_history_on_refresh,
            record_refresh_outcome_history=args.record_refresh_outcome_history_on_refresh,
            render_dashboard=args.render_dashboard_on_refresh,
            refresh_artifact_registry=args.refresh_artifact_registry_on_refresh,
            comparison_manifest=args.compare_evidence_on_refresh,
            output_root=args.output_root,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.report_posted_performance:
        result = run_report_posted_performance_fn(
            registry_path=args.registry_path,
            game=args.game,
            platform=args.platform,
            account_id=args.account_id,
            workflow_run_id=args.workflow_run_id,
            candidate_id=args.candidate_id,
            fixture_id=args.fixture_id,
            hook_archetype=args.hook_archetype,
            hook_mode=args.hook_mode,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, command_name="report_posted_performance", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    return None


def _json_exit(result: dict[str, Any], *, check_ok: bool = True) -> int:
    print(json.dumps(result, indent=2))
    if not check_ok:
        return 0
    return 0 if result.get("ok") else 1
