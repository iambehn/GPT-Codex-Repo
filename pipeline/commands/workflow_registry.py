from __future__ import annotations

import argparse
import json
from typing import Any, Callable


def dispatch_workflow_registry_commands(
    args: argparse.Namespace,
    *,
    parser: argparse.ArgumentParser,
    phase: str = "all",
    print_cli_result_fn: Callable[..., None],
    run_compare_fixture_sidecars_fn: Callable[..., dict[str, Any]],
    run_fixture_trial_fn: Callable[..., dict[str, Any]],
    run_compare_fixture_trials_fn: Callable[..., dict[str, Any]],
    run_fixture_trial_batch_fn: Callable[..., dict[str, Any]],
    run_query_workflow_queue_fn: Callable[..., dict[str, Any]],
    run_refresh_clip_registry_fn: Callable[..., dict[str, Any]],
    run_query_clip_registry_fn: Callable[..., dict[str, Any]],
    run_transition_candidate_lifecycle_fn: Callable[..., dict[str, Any]],
) -> int | None:
    if phase not in {"all", "pre_export", "post_export"}:
        raise ValueError(f"unsupported workflow registry dispatch phase: {phase}")

    if phase in {"all", "pre_export"} and args.compare_fixture_sidecars:
        if not args.baseline_sidecar_root or not args.trial_sidecar_root:
            parser.error("--compare-fixture-sidecars requires --baseline-sidecar-root and --trial-sidecar-root")
        print(
            json.dumps(
                run_compare_fixture_sidecars_fn(
                    args.compare_fixture_sidecars,
                    baseline_sidecar_root=args.baseline_sidecar_root,
                    trial_sidecar_root=args.trial_sidecar_root,
                    artifact_layer=args.artifact_layer,
                    game=args.game,
                    output_path=args.output_path,
                ),
                indent=2,
            )
        )
        return 0

    if phase in {"all", "pre_export"} and args.run_fixture_trial:
        if not args.fixture_source_manifest or not args.trial_name:
            parser.error("--run-fixture-trial requires --fixture-source-manifest and --trial-name")
        result = run_fixture_trial_fn(
            args.run_fixture_trial,
            fixture_source_manifest=args.fixture_source_manifest,
            trial_name=args.trial_name,
            output_root=args.output_root,
            game=args.game,
            pattern=args.pattern,
            limit=args.limit,
            proposal_backend=args.proposal_backend,
            asr_backend=args.asr_backend,
            emit_runtime=args.emit_runtime,
            emit_fused=args.emit_fused,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if phase in {"all", "pre_export"} and args.compare_fixture_trials:
        if not args.baseline_run_root or not args.trial_run_root:
            parser.error("--compare-fixture-trials requires --baseline-run-root and --trial-run-root")
        result = run_compare_fixture_trials_fn(
            args.compare_fixture_trials,
            baseline_run_root=args.baseline_run_root,
            trial_run_root=args.trial_run_root,
            artifact_layer=args.artifact_layer,
            game=args.game,
            output_path=args.output_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if phase in {"all", "pre_export"} and args.run_fixture_trial_batch:
        if not args.fixture_source_manifest:
            parser.error("--run-fixture-trial-batch requires --fixture-source-manifest")
        result = run_fixture_trial_batch_fn(
            args.run_fixture_trial_batch,
            fixture_source_manifest=args.fixture_source_manifest,
            trial_names=args.trial,
            batch_name=args.batch_name,
            output_root=args.output_root,
            game=args.game,
            pattern=args.pattern,
            limit=args.limit,
            emit_runtime=args.emit_runtime,
            emit_fused=args.emit_fused,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if phase not in {"all", "post_export"}:
        return None

    if args.query_workflow_queue:
        if not args.workflow_type:
            parser.error("--query-workflow-queue requires --workflow-type")
        result = run_query_workflow_queue_fn(
            args.workflow_type,
            registry_path=args.registry_path,
            game=args.game,
            fixture_id=args.fixture_id,
            limit=args.limit,
        )
        print_cli_result_fn(result, command_name="query_workflow_queue", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.refresh_clip_registry:
        result = run_refresh_clip_registry_fn(
            args.refresh_clip_registry,
            game=args.game,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            registry_path=args.registry_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.query_clip_registry:
        result = run_query_clip_registry_fn(
            mode=args.mode or "fused-events",
            game=args.game,
            event_type=args.event_type,
            action=args.action,
            review_status=args.review_status,
            gate_status=args.gate_status,
            fixture_id=args.fixture_id,
            trial_name=args.trial_name,
            artifact_layer=args.artifact_layer,
            recommendation_decision=args.recommendation_decision,
            coverage_status=args.coverage_status,
            has_disagreement=True if args.has_disagreement else None,
            candidate_id=args.candidate_id,
            lifecycle_state=args.lifecycle_state,
            hook_archetype=args.hook_archetype,
            hook_mode=args.hook_mode,
            comparison_status=args.comparison_status,
            export_status=args.export_status,
            post_status=args.post_status,
            platform=args.platform,
            account_id=args.account_id,
            evidence_mode=args.evidence_mode,
            model_family=args.model_family,
            training_target=args.training_target,
            workflow_type=args.workflow_type,
            workflow_run_id=args.workflow_run_id,
            stage=args.stage,
            status=args.status,
            limit=args.limit,
            registry_path=args.registry_path,
        )
        print_cli_result_fn(result, command_name="query_clip_registry", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.transition_candidate_lifecycle:
        if not args.candidate_id or not args.to_state:
            parser.error("--transition-candidate-lifecycle requires --candidate-id and --to-state")
        result = run_transition_candidate_lifecycle_fn(
            args.candidate_id,
            args.to_state,
            reason=args.reason,
            source_artifact=args.source_artifact,
            actor=args.actor,
            registry_path=args.registry_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    return None
