from __future__ import annotations

import argparse
import json
from typing import Any, Callable


def dispatch_review_calibration_commands(
    args: argparse.Namespace,
    *,
    parser: argparse.ArgumentParser,
    phase: str = "all",
    run_calibrate_proxy_review_fn: Callable[..., dict[str, Any]],
    run_replay_proxy_scoring_fn: Callable[..., dict[str, Any]],
    run_calibrate_runtime_review_fn: Callable[..., dict[str, Any]],
    run_replay_runtime_scoring_fn: Callable[..., dict[str, Any]],
    run_validate_fusion_goldset_fn: Callable[..., dict[str, Any]],
    run_replay_fusion_rules_fn: Callable[..., dict[str, Any]],
    run_replay_template_thresholds_fn: Callable[..., dict[str, Any]],
    run_replay_runtime_event_rules_fn: Callable[..., dict[str, Any]],
    run_promote_runtime_scoring_fn: Callable[..., dict[str, Any]],
    run_rollback_runtime_scoring_fn: Callable[..., dict[str, Any]],
    run_prepare_proxy_review_fn: Callable[..., dict[str, Any]],
    run_prepare_runtime_review_fn: Callable[..., dict[str, Any]],
    run_prepare_fused_review_fn: Callable[..., dict[str, Any]],
    run_apply_proxy_review_fn: Callable[..., dict[str, Any]],
    run_apply_runtime_review_fn: Callable[..., dict[str, Any]],
    run_apply_fused_review_fn: Callable[..., dict[str, Any]],
    run_cleanup_proxy_review_fn: Callable[..., dict[str, Any]],
    run_cleanup_runtime_review_fn: Callable[..., dict[str, Any]],
    run_cleanup_fused_review_fn: Callable[..., dict[str, Any]],
    run_prepare_onboarding_identity_review_fn: Callable[..., dict[str, Any]],
    run_apply_onboarding_identity_review_fn: Callable[..., dict[str, Any]],
    run_cleanup_onboarding_identity_review_fn: Callable[..., dict[str, Any]],
    run_render_replay_viewer_fn: Callable[..., dict[str, Any]],
    run_render_proxy_replay_viewer_fn: Callable[..., dict[str, Any]],
    run_render_unified_replay_viewer_fn: Callable[..., dict[str, Any]],
    run_launch_highlight_review_app_fn: Callable[..., dict[str, Any]],
) -> int | None:
    if phase not in {"all", "pre", "post"}:
        raise ValueError(f"unsupported review calibration dispatch phase: {phase}")

    if phase in {"all", "pre"} and args.calibrate_proxy_review:
        result = run_calibrate_proxy_review_fn(
            args.calibrate_proxy_review,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.replay_proxy_scoring:
        if not args.trial_proxy_config:
            parser.error("--replay-proxy-scoring requires --trial-proxy-config")
        result = run_replay_proxy_scoring_fn(
            args.replay_proxy_scoring,
            args.trial_proxy_config,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
            trial_name=args.trial_name,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.calibrate_runtime_review:
        result = run_calibrate_runtime_review_fn(
            args.calibrate_runtime_review,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.replay_runtime_scoring:
        if not args.trial_config:
            parser.error("--replay-runtime-scoring requires --trial-config")
        result = run_replay_runtime_scoring_fn(
            args.replay_runtime_scoring,
            args.trial_config,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
            trial_name=args.trial_name,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.validate_fusion_goldset:
        result = run_validate_fusion_goldset_fn(
            args.validate_fusion_goldset,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            proxy_sidecar_root=args.proxy_sidecar_root,
            runtime_sidecar_root=args.runtime_sidecar_root,
            fused_sidecar_root=args.fused_sidecar_root,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.replay_fusion_rules:
        if not args.trial_rules:
            parser.error("--replay-fusion-rules requires --trial-rules")
        result = run_replay_fusion_rules_fn(
            args.replay_fusion_rules,
            args.trial_rules,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            proxy_sidecar_root=args.proxy_sidecar_root,
            runtime_sidecar_root=args.runtime_sidecar_root,
            trial_name=args.trial_name,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.replay_template_thresholds:
        if not args.trial_templates:
            parser.error("--replay-template-thresholds requires --trial-templates")
        result = run_replay_template_thresholds_fn(
            args.replay_template_thresholds,
            args.trial_templates,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            trial_name=args.trial_name,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.replay_runtime_event_rules:
        if not args.trial_runtime_rules:
            parser.error("--replay-runtime-event-rules requires --trial-runtime-rules")
        result = run_replay_runtime_event_rules_fn(
            args.replay_runtime_event_rules,
            args.trial_runtime_rules,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            trial_name=args.trial_name,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.promote_runtime_scoring:
        result = run_promote_runtime_scoring_fn(
            args.promote_runtime_scoring,
            sidecar_root=args.sidecar_root,
            game=args.game,
            min_reviewed=args.min_reviewed,
            force=args.force,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            trial_name=args.trial_name,
        )
        return _json_exit(result)

    if phase in {"all", "pre"} and args.rollback_runtime_scoring:
        result = run_rollback_runtime_scoring_fn(
            args.rollback_runtime_scoring,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            rollback_name=args.rollback_name,
        )
        return _json_exit(result)

    if phase in {"all", "post"} and args.prepare_proxy_review:
        return _json_exit(
            run_prepare_proxy_review_fn(
                args.prepare_proxy_review,
                batch_report=args.batch_report,
                sidecar_root=args.sidecar_root,
                action=args.action or "download_candidate",
                limit=args.limit,
                gpt_repo=args.gpt_repo,
                session_name=args.session_name,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.prepare_runtime_review:
        return _json_exit(
            run_prepare_runtime_review_fn(
                args.prepare_runtime_review,
                sidecar_root=args.sidecar_root,
                action=args.action,
                limit=args.limit,
                gpt_repo=args.gpt_repo,
                session_name=args.session_name,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.prepare_fused_review:
        return _json_exit(
            run_prepare_fused_review_fn(
                args.prepare_fused_review,
                sidecar_root=args.sidecar_root,
                action=args.action,
                limit=args.limit,
                gpt_repo=args.gpt_repo,
                session_name=args.session_name,
                event_type=args.event_type,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.render_replay_viewer:
        return _json_exit(
            run_render_replay_viewer_fn(
                args.render_replay_viewer,
                fused_sidecar=args.fused_sidecar,
                output_path=args.output_path,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.render_proxy_replay_viewer:
        return _json_exit(
            run_render_proxy_replay_viewer_fn(
                args.render_proxy_replay_viewer,
                output_path=args.output_path,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.render_unified_replay_viewer:
        return _json_exit(
            run_render_unified_replay_viewer_fn(
                proxy_sidecar=args.proxy_sidecar,
                runtime_sidecar=args.runtime_sidecar,
                fused_sidecar=args.fused_sidecar,
                fixture_comparison_report=args.fixture_comparison_report,
                fixture_trial_batch_manifest=args.fixture_trial_batch_manifest,
                proxy_calibration_report=args.proxy_calibration_report,
                proxy_replay_report=args.proxy_replay_report,
                runtime_calibration_report=args.runtime_calibration_report,
                runtime_replay_report=args.runtime_replay_report,
                registry_path=args.registry_path,
                output_path=args.output_path,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.launch_highlight_review_app:
        result = run_launch_highlight_review_app_fn(
            args.launch_highlight_review_app,
            fixture_manifest=args.fixture_manifest,
            fixture_comparison_report=args.fixture_comparison_report,
            fixture_trial_batch_manifest=args.fixture_trial_batch_manifest,
            proxy_review_session_manifest=args.proxy_review_session_manifest,
            fused_review_session_manifest=args.fused_review_session_manifest,
            proxy_calibration_report=args.proxy_calibration_report,
            proxy_replay_report=args.proxy_replay_report,
            runtime_calibration_report=args.runtime_calibration_report,
            runtime_replay_report=args.runtime_replay_report,
            registry_path=args.registry_path,
            output_path=args.output_path,
            launch=True,
        )
        print(json.dumps({key: value for key, value in result.items() if key != "app"}, indent=2))
        return 0 if result.get("ok") else 1

    if phase in {"all", "post"} and args.apply_proxy_review:
        return _json_exit(run_apply_proxy_review_fn(args.apply_proxy_review, gpt_repo=args.gpt_repo), check_ok=False)

    if phase in {"all", "post"} and args.apply_runtime_review:
        return _json_exit(run_apply_runtime_review_fn(args.apply_runtime_review, gpt_repo=args.gpt_repo), check_ok=False)

    if phase in {"all", "post"} and args.apply_fused_review:
        return _json_exit(run_apply_fused_review_fn(args.apply_fused_review, gpt_repo=args.gpt_repo), check_ok=False)

    if phase in {"all", "post"} and args.cleanup_proxy_review:
        return _json_exit(run_cleanup_proxy_review_fn(args.cleanup_proxy_review, gpt_repo=args.gpt_repo), check_ok=False)

    if phase in {"all", "post"} and args.cleanup_runtime_review:
        return _json_exit(run_cleanup_runtime_review_fn(args.cleanup_runtime_review, gpt_repo=args.gpt_repo), check_ok=False)

    if phase in {"all", "post"} and args.cleanup_fused_review:
        return _json_exit(run_cleanup_fused_review_fn(args.cleanup_fused_review, gpt_repo=args.gpt_repo), check_ok=False)

    if phase in {"all", "post"} and args.prepare_onboarding_identity_review:
        return _json_exit(
            run_prepare_onboarding_identity_review_fn(
                args.prepare_onboarding_identity_review,
                gpt_repo=args.gpt_repo,
                session_name=args.session_name,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.apply_onboarding_identity_review:
        return _json_exit(
            run_apply_onboarding_identity_review_fn(
                args.apply_onboarding_identity_review,
                gpt_repo=args.gpt_repo,
            ),
            check_ok=False,
        )

    if phase in {"all", "post"} and args.cleanup_onboarding_identity_review:
        return _json_exit(
            run_cleanup_onboarding_identity_review_fn(
                args.cleanup_onboarding_identity_review,
                gpt_repo=args.gpt_repo,
            ),
            check_ok=False,
        )

    return None


def _json_exit(result: dict[str, Any], *, check_ok: bool = True) -> int:
    print(json.dumps(result, indent=2))
    if not check_ok:
        return 0
    return 0 if result.get("ok") else 1
