from __future__ import annotations

import argparse
import json
from typing import Any, Callable


def dispatch_onboarding_analysis_commands(
    args: argparse.Namespace,
    *,
    parser: argparse.ArgumentParser,
    print_cli_result_fn: Callable[..., None],
    run_enrich_game_from_wiki_fn: Callable[..., dict[str, Any]],
    run_adapt_game_schema_fn: Callable[..., dict[str, Any]],
    run_ingest_game_sources_fn: Callable[..., dict[str, Any]],
    run_curate_wiki_medal_draft_fn: Callable[..., dict[str, Any]],
    run_bridge_wiki_draft_to_onboarding_fn: Callable[..., dict[str, Any]],
    run_build_onboarding_draft_fn: Callable[..., dict[str, Any]],
    run_report_unresolved_derived_rows_fn: Callable[..., dict[str, Any]],
    run_derive_game_detection_manifest_fn: Callable[..., dict[str, Any]],
    run_fill_derived_detection_rows_fn: Callable[..., dict[str, Any]],
    run_prepare_derived_row_review_fn: Callable[..., dict[str, Any]],
    run_summarize_derived_row_review_fn: Callable[..., dict[str, Any]],
    run_apply_derived_row_review_fn: Callable[..., dict[str, Any]],
    run_onboard_game_fn: Callable[..., dict[str, Any]],
    run_publish_onboarding_draft_fn: Callable[..., dict[str, Any]],
    run_publish_onboarding_batch_fn: Callable[..., dict[str, Any]],
    run_validate_onboarding_publish_fn: Callable[..., dict[str, Any]],
    run_report_onboarding_batch_fn: Callable[..., dict[str, Any]],
    run_fuse_clip_signals_fn: Callable[..., dict[str, Any]],
    run_match_roi_templates_fn: Callable[..., dict[str, Any]],
    run_map_roi_events_fn: Callable[..., dict[str, Any]],
    run_analyze_roi_runtime_fn: Callable[..., dict[str, Any]],
) -> int | None:
    if args.enrich_game_from_wiki:
        if not args.wiki_url and not args.wiki_manifest and not args.wiki_source:
            parser.error("--enrich-game-from-wiki requires --wiki-url, --wiki-manifest, or --wiki-source")
        result = run_enrich_game_from_wiki_fn(
            args.enrich_game_from_wiki,
            args.wiki_url,
            wiki_manifest=args.wiki_manifest,
            wiki_sources=args.wiki_source,
        )
        return _json_exit(result)

    if args.adapt_game_schema:
        return _json_exit(run_adapt_game_schema_fn(args.adapt_game_schema))

    if args.ingest_game_sources:
        if not args.source_manifest:
            parser.error("--ingest-game-sources requires --source-manifest")
        result = run_ingest_game_sources_fn(args.ingest_game_sources, args.source_manifest)
        return _json_exit(result)

    if args.curate_wiki_medal_draft:
        result = run_curate_wiki_medal_draft_fn(
            args.curate_wiki_medal_draft,
            output_path=args.output_path,
            profile=args.curation_profile,
        )
        return _json_exit(result)

    if args.bridge_wiki_draft_to_onboarding:
        result = run_bridge_wiki_draft_to_onboarding_fn(
            args.bridge_wiki_draft_to_onboarding,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.build_onboarding_draft:
        return _json_exit(run_build_onboarding_draft_fn(args.build_onboarding_draft))

    if args.report_unresolved_derived_rows:
        result = run_report_unresolved_derived_rows_fn(
            args.report_unresolved_derived_rows,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, command_name="report_unresolved_derived_rows", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.derive_game_detection_manifest:
        result = run_derive_game_detection_manifest_fn(
            args.derive_game_detection_manifest,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.fill_derived_detection_rows:
        if not args.detection_id:
            parser.error("--fill-derived-detection-rows requires at least one --detection-id")
        if not args.fill_source_manifest:
            parser.error("--fill-derived-detection-rows requires at least one --fill-source-manifest")
        result = run_fill_derived_detection_rows_fn(
            args.fill_derived_detection_rows,
            detection_ids=args.detection_id,
            source_manifests=args.fill_source_manifest,
            output_path=args.output_path,
        )
        return _json_exit(result)

    if args.prepare_derived_row_review:
        if not args.detection_id:
            parser.error("--prepare-derived-row-review requires at least one --detection-id")
        result = run_prepare_derived_row_review_fn(
            args.prepare_derived_row_review,
            detection_ids=args.detection_id,
        )
        return _json_exit(result)

    if args.summarize_derived_row_review:
        result = run_summarize_derived_row_review_fn(args.summarize_derived_row_review)
        print_cli_result_fn(result, command_name="summarize_derived_row_review", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.apply_derived_row_review:
        result = run_apply_derived_row_review_fn(
            args.apply_derived_row_review,
            accept_recommended=args.accept_recommended,
            only_auto_populated=args.only_auto_populated,
            reject_zero_candidate=args.reject_zero_candidate,
            defer_zero_candidate=args.defer_zero_candidate,
        )
        print_cli_result_fn(result, command_name="apply_derived_row_review", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.onboard_game:
        if not args.source_manifest:
            parser.error("--onboard-game requires --source-manifest")
        result = run_onboard_game_fn(args.onboard_game, args.source_manifest)
        return _json_exit(result)

    if args.publish_onboarding_draft:
        return _json_exit(run_publish_onboarding_draft_fn(args.publish_onboarding_draft))

    if args.publish_onboarding_batch:
        result = run_publish_onboarding_batch_fn(
            args.publish_onboarding_batch,
            game=args.game,
            apply=args.apply,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, command_name="publish_onboarding_batch", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.validate_onboarding_publish:
        result = run_validate_onboarding_publish_fn(args.validate_onboarding_publish)
        print_cli_result_fn(result, command_name="validate_onboarding_publish", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.report_onboarding_batch:
        result = run_report_onboarding_batch_fn(
            args.report_onboarding_batch,
            game=args.game,
            output_path=args.output_path,
        )
        print_cli_result_fn(result, command_name="report_onboarding_batch", full_json=args.full_json)
        return 0 if result.get("ok") else 1

    if args.fuse_clip_signals:
        source, game = args.fuse_clip_signals
        result = run_fuse_clip_signals_fn(
            source,
            game,
            proxy_sidecar=args.proxy_sidecar,
            runtime_sidecar=args.runtime_sidecar,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
        )
        return _json_exit(result)

    if args.match_roi_templates:
        source, game = args.match_roi_templates
        result = run_match_roi_templates_fn(
            source,
            game,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            output_path=args.output_path,
            min_score=args.min_score,
            debug_output_dir=args.debug_output_dir,
        )
        return _json_exit(result)

    if args.map_roi_events:
        source, game = args.map_roi_events
        result = run_map_roi_events_fn(
            source,
            game,
            matcher_report=args.matcher_report,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
        )
        return _json_exit(result)

    if args.analyze_roi_runtime:
        source, game = args.analyze_roi_runtime
        result = run_analyze_roi_runtime_fn(
            source,
            game,
            matcher_report=args.matcher_report,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
        )
        return _json_exit(result)

    return None


def _json_exit(result: dict[str, Any]) -> int:
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1
