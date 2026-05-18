from __future__ import annotations

import argparse
import json
from typing import Any, Callable


def dispatch_detector_calibration_operator_commands(
    args: argparse.Namespace,
    *,
    run_inspect_detector_calibration_followup_report_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_promotion_triage_manifest_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_publish_decision_manifest_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_evidence_expansion_queue_manifest_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_evidence_expansion_progress_manifest_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_next_actions_manifest_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_next_action_apply_ledger_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_next_action_apply_history_summary_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_next_action_apply_history_trend_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger_fn: Callable[..., dict[str, Any]],
    run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary_fn: Callable[..., dict[str, Any]],
    run_apply_detector_calibration_next_action_fn: Callable[..., dict[str, Any]],
    run_apply_detector_calibration_next_actions_batch_fn: Callable[..., dict[str, Any]],
) -> int | None:
    if args.inspect_detector_calibration_followup_report:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_followup_report_fn(
                args.inspect_detector_calibration_followup_report,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_promotion_triage_manifest:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_promotion_triage_manifest_fn(
                args.inspect_detector_calibration_promotion_triage_manifest,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_publish_decision_manifest:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_publish_decision_manifest_fn(
                args.inspect_detector_calibration_publish_decision_manifest,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_evidence_expansion_queue_manifest:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_evidence_expansion_queue_manifest_fn(
                args.inspect_detector_calibration_evidence_expansion_queue_manifest,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_evidence_expansion_progress_manifest:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_evidence_expansion_progress_manifest_fn(
                args.inspect_detector_calibration_evidence_expansion_progress_manifest,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_next_actions_manifest:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_next_actions_manifest_fn(
                args.inspect_detector_calibration_next_actions_manifest,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_next_action_apply_ledger:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_next_action_apply_ledger_fn(
                args.inspect_detector_calibration_next_action_apply_ledger,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_next_action_apply_history_summary:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_next_action_apply_history_summary_fn(
                args.inspect_detector_calibration_next_action_apply_history_summary,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_next_action_apply_history_trend:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_next_action_apply_history_trend_fn(
                args.inspect_detector_calibration_next_action_apply_history_trend,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_next_action_cross_game_ledger_comparison:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_fn(
                args.inspect_detector_calibration_next_action_cross_game_ledger_comparison,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger_fn(
                args.inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger,
                emit_json=bool(args.json),
            )
        )

    if args.inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary:
        return _rendered_command_exit(
            lambda: run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary_fn(
                args.inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary,
                emit_json=bool(args.json),
            )
        )

    if args.apply_detector_calibration_next_action:
        if not args.asset_id:
            print("Error: --apply-detector-calibration-next-action requires --asset-id")
            return 1
        result = run_apply_detector_calibration_next_action_fn(
            args.apply_detector_calibration_next_action,
            asset_id=args.asset_id,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.apply_detector_calibration_next_actions:
        result = run_apply_detector_calibration_next_actions_batch_fn(
            args.apply_detector_calibration_next_actions,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    return None


def _rendered_command_exit(command: Callable[[], dict[str, Any]]) -> int:
    try:
        result = command()
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    print(result["rendered_output"])
    return 0 if result.get("ok") else 1
