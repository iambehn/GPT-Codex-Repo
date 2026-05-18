from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.artifact_paths import (
    resolve_timestamped_output_path as _artifact_resolve_timestamped_output_path,
    utc_now_iso as _artifact_utc_now_iso,
    utc_timestamp_slug as _artifact_utc_timestamp_slug,
    write_json as _artifact_write_json,
)
from tools.detector_calibration_next_action_apply_history_trend import (  # noqa: E402
    _build_trend,
)
from tools.inspect_detector_calibration_next_action_apply_ledger import (  # noqa: E402
    _load_json,
    _resolve_path,
    _validate_ledger_payload,
)


SCHEMA_VERSION = "detector_calibration_next_action_cross_game_ledger_comparison_v1"
LEDGER_SCHEMA_VERSION = "detector_calibration_next_action_cross_game_ledger_comparison_ledger_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"


def generate_detector_calibration_next_action_cross_game_ledger_comparison(
    *,
    ledgers: list[str | Path],
    output_path: str | Path | None = None,
    record_ledger: bool = False,
    ledger_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_source_paths = [str(_resolve_path(path)) for path in ledgers]
    rows: list[dict[str, Any]] = []
    input_errors: list[dict[str, Any]] = []
    for ledger in ledgers:
        source_ledger_path = _resolve_path(ledger)
        try:
            ledger_payload = _load_json(source_ledger_path)
            _validate_ledger_payload(ledger_payload)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            input_errors.append(
                {
                    "source_ledger_path": str(source_ledger_path),
                    "error": str(exc),
                }
            )
            continue
        game = str(ledger_payload.get("game") or "").strip()
        source_rows = ledger_payload.get("rows") if isinstance(ledger_payload.get("rows"), list) else []
        trend = _build_trend(source_rows)
        rows.append(
            {
                "game": game,
                "source_ledger_path": str(source_ledger_path),
                "source_row_count": len(source_rows),
                "total_runs": trend["total_runs"],
                "latest_run_id": trend["latest_run_id"],
                "latest_status": trend["latest_status"],
                "ok_rate": trend["ok_rate"],
                "partial_failure_rate": trend["partial_failure_rate"],
                "ledger_record_failed_rate": trend["ledger_record_failed_rate"],
                "average_selected_rows_per_run": trend["average_selected_rows_per_run"],
                "average_applied_rows_per_run": trend["average_applied_rows_per_run"],
                "average_created_count_per_run": trend["average_created_count_per_run"],
                "average_reused_count_per_run": trend["average_reused_count_per_run"],
                "average_failed_count_per_run": trend["average_failed_count_per_run"],
                "recent_window_size": trend["recent_window_size"],
                "recent_window_size_actual": trend["recent_window_size_actual"],
                "recent_ok_run_count": trend["recent_ok_run_count"],
                "recent_partial_failure_run_count": trend["recent_partial_failure_run_count"],
                "recent_ledger_record_failed_run_count": trend["recent_ledger_record_failed_run_count"],
                "latest_top_asset_id": source_rows[-1].get("top_asset_id") if source_rows else None,
            }
        )

    if not rows:
        return {
            "ok": False,
            "status": "invalid_input",
            "source_ledger_paths": resolved_source_paths,
            "row_count": 0,
            "rows": [],
            "input_errors": input_errors,
        }

    ordered_rows = sorted(rows, key=_comparison_sort_key)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_ledger_paths": resolved_source_paths,
        "row_count": len(ordered_rows),
        "rows": ordered_rows,
        "input_errors": input_errors,
    }
    target_output_path = _resolve_output_path(output_path=output_path)
    _write_json(target_output_path, artifact)
    status = "partial_input_failure" if input_errors else "ok"
    top_row = ordered_rows[0] if ordered_rows else None
    result = {
        "ok": True,
        "status": status,
        "output_path": str(target_output_path),
        "source_ledger_paths": resolved_source_paths,
        "row_count": len(ordered_rows),
        "rows": ordered_rows,
        "input_errors": input_errors,
        "emitted_comparison": {
            "row_count": len(ordered_rows),
            "source_ledger_paths": resolved_source_paths,
            "top_game": top_row.get("game") if top_row else None,
            "top_ok_rate": top_row.get("ok_rate") if top_row else None,
            "top_latest_status": top_row.get("latest_status") if top_row else None,
            "input_error_count": len(input_errors),
        },
    }
    if not record_ledger:
        return result
    try:
        resolved_ledger_path, ledger_run_id = _record_comparison_ledger(
            comparison_result=result,
            ledger_path=ledger_path,
        )
    except (OSError, json.JSONDecodeError, ValueError) as ledger_exc:
        result["ok"] = False
        result["status"] = "ledger_record_failed"
        result["error"] = str(ledger_exc)
        return result
    result["ledger_recorded"] = True
    result["ledger_path"] = str(resolved_ledger_path)
    result["ledger_run_id"] = ledger_run_id
    return result


def _comparison_sort_key(row: dict[str, Any]) -> tuple[float, float, float, str]:
    return (
        float(row.get("ok_rate") or 0.0),
        -float(row.get("ledger_record_failed_rate") or 0.0),
        -float(row.get("average_failed_count_per_run") or 0.0),
        str(row.get("game") or ""),
    )


def _resolve_output_path(*, output_path: str | Path | None) -> Path:
    return _artifact_resolve_timestamped_output_path(
        output_path=output_path,
        default_dir=DEFAULT_OUTPUT_ROOT / "cross_game_analysis",
        filename_suffix="detector_calibration_next_action_cross_game_ledger_comparison.json",
        timestamp_slug=_utc_timestamp_slug(),
    )


def _default_ledger_path() -> Path:
    return (
        DEFAULT_OUTPUT_ROOT
        / "cross_game_analysis"
        / "history"
        / "detector_calibration_next_action_cross_game_ledger_comparison_ledger.json"
    )


def _build_ledger_row(*, run_id: str, recorded_at: str, comparison_result: dict[str, Any]) -> dict[str, Any]:
    rows = comparison_result.get("rows") if isinstance(comparison_result.get("rows"), list) else []
    top_row = rows[0] if rows and isinstance(rows[0], dict) else None
    games: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        games.append(
            {
                "game": row.get("game"),
                "ok_rate": row.get("ok_rate"),
                "latest_status": row.get("latest_status"),
                "average_failed_count_per_run": row.get("average_failed_count_per_run"),
            }
        )
    return {
        "run_id": run_id,
        "recorded_at": recorded_at,
        "source_ledger_paths": list(comparison_result.get("source_ledger_paths") or []),
        "status": comparison_result.get("status"),
        "ok": bool(comparison_result.get("ok")),
        "valid_game_count": int(comparison_result.get("row_count") or 0),
        "input_error_count": len(comparison_result.get("input_errors") or []),
        "top_game": top_row.get("game") if top_row else None,
        "top_ok_rate": top_row.get("ok_rate") if top_row else None,
        "top_latest_status": top_row.get("latest_status") if top_row else None,
        "games": games,
    }


def _load_or_initialize_ledger(*, ledger_path: Path) -> dict[str, Any]:
    if ledger_path.exists():
        payload = _load_json(ledger_path)
        if str(payload.get("schema_version") or "").strip() != LEDGER_SCHEMA_VERSION:
            raise ValueError("cross-game comparison history ledger schema_version is invalid")
        if not isinstance(payload.get("rows"), list):
            raise ValueError("cross-game comparison history ledger rows must be a list")
        return payload
    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "updated_at": None,
        "row_count": 0,
        "rows": [],
    }


def _record_comparison_ledger(
    *,
    comparison_result: dict[str, Any],
    ledger_path: str | Path | None,
) -> tuple[Path, str]:
    resolved_ledger_path = _resolve_path(ledger_path) if ledger_path is not None else _default_ledger_path()
    run_id = _utc_timestamp_slug()
    recorded_at = _utc_now()
    ledger = _load_or_initialize_ledger(ledger_path=resolved_ledger_path)
    ledger["rows"].append(_build_ledger_row(run_id=run_id, recorded_at=recorded_at, comparison_result=comparison_result))
    ledger["updated_at"] = recorded_at
    ledger["row_count"] = len(ledger["rows"])
    _artifact_write_json(resolved_ledger_path, ledger)
    return resolved_ledger_path, run_id


def _utc_now() -> str:
    return _artifact_utc_now_iso().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _artifact_write_json(path, payload)


def _utc_timestamp_slug() -> str:
    return _artifact_utc_timestamp_slug()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate cross-game detector calibration next-action ledger comparison")
    parser.add_argument("--ledger", dest="ledgers", action="append", required=True)
    parser.add_argument("--output-path")
    parser.add_argument("--record-ledger", action="store_true")
    parser.add_argument("--ledger-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_next_action_cross_game_ledger_comparison(
        ledgers=args.ledgers,
        output_path=args.output_path,
        record_ledger=bool(args.record_ledger),
        ledger_path=args.ledger_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
