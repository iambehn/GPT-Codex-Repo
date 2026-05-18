from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

REQUIRED_TOP_LEVEL_FIELDS = (
    "schema_version",
    "game",
    "source_ledger_path",
    "source_row_count",
    "trend",
)
REQUIRED_TREND_FIELDS = (
    "total_runs",
    "latest_run_id",
    "latest_status",
    "latest_recorded_at",
    "ok_rate",
    "partial_failure_rate",
    "ledger_record_failed_rate",
    "average_selected_rows_per_run",
    "average_applied_rows_per_run",
    "average_created_count_per_run",
    "average_reused_count_per_run",
    "average_failed_count_per_run",
    "recent_window_size",
    "recent_window_size_actual",
    "recent_ok_run_count",
    "recent_partial_failure_run_count",
    "recent_ledger_record_failed_run_count",
    "recent_average_selected_rows_per_run",
    "recent_average_applied_rows_per_run",
)


def inspect_detector_calibration_next_action_apply_history_trend(
    *,
    trend: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    trend_path = _resolve_path(trend)
    payload = _load_json(trend_path)
    _validate_trend_payload(payload)
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(trend_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "trend_path": str(trend_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "trend_payload": payload,
    }


def _render_compact_text(trend_path: Path, payload: dict[str, Any]) -> str:
    trend = payload.get("trend") if isinstance(payload.get("trend"), dict) else {}
    lines = [
        f"Trend path: {trend_path}",
        f"Game: {payload.get('game')}",
        f"Source ledger path: {payload.get('source_ledger_path')}",
        f"Source row count: {payload.get('source_row_count')}",
        "",
        "Trend",
        f"Total runs: {_display(trend.get('total_runs'))}",
        f"Latest run id: {_display(trend.get('latest_run_id'))}",
        f"Latest status: {_display(trend.get('latest_status'))}",
        f"Latest recorded at: {_display(trend.get('latest_recorded_at'))}",
        f"Ok rate: {_display(trend.get('ok_rate'))}",
        f"Partial failure rate: {_display(trend.get('partial_failure_rate'))}",
        f"Ledger record failed rate: {_display(trend.get('ledger_record_failed_rate'))}",
        f"Average selected rows per run: {_display(trend.get('average_selected_rows_per_run'))}",
        f"Average applied rows per run: {_display(trend.get('average_applied_rows_per_run'))}",
        f"Average created count per run: {_display(trend.get('average_created_count_per_run'))}",
        f"Average reused count per run: {_display(trend.get('average_reused_count_per_run'))}",
        f"Average failed count per run: {_display(trend.get('average_failed_count_per_run'))}",
        f"Recent window size: {_display(trend.get('recent_window_size'))}",
        f"Recent window size actual: {_display(trend.get('recent_window_size_actual'))}",
        f"Recent ok run count: {_display(trend.get('recent_ok_run_count'))}",
        f"Recent partial failure run count: {_display(trend.get('recent_partial_failure_run_count'))}",
        f"Recent ledger record failed run count: {_display(trend.get('recent_ledger_record_failed_run_count'))}",
        f"Recent average selected rows per run: {_display(trend.get('recent_average_selected_rows_per_run'))}",
        f"Recent average applied rows per run: {_display(trend.get('recent_average_applied_rows_per_run'))}",
    ]
    return "\n".join(lines)


def _validate_trend_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("trend payload must be a mapping")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"trend missing required fields: {', '.join(missing)}")
    trend = payload.get("trend")
    if not isinstance(trend, dict):
        raise ValueError("trend field must be a mapping")
    missing_trend_fields = [field for field in REQUIRED_TREND_FIELDS if field not in trend]
    if missing_trend_fields:
        raise ValueError(f"trend missing required nested fields: {', '.join(missing_trend_fields)}")


def _display(value: Any) -> str:
    if value is None or value == "":
        return "None"
    return str(value)


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect detector calibration next-action apply history trend")
    parser.add_argument("--trend", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_next_action_apply_history_trend(
            trend=args.trend,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
