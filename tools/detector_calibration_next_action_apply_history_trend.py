from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_detector_calibration_next_action_apply_ledger import (  # noqa: E402
    _load_json,
    _resolve_path,
    _validate_ledger_payload,
)


SCHEMA_VERSION = "detector_calibration_next_action_apply_history_trend_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
RECENT_WINDOW_SIZE = 5


def generate_detector_calibration_next_action_apply_history_trend(
    *,
    ledger: str | Path,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    ledger_path = _resolve_path(ledger)
    try:
        ledger_payload = _load_json(ledger_path)
        _validate_ledger_payload(ledger_payload)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        return {
            "ok": False,
            "status": "invalid_ledger",
            "source_ledger_path": str(ledger_path),
            "error": str(exc),
        }

    game = str(ledger_payload.get("game") or "").strip()
    rows = ledger_payload.get("rows") if isinstance(ledger_payload.get("rows"), list) else []
    trend = _build_trend(rows)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_ledger_path": str(ledger_path),
        "source_row_count": len(rows),
        "trend": trend,
    }
    target_output_path = _resolve_output_path(game=game, output_path=output_path)
    _write_json(target_output_path, artifact)
    status = "empty_ledger" if not rows else "ok"
    return {
        "ok": True,
        "status": status,
        "output_path": str(target_output_path),
        "source_ledger_path": str(ledger_path),
        "source_row_count": len(rows),
        "trend": trend,
        "emitted_trend": {
            "source_ledger_path": str(ledger_path),
            "source_row_count": len(rows),
            "total_runs": trend["total_runs"],
            "latest_run_id": trend["latest_run_id"],
            "latest_status": trend["latest_status"],
            "ok_rate": trend["ok_rate"],
            "partial_failure_rate": trend["partial_failure_rate"],
            "ledger_record_failed_rate": trend["ledger_record_failed_rate"],
            "average_selected_rows_per_run": trend["average_selected_rows_per_run"],
            "average_applied_rows_per_run": trend["average_applied_rows_per_run"],
            "recent_window_size": trend["recent_window_size"],
            "recent_window_size_actual": trend["recent_window_size_actual"],
            "recent_ok_run_count": trend["recent_ok_run_count"],
            "recent_partial_failure_run_count": trend["recent_partial_failure_run_count"],
        },
    }


def _build_trend(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_runs = len(rows)
    latest_row = rows[-1] if rows else None
    recent_rows = rows[-min(RECENT_WINDOW_SIZE, total_runs) :] if rows else []
    recent_window_size_actual = len(recent_rows)
    return {
        "total_runs": total_runs,
        "latest_run_id": latest_row.get("run_id") if isinstance(latest_row, dict) else None,
        "latest_status": latest_row.get("status") if isinstance(latest_row, dict) else None,
        "latest_recorded_at": latest_row.get("recorded_at") if isinstance(latest_row, dict) else None,
        "ok_rate": _rate(_count_status(rows, "ok"), total_runs),
        "partial_failure_rate": _rate(_count_status(rows, "partial_failure"), total_runs),
        "ledger_record_failed_rate": _rate(_count_status(rows, "ledger_record_failed"), total_runs),
        "average_selected_rows_per_run": _average(rows, "selected_row_count"),
        "average_applied_rows_per_run": _average(rows, "applied_row_count"),
        "average_created_count_per_run": _average(rows, "created_count"),
        "average_reused_count_per_run": _average(rows, "reused_count"),
        "average_failed_count_per_run": _average(rows, "failed_count"),
        "recent_window_size": RECENT_WINDOW_SIZE,
        "recent_window_size_actual": recent_window_size_actual,
        "recent_ok_run_count": _count_status(recent_rows, "ok"),
        "recent_partial_failure_run_count": _count_status(recent_rows, "partial_failure"),
        "recent_ledger_record_failed_run_count": _count_status(recent_rows, "ledger_record_failed"),
        "recent_average_selected_rows_per_run": _average(recent_rows, "selected_row_count"),
        "recent_average_applied_rows_per_run": _average(recent_rows, "applied_row_count"),
    }


def _count_status(rows: list[dict[str, Any]], status: str) -> int:
    return sum(1 for row in rows if str(row.get("status") or "").strip() == status)


def _average(rows: list[dict[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    return sum(_safe_int(row.get(field)) for row in rows) / len(rows)


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return (
        DEFAULT_OUTPUT_ROOT
        / game
        / "batch_apply_history"
        / "trends"
        / f"{timestamp}.detector_calibration_next_action_apply_history_trend.json"
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration next-action apply history trend")
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_next_action_apply_history_trend(
        ledger=args.ledger,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
