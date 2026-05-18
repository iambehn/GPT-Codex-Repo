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

from tools.inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger import (  # noqa: E402
    _load_json,
    _resolve_path,
    _validate_ledger_payload,
)


SCHEMA_VERSION = "detector_calibration_next_action_cross_game_ledger_comparison_history_summary_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"


def generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
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

    rows = ledger_payload.get("rows") if isinstance(ledger_payload.get("rows"), list) else []
    summary = _build_summary(rows)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_ledger_path": str(ledger_path),
        "source_row_count": len(rows),
        "summary": summary,
    }
    target_output_path = _resolve_output_path(output_path=output_path)
    _write_json(target_output_path, artifact)
    status = "empty_ledger" if not rows else "ok"
    return {
        "ok": True,
        "status": status,
        "output_path": str(target_output_path),
        "source_ledger_path": str(ledger_path),
        "source_row_count": len(rows),
        "summary": summary,
        "emitted_summary": {
            "source_ledger_path": str(ledger_path),
            "source_row_count": len(rows),
            "total_runs": summary["total_runs"],
            "ok_run_count": summary["ok_run_count"],
            "partial_input_failure_run_count": summary["partial_input_failure_run_count"],
            "ledger_record_failed_run_count": summary["ledger_record_failed_run_count"],
            "latest_run_id": summary["latest_run_id"],
            "latest_status": summary["latest_status"],
            "latest_top_game": summary["latest_top_game"],
        },
    }


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latest_row = rows[-1] if rows else None
    top_game_counts: dict[str, int] = {}
    for row in rows:
        top_game = str(row.get("top_game") or "").strip()
        if not top_game:
            continue
        top_game_counts[top_game] = top_game_counts.get(top_game, 0) + 1
    return {
        "total_runs": len(rows),
        "ok_run_count": sum(1 for row in rows if str(row.get("status") or "").strip() == "ok"),
        "partial_input_failure_run_count": sum(
            1 for row in rows if str(row.get("status") or "").strip() == "partial_input_failure"
        ),
        "ledger_record_failed_run_count": sum(
            1 for row in rows if str(row.get("status") or "").strip() == "ledger_record_failed"
        ),
        "top_game_counts": top_game_counts,
        "latest_run_id": latest_row.get("run_id") if isinstance(latest_row, dict) else None,
        "latest_status": latest_row.get("status") if isinstance(latest_row, dict) else None,
        "latest_top_game": latest_row.get("top_game") if isinstance(latest_row, dict) else None,
        "latest_valid_game_count": _safe_int(latest_row.get("valid_game_count")) if isinstance(latest_row, dict) else None,
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _resolve_output_path(*, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return (
        DEFAULT_OUTPUT_ROOT
        / "cross_game_analysis"
        / "history"
        / "summary"
        / f"{timestamp}.detector_calibration_next_action_cross_game_ledger_comparison_history_summary.json"
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate detector calibration next-action cross-game ledger comparison history summary"
    )
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
        ledger=args.ledger,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
