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


SCHEMA_VERSION = "detector_calibration_next_action_apply_history_summary_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"


def generate_detector_calibration_next_action_apply_history_summary(
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
    summary = _build_summary(rows)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_ledger_path": str(ledger_path),
        "source_row_count": len(rows),
        "summary": summary,
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
        "summary": summary,
        "emitted_summary": {
            "source_ledger_path": str(ledger_path),
            "source_row_count": len(rows),
            "total_runs": summary["total_runs"],
            "ok_run_count": summary["ok_run_count"],
            "partial_failure_run_count": summary["partial_failure_run_count"],
            "total_selected_rows": summary["total_selected_rows"],
            "total_applied_rows": summary["total_applied_rows"],
            "total_created_count": summary["total_created_count"],
            "total_reused_count": summary["total_reused_count"],
            "total_failed_count": summary["total_failed_count"],
            "latest_run_id": summary["latest_run_id"],
            "latest_status": summary["latest_status"],
            "latest_top_asset_id": summary["latest_top_asset_id"],
        },
    }


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latest_row = rows[-1] if rows else None
    return {
        "total_runs": len(rows),
        "ok_run_count": sum(1 for row in rows if str(row.get("status") or "").strip() == "ok"),
        "partial_failure_run_count": sum(
            1 for row in rows if str(row.get("status") or "").strip() == "partial_failure"
        ),
        "ledger_record_failed_run_count": sum(
            1 for row in rows if str(row.get("status") or "").strip() == "ledger_record_failed"
        ),
        "no_actionable_rows_count": sum(
            1 for row in rows if str(row.get("status") or "").strip() == "no_actionable_rows"
        ),
        "invalid_next_actions_manifest_count": sum(
            1 for row in rows if str(row.get("status") or "").strip() == "invalid_next_actions_manifest"
        ),
        "total_selected_rows": sum(_safe_int(row.get("selected_row_count")) for row in rows),
        "total_applied_rows": sum(_safe_int(row.get("applied_row_count")) for row in rows),
        "total_created_count": sum(_safe_int(row.get("created_count")) for row in rows),
        "total_reused_count": sum(_safe_int(row.get("reused_count")) for row in rows),
        "total_failed_count": sum(_safe_int(row.get("failed_count")) for row in rows),
        "latest_run_id": latest_row.get("run_id") if isinstance(latest_row, dict) else None,
        "latest_status": latest_row.get("status") if isinstance(latest_row, dict) else None,
        "latest_top_asset_id": latest_row.get("top_asset_id") if isinstance(latest_row, dict) else None,
    }


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
        / "summary"
        / f"{timestamp}.detector_calibration_next_action_apply_history_summary.json"
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration next-action apply history summary")
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_next_action_apply_history_summary(
        ledger=args.ledger,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
