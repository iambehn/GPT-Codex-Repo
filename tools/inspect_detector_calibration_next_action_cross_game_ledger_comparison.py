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
    "generated_at",
    "source_ledger_paths",
    "row_count",
    "rows",
    "input_errors",
)
REQUIRED_ROW_FIELDS = (
    "game",
    "latest_status",
    "ok_rate",
    "partial_failure_rate",
    "ledger_record_failed_rate",
    "average_failed_count_per_run",
    "latest_top_asset_id",
)
REQUIRED_ERROR_FIELDS = (
    "source_ledger_path",
    "error",
)


def inspect_detector_calibration_next_action_cross_game_ledger_comparison(
    *,
    comparison: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    comparison_path = _resolve_path(comparison)
    payload = _load_json(comparison_path)
    _validate_comparison_payload(payload)
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(comparison_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "comparison_path": str(comparison_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "comparison_payload": payload,
    }


def _render_compact_text(comparison_path: Path, payload: dict[str, Any]) -> str:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    input_errors = payload.get("input_errors") if isinstance(payload.get("input_errors"), list) else []
    lines = [
        f"Comparison path: {comparison_path}",
        f"Row count: {payload.get('row_count')}",
        f"Source ledger count: {len(payload.get('source_ledger_paths') or [])}",
        f"Input error count: {len(input_errors)}",
        "",
        "Top game",
    ]
    top_row = rows[0] if rows else None
    if isinstance(top_row, dict):
        lines.append(f"Game: {_display(top_row.get('game'))}")
        lines.append(f"Latest status: {_display(top_row.get('latest_status'))}")
        lines.append(f"Ok rate: {_display(top_row.get('ok_rate'))}")
        lines.append(f"Partial failure rate: {_display(top_row.get('partial_failure_rate'))}")
        lines.append(f"Ledger record failed rate: {_display(top_row.get('ledger_record_failed_rate'))}")
        lines.append(f"Average failed count per run: {_display(top_row.get('average_failed_count_per_run'))}")
        lines.append(f"Latest top asset id: {_display(top_row.get('latest_top_asset_id'))}")
    else:
        lines.append("None")

    lines.append("")
    lines.append("Games")
    if not rows:
        lines.append("None")
    else:
        for row in rows:
            lines.append(
                f"- {_display(row.get('game'))} | latest_status={_display(row.get('latest_status'))} | ok_rate={_display(row.get('ok_rate'))} | average_failed_count_per_run={_display(row.get('average_failed_count_per_run'))}"
            )

    lines.append("")
    lines.append("Input errors")
    if not input_errors:
        lines.append("None")
    else:
        for error in input_errors:
            lines.append(
                f"- {_display(error.get('source_ledger_path'))} | error={_display(error.get('error'))}"
            )
    return "\n".join(lines)


def _validate_comparison_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("comparison payload must be a mapping")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"comparison missing required fields: {', '.join(missing)}")
    if not isinstance(payload.get("source_ledger_paths"), list):
        raise ValueError("comparison source_ledger_paths must be a list")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("comparison rows must be a list")
    if not isinstance(payload.get("input_errors"), list):
        raise ValueError("comparison input_errors must be a list")
    for row in payload["rows"]:
        if not isinstance(row, dict):
            raise ValueError("comparison rows must contain mappings")
        missing_row_fields = [field for field in REQUIRED_ROW_FIELDS if field not in row]
        if missing_row_fields:
            raise ValueError(f"comparison row missing required fields: {', '.join(missing_row_fields)}")
    for error in payload["input_errors"]:
        if not isinstance(error, dict):
            raise ValueError("comparison input_errors must contain mappings")
        missing_error_fields = [field for field in REQUIRED_ERROR_FIELDS if field not in error]
        if missing_error_fields:
            raise ValueError(f"comparison input error missing required fields: {', '.join(missing_error_fields)}")


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
    parser = argparse.ArgumentParser(description="Inspect detector calibration next-action cross-game ledger comparison")
    parser.add_argument("--comparison", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_next_action_cross_game_ledger_comparison(
            comparison=args.comparison,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
