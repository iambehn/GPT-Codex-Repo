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
    "updated_at",
    "row_count",
    "rows",
)


def inspect_detector_calibration_next_action_apply_ledger(
    *,
    ledger: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    ledger_path = _resolve_path(ledger)
    payload = _load_json(ledger_path)
    _validate_ledger_payload(payload)
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(ledger_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "ledger_path": str(ledger_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "ledger": payload,
    }


def _render_compact_text(ledger_path: Path, payload: dict[str, Any]) -> str:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    lines = [
        f"Ledger path: {ledger_path}",
        f"Game: {payload.get('game')}",
        f"Row count: {payload.get('row_count')}",
        f"Updated at: {payload.get('updated_at')}",
        "",
        "Latest run",
    ]
    latest_row = rows[-1] if rows else None
    if isinstance(latest_row, dict):
        lines.append(f"Run id: {_display(latest_row.get('run_id'))}")
        lines.append(f"Status: {_display(latest_row.get('status'))}")
        lines.append(f"Ok: {_display(latest_row.get('ok'))}")
        lines.append(f"Selected row count: {_display(latest_row.get('selected_row_count'))}")
        lines.append(f"Applied row count: {_display(latest_row.get('applied_row_count'))}")
        lines.append(f"Created count: {_display(latest_row.get('created_count'))}")
        lines.append(f"Reused count: {_display(latest_row.get('reused_count'))}")
        lines.append(f"Failed count: {_display(latest_row.get('failed_count'))}")
        lines.append(f"Top asset id: {_display(latest_row.get('top_asset_id'))}")
    else:
        lines.append("None")

    lines.append("")
    lines.append("Runs")
    if not rows:
        lines.append("None")
        return "\n".join(lines)
    for row in rows:
        lines.append(
            f"- {_display(row.get('run_id'))} | {_display(row.get('status'))} | applied/selected={_display(row.get('applied_row_count'))}/{_display(row.get('selected_row_count'))} | top_asset_id={_display(row.get('top_asset_id'))}"
        )
    return "\n".join(lines)


def _validate_ledger_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("ledger payload must be a mapping")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"ledger missing required fields: {', '.join(missing)}")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("ledger rows must be a list")
    for row in payload["rows"]:
        if not isinstance(row, dict):
            raise ValueError("ledger rows must contain mappings")
        for field in (
            "run_id",
            "status",
            "ok",
            "selected_row_count",
            "applied_row_count",
            "created_count",
            "reused_count",
            "failed_count",
            "top_asset_id",
        ):
            if field not in row:
                raise ValueError(f"ledger row missing required field: {field}")


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
    parser = argparse.ArgumentParser(description="Inspect detector calibration next-action apply ledger")
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_next_action_apply_ledger(
            ledger=args.ledger,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
