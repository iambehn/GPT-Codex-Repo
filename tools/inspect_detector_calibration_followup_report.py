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
    "source_row_count",
    "asset_count",
    "top_followup",
    "top_followups_by_asset",
)


def inspect_detector_calibration_followup_report(
    *,
    report: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    report_path = _resolve_path(report)
    payload = _load_json(report_path)
    _validate_report_payload(payload)
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(report_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "report_path": str(report_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "report": payload,
    }


def _render_compact_text(report_path: Path, payload: dict[str, Any]) -> str:
    lines = [
        f"Report path: {report_path}",
        f"Game: {payload.get('game')}",
        f"Source row count: {payload.get('source_row_count')}",
        f"Asset count: {payload.get('asset_count')}",
    ]
    top_followup = payload.get("top_followup")
    per_asset = payload.get("top_followups_by_asset")
    if top_followup is None and isinstance(per_asset, list) and not per_asset:
        lines.append("")
        lines.append("Top follow-up")
        lines.append("No detector calibration follow-up rows.")
        return "\n".join(lines)

    if isinstance(top_followup, dict):
        lines.append("")
        lines.append("Top follow-up")
        lines.append(f"Asset id: {_display(top_followup.get('asset_id'))}")
        lines.append(f"Run id: {_display(top_followup.get('run_id'))}")
        lines.append(f"Delta IoU: {_display(top_followup.get('delta_iou'))}")
        lines.append(f"Primary source: {_display(top_followup.get('primary_source'))}")
        lines.append(f"Secondary source: {_display(top_followup.get('secondary_source'))}")
        lines.append(f"Difference summary: {_display(top_followup.get('difference_summary'))}")

    if isinstance(per_asset, list):
        lines.append("")
        lines.append("Per-asset leaders")
        if not per_asset:
            lines.append("None")
        else:
            for row in per_asset:
                if not isinstance(row, dict):
                    continue
                lines.append(
                    f"- {_display(row.get('asset_id'))} | delta_iou={_display(row.get('delta_iou'))} | run_id={_display(row.get('run_id'))}"
                )
    return "\n".join(lines)


def _validate_report_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("report payload must be a mapping")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"report missing required fields: {', '.join(missing)}")
    if not isinstance(payload.get("top_followups_by_asset"), list):
        raise ValueError("report top_followups_by_asset must be a list")


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
    parser = argparse.ArgumentParser(description="Inspect detector calibration follow-up report")
    parser.add_argument("--report", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_followup_report(
            report=args.report,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
