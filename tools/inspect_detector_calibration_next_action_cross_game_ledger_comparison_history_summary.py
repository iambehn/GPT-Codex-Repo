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
    "source_ledger_path",
    "source_row_count",
    "summary",
)
REQUIRED_SUMMARY_FIELDS = (
    "total_runs",
    "ok_run_count",
    "partial_input_failure_run_count",
    "ledger_record_failed_run_count",
    "top_game_counts",
    "latest_run_id",
    "latest_status",
    "latest_top_game",
    "latest_valid_game_count",
)


def inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
    *,
    summary: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    summary_path = _resolve_path(summary)
    payload = _load_json(summary_path)
    _validate_summary_payload(payload)
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(summary_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "summary_path": str(summary_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "summary_payload": payload,
    }


def _render_compact_text(summary_path: Path, payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    top_game_counts = summary.get("top_game_counts") if isinstance(summary.get("top_game_counts"), dict) else {}
    lines = [
        f"Summary path: {summary_path}",
        f"Source ledger path: {payload.get('source_ledger_path')}",
        f"Source row count: {payload.get('source_row_count')}",
        "",
        "Summary",
        f"Total runs: {_display(summary.get('total_runs'))}",
        f"Ok run count: {_display(summary.get('ok_run_count'))}",
        f"Partial input failure run count: {_display(summary.get('partial_input_failure_run_count'))}",
        f"Ledger record failed run count: {_display(summary.get('ledger_record_failed_run_count'))}",
        f"Latest run id: {_display(summary.get('latest_run_id'))}",
        f"Latest status: {_display(summary.get('latest_status'))}",
        f"Latest top game: {_display(summary.get('latest_top_game'))}",
        f"Latest valid game count: {_display(summary.get('latest_valid_game_count'))}",
        "",
        "Top game counts",
    ]
    if not top_game_counts:
        lines.append("None")
        return "\n".join(lines)
    for game, count in top_game_counts.items():
        lines.append(f"- {game}: {_display(count)}")
    return "\n".join(lines)


def _validate_summary_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("summary payload must be a mapping")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"summary missing required fields: {', '.join(missing)}")
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise ValueError("summary field must be a mapping")
    missing_summary_fields = [field for field in REQUIRED_SUMMARY_FIELDS if field not in summary]
    if missing_summary_fields:
        raise ValueError(f"summary missing required nested fields: {', '.join(missing_summary_fields)}")


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
    parser = argparse.ArgumentParser(
        description="Inspect detector calibration next-action cross-game ledger comparison history summary"
    )
    parser.add_argument("--summary", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary(
            summary=args.summary,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
