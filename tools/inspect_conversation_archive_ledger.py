from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EXPECTED_SCHEMA_VERSION = "conversation_archive_ledger_v1"
REQUIRED_TOP_LEVEL_FIELDS = (
    "schema_version",
    "generated_at",
    "words_per_page_estimate",
    "soft_open_threshold",
    "soft_close_threshold",
    "hard_close_threshold",
    "rows",
)
REQUIRED_ROW_FIELDS = (
    "batch_id",
    "topic",
    "status",
    "conversation_ids",
    "record_paths",
    "word_count",
    "estimated_pages",
    "measured_pages",
    "drive_doc_id",
    "drive_url",
    "opened_at",
    "closed_at",
    "uploaded_at",
    "superseded_by",
    "local_batch_markdown_path",
    "date_range_start",
    "date_range_end",
    "batch_summary",
    "is_single_conversation_exception",
)


def inspect_conversation_archive_ledger(
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
    open_rows = [row for row in rows if isinstance(row, dict) and row.get("status") == "open"]
    pending_rows = [row for row in rows if isinstance(row, dict) and row.get("status") == "closed_pending_upload"]
    uploaded_rows = [row for row in rows if isinstance(row, dict) and row.get("status") == "uploaded"]
    superseded_rows = [row for row in rows if isinstance(row, dict) and row.get("status") == "superseded"]
    lines = [
        f"Ledger path: {ledger_path}",
        f"Row count: {len(rows)}",
        f"Generated at: {payload.get('generated_at')}",
        f"Thresholds: open={payload.get('soft_open_threshold')} close={payload.get('soft_close_threshold')} hard={payload.get('hard_close_threshold')}",
        "",
        "Status counts",
        f"- open: {len(open_rows)}",
        f"- closed_pending_upload: {len(pending_rows)}",
        f"- uploaded: {len(uploaded_rows)}",
        f"- superseded: {len(superseded_rows)}",
        "",
        "Latest batch",
    ]
    latest_row = rows[-1] if rows else None
    if isinstance(latest_row, dict):
        lines.append(f"Batch id: {_display(latest_row.get('batch_id'))}")
        lines.append(f"Topic: {_display(latest_row.get('topic'))}")
        lines.append(f"Status: {_display(latest_row.get('status'))}")
        lines.append(f"Conversation count: {len(latest_row.get('conversation_ids') or [])}")
        lines.append(f"Word count: {_display(latest_row.get('word_count'))}")
        lines.append(f"Estimated pages: {_display(latest_row.get('estimated_pages'))}")
        lines.append(f"Drive doc id: {_display(latest_row.get('drive_doc_id'))}")
        lines.append(f"Exception batch: {_display(latest_row.get('is_single_conversation_exception'))}")
    else:
        lines.append("None")

    lines.append("")
    lines.append("Batches")
    if not rows:
        lines.append("None")
        return "\n".join(lines)
    for row in rows:
        lines.append(
            f"- {_display(row.get('batch_id'))} | {_display(row.get('topic'))} | {_display(row.get('status'))} | conversations={len(row.get('conversation_ids') or [])} | words={_display(row.get('word_count'))} | drive_doc_id={_display(row.get('drive_doc_id'))}"
        )
    return "\n".join(lines)


def _validate_ledger_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("ledger payload must be a mapping")
    if str(payload.get("schema_version") or "").strip() != EXPECTED_SCHEMA_VERSION:
        raise ValueError("ledger schema_version is invalid")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"ledger missing required fields: {', '.join(missing)}")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError("ledger rows must be a list")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("ledger rows must contain mappings")
        missing_row = [field for field in REQUIRED_ROW_FIELDS if field not in row]
        if missing_row:
            raise ValueError(f"ledger row missing required fields: {', '.join(missing_row)}")
        if not isinstance(row.get("conversation_ids"), list):
            raise ValueError("ledger row conversation_ids must be a list")
        if not isinstance(row.get("record_paths"), list):
            raise ValueError("ledger row record_paths must be a list")


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
    parser = argparse.ArgumentParser(description="Inspect conversation archive ledger")
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_conversation_archive_ledger(
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
