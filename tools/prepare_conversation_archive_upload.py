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
    resolve_path as _artifact_resolve_path,
    utc_now_iso as _artifact_utc_now_iso,
    write_json as _artifact_write_json,
)

SCHEMA_VERSION = "conversation_archive_upload_manifest_v1"
DEFAULT_UPLOAD_ROOT = REPO_ROOT / "outputs" / "conversation_archives" / "uploads"


def prepare_conversation_archive_upload(
    *,
    ledger: str | Path,
    batch_id: str,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    ledger_path = _resolve_path(ledger)
    payload = _load_json(ledger_path)
    _validate_ledger_payload(payload)
    row = _find_batch_row(payload, batch_id=batch_id)
    if row is None:
        return {
            "ok": False,
            "status": "unknown_batch_id",
            "error": f"batch_id not found: {batch_id}",
            "ledger_path": str(ledger_path),
        }
    if str(row.get("status") or "").strip() != "closed_pending_upload":
        return {
            "ok": False,
            "status": "batch_not_ready_for_upload",
            "error": "batch must be closed_pending_upload before upload preparation",
            "ledger_path": str(ledger_path),
            "batch_id": batch_id,
        }
    batch_markdown_path = _resolve_path(str(row.get("local_batch_markdown_path") or "").strip())
    if not batch_markdown_path.exists():
        return {
            "ok": False,
            "status": "missing_batch_markdown",
            "error": f"batch markdown not found: {batch_markdown_path}",
            "ledger_path": str(ledger_path),
            "batch_id": batch_id,
        }

    topic = str(row.get("topic") or "").strip()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "prepared_at": _utc_now(),
        "source_ledger_path": str(ledger_path),
        "batch_id": batch_id,
        "topic": topic,
        "batch_status": str(row.get("status") or "").strip(),
        "conversation_ids": list(row.get("conversation_ids") or []),
        "record_paths": list(row.get("record_paths") or []),
        "word_count": row.get("word_count"),
        "estimated_pages": row.get("estimated_pages"),
        "measured_pages": row.get("measured_pages"),
        "date_range_start": row.get("date_range_start"),
        "date_range_end": row.get("date_range_end"),
        "batch_summary": row.get("batch_summary"),
        "is_single_conversation_exception": bool(row.get("is_single_conversation_exception")),
        "batch_markdown_path": str(batch_markdown_path),
        "suggested_drive_folder": _suggested_drive_folder(topic=topic),
        "suggested_doc_title": _suggested_doc_title(row),
    }
    target = _resolve_output_path(topic=topic, batch_id=batch_id, output_path=output_path)
    _write_json(target, manifest)
    return {
        "ok": True,
        "status": "ok",
        "ledger_path": str(ledger_path),
        "batch_id": batch_id,
        "output_path": str(target),
        "suggested_doc_title": manifest["suggested_doc_title"],
        "suggested_drive_folder": manifest["suggested_drive_folder"],
    }


def _find_batch_row(payload: dict[str, Any], *, batch_id: str) -> dict[str, Any] | None:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("batch_id") or "").strip() == batch_id:
            return row
    return None


def _suggested_drive_folder(*, topic: str) -> str:
    return f"Codex Conversation Archive/{topic}"


def _suggested_doc_title(row: dict[str, Any]) -> str:
    topic = str(row.get("topic") or "").strip() or "misc_project_admin"
    start = str(row.get("date_range_start") or "").strip()[:10] or "unknown-start"
    end = str(row.get("date_range_end") or "").strip()[:10] or "unknown-end"
    return f"{topic}__archive_batch__{start}_to_{end}"


def _validate_ledger_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != "conversation_archive_ledger_v1":
        raise ValueError("archive ledger schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("archive ledger rows must be a list")


def _resolve_output_path(*, topic: str, batch_id: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    return _resolve_path(DEFAULT_UPLOAD_ROOT / topic / f"{batch_id}.conversation_archive_upload_manifest.json")


def _resolve_path(path: str | Path) -> Path:
    return _artifact_resolve_path(path)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    _artifact_write_json(path, payload, trailing_newline=True)


def _utc_now() -> str:
    return _artifact_utc_now_iso()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare one closed conversation archive batch for Google Docs upload")
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = prepare_conversation_archive_upload(
        ledger=args.ledger,
        batch_id=args.batch_id,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
