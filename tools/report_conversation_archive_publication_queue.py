from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.artifact_paths import resolve_path as _artifact_resolve_path


def report_conversation_archive_publication_queue(
    *,
    ledger: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    ledger_path = _resolve_path(ledger)
    payload = _load_json(ledger_path)
    _validate_ledger_payload(payload)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    queue_rows = [_queue_row_from_ledger_row(row) for row in rows if isinstance(row, dict)]
    queue_rows.sort(key=lambda row: (row["publication_status"], row["topic"], row["batch_id"]))
    status_counts = _status_counts(queue_rows)
    inspection_payload = {
        "ledger_path": str(ledger_path),
        "row_count": len(queue_rows),
        "status_counts": status_counts,
        "rows": queue_rows,
    }
    rendered_output = json.dumps(inspection_payload, indent=2) if emit_json else _render_compact_text(inspection_payload)
    return {
        "ok": True,
        "status": "ok",
        "ledger_path": str(ledger_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "inspection_payload": inspection_payload,
    }


def _queue_row_from_ledger_row(row: dict[str, Any]) -> dict[str, Any]:
    batch_id = str(row.get("batch_id") or "").strip()
    topic = str(row.get("topic") or "").strip()
    batch_status = str(row.get("status") or "").strip()
    upload_manifest_path = _upload_manifest_path(topic=topic, batch_id=batch_id)
    upload_manifest_exists = upload_manifest_path.exists()
    doc_source_manifest_path = _doc_source_manifest_path(topic=topic, batch_id=batch_id)
    doc_source_manifest_exists = doc_source_manifest_path.exists()
    doc_source_path = _doc_source_path(topic=topic, batch_id=batch_id)
    doc_source_exists = doc_source_path.exists()
    publication_status = _publication_status(
        batch_status=batch_status,
        upload_manifest_exists=upload_manifest_exists,
        doc_source_manifest_exists=doc_source_manifest_exists,
        doc_source_exists=doc_source_exists,
        drive_doc_id=str(row.get("drive_doc_id") or "").strip() or None,
    )
    return {
        "batch_id": batch_id,
        "topic": topic,
        "batch_status": batch_status,
        "publication_status": publication_status,
        "conversation_count": len(row.get("conversation_ids") or []),
        "word_count": row.get("word_count"),
        "estimated_pages": row.get("estimated_pages"),
        "drive_doc_id": str(row.get("drive_doc_id") or "").strip() or None,
        "upload_manifest_path": str(upload_manifest_path),
        "upload_manifest_exists": upload_manifest_exists,
        "doc_source_manifest_path": str(doc_source_manifest_path),
        "doc_source_manifest_exists": doc_source_manifest_exists,
        "doc_source_path": str(doc_source_path),
        "doc_source_exists": doc_source_exists,
    }


def _publication_status(
    *,
    batch_status: str,
    upload_manifest_exists: bool,
    doc_source_manifest_exists: bool,
    doc_source_exists: bool,
    drive_doc_id: str | None,
) -> str:
    if batch_status == "uploaded" and drive_doc_id:
        return "published"
    if batch_status == "superseded":
        return "superseded"
    if batch_status != "closed_pending_upload":
        return "not_ready"
    if not upload_manifest_exists:
        return "needs_upload_manifest"
    if not doc_source_manifest_exists or not doc_source_exists:
        return "needs_doc_source"
    return "ready_for_drive_import"


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("publication_status") or "").strip() or "unknown"
        counts[status] = counts.get(status, 0) + 1
    return counts


def _render_compact_text(payload: dict[str, Any]) -> str:
    status_counts = payload.get("status_counts", {}) if isinstance(payload.get("status_counts"), dict) else {}
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    lines = [
        f"Ledger path: {payload.get('ledger_path')}",
        f"Row count: {payload.get('row_count')}",
        "",
        "Publication status counts",
    ]
    if status_counts:
        for key in sorted(status_counts):
            lines.append(f"- {key}: {status_counts[key]}")
    else:
        lines.append("None")
    lines.append("")
    lines.append("Rows")
    if not rows:
        lines.append("None")
        return "\n".join(lines)
    for row in rows:
        lines.append(
            f"- {row['batch_id']} | topic={row['topic']} | batch_status={row['batch_status']} | publication_status={row['publication_status']} | upload_manifest={row['upload_manifest_exists']} | doc_source={row['doc_source_exists']} | drive_doc_id={row['drive_doc_id'] or 'None'}"
        )
    return "\n".join(lines)


def _upload_manifest_path(*, topic: str, batch_id: str) -> Path:
    return _resolve_path(REPO_ROOT / "outputs" / "conversation_archives" / "uploads" / topic / f"{batch_id}.conversation_archive_upload_manifest.json")


def _doc_source_manifest_path(*, topic: str, batch_id: str) -> Path:
    return _resolve_path(REPO_ROOT / "outputs" / "conversation_archives" / "doc_sources" / topic / f"{batch_id}.conversation_archive_doc_source_manifest.json")


def _doc_source_path(*, topic: str, batch_id: str) -> Path:
    return _resolve_path(REPO_ROOT / "outputs" / "conversation_archives" / "doc_sources" / topic / f"{batch_id}.google_doc_source.txt")


def _validate_ledger_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != "conversation_archive_ledger_v1":
        raise ValueError("archive ledger schema_version is invalid")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError("archive ledger rows must be a list")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("archive ledger rows must contain mappings")
        if str(row.get("batch_id") or "").strip() == "":
            raise ValueError("archive ledger row batch_id must be non-empty")
        if str(row.get("topic") or "").strip() == "":
            raise ValueError("archive ledger row topic must be non-empty")
        if str(row.get("status") or "").strip() == "":
            raise ValueError("archive ledger row status must be non-empty")
        if not isinstance(row.get("conversation_ids"), list):
            raise ValueError("archive ledger row conversation_ids must be a list")


def _resolve_path(path: str | Path) -> Path:
    return _artifact_resolve_path(path)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Report conversation archive publication queue readiness")
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = report_conversation_archive_publication_queue(
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
