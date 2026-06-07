from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EXPECTED_SCHEMA_VERSION = "conversation_archive_upload_manifest_v1"
REQUIRED_TOP_LEVEL_FIELDS = (
    "schema_version",
    "prepared_at",
    "source_ledger_path",
    "batch_id",
    "topic",
    "batch_status",
    "conversation_ids",
    "record_paths",
    "word_count",
    "estimated_pages",
    "measured_pages",
    "date_range_start",
    "date_range_end",
    "batch_summary",
    "is_single_conversation_exception",
    "batch_markdown_path",
    "suggested_drive_folder",
    "suggested_doc_title",
)


def inspect_conversation_archive_upload_manifest(
    *,
    manifest: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    manifest_path = _resolve_path(manifest)
    payload = _load_json(manifest_path)
    _validate_manifest_payload(payload)
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(manifest_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "manifest_path": str(manifest_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "manifest": payload,
    }


def _render_compact_text(manifest_path: Path, payload: dict[str, Any]) -> str:
    lines = [
        f"Manifest path: {manifest_path}",
        f"Batch id: {_display(payload.get('batch_id'))}",
        f"Topic: {_display(payload.get('topic'))}",
        f"Batch status: {_display(payload.get('batch_status'))}",
        f"Conversation count: {len(payload.get('conversation_ids') or [])}",
        f"Word count: {_display(payload.get('word_count'))}",
        f"Estimated pages: {_display(payload.get('estimated_pages'))}",
        f"Measured pages: {_display(payload.get('measured_pages'))}",
        f"Drive folder: {_display(payload.get('suggested_drive_folder'))}",
        f"Doc title: {_display(payload.get('suggested_doc_title'))}",
        f"Exception batch: {_display(payload.get('is_single_conversation_exception'))}",
        "",
        "Source paths",
        f"- ledger: {_display(payload.get('source_ledger_path'))}",
        f"- batch markdown: {_display(payload.get('batch_markdown_path'))}",
        "",
        "Records",
    ]
    record_paths = payload.get("record_paths") if isinstance(payload.get("record_paths"), list) else []
    if not record_paths:
        lines.append("None")
    else:
        for path in record_paths:
            lines.append(f"- {_display(path)}")
    return "\n".join(lines)


def _validate_manifest_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("upload manifest payload must be a mapping")
    if str(payload.get("schema_version") or "").strip() != EXPECTED_SCHEMA_VERSION:
        raise ValueError("upload manifest schema_version is invalid")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"upload manifest missing required fields: {', '.join(missing)}")
    if not isinstance(payload.get("conversation_ids"), list):
        raise ValueError("upload manifest conversation_ids must be a list")
    if not isinstance(payload.get("record_paths"), list):
        raise ValueError("upload manifest record_paths must be a list")
    if str(payload.get("source_ledger_path") or "").strip() == "":
        raise ValueError("upload manifest source_ledger_path must be non-empty")
    if str(payload.get("batch_id") or "").strip() == "":
        raise ValueError("upload manifest batch_id must be non-empty")
    if str(payload.get("topic") or "").strip() == "":
        raise ValueError("upload manifest topic must be non-empty")
    if str(payload.get("batch_status") or "").strip() == "":
        raise ValueError("upload manifest batch_status must be non-empty")
    if str(payload.get("batch_markdown_path") or "").strip() == "":
        raise ValueError("upload manifest batch_markdown_path must be non-empty")
    if str(payload.get("suggested_drive_folder") or "").strip() == "":
        raise ValueError("upload manifest suggested_drive_folder must be non-empty")
    if str(payload.get("suggested_doc_title") or "").strip() == "":
        raise ValueError("upload manifest suggested_doc_title must be non-empty")


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
    parser = argparse.ArgumentParser(description="Inspect one prepared conversation archive upload manifest")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_conversation_archive_upload_manifest(
            manifest=args.manifest,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
