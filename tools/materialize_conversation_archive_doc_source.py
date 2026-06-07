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

SCHEMA_VERSION = "conversation_archive_doc_source_manifest_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "conversation_archives" / "doc_sources"
UPLOAD_MANIFEST_SCHEMA_VERSION = "conversation_archive_upload_manifest_v1"


def materialize_conversation_archive_doc_source(
    *,
    upload_manifest: str | Path,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(upload_manifest)
    payload = _load_json(manifest_path)
    _validate_upload_manifest_payload(payload)
    batch_markdown_path = _resolve_path(str(payload.get("batch_markdown_path") or "").strip())
    if not batch_markdown_path.exists():
        return {
            "ok": False,
            "status": "missing_batch_markdown",
            "error": f"batch markdown not found: {batch_markdown_path}",
            "upload_manifest_path": str(manifest_path),
        }

    target_root = _resolve_output_root(output_root=output_root, topic=str(payload.get("topic") or "").strip())
    target_root.mkdir(parents=True, exist_ok=True)
    doc_source_path = target_root / f"{payload['batch_id']}.google_doc_source.txt"
    doc_manifest_path = target_root / f"{payload['batch_id']}.conversation_archive_doc_source_manifest.json"

    rendered_text = _render_doc_source(payload=payload, batch_markdown=batch_markdown_path.read_text(encoding="utf-8"))
    doc_source_path.write_text(rendered_text, encoding="utf-8")

    doc_manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_upload_manifest_path": str(manifest_path),
        "batch_id": payload["batch_id"],
        "topic": payload["topic"],
        "suggested_doc_title": payload["suggested_doc_title"],
        "suggested_drive_folder": payload["suggested_drive_folder"],
        "doc_source_path": str(doc_source_path),
        "conversation_count": len(payload.get("conversation_ids") or []),
        "word_count": payload.get("word_count"),
        "estimated_pages": payload.get("estimated_pages"),
    }
    _write_json(doc_manifest_path, doc_manifest)
    return {
        "ok": True,
        "status": "ok",
        "upload_manifest_path": str(manifest_path),
        "doc_source_path": str(doc_source_path),
        "doc_source_manifest_path": str(doc_manifest_path),
        "suggested_doc_title": payload["suggested_doc_title"],
        "suggested_drive_folder": payload["suggested_drive_folder"],
    }


def _render_doc_source(*, payload: dict[str, Any], batch_markdown: str) -> str:
    lines = [
        payload["suggested_doc_title"],
        "=" * len(payload["suggested_doc_title"]),
        "",
        f"Topic: {payload.get('topic') or 'n/a'}",
        f"Suggested Drive folder: {payload.get('suggested_drive_folder') or 'n/a'}",
        f"Batch id: {payload.get('batch_id') or 'n/a'}",
        f"Date range: {payload.get('date_range_start') or 'n/a'} -> {payload.get('date_range_end') or 'n/a'}",
        f"Conversation count: {len(payload.get('conversation_ids') or [])}",
        f"Word count: {payload.get('word_count') or 'n/a'}",
        f"Estimated pages: {payload.get('estimated_pages') or 'n/a'}",
        "",
        "Batch Summary",
        "-------------",
        "",
        str(payload.get("batch_summary") or "").strip() or "n/a",
        "",
        "Conversation Archive",
        "--------------------",
        "",
        batch_markdown.rstrip(),
        "",
    ]
    return "\n".join(lines)


def _validate_upload_manifest_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != UPLOAD_MANIFEST_SCHEMA_VERSION:
        raise ValueError("upload manifest schema_version is invalid")
    for field in (
        "batch_id",
        "topic",
        "conversation_ids",
        "record_paths",
        "word_count",
        "estimated_pages",
        "batch_markdown_path",
        "suggested_drive_folder",
        "suggested_doc_title",
    ):
        if field not in payload:
            raise ValueError(f"upload manifest missing required field: {field}")
    if not isinstance(payload.get("conversation_ids"), list):
        raise ValueError("upload manifest conversation_ids must be a list")
    if any(str(item).strip() == "" for item in list(payload.get("conversation_ids") or [])):
        raise ValueError("upload manifest conversation_ids entries must be non-empty")
    if not isinstance(payload.get("record_paths"), list):
        raise ValueError("upload manifest record_paths must be a list")
    if any(str(path).strip() == "" for path in list(payload.get("record_paths") or [])):
        raise ValueError("upload manifest record_paths entries must be non-empty")
    if str(payload.get("batch_id") or "").strip() == "":
        raise ValueError("upload manifest batch_id must be non-empty")
    if str(payload.get("topic") or "").strip() == "":
        raise ValueError("upload manifest topic must be non-empty")
    if str(payload.get("batch_markdown_path") or "").strip() == "":
        raise ValueError("upload manifest batch_markdown_path must be non-empty")
    if str(payload.get("suggested_drive_folder") or "").strip() == "":
        raise ValueError("upload manifest suggested_drive_folder must be non-empty")
    if str(payload.get("suggested_doc_title") or "").strip() == "":
        raise ValueError("upload manifest suggested_doc_title must be non-empty")


def _resolve_output_root(*, output_root: str | Path | None, topic: str) -> Path:
    if output_root is not None:
        return _resolve_path(output_root)
    return _resolve_path(DEFAULT_OUTPUT_ROOT / topic)


def _resolve_path(path: str | Path) -> Path:
    return _artifact_resolve_path(path)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    _artifact_write_json(path, payload, trailing_newline=True)


def _utc_now() -> str:
    return _artifact_utc_now_iso()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize one Google Docs-importable text source from a prepared conversation archive upload manifest")
    parser.add_argument("--upload-manifest", required=True)
    parser.add_argument("--output-root")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = materialize_conversation_archive_doc_source(
        upload_manifest=args.upload_manifest,
        output_root=args.output_root,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
