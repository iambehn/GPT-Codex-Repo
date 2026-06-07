from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.artifact_paths import (
    resolve_path as _artifact_resolve_path,
    utc_now_iso as _artifact_utc_now_iso,
    utc_timestamp_slug as _artifact_utc_timestamp_slug,
    write_json as _artifact_write_json,
)

ARCHIVE_RECORD_SCHEMA_VERSION = "conversation_archive_record_v1"
ARCHIVE_LEDGER_SCHEMA_VERSION = "conversation_archive_ledger_v1"
ALLOWED_ARCHIVE_STATUSES = {"ready", "pending_review", "archived", "superseded"}
ALLOWED_BATCH_STATUSES = {"open", "closed_pending_upload", "uploaded", "superseded"}
WORDS_PER_PAGE_ESTIMATE = 275
SOFT_OPEN_THRESHOLD = 103_125
SOFT_CLOSE_THRESHOLD = 116_875
HARD_CLOSE_THRESHOLD = 123_750
TOPIC_TAXONOMY = (
    "pipeline_runtime_and_fusion",
    "onboarding_and_game_packs",
    "review_calibration_and_replay",
    "research_agent_and_packets",
    "operator_workflows_and_automation",
    "publishing_lineage_and_metrics",
    "misc_project_admin",
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "conversation_archives"
DEFAULT_RECORDS_ROOT = DEFAULT_OUTPUT_ROOT / "records"
DEFAULT_BATCHES_ROOT = DEFAULT_OUTPUT_ROOT / "batches"
DEFAULT_LEDGER_PATH = DEFAULT_OUTPUT_ROOT / "conversation_archive_ledger.json"

TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "pipeline_runtime_and_fusion": (
        "runtime",
        "fusion",
        "roi",
        "matcher",
        "detector",
        "signal",
        "template",
        "hud",
    ),
    "onboarding_and_game_packs": (
        "onboarding",
        "game pack",
        "assets/games",
        "manifest",
        "hud.yaml",
        "weights.yaml",
        "entities.yaml",
    ),
    "review_calibration_and_replay": (
        "review",
        "calibration",
        "replay",
        "goldset",
        "trial",
        "tuning",
    ),
    "research_agent_and_packets": (
        "researcher",
        "packet",
        "handoff",
        "custom gpt",
        "brief",
        "appendix",
    ),
    "operator_workflows_and_automation": (
        "automation",
        "dashboard",
        "backlog",
        "codex",
        "operator pack",
        "conversation archive",
        "heartbeat",
        "drive",
    ),
    "publishing_lineage_and_metrics": (
        "post",
        "posted",
        "metrics",
        "analytics",
        "lineage",
        "export batch",
        "platform",
    ),
}


def record_conversation_archive(
    *,
    source_thread_id: str,
    agent_name: str,
    started_at: str,
    ended_at: str,
    summary: str,
    body_markdown: str,
    primary_topic: str | None = None,
    secondary_topics: list[str] | None = None,
    repo_refs: list[str] | None = None,
    archivable_status: str = "ready",
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_secondary_topics = _normalize_topics(secondary_topics or [])
    normalized_repo_refs = [str(item).strip() for item in repo_refs or [] if str(item).strip()]
    if archivable_status not in ALLOWED_ARCHIVE_STATUSES:
        return _failure("invalid_archive_status", f"archivable_status must be one of {sorted(ALLOWED_ARCHIVE_STATUSES)}")
    resolved_primary_topic = _resolve_primary_topic(
        primary_topic=primary_topic,
        summary=summary,
        body_markdown=body_markdown,
        repo_refs=normalized_repo_refs,
    )
    if not summary.strip():
        return _failure("invalid_summary", "summary must be non-empty")
    if not body_markdown.strip():
        return _failure("invalid_body_markdown", "body_markdown must be non-empty")
    archive_record_id = _archive_record_id(
        source_thread_id=source_thread_id,
        agent_name=agent_name,
        started_at=started_at,
        ended_at=ended_at,
        summary=summary,
    )
    payload = {
        "schema_version": ARCHIVE_RECORD_SCHEMA_VERSION,
        "archive_record_id": archive_record_id,
        "source_thread_id": str(source_thread_id).strip(),
        "agent_name": str(agent_name).strip(),
        "started_at": str(started_at).strip(),
        "ended_at": str(ended_at).strip(),
        "primary_topic": resolved_primary_topic,
        "secondary_topics": normalized_secondary_topics,
        "summary": str(summary).strip(),
        "body_markdown": body_markdown,
        "word_count": _word_count(body_markdown),
        "repo_refs": normalized_repo_refs,
        "archivable_status": archivable_status,
        "recorded_at": _utc_now(),
    }
    target = _resolve_archive_record_output_path(
        primary_topic=resolved_primary_topic,
        archive_record_id=archive_record_id,
        output_path=output_path,
    )
    _write_json(target, payload)
    return {
        "ok": True,
        "status": "ok",
        "archive_record_id": archive_record_id,
        "output_path": str(target),
        "primary_topic": resolved_primary_topic,
        "secondary_topics": normalized_secondary_topics,
        "word_count": payload["word_count"],
        "archivable_status": archivable_status,
    }


def append_conversation_archive_batch(
    *,
    archive_record: str | Path,
    ledger_path: str | Path | None = None,
) -> dict[str, Any]:
    record_path = _resolve_path(archive_record)
    payload = _load_json(record_path)
    _validate_archive_record_payload(payload)
    record_word_count = int(payload.get("word_count") or 0)
    topic = str(payload.get("primary_topic") or "").strip()
    ledger_target = _resolve_path(ledger_path) if ledger_path is not None else DEFAULT_LEDGER_PATH
    ledger = _load_or_create_ledger(ledger_target)
    existing_row = _find_row_for_record(ledger, archive_record_id=str(payload["archive_record_id"]))
    if existing_row is not None:
        return {
            "ok": True,
            "status": "already_batched",
            "archive_record_id": payload["archive_record_id"],
            "batch_id": existing_row["batch_id"],
            "ledger_path": str(ledger_target),
            "batch_path": existing_row["local_batch_markdown_path"],
        }

    if record_word_count > HARD_CLOSE_THRESHOLD:
        row = _create_exception_batch_row(payload=payload, record_path=record_path)
        ledger["rows"].append(row)
        _finalize_ledger(ledger_target, ledger)
        _render_batch_markdown(row)
        return {
            "ok": True,
            "status": "batched_exception",
            "archive_record_id": payload["archive_record_id"],
            "batch_id": row["batch_id"],
            "ledger_path": str(ledger_target),
            "batch_path": row["local_batch_markdown_path"],
        }

    open_row = _find_open_row(ledger, topic=topic)
    if open_row is None:
        open_row = _new_open_batch_row(topic=topic)
        ledger["rows"].append(open_row)
    elif _should_rotate_before_append(open_row=open_row, next_word_count=record_word_count):
        _close_batch_row(open_row)
        open_row = _new_open_batch_row(topic=topic)
        ledger["rows"].append(open_row)
    elif int(open_row.get("word_count") or 0) + record_word_count > HARD_CLOSE_THRESHOLD:
        _close_batch_row(open_row)
        open_row = _new_open_batch_row(topic=topic)
        ledger["rows"].append(open_row)

    _append_record_to_row(open_row, payload=payload, record_path=record_path)
    if int(open_row.get("word_count") or 0) >= SOFT_CLOSE_THRESHOLD:
        _close_batch_row(open_row)

    _finalize_ledger(ledger_target, ledger)
    _render_batch_markdown(open_row)
    return {
        "ok": True,
        "status": "ok",
        "archive_record_id": payload["archive_record_id"],
        "batch_id": open_row["batch_id"],
        "batch_status": open_row["status"],
        "ledger_path": str(ledger_target),
        "batch_path": open_row["local_batch_markdown_path"],
        "word_count": open_row["word_count"],
    }


def mark_conversation_archive_uploaded(
    *,
    batch_id: str,
    drive_doc_id: str,
    drive_url: str,
    ledger_path: str | Path | None = None,
    measured_pages: float | None = None,
) -> dict[str, Any]:
    ledger_target = _resolve_path(ledger_path) if ledger_path is not None else DEFAULT_LEDGER_PATH
    ledger = _load_or_create_ledger(ledger_target)
    row = _find_batch_row(ledger, batch_id=batch_id)
    if row is None:
        return _failure("unknown_batch_id", f"batch_id not found: {batch_id}")
    if str(row.get("status") or "").strip() != "closed_pending_upload":
        return _failure("invalid_batch_status", "batch must be closed_pending_upload before it can be marked uploaded")
    normalized_drive_doc_id = str(drive_doc_id).strip()
    normalized_drive_url = str(drive_url).strip()
    if not normalized_drive_doc_id:
        return _failure("invalid_drive_doc_id", "drive_doc_id must be non-empty")
    if not normalized_drive_url:
        return _failure("invalid_drive_url", "drive_url must be non-empty")
    if measured_pages is not None and float(measured_pages) <= 0:
        return _failure("invalid_measured_pages", "measured_pages must be greater than zero when provided")
    row["status"] = "uploaded"
    row["drive_doc_id"] = normalized_drive_doc_id
    row["drive_url"] = normalized_drive_url
    row["uploaded_at"] = _utc_now()
    row["measured_pages"] = measured_pages
    _finalize_ledger(ledger_target, ledger)
    return {
        "ok": True,
        "status": "ok",
        "batch_id": batch_id,
        "ledger_path": str(ledger_target),
    }


def supersede_conversation_archive_batch(
    *,
    batch_id: str,
    superseded_by: str,
    ledger_path: str | Path | None = None,
) -> dict[str, Any]:
    ledger_target = _resolve_path(ledger_path) if ledger_path is not None else DEFAULT_LEDGER_PATH
    ledger = _load_or_create_ledger(ledger_target)
    row = _find_batch_row(ledger, batch_id=batch_id)
    if row is None:
        return _failure("unknown_batch_id", f"batch_id not found: {batch_id}")
    normalized_superseded_by = str(superseded_by).strip()
    if not normalized_superseded_by:
        return _failure("invalid_superseded_by", "superseded_by must be non-empty")
    if normalized_superseded_by == batch_id:
        return _failure("invalid_superseded_by", "superseded_by must not equal batch_id")
    row["status"] = "superseded"
    row["superseded_by"] = normalized_superseded_by
    _finalize_ledger(ledger_target, ledger)
    return {
        "ok": True,
        "status": "ok",
        "batch_id": batch_id,
        "ledger_path": str(ledger_target),
        "superseded_by": row["superseded_by"],
    }


def _resolve_primary_topic(
    *,
    primary_topic: str | None,
    summary: str,
    body_markdown: str,
    repo_refs: list[str],
) -> str:
    if primary_topic:
        normalized = str(primary_topic).strip()
        if normalized not in TOPIC_TAXONOMY:
            raise ValueError(f"primary_topic must be one of {TOPIC_TAXONOMY}")
        return normalized
    haystack = " ".join([summary, body_markdown, *repo_refs]).lower()
    best_topic = "misc_project_admin"
    best_score = 0
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in haystack)
        if score > best_score:
            best_topic = topic
            best_score = score
    return best_topic


def _normalize_topics(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        topic = str(value).strip()
        if not topic:
            continue
        if topic not in TOPIC_TAXONOMY:
            raise ValueError(f"secondary topic must be one of {TOPIC_TAXONOMY}")
        if topic not in normalized:
            normalized.append(topic)
    return normalized


def _load_or_create_ledger(path: Path) -> dict[str, Any]:
    if path.exists():
        payload = _load_json(path)
        _validate_ledger_payload(payload)
        return payload
    return {
        "schema_version": ARCHIVE_LEDGER_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "words_per_page_estimate": WORDS_PER_PAGE_ESTIMATE,
        "soft_open_threshold": SOFT_OPEN_THRESHOLD,
        "soft_close_threshold": SOFT_CLOSE_THRESHOLD,
        "hard_close_threshold": HARD_CLOSE_THRESHOLD,
        "rows": [],
    }


def _finalize_ledger(path: Path, payload: dict[str, Any]) -> None:
    payload["generated_at"] = _utc_now()
    _write_json(path, payload)


def _find_row_for_record(ledger: dict[str, Any], *, archive_record_id: str) -> dict[str, Any] | None:
    rows = ledger.get("rows") if isinstance(ledger.get("rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if archive_record_id in row.get("conversation_ids", []):
            return row
    return None


def _find_open_row(ledger: dict[str, Any], *, topic: str) -> dict[str, Any] | None:
    rows = ledger.get("rows") if isinstance(ledger.get("rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("topic") == topic and row.get("status") == "open":
            return row
    return None


def _find_batch_row(ledger: dict[str, Any], *, batch_id: str) -> dict[str, Any] | None:
    rows = ledger.get("rows") if isinstance(ledger.get("rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("batch_id") == batch_id:
            return row
    return None


def _new_open_batch_row(*, topic: str) -> dict[str, Any]:
    batch_id = _batch_id(topic=topic)
    batch_path = _resolve_batch_markdown_path(topic=topic, batch_id=batch_id)
    return {
        "batch_id": batch_id,
        "topic": topic,
        "status": "open",
        "conversation_ids": [],
        "record_paths": [],
        "word_count": 0,
        "estimated_pages": 0.0,
        "measured_pages": None,
        "drive_doc_id": None,
        "drive_url": None,
        "opened_at": _utc_now(),
        "closed_at": None,
        "uploaded_at": None,
        "superseded_by": None,
        "local_batch_markdown_path": str(batch_path),
        "date_range_start": None,
        "date_range_end": None,
        "batch_summary": "",
        "is_single_conversation_exception": False,
    }


def _create_exception_batch_row(*, payload: dict[str, Any], record_path: Path) -> dict[str, Any]:
    row = _new_open_batch_row(topic=str(payload["primary_topic"]))
    row["is_single_conversation_exception"] = True
    _append_record_to_row(row, payload=payload, record_path=record_path)
    _close_batch_row(row)
    return row


def _append_record_to_row(row: dict[str, Any], *, payload: dict[str, Any], record_path: Path) -> None:
    conversation_ids = row.setdefault("conversation_ids", [])
    record_paths = row.setdefault("record_paths", [])
    conversation_ids.append(str(payload["archive_record_id"]))
    record_paths.append(str(record_path))
    row["word_count"] = int(row.get("word_count") or 0) + int(payload["word_count"])
    row["estimated_pages"] = round(float(row["word_count"]) / WORDS_PER_PAGE_ESTIMATE, 1)
    started_at = str(payload.get("started_at") or "").strip()
    ended_at = str(payload.get("ended_at") or "").strip()
    if not row.get("date_range_start") or started_at < str(row.get("date_range_start")):
        row["date_range_start"] = started_at
    if not row.get("date_range_end") or ended_at > str(row.get("date_range_end")):
        row["date_range_end"] = ended_at
    summaries = [str(item).strip() for item in row.get("conversation_ids", [])]
    row["batch_summary"] = (
        f"{len(conversation_ids)} conversation"
        f"{'' if len(conversation_ids) == 1 else 's'} covering {row['date_range_start']} to {row['date_range_end']}"
    )


def _close_batch_row(row: dict[str, Any]) -> None:
    row["status"] = "closed_pending_upload"
    row["closed_at"] = _utc_now()


def _should_rotate_before_append(*, open_row: dict[str, Any], next_word_count: int) -> bool:
    current_word_count = int(open_row.get("word_count") or 0)
    if current_word_count >= SOFT_OPEN_THRESHOLD and current_word_count + next_word_count > SOFT_CLOSE_THRESHOLD:
        return True
    return False


def _render_batch_markdown(row: dict[str, Any]) -> Path:
    batch_path = _resolve_path(row["local_batch_markdown_path"])
    record_paths = [Path(path) for path in row.get("record_paths", [])]
    record_payloads = [_load_json(path) for path in record_paths]
    lines = [
        f"# {row['batch_id']}",
        "",
        f"- topic: `{row['topic']}`",
        f"- status: `{row['status']}`",
        f"- opened_at: `{row['opened_at']}`",
        f"- closed_at: `{row.get('closed_at') or 'n/a'}`",
        f"- conversation_count: `{len(record_payloads)}`",
        f"- word_count: `{row['word_count']}`",
        f"- estimated_pages: `{row['estimated_pages']}`",
        f"- measured_pages: `{row.get('measured_pages') or 'n/a'}`",
        f"- date_range: `{row.get('date_range_start') or 'n/a'} -> {row.get('date_range_end') or 'n/a'}`",
        f"- drive_doc_id: `{row.get('drive_doc_id') or 'pending'}`",
        f"- drive_url: `{row.get('drive_url') or 'pending'}`",
        "",
        "## Batch Summary",
        "",
        row.get("batch_summary") or "n/a",
        "",
    ]
    for payload in record_payloads:
        lines.extend(
            [
                f"## Conversation {payload['archive_record_id']}",
                "",
                f"- source_thread_id: `{payload.get('source_thread_id') or 'n/a'}`",
                f"- agent_name: `{payload.get('agent_name') or 'n/a'}`",
                f"- primary_topic: `{payload.get('primary_topic') or 'n/a'}`",
                f"- secondary_topics: `{', '.join(payload.get('secondary_topics', [])) or 'n/a'}`",
                f"- started_at: `{payload.get('started_at') or 'n/a'}`",
                f"- ended_at: `{payload.get('ended_at') or 'n/a'}`",
                f"- repo_refs: `{', '.join(payload.get('repo_refs', [])) or 'n/a'}`",
                "",
                "### Summary",
                "",
                str(payload.get("summary") or "").strip(),
                "",
                "### Body",
                "",
                str(payload.get("body_markdown") or "").rstrip(),
                "",
            ]
        )
    batch_path.parent.mkdir(parents=True, exist_ok=True)
    batch_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return batch_path


def _validate_archive_record_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != ARCHIVE_RECORD_SCHEMA_VERSION:
        raise ValueError("archive record schema_version is invalid")
    required_fields = (
        "archive_record_id",
        "source_thread_id",
        "agent_name",
        "started_at",
        "ended_at",
        "primary_topic",
        "secondary_topics",
        "summary",
        "body_markdown",
        "word_count",
        "repo_refs",
        "archivable_status",
    )
    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise ValueError(f"archive record missing required fields: {', '.join(missing)}")
    if str(payload.get("primary_topic") or "").strip() not in TOPIC_TAXONOMY:
        raise ValueError("archive record primary_topic is invalid")
    if not isinstance(payload.get("secondary_topics"), list):
        raise ValueError("archive record secondary_topics must be a list")
    if not isinstance(payload.get("repo_refs"), list):
        raise ValueError("archive record repo_refs must be a list")
    if str(payload.get("archivable_status") or "").strip() not in ALLOWED_ARCHIVE_STATUSES:
        raise ValueError("archive record archivable_status is invalid")


def _validate_ledger_payload(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != ARCHIVE_LEDGER_SCHEMA_VERSION:
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
        if str(row.get("status") or "").strip() not in ALLOWED_BATCH_STATUSES:
            raise ValueError("archive ledger row status is invalid")
        if str(row.get("local_batch_markdown_path") or "").strip() == "":
            raise ValueError("archive ledger row local_batch_markdown_path must be non-empty")
        if not isinstance(row.get("conversation_ids"), list):
            raise ValueError("archive ledger row conversation_ids must be a list")
        if not isinstance(row.get("record_paths"), list):
            raise ValueError("archive ledger row record_paths must be a list")


def _archive_record_id(
    *,
    source_thread_id: str,
    agent_name: str,
    started_at: str,
    ended_at: str,
    summary: str,
) -> str:
    digest = hashlib.sha1(
        "::".join([source_thread_id.strip(), agent_name.strip(), started_at.strip(), ended_at.strip(), summary.strip()]).encode("utf-8")
    ).hexdigest()[:16]
    return f"conv-archive-{digest}"


def _batch_id(*, topic: str) -> str:
    return f"{topic}__batch__{_utc_timestamp_slug()}__{uuid4().hex[:6]}"


def _resolve_archive_record_output_path(
    *,
    primary_topic: str,
    archive_record_id: str,
    output_path: str | Path | None,
) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    return _resolve_path(DEFAULT_RECORDS_ROOT / primary_topic / f"{archive_record_id}.conversation_archive_record.json")


def _resolve_batch_markdown_path(*, topic: str, batch_id: str) -> Path:
    return _resolve_path(DEFAULT_BATCHES_ROOT / topic / f"{batch_id}.conversation_archive_batch.md")


def _word_count(text: str) -> int:
    return len([token for token in str(text).split() if token.strip()])


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    _artifact_write_json(path, payload, trailing_newline=True)


def _resolve_path(path: str | Path) -> Path:
    return _artifact_resolve_path(path)


def _utc_now() -> str:
    return _artifact_utc_now_iso()


def _utc_timestamp_slug() -> str:
    return _artifact_utc_timestamp_slug()


def _failure(status: str, error: str) -> dict[str, Any]:
    return {"ok": False, "status": status, "error": error}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Conversation archive tooling")
    subparsers = parser.add_subparsers(dest="command", required=True)

    record_parser = subparsers.add_parser("record")
    record_parser.add_argument("--source-thread-id", required=True)
    record_parser.add_argument("--agent-name", required=True)
    record_parser.add_argument("--started-at", required=True)
    record_parser.add_argument("--ended-at", required=True)
    record_parser.add_argument("--summary", required=True)
    record_parser.add_argument("--body-path", required=True)
    record_parser.add_argument("--primary-topic")
    record_parser.add_argument("--secondary-topic", action="append", default=[])
    record_parser.add_argument("--repo-ref", action="append", default=[])
    record_parser.add_argument("--archivable-status", default="ready")
    record_parser.add_argument("--output-path")

    batch_parser = subparsers.add_parser("append-batch")
    batch_parser.add_argument("--archive-record", required=True)
    batch_parser.add_argument("--ledger-path")

    uploaded_parser = subparsers.add_parser("mark-uploaded")
    uploaded_parser.add_argument("--batch-id", required=True)
    uploaded_parser.add_argument("--drive-doc-id", required=True)
    uploaded_parser.add_argument("--drive-url", required=True)
    uploaded_parser.add_argument("--ledger-path")
    uploaded_parser.add_argument("--measured-pages", type=float)

    supersede_parser = subparsers.add_parser("supersede-batch")
    supersede_parser.add_argument("--batch-id", required=True)
    supersede_parser.add_argument("--superseded-by", required=True)
    supersede_parser.add_argument("--ledger-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "record":
        body_markdown = _resolve_path(args.body_path).read_text(encoding="utf-8")
        result = record_conversation_archive(
            source_thread_id=args.source_thread_id,
            agent_name=args.agent_name,
            started_at=args.started_at,
            ended_at=args.ended_at,
            summary=args.summary,
            body_markdown=body_markdown,
            primary_topic=args.primary_topic,
            secondary_topics=args.secondary_topic,
            repo_refs=args.repo_ref,
            archivable_status=args.archivable_status,
            output_path=args.output_path,
        )
    elif args.command == "append-batch":
        result = append_conversation_archive_batch(
            archive_record=args.archive_record,
            ledger_path=args.ledger_path,
        )
    elif args.command == "mark-uploaded":
        result = mark_conversation_archive_uploaded(
            batch_id=args.batch_id,
            drive_doc_id=args.drive_doc_id,
            drive_url=args.drive_url,
            ledger_path=args.ledger_path,
            measured_pages=args.measured_pages,
        )
    else:
        result = supersede_conversation_archive_batch(
            batch_id=args.batch_id,
            superseded_by=args.superseded_by,
            ledger_path=args.ledger_path,
        )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
