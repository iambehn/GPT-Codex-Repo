from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_conversation_archive_ledger import (
    inspect_conversation_archive_ledger,
    main as inspect_main,
)


def _ledger_row(
    *,
    batch_id: str = "operator_workflows_and_automation__batch__20260605T100000Z__abc123",
    topic: str = "operator_workflows_and_automation",
    status: str = "open",
    conversation_ids: list[str] | None = None,
    word_count: int = 5000,
    estimated_pages: float = 18.2,
    drive_doc_id: str | None = None,
    is_single_conversation_exception: bool = False,
) -> dict:
    return {
        "batch_id": batch_id,
        "topic": topic,
        "status": status,
        "conversation_ids": conversation_ids or ["conv-archive-123"],
        "record_paths": ["/tmp/record-1.json"],
        "word_count": word_count,
        "estimated_pages": estimated_pages,
        "measured_pages": None,
        "drive_doc_id": drive_doc_id,
        "drive_url": None,
        "opened_at": "2026-06-05T10:00:00Z",
        "closed_at": None,
        "uploaded_at": None,
        "superseded_by": None,
        "local_batch_markdown_path": "/tmp/batch.md",
        "date_range_start": "2026-06-05T10:00:00Z",
        "date_range_end": "2026-06-05T11:00:00Z",
        "batch_summary": "1 conversation covering one hour",
        "is_single_conversation_exception": is_single_conversation_exception,
    }


def _ledger_payload(*, rows: list[dict] | None = None) -> dict:
    rows = rows or []
    return {
        "schema_version": "conversation_archive_ledger_v1",
        "generated_at": "2026-06-05T12:00:00Z",
        "words_per_page_estimate": 275,
        "soft_open_threshold": 103125,
        "soft_close_threshold": 116875,
        "hard_close_threshold": 123750,
        "rows": rows,
    }


class InspectConversationArchiveLedgerTests(unittest.TestCase):
    def test_empty_ledger_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            ledger_path.write_text(json.dumps(_ledger_payload(), indent=2), encoding="utf-8")
            result = inspect_conversation_archive_ledger(ledger=ledger_path)
            self.assertTrue(result["ok"])
            self.assertIn("Status counts", result["rendered_output"])
            self.assertIn("Latest batch", result["rendered_output"])
            self.assertIn("None", result["rendered_output"])

    def test_non_empty_ledger_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = _ledger_payload(
                rows=[
                    _ledger_row(status="uploaded", drive_doc_id="doc-111"),
                    _ledger_row(
                        batch_id="operator_workflows_and_automation__batch__20260605T120000Z__def456",
                        status="closed_pending_upload",
                        conversation_ids=["conv-archive-123", "conv-archive-456"],
                        word_count=120000,
                        estimated_pages=436.4,
                    ),
                ]
            )
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_conversation_archive_ledger(ledger=ledger_path)
            self.assertIn("Row count: 2", result["rendered_output"])
            self.assertIn("- open: 0", result["rendered_output"])
            self.assertIn("- closed_pending_upload: 1", result["rendered_output"])
            self.assertIn("Batch id: operator_workflows_and_automation__batch__20260605T120000Z__def456", result["rendered_output"])
            self.assertIn("Conversation count: 2", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = _ledger_payload(rows=[_ledger_row()])
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_conversation_archive_ledger(ledger=ledger_path, emit_json=True)
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_ledger_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            ledger_path.write_text(json.dumps({"schema_version": "conversation_archive_ledger_v1"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "ledger missing required fields"):
                inspect_conversation_archive_ledger(ledger=ledger_path)

    def test_invalid_ledger_schema_version_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = _ledger_payload(rows=[_ledger_row()])
            payload["schema_version"] = "wrong_schema"
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "ledger schema_version is invalid"):
                inspect_conversation_archive_ledger(ledger=ledger_path)

    def test_invalid_ledger_empty_required_row_string_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = _ledger_payload(rows=[_ledger_row(batch_id="")])
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "ledger row batch_id must be non-empty"):
                inspect_conversation_archive_ledger(ledger=ledger_path)

    def test_main_returns_error_code_for_invalid_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            ledger_path.write_text(json.dumps({"schema_version": "conversation_archive_ledger_v1"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--ledger", str(ledger_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
