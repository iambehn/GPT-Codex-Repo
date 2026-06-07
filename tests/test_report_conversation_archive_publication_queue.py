from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.conversation_archive import append_conversation_archive_batch, record_conversation_archive, supersede_conversation_archive_batch
from tools.materialize_conversation_archive_doc_source import materialize_conversation_archive_doc_source
from tools.prepare_conversation_archive_upload import prepare_conversation_archive_upload
from tools.report_conversation_archive_publication_queue import report_conversation_archive_publication_queue


def _body_with_words(count: int) -> str:
    return " ".join(f"word{i}" for i in range(count))


class ReportConversationArchivePublicationQueueTests(unittest.TestCase):
    def test_reports_ready_for_drive_import_when_all_local_artifacts_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive queue",
                body_markdown=_body_with_words(116900),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            upload = prepare_conversation_archive_upload(ledger=ledger_path, batch_id=batch["batch_id"])
            materialize_conversation_archive_doc_source(upload_manifest=upload["output_path"])

            result = report_conversation_archive_publication_queue(ledger=ledger_path)
            self.assertTrue(result["ok"])
            self.assertIn("ready_for_drive_import", result["rendered_output"])

    def test_reports_missing_upload_manifest_when_batch_is_pending_without_prep(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive queue",
                body_markdown=_body_with_words(116900),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)

            result = report_conversation_archive_publication_queue(ledger=ledger_path)
            self.assertTrue(result["ok"])
            self.assertIn("needs_upload_manifest", result["rendered_output"])

    def test_json_mode_returns_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = {
                "schema_version": "conversation_archive_ledger_v1",
                "generated_at": "2026-06-05T00:00:00Z",
                "words_per_page_estimate": 275,
                "soft_open_threshold": 103125,
                "soft_close_threshold": 116875,
                "hard_close_threshold": 123750,
                "rows": [],
            }
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = report_conversation_archive_publication_queue(ledger=ledger_path, emit_json=True)
            self.assertEqual(json.loads(result["rendered_output"])["row_count"], 0)

    def test_reports_superseded_rows_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive queue",
                body_markdown=_body_with_words(116900),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            supersede_conversation_archive_batch(
                batch_id=batch["batch_id"],
                superseded_by="operator_workflows_and_automation__batch__replacement",
                ledger_path=ledger_path,
            )

            result = report_conversation_archive_publication_queue(ledger=ledger_path)
            self.assertTrue(result["ok"])
            self.assertIn("superseded", result["rendered_output"])

    def test_rejects_invalid_ledger_row_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = {
                "schema_version": "conversation_archive_ledger_v1",
                "generated_at": "2026-06-05T00:00:00Z",
                "words_per_page_estimate": 275,
                "soft_open_threshold": 103125,
                "soft_close_threshold": 116875,
                "hard_close_threshold": 123750,
                "rows": [
                    {
                        "batch_id": "batch-1",
                        "topic": "operator_workflows_and_automation",
                        "status": "closed_pending_upload",
                        "conversation_ids": "not-a-list",
                    }
                ],
            }
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "archive ledger row conversation_ids must be a list"):
                report_conversation_archive_publication_queue(ledger=ledger_path)

    def test_rejects_empty_required_row_identifiers(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = {
                "schema_version": "conversation_archive_ledger_v1",
                "generated_at": "2026-06-05T00:00:00Z",
                "words_per_page_estimate": 275,
                "soft_open_threshold": 103125,
                "soft_close_threshold": 116875,
                "hard_close_threshold": 123750,
                "rows": [
                    {
                        "batch_id": "",
                        "topic": "operator_workflows_and_automation",
                        "status": "closed_pending_upload",
                        "conversation_ids": [],
                    }
                ],
            }
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "archive ledger row batch_id must be non-empty"):
                report_conversation_archive_publication_queue(ledger=ledger_path)

    def test_rejects_empty_conversation_id_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            ledger_path = Path(tempdir) / "ledger.json"
            payload = {
                "schema_version": "conversation_archive_ledger_v1",
                "generated_at": "2026-06-05T00:00:00Z",
                "words_per_page_estimate": 275,
                "soft_open_threshold": 103125,
                "soft_close_threshold": 116875,
                "hard_close_threshold": 123750,
                "rows": [
                    {
                        "batch_id": "batch-1",
                        "topic": "operator_workflows_and_automation",
                        "status": "closed_pending_upload",
                        "conversation_ids": [""],
                    }
                ],
            }
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "archive ledger row conversation_ids entries must be non-empty"):
                report_conversation_archive_publication_queue(ledger=ledger_path)


if __name__ == "__main__":
    unittest.main()
