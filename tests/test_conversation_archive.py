from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.conversation_archive import (
    HARD_CLOSE_THRESHOLD,
    SOFT_CLOSE_THRESHOLD,
    append_conversation_archive_batch,
    mark_conversation_archive_uploaded,
    record_conversation_archive,
    supersede_conversation_archive_batch,
)


def _body_with_words(count: int) -> str:
    return " ".join(f"word{i}" for i in range(count))


class ConversationArchiveTests(unittest.TestCase):
    def test_record_conversation_archive_infers_primary_topic(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            output_path = Path(tempdir) / "record.json"
            result = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T11:00:00Z",
                summary="Runtime fusion and ROI matcher debugging",
                body_markdown="We inspected runtime sidecars, ROI matcher output, and fusion behavior.",
                repo_refs=["pipeline/runtime_analysis.py"],
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["primary_topic"], "pipeline_runtime_and_fusion")
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["primary_topic"], "pipeline_runtime_and_fusion")

    def test_append_rejects_invalid_existing_record_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record_path.write_text(
                json.dumps(
                    {
                        "schema_version": "conversation_archive_record_v1",
                        "archive_record_id": "",
                        "source_thread_id": "thread-1",
                        "agent_name": "codex",
                        "started_at": "2026-06-05T10:00:00Z",
                        "ended_at": "2026-06-05T10:30:00Z",
                        "primary_topic": "operator_workflows_and_automation",
                        "secondary_topics": [],
                        "summary": "Bad record",
                        "body_markdown": "hello world",
                        "word_count": 2,
                        "repo_refs": [],
                        "archivable_status": "ready",
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "archive record archive_record_id must be non-empty"):
                append_conversation_archive_batch(archive_record=record_path, ledger_path=ledger_path)

    def test_append_batches_multiple_records_into_one_topic_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            first_record_path = root / "record-1.json"
            second_record_path = root / "record-2.json"
            ledger_path = root / "ledger.json"

            first = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Operator automation heartbeat",
                body_markdown=_body_with_words(400),
                primary_topic="operator_workflows_and_automation",
                output_path=first_record_path,
            )
            second = record_conversation_archive(
                source_thread_id="thread-2",
                agent_name="codex",
                started_at="2026-06-05T11:00:00Z",
                ended_at="2026-06-05T11:30:00Z",
                summary="Conversation archive ledger work",
                body_markdown=_body_with_words(500),
                primary_topic="operator_workflows_and_automation",
                output_path=second_record_path,
            )

            first_batch = append_conversation_archive_batch(
                archive_record=first["output_path"],
                ledger_path=ledger_path,
            )
            second_batch = append_conversation_archive_batch(
                archive_record=second["output_path"],
                ledger_path=ledger_path,
            )
            self.assertTrue(first_batch["ok"])
            self.assertTrue(second_batch["ok"])
            self.assertEqual(first_batch["batch_id"], second_batch["batch_id"])

            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(len(ledger["rows"]), 1)
            self.assertEqual(len(ledger["rows"][0]["conversation_ids"]), 2)

    def test_append_rotates_before_exceeding_soft_close_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            first_record_path = root / "record-1.json"
            second_record_path = root / "record-2.json"

            first = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Large archive batch",
                body_markdown=_body_with_words(SOFT_CLOSE_THRESHOLD - 2000),
                primary_topic="operator_workflows_and_automation",
                output_path=first_record_path,
            )
            second = record_conversation_archive(
                source_thread_id="thread-2",
                agent_name="codex",
                started_at="2026-06-05T11:00:00Z",
                ended_at="2026-06-05T11:30:00Z",
                summary="Second archive batch record",
                body_markdown=_body_with_words(4000),
                primary_topic="operator_workflows_and_automation",
                output_path=second_record_path,
            )

            first_batch = append_conversation_archive_batch(archive_record=first["output_path"], ledger_path=ledger_path)
            second_batch = append_conversation_archive_batch(archive_record=second["output_path"], ledger_path=ledger_path)
            self.assertNotEqual(first_batch["batch_id"], second_batch["batch_id"])

            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(len(ledger["rows"]), 2)
            self.assertEqual(ledger["rows"][0]["status"], "closed_pending_upload")
            self.assertIn(ledger["rows"][1]["status"], {"open", "closed_pending_upload"})

    def test_single_conversation_exception_batch_only_when_record_exceeds_hard_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T12:00:00Z",
                summary="Oversized conversation",
                body_markdown=_body_with_words(HARD_CLOSE_THRESHOLD + 500),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            self.assertEqual(batch["status"], "batched_exception")

            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(len(ledger["rows"]), 1)
            self.assertTrue(ledger["rows"][0]["is_single_conversation_exception"])
            self.assertEqual(len(ledger["rows"][0]["conversation_ids"]), 1)

    def test_mark_uploaded_and_superseded_updates_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Archive upload flow",
                body_markdown=_body_with_words(SOFT_CLOSE_THRESHOLD + 50),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            self.assertEqual(batch["batch_status"], "closed_pending_upload")
            upload = mark_conversation_archive_uploaded(
                batch_id=batch["batch_id"],
                drive_doc_id="doc-123",
                drive_url="https://docs.google.com/document/d/doc-123",
                ledger_path=ledger_path,
                measured_pages=401.0,
            )
            self.assertTrue(upload["ok"])
            supersede = supersede_conversation_archive_batch(
                batch_id=batch["batch_id"],
                superseded_by="operator_workflows_and_automation__batch__replacement",
                ledger_path=ledger_path,
            )
            self.assertTrue(supersede["ok"])

            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            row = ledger["rows"][0]
            self.assertEqual(row["status"], "superseded")
            self.assertEqual(row["drive_doc_id"], "doc-123")
            self.assertEqual(row["superseded_by"], "operator_workflows_and_automation__batch__replacement")

    def test_mark_uploaded_rejects_open_batch_and_empty_drive_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Archive upload flow",
                body_markdown=_body_with_words(500),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            self.assertEqual(batch["batch_status"], "open")

            result = mark_conversation_archive_uploaded(
                batch_id=batch["batch_id"],
                drive_doc_id="",
                drive_url="",
                ledger_path=ledger_path,
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_batch_status")

    def test_mark_uploaded_rejects_empty_drive_fields_for_closed_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Archive upload flow",
                body_markdown=_body_with_words(SOFT_CLOSE_THRESHOLD + 50),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            self.assertEqual(batch["batch_status"], "closed_pending_upload")

            missing_id = mark_conversation_archive_uploaded(
                batch_id=batch["batch_id"],
                drive_doc_id="",
                drive_url="https://docs.google.com/document/d/doc-123",
                ledger_path=ledger_path,
            )
            self.assertFalse(missing_id["ok"])
            self.assertEqual(missing_id["status"], "invalid_drive_doc_id")

            missing_url = mark_conversation_archive_uploaded(
                batch_id=batch["batch_id"],
                drive_doc_id="doc-123",
                drive_url="",
                ledger_path=ledger_path,
            )
            self.assertFalse(missing_url["ok"])
            self.assertEqual(missing_url["status"], "invalid_drive_url")

    def test_supersede_rejects_empty_and_self_referential_replacement_id(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Archive supersede flow",
                body_markdown=_body_with_words(SOFT_CLOSE_THRESHOLD + 50),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)

            empty_result = supersede_conversation_archive_batch(
                batch_id=batch["batch_id"],
                superseded_by="",
                ledger_path=ledger_path,
            )
            self.assertFalse(empty_result["ok"])
            self.assertEqual(empty_result["status"], "invalid_superseded_by")

            self_result = supersede_conversation_archive_batch(
                batch_id=batch["batch_id"],
                superseded_by=batch["batch_id"],
                ledger_path=ledger_path,
            )
            self.assertFalse(self_result["ok"])
            self.assertEqual(self_result["status"], "invalid_superseded_by")

    def test_append_rejects_invalid_existing_ledger_row_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Archive append flow",
                body_markdown=_body_with_words(500),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            ledger_path.write_text(
                json.dumps(
                    {
                        "schema_version": "conversation_archive_ledger_v1",
                        "generated_at": "2026-06-05T12:00:00Z",
                        "words_per_page_estimate": 275,
                        "soft_open_threshold": 103125,
                        "soft_close_threshold": 116875,
                        "hard_close_threshold": 123750,
                        "rows": [
                            {
                                "batch_id": "",
                                "topic": "operator_workflows_and_automation",
                                "status": "open",
                                "conversation_ids": [],
                                "record_paths": [],
                                "word_count": 0,
                                "estimated_pages": 0.0,
                                "measured_pages": None,
                                "drive_doc_id": None,
                                "drive_url": None,
                                "opened_at": "2026-06-05T10:00:00Z",
                                "closed_at": None,
                                "uploaded_at": None,
                                "superseded_by": None,
                                "local_batch_markdown_path": "/tmp/batch.md",
                                "date_range_start": "2026-06-05T10:00:00Z",
                                "date_range_end": "2026-06-05T10:30:00Z",
                                "batch_summary": "bad row",
                                "is_single_conversation_exception": False,
                            }
                        ],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "archive ledger row batch_id must be non-empty"):
                append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)

    def test_append_rejects_empty_record_path_entries_in_existing_ledger_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Archive append flow",
                body_markdown=_body_with_words(500),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            ledger_path.write_text(
                json.dumps(
                    {
                        "schema_version": "conversation_archive_ledger_v1",
                        "generated_at": "2026-06-05T12:00:00Z",
                        "words_per_page_estimate": 275,
                        "soft_open_threshold": 103125,
                        "soft_close_threshold": 116875,
                        "hard_close_threshold": 123750,
                        "rows": [
                            {
                                "batch_id": "operator_workflows_and_automation__batch__20260605T100000Z__abc123",
                                "topic": "operator_workflows_and_automation",
                                "status": "open",
                                "conversation_ids": [],
                                "record_paths": [""],
                                "word_count": 0,
                                "estimated_pages": 0.0,
                                "measured_pages": None,
                                "drive_doc_id": None,
                                "drive_url": None,
                                "opened_at": "2026-06-05T10:00:00Z",
                                "closed_at": None,
                                "uploaded_at": None,
                                "superseded_by": None,
                                "local_batch_markdown_path": "/tmp/batch.md",
                                "date_range_start": "2026-06-05T10:00:00Z",
                                "date_range_end": "2026-06-05T10:30:00Z",
                                "batch_summary": "bad row",
                                "is_single_conversation_exception": False,
                            }
                        ],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "archive ledger row record_paths entries must be non-empty"):
                append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)

    def test_append_rejects_empty_conversation_id_entries_in_existing_ledger_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Archive append flow",
                body_markdown=_body_with_words(500),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            ledger_path.write_text(
                json.dumps(
                    {
                        "schema_version": "conversation_archive_ledger_v1",
                        "generated_at": "2026-06-05T12:00:00Z",
                        "words_per_page_estimate": 275,
                        "soft_open_threshold": 103125,
                        "soft_close_threshold": 116875,
                        "hard_close_threshold": 123750,
                        "rows": [
                            {
                                "batch_id": "operator_workflows_and_automation__batch__20260605T100000Z__abc123",
                                "topic": "operator_workflows_and_automation",
                                "status": "open",
                                "conversation_ids": [""],
                                "record_paths": [],
                                "word_count": 0,
                                "estimated_pages": 0.0,
                                "measured_pages": None,
                                "drive_doc_id": None,
                                "drive_url": None,
                                "opened_at": "2026-06-05T10:00:00Z",
                                "closed_at": None,
                                "uploaded_at": None,
                                "superseded_by": None,
                                "local_batch_markdown_path": "/tmp/batch.md",
                                "date_range_start": "2026-06-05T10:00:00Z",
                                "date_range_end": "2026-06-05T10:30:00Z",
                                "batch_summary": "bad row",
                                "is_single_conversation_exception": False,
                            }
                        ],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "archive ledger row conversation_ids entries must be non-empty"):
                append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)


if __name__ == "__main__":
    unittest.main()
