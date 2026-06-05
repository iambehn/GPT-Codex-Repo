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
                body_markdown=_body_with_words(500),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
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


if __name__ == "__main__":
    unittest.main()
