from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.conversation_archive import append_conversation_archive_batch, record_conversation_archive
from tools.prepare_conversation_archive_upload import prepare_conversation_archive_upload


def _body_with_words(count: int) -> str:
    return " ".join(f"word{i}" for i in range(count))


class PrepareConversationArchiveUploadTests(unittest.TestCase):
    def test_prepare_closed_pending_upload_batch_emits_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive upload prep",
                body_markdown=_body_with_words(116900),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            result = prepare_conversation_archive_upload(
                ledger=ledger_path,
                batch_id=batch["batch_id"],
            )
            self.assertTrue(result["ok"])
            manifest_path = Path(result["output_path"])
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["batch_id"], batch["batch_id"])
            self.assertEqual(payload["batch_status"], "closed_pending_upload")
            self.assertIn("Codex Conversation Archive/operator_workflows_and_automation", payload["suggested_drive_folder"])

    def test_prepare_rejects_non_pending_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive upload prep",
                body_markdown=_body_with_words(500),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            result = prepare_conversation_archive_upload(
                ledger=ledger_path,
                batch_id=batch["batch_id"],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "batch_not_ready_for_upload")

    def test_prepare_rejects_missing_batch_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive upload prep",
                body_markdown=_body_with_words(116900),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            ledger["rows"][0]["local_batch_markdown_path"] = str(root / "missing.md")
            ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
            result = prepare_conversation_archive_upload(
                ledger=ledger_path,
                batch_id=batch["batch_id"],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "missing_batch_markdown")

    def test_prepare_rejects_invalid_batch_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive upload prep",
                body_markdown=_body_with_words(116900),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            ledger["rows"][0]["conversation_ids"] = "not-a-list"
            ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

            result = prepare_conversation_archive_upload(
                ledger=ledger_path,
                batch_id=batch["batch_id"],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_batch_shape")

    def test_prepare_rejects_empty_record_path_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            record_path = root / "record.json"
            record = record_conversation_archive(
                source_thread_id="thread-1",
                agent_name="codex",
                started_at="2026-06-05T10:00:00Z",
                ended_at="2026-06-05T10:30:00Z",
                summary="Conversation archive upload prep",
                body_markdown=_body_with_words(116900),
                primary_topic="operator_workflows_and_automation",
                output_path=record_path,
            )
            batch = append_conversation_archive_batch(archive_record=record["output_path"], ledger_path=ledger_path)
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            ledger["rows"][0]["record_paths"] = [""]
            ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")

            result = prepare_conversation_archive_upload(
                ledger=ledger_path,
                batch_id=batch["batch_id"],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_batch_shape")
            self.assertEqual(result["error"], "record_paths entries must be non-empty")


if __name__ == "__main__":
    unittest.main()
