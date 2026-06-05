from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.conversation_archive import append_conversation_archive_batch, record_conversation_archive
from tools.materialize_conversation_archive_doc_source import materialize_conversation_archive_doc_source
from tools.prepare_conversation_archive_upload import prepare_conversation_archive_upload


def _body_with_words(count: int) -> str:
    return " ".join(f"word{i}" for i in range(count))


class MaterializeConversationArchiveDocSourceTests(unittest.TestCase):
    def test_materialize_doc_source_from_prepared_upload_manifest(self) -> None:
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
            upload = prepare_conversation_archive_upload(ledger=ledger_path, batch_id=batch["batch_id"])
            result = materialize_conversation_archive_doc_source(upload_manifest=upload["output_path"])
            self.assertTrue(result["ok"])
            source_path = Path(result["doc_source_path"])
            manifest_path = Path(result["doc_source_manifest_path"])
            self.assertTrue(source_path.exists())
            self.assertTrue(manifest_path.exists())
            self.assertIn("Suggested Drive folder:", source_path.read_text(encoding="utf-8"))

    def test_materialize_rejects_invalid_upload_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload.json"
            manifest_path.write_text(json.dumps({"schema_version": "wrong"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "upload manifest schema_version is invalid"):
                materialize_conversation_archive_doc_source(upload_manifest=manifest_path)


if __name__ == "__main__":
    unittest.main()
