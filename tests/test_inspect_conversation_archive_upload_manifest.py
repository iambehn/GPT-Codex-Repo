from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_conversation_archive_upload_manifest import (
    inspect_conversation_archive_upload_manifest,
    main as inspect_main,
)


def _manifest_payload() -> dict:
    return {
        "schema_version": "conversation_archive_upload_manifest_v1",
        "prepared_at": "2026-06-05T18:00:00Z",
        "source_ledger_path": "/tmp/ledger.json",
        "batch_id": "operator_workflows_and_automation__batch__20260605T120000Z__abc123",
        "topic": "operator_workflows_and_automation",
        "batch_status": "closed_pending_upload",
        "conversation_ids": ["conv-archive-123"],
        "record_paths": ["/tmp/record-1.json"],
        "word_count": 120000,
        "estimated_pages": 436.4,
        "measured_pages": None,
        "date_range_start": "2026-06-05T10:00:00Z",
        "date_range_end": "2026-06-05T11:00:00Z",
        "batch_summary": "1 conversation covering one hour",
        "is_single_conversation_exception": True,
        "batch_markdown_path": "/tmp/batch.md",
        "suggested_drive_folder": "Codex Conversation Archive/operator_workflows_and_automation",
        "suggested_doc_title": "operator_workflows_and_automation__archive_batch__2026-06-05_to_2026-06-05",
    }


class InspectConversationArchiveUploadManifestTests(unittest.TestCase):
    def test_compact_render_contains_drive_routing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload-manifest.json"
            manifest_path.write_text(json.dumps(_manifest_payload(), indent=2), encoding="utf-8")
            result = inspect_conversation_archive_upload_manifest(manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertIn("Drive folder:", result["rendered_output"])
            self.assertIn("Doc title:", result["rendered_output"])
            self.assertIn("Records", result["rendered_output"])

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload-manifest.json"
            payload = _manifest_payload()
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_conversation_archive_upload_manifest(manifest=manifest_path, emit_json=True)
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_manifest_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload-manifest.json"
            manifest_path.write_text(json.dumps({"schema_version": "conversation_archive_upload_manifest_v1"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "upload manifest missing required fields"):
                inspect_conversation_archive_upload_manifest(manifest=manifest_path)

    def test_invalid_manifest_schema_version_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload-manifest.json"
            payload = _manifest_payload()
            payload["schema_version"] = "wrong_schema"
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "upload manifest schema_version is invalid"):
                inspect_conversation_archive_upload_manifest(manifest=manifest_path)

    def test_invalid_manifest_empty_required_string_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload-manifest.json"
            payload = _manifest_payload()
            payload["batch_id"] = ""
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "upload manifest batch_id must be non-empty"):
                inspect_conversation_archive_upload_manifest(manifest=manifest_path)

    def test_invalid_manifest_empty_record_path_entry_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload-manifest.json"
            payload = _manifest_payload()
            payload["record_paths"] = [""]
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "upload manifest record_paths entries must be non-empty"):
                inspect_conversation_archive_upload_manifest(manifest=manifest_path)

    def test_main_returns_error_code_for_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "upload-manifest.json"
            manifest_path.write_text(json.dumps({"schema_version": "conversation_archive_upload_manifest_v1"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--manifest", str(manifest_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
