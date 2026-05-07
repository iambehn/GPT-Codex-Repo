from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.accepted_clip_source_manifest_adapter import adapt_accepted_clip_intake_to_source_manifest
from pipeline.fixture_source_manifest import FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION, load_fixture_source_manifest


def _intake_payload(*, rows: list[dict[str, object]], schema_version: str = "accepted_clip_intake_manifest_v1") -> dict[str, object]:
    return {
        "ok": True,
        "status": "ok",
        "schema_version": schema_version,
        "intake_manifest_id": "intake-123",
        "created_at": "2026-05-07T01:00:00+00:00",
        "source_inventory_manifest_path": "/tmp/accepted-inventory.manifest.json",
        "source_inventory_id": "inventory-123",
        "game": "marvel_rivals",
        "row_count": len(rows),
        "ingestion_ready_count": sum(1 for row in rows if row.get("ingestion_ready")),
        "rows": rows,
        "manifest_path": "/tmp/accepted-intake.manifest.json",
        "csv_path": "/tmp/accepted-intake.manifest.csv",
    }


class AcceptedClipSourceManifestAdapterTests(unittest.TestCase):
    def test_adapt_accepted_intake_to_fixture_source_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_root = Path(tempdir)
            clip_path = temp_root / "ABSOLUTE CINEMA_3522796292.mp4"
            clip_path.write_bytes(b"clip")
            intake_manifest = temp_root / "accepted-clip-intake.manifest.json"
            intake_manifest.write_text(
                json.dumps(
                    _intake_payload(
                        rows=[
                            {
                                "clip_id": "ABSOLUTE CINEMA_3522796292",
                                "game": "marvel_rivals",
                                "canonical_clip_path": str(clip_path),
                                "meta_path": None,
                                "quality_tag": "high",
                                "downloaded_at": "2026-04-27T05:55:23",
                                "duration_seconds": 53.311,
                                "has_audio": True,
                                "resolution_width": 1920,
                                "resolution_height": 1080,
                                "fps": 60.0,
                                "has_meta": False,
                                "ingestion_ready": True,
                            }
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = adapt_accepted_clip_intake_to_source_manifest(
                intake_manifest,
                output_root=temp_root / "outputs",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["schema_version"], FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION)
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(result["source_accepted_clip_intake_manifest_id"], "intake-123")
            fixture = result["fixtures"][0]
            self.assertEqual(fixture["game"], "marvel_rivals")
            self.assertEqual(fixture["source_path"], str(clip_path.resolve()))
            self.assertEqual(fixture["chat_log_path"], None)
            self.assertEqual(fixture["produce_layers"], {})
            self.assertEqual(fixture["accepted_clip_id"], "ABSOLUTE CINEMA_3522796292")
            self.assertIn("accepted_clip_id=ABSOLUTE CINEMA_3522796292", fixture["notes"])
            self.assertNotIn("variant_paths", fixture)
            manifest_path = Path(result["manifest_path"])
            self.assertTrue(manifest_path.exists())

            loaded = load_fixture_source_manifest(manifest_path)
            self.assertEqual(loaded["schema_version"], FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION)
            self.assertEqual(loaded["fixture_count"], 1)
            self.assertEqual(loaded["fixtures"][0]["source_path"], str(clip_path.resolve()))

    def test_adapt_rejects_invalid_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_root = Path(tempdir)
            intake_manifest = temp_root / "accepted-clip-intake.manifest.json"
            intake_manifest.write_text(
                json.dumps(_intake_payload(rows=[], schema_version="wrong_schema"), indent=2),
                encoding="utf-8",
            )

            result = adapt_accepted_clip_intake_to_source_manifest(intake_manifest)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_accepted_intake_schema")

    def test_adapt_reports_no_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_root = Path(tempdir)
            intake_manifest = temp_root / "accepted-clip-intake.manifest.json"
            intake_manifest.write_text(
                json.dumps(_intake_payload(rows=[]), indent=2),
                encoding="utf-8",
            )

            result = adapt_accepted_clip_intake_to_source_manifest(intake_manifest)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "no_rows")
            self.assertEqual(result["row_count"], 0)
            self.assertIsNone(result["manifest_path"])

    def test_adapt_rejects_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            missing_manifest = Path(tempdir) / "missing.manifest.json"
            result = adapt_accepted_clip_intake_to_source_manifest(missing_manifest)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_accepted_intake_manifest")


if __name__ == "__main__":
    unittest.main()
