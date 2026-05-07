from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.accepted_clip_intake_manifest import build_accepted_clip_intake_manifest


def _inventory_payload(*, rows: list[dict[str, object]], schema_version: str = "accepted_clip_inventory_v1") -> dict[str, object]:
    return {
        "ok": True,
        "status": "ok",
        "schema_version": schema_version,
        "inventory_id": "inventory-123",
        "created_at": "2026-05-07T01:00:00+00:00",
        "source_root": "/tmp/accepted/marvel_rivals",
        "game": "marvel_rivals",
        "row_count": len(rows),
        "canonical_clip_count": len(rows),
        "meta_linked_count": sum(1 for row in rows if row.get("has_meta")),
        "duplicate_group_count": 0,
        "rows": rows,
        "manifest_path": "/tmp/source-inventory.manifest.json",
        "csv_path": "/tmp/source-inventory.manifest.csv",
    }


class AcceptedClipIntakeManifestTests(unittest.TestCase):
    def test_build_intake_manifest_copies_canonical_rows_only(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_root = Path(tempdir)
            clip_path = temp_root / "ABSOLUTE CINEMA_3522796292.mp4"
            meta_path = temp_root / "ABSOLUTE CINEMA_3522796292.meta.json"
            clip_path.write_bytes(b"clip")
            meta_path.write_text("{}", encoding="utf-8")
            inventory_manifest = temp_root / "accepted-clip-inventory.manifest.json"
            inventory_manifest.write_text(
                json.dumps(
                    _inventory_payload(
                        rows=[
                            {
                                "clip_id": "ABSOLUTE CINEMA_3522796292",
                                "game": "marvel_rivals",
                                "canonical_clip_path": str(clip_path),
                                "canonical_filename": clip_path.name,
                                "meta_path": str(meta_path),
                                "quality_tag": "high",
                                "downloaded_at": "2026-04-27T05:55:23",
                                "duration_seconds": 53.311,
                                "has_audio": True,
                                "resolution_width": 1920,
                                "resolution_height": 1080,
                                "fps": 60.0,
                                "variant_paths": [str(clip_path), "/tmp/variant.mp4"],
                                "variant_filenames": [clip_path.name, "variant.mp4"],
                                "variant_count": 2,
                                "has_meta": True,
                                "naming_pattern": "mixed_variants",
                                "preferred_source_reason": "unprefixed_with_meta",
                                "ingestion_ready": True,
                            }
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = build_accepted_clip_intake_manifest(
                inventory_manifest,
                output_root=temp_root / "outputs",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["schema_version"], "accepted_clip_intake_manifest_v1")
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(result["ingestion_ready_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["clip_id"], "ABSOLUTE CINEMA_3522796292")
            self.assertEqual(row["canonical_clip_path"], str(clip_path.resolve()))
            self.assertNotIn("variant_paths", row)
            self.assertNotIn("variant_filenames", row)
            self.assertNotIn("variant_count", row)
            manifest_path = Path(result["manifest_path"])
            csv_path = Path(result["csv_path"])
            self.assertTrue(manifest_path.exists())
            self.assertTrue(csv_path.exists())

            written_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(written_payload["row_count"], 1)
            self.assertEqual(written_payload["rows"][0]["canonical_clip_path"], str(clip_path.resolve()))

            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            self.assertEqual(len(csv_rows), 1)
            self.assertNotIn("variant_paths", csv_rows[0])

    def test_build_intake_manifest_rejects_invalid_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_root = Path(tempdir)
            inventory_manifest = temp_root / "accepted-clip-inventory.manifest.json"
            inventory_manifest.write_text(
                json.dumps(_inventory_payload(rows=[], schema_version="wrong_schema"), indent=2),
                encoding="utf-8",
            )

            result = build_accepted_clip_intake_manifest(inventory_manifest)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_inventory_schema")

    def test_build_intake_manifest_reports_no_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            temp_root = Path(tempdir)
            inventory_manifest = temp_root / "accepted-clip-inventory.manifest.json"
            inventory_manifest.write_text(
                json.dumps(_inventory_payload(rows=[]), indent=2),
                encoding="utf-8",
            )

            result = build_accepted_clip_intake_manifest(inventory_manifest)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "no_rows")
            self.assertEqual(result["row_count"], 0)
            self.assertIsNone(result["manifest_path"])
            self.assertIsNone(result["csv_path"])

    def test_build_intake_manifest_rejects_missing_inventory_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            missing_manifest = Path(tempdir) / "missing.manifest.json"
            result = build_accepted_clip_intake_manifest(missing_manifest)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_inventory_manifest")


if __name__ == "__main__":
    unittest.main()
