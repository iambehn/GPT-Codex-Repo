from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.accepted_clip_inventory import build_accepted_clip_inventory


def _write_meta(path: Path, *, clip_id: str, clip_path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "clip_id": clip_id,
                "clip_path": str(clip_path),
                "quality_tag": "high",
                "duration_seconds": 12.34,
                "resolution_width": 1920,
                "resolution_height": 1080,
                "fps": 60.0,
                "has_audio": True,
                "downloaded_at": "2026-04-27T05:55:23",
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


class AcceptedClipInventoryTests(unittest.TestCase):
    def test_build_inventory_groups_prefixed_and_unprefixed_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir) / "accepted" / "marvel_rivals"
            root.mkdir(parents=True, exist_ok=True)
            canonical = root / "ABSOLUTE CINEMA_3522796292.mp4"
            prefixed = root / "marvel_rivals_20260418_ABSOLUTE CINEMA_3522796292.mp4"
            canonical.write_bytes(b"canonical")
            prefixed.write_bytes(b"prefixed")
            _write_meta(root / "ABSOLUTE CINEMA_3522796292.meta.json", clip_id="ABSOLUTE CINEMA_3522796292", clip_path=canonical)

            result = build_accepted_clip_inventory(
                source_root=root,
                game="marvel_rivals",
                output_root=Path(tempdir) / "outputs",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(result["meta_linked_count"], 1)
            self.assertEqual(result["duplicate_group_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["clip_id"], "ABSOLUTE CINEMA_3522796292")
            self.assertEqual(row["canonical_filename"], "ABSOLUTE CINEMA_3522796292.mp4")
            self.assertEqual(row["variant_count"], 2)
            self.assertEqual(row["naming_pattern"], "mixed_variants")
            self.assertTrue(row["ingestion_ready"])

    def test_build_inventory_keeps_rows_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir) / "accepted" / "marvel_rivals"
            root.mkdir(parents=True, exist_ok=True)
            clip = root / "marvel_rivals_20260418_Wolverine guy_1129625028.mp4"
            clip.write_bytes(b"clip")

            result = build_accepted_clip_inventory(
                source_root=root,
                game="marvel_rivals",
                output_root=Path(tempdir) / "outputs",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertFalse(row["has_meta"])
            self.assertEqual(row["meta_path"], None)
            self.assertTrue(row["ingestion_ready"])
            self.assertEqual(row["clip_id"], "Wolverine guy_1129625028")

    def test_build_inventory_marks_unresolved_clip_id_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir) / "accepted" / "marvel_rivals"
            root.mkdir(parents=True, exist_ok=True)
            clip = root / "oddname.mp4"
            clip.write_bytes(b"clip")

            result = build_accepted_clip_inventory(
                source_root=root,
                game="marvel_rivals",
                output_root=Path(tempdir) / "outputs",
            )

            self.assertTrue(result["ok"])
            row = result["rows"][0]
            self.assertIsNone(row["clip_id"])
            self.assertFalse(row["ingestion_ready"])

    def test_build_inventory_reports_no_clips_found(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir) / "accepted" / "marvel_rivals"
            root.mkdir(parents=True, exist_ok=True)

            result = build_accepted_clip_inventory(
                source_root=root,
                game="marvel_rivals",
                output_root=Path(tempdir) / "outputs",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "no_clips_found")
            self.assertEqual(result["row_count"], 0)

    def test_build_inventory_rejects_invalid_source_root(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            missing = Path(tempdir) / "missing"
            result = build_accepted_clip_inventory(
                source_root=missing,
                game="marvel_rivals",
                output_root=Path(tempdir) / "outputs",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_source_root")


if __name__ == "__main__":
    unittest.main()
