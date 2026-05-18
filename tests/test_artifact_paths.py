from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.artifact_paths import resolve_path, resolve_timestamped_output_path, write_json


class ArtifactPathTests(unittest.TestCase):
    def test_resolve_timestamped_output_path_uses_override_when_provided(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            output_path = Path(tempdir) / "nested" / "result.json"
            resolved = resolve_timestamped_output_path(
                output_path=output_path,
                default_dir=Path(tempdir) / "ignored",
                filename_suffix="ignored.json",
                timestamp_slug="20260519T000000Z",
            )
            self.assertEqual(resolved, output_path.resolve())

    def test_resolve_timestamped_output_path_builds_default_name(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            resolved = resolve_timestamped_output_path(
                output_path=None,
                default_dir=Path(tempdir) / "outputs" / "marvel_rivals" / "followup",
                filename_suffix="detector_calibration_followup_manifest.json",
                timestamp_slug="20260519T000000Z",
            )
            expected = Path(tempdir) / "outputs" / "marvel_rivals" / "followup" / "20260519T000000Z.detector_calibration_followup_manifest.json"
            self.assertEqual(resolved, expected.resolve())

    def test_write_json_materializes_parent_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            output_path = Path(tempdir) / "nested" / "payload.json"
            written = write_json(output_path, {"ok": True})
            self.assertEqual(written, output_path.resolve())
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8")), {"ok": True})

    def test_resolve_path_normalizes_relative_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            cwd = Path.cwd()
            try:
                Path(tempdir).mkdir(parents=True, exist_ok=True)
                import os

                os.chdir(tempdir)
                resolved = resolve_path("payload.json")
                self.assertEqual(resolved, (Path(tempdir) / "payload.json").resolve())
            finally:
                os.chdir(cwd)
