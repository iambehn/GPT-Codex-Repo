from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.accepted_fixture_trial_batch import run_accepted_fixture_trial_batch


def _source_manifest_payload(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema_version": "fixture_source_manifest_v1",
        "adapted_manifest_id": "adapted-123",
        "game": "marvel_rivals",
        "fixtures": rows,
    }


class AcceptedFixtureTrialBatchTests(unittest.TestCase):
    def test_batch_runner_succeeds_when_all_trials_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "accepted-source-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    _source_manifest_payload(
                        [
                            {"fixture_id": "fixture-a", "game": "marvel_rivals", "source_path": str(root / "a.mp4")},
                            {"fixture_id": "fixture-b", "game": "marvel_rivals", "source_path": str(root / "b.mp4")},
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            def _trial_runner(*args, **kwargs):
                fixture_id = kwargs["pattern"]
                manifest = root / f"{fixture_id}.fixture_trial_run_manifest.json"
                manifest.write_text("{}", encoding="utf-8")
                return {
                    "ok": True,
                    "manifest_path": str(manifest),
                    "fixtures": [
                        {
                            "fixture_id": fixture_id,
                            "status": "ok",
                            "layers": {
                                "proxy": {"sidecar_path": f"/tmp/{fixture_id}.proxy.json"},
                                "runtime": {"sidecar_path": None},
                                "fused": {"sidecar_path": None},
                            },
                        }
                    ],
                }

            result = run_accepted_fixture_trial_batch(
                manifest_path,
                trial_runner=_trial_runner,
                output_root=root / "outputs",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["fixture_count"], 2)
            self.assertEqual(result["success_count"], 2)
            self.assertEqual(result["failed_count"], 0)
            self.assertEqual(
                [row["fixture_id"] for row in result["results"]],
                ["fixture-a", "fixture-b"],
            )
            self.assertTrue(Path(result["manifest_path"]).exists())

    def test_batch_runner_reports_partial_when_one_trial_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "accepted-source-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    _source_manifest_payload(
                        [
                            {"fixture_id": "fixture-a", "game": "marvel_rivals", "source_path": str(root / "a.mp4")},
                            {"fixture_id": "fixture-b", "game": "marvel_rivals", "source_path": str(root / "b.mp4")},
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            def _trial_runner(*args, **kwargs):
                fixture_id = kwargs["pattern"]
                if fixture_id == "fixture-b":
                    return {
                        "ok": False,
                        "manifest_path": str(root / "fixture-b.fixture_trial_run_manifest.json"),
                        "fixtures": [
                            {
                                "fixture_id": fixture_id,
                                "status": "failed",
                                "error": "proxy failed",
                                "layers": {
                                    "proxy": {"sidecar_path": None},
                                    "runtime": {"sidecar_path": None},
                                    "fused": {"sidecar_path": None},
                                },
                            }
                        ],
                    }
                return {
                    "ok": True,
                    "manifest_path": str(root / "fixture-a.fixture_trial_run_manifest.json"),
                    "fixtures": [
                        {
                            "fixture_id": fixture_id,
                            "status": "ok",
                            "layers": {
                                "proxy": {"sidecar_path": f"/tmp/{fixture_id}.proxy.json"},
                                "runtime": {"sidecar_path": None},
                                "fused": {"sidecar_path": None},
                            },
                        }
                    ],
                }

            result = run_accepted_fixture_trial_batch(manifest_path, trial_runner=_trial_runner)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["success_count"], 1)
            self.assertEqual(result["failed_count"], 1)
            failed = next(row for row in result["results"] if row["fixture_id"] == "fixture-b")
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["error"], "proxy failed")

    def test_batch_runner_reports_failed_when_all_trials_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "accepted-source-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    _source_manifest_payload(
                        [{"fixture_id": "fixture-a", "game": "marvel_rivals", "source_path": str(root / "a.mp4")}]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            def _trial_runner(*args, **kwargs):
                fixture_id = kwargs["pattern"]
                return {
                    "ok": False,
                    "manifest_path": str(root / f"{fixture_id}.fixture_trial_run_manifest.json"),
                    "fixtures": [
                        {
                            "fixture_id": fixture_id,
                            "status": "failed",
                            "error": "proxy failed",
                            "layers": {
                                "proxy": {"sidecar_path": None},
                                "runtime": {"sidecar_path": None},
                                "fused": {"sidecar_path": None},
                            },
                        }
                    ],
                }

            result = run_accepted_fixture_trial_batch(manifest_path, trial_runner=_trial_runner)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["success_count"], 0)
            self.assertEqual(result["failed_count"], 1)

    def test_batch_runner_rejects_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "accepted-source-manifest.json"
            manifest_path.write_text(json.dumps({"schema_version": "wrong"}), encoding="utf-8")
            result = run_accepted_fixture_trial_batch(manifest_path, trial_runner=lambda *args, **kwargs: {})
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_fixture_source_manifest")

    def test_batch_runner_matches_default_mp4_pattern_against_source_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "accepted-source-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    _source_manifest_payload(
                        [{"fixture_id": "fixture-a", "game": "marvel_rivals", "source_path": str(root / "a.mp4")}]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            def _trial_runner(*args, **kwargs):
                fixture_id = kwargs["pattern"]
                return {
                    "ok": True,
                    "manifest_path": str(root / f"{fixture_id}.fixture_trial_run_manifest.json"),
                    "fixtures": [
                        {
                            "fixture_id": fixture_id,
                            "status": "ok",
                            "layers": {
                                "proxy": {"sidecar_path": f"/tmp/{fixture_id}.proxy.json"},
                                "runtime": {"sidecar_path": None},
                                "fused": {"sidecar_path": None},
                            },
                        }
                    ],
                }

            result = run_accepted_fixture_trial_batch(
                manifest_path,
                trial_runner=_trial_runner,
                pattern="*.mp4",
            )

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["fixture_count"], 1)


if __name__ == "__main__":
    unittest.main()
