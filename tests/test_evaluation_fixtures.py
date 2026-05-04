from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.evaluation_fixtures import (
    EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION,
    load_evaluation_fixture_manifest,
)


class EvaluationFixtureManifestTests(unittest.TestCase):
    def test_load_evaluation_fixture_manifest_validates_default_shape(self) -> None:
        payload = load_evaluation_fixture_manifest()
        self.assertEqual(payload["schema_version"], EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION)
        self.assertEqual(payload["fixture_count"], 5)
        self.assertEqual({row["latency_budget_class"] for row in payload["fixtures"]}, {"smoke", "integration", "golden", "slow"})

    def test_load_evaluation_fixture_manifest_rejects_duplicate_fixture_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "bad_manifest.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION,
                        "fixtures": [
                            {
                                "fixture_id": "dup",
                                "label": "A",
                                "task_intent": "x",
                                "expected_review_outcome": "approved",
                                "latency_budget_class": "smoke",
                                "artifact_refs": {},
                            },
                            {
                                "fixture_id": "dup",
                                "label": "B",
                                "task_intent": "y",
                                "expected_review_outcome": "rejected",
                                "latency_budget_class": "golden",
                                "artifact_refs": {},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_evaluation_fixture_manifest(path)


if __name__ == "__main__":
    unittest.main()
