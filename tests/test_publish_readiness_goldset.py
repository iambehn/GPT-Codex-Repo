from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = REPO_ROOT / "tests" / "fixtures" / "publish_readiness_goldsets" / "publish_readiness_goldset.json"


class PublishReadinessGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "publish_readiness_goldset_v1")
        self.assertGreaterEqual(len(payload["cases"]), 5)

    def test_publish_readiness_scenarios_match_goldset_expectations(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                with tempfile.TemporaryDirectory() as tempdir:
                    draft_root = self._write_draft(
                        Path(tempdir),
                        accepted=bool(case.get("accepted", False)),
                        qa_rows=list(case.get("qa_rows", [])),
                        duplicate_accept=bool(case.get("duplicate_accept", False)),
                    )
                    derived_manifest = case.get("derived_detection_manifest")
                    if isinstance(derived_manifest, dict):
                        dump_yaml_file(draft_root / "manifests" / "derived_detection_manifest.yaml", derived_manifest)

                    result = validate_onboarding_publish(draft_root)
                    self.assertTrue(result["ok"])
                    self.assertEqual(result["can_publish"], case["expected_can_publish"])
                    self.assertEqual(result["readiness"], case["expected_readiness"])

                    expected_finding_type = str(case.get("expected_finding_type") or "").strip()
                    if expected_finding_type:
                        self.assertTrue(
                            any(row["type"] == expected_finding_type for row in result["findings"]),
                            f"missing finding type {expected_finding_type}",
                        )

    def _write_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        headers = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            if not rows:
                writer.writerow({"empty": ""})
                return
            for row in rows:
                writer.writerow(row)

    def _write_draft(
        self,
        root: Path,
        *,
        accepted: bool,
        qa_rows: list[dict[str, str]],
        duplicate_accept: bool,
    ) -> Path:
        draft_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "onboarding" / "20260503T120000Z"
        manifests_root = draft_root / "manifests"
        catalog_root = draft_root / "catalog"
        masters_root = draft_root / "masters"
        manifests_root.mkdir(parents=True, exist_ok=True)
        catalog_root.mkdir(parents=True, exist_ok=True)
        masters_root.mkdir(parents=True, exist_ok=True)

        candidate_path = masters_root / "punisher.png"
        candidate_path.write_bytes(b"fakepng")

        dump_yaml_file(
            draft_root / "game.yaml",
            {
                "game_id": "marvel_rivals",
                "display_name": "Marvel Rivals",
                "patch_tag": "2026-05",
            },
        )
        dump_yaml_file(
            draft_root / "entities.yaml",
            {"heroes": [], "abilities": [], "events": []},
        )
        dump_yaml_file(
            manifests_root / "detection_manifest.yaml",
            {
                "schema_version": "game_detection_manifest_v1",
                "game_id": "marvel_rivals",
                "row_count": 1,
                "required_row_count": 1,
                "ready_row_count": 1,
                "rows_needing_assets": 0,
                "rows": [
                    {
                        "detection_id": "marvel_rivals.punisher.hero_portrait",
                        "target_id": "punisher",
                        "requires_asset": True,
                    }
                ],
            },
        )
        (manifests_root / "onboarding_state.json").write_text(
            json.dumps(
                {
                    "schema_version": "game_onboarding_state_v1",
                    "game_id": "marvel_rivals",
                    "phase_status": "bindings_pending",
                    "schema_path": "manifests/game_detection_schema.yaml",
                    "source_count": 1,
                    "updated_at": "2026-05-03T12:00:00+00:00",
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (manifests_root / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": "marvel_rivals",
                    "phase_status": "bindings_pending",
                    "source_count": 1,
                    "source_fetch_log": [{"status": "fetched", "source_role": "roster"}],
                    "candidates": [
                        {
                            "candidate_id": "candidate-1",
                            "master_path": str(candidate_path),
                            "source_url": "https://example.com/punisher.png",
                            "license_note": "internal_review_required",
                        }
                    ],
                    "bindings": [],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        binding_rows: list[dict[str, str]] = []
        if accepted:
            binding_rows.append(
                {
                    "detection_id": "marvel_rivals.punisher.hero_portrait",
                    "candidate_id": "candidate-1",
                    "status": "accepted",
                }
            )
            if duplicate_accept:
                binding_rows.append(
                    {
                        "detection_id": "marvel_rivals.punisher.hero_portrait",
                        "candidate_id": "candidate-1",
                        "status": "accepted",
                    }
                )
        self._write_csv(catalog_root / "bindings.csv", binding_rows)
        self._write_csv(catalog_root / "qa_queue.csv", qa_rows)
        return draft_root


if __name__ == "__main__":
    unittest.main()
