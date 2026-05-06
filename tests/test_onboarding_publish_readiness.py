from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file


class OnboardingPublishReadinessTests(unittest.TestCase):
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
        qa_rows: list[dict[str, str]] | None = None,
        source_fetch_log: list[dict[str, str]] | None = None,
        duplicate_accept: bool = False,
        write_asset_file: bool = True,
        patch_tag: str = "2026-05",
        source_url: str = "https://example.com/punisher.png",
        license_note: str = "internal_review_required",
    ) -> Path:
        draft_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "onboarding" / "20260503T120000Z"
        manifests_root = draft_root / "manifests"
        catalog_root = draft_root / "catalog"
        masters_root = draft_root / "masters"
        manifests_root.mkdir(parents=True, exist_ok=True)
        catalog_root.mkdir(parents=True, exist_ok=True)
        masters_root.mkdir(parents=True, exist_ok=True)

        candidate_path = masters_root / "punisher.png"
        if write_asset_file:
            candidate_path.write_bytes(b"fakepng")

        dump_yaml_file(
            draft_root / "game.yaml",
            {
                "game_id": "marvel_rivals",
                "display_name": "Marvel Rivals",
                "patch_tag": patch_tag,
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
                    "source_count": len(source_fetch_log or [{"status": "fetched"}]),
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
                    "source_count": len(source_fetch_log or [{"status": "fetched"}]),
                    "source_fetch_log": source_fetch_log or [{"status": "fetched", "source_role": "roster"}],
                    "candidates": [
                        {
                            "candidate_id": "candidate-1",
                            "master_path": str(candidate_path),
                            "source_url": source_url,
                            "license_note": license_note,
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
        self._write_csv(catalog_root / "qa_queue.csv", qa_rows or [])
        return draft_root

    def test_validate_onboarding_publish_reports_ready_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            draft_root = self._write_draft(Path(tempdir), accepted=True)
            result = validate_onboarding_publish(draft_root)
            self.assertTrue(result["ok"])
            self.assertTrue(result["can_publish"])
            self.assertEqual(result["readiness"], "ready_to_publish")

    def test_validate_onboarding_publish_reports_population_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            draft_root = self._write_draft(
                Path(tempdir),
                accepted=True,
                qa_rows=[{"item_type": "source_seed_disagreement", "reason": "source and starter seed disagree"}],
            )
            result = validate_onboarding_publish(draft_root)
            self.assertFalse(result["can_publish"])
            self.assertEqual(result["readiness"], "needs_population_review")

    def test_validate_onboarding_publish_blocks_unresolved_identity_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            draft_root = self._write_draft(
                Path(tempdir),
                accepted=True,
                qa_rows=[{"item_type": "conflicting_identity_match", "reason": "canonical identity could not be reconciled safely"}],
            )
            result = validate_onboarding_publish(draft_root)
            self.assertFalse(result["can_publish"])
            self.assertEqual(result["readiness"], "needs_population_review")

    def test_validate_onboarding_publish_reports_binding_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            draft_root = self._write_draft(Path(tempdir), accepted=False)
            result = validate_onboarding_publish(draft_root)
            self.assertFalse(result["can_publish"])
            self.assertEqual(result["readiness"], "needs_binding_review")
            self.assertTrue(any(row["type"] == "missing_accepted_binding" for row in result["findings"]))

    def test_validate_onboarding_publish_reports_structural_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            draft_root = self._write_draft(Path(tempdir), accepted=True, duplicate_accept=True)
            result = validate_onboarding_publish(draft_root)
            self.assertFalse(result["can_publish"])
            self.assertEqual(result["readiness"], "structurally_invalid")
            self.assertTrue(any(row["type"] == "conflicting_accepted_bindings" for row in result["findings"]))

    def test_validate_onboarding_publish_reports_missing_provenance_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            draft_root = self._write_draft(
                Path(tempdir),
                accepted=True,
                patch_tag="draft",
                license_note="unknown",
            )
            result = validate_onboarding_publish(draft_root)
            self.assertFalse(result["can_publish"])
            self.assertEqual(result["readiness"], "needs_binding_review")
            self.assertTrue(any(row["type"] == "missing_asset_provenance" for row in result["findings"]))
            self.assertTrue(any(row.get("field") == "patch_tag" for row in result["findings"]))
            self.assertTrue(any(row.get("field") == "source_license_note" for row in result["findings"]))

    def test_validate_onboarding_publish_reports_unresolved_required_derived_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            draft_root = self._write_draft(Path(tempdir), accepted=True)
            dump_yaml_file(
                draft_root / "manifests" / "derived_detection_manifest.yaml",
                {
                    "schema_version": "derived_game_detection_manifest_v1",
                    "rows": [
                        {
                            "detection_id": "marvel_rivals.final_judgment.ability_icon",
                            "target_id": "final_judgment",
                            "required": True,
                            "status": "unresolved",
                            "reason": "no accepted binding exists for this required detection row yet",
                        }
                    ],
                },
            )
            result = validate_onboarding_publish(draft_root)
            self.assertFalse(result["can_publish"])
            self.assertEqual(result["readiness"], "needs_binding_review")
            self.assertEqual(result["counts"]["completeness_findings"], 1)
            self.assertTrue(any(row["type"] == "unresolved_required_derived_row" for row in result["findings"]))


if __name__ == "__main__":
    unittest.main()
