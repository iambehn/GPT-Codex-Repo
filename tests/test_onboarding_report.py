from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.onboarding_report import summarize_onboarding_batch
from pipeline.simple_yaml import dump_yaml_file


class OnboardingReportTests(unittest.TestCase):
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
        game: str,
        slug: str,
        phase_status: str,
        source_fetch_log: list[dict[str, str]],
        qa_rows: list[dict[str, str]],
        binding_rows: list[dict[str, str]],
        candidate_rows: list[dict[str, object]],
        published: bool = False,
        updated_at: str = "2026-05-03T12:00:00+00:00",
    ) -> Path:
        draft_root = root / "assets" / "games" / game / "drafts" / "onboarding" / slug
        manifests_root = draft_root / "manifests"
        catalog_root = draft_root / "catalog"
        manifests_root.mkdir(parents=True, exist_ok=True)
        catalog_root.mkdir(parents=True, exist_ok=True)

        dump_yaml_file(
            draft_root / "entities.yaml",
            {
                "heroes": [{"id": f"{game}_hero", "display_name": "Hero"}],
                "abilities": [{"id": f"{game}_ability", "display_name": "Ability"}],
                "events": [{"id": f"{game}_event", "display_name": "Event"}],
            },
        )
        dump_yaml_file(
            manifests_root / "detection_manifest.yaml",
            {
                "schema_version": "game_detection_manifest_v1",
                "game_id": game,
                "row_count": 2,
                "required_row_count": 2,
                "ready_row_count": 2,
                "rows_needing_assets": 0,
                "rows": [
                    {"detection_id": f"{game}.hero", "asset_family": "hero_portrait"},
                    {"detection_id": f"{game}.event", "asset_family": "medal_icon"},
                ],
            },
        )
        (manifests_root / "onboarding_state.json").write_text(
            json.dumps(
                {
                    "schema_version": "game_onboarding_state_v1",
                    "game_id": game,
                    "phase_status": phase_status,
                    "schema_path": "manifests/game_detection_schema.yaml",
                    "source_count": len(source_fetch_log),
                    "updated_at": updated_at,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (manifests_root / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": game,
                    "phase_status": phase_status,
                    "source_count": len(source_fetch_log),
                    "source_fetch_log": source_fetch_log,
                    "detection_manifest": {
                        "schema_version": "game_detection_manifest_v1",
                        "row_count": 2,
                        "required_row_count": 2,
                        "ready_row_count": 2,
                        "rows_needing_assets": 0,
                    },
                    "candidates": candidate_rows,
                    "bindings": [],
                    "population_findings": [],
                    "source_failures": [row for row in source_fetch_log if row.get("status") == "fetch_failed"],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_csv(catalog_root / "bindings.csv", binding_rows)
        self._write_csv(catalog_root / "qa_queue.csv", qa_rows)
        self._write_csv(catalog_root / "source_fetch_log.csv", source_fetch_log)

        if published:
            published_root = root / "assets" / "games" / game / "manifests"
            published_root.mkdir(parents=True, exist_ok=True)
            dump_yaml_file(published_root / "detection_manifest.yaml", {"rows": []})
        return draft_root

    def test_summarize_onboarding_batch_reports_recursive_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_draft(
                root,
                game="marvel_rivals",
                slug="20260503T120000Z",
                phase_status="ready_to_publish",
                source_fetch_log=[
                    {"status": "fetched", "source_role": "roster"},
                    {"status": "empty_source", "source_role": "medals"},
                    {"status": "fetch_failed", "source_role": "abilities"},
                ],
                qa_rows=[],
                binding_rows=[{"detection_id": "marvel_rivals.hero", "status": "accepted"}],
                candidate_rows=[
                    {"candidate_id": "hero", "asset_family": "hero_portrait", "candidate_quality": "strong"},
                    {"candidate_id": "event", "asset_family": "medal_icon", "candidate_quality": "reviewable"},
                ],
                published=True,
            )
            self._write_draft(
                root,
                game="call_of_duty",
                slug="20260503T130000Z",
                phase_status="bindings_pending",
                source_fetch_log=[{"status": "fetched", "source_role": "operators"}],
                qa_rows=[
                    {"item_type": "weak_source_extraction", "status": "needs_population_review"},
                    {"item_type": "canonical_identity_preference_applied", "status": "info"},
                ],
                binding_rows=[{"detection_id": "call_of_duty.hero", "status": "proposed"}],
                candidate_rows=[{"candidate_id": "op", "asset_family": "hero_portrait", "candidate_quality": "strong"}],
            )

            result = summarize_onboarding_batch(root / "assets" / "games")

            self.assertTrue(result["ok"])
            self.assertEqual(result["draft_count"], 2)
            self.assertEqual(result["summary"]["readiness_counts"]["published"], 1)
            self.assertEqual(result["summary"]["readiness_counts"]["needs_binding_review"], 1)
            marvel = next(row for row in result["drafts"] if row["game"] == "marvel_rivals")
            call_of_duty = next(row for row in result["drafts"] if row["game"] == "call_of_duty")
            self.assertEqual(marvel["source_status_counts"]["fetch_failed"], 1)
            self.assertEqual(marvel["source_status_counts"]["empty_source"], 1)
            self.assertEqual(marvel["candidate_counts"]["by_quality"]["strong"], 1)
            self.assertTrue(marvel["published_pack_present"])
            self.assertEqual(call_of_duty["qa_counts"]["by_type"]["canonical_identity_preference_applied"], 1)
            self.assertFalse(call_of_duty["identity_summary"]["identity_blocked"])
            self.assertEqual(call_of_duty["identity_summary"]["informational_total"], 1)
            self.assertEqual(
                call_of_duty["identity_summary"]["by_type"]["canonical_identity_preference_applied"],
                1,
            )
            self.assertEqual(result["summary"]["identity_summary"]["informational_total"], 1)
            self.assertEqual(
                result["summary"]["games"]["call_of_duty"]["identity_summary"]["by_type"]["canonical_identity_preference_applied"],
                1,
            )

    def test_summarize_onboarding_batch_filters_game_and_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_draft(
                root,
                game="marvel_rivals",
                slug="20260503T120000Z",
                phase_status="schema_adapted",
                source_fetch_log=[],
                qa_rows=[],
                binding_rows=[],
                candidate_rows=[],
            )
            self._write_draft(
                root,
                game="call_of_duty",
                slug="20260503T130000Z",
                phase_status="bindings_pending",
                source_fetch_log=[{"status": "fetched", "source_role": "operators"}],
                qa_rows=[{"item_type": "image_kind_mismatch", "status": "needs_binding_review"}],
                binding_rows=[],
                candidate_rows=[],
            )
            output_path = root / "reports" / "onboarding_batch.json"

            result = summarize_onboarding_batch(root, game="call_of_duty", output_path=output_path)

            self.assertTrue(result["ok"])
            self.assertEqual(result["draft_count"], 1)
            self.assertEqual(result["drafts"][0]["game"], "call_of_duty")
            self.assertEqual(result["output_path"], str(output_path.resolve()))
            self.assertTrue(output_path.is_file())

    def test_summarize_onboarding_batch_adds_latest_vs_previous_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_draft(
                root,
                game="marvel_rivals",
                slug="20260503T120000Z",
                phase_status="bindings_pending",
                source_fetch_log=[
                    {"status": "fetched", "source_role": "roster"},
                    {"status": "fetch_failed", "source_role": "abilities"},
                ],
                qa_rows=[
                    {"item_type": "weak_source_extraction", "status": "needs_population_review"},
                    {"item_type": "conflicting_identity_match", "status": "needs_population_review", "reason": "identity conflict"},
                    {"item_type": "filename_only_anchor", "status": "needs_candidate_review"},
                ],
                binding_rows=[],
                candidate_rows=[
                    {"candidate_id": "hero", "asset_family": "hero_portrait", "candidate_quality": "medium"},
                ],
                updated_at="2026-05-03T12:00:00+00:00",
            )
            self._write_draft(
                root,
                game="marvel_rivals",
                slug="20260503T130000Z",
                phase_status="ready_to_publish",
                source_fetch_log=[
                    {"status": "fetched", "source_role": "roster"},
                ],
                qa_rows=[],
                binding_rows=[{"detection_id": "marvel_rivals.hero", "status": "accepted"}],
                candidate_rows=[
                    {"candidate_id": "hero", "asset_family": "hero_portrait", "candidate_quality": "high"},
                    {"candidate_id": "event", "asset_family": "medal_icon", "candidate_quality": "medium"},
                ],
                updated_at="2026-05-03T13:00:00+00:00",
            )

            result = summarize_onboarding_batch(root / "assets" / "games", game="marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["draft_count"], 2)
            latest = next(row for row in result["drafts"] if row["draft_root"].endswith("20260503T130000Z"))
            comparison = latest["comparison_to_previous"]
            self.assertIsNotNone(comparison)
            self.assertEqual(comparison["previous_publish_readiness"], "needs_population_review")
            self.assertEqual(comparison["current_publish_readiness"], "structurally_invalid")
            self.assertTrue(comparison["readiness_changed"])
            self.assertEqual(comparison["delta"]["source_failures"], -1)
            self.assertEqual(comparison["delta"]["filename_only_anchor"], -1)
            self.assertEqual(comparison["delta"]["conflicting_identity_match"], -1)
            self.assertEqual(comparison["delta"]["identity_blocking_total"], -1)
            self.assertEqual(comparison["delta"]["candidate_assets"], 1)
            game_summary = result["summary"]["games"]["marvel_rivals"]
            self.assertEqual(game_summary["latest_draft_root"], latest["draft_root"])
            self.assertIsNotNone(game_summary["comparison"])

    def test_summarize_onboarding_batch_identity_blockers_are_reported_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_draft(
                root,
                game="call_of_duty",
                slug="20260503T130000Z",
                phase_status="bindings_pending",
                source_fetch_log=[{"status": "fetched", "source_role": "operators"}],
                qa_rows=[
                    {"item_type": "conflicting_identity_match", "status": "needs_population_review", "reason": "operator identity conflict"},
                    {"item_type": "identity_match_rejected", "status": "needs_population_review", "reason": "row identity rejected"},
                ],
                binding_rows=[{"detection_id": "call_of_duty.hero", "status": "accepted"}],
                candidate_rows=[{"candidate_id": "op", "asset_family": "hero_portrait", "candidate_quality": "strong"}],
            )

            result = summarize_onboarding_batch(root / "assets" / "games")

            draft = result["drafts"][0]
            self.assertTrue(draft["identity_summary"]["identity_blocked"])
            self.assertEqual(draft["identity_summary"]["blocking_total"], 2)
            self.assertEqual(draft["identity_summary"]["by_type"]["conflicting_identity_match"], 1)
            self.assertEqual(draft["identity_summary"]["by_type"]["identity_match_rejected"], 1)
            self.assertEqual(result["summary"]["identity_summary"]["blocking_total"], 2)
            self.assertEqual(result["summary"]["identity_summary"]["by_type"]["conflicting_identity_match"], 1)


if __name__ == "__main__":
    unittest.main()
