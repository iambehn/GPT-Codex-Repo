from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline.onboarding_identity_review_bridge as onboarding_identity_review_bridge
from pipeline.onboarding_identity_review_bridge import (
    apply_onboarding_identity_review,
    prepare_onboarding_identity_review,
)
from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from tests.test_run import _write_gpt_review_repo


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = REPO_ROOT / "tests" / "fixtures" / "onboarding_identity_review_goldsets" / "marvel_rivals_identity_review_goldset.json"


class OnboardingIdentityReviewGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "onboarding_identity_review_goldset_v1")
        self.assertEqual(len(payload["cases"]), 3)

    def test_identity_review_scenarios_match_goldset_expectations(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                with tempfile.TemporaryDirectory() as tempdir:
                    root = Path(tempdir)
                    self._write_marvel_starter_seed(root)
                    draft_root = self._write_draft(
                        root,
                        include_info_identity_row=bool(case["include_info_identity_row"]),
                        hero_display_name=str(case["hero_display_name"]),
                        aliases=list(case["aliases"]),
                    )
                    gpt_repo = root / "gpt"
                    _write_gpt_review_repo(gpt_repo)

                    with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                        prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)

                        item = prepared["items"][0]
                        meta = json.loads(Path(item["gpt_meta_path"]).read_text(encoding="utf-8"))
                        bridge = meta["onboarding_identity_review_bridge"]

                        self.assertEqual(item["target_id"], case["expected_target_id"])
                        self.assertEqual(bridge["target_id"], case["expected_target_id"])
                        self.assertEqual(
                            bridge["seed_candidates"][0]["display_name"],
                            case["expected_seed_candidate_display_name"],
                        )

                        if case["scenario"] == "prepare_review":
                            self.assertEqual(item["recommended_decision"], case["expected_recommended_decision"])
                            self.assertEqual(bridge["recommended_decision"], case["expected_recommended_decision"])
                            continue

                        meta["review_status"] = "accepted"
                        meta["reviewed_at"] = "2026-05-18T00:00:00Z"
                        meta["review_decision"] = case["applied_review_decision"]
                        Path(item["gpt_meta_path"]).write_text(json.dumps(meta, indent=2), encoding="utf-8")

                        result = apply_onboarding_identity_review(prepared["manifest_path"])
                        self.assertTrue(result["ok"])
                        self.assertEqual(result[case["expected_result_counter"]], case["expected_result_count"])

                        readiness = validate_onboarding_publish(draft_root)
                        self.assertEqual(readiness["can_publish"], case["expected_can_publish"])

                        entities = load_yaml_file(draft_root / "entities.yaml")
                        hero = entities["heroes"][0]
                        self.assertEqual(hero["identity_review_status"], case["expected_entity_review_status"])

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

    def _write_marvel_starter_seed(self, root: Path) -> None:
        starter_root = root / "starter_assets" / "marvel_rivals"
        starter_root.mkdir(parents=True, exist_ok=True)
        (starter_root / "characters.yaml").write_text(
            "\n".join(
                [
                    "characters:",
                    "  - id: punisher",
                    '    display_name: "The Punisher"',
                    '    aliases: ["punisher", "frank castle"]',
                    "    role: duelist",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    def _write_draft(
        self,
        root: Path,
        *,
        include_info_identity_row: bool,
        hero_display_name: str,
        aliases: list[str],
    ) -> Path:
        draft_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "onboarding" / "20260503T120000Z"
        manifests_root = draft_root / "manifests"
        catalog_root = draft_root / "catalog"
        masters_root = draft_root / "masters"
        manifests_root.mkdir(parents=True, exist_ok=True)
        catalog_root.mkdir(parents=True, exist_ok=True)
        masters_root.mkdir(parents=True, exist_ok=True)

        asset_path = masters_root / "punisher.png"
        asset_path.write_bytes(b"fakepng")

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
            {
                "heroes": [
                    {
                        "hero_id": "punisher",
                        "display_name": hero_display_name,
                        "aliases": aliases,
                        "source_page_url": "file:///tmp/roster.html",
                        "source_role": "roster",
                    }
                ],
                "abilities": [],
                "events": [],
            },
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
                            "master_path": str(asset_path),
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
        self._write_csv(
            catalog_root / "bindings.csv",
            [
                {
                    "detection_id": "marvel_rivals.punisher.hero_portrait",
                    "candidate_id": "candidate-1",
                    "status": "accepted",
                }
            ],
        )
        qa_rows = [
            {
                "item_type": "conflicting_identity_match",
                "target_kind": "hero",
                "target_id": "punisher",
                "display_name": "Punisher",
                "status": "needs_population_review",
                "reason": "candidate identities conflict with the current canonical row: The Punisher",
            }
        ]
        if include_info_identity_row:
            qa_rows.append(
                {
                    "item_type": "canonical_identity_preference_applied",
                    "target_kind": "hero",
                    "target_id": "punisher",
                    "display_name": "Punisher",
                    "status": "info",
                    "reason": "canonical identity preference was applied from starter_seed via display_name_preference",
                }
            )
        self._write_csv(catalog_root / "qa_queue.csv", qa_rows)
        self._write_csv(catalog_root / "heroes.csv", load_yaml_file(draft_root / "entities.yaml")["heroes"])
        self._write_csv(catalog_root / "abilities.csv", [])
        self._write_csv(catalog_root / "events.csv", [])
        return draft_root


if __name__ == "__main__":
    unittest.main()
