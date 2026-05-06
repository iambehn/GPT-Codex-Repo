from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline.onboarding_identity_review_bridge as onboarding_identity_review_bridge
from pipeline.onboarding_batch_publish import publish_onboarding_batch
from pipeline.onboarding_identity_review_bridge import (
    apply_onboarding_identity_review,
    cleanup_onboarding_identity_review,
    prepare_onboarding_identity_review,
)
from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from tests.test_run import _write_gpt_review_repo


class OnboardingIdentityReviewBridgeTests(unittest.TestCase):
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
        include_info_identity_row: bool = True,
        hero_display_name: str = "Punisher",
        aliases: list[str] | None = None,
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
                        "aliases": aliases or ["punisher"],
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

    def test_prepare_onboarding_identity_review_creates_one_item_per_blocked_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                result = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)

            self.assertEqual(result["item_count"], 1)
            item = result["items"][0]
            self.assertIn("adopt_seed_identity", item["allowed_decisions"])
            self.assertEqual(item["recommended_decision"], "adopt_seed_identity")
            meta = json.loads(Path(item["gpt_meta_path"]).read_text(encoding="utf-8"))
            bridge = meta["onboarding_identity_review_bridge"]
            self.assertEqual(bridge["target_kind"], "hero")
            self.assertEqual(bridge["row_snapshot"]["display_name"], "Punisher")
            self.assertEqual(len(bridge["seed_candidates"]), 1)
            self.assertEqual(bridge["seed_candidates"][0]["display_name"], "The Punisher")
            self.assertIn("punisher", bridge["seed_candidates"][0]["match_evidence"]["shared_aliases"])
            self.assertEqual(len(bridge["blocking_identity_findings"]), 1)
            self.assertEqual(bridge["blocking_identity_findings"][0]["item_type"], "conflicting_identity_match")
            self.assertEqual(bridge["recommended_decision"], "adopt_seed_identity")
            self.assertIn("upgrades the canonical name", bridge["recommendation_reason"])
            self.assertIn("keep_source_identity", bridge["decision_previews"])
            self.assertIn("adopt_seed_identity", bridge["decision_previews"])

    def test_prepare_onboarding_identity_review_recommends_keep_when_no_upgrade_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root, hero_display_name="The Punisher", aliases=["punisher"])
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                result = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)

            item = result["items"][0]
            self.assertEqual(item["recommended_decision"], "keep_source_identity")
            self.assertIn("source-derived row is internally stable", item["recommendation_reason"])

    def test_apply_onboarding_identity_review_keep_source_identity_clears_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["review_status"] = "accepted"
                meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                meta["review_decision"] = "keep_source_identity"
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertEqual(result["resolved_count"], 1)
            readiness = validate_onboarding_publish(draft_root)
            self.assertTrue(readiness["can_publish"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            hero = entities["heroes"][0]
            self.assertEqual(hero["display_name"], "Punisher")
            self.assertEqual(hero["identity_review_decision"], "keep_source_identity")
            self.assertEqual(hero["identity_review_previous_id"], "punisher")
            self.assertEqual(hero["identity_review_applied_display_name"], "Punisher")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertFalse(any(row["item_type"] == "conflicting_identity_match" for row in qa_rows))
            self.assertTrue(any(row["item_type"] == "identity_review_applied" for row in qa_rows))
            batch = publish_onboarding_batch(root / "assets" / "games")
            self.assertEqual(batch["summary"]["ready"], 1)

    def test_apply_onboarding_identity_review_adopt_seed_identity_updates_canonical_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root, include_info_identity_row=False)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["review_status"] = "accepted"
                meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                meta["review_decision"] = "adopt_seed_identity"
                meta["selected_seed_id"] = "punisher"
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertEqual(result["resolved_count"], 1)
            readiness = validate_onboarding_publish(draft_root)
            self.assertTrue(readiness["can_publish"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            hero = entities["heroes"][0]
            self.assertEqual(hero["display_name"], "The Punisher")
            self.assertEqual(hero["canonical_display_name_source"], "starter_seed")
            self.assertIn("frank castle", hero["aliases"])
            self.assertEqual(hero["identity_review_decision"], "adopt_seed_identity")
            self.assertEqual(hero["identity_review_previous_display_name"], "Punisher")
            self.assertEqual(hero["identity_review_applied_display_name"], "The Punisher")

    def test_apply_onboarding_identity_review_defer_keeps_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["review_status"] = "accepted"
                meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                meta["review_decision"] = "defer_identity_resolution"
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertEqual(result["deferred_count"], 1)
            readiness = validate_onboarding_publish(draft_root)
            self.assertFalse(readiness["can_publish"])
            self.assertEqual(readiness["readiness"], "needs_population_review")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["item_type"] == "conflicting_identity_match" for row in qa_rows))
            self.assertTrue(any(row["item_type"] == "identity_review_deferred" for row in qa_rows))
            entities = load_yaml_file(draft_root / "entities.yaml")
            hero = entities["heroes"][0]
            self.assertEqual(hero["identity_review_status"], "deferred")

    def test_apply_onboarding_identity_review_rejects_missing_decision_and_keeps_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["review_status"] = "accepted"
                meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertEqual(result["failed_item_count"], 1)
            self.assertEqual(result["failed_items"][0]["apply_status"], "invalid_review_decision")
            readiness = validate_onboarding_publish(draft_root)
            self.assertFalse(readiness["can_publish"])

    def test_apply_onboarding_identity_review_rejects_unknown_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["review_status"] = "accepted"
                meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                meta["review_decision"] = "rename_everything"
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertEqual(result["failed_items"][0]["apply_status"], "invalid_review_decision")

    def test_apply_onboarding_identity_review_rejects_invalid_selected_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["review_status"] = "accepted"
                meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                meta["review_decision"] = "adopt_seed_identity"
                meta["selected_seed_id"] = "not-a-real-seed"
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertEqual(result["failed_items"][0]["apply_status"], "invalid_seed_selection")

    def test_apply_onboarding_identity_review_rejects_disallowed_seed_adoption(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["review_status"] = "accepted"
                meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                meta["review_decision"] = "adopt_seed_identity"
                meta["selected_seed_id"] = "punisher"
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertEqual(result["failed_items"][0]["apply_status"], "disallowed_seed_adoption")

    def test_apply_onboarding_identity_review_mixed_valid_and_invalid_items_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["heroes"].append(
                {
                    "hero_id": "mantis",
                    "display_name": "Mantis",
                    "aliases": ["mantis"],
                    "source_page_url": "file:///tmp/roster.html",
                    "source_role": "roster",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)
            self._write_csv(draft_root / "catalog" / "heroes.csv", entities["heroes"])
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            qa_rows.append(
                {
                    "item_type": "conflicting_identity_match",
                    "target_kind": "hero",
                    "target_id": "mantis",
                    "display_name": "Mantis",
                    "status": "needs_population_review",
                    "reason": "candidate identities conflict with the current canonical row: Mantis",
                }
            )
            self._write_csv(draft_root / "catalog" / "qa_queue.csv", qa_rows)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                punisher_item = next(item for item in prepared["items"] if item["target_id"] == "punisher")
                mantis_item = next(item for item in prepared["items"] if item["target_id"] == "mantis")
                first_meta_path = Path(punisher_item["gpt_meta_path"])
                first_meta = json.loads(first_meta_path.read_text(encoding="utf-8"))
                first_meta["review_status"] = "accepted"
                first_meta["reviewed_at"] = "2026-05-03T13:00:00Z"
                first_meta["review_decision"] = "adopt_seed_identity"
                first_meta["selected_seed_id"] = "punisher"
                first_meta_path.write_text(json.dumps(first_meta, indent=2), encoding="utf-8")

                second_meta_path = Path(mantis_item["gpt_meta_path"])
                second_meta = json.loads(second_meta_path.read_text(encoding="utf-8"))
                second_meta["review_status"] = "accepted"
                second_meta["reviewed_at"] = "2026-05-03T13:01:00Z"
                second_meta["review_decision"] = "adopt_seed_identity"
                second_meta["selected_seed_id"] = "bad-id"
                second_meta_path.write_text(json.dumps(second_meta, indent=2), encoding="utf-8")

                result = apply_onboarding_identity_review(prepared["manifest_path"])

            self.assertEqual(result["resolved_count"], 1)
            self.assertEqual(result["failed_item_count"], 1)
            entities = load_yaml_file(draft_root / "entities.yaml")
            punisher = next(row for row in entities["heroes"] if row["hero_id"] == "punisher")
            mantis = next(row for row in entities["heroes"] if row["hero_id"] == "mantis")
            self.assertEqual(punisher["display_name"], "The Punisher")
            self.assertEqual(mantis["display_name"], "Mantis")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["target_id"] == "mantis" and row["item_type"] == "conflicting_identity_match" for row in qa_rows))

    def test_cleanup_onboarding_identity_review_removes_bridge_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_marvel_starter_seed(root)
            draft_root = self._write_draft(root)
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            with patch.object(onboarding_identity_review_bridge, "REPO_ROOT", root):
                prepared = prepare_onboarding_identity_review(draft_root, gpt_repo=gpt_repo)
                item = prepared["items"][0]
                meta_path = Path(item["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                final_path = gpt_repo / "accepted" / "marvel_rivals" / f"{meta['clip_id']}.json"
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_text("{}", encoding="utf-8")
                meta["final_path"] = str(final_path)
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = cleanup_onboarding_identity_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertFalse(Path(item["gpt_processed_path"]).exists())
            self.assertFalse(Path(item["gpt_meta_path"]).exists())
            self.assertFalse(final_path.exists())

    def _read_csv(self, path: Path) -> list[dict[str, str]]:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
        if len(rows) == 1 and set(rows[0].keys()) == {"empty"}:
            return []
        return rows


if __name__ == "__main__":
    unittest.main()
