from __future__ import annotations

import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pipeline.derived_row_review import apply_derived_row_review, prepare_derived_row_review
from pipeline.game_onboarding import bridge_wiki_draft_to_onboarding, publish_onboarding_draft
from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
_ONE_BY_ONE_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0cIDATx\x9cc``\xf8\x0f\x00\x01\x01\x01\x00\x18\xdd\x8d\xb1"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


class WikiDraftOnboardingBridgeTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[dict[str, object]]) -> None:
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

    def _copy_call_of_duty_starter_seed(self, repo_root: Path) -> None:
        starter_root = repo_root / "starter_assets"
        starter_root.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO_ROOT / "starter_assets" / "runtime_detection_schema.yaml", starter_root / "runtime_detection_schema.yaml")
        shutil.copyfile(REPO_ROOT / "starter_assets" / "runtime_signal_event_ontology.yaml", starter_root / "runtime_signal_event_ontology.yaml")
        shutil.copytree(REPO_ROOT / "starter_assets" / "call_of_duty", starter_root / "call_of_duty")

    def _write_published_pack_fixture(self, repo_root: Path) -> None:
        source_root = REPO_ROOT / "assets" / "games" / "call_of_duty"
        published_root = repo_root / "assets" / "games" / "call_of_duty"
        (published_root / "manifests").mkdir(parents=True, exist_ok=True)

        source_entities = load_yaml_file(source_root / "entities.yaml")
        source_detection_manifest = load_yaml_file(source_root / "manifests" / "detection_manifest.yaml")
        source_assets_manifest = json.loads((source_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
        self.assertIsInstance(source_entities, dict)
        self.assertIsInstance(source_detection_manifest, dict)

        hero_row = next(row for row in source_entities["heroes"] if row["hero_id"] == "alex_keller")
        equipment_row = next(row for row in source_entities["abilities"] if row["ability_id"] == "armor_plates")
        selected_detection_ids = {
            "call_of_duty.alex_keller.hero_portrait",
            "call_of_duty.armor_plates.equipment_icon",
        }
        selected_detection_rows = [
            row
            for row in source_detection_manifest["rows"]
            if row["detection_id"] in selected_detection_ids
        ]
        selected_published_assets = [
            row
            for row in source_assets_manifest["published_assets"]
            if row["detection_id"] in selected_detection_ids
        ]
        selected_candidate_ids = {row["candidate_id"] for row in selected_published_assets}
        baseline_draft_root = published_root / "drafts" / "onboarding" / "20260505T000000Z"
        selected_candidates: list[dict[str, object]] = []
        for row in source_assets_manifest["candidates"]:
            if row["candidate_id"] not in selected_candidate_ids:
                continue
            copied = json.loads(json.dumps(row))
            source_master_path = Path(str(row["master_path"]))
            target_master_path = baseline_draft_root / "masters" / source_master_path.parent.name / source_master_path.name
            target_master_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_master_path, target_master_path)
            copied["master_path"] = str(target_master_path)
            selected_candidates.append(copied)

        selected_bindings = [
            row
            for row in source_assets_manifest["bindings"]
            if row["detection_id"] in selected_detection_ids
        ]

        dump_yaml_file(published_root / "game.yaml", load_yaml_file(source_root / "game.yaml"))
        dump_yaml_file(
            published_root / "entities.yaml",
            {
                "heroes": [hero_row],
                "abilities": [equipment_row],
                "events": [],
            },
        )
        dump_yaml_file(published_root / "medals.yaml", {"medals": []})
        dump_yaml_file(published_root / "hud.yaml", load_yaml_file(source_root / "hud.yaml"))
        dump_yaml_file(published_root / "weights.yaml", load_yaml_file(source_root / "weights.yaml"))

        for row in selected_published_assets:
            for relative_key in ("master_path", "template_path"):
                relative_path = Path(str(row[relative_key]))
                target_path = published_root / relative_path
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source_root / relative_path, target_path)

        dump_yaml_file(
            published_root / "manifests" / "detection_manifest.yaml",
            {
                "schema_version": source_detection_manifest["schema_version"],
                "baseline_schema_version": source_detection_manifest["baseline_schema_version"],
                "game_id": "call_of_duty",
                "row_count": len(selected_detection_rows),
                "required_row_count": len(selected_detection_rows),
                "ready_row_count": len(selected_detection_rows),
                "rows_needing_assets": 0,
                "rows": selected_detection_rows,
            },
        )
        (published_root / "manifests" / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": "call_of_duty",
                    "published_at": "2026-05-25T00:00:00+00:00",
                    "published_assets": selected_published_assets,
                    "published_detection_rows": [row["detection_id"] for row in selected_detection_rows],
                    "candidates": selected_candidates,
                    "bindings": selected_bindings,
                },
                indent=2,
            ) + "\n",
            encoding="utf-8",
        )

    def _write_wiki_draft_fixture(self, repo_root: Path, *, manual_crop: bool) -> Path:
        wiki_root = repo_root / "assets" / "games" / "call_of_duty" / "drafts" / "wiki" / "20260430T015758Z"
        (wiki_root / "catalog").mkdir(parents=True, exist_ok=True)
        image_root = repo_root / "wiki_media"
        image_root.mkdir(parents=True, exist_ok=True)
        medal_png = image_root / "triple_kill_medal.png"
        medal_png.write_bytes(_ONE_BY_ONE_PNG)
        medal_url = medal_png.resolve().as_uri()

        asset_row = {
            "asset_id": "call_of_duty.event_badge_or_medal.triple_kill.test1234",
            "game_id": "call_of_duty",
            "entity_id": "call_of_duty.triple_kill",
            "asset_family": "medal_icon",
            "display_name": "Triple Kill",
            "source_url": medal_url,
            "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
            "source_role": "events",
            "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
            "draft_local_path": "",
            "template_path": "",
            "mask_path": "",
            "roi_ref": "hud.event_badge",
            "match_method": "TM_CCORR_NORMED",
            "threshold": 0.93,
            "scale_set": [0.9, 1.0, 1.1],
            "temporal_window": 3,
            "license_note": "internal_review_required",
            "qa_status": "needs_manual_crop" if manual_crop else "verified",
        }
        (wiki_root / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": "call_of_duty",
                    "source_count": 1,
                    "assets": [asset_row],
                    "qa_queue": [asset_row] if manual_crop else [],
                },
                indent=2,
            ) + "\n",
            encoding="utf-8",
        )
        self._write_csv(wiki_root / "catalog" / "assets.csv", [asset_row])
        self._write_csv(
            wiki_root / "catalog" / "events_or_medals.csv",
            [
                {
                    "event_id": "call_of_duty.triple_kill",
                    "game_id": "call_of_duty",
                    "display_name": "Triple Kill",
                    "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                    "source_role": "events",
                    "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                    "section_heading": "Medals",
                }
            ],
        )
        self._write_csv(
            wiki_root / "catalog" / "source_fetch_log.csv",
            [
                {
                    "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                    "source_role": "events",
                    "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                    "status": "fetched",
                    "content_type": "text/html",
                    "section_count": 1,
                }
            ],
        )
        qa_rows = (
            [
                {
                    "asset_id": asset_row["asset_id"],
                    "display_name": asset_row["display_name"],
                    "qa_status": "needs_manual_crop",
                    "source_url": asset_row["source_url"],
                    "source_role": asset_row["source_role"],
                    "source_page_url": asset_row["source_page_url"],
                }
            ]
            if manual_crop
            else []
        )
        self._write_csv(wiki_root / "catalog" / "qa_queue.csv", qa_rows)
        return wiki_root

    def test_bridge_creates_canonical_draft_and_preserves_baseline_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._copy_call_of_duty_starter_seed(repo_root)
            self._write_published_pack_fixture(repo_root)
            wiki_root = self._write_wiki_draft_fixture(repo_root, manual_crop=True)
            output_root = repo_root / "outputs" / "bridged_onboarding"

            result = bridge_wiki_draft_to_onboarding(wiki_root, output_path=output_root, repo_root=repo_root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "bindings_pending")
            manifest = json.loads((output_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["source_count"], 1)
            self.assertEqual(len(manifest["candidates"]), 3)
            self.assertEqual(len(manifest["bindings"]), 3)
            accepted_count = sum(1 for row in manifest["bindings"] if row["status"] == "accepted")
            pending_medal_count = sum(
                1
                for row in manifest["bindings"]
                if row["status"] == "pending_review" and row["detection_id"] == "call_of_duty.triple_kill.medal_icon"
            )
            self.assertEqual(accepted_count, 2)
            self.assertEqual(pending_medal_count, 1)
            resolved_output_root = output_root.resolve()
            self.assertTrue(
                all(Path(str(row["master_path"])).resolve().is_relative_to(resolved_output_root) for row in manifest["candidates"])
            )
            detection_manifest = load_yaml_file(output_root / "manifests" / "detection_manifest.yaml")
            families = {row["asset_family"] for row in detection_manifest["rows"]}
            self.assertEqual(families, {"hero_portrait", "equipment_icon", "medal_icon"})
            qa_rows = self._read_csv(output_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["item_type"] == "manual_crop_required" for row in qa_rows))
            readiness = validate_onboarding_publish(output_root, repo_root=repo_root)
            self.assertFalse(readiness["can_publish"])
            self.assertEqual(readiness["readiness"], "needs_binding_review")
            self.assertEqual(readiness["counts"]["structural_findings"], 0)

    def test_bridge_review_and_publish_promotes_medal_family(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._copy_call_of_duty_starter_seed(repo_root)
            self._write_published_pack_fixture(repo_root)
            wiki_root = self._write_wiki_draft_fixture(repo_root, manual_crop=False)
            output_root = repo_root / "outputs" / "publish_ready_bridge"

            bridge_wiki_draft_to_onboarding(wiki_root, output_path=output_root, repo_root=repo_root)
            prepared = prepare_derived_row_review(output_root, ["call_of_duty.triple_kill.medal_icon"])
            review_file = Path(prepared["items"][0]["review_file_path"])
            applied = apply_derived_row_review(review_file, accept_recommended=True)
            self.assertEqual(applied["applied_count"], 1)

            published = publish_onboarding_draft(output_root, repo_root=repo_root)
            self.assertTrue(published["ok"])
            published_assets_manifest = json.loads(Path(published["artifacts"]["assets_manifest"]).read_text(encoding="utf-8"))
            self.assertEqual(
                {row["asset_family"] for row in published_assets_manifest["published_assets"]},
                {"hero_portrait", "equipment_icon", "medal_icon"},
            )
            runtime_cv_rules = load_yaml_file(Path(published["artifacts"]["runtime_cv_rules"]))
            self.assertIn("medal_icon", runtime_cv_rules["event_mappings"])
            self.assertEqual(runtime_cv_rules["event_mappings"]["medal_icon"]["signal_type"], "medal_visibility")
            fusion_rules = load_yaml_file(Path(published["artifacts"]["fusion_rules"]))
            rule_ids = {row["rule_id"] for row in fusion_rules["rules"]}
            self.assertIn("medal_visibility_atomic", rule_ids)
            self.assertIn("ability_or_equipment_plus_medal_combo", rule_ids)

    def _read_csv(self, path: Path) -> list[dict[str, str]]:
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))


if __name__ == "__main__":
    unittest.main()
