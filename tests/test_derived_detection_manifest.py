from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.derived_detection_manifest import derive_game_detection_manifest
from pipeline.game_onboarding import _adapt_detection_schema, _load_runtime_detection_schema
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file


class DerivedDetectionManifestTests(unittest.TestCase):
    def _write_draft(
        self,
        root: Path,
        *,
        disable_medals: bool = False,
        include_binding: bool = True,
        include_events: bool = True,
    ) -> Path:
        draft_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "onboarding" / "20260505T120000Z"
        manifests_root = draft_root / "manifests"
        catalog_root = draft_root / "catalog"
        masters_root = draft_root / "masters"
        manifests_root.mkdir(parents=True, exist_ok=True)
        catalog_root.mkdir(parents=True, exist_ok=True)
        masters_root.mkdir(parents=True, exist_ok=True)

        hero_path = masters_root / "punisher.png"
        hero_path.write_bytes(b"fakepng")

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
                        "display_name": "The Punisher",
                        "aliases": ["punisher"],
                        "source_page_url": "https://example.com/roster",
                        "source_role": "roster",
                    }
                ],
                "abilities": [
                    {
                        "ability_id": "final_judgment",
                        "display_name": "Final Judgment",
                        "aliases": ["final judgment"],
                        "source_page_url": "https://example.com/abilities",
                        "source_role": "abilities",
                    }
                ],
                "events": (
                    [
                        {
                            "event_id": "triple_ko",
                            "display_name": "Triple KO",
                            "aliases": ["triple ko"],
                            "source_page_url": "https://example.com/medals",
                            "source_role": "medals",
                        }
                    ]
                    if include_events
                    else []
                ),
            },
        )

        baseline = _load_runtime_detection_schema(repo_root=root)
        schema = _adapt_detection_schema("marvel_rivals", baseline, repo_root=root)
        if disable_medals:
            schema["families"]["medal_icon"]["enabled"] = False
        dump_yaml_file(manifests_root / "game_detection_schema.yaml", schema)

        assets_manifest = {
            "game_id": "marvel_rivals",
            "phase_status": "bindings_pending",
            "source_count": 1,
            "source_fetch_log": [{"status": "fetched", "source_role": "roster"}],
            "candidates": [
                {
                    "candidate_id": "candidate-hero",
                    "display_name": "The Punisher",
                    "master_path": str(hero_path),
                    "source_url": "https://example.com/punisher.png",
                    "license_note": "internal_review_required",
                }
            ],
            "bindings": [],
        }
        (manifests_root / "assets_manifest.json").write_text(json.dumps(assets_manifest, indent=2), encoding="utf-8")

        bindings_csv = catalog_root / "bindings.csv"
        bindings_csv.write_text(
            "\n".join(
                [
                    "candidate_id,confidence,detection_id,status",
                    "candidate-hero,0.99,marvel_rivals.punisher.hero_portrait,accepted" if include_binding else "",
                    "candidate-hero,0.55,marvel_rivals.final_judgment.ability_icon,pending_review",
                ]
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        return draft_root

    def test_derive_detection_manifest_reports_resolved_and_unresolved_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            draft_root = self._write_draft(root)

            result = derive_game_detection_manifest(draft_root)

            self.assertTrue(result["ok"])
            payload = load_yaml_file(Path(result["manifest_path"]))
            self.assertEqual(payload["schema_version"], "derived_game_detection_manifest_v1")
            self.assertEqual(payload["counts"]["row_count"], 3)
            self.assertEqual(payload["counts"]["required_row_count"], 3)
            self.assertEqual(payload["counts"]["resolved_row_count"], 1)
            self.assertEqual(payload["counts"]["unresolved_required_row_count"], 2)
            hero_row = next(row for row in payload["rows"] if row["asset_family"] == "hero_portrait")
            ability_row = next(row for row in payload["rows"] if row["asset_family"] == "ability_icon")
            self.assertEqual(hero_row["status"], "resolved")
            self.assertFalse(hero_row["blocking_publish"])
            self.assertEqual(hero_row["accepted_candidate_id"], "candidate-hero")
            self.assertEqual(ability_row["status"], "unresolved_pending_review")
            self.assertTrue(ability_row["blocking_publish"])

    def test_derive_detection_manifest_marks_disabled_family_optional(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            draft_root = self._write_draft(root, disable_medals=True, include_binding=False)

            result = derive_game_detection_manifest(draft_root, output_path=root / "reports" / "derived.yaml")

            self.assertTrue(result["ok"])
            self.assertEqual(Path(result["manifest_path"]), (root / "reports" / "derived.yaml").resolve())
            payload = load_yaml_file(Path(result["manifest_path"]))
            medal_row = next(row for row in payload["rows"] if row["asset_family"] == "medal_icon")
            medal_family = next(row for row in payload["family_summaries"] if row["asset_family"] == "medal_icon")
            self.assertEqual(medal_row["status"], "optional_family_disabled")
            self.assertFalse(medal_row["required"])
            self.assertEqual(medal_family["status"], "optional_family_disabled")
            self.assertEqual(payload["counts"]["disabled_family_count"], 1)

    def test_derive_detection_manifest_marks_missing_family_targets_as_optional_unsupported(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            draft_root = self._write_draft(root, include_events=False)

            result = derive_game_detection_manifest(draft_root)

            self.assertTrue(result["ok"])
            payload = load_yaml_file(Path(result["manifest_path"]))
            medal_family = next(row for row in payload["family_summaries"] if row["asset_family"] == "medal_icon")
            self.assertEqual(medal_family["status"], "optional_unsupported")
            self.assertEqual(medal_family["reason"], "no ontology targets were derived for this family")
            self.assertGreaterEqual(payload["counts"]["unsupported_family_count"], 1)
            self.assertFalse(any(row["asset_family"] == "medal_icon" for row in payload["rows"]))


if __name__ == "__main__":
    unittest.main()
