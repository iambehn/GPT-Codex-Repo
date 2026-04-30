from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.simple_yaml import load_yaml_file
from pipeline.wiki_enrichment import enrich_game_from_wiki
from run import main as run_main


_ONE_BY_ONE_PNG = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\rIDATx\x9cc`\x00\x00\x00\x02\x00\x01\xe2!\xbc3"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


class WikiEnrichmentTests(unittest.TestCase):
    def _write_source_bundle(self, root: Path, *, with_direct_images: bool = True) -> str:
        source_dir = root / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        image_path = source_dir / "operator_alpha.png"
        medal_path = source_dir / "triple_kill.png"
        equipment_path = source_dir / "flash_grenade.png"
        for path in (image_path, medal_path, equipment_path):
            path.write_bytes(_ONE_BY_ONE_PNG)

        if with_direct_images:
            operator_src = image_path.resolve().as_uri()
            medal_src = medal_path.resolve().as_uri()
            equipment_src = equipment_path.resolve().as_uri()
        else:
            operator_src = "https://example.com/operators"
            medal_src = "https://example.com/medals"
            equipment_src = "https://example.com/equipment"

        html = f"""
        <html>
          <head><title>Call of Duty Wiki Seed</title></head>
          <body>
            <h1>Operators</h1>
            <ul>
              <li>Operator Alpha</li>
              <li>Operator Bravo</li>
            </ul>
            <img src="{operator_src}" alt="Operator Alpha" />
            <h2>Equipment</h2>
            <ul>
              <li>Flash Grenade</li>
            </ul>
            <img src="{equipment_src}" alt="Flash Grenade" />
            <h2>Medals</h2>
            <ul>
              <li>Triple Kill</li>
              <li>Longshot</li>
            </ul>
            <img src="{medal_src}" alt="Triple Kill" />
          </body>
        </html>
        """
        html_path = source_dir / "cod_seed.html"
        html_path.write_text(html, encoding="utf-8")
        return html_path.resolve().as_uri()

    def test_enrich_game_from_wiki_writes_draft_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_url = self._write_source_bundle(repo_root)

            result = enrich_game_from_wiki("call_of_duty", wiki_url, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            self.assertTrue((draft_root / "game.draft.yaml").is_file())
            self.assertTrue((draft_root / "entities.draft.yaml").is_file())
            self.assertTrue((draft_root / "abilities.draft.yaml").is_file())
            self.assertTrue((draft_root / "events.draft.yaml").is_file())
            self.assertTrue((draft_root / "assets_manifest.json").is_file())
            self.assertTrue((draft_root / "catalog" / "assets.csv").is_file())
            self.assertTrue((draft_root / "downloads").is_dir())
            self.assertTrue((draft_root / "templates").is_dir())
            self.assertTrue((draft_root / "masks").is_dir())

    def test_adapter_parses_entities_abilities_and_medals(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_url = self._write_source_bundle(repo_root)
            result = enrich_game_from_wiki("call_of_duty", wiki_url, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            abilities = load_yaml_file(draft_root / "abilities.draft.yaml")
            events = load_yaml_file(draft_root / "events.draft.yaml")

            self.assertEqual(len(entities["characters"]), 2)
            self.assertEqual(len(abilities["abilities"]), 1)
            self.assertEqual(len(events["events"]), 2)

    def test_missing_optional_image_fields_stay_in_qa_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_url = self._write_source_bundle(repo_root, with_direct_images=False)
            result = enrich_game_from_wiki("call_of_duty", wiki_url, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))
            self.assertTrue(manifest["qa_queue"])
            self.assertTrue(all(row["qa_status"] == "needs_manual_crop" for row in manifest["qa_queue"]))

    def test_catalog_csv_contains_required_tables_and_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_url = self._write_source_bundle(repo_root)
            result = enrich_game_from_wiki("call_of_duty", wiki_url, repo_root=repo_root)
            catalog_root = Path(result["catalog_root"])

            expected = {
                "games.csv",
                "entities.csv",
                "abilities_or_equipment.csv",
                "events_or_medals.csv",
                "assets.csv",
                "source_fetch_log.csv",
                "qa_queue.csv",
            }
            self.assertEqual({path.name for path in catalog_root.iterdir()}, expected)
            assets_header = (catalog_root / "assets.csv").read_text(encoding="utf-8").splitlines()[0]
            self.assertIn("asset_id", assets_header)
            self.assertIn("asset_family", assets_header)
            self.assertIn("qa_status", assets_header)

    def test_direct_image_urls_download_into_downloads_and_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_url = self._write_source_bundle(repo_root)
            result = enrich_game_from_wiki("call_of_duty", wiki_url, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            downloaded = list((draft_root / "downloads").rglob("*.png"))
            templated = list((draft_root / "templates").rglob("*.png"))
            self.assertGreaterEqual(len(downloaded), 3)
            self.assertGreaterEqual(len(templated), 3)

    def test_cli_routes_to_wiki_enrichment(self) -> None:
        with patch("run.run_enrich_game_from_wiki", return_value={"ok": True}) as mocked:
            with patch.object(
                sys,
                "argv",
                [
                    "run.py",
                    "--enrich-game-from-wiki",
                    "call_of_duty",
                    "--wiki-url",
                    "https://example.com/cod",
                ],
            ):
                self.assertEqual(run_main(), 0)
        mocked.assert_called_once_with("call_of_duty", "https://example.com/cod")

    def test_no_live_mutation_of_starter_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            starter_root = repo_root / "starter_assets" / "marvel_rivals"
            starter_root.mkdir(parents=True, exist_ok=True)
            marker = starter_root / "characters.yaml"
            marker.write_text("characters: []\n", encoding="utf-8")
            wiki_url = self._write_source_bundle(repo_root)

            enrich_game_from_wiki("call_of_duty", wiki_url, repo_root=repo_root)

            self.assertEqual(marker.read_text(encoding="utf-8"), "characters: []\n")
            self.assertTrue((repo_root / "assets" / "games" / "call_of_duty" / "drafts").is_dir())
