from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    import run as run_module
    from pipeline.wiki_enrichment import (
        enrich_game_from_wiki,
        parse_fandom_entities,
        slugify_entity_id,
    )
finally:
    os.chdir(_ORIGINAL_CWD)


FANDOM_HTML = """
<html>
  <body>
    <table class="article-table">
      <tr><th>Name</th><th>Role</th><th>Icon</th></tr>
      <tr>
        <td><a href="/wiki/Hero_One">Hero One</a></td>
        <td>Duelist</td>
        <td><img src="https://static.wikia.nocookie.net/game/HeroOne.png"></td>
      </tr>
      <tr>
        <td><a href="/wiki/Spider-Man">Spider-Man</a></td>
        <td>Vanguard</td>
        <td><img data-src="//static.wikia.nocookie.net/game/SpiderMan.webp"></td>
      </tr>
    </table>
    <div class="character-card">
      <a href="/wiki/Star-Lord">Star-Lord</a>
      <img alt="Star-Lord" src="https://static.wikia.nocookie.net/game/StarLord.jpg">
    </div>
  </body>
</html>
"""


class WikiEnrichmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.assets = self.root / "assets"
        self.pack_dir = self.assets / "games" / "test_game"
        self.pack_dir.mkdir(parents=True, exist_ok=True)
        self.config = {
            "paths": {"assets": str(self.assets)},
            "games": {"test_game": {"display_name": "Test Game"}},
        }
        self.existing_entities = {
            "primary_kind": "heroes",
            "heroes": {"existing_hero": {"display_name": "Existing Hero"}},
            "aliases": {},
        }
        (self.pack_dir / "entities.yaml").write_text(yaml.safe_dump(self.existing_entities))
        for filename in ("game.yaml", "moments.yaml", "hud.yaml", "weights.yaml"):
            (self.pack_dir / filename).write_text("{}\n")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_slugify_entity_id_handles_punctuation_and_hyphens(self) -> None:
        self.assertEqual(slugify_entity_id("Spider-Man"), "spider_man")
        self.assertEqual(slugify_entity_id("Soldier: 76"), "soldier_76")
        self.assertEqual(slugify_entity_id("Jeff's Shark!"), "jeffs_shark")

    def test_fandom_parser_extracts_table_and_card_entities(self) -> None:
        entities, warnings = parse_fandom_entities(
            FANDOM_HTML,
            "https://example.fandom.com/wiki/Characters",
        )

        self.assertEqual(warnings, [])
        by_id = {entity.entity_id: entity for entity in entities}
        self.assertEqual(by_id["hero_one"].display_name, "Hero One")
        self.assertEqual(by_id["hero_one"].role, "Duelist")
        self.assertEqual(by_id["spider_man"].source_icon_url, "https://static.wikia.nocookie.net/game/SpiderMan.webp")
        self.assertEqual(by_id["star_lord"].source_url, "https://example.fandom.com/wiki/Star-Lord")

    def test_enrichment_writes_draft_without_modifying_entities_yaml(self) -> None:
        def image_fetcher(url: str):
            if url.endswith(".webp"):
                return b"RIFFxxxxWEBP", "image/webp"
            if url.endswith(".jpg"):
                return b"\xff\xd8\xfffake", "image/jpeg"
            return b"\x89PNG\r\n\x1a\nfake", "image/png"

        result = enrich_game_from_wiki(
            "test_game",
            "https://example.fandom.com/wiki/Characters",
            self.config,
            html=FANDOM_HTML,
            timestamp="20260422-000000",
            image_fetcher=image_fetcher,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["entities_found"], 3)
        self.assertEqual(result["icons_downloaded"], 3)

        draft_dir = self.pack_dir / "drafts" / "wiki" / "20260422-000000"
        self.assertTrue((draft_dir / "entities.draft.yaml").exists())
        self.assertTrue((draft_dir / "assets_manifest.json").exists())
        self.assertTrue((draft_dir / "icons" / "hero_one.png").exists())

        self.assertEqual(yaml.safe_load((self.pack_dir / "entities.yaml").read_text()), self.existing_entities)
        draft = yaml.safe_load((draft_dir / "entities.draft.yaml").read_text())
        self.assertEqual(draft["heroes"]["hero_one"]["scrape_status"], "ok")

        manifest = json.loads((draft_dir / "assets_manifest.json").read_text())
        self.assertEqual(manifest["parser"], "fandom")
        self.assertEqual(manifest["icons_downloaded"], 3)

    def test_unsupported_domain_returns_structured_failure(self) -> None:
        result = enrich_game_from_wiki(
            "test_game",
            "https://example.com/wiki/Characters",
            self.config,
            html=FANDOM_HTML,
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["parser"], "unsupported")
        self.assertIsNone(result["draft_dir"])

    def test_failed_image_download_is_partial_not_fatal(self) -> None:
        def image_fetcher(url: str):
            raise RuntimeError("download blocked")

        result = enrich_game_from_wiki(
            "test_game",
            "https://example.fandom.com/wiki/Characters",
            self.config,
            html=FANDOM_HTML,
            timestamp="20260422-010000",
            image_fetcher=image_fetcher,
        )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["entities_found"], 3)
        self.assertEqual(result["icons_downloaded"], 0)
        self.assertTrue(any("Failed to download icon" in warning for warning in result["warnings"]))

    def test_no_entities_is_failed_but_writes_debuggable_draft(self) -> None:
        result = enrich_game_from_wiki(
            "test_game",
            "https://example.fandom.com/wiki/Characters",
            self.config,
            html="<html><body><p>No roster here</p></body></html>",
            timestamp="20260422-020000",
        )

        self.assertEqual(result["status"], "failed")
        draft_dir = self.pack_dir / "drafts" / "wiki" / "20260422-020000"
        self.assertTrue((draft_dir / "assets_manifest.json").exists())

    def test_run_cli_routes_to_wiki_enrichment(self) -> None:
        config_path = self.root / "config.yaml"
        config_path.write_text(yaml.safe_dump(self.config))
        with patch.object(run_module, "enrich_game_from_wiki", return_value={
            "game": "test_game",
            "source_url": "https://example.fandom.com/wiki/Characters",
            "parser": "fandom",
            "status": "ok",
            "entities_found": 1,
            "icons_downloaded": 0,
            "warnings": [],
            "draft_dir": "draft",
        }) as mocked, patch("builtins.print"), patch.object(sys, "argv", [
            "run.py",
            "--enrich-game-from-wiki",
            "test_game",
            "--wiki-url",
            "https://example.fandom.com/wiki/Characters",
            "--config",
            str(config_path),
        ]):
            run_module.main()

        mocked.assert_called_once()
        self.assertEqual(mocked.call_args.args[0], "test_game")
        self.assertEqual(mocked.call_args.args[1], "https://example.fandom.com/wiki/Characters")


if __name__ == "__main__":
    unittest.main()
