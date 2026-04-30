from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from pipeline.simple_yaml import load_yaml_file
from pipeline.wiki_enrichment import WikiSource, _build_fetch_target, enrich_game_from_sources, enrich_game_from_wiki
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

    def _write_role_specific_sources(self, root: Path, *, with_direct_images: bool = True) -> list[WikiSource]:
        source_dir = root / "source_roles"
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

        pages = {
            "operators": f"""
                <html><body><div class="mw-parser-output">
                  <h2>Roster</h2>
                  <ul><li>Operator Alpha</li><li>Operator Bravo</li></ul>
                  <img src="{operator_src}" alt="Operator Alpha" />
                </div></body></html>
            """,
            "equipment": f"""
                <html><body><div class="mw-parser-output">
                  <h2>Loadout</h2>
                  <ul><li>Flash Grenade</li></ul>
                  <img src="{equipment_src}" alt="Flash Grenade" />
                </div></body></html>
            """,
            "events": f"""
                <html><body><div class="mw-parser-output">
                  <h2>Live Events</h2>
                  <ul><li>Triple Kill</li><li>Longshot</li></ul>
                  <img src="{medal_src}" alt="Triple Kill" />
                </div></body></html>
            """,
        }
        sources: list[WikiSource] = []
        for role, html in pages.items():
            html_path = source_dir / f"{role}.html"
            html_path.write_text(html, encoding="utf-8")
            sources.append(WikiSource(url=html_path.resolve().as_uri(), role=role))
        return sources

    def _write_fandom_like_bundle(self, root: Path) -> str:
        source_dir = root / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        operator_path = source_dir / "operator_alpha.png"
        medal_path = source_dir / "triple_kill.png"
        operator_path.write_bytes(_ONE_BY_ONE_PNG)
        medal_path.write_bytes(_ONE_BY_ONE_PNG)

        html = f"""
        <html>
          <head><title>Fandom Seed</title></head>
          <body>
            <div class="global-navigation">
              <img src="https://static.wikia.nocookie.net/callofduty/images/e/e6/Site-logo.png/revision/latest?cb=1" alt="Call of Duty Wiki" />
            </div>
            <div id="mw-content-text">
              <div class="mw-parser-output">
                <div class="toc">
                  <ul><li>1.1 Health</li><li>10 Trivia</li></ul>
                </div>
                <h2>Operators</h2>
                <ul>
                  <li>Operator Alpha</li>
                </ul>
                <img src="{operator_path.resolve().as_uri()}" alt="Operator Alpha" />
                <h2>Equipment</h2>
                <ul>
                  <li>Flash Grenade</li>
                </ul>
                <img src="data:image/gif;base64,R0lGODlhAQABAIABAAAAAP///yH5BAEAAAEALAAAAAABAAEAQAICTAEAOw==" alt="Flash Grenade" />
                <h2>Medals</h2>
                <ul>
                  <li>Triple Kill</li>
                </ul>
                <img src="{medal_path.resolve().as_uri()}" alt="Triple Kill" />
                <img src="{medal_path.resolve().as_uri()}" alt="Triple Kill" />
                <h2>Overview</h2>
                <ul>
                  <li>Should Be Ignored</li>
                </ul>
              </div>
            </div>
          </body>
        </html>
        """
        html_path = source_dir / "fandom_seed.html"
        html_path.write_text(html, encoding="utf-8")
        return html_path.resolve().as_uri()

    def _write_multi_page_sources(self, root: Path) -> list[WikiSource]:
        source_dir = root / "multi"
        source_dir.mkdir(parents=True, exist_ok=True)
        operator_path = source_dir / "operator_alpha.png"
        medal_path = source_dir / "triple_kill.png"
        operator_path.write_bytes(_ONE_BY_ONE_PNG)
        medal_path.write_bytes(_ONE_BY_ONE_PNG)

        overview_html = """
        <html>
          <body>
            <div class="mw-parser-output">
              <h2>Overview</h2>
              <ul><li>Should Stay Ignored</li></ul>
            </div>
          </body>
        </html>
        """
        operators_html = f"""
        <html>
          <body>
            <div class="mw-parser-output">
              <h2>Roster</h2>
              <ul>
                <li>Operator Alpha</li>
                <li>Operator Alpha</li>
              </ul>
              <img src="{operator_path.resolve().as_uri()}" alt="Operator Alpha" />
            </div>
          </body>
        </html>
        """
        events_html = f"""
        <html>
          <body>
            <div class="mw-parser-output">
              <h2>Live Events</h2>
              <ul><li>Triple Kill</li></ul>
              <img src="{medal_path.resolve().as_uri()}" alt="Triple Kill" />
            </div>
          </body>
        </html>
        """
        equipment_html = """
        <html>
          <body>
            <div class="mw-parser-output">
              <h2>Loadout</h2>
              <ul><li>Flash Grenade</li></ul>
            </div>
          </body>
        </html>
        """

        overview_path = source_dir / "overview.html"
        operators_path = source_dir / "operators.html"
        events_path = source_dir / "events.html"
        equipment_path_html = source_dir / "equipment.html"
        overview_path.write_text(overview_html, encoding="utf-8")
        operators_path.write_text(operators_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")
        equipment_path_html.write_text(equipment_html, encoding="utf-8")

        return [
            WikiSource(url=overview_path.resolve().as_uri(), role="overview"),
            WikiSource(url=operators_path.resolve().as_uri(), role="operators"),
            WikiSource(url=events_path.resolve().as_uri(), role="events"),
            WikiSource(url=equipment_path_html.resolve().as_uri(), role="equipment"),
        ]

    def _write_category_page_sources(self, root: Path) -> list[WikiSource]:
        source_dir = root / "category_sources"
        source_dir.mkdir(parents=True, exist_ok=True)
        operators_html = """
        <html>
          <head><title>Category: Operators</title></head>
          <body>
            <div class="category-page__alphabet-shortcuts">
              <a href="#A">A</a>
            </div>
            <div class="category-page__trending-pages">
              <a href="/wiki/Trending_Operator">Trending Pages</a>
            </div>
            <section class="category-page__members">
              <a class="category-page__member-link" href="/wiki/Ghost">Ghost</a>
              <a class="category-page__member-link" href="/wiki/Farah">Farah</a>
            </section>
          </body>
        </html>
        """
        equipment_html = """
        <html>
          <head><title>Category: Equipment</title></head>
          <body>
            <section class="category-page__members">
              <a class="category-page__member-link" href="/wiki/Flash_Grenade">Flash Grenade</a>
              <a class="category-page__member-link" href="/wiki/Smoke_Grenade">Smoke Grenade</a>
            </section>
          </body>
        </html>
        """
        operators_path = source_dir / "category_operators.html"
        equipment_path = source_dir / "category_equipment.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        return [
            WikiSource(url=operators_path.resolve().as_uri(), role="operators"),
            WikiSource(url=equipment_path.resolve().as_uri(), role="equipment"),
        ]

    def _write_category_page_sources_with_thumbnails(self, root: Path) -> list[WikiSource]:
        source_dir = root / "category_sources_thumbs"
        source_dir.mkdir(parents=True, exist_ok=True)
        operator_image = source_dir / "ghost.png"
        equipment_image = source_dir / "flash_grenade.png"
        operator_image.write_bytes(_ONE_BY_ONE_PNG)
        equipment_image.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html>
          <head><title>Category: Operators</title></head>
          <body>
            <section class="category-page__members">
              <article class="category-page__member">
                <img src="{operator_image.resolve().as_uri()}" alt="Ghost portrait" />
                <a class="category-page__member-link" href="/wiki/Ghost">Ghost</a>
              </article>
            </section>
          </body>
        </html>
        """
        equipment_html = f"""
        <html>
          <head><title>Category: Equipment</title></head>
          <body>
            <section class="category-page__members">
              <article class="category-page__member">
                <img data-src="{equipment_image.resolve().as_uri()}" alt="Flash Grenade icon" />
                <a class="category-page__member-link" href="/wiki/Flash_Grenade">Flash Grenade</a>
              </article>
            </section>
          </body>
        </html>
        """
        operators_path = source_dir / "category_operators.html"
        equipment_path = source_dir / "category_equipment.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        return [
            WikiSource(url=operators_path.resolve().as_uri(), role="operators"),
            WikiSource(url=equipment_path.resolve().as_uri(), role="equipment"),
        ]

    def _write_events_role_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "events_role"
        source_dir.mkdir(parents=True, exist_ok=True)
        icon_path = source_dir / "fire_sale_icon.png"
        map_path = source_dir / "verdansk_map.png"
        icon_path.write_bytes(_ONE_BY_ONE_PNG)
        map_path.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Warzone Events</title></head>
          <body>
            <div class="mw-parser-output">
              <h2>Events</h2>
              <ul>
                <li>Jailbreak</li>
                <li>The Fire Sale event temporarily lowers Buy Station prices.</li>
              </ul>
              <img src="{icon_path.resolve().as_uri()}" alt="Fire Sale Icon" />
              <h2>Contracts</h2>
              <ul><li>A Bounty contract tasks the team with killing a specific player.</li></ul>
              <h2>Intel Missions</h2>
              <ul><li>A hidden code was found in the prison...</li></ul>
              <h2>Maps</h2>
              <img src="{map_path.resolve().as_uri()}" alt="Verdansk at launch." />
            </div>
          </body>
        </html>
        """
        path = source_dir / "events.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="events")

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

    def test_single_url_still_runs_as_overview_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_url = self._write_source_bundle(repo_root)
            result = enrich_game_from_wiki("call_of_duty", wiki_url, repo_root=repo_root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["source_count"], 1)
            self.assertEqual(result["sources"], [{"url": wiki_url, "role": "overview"}])

    def test_plain_local_path_sources_are_resolved_to_file_uris(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source_dir = repo_root / "source_local"
            source_dir.mkdir(parents=True, exist_ok=True)
            html_path = source_dir / "operators.html"
            html_path.write_text(
                """
                <html><body><div class="mw-parser-output">
                  <h2>Roster</h2>
                  <ul><li>Operator Alpha</li></ul>
                </div></body></html>
                """,
                encoding="utf-8",
            )

            result = enrich_game_from_sources(
                "call_of_duty",
                [WikiSource(url=str(html_path), role="operators")],
                repo_root=repo_root,
            )

            self.assertTrue(result["sources"][0]["url"].startswith("file://"))

    def test_adapter_parses_entities_abilities_and_medals(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_role_specific_sources(repo_root)
            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            abilities = load_yaml_file(draft_root / "abilities.draft.yaml")
            events = load_yaml_file(draft_root / "events.draft.yaml")

            self.assertEqual(len(entities["characters"]), 2)
            self.assertEqual(len(abilities["abilities"]), 1)
            self.assertEqual(len(events["events"]), 2)

    def test_multi_page_ingestion_merges_role_aware_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_multi_page_sources(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            abilities = load_yaml_file(draft_root / "abilities.draft.yaml")
            events = load_yaml_file(draft_root / "events.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Operator Alpha"])
            self.assertEqual([row["display_name"] for row in abilities["abilities"]], ["Flash Grenade"])
            self.assertEqual([row["display_name"] for row in events["events"]], ["Triple Kill"])
            self.assertEqual(result["source_count"], 4)
            self.assertEqual(len(manifest["assets"]), 2)

    def test_provenance_is_retained_on_normalized_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_multi_page_sources(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            assets = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))["assets"]
            events_csv = (draft_root / "catalog" / "events_or_medals.csv").read_text(encoding="utf-8")

            self.assertTrue(all("source_role" in row for row in assets))
            self.assertTrue(all("source_title" in row for row in assets))
            self.assertIn("source_role", events_csv.splitlines()[0])
            self.assertIn("source_title", events_csv.splitlines()[0])

    def test_missing_optional_image_fields_stay_in_qa_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_role_specific_sources(repo_root, with_direct_images=False)
            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))
            self.assertTrue(manifest["qa_queue"])
            self.assertTrue(all(row["qa_status"] == "needs_manual_crop" for row in manifest["qa_queue"]))

    def test_catalog_csv_contains_required_tables_and_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_role_specific_sources(repo_root)
            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
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
            sources = self._write_role_specific_sources(repo_root)
            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            downloaded = list((draft_root / "downloads").rglob("*.png"))
            templated = list((draft_root / "templates").rglob("*.png"))
            self.assertGreaterEqual(len(downloaded), 3)
            self.assertGreaterEqual(len(templated), 3)

    def test_fandom_article_parser_scopes_to_article_and_filters_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_url = self._write_fandom_like_bundle(repo_root)

            result = enrich_game_from_sources(
                "call_of_duty",
                [WikiSource(url=wiki_url, role="events")],
                repo_root=repo_root,
            )
            draft_root = Path(result["draft_root"])
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))
            events = load_yaml_file(draft_root / "events.draft.yaml")
            entities = load_yaml_file(draft_root / "entities.draft.yaml")

            self.assertEqual([row["display_name"] for row in manifest["assets"]], ["Triple Kill"])
            self.assertEqual([row["display_name"] for row in events["events"]], ["Triple Kill"])
            self.assertEqual(entities["characters"], [])
            self.assertFalse(any("Call of Duty Wiki" in row["display_name"] for row in manifest["assets"]))
            self.assertFalse(any(row["source_url"].startswith("data:") for row in manifest["assets"]))

    def test_category_pages_emit_role_specific_rows_without_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_category_page_sources(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            abilities = load_yaml_file(draft_root / "abilities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))
            fetch_log = (draft_root / "catalog" / "source_fetch_log.csv").read_text(encoding="utf-8")

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Farah", "Ghost"])
            self.assertEqual([row["display_name"] for row in abilities["abilities"]], ["Flash Grenade", "Smoke Grenade"])
            self.assertEqual(manifest["assets"], [])
            self.assertIn("page_type", fetch_log.splitlines()[0])
            self.assertNotIn("Trending Pages", (draft_root / "catalog" / "entities.csv").read_text(encoding="utf-8"))

    def test_category_member_thumbnails_bind_assets_to_existing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_category_page_sources_with_thumbnails(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in manifest["assets"]], ["Ghost", "Flash Grenade"])
            self.assertEqual([row["entity_id"] for row in manifest["assets"]], ["call_of_duty.ghost", "call_of_duty.flash_grenade"])
            self.assertEqual([row["asset_family"] for row in manifest["assets"]], ["hero_portrait", "equipment_icon"])

    def test_events_role_only_emits_event_section_rows_and_icon_like_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_events_role_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            events = load_yaml_file(draft_root / "events.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))
            events_csv = (draft_root / "catalog" / "events_or_medals.csv").read_text(encoding="utf-8")

            self.assertEqual([row["display_name"] for row in events["events"]], ["Fire Sale", "Jailbreak"])
            self.assertEqual([row["display_name"] for row in manifest["assets"]], ["Fire Sale"])
            self.assertNotIn("Bounty contract", events_csv)
            self.assertNotIn("hidden code", events_csv)
            self.assertFalse(any("Verdansk" in row["display_name"] for row in manifest["assets"]))

    def test_unknown_sections_do_not_default_to_events(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source_dir = repo_root / "source"
            source_dir.mkdir(parents=True, exist_ok=True)
            html_path = source_dir / "unknown.html"
            html_path.write_text(
                """
                <html>
                  <body>
                    <div class="mw-parser-output">
                      <h2>Overview</h2>
                      <ul><li>Should Be Ignored</li></ul>
                    </div>
                  </body>
                </html>
                """,
                encoding="utf-8",
            )

            result = enrich_game_from_wiki("call_of_duty", html_path.resolve().as_uri(), repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(result["counts"]["events_or_medals"], 0)
            self.assertEqual(manifest["assets"], [])

    def test_cli_routes_to_multi_source_enrichment(self) -> None:
        with patch(
            "run.run_enrich_game_from_wiki",
            return_value={"ok": True},
        ) as mocked:
            with patch.object(
                sys,
                "argv",
                [
                    "run.py",
                    "--enrich-game-from-wiki",
                    "call_of_duty",
                    "--wiki-source",
                    "operators",
                    "https://example.com/operators",
                    "--wiki-source",
                    "events",
                    "https://example.com/events",
                ],
            ):
                self.assertEqual(run_main(), 0)
        mocked.assert_called_once_with(
            "call_of_duty",
            None,
            wiki_manifest=None,
            wiki_sources=[["operators", "https://example.com/operators"], ["events", "https://example.com/events"]],
        )

    def test_http_fetch_target_uses_browser_like_headers(self) -> None:
        request = _build_fetch_target("https://example.com/wiki")

        self.assertEqual(request.get_header("User-agent"), "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        self.assertEqual(request.get_header("Accept"), "text/html,application/xhtml+xml")
        self.assertEqual(request.get_header("Accept-language"), "en-US,en;q=0.9")
        self.assertEqual(request.get_header("Referer"), "https://example.com/")
        self.assertEqual(request.get_header("Cache-control"), "no-cache")
        self.assertEqual(request.get_header("Pragma"), "no-cache")
        self.assertEqual(request.get_header("Upgrade-insecure-requests"), "1")

    def test_file_sources_keep_existing_fetch_path(self) -> None:
        self.assertTrue(_build_fetch_target("file:///tmp/wiki.html").startswith("file://"))

    def test_cli_reports_structured_fetch_failure(self) -> None:
        with patch(
            "pipeline.wiki_enrichment.urlopen",
            side_effect=HTTPError(
                url="https://example.com/wiki",
                code=403,
                msg="Forbidden",
                hdrs=None,
                fp=None,
            ),
        ):
            with patch.object(
                sys,
                "argv",
                [
                    "run.py",
                    "--enrich-game-from-wiki",
                    "call_of_duty",
                    "--wiki-url",
                    "https://example.com/wiki",
                ],
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = run_main()

        self.assertEqual(exit_code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fetch_failed")
        self.assertEqual(payload["wiki_url"], "https://example.com/wiki")
        self.assertEqual(payload["http_status"], 403)
        self.assertIn("browser-like headers", payload["hint"])
        self.assertIn("Save the page locally", payload["hint"])

    def test_atomic_failed_run_leaves_no_final_draft_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            final_slug = "20260101T000000Z"
            drafts_root = repo_root / "assets" / "games" / "call_of_duty" / "drafts" / "wiki"

            with patch("pipeline.wiki_enrichment._timestamp_slug", return_value=final_slug):
                with patch(
                    "pipeline.wiki_enrichment.urlopen",
                    side_effect=HTTPError(
                        url="https://example.com/wiki",
                        code=403,
                        msg="Forbidden",
                        hdrs=None,
                        fp=None,
                    ),
                ):
                    with self.assertRaises(Exception):
                        enrich_game_from_sources(
                            "call_of_duty",
                            [WikiSource(url="https://example.com/wiki", role="overview")],
                            repo_root=repo_root,
                        )

            self.assertFalse((drafts_root / final_slug).exists())
            if drafts_root.exists():
                self.assertEqual(list(drafts_root.iterdir()), [])

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
        mocked.assert_called_once_with("call_of_duty", "https://example.com/cod", wiki_manifest=None, wiki_sources=None)

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
