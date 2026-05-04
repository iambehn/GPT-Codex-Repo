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
from pipeline.wiki_enrichment import WikiFetchError, WikiSource, _build_fetch_target, enrich_game_from_sources, enrich_game_from_wiki
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

    def _write_card_grid_page_sources(self, root: Path) -> list[WikiSource]:
        source_dir = root / "card_grid_sources"
        source_dir.mkdir(parents=True, exist_ok=True)
        operator_image = source_dir / "ghost.png"
        equipment_image = source_dir / "flash_grenade.png"
        event_image = source_dir / "triple_kill.png"
        operator_image.write_bytes(_ONE_BY_ONE_PNG)
        equipment_image.write_bytes(_ONE_BY_ONE_PNG)
        event_image.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html>
          <body>
            <section class="card-grid">
              <article class="item-card">
                <img src="{operator_image.resolve().as_uri()}" alt="Ghost portrait" />
                <a href="/wiki/Ghost">Ghost</a>
              </article>
            </section>
          </body>
        </html>
        """
        equipment_html = f"""
        <html>
          <body>
            <div class="item-grid">
              <div class="grid-card">
                <img src="{equipment_image.resolve().as_uri()}" alt="Flash Grenade icon" />
                <a href="/wiki/Flash_Grenade">Flash Grenade</a>
              </div>
            </div>
          </body>
        </html>
        """
        events_html = f"""
        <html>
          <body>
            <section class="roster-grid">
              <article class="member-card">
                <img src="{event_image.resolve().as_uri()}" alt="Triple Kill medal" />
                <a href="/wiki/Triple_Kill">Triple Kill</a>
              </article>
            </section>
          </body>
        </html>
        """
        operators_path = source_dir / "grid_operators.html"
        equipment_path = source_dir / "grid_equipment.html"
        events_path = source_dir / "grid_events.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")
        return [
            WikiSource(url=operators_path.resolve().as_uri(), role="operators"),
            WikiSource(url=equipment_path.resolve().as_uri(), role="equipment"),
            WikiSource(url=events_path.resolve().as_uri(), role="events"),
        ]

    def _write_gallery_card_label_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "gallery_card_label"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <body>
            <section class="gallery-grid">
              <article class="gallery-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="card-label">Ghost</span>
              </article>
              <article class="gallery-card">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <figcaption class="gallerytext">Soap</figcaption>
              </article>
            </section>
          </body>
        </html>
        """
        path = source_dir / "gallery_card_label.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_directory_listing_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "directory_listing"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
          </body>
        </html>
        """
        path = source_dir / "directory_listing.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_directory_detail_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "directory_detail_listing"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost is a support operator (Simon Riley).</p>
              <p>Price is a veteran operator who coordinates assaults.</p>
            </section>
          </body>
        </html>
        """
        path = source_dir / "directory_detail_listing.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_directory_detail_ambiguous_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "directory_detail_ambiguous"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost and Soap are support operators known for coordinated assaults.</p>
            </section>
          </body>
        </html>
        """
        path = source_dir / "directory_detail_ambiguous.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_directory_detail_conflicting_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "directory_detail_conflicting"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost is a support operator (Simon Riley).</p>
              <p>Ghost is a tank operator (Spectre).</p>
            </section>
          </body>
        </html>
        """
        path = source_dir / "directory_detail_conflicting.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_directory_detail_existing_conflict_sources(self, root: Path) -> list[WikiSource]:
        source_dir = root / "directory_detail_existing_conflict"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        established_html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost is a tank operator (Spectre).</p>
            </section>
          </body>
        </html>
        """
        detail_html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost is a support operator (Simon Riley).</p>
            </section>
          </body>
        </html>
        """
        roster_path = source_dir / "operators_established.html"
        detail_path = source_dir / "operators_directory_detail_existing_conflict.html"
        roster_path.write_text(established_html, encoding="utf-8")
        detail_path.write_text(detail_html, encoding="utf-8")
        return [
            WikiSource(url=roster_path.resolve().as_uri(), role="operators"),
            WikiSource(url=detail_path.resolve().as_uri(), role="operators"),
        ]

    def _write_directory_detail_canonical_alias_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "directory_detail_canonical_alias"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost is a support operator (ghost).</p>
            </section>
          </body>
        </html>
        """
        path = source_dir / "directory_detail_canonical_alias.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_directory_detail_existing_alias_sources(self, root: Path) -> list[WikiSource]:
        source_dir = root / "directory_detail_existing_alias"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_image = source_dir / "ghost.png"
        soap_image = source_dir / "soap.png"
        ghost_image.write_bytes(_ONE_BY_ONE_PNG)
        soap_image.write_bytes(_ONE_BY_ONE_PNG)
        established_html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost is a tank operator (Spectre).</p>
            </section>
          </body>
        </html>
        """
        detail_html = f"""
        <html>
          <body>
            <div class="directory-grid">
              <div class="directory-card">
                <img src="{ghost_image.resolve().as_uri()}" alt="operator portrait" />
                <p class="item-name">Ghost</p>
              </div>
              <div class="directory-item">
                <img src="{soap_image.resolve().as_uri()}" alt="operator portrait" />
                <span class="member-name">Soap</span>
              </div>
            </div>
            <section class="details">
              <p>Ghost is a tank operator (spectre).</p>
            </section>
          </body>
        </html>
        """
        established_path = source_dir / "operators_established_alias.html"
        detail_path = source_dir / "operators_directory_detail_existing_alias.html"
        established_path.write_text(established_html, encoding="utf-8")
        detail_path.write_text(detail_html, encoding="utf-8")
        return [
            WikiSource(url=established_path.resolve().as_uri(), role="operators"),
            WikiSource(url=detail_path.resolve().as_uri(), role="operators"),
        ]

    def _write_table_hybrid_page_sources(self, root: Path) -> list[WikiSource]:
        source_dir = root / "table_hybrid_sources"
        source_dir.mkdir(parents=True, exist_ok=True)
        operator_image = source_dir / "ghost.png"
        equipment_image = source_dir / "flash_grenade.png"
        event_image = source_dir / "triple_kill.png"
        for path in (operator_image, equipment_image, event_image):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <table class="wikitable item-table">
            <tr><th>Portrait</th><th>Name</th><th>Role</th></tr>
            <tr>
              <td><img src="{operator_image.resolve().as_uri()}" alt="Ghost operator portrait" /></td>
              <td><a href="/wiki/Ghost">Ghost</a></td>
              <td>Stealth</td>
            </tr>
          </table>
        </body></html>
        """
        equipment_html = f"""
        <html><body>
          <table class="wikitable items-table">
            <tr><th>Icon</th><th>Name</th></tr>
            <tr>
              <td><img src="{equipment_image.resolve().as_uri()}" alt="Flash Grenade icon" /></td>
              <td><a href="/wiki/Flash_Grenade">Flash Grenade</a></td>
            </tr>
          </table>
        </body></html>
        """
        events_html = f"""
        <html><body>
          <table class="wikitable roster-table">
            <tr><th>Badge</th><th>Name</th></tr>
            <tr>
              <td><img src="{event_image.resolve().as_uri()}" alt="Triple Kill medal" /></td>
              <td><a href="/wiki/Triple_Kill">Triple Kill</a></td>
            </tr>
          </table>
        </body></html>
        """
        operators_path = source_dir / "table_operators.html"
        equipment_path = source_dir / "table_equipment.html"
        events_path = source_dir / "table_events.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")
        return [
            WikiSource(url=operators_path.resolve().as_uri(), role="operators"),
            WikiSource(url=equipment_path.resolve().as_uri(), role="equipment"),
            WikiSource(url=events_path.resolve().as_uri(), role="events"),
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

    def _write_infobox_heavy_fixture(self, root: Path, *, infobox_only: bool = False) -> WikiSource:
        source_dir = root / "infobox_heavy"
        source_dir.mkdir(parents=True, exist_ok=True)
        infobox_png = source_dir / "ghost_infobox.png"
        body_png = source_dir / "ghost_body.png"
        infobox_png.write_bytes(_ONE_BY_ONE_PNG)
        body_png.write_bytes(_ONE_BY_ONE_PNG)
        body_block = ""
        if not infobox_only:
            body_block = f"""
              <h2>Roster</h2>
              <ul><li>Ghost</li></ul>
              <img src="{body_png.resolve().as_uri()}" alt="Ghost portrait" />
            """
        html = f"""
        <html>
          <head><title>Ghost</title></head>
          <body>
            <aside class="portable-infobox">
              <figure class="pi-image">
                <img src="{infobox_png.resolve().as_uri()}" alt="Ghost portrait" />
              </figure>
              <div class="pi-data">Ghost</div>
            </aside>
            <div class="mw-parser-output">
              {body_block}
            </div>
          </body>
        </html>
        """
        path = source_dir / ("ghost_infobox_only.html" if infobox_only else "ghost_infobox_with_body.html")
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_gallery_heavy_fixture(self, root: Path, *, gallery_only: bool = False) -> WikiSource:
        source_dir = root / "gallery_heavy"
        source_dir.mkdir(parents=True, exist_ok=True)
        gallery_png = source_dir / "ghost_gallery.png"
        body_png = source_dir / "ghost_body.png"
        gallery_png.write_bytes(_ONE_BY_ONE_PNG)
        body_png.write_bytes(_ONE_BY_ONE_PNG)
        body_block = ""
        if not gallery_only:
            body_block = f"""
              <h2>Roster</h2>
              <ul><li>Ghost</li></ul>
              <img src="{body_png.resolve().as_uri()}" alt="Ghost portrait" />
            """
        html = f"""
        <html>
          <head><title>Ghost Gallery</title></head>
          <body>
            <div class="mw-parser-output">
              {body_block}
              <div class="gallery">
                <figure class="gallerybox">
                  <img src="{gallery_png.resolve().as_uri()}" alt="" />
                  <figcaption>Ghost portrait</figcaption>
                </figure>
              </div>
            </div>
          </body>
        </html>
        """
        path = source_dir / ("ghost_gallery_only.html" if gallery_only else "ghost_gallery_with_body.html")
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_plain_figure_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "plain_figure"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_png = source_dir / "ghost.png"
        soap_png = source_dir / "soap.png"
        ghost_png.write_bytes(_ONE_BY_ONE_PNG)
        soap_png.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Operators</title></head>
          <body>
            <h2>Operators</h2>
            <figure>
              <img src="{ghost_png.resolve().as_uri()}" alt="Ghost portrait" />
              <figcaption>Ghost portrait</figcaption>
            </figure>
            <img src="{soap_png.resolve().as_uri()}" alt="Soap portrait" />
          </body>
        </html>
        """
        path = source_dir / "plain_figure.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_plain_paragraph_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "plain_paragraph"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_png = source_dir / "ghost.png"
        soap_png = source_dir / "soap.png"
        ghost_png.write_bytes(_ONE_BY_ONE_PNG)
        soap_png.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Operators</title></head>
          <body>
            <h2>Operators</h2>
            <img src="{ghost_png.resolve().as_uri()}" alt="" />
            <p>Ghost is a stealth-focused operator who excels at flanking.</p>
            <img src="{soap_png.resolve().as_uri()}" alt="" />
            <p>This veteran operator uses assault rifles and adapts quickly in combat.</p>
          </body>
        </html>
        """
        path = source_dir / "plain_paragraph.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_ambiguous_plain_paragraph_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "ambiguous_plain_paragraph"
        source_dir.mkdir(parents=True, exist_ok=True)
        image_path = source_dir / "operators.png"
        image_path.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Operators</title></head>
          <body>
            <h2>Operators</h2>
            <img src="{image_path.resolve().as_uri()}" alt="" />
            <p>Ghost is a stealth-focused operator, and Soap is a front-line breacher.</p>
          </body>
        </html>
        """
        path = source_dir / "ambiguous_plain_paragraph.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_cross_paragraph_ambiguous_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "cross_paragraph_ambiguous"
        source_dir.mkdir(parents=True, exist_ok=True)
        image_path = source_dir / "operators.png"
        image_path.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Operators</title></head>
          <body>
            <h2>Operators</h2>
            <img src="{image_path.resolve().as_uri()}" alt="" />
            <p>Ghost is a stealth-focused operator.</p>
            <p>Soap is a front-line breacher.</p>
          </body>
        </html>
        """
        path = source_dir / "cross_paragraph_ambiguous.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_surrounding_paragraph_ambiguous_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "surrounding_paragraph_ambiguous"
        source_dir.mkdir(parents=True, exist_ok=True)
        image_path = source_dir / "operators.png"
        image_path.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Operators</title></head>
          <body>
            <h2>Operators</h2>
            <p>Ghost is a stealth-focused operator.</p>
            <img src="{image_path.resolve().as_uri()}" alt="" />
            <p>Soap is a front-line breacher.</p>
          </body>
        </html>
        """
        path = source_dir / "surrounding_paragraph_ambiguous.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_referential_paragraph_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "referential_paragraph"
        source_dir.mkdir(parents=True, exist_ok=True)
        image_path = source_dir / "operators.png"
        image_path.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Operators</title></head>
          <body>
            <h2>Operators</h2>
            <img src="{image_path.resolve().as_uri()}" alt="" />
            <p>This veteran operator excels at flanking and stealth.</p>
          </body>
        </html>
        """
        path = source_dir / "referential_paragraph.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_mixed_gallery_paragraph_fixture(self, root: Path) -> WikiSource:
        source_dir = root / "mixed_gallery_paragraph"
        source_dir.mkdir(parents=True, exist_ok=True)
        ghost_png = source_dir / "ghost.png"
        soap_png = source_dir / "soap.png"
        ghost_png.write_bytes(_ONE_BY_ONE_PNG)
        soap_png.write_bytes(_ONE_BY_ONE_PNG)
        html = f"""
        <html>
          <head><title>Operators</title></head>
          <body>
            <div class="gallery">
              <img src="{ghost_png.resolve().as_uri()}" alt="" />
              <p>Ghost is a stealth-focused operator.</p>
              <img src="{soap_png.resolve().as_uri()}" alt="" />
              <p>This veteran operator adapts quickly in combat.</p>
            </div>
          </body>
        </html>
        """
        path = source_dir / "mixed_gallery_paragraph.html"
        path.write_text(html, encoding="utf-8")
        return WikiSource(url=path.resolve().as_uri(), role="operators")

    def _write_structured_role_sources(self, root: Path) -> list[WikiSource]:
        source_dir = root / "structured_roles"
        source_dir.mkdir(parents=True, exist_ok=True)
        hero_path = source_dir / "psylocke.png"
        ability_path = source_dir / "psychic_katana.png"
        medal_path = source_dir / "payload_savior.png"
        for path in (hero_path, ability_path, medal_path):
            path.write_bytes(_ONE_BY_ONE_PNG)

        operators_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Duelists</h2>
          <ul><li>Psylocke (Sai)</li></ul>
          <img src="{hero_path.resolve().as_uri()}" alt="Psylocke portrait" />
        </div></body></html>
        """
        equipment_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Ultimate Abilities</h2>
          <ul><li>Psychic Katana</li></ul>
          <img src="{ability_path.resolve().as_uri()}" alt="Psychic Katana icon" />
        </div></body></html>
        """
        events_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Objective Events</h2>
          <ul><li>Payload Savior</li></ul>
          <img src="{medal_path.resolve().as_uri()}" alt="Payload Savior medal" />
        </div></body></html>
        """
        operators_path = source_dir / "operators.html"
        equipment_path = source_dir / "equipment.html"
        events_path = source_dir / "events.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")
        return [
            WikiSource(url=operators_path.resolve().as_uri(), role="operators"),
            WikiSource(url=equipment_path.resolve().as_uri(), role="equipment"),
            WikiSource(url=events_path.resolve().as_uri(), role="events"),
        ]

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

    def test_structured_fields_are_written_into_draft_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_structured_role_sources(repo_root)

            result = enrich_game_from_sources("marvel_rivals", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            abilities = load_yaml_file(draft_root / "abilities.draft.yaml")
            events = load_yaml_file(draft_root / "events.draft.yaml")
            entities_csv = (draft_root / "catalog" / "entities.csv").read_text(encoding="utf-8")
            abilities_csv = (draft_root / "catalog" / "abilities_or_equipment.csv").read_text(encoding="utf-8")
            events_csv = (draft_root / "catalog" / "events_or_medals.csv").read_text(encoding="utf-8")

            self.assertEqual(entities["characters"][0]["role"], "duelist")
            self.assertEqual(entities["characters"][0]["aliases"], ["Sai"])
            self.assertEqual(abilities["abilities"][0]["class"], "ultimate")
            self.assertEqual(events["events"][0]["category"], "objective")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))
            self.assertIn("candidate_quality", manifest["assets"][0])
            self.assertIn("quality_score", manifest["assets"][0])
            self.assertIn("role_source", entities_csv.splitlines()[0])
            self.assertIn("class_source", abilities_csv.splitlines()[0])
            self.assertIn("category_source", events_csv.splitlines()[0])

    def test_image_heavy_pages_use_caption_anchors_and_flag_filename_only_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source_dir = repo_root / "image_heavy"
            source_dir.mkdir(parents=True, exist_ok=True)
            caption_png = source_dir / "mystery_operator.png"
            filename_png = source_dir / "payload_savior.png"
            caption_png.write_bytes(_ONE_BY_ONE_PNG)
            filename_png.write_bytes(_ONE_BY_ONE_PNG)
            operators_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Roster</h2>
              <figure>
                <img src="{caption_png.resolve().as_uri()}" alt="operator portrait" />
                <figcaption>Operator Alpha</figcaption>
              </figure>
            </div></body></html>
            """
            events_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Events</h2>
              <img src="{filename_png.resolve().as_uri()}" alt="" />
            </div></body></html>
            """
            operators_path = source_dir / "operators.html"
            events_path = source_dir / "events.html"
            operators_path.write_text(operators_html, encoding="utf-8")
            events_path.write_text(events_html, encoding="utf-8")

            result = enrich_game_from_sources(
                "call_of_duty",
                [
                    WikiSource(url=operators_path.resolve().as_uri(), role="operators"),
                    WikiSource(url=events_path.resolve().as_uri(), role="events"),
                ],
                repo_root=repo_root,
            )
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Operator Alpha"])
            self.assertTrue(any(row["qa_status"] == "filename_only_anchor" for row in manifest["qa_queue"]))

    def test_infobox_images_are_assets_only_and_lower_trust_than_body_images(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_infobox_heavy_fixture(repo_root, infobox_only=False)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual(len(manifest["assets"]), 2)
            source_kinds = {row["source_kind"] for row in manifest["assets"]}
            self.assertEqual(source_kinds, {"page_image", "infobox_image"})
            infobox_rows = [row for row in manifest["assets"] if row["source_kind"] == "infobox_image"]
            self.assertEqual(len(infobox_rows), 1)

    def test_infobox_only_pages_keep_reviewable_candidates_and_emit_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_infobox_heavy_fixture(repo_root, infobox_only=True)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(entities["characters"], [])
            self.assertEqual(len(manifest["assets"]), 1)
            self.assertEqual(manifest["assets"][0]["source_kind"], "infobox_image")
            self.assertTrue(any(row["qa_status"] == "infobox_only_candidate" for row in manifest["qa_queue"]))

    def test_gallery_images_stay_candidates_and_body_images_rank_above_them(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_gallery_heavy_fixture(repo_root, gallery_only=False)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual({row["source_kind"] for row in manifest["assets"]}, {"page_image", "gallery_image"})
            gallery_rows = [row for row in manifest["assets"] if row["source_kind"] == "gallery_image"]
            self.assertEqual(len(gallery_rows), 1)

    def test_gallery_only_pages_keep_reviewable_candidates_and_emit_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_gallery_heavy_fixture(repo_root, gallery_only=True)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual(len(manifest["assets"]), 1)
            self.assertEqual(manifest["assets"][0]["source_kind"], "gallery_image")
            self.assertTrue(any(row["qa_status"] == "gallery_only_candidate" for row in manifest["qa_queue"]))

    def test_plain_figure_layout_uses_captioned_figure_for_rows_and_keeps_bare_image_as_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_plain_figure_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual({row["display_name"] for row in manifest["assets"]}, {"Ghost", "Soap"})

    def test_plain_paragraph_layout_uses_explicit_adjacent_paragraph_only(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_plain_paragraph_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual({row["display_name"] for row in manifest["assets"]}, {"Ghost", "soap"})

    def test_ambiguous_plain_paragraph_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_ambiguous_plain_paragraph_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(entities["characters"], [])
            self.assertEqual(len(manifest["assets"]), 1)
            self.assertTrue(any(row["qa_status"] == "ambiguous_paragraph_anchor" for row in manifest["qa_queue"]))

    def test_cross_paragraph_ambiguous_anchor_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_cross_paragraph_ambiguous_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(entities["characters"], [])
            self.assertEqual(len(manifest["assets"]), 1)
            self.assertTrue(any(row["qa_status"] == "cross_paragraph_ambiguous_anchor" for row in manifest["qa_queue"]))

    def test_surrounding_paragraph_ambiguous_anchor_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_surrounding_paragraph_ambiguous_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(entities["characters"], [])
            self.assertEqual(len(manifest["assets"]), 1)
            self.assertTrue(any(row["qa_status"] == "surrounding_paragraph_ambiguous_anchor" for row in manifest["qa_queue"]))

    def test_referential_paragraph_anchor_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_referential_paragraph_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(entities["characters"], [])
            self.assertEqual(len(manifest["assets"]), 1)
            self.assertTrue(any(row["qa_status"] == "referential_paragraph_anchor" for row in manifest["qa_queue"]))

    def test_mixed_gallery_paragraph_promotes_only_explicit_gallery_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_mixed_gallery_paragraph_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual({row["display_name"] for row in manifest["assets"]}, {"Ghost", "soap"})
            self.assertTrue(any(row["qa_status"] == "referential_paragraph_anchor" for row in manifest["qa_queue"]))

    def test_gallery_card_label_pages_bind_assets_to_category_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_gallery_card_label_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost", "Soap"])
            self.assertEqual({row["display_name"] for row in manifest["assets"]}, {"Ghost", "Soap"})
            self.assertEqual({row["source_kind"] for row in manifest["assets"]}, {"category_member_image"})

    def test_directory_listing_pages_bind_assets_to_category_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_directory_listing_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost", "Soap"])
            self.assertEqual({row["display_name"] for row in manifest["assets"]}, {"Ghost", "Soap"})
            self.assertEqual({row["source_kind"] for row in manifest["assets"]}, {"category_member_image"})

    def test_directory_detail_pages_keep_listing_rows_primary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_directory_detail_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost", "Soap"])
            self.assertEqual({row["display_name"] for row in manifest["assets"]}, {"Ghost", "Soap"})
            ghost_row = next(row for row in entities["characters"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row["role"], "support")
            self.assertEqual(ghost_row["aliases"], ["Simon Riley"])
            self.assertFalse(any(row["display_name"] == "Price" for row in entities["characters"]))

    def test_directory_detail_ambiguous_pages_emit_qa_and_skip_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_directory_detail_ambiguous_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost", "Soap"])
            ghost_row = next(row for row in entities["characters"] if row["display_name"] == "Ghost")
            soap_row = next(row for row in entities["characters"] if row["display_name"] == "Soap")
            self.assertEqual(ghost_row.get("role", ""), "")
            self.assertEqual(soap_row.get("role", ""), "")
            self.assertTrue(any(row["qa_status"] == "ambiguous_listing_detail_enrichment" for row in manifest["qa_queue"]))

    def test_directory_detail_conflicting_pages_emit_qa_and_skip_conflicting_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_directory_detail_conflicting_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            ghost_row = next(row for row in entities["characters"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "")
            self.assertEqual(ghost_row.get("aliases", []), [])
            self.assertTrue(
                any(
                    row["qa_status"] == "conflicting_listing_detail_enrichment"
                    and row.get("display_name", "") == "Ghost"
                    and row.get("field", "") == "role"
                    for row in manifest["qa_queue"]
                )
            )

    def test_directory_detail_existing_row_conflict_emits_qa_and_keeps_existing_value(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_directory_detail_existing_conflict_sources(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            ghost_row = next(row for row in entities["characters"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "tank")
            self.assertEqual(ghost_row.get("aliases", []), ["Spectre"])
            self.assertTrue(
                any(
                    row["qa_status"] == "existing_listing_detail_enrichment_conflict"
                    and row.get("display_name", "") == "Ghost"
                    and row.get("field", "") == "role"
                    and row.get("existing_value", "") == "tank"
                    for row in manifest["qa_queue"]
                )
            )

    def test_directory_detail_canonical_alias_is_rejected_with_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_directory_detail_canonical_alias_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            ghost_row = next(row for row in entities["characters"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "support")
            self.assertEqual(ghost_row.get("aliases", []), [])
            self.assertTrue(
                any(
                    row["qa_status"] == "alias_equivalent_to_canonical_name"
                    and row.get("display_name", "") == "Ghost"
                    and row.get("alias", "") == "ghost"
                    for row in manifest["qa_queue"]
                )
            )

    def test_directory_detail_existing_alias_equivalence_is_rejected_with_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_directory_detail_existing_alias_sources(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            ghost_row = next(row for row in entities["characters"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "tank")
            self.assertEqual(ghost_row.get("aliases", []), ["Spectre"])
            self.assertTrue(
                any(
                    row["qa_status"] == "alias_equivalent_to_existing_alias"
                    and row.get("display_name", "") == "Ghost"
                    and row.get("alias", "") == "spectre"
                    for row in manifest["qa_queue"]
                )
            )

    def test_directory_detail_conflict_suppresses_aliases_with_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source = self._write_directory_detail_conflicting_fixture(repo_root)

            result = enrich_game_from_sources("call_of_duty", [source], repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertTrue(
                any(
                    row["qa_status"] == "alias_suppressed_by_detail_conflict"
                    and row.get("display_name", "") == "Ghost"
                    and row.get("alias", "") in {"Simon Riley", "Spectre"}
                    for row in manifest["qa_queue"]
                )
            )

    def test_generic_source_merge_rejects_alias_equivalent_to_canonical_name(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source_dir = repo_root / "generic_alias_canonical"
            source_dir.mkdir(parents=True, exist_ok=True)
            html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Soap (soap)</li></ul>
            </div></body></html>
            """
            path = source_dir / "operators.html"
            path.write_text(html, encoding="utf-8")

            result = enrich_game_from_sources(
                "call_of_duty",
                [WikiSource(url=path.resolve().as_uri(), role="operators")],
                repo_root=repo_root,
            )
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            soap_row = next(row for row in entities["characters"] if row["display_name"] == "Soap")
            self.assertEqual(soap_row.get("aliases", []), [])
            self.assertTrue(
                any(
                    row["qa_status"] == "alias_equivalent_to_canonical_name"
                    and row.get("display_name", "") == "Soap"
                    and row.get("alias", "") == "soap"
                    for row in manifest["qa_queue"]
                )
            )

    def test_generic_source_merge_rejects_alias_equivalent_to_existing_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source_dir = repo_root / "generic_alias_existing"
            source_dir.mkdir(parents=True, exist_ok=True)
            first_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Soap (Spectre)</li></ul>
            </div></body></html>
            """
            second_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Soap (spectre)</li></ul>
            </div></body></html>
            """
            first_path = source_dir / "operators_first.html"
            second_path = source_dir / "operators_second.html"
            first_path.write_text(first_html, encoding="utf-8")
            second_path.write_text(second_html, encoding="utf-8")

            result = enrich_game_from_sources(
                "call_of_duty",
                [
                    WikiSource(url=first_path.resolve().as_uri(), role="operators"),
                    WikiSource(url=second_path.resolve().as_uri(), role="operators"),
                ],
                repo_root=repo_root,
            )
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            soap_row = next(row for row in entities["characters"] if row["display_name"] == "Soap")
            self.assertEqual(soap_row.get("aliases", []), ["Spectre"])
            self.assertTrue(
                any(
                    row["qa_status"] == "alias_equivalent_to_existing_alias"
                    and row.get("display_name", "") == "Soap"
                    and row.get("alias", "") == "spectre"
                    for row in manifest["qa_queue"]
                )
            )

    def test_generic_source_merge_converges_safe_identity_without_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source_dir = repo_root / "generic_identity_convergence"
            source_dir.mkdir(parents=True, exist_ok=True)
            first_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Ghost (Simon Riley)</li></ul>
            </div></body></html>
            """
            second_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Simon Riley</li></ul>
            </div></body></html>
            """
            first_path = source_dir / "first.html"
            second_path = source_dir / "second.html"
            first_path.write_text(first_html, encoding="utf-8")
            second_path.write_text(second_html, encoding="utf-8")

            result = enrich_game_from_sources(
                "call_of_duty",
                [
                    WikiSource(url=first_path.resolve().as_uri(), role="operators"),
                    WikiSource(url=second_path.resolve().as_uri(), role="operators"),
                ],
                repo_root=repo_root,
            )
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([item["display_name"] for item in entities["characters"]], ["Ghost"])
            self.assertFalse(any(item["qa_status"] == "conflicting_identity_match" for item in manifest["qa_queue"]))

    def test_generic_source_merge_reports_conflicting_identity_match(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            source_dir = repo_root / "generic_identity_conflict"
            source_dir.mkdir(parents=True, exist_ok=True)
            first_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Ghost (Spectre)</li></ul>
            </div></body></html>
            """
            second_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Reaper (Spectre)</li></ul>
            </div></body></html>
            """
            first_path = source_dir / "first.html"
            second_path = source_dir / "second.html"
            first_path.write_text(first_html, encoding="utf-8")
            second_path.write_text(second_html, encoding="utf-8")

            result = enrich_game_from_sources(
                "call_of_duty",
                [
                    WikiSource(url=first_path.resolve().as_uri(), role="operators"),
                    WikiSource(url=second_path.resolve().as_uri(), role="operators"),
                ],
                repo_root=repo_root,
            )
            draft_root = Path(result["draft_root"])
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertTrue(any(item["qa_status"] == "conflicting_identity_match" for item in manifest["qa_queue"]))

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

    def test_card_grid_pages_bind_assets_to_role_specific_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_card_grid_page_sources(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            abilities = load_yaml_file(draft_root / "abilities.draft.yaml")
            events = load_yaml_file(draft_root / "events.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual([row["display_name"] for row in abilities["abilities"]], ["Flash Grenade"])
            self.assertEqual([row["display_name"] for row in events["events"]], ["Triple Kill"])
            self.assertEqual([row["display_name"] for row in manifest["assets"]], ["Ghost", "Flash Grenade", "Triple Kill"])

    def test_table_hybrid_pages_bind_assets_to_role_specific_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            sources = self._write_table_hybrid_page_sources(repo_root)

            result = enrich_game_from_sources("call_of_duty", sources, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.draft.yaml")
            abilities = load_yaml_file(draft_root / "abilities.draft.yaml")
            events = load_yaml_file(draft_root / "events.draft.yaml")
            manifest = json.loads((draft_root / "assets_manifest.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in entities["characters"]], ["Ghost"])
            self.assertEqual([row["display_name"] for row in abilities["abilities"]], ["Flash Grenade"])
            self.assertEqual([row["display_name"] for row in events["events"]], ["Triple Kill"])
            self.assertEqual([row["display_name"] for row in manifest["assets"]], ["Ghost", "Flash Grenade", "Triple Kill"])

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
            "pipeline.source_normalization.urlopen",
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
                    "pipeline.source_normalization.urlopen",
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

    def test_mixed_sources_keep_successful_rows_when_one_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            valid_sources = self._write_role_specific_sources(repo_root)
            failing = WikiSource(url="https://example.com/missing", role="events")
            valid_record = {
                "url": valid_sources[0].url,
                "role": "operators",
                "source_scheme": "file",
                "content_type": "text/html",
                "title": "Operators",
                "page_type": "article",
                "sections": [
                    type("Section", (), {"heading": "Operators", "items": ["Ghost"], "images": []})(),
                ],
                "category_items": [],
                "direct_image_url": "",
            }
            with patch(
                "pipeline.wiki_enrichment._fetch_source_record",
                side_effect=[valid_record, WikiFetchError(source_url=failing.url, category="http_error", message="failed to fetch source page: HTTP 404", hint="missing", http_status=404)],
            ):
                result = enrich_game_from_sources("call_of_duty", [valid_sources[0], failing], repo_root=repo_root)

            self.assertTrue(result["ok"])
            fetch_log = (Path(result["catalog_root"]) / "source_fetch_log.csv").read_text(encoding="utf-8")
            self.assertIn("fetch_failed", fetch_log)
