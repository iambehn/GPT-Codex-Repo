from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.onboarding_batch_publish import publish_onboarding_batch
from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from pipeline.asset_candidate_quality import score_binding_candidate
from pipeline.game_onboarding import (
    _build_qa_queue,
    _load_runtime_detection_schema,
    OnboardingSource,
    adapt_game_schema,
    build_onboarding_draft,
    fill_derived_detection_rows,
    ingest_onboarding_sources,
    onboard_game_from_manifest,
    publish_onboarding_draft,
    report_unresolved_derived_rows,
)
from pipeline.derived_row_review import (
    apply_derived_row_review,
    prepare_derived_row_review,
    summarize_derived_row_review,
)
from pipeline.game_pack import load_game_pack


_ONE_BY_ONE_PNG = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\rIDATx\x9cc`\x00\x00\x00\x02\x00\x01\xe2!\xbc3"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


class GameOnboardingTests(unittest.TestCase):
    def _write_marvel_starter_seed(self, root: Path) -> None:
        starter_root = root / "starter_assets" / "marvel_rivals"
        starter_root.mkdir(parents=True, exist_ok=True)
        (starter_root / "game.yaml").write_text(
            "\n".join(
                [
                    "game_id: marvel_rivals",
                    'display_name: "Marvel Rivals"',
                    "genre: hero_shooter",
                    "camera_mode: first_person",
                    "patch_tag: 2026-05",
                    "ui_version: draft",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "game_detection_schema_overrides.yaml").write_text(
            "\n".join(
                [
                    "disabled_families: []",
                    "families:",
                    "  ability_icon:",
                    "    threshold: 0.91",
                    "  equipment_icon:",
                    "    threshold: 0.91",
                    "  medal_icon:",
                    "    temporal_window: 3",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "characters.yaml").write_text(
            "\n".join(
                [
                    "characters:",
                    "  - id: punisher",
                    '    display_name: "The Punisher"',
                    '    aliases: ["punisher"]',
                    "    role: duelist",
                    "  - id: mantis",
                    '    display_name: "Mantis"',
                    '    aliases: ["mantis"]',
                    "    role: strategist",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "abilities.yaml").write_text(
            "\n".join(
                [
                    "abilities:",
                    "  - id: punisher_ult",
                    "    character_id: punisher",
                    '    display_name: "Final Judgment"',
                    '    aliases: ["ult", "ultimate", "final judgment"]',
                    "    class: ultimate",
                ]
            )
            + "\n",
                encoding="utf-8",
            )

    def _write_callofduty_starter_seed(self, root: Path) -> None:
        starter_root = root / "starter_assets" / "call_of_duty"
        starter_root.mkdir(parents=True, exist_ok=True)
        (starter_root / "game.yaml").write_text(
            "\n".join(
                [
                    "game_id: call_of_duty",
                    'display_name: "Call of Duty: Warzone"',
                    "genre: battle_royale",
                    "camera_mode: first_person",
                    "patch_tag: 2026-05",
                    "ui_version: draft",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "game_detection_schema_overrides.yaml").write_text(
            "\n".join(
                [
                    "disabled_families:",
                    "  - ability_icon",
                    "families:",
                    "  equipment_icon:",
                    "    threshold: 0.92",
                    "  hero_portrait:",
                    "    threshold: 0.89",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "characters.yaml").write_text(
            "\n".join(
                [
                    "characters:",
                    "  - id: ghost",
                    '    display_name: "Ghost"',
                    '    aliases: ["simon riley"]',
                    "    role: operator",
                    "  - id: farah",
                    '    display_name: "Farah"',
                    '    aliases: ["farah karim"]',
                    "    role: operator",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "abilities.yaml").write_text(
            "\n".join(
                [
                    "abilities:",
                    "  - id: flash_grenade",
                    '    display_name: "Flash Grenade"',
                    '    aliases: ["flash"]',
                    "    class: equipment",
                    "  - id: cluster_strike",
                    '    display_name: "Cluster Strike"',
                    '    aliases: ["cluster"]',
                    "    class: equipment",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "action_moments.yaml").write_text(
            "\n".join(
                [
                    "moments:",
                    "  - id: triple_kill",
                    '    display_name: "Triple Kill"',
                    '    aliases: ["triple kill"]',
                    "    category: combat",
                    "  - id: longshot",
                    '    display_name: "Longshot"',
                    '    aliases: ["long shot"]',
                    "    category: combat",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    def _write_valorant_starter_seed(self, root: Path) -> None:
        starter_root = root / "starter_assets" / "valorant"
        starter_root.mkdir(parents=True, exist_ok=True)
        (starter_root / "game.yaml").write_text(
            "\n".join(
                [
                    "game_id: valorant",
                    'display_name: "VALORANT"',
                    "genre: tactical_shooter",
                    "camera_mode: first_person",
                    "patch_tag: 2026-05",
                    "ui_version: draft",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "game_detection_schema_overrides.yaml").write_text(
            "\n".join(
                [
                    "disabled_families:",
                    "  - ability_icon",
                    "  - medal_icon",
                    "families:",
                    "  equipment_icon:",
                    "    threshold: 0.92",
                    "  hero_portrait:",
                    "    threshold: 0.89",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "characters.yaml").write_text(
            "\n".join(
                [
                    "characters:",
                    "  - id: jett",
                    '    display_name: "Jett"',
                    '    aliases: ["jett"]',
                    "    role: duelist",
                    "  - id: sage",
                    '    display_name: "Sage"',
                    '    aliases: ["sage"]',
                    "    role: strategist",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "abilities.yaml").write_text(
            "\n".join(
                [
                    "abilities:",
                    "  - id: vandal",
                    '    display_name: "Vandal"',
                    '    aliases: ["vandal"]',
                    "    class: equipment",
                    "  - id: phantom",
                    '    display_name: "Phantom"',
                    '    aliases: ["phantom"]',
                    "    class: equipment",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (starter_root / "action_moments.yaml").write_text(
            "\n".join(
                [
                    "moments:",
                    "  - id: radiant",
                    '    display_name: "Radiant"',
                    '    aliases: ["radiant"]',
                    "    category: outcome",
                    "  - id: immortal",
                    '    display_name: "Immortal"',
                    '    aliases: ["immortal"]',
                    "    category: outcome",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    def _write_sources(self, root: Path, *, plain_local_paths: bool = False) -> Path:
        image_root = root / "images"
        image_root.mkdir(parents=True, exist_ok=True)
        punisher = image_root / "punisher.png"
        mantis = image_root / "mantis.png"
        final_judgment = image_root / "final_judgment.png"
        triple_ko = image_root / "triple_ko.png"
        for path in (punisher, mantis, final_judgment, triple_ko):
            path.write_bytes(_ONE_BY_ONE_PNG)

        roster_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Roster</h2>
          <ul>
            <li>The Punisher</li>
            <li>Mantis</li>
          </ul>
          <img src="{punisher.resolve().as_uri()}" alt="The Punisher portrait" />
          <img src="{mantis.resolve().as_uri()}" alt="Mantis portrait" />
        </div></body></html>
        """
        abilities_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Abilities</h2>
          <ul>
            <li>Final Judgment</li>
          </ul>
          <img src="{final_judgment.resolve().as_uri()}" alt="Final Judgment icon" />
        </div></body></html>
        """
        medals_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Medals</h2>
          <ul>
            <li>Triple KO</li>
          </ul>
          <img src="{triple_ko.resolve().as_uri()}" alt="Triple KO medal" />
        </div></body></html>
        """
        roster_path = root / "roster.html"
        abilities_path = root / "abilities.html"
        medals_path = root / "medals.html"
        roster_path.write_text(roster_html, encoding="utf-8")
        abilities_path.write_text(abilities_html, encoding="utf-8")
        medals_path.write_text(medals_html, encoding="utf-8")

        def _ref(path: Path) -> str:
            return str(path.resolve()) if plain_local_paths else path.resolve().as_uri()

        manifest_path = root / "sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: marvel_rivals",
                    "sources:",
                    f"  - role: roster",
                    f"    url: \"{_ref(roster_path)}\"",
                    '    notes: "internal_review_required"',
                    f"  - role: abilities",
                    f"    url: \"{_ref(abilities_path)}\"",
                    '    notes: "internal_review_required"',
                    f"  - role: medals",
                    f"    url: \"{_ref(medals_path)}\"",
                    '    notes: "internal_review_required"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_sources(self, root: Path, *, plain_local_paths: bool = False) -> Path:
        image_root = root / "cod_images"
        image_root.mkdir(parents=True, exist_ok=True)
        ghost = image_root / "ghost.png"
        farah = image_root / "farah.png"
        flash = image_root / "flash_grenade.png"
        cluster = image_root / "cluster_strike.png"
        triple = image_root / "triple_kill.png"
        for path in (ghost, farah, flash, cluster, triple):
            path.write_bytes(_ONE_BY_ONE_PNG)

        operators_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Operators</h2>
          <ul>
            <li>Ghost</li>
            <li>Farah</li>
          </ul>
          <img src="{ghost.resolve().as_uri()}" alt="Ghost operator portrait" />
          <img src="{farah.resolve().as_uri()}" alt="Farah operator portrait" />
        </div></body></html>
        """
        equipment_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Equipment</h2>
          <ul>
            <li>Flash Grenade</li>
            <li>Cluster Strike</li>
          </ul>
          <img src="{flash.resolve().as_uri()}" alt="Flash Grenade icon" />
          <img src="{cluster.resolve().as_uri()}" alt="Cluster Strike icon" />
        </div></body></html>
        """
        events_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Medals</h2>
          <ul>
            <li>Triple Kill</li>
            <li>Longshot</li>
          </ul>
          <img src="{triple.resolve().as_uri()}" alt="Triple Kill medal" />
        </div></body></html>
        """
        operators_path = root / "operators.html"
        equipment_path = root / "equipment.html"
        events_path = root / "events.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")

        def _ref(path: Path) -> str:
            return str(path.resolve()) if plain_local_paths else path.resolve().as_uri()

        manifest_path = root / "call_of_duty_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{_ref(operators_path)}\"",
                    "  - role: equipment",
                    f"    url: \"{_ref(equipment_path)}\"",
                    "  - role: events",
                    f"    url: \"{_ref(events_path)}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_plain_figure_source(self, root: Path) -> Path:
        image_root = root / "cod_plain_figure_images"
        image_root.mkdir(parents=True, exist_ok=True)
        ghost = image_root / "ghost.png"
        soap = image_root / "soap.png"
        ghost.write_bytes(_ONE_BY_ONE_PNG)
        soap.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <h2>Operators</h2>
          <figure>
            <img src="{ghost.resolve().as_uri()}" alt="Ghost portrait" />
            <figcaption>Ghost portrait</figcaption>
          </figure>
          <img src="{soap.resolve().as_uri()}" alt="Soap portrait" />
        </body></html>
        """
        operators_path = root / "operators_plain_figure.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "plain_figure_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_extra_marvel_ability_source(self, root: Path, *, ability_name: str, image_name: str = "extra_ability.png") -> Path:
        image_root = root / "extra_marvel_images"
        image_root.mkdir(parents=True, exist_ok=True)
        ability_png = image_root / image_name
        ability_png.write_bytes(_ONE_BY_ONE_PNG)
        abilities_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Abilities</h2>
          <ul>
            <li>{ability_name}</li>
          </ul>
          <img src="{ability_png.resolve().as_uri()}" alt="{ability_name} icon" />
        </div></body></html>
        """
        abilities_path = root / "extra_ability.html"
        abilities_path.write_text(abilities_html, encoding="utf-8")
        manifest_path = root / "extra_ability_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: marvel_rivals",
                    "sources:",
                    "  - role: abilities",
                    f'    url: "{abilities_path.resolve().as_uri()}"',
                    '    notes: "internal_review_required"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_plain_paragraph_source(self, root: Path) -> Path:
        image_root = root / "cod_plain_paragraph_images"
        image_root.mkdir(parents=True, exist_ok=True)
        ghost = image_root / "ghost.png"
        soap = image_root / "soap.png"
        ghost.write_bytes(_ONE_BY_ONE_PNG)
        soap.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <h2>Operators</h2>
          <img src="{ghost.resolve().as_uri()}" alt="" />
          <p>Ghost is a stealth-focused operator who excels at flanking.</p>
          <img src="{soap.resolve().as_uri()}" alt="" />
          <p>This veteran operator uses assault rifles and adapts quickly in combat.</p>
        </body></html>
        """
        operators_path = root / "operators_plain_paragraph.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "plain_paragraph_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_ambiguous_plain_paragraph_source(self, root: Path) -> Path:
        image_root = root / "cod_ambiguous_plain_paragraph_images"
        image_root.mkdir(parents=True, exist_ok=True)
        operators_png = image_root / "operators.png"
        operators_png.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <h2>Operators</h2>
          <img src="{operators_png.resolve().as_uri()}" alt="" />
          <p>Ghost is a stealth-focused operator, and Soap is a front-line breacher.</p>
        </body></html>
        """
        operators_path = root / "operators_ambiguous_plain_paragraph.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "ambiguous_plain_paragraph_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_cross_paragraph_ambiguous_source(self, root: Path) -> Path:
        image_root = root / "cod_cross_paragraph_ambiguous_images"
        image_root.mkdir(parents=True, exist_ok=True)
        operators_png = image_root / "operators.png"
        operators_png.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <h2>Operators</h2>
          <img src="{operators_png.resolve().as_uri()}" alt="" />
          <p>Ghost is a stealth-focused operator.</p>
          <p>Soap is a front-line breacher.</p>
        </body></html>
        """
        operators_path = root / "operators_cross_paragraph_ambiguous.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "cross_paragraph_ambiguous_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_surrounding_paragraph_ambiguous_source(self, root: Path) -> Path:
        image_root = root / "cod_surrounding_paragraph_ambiguous_images"
        image_root.mkdir(parents=True, exist_ok=True)
        operators_png = image_root / "operators.png"
        operators_png.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <h2>Operators</h2>
          <p>Ghost is a stealth-focused operator.</p>
          <img src="{operators_png.resolve().as_uri()}" alt="" />
          <p>Soap is a front-line breacher.</p>
        </body></html>
        """
        operators_path = root / "operators_surrounding_paragraph_ambiguous.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "surrounding_paragraph_ambiguous_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_referential_paragraph_source(self, root: Path) -> Path:
        image_root = root / "cod_referential_paragraph_images"
        image_root.mkdir(parents=True, exist_ok=True)
        operators_png = image_root / "operators.png"
        operators_png.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <h2>Operators</h2>
          <img src="{operators_png.resolve().as_uri()}" alt="" />
          <p>This veteran operator excels at flanking and stealth.</p>
        </body></html>
        """
        operators_path = root / "operators_referential_paragraph.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "referential_paragraph_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_mixed_gallery_paragraph_source(self, root: Path) -> Path:
        image_root = root / "cod_mixed_gallery_paragraph_images"
        image_root.mkdir(parents=True, exist_ok=True)
        ghost_png = image_root / "ghost.png"
        soap_png = image_root / "soap.png"
        ghost_png.write_bytes(_ONE_BY_ONE_PNG)
        soap_png.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <div class="gallery">
            <img src="{ghost_png.resolve().as_uri()}" alt="" />
            <p>Ghost is a stealth-focused operator.</p>
            <img src="{soap_png.resolve().as_uri()}" alt="" />
            <p>This veteran operator adapts quickly in combat.</p>
          </div>
        </body></html>
        """
        operators_path = root / "operators_mixed_gallery_paragraph.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "mixed_gallery_paragraph_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_category_sources(self, root: Path) -> Path:
        source_root = root / "cod_category"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        flash = source_root / "flash.png"
        triple = source_root / "triple.png"
        for path in (ghost, flash, triple):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <section class="category-page__members">
            <article class="category-page__member">
              <img src="{ghost.resolve().as_uri()}" alt="Ghost operator portrait" />
              <a class="category-page__member-link" href="/wiki/Ghost">Ghost</a>
            </article>
          </section>
        </body></html>
        """
        equipment_html = f"""
        <html><body>
          <section class="category-page__members">
            <article class="category-page__member">
              <img src="{flash.resolve().as_uri()}" alt="Flash Grenade icon" />
              <a class="category-page__member-link" href="/wiki/Flash_Grenade">Flash Grenade</a>
            </article>
          </section>
        </body></html>
        """
        events_html = f"""
        <html><body>
          <section class="category-page__members">
            <article class="category-page__member">
              <img src="{triple.resolve().as_uri()}" alt="Triple Kill medal" />
              <a class="category-page__member-link" href="/wiki/Triple_Kill">Triple Kill</a>
            </article>
          </section>
        </body></html>
        """
        operators_path = source_root / "operators_category.html"
        equipment_path = source_root / "equipment_category.html"
        events_path = source_root / "events_category.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_category_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                    f"  - role: equipment",
                    f"    url: \"{equipment_path.resolve().as_uri()}\"",
                    f"  - role: events",
                    f"    url: \"{events_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_card_grid_sources(self, root: Path) -> Path:
        source_root = root / "cod_card_grid"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        flash = source_root / "flash.png"
        triple = source_root / "triple.png"
        for path in (ghost, flash, triple):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <section class="card-grid">
            <article class="item-card">
              <img src="{ghost.resolve().as_uri()}" alt="Ghost operator portrait" />
              <a href="/wiki/Ghost">Ghost</a>
            </article>
          </section>
        </body></html>
        """
        equipment_html = f"""
        <html><body>
          <div class="item-grid">
            <div class="grid-card">
              <img src="{flash.resolve().as_uri()}" alt="Flash Grenade icon" />
              <a href="/wiki/Flash_Grenade">Flash Grenade</a>
            </div>
          </div>
        </body></html>
        """
        events_html = f"""
        <html><body>
          <section class="roster-grid">
            <article class="member-card">
              <img src="{triple.resolve().as_uri()}" alt="Triple Kill medal" />
              <a href="/wiki/Triple_Kill">Triple Kill</a>
            </article>
          </section>
        </body></html>
        """
        operators_path = source_root / "operators_grid.html"
        equipment_path = source_root / "equipment_grid.html"
        events_path = source_root / "events_grid.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_card_grid_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                    f"  - role: equipment",
                    f"    url: \"{equipment_path.resolve().as_uri()}\"",
                    f"  - role: events",
                    f"    url: \"{events_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_gallery_card_label_sources(self, root: Path) -> Path:
        source_root = root / "cod_gallery_card_label"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <section class="gallery-grid">
            <article class="gallery-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <span class="card-label">Ghost</span>
            </article>
            <article class="gallery-card">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <figcaption class="gallerytext">Soap</figcaption>
            </article>
          </section>
        </body></html>
        """
        operators_path = source_root / "operators_gallery_cards.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_gallery_card_label_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_directory_listing_sources(self, root: Path) -> Path:
        source_root = root / "cod_directory_listing"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
        </body></html>
        """
        operators_path = source_root / "operators_directory.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_directory_listing_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_directory_detail_sources(self, root: Path) -> Path:
        source_root = root / "cod_directory_detail"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost is a support operator (Simon Riley).</p>
            <p>Price is a veteran operator who coordinates assaults.</p>
          </section>
        </body></html>
        """
        operators_path = source_root / "operators_directory_detail.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_directory_detail_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_directory_detail_ambiguous_sources(self, root: Path) -> Path:
        source_root = root / "cod_directory_detail_ambiguous"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost and Soap are support operators known for coordinated assaults.</p>
          </section>
        </body></html>
        """
        operators_path = source_root / "operators_directory_detail_ambiguous.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_directory_detail_ambiguous_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_directory_detail_conflicting_sources(self, root: Path) -> Path:
        source_root = root / "cod_directory_detail_conflicting"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost is a support operator (Simon Riley).</p>
            <p>Ghost is a tank operator (Spectre).</p>
          </section>
        </body></html>
        """
        operators_path = source_root / "operators_directory_detail_conflicting.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_directory_detail_conflicting_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_directory_detail_existing_conflict_sources(self, root: Path) -> Path:
        source_root = root / "cod_directory_detail_existing_conflict"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        roster_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost is a tank operator (Spectre).</p>
          </section>
        </body></html>
        """
        detail_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost is a support operator (Simon Riley).</p>
          </section>
        </body></html>
        """
        roster_path = source_root / "operators_roster.html"
        detail_path = source_root / "operators_directory_detail_existing_conflict.html"
        roster_path.write_text(roster_html, encoding="utf-8")
        detail_path.write_text(detail_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_directory_detail_existing_conflict_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{roster_path.resolve().as_uri()}\"",
                    "  - role: operators",
                    f"    url: \"{detail_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_directory_detail_canonical_alias_sources(self, root: Path) -> Path:
        source_root = root / "cod_directory_detail_canonical_alias"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost is a support operator (ghost).</p>
          </section>
        </body></html>
        """
        operators_path = source_root / "operators_directory_detail_canonical_alias.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_directory_detail_canonical_alias_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_directory_detail_existing_alias_sources(self, root: Path) -> Path:
        source_root = root / "cod_directory_detail_existing_alias"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        soap = source_root / "soap.png"
        for path in (ghost, soap):
            path.write_bytes(_ONE_BY_ONE_PNG)
        established_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost is a tank operator (Spectre).</p>
          </section>
        </body></html>
        """
        detail_html = f"""
        <html><body>
          <div class="directory-grid">
            <div class="directory-card">
              <img src="{ghost.resolve().as_uri()}" alt="operator portrait" />
              <p class="item-name">Ghost</p>
            </div>
            <div class="directory-item">
              <img src="{soap.resolve().as_uri()}" alt="operator portrait" />
              <span class="member-name">Soap</span>
            </div>
          </div>
          <section class="details">
            <p>Ghost is a tank operator (spectre).</p>
          </section>
        </body></html>
        """
        established_path = source_root / "operators_established_alias.html"
        detail_path = source_root / "operators_directory_detail_existing_alias.html"
        established_path.write_text(established_html, encoding="utf-8")
        detail_path.write_text(detail_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_directory_detail_existing_alias_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    "  - role: operators",
                    f"    url: \"{established_path.resolve().as_uri()}\"",
                    "  - role: operators",
                    f"    url: \"{detail_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_callofduty_table_hybrid_sources(self, root: Path) -> Path:
        source_root = root / "cod_table_hybrid"
        source_root.mkdir(parents=True, exist_ok=True)
        ghost = source_root / "ghost.png"
        flash = source_root / "flash.png"
        triple = source_root / "triple.png"
        for path in (ghost, flash, triple):
            path.write_bytes(_ONE_BY_ONE_PNG)
        operators_html = f"""
        <html><body>
          <table class="wikitable item-table">
            <tr><th>Portrait</th><th>Name</th><th>Role</th></tr>
            <tr>
              <td><img src="{ghost.resolve().as_uri()}" alt="Ghost operator portrait" /></td>
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
              <td><img src="{flash.resolve().as_uri()}" alt="Flash Grenade icon" /></td>
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
              <td><img src="{triple.resolve().as_uri()}" alt="Triple Kill medal" /></td>
              <td><a href="/wiki/Triple_Kill">Triple Kill</a></td>
            </tr>
          </table>
        </body></html>
        """
        operators_path = source_root / "operators_table.html"
        equipment_path = source_root / "equipment_table.html"
        events_path = source_root / "events_table.html"
        operators_path.write_text(operators_html, encoding="utf-8")
        equipment_path.write_text(equipment_html, encoding="utf-8")
        events_path.write_text(events_html, encoding="utf-8")
        manifest_path = root / "call_of_duty_table_hybrid_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: call_of_duty",
                    "sources:",
                    f"  - role: operators",
                    f"    url: \"{operators_path.resolve().as_uri()}\"",
                    f"  - role: equipment",
                    f"    url: \"{equipment_path.resolve().as_uri()}\"",
                    f"  - role: events",
                    f"    url: \"{events_path.resolve().as_uri()}\"",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_valorant_sources(self, root: Path) -> Path:
        image_root = root / "valorant_images"
        image_root.mkdir(parents=True, exist_ok=True)
        jett = image_root / "jett.png"
        sage = image_root / "sage.png"
        vandal = image_root / "vandal.png"
        phantom = image_root / "phantom.png"
        radiant = image_root / "radiant.png"
        for path in (jett, sage, vandal, phantom, radiant):
            path.write_bytes(_ONE_BY_ONE_PNG)

        agents_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Duelists</h2>
          <ul><li>Jett</li></ul>
          <h2>Strategists</h2>
          <ul><li>Sage</li></ul>
          <img src="{jett.resolve().as_uri()}" alt="Jett agent portrait" />
          <img src="{sage.resolve().as_uri()}" alt="Sage agent portrait" />
        </div></body></html>
        """
        gear_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Loadout</h2>
          <ul>
            <li>Vandal</li>
            <li>Phantom</li>
          </ul>
          <img src="{vandal.resolve().as_uri()}" alt="Vandal weapon icon" />
          <img src="{phantom.resolve().as_uri()}" alt="Phantom weapon icon" />
        </div></body></html>
        """
        ranks_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Competitive Ranks</h2>
          <ul>
            <li>Radiant</li>
            <li>Immortal</li>
          </ul>
          <img src="{radiant.resolve().as_uri()}" alt="Radiant rank badge" />
        </div></body></html>
        """
        agents_path = root / "agents.html"
        gear_path = root / "gear.html"
        ranks_path = root / "ranks.html"
        agents_path.write_text(agents_html, encoding="utf-8")
        gear_path.write_text(gear_html, encoding="utf-8")
        ranks_path.write_text(ranks_html, encoding="utf-8")

        manifest_path = root / "valorant_sources.yaml"
        manifest_path.write_text(
            "\n".join(
                [
                    "game: valorant",
                    "sources:",
                    "  - role: agents",
                    f'    url: "{agents_path.resolve().as_uri()}"',
                    "  - role: gear",
                    f'    url: "{gear_path.resolve().as_uri()}"',
                    "  - role: ranks",
                    f'    url: "{ranks_path.resolve().as_uri()}"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _accept_all_bindings(self, draft_root: Path) -> None:
        bindings_path = draft_root / "catalog" / "bindings.csv"
        with bindings_path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
            headers = rows[0].keys()
        for row in rows:
            row["status"] = "accepted"
        with bindings_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _read_csv(self, path: Path) -> list[dict[str, str]]:
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def _assert_phase_status_matches_publish_readiness(self, draft_root: Path, *, repo_root: Path) -> dict[str, object]:
        readiness = validate_onboarding_publish(draft_root, repo_root=repo_root)
        expected_phase_status = "ready_to_publish" if bool(readiness["can_publish"]) else "bindings_pending"
        state_payload = json.loads((draft_root / "manifests" / "onboarding_state.json").read_text(encoding="utf-8"))
        manifest_payload = json.loads((draft_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(state_payload["phase_status"], expected_phase_status)
        self.assertEqual(manifest_payload["phase_status"], expected_phase_status)
        return readiness

    def test_onboarding_manifest_requires_valid_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            manifest_path = repo_root / "bad_sources.yaml"
            manifest_path.write_text("game: marvel_rivals\nsources: {}\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)

    def test_schema_adaptation_creates_saved_game_detection_schema_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            result = adapt_game_schema("marvel_rivals", repo_root=repo_root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "schema_adapted")
            draft_root = Path(result["draft_root"])
            self.assertTrue((draft_root / "manifests" / "game_detection_schema.yaml").exists())
            self.assertTrue((draft_root / "manifests" / "onboarding_state.json").exists())
            schema = load_yaml_file(draft_root / "manifests" / "game_detection_schema.yaml")
            self.assertEqual(schema["families"]["ability_icon"]["threshold"], 0.91)
            self.assertEqual(schema["families"]["medal_icon"]["temporal_window"], 3)

    def test_source_ingestion_populates_draft_from_saved_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            self._write_sources(repo_root)
            ingest_result = ingest_onboarding_sources(
                Path(schema_result["draft_root"]),
                [
                    OnboardingSource(role="roster", url=(repo_root / "roster.html").resolve().as_uri()),
                    OnboardingSource(role="abilities", url=(repo_root / "abilities.html").resolve().as_uri()),
                    OnboardingSource(role="medals", url=(repo_root / "medals.html").resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            self.assertTrue(ingest_result["ok"])
            self.assertEqual(ingest_result["status"], "sources_ingested")
            self.assertGreaterEqual(ingest_result["counts"]["detection_rows"], 3)
            self.assertGreaterEqual(ingest_result["counts"]["candidate_assets"], 3)
            draft_root = Path(schema_result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            ability_row = next(row for row in entities["abilities"] if row["display_name"] == "Final Judgment")
            self.assertEqual(ability_row["ability_id"], "punisher_ult")
            self.assertEqual(ability_row["class"], "ultimate")
            self.assertTrue(ability_row["starter_seed_applied"])
            self.assertTrue((draft_root / "catalog" / "asset_candidates.csv").exists())
            self.assertTrue((draft_root / "catalog" / "source_fetch_log.csv").exists())

    def test_source_ingestion_preserves_source_derived_structured_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            hero_png = image_root / "punisher.png"
            ability_png = image_root / "final_judgment.png"
            medal_png = image_root / "payload_savior.png"
            for path in (hero_png, ability_png, medal_png):
                path.write_bytes(_ONE_BY_ONE_PNG)
            roster_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Duelists</h2>
              <ul><li>The Punisher (Frank Castle)</li></ul>
              <img src="{hero_png.resolve().as_uri()}" alt="The Punisher portrait" />
            </div></body></html>
            """
            abilities_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Ultimate Abilities</h2>
              <ul><li>Final Judgment</li></ul>
              <img src="{ability_png.resolve().as_uri()}" alt="Final Judgment icon" />
            </div></body></html>
            """
            medals_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Objective Honors</h2>
              <ul><li>Payload Savior</li></ul>
              <img src="{medal_png.resolve().as_uri()}" alt="Payload Savior medal" />
            </div></body></html>
            """
            roster_path = repo_root / "roster.html"
            abilities_path = repo_root / "abilities.html"
            medals_path = repo_root / "medals.html"
            roster_path.write_text(roster_html, encoding="utf-8")
            abilities_path.write_text(abilities_html, encoding="utf-8")
            medals_path.write_text(medals_html, encoding="utf-8")

            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="roster", url=roster_path.resolve().as_uri()),
                    OnboardingSource(role="abilities", url=abilities_path.resolve().as_uri()),
                    OnboardingSource(role="medals", url=medals_path.resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            hero_row = next(row for row in entities["heroes"] if row["display_name"] == "The Punisher")
            ability_row = next(row for row in entities["abilities"] if row["display_name"] == "Final Judgment")
            event_row = next(row for row in entities["events"] if row["display_name"] == "Payload Savior")

            self.assertEqual(hero_row["role"], "duelist")
            self.assertEqual(hero_row["role_source"], "source")
            self.assertEqual(hero_row["aliases"], ["Frank Castle", "punisher"])
            self.assertEqual(ability_row["class"], "ultimate")
            self.assertEqual(ability_row["class_source"], "source")
            self.assertEqual(event_row["category"], "objective")
            self.assertEqual(event_row["category_source"], "source")

    def test_generic_source_merge_rejects_alias_equivalent_to_canonical_name(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
            operators_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Soap (soap)</li></ul>
            </div></body></html>
            """
            operators_path = repo_root / "operators_canonical_alias.html"
            operators_path.write_text(operators_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])

            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="operators", url=operators_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            soap_row = next(row for row in entities["heroes"] if row["display_name"] == "Soap")
            self.assertEqual(soap_row.get("aliases", []), [])
            self.assertTrue(
                any(
                    row["item_type"] == "alias_equivalent_to_canonical_name"
                    and row.get("display_name", "") == "Soap"
                    and "soap" in row.get("reason", "")
                    for row in qa_rows
                )
            )

    def test_generic_source_merge_rejects_alias_equivalent_to_existing_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
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
            first_path = repo_root / "operators_existing_alias_first.html"
            second_path = repo_root / "operators_existing_alias_second.html"
            first_path.write_text(first_html, encoding="utf-8")
            second_path.write_text(second_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])

            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="operators", url=first_path.resolve().as_uri()),
                    OnboardingSource(role="operators", url=second_path.resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            soap_row = next(row for row in entities["heroes"] if row["display_name"] == "Soap")
            self.assertEqual(soap_row.get("aliases", []), ["Spectre"])
            self.assertTrue(
                any(
                    row["item_type"] == "alias_equivalent_to_existing_alias"
                    and row.get("display_name", "") == "Soap"
                    and "spectre" in row.get("reason", "")
                    for row in qa_rows
                )
            )

    def test_starter_seed_alias_merge_rejects_alias_equivalent_to_canonical_name(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            (repo_root / "starter_assets" / "call_of_duty" / "characters.yaml").write_text(
                "\n".join(
                    [
                        "characters:",
                        "  - id: ghost",
                        '    display_name: "Ghost"',
                        '    aliases: ["ghost", "Simon Riley"]',
                        "    role: operator",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
            operators_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Ghost</li></ul>
            </div></body></html>
            """
            operators_path = repo_root / "operators_seed_canonical_alias.html"
            operators_path.write_text(operators_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])

            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="operators", url=operators_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("aliases", []), ["Simon Riley"])
            self.assertTrue(
                any(
                    row["item_type"] == "alias_equivalent_to_canonical_name"
                    and row.get("display_name", "") == "Ghost"
                    and "ghost" in row.get("reason", "")
                    for row in qa_rows
                )
            )

    def test_starter_seed_alias_merge_rejects_alias_equivalent_to_existing_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
            operators_html = """
            <html><body><div class="mw-parser-output">
              <h2>Operators</h2>
              <ul><li>Ghost (Simon Riley)</li></ul>
            </div></body></html>
            """
            operators_path = repo_root / "operators_seed_existing_alias.html"
            operators_path.write_text(operators_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])

            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="operators", url=operators_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("aliases", []), ["Simon Riley"])
            self.assertTrue(
                any(
                    row["item_type"] == "alias_equivalent_to_existing_alias"
                    and row.get("display_name", "") == "Ghost"
                    and "simon_riley" in row.get("reason", "").casefold().replace(" ", "_")
                    for row in qa_rows
                )
            )

    def test_source_ingestion_filters_artwork_candidates_but_keeps_plausible_icons(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            icon_png = image_root / "final_judgment_icon.png"
            artwork_png = image_root / "final_judgment_banner.png"
            icon_png.write_bytes(_ONE_BY_ONE_PNG)
            artwork_png.write_bytes(_ONE_BY_ONE_PNG)
            abilities_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Ultimate Abilities</h2>
              <ul><li>Final Judgment</li></ul>
              <img src="{icon_png.resolve().as_uri()}" alt="Final Judgment icon" />
              <img src="{artwork_png.resolve().as_uri()}" alt="Final Judgment banner artwork" />
            </div></body></html>
            """
            abilities_path = repo_root / "abilities.html"
            abilities_path.write_text(abilities_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="abilities", url=abilities_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0]["display_name"], "Final Judgment")
            self.assertIn(candidates[0]["candidate_quality"], {"medium", "high"})
            self.assertIn("quality_score", candidates[0])

    def test_source_ingestion_uses_image_anchors_and_reports_weak_filename_only_pages(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            caption_png = image_root / "mystery_ult.png"
            filename_png = image_root / "payload_savior.png"
            caption_png.write_bytes(_ONE_BY_ONE_PNG)
            filename_png.write_bytes(_ONE_BY_ONE_PNG)
            abilities_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Ultimate Abilities</h2>
              <figure>
                <img src="{caption_png.resolve().as_uri()}" alt="ability icon" />
                <figcaption>Final Judgment</figcaption>
              </figure>
            </div></body></html>
            """
            medals_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Objective Honors</h2>
              <img src="{filename_png.resolve().as_uri()}" alt="" />
            </div></body></html>
            """
            abilities_path = repo_root / "abilities.html"
            medals_path = repo_root / "medals.html"
            abilities_path.write_text(abilities_html, encoding="utf-8")
            medals_path.write_text(medals_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])

            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="abilities", url=abilities_path.resolve().as_uri()),
                    OnboardingSource(role="medals", url=medals_path.resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            ability_row = next(row for row in entities["abilities"] if row["display_name"] == "Final Judgment")
            self.assertEqual(ability_row["class"], "ultimate")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["item_type"] == "weak_image_anchor" for row in qa_rows))
            self.assertTrue(any(row["item_type"] == "filename_only_anchor" for row in qa_rows))

    def test_build_onboarding_draft_prefers_direct_images_over_page_images(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            direct_root = repo_root / "direct"
            direct_root.mkdir(parents=True, exist_ok=True)
            page_png = image_root / "punisher_page.png"
            direct_png = direct_root / "the_punisher.png"
            page_png.write_bytes(_ONE_BY_ONE_PNG)
            direct_png.write_bytes(_ONE_BY_ONE_PNG)
            roster_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Duelists</h2>
              <ul><li>The Punisher</li></ul>
              <img src="{page_png.resolve().as_uri()}" alt="The Punisher portrait" />
            </div></body></html>
            """
            roster_path = repo_root / "roster.html"
            roster_path.write_text(roster_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="roster", url=roster_path.resolve().as_uri()),
                    OnboardingSource(role="roster", url=direct_png.resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            build_onboarding_draft(draft_root, repo_root=repo_root)
            bindings = self._read_csv(draft_root / "catalog" / "bindings.csv")
            punisher_rows = [row for row in bindings if row["target_display_name"] == "The Punisher"]
            self.assertGreaterEqual(len(punisher_rows), 2)
            self.assertEqual(punisher_rows[0]["source_kind"], "direct_image")
            self.assertGreater(float(punisher_rows[0]["binding_score"]), float(punisher_rows[1]["binding_score"]))

    def test_build_onboarding_draft_reports_conflicting_binding_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            roster_png = image_root / "punisher.png"
            alt_root = repo_root / "alt"
            alt_two_root = repo_root / "alt_two"
            alt_root.mkdir(parents=True, exist_ok=True)
            alt_two_root.mkdir(parents=True, exist_ok=True)
            direct_one = alt_root / "the_punisher.png"
            direct_two = alt_two_root / "the_punisher.png"
            for path in (roster_png, direct_one, direct_two):
                path.write_bytes(_ONE_BY_ONE_PNG)
            roster_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Duelists</h2>
              <ul><li>The Punisher</li></ul>
              <img src="{roster_png.resolve().as_uri()}" alt="The Punisher portrait" />
            </div></body></html>
            """
            roster_path = repo_root / "roster.html"
            roster_path.write_text(roster_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="roster", url=roster_path.resolve().as_uri()),
                    OnboardingSource(role="roster", url=direct_one.resolve().as_uri()),
                    OnboardingSource(role="roster", url=direct_two.resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            build_onboarding_draft(draft_root, repo_root=repo_root)
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["item_type"] == "conflicting_binding_candidates" for row in qa_rows))

    def test_build_onboarding_draft_prefers_body_candidate_over_infobox_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            body_png = image_root / "ghost_body.png"
            infobox_png = image_root / "ghost_infobox.png"
            body_png.write_bytes(_ONE_BY_ONE_PNG)
            infobox_png.write_bytes(_ONE_BY_ONE_PNG)
            operators_html = f"""
            <html><body>
              <aside class="portable-infobox">
                <figure class="pi-image">
                  <img src="{infobox_png.resolve().as_uri()}" alt="Ghost portrait" />
                </figure>
              </aside>
              <div class="mw-parser-output">
                <h2>Roster</h2>
                <ul><li>Ghost</li></ul>
                <img src="{body_png.resolve().as_uri()}" alt="Ghost portrait" />
              </div>
            </body></html>
            """
            operators_path = repo_root / "operators.html"
            operators_path.write_text(operators_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="operators", url=operators_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            build_onboarding_draft(draft_root, repo_root=repo_root)
            bindings = self._read_csv(draft_root / "catalog" / "bindings.csv")
            ghost_rows = [row for row in bindings if row["target_display_name"] == "Ghost"]
            self.assertEqual(ghost_rows[0]["source_kind"], "page_image")
            self.assertGreater(float(ghost_rows[0]["binding_score"]), float(ghost_rows[1]["binding_score"]))
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["item_type"] == "infobox_competes_with_body_candidate" for row in qa_rows))

    def test_build_onboarding_draft_prefers_body_candidate_over_gallery_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            body_png = image_root / "ghost_body.png"
            gallery_png = image_root / "ghost_gallery.png"
            body_png.write_bytes(_ONE_BY_ONE_PNG)
            gallery_png.write_bytes(_ONE_BY_ONE_PNG)
            operators_html = f"""
            <html><body>
              <div class="mw-parser-output">
                <h2>Roster</h2>
                <ul><li>Ghost</li></ul>
                <img src="{body_png.resolve().as_uri()}" alt="Ghost portrait" />
                <div class="gallery">
                  <figure class="gallerybox">
                    <img src="{gallery_png.resolve().as_uri()}" alt="" />
                    <figcaption>Ghost portrait</figcaption>
                  </figure>
                </div>
              </div>
            </body></html>
            """
            operators_path = repo_root / "operators_gallery.html"
            operators_path.write_text(operators_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="operators", url=operators_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            build_onboarding_draft(draft_root, repo_root=repo_root)
            bindings = self._read_csv(draft_root / "catalog" / "bindings.csv")
            ghost_rows = [row for row in bindings if row["target_display_name"] == "Ghost"]
            self.assertEqual(ghost_rows[0]["source_kind"], "page_image")
            self.assertGreater(float(ghost_rows[0]["binding_score"]), float(ghost_rows[1]["binding_score"]))
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["item_type"] == "gallery_competes_with_body_candidate" for row in qa_rows))

    def test_binding_score_flags_weak_name_matches(self) -> None:
        binding = score_binding_candidate(
            target_display_name="The Punisher",
            target_aliases=[],
            asset_family="hero_portrait",
            candidate={
                "display_name": "The Punisher Variant Portrait",
                "binding_key": "punisher_variant",
                "candidate_quality": "medium",
                "source_kind": "page_image",
                "portrait_like": True,
                "icon_like": False,
                "badge_like": False,
                "artwork_like": False,
                "generic_page_art": False,
                "map_like": False,
            },
        )
        self.assertGreater(binding["score"], 0.0)
        self.assertEqual(binding["name_match_quality"], "weak")
        self.assertTrue(binding["flags"]["weak_name_match"])

    def test_build_onboarding_draft_creates_bindings_and_qa_from_populated_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            self._write_sources(repo_root)
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="roster", url=(repo_root / "roster.html").resolve().as_uri()),
                    OnboardingSource(role="abilities", url=(repo_root / "abilities.html").resolve().as_uri()),
                    OnboardingSource(role="medals", url=(repo_root / "medals.html").resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            result = build_onboarding_draft(draft_root, repo_root=repo_root)
            self.assertTrue(result["ok"])
            self.assertIn(result["status"], {"bindings_pending", "ready_to_publish"})
            self.assertGreaterEqual(result["counts"]["binding_candidates"], 3)
            self.assertTrue((draft_root / "catalog" / "bindings.csv").exists())
            self.assertTrue((draft_root / "catalog" / "qa_queue.csv").exists())

    def test_source_ingestion_adds_population_qa_for_unknown_event_category(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            draft_root = Path(schema_result["draft_root"])
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            unknown_medal = image_root / "spotlight.png"
            unknown_medal.write_bytes(_ONE_BY_ONE_PNG)
            medals_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Highlights</h2>
              <ul><li>Spotlight Moment</li></ul>
              <img src="{unknown_medal.resolve().as_uri()}" alt="Spotlight Moment medal" />
            </div></body></html>
            """
            medals_path = repo_root / "medals.html"
            medals_path.write_text(medals_html, encoding="utf-8")
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="medals", url=medals_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            with (draft_root / "catalog" / "qa_queue.csv").open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(any(row["item_type"] == "missing_classification" and row["target_kind"] == "event" for row in rows))

    def test_source_ingestion_reports_ambiguous_starter_seed_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            abilities_path = repo_root / "starter_assets" / "marvel_rivals" / "abilities.yaml"
            abilities_path.parent.mkdir(parents=True, exist_ok=True)
            abilities_path.write_text(
                "\n".join(
                    [
                        "abilities:",
                        "  - id: alpha_ult",
                        '    display_name: "Alpha Burst"',
                        '    aliases: ["burst"]',
                        "    class: ultimate",
                        "  - id: beta_ult",
                        '    display_name: "Beta Burst"',
                        '    aliases: ["burst"]',
                        "    class: ultimate",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            burst_png = image_root / "burst.png"
            burst_png.write_bytes(_ONE_BY_ONE_PNG)
            abilities_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Abilities</h2>
              <ul><li>Burst</li></ul>
              <img src="{burst_png.resolve().as_uri()}" alt="Burst icon" />
            </div></body></html>
            """
            abilities_source = repo_root / "abilities.html"
            abilities_source.write_text(abilities_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="abilities", url=abilities_source.resolve().as_uri())],
                repo_root=repo_root,
            )

            with (draft_root / "catalog" / "qa_queue.csv").open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(any(row["item_type"] == "ambiguous_identity_match" for row in rows))

    def test_source_ingestion_applies_safe_canonical_identity_preference_from_starter_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            roster_html = """
            <html><body><div class="mw-parser-output">
              <h2>Roster</h2>
              <ul><li>Punisher</li></ul>
            </div></body></html>
            """
            roster_path = repo_root / "roster.html"
            roster_path.write_text(roster_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="roster", url=roster_path.resolve().as_uri())],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            hero_row = next(row for row in entities["heroes"] if row["hero_id"] == "punisher")
            self.assertEqual(hero_row["display_name"], "The Punisher")
            self.assertEqual(hero_row["canonical_display_name_source"], "starter_seed")
            self.assertTrue(any(row["item_type"] == "canonical_identity_preference_applied" for row in qa_rows))

    def test_build_qa_queue_drops_stale_binding_rows_from_existing_queue(self) -> None:
        detection_rows = [
            {
                "detection_id": "call_of_duty.alex_keller.hero_portrait",
                "target_id": "alex_keller",
                "target_display_name": "Alex Keller",
                "status": "ready",
                "requires_asset": True,
            }
        ]
        candidates = [
            {
                "candidate_id": "candidate_1",
                "display_name": "Alex Keller",
                "anchor_source": "text",
                "anchor_ambiguous": False,
                "paragraph_referential": False,
                "raw_label": "Alex Keller",
                "candidate_quality": "high",
                "quality_reasons": [],
                "artwork_like": False,
                "generic_page_art": False,
                "source_page_url": "https://example.test/alex-keller",
                "binding_key": "alex keller",
                "asset_family": "hero_portrait",
                "source_kind": "category_member_image",
            }
        ]
        bindings = [
            {
                "detection_id": "call_of_duty.alex_keller.hero_portrait",
                "target_id": "alex_keller",
                "target_display_name": "Alex Keller",
                "candidate_id": "candidate_1",
                "status": "accepted",
                "reason": "clean accepted binding",
                "weak_name_match": "False",
                "lower_trust_source_kind": "False",
                "image_kind_mismatch": "False",
                "confidence": 0.92,
            }
        ]
        existing_qa = [
            {
                "item_type": "weak_name_match",
                "detection_id": "call_of_duty.alex_keller.hero_portrait",
                "target_id": "alex_keller",
                "display_name": "Alex Keller",
                "status": "needs_binding_review",
                "reason": "stale binding heuristic",
            },
            {
                "item_type": "lower_trust_source_kind",
                "detection_id": "call_of_duty.alex_keller.hero_portrait",
                "target_id": "alex_keller",
                "display_name": "Alex Keller",
                "status": "needs_binding_review",
                "reason": "stale binding heuristic",
            },
            {
                "item_type": "binding_image_kind_mismatch",
                "detection_id": "call_of_duty.alex_keller.hero_portrait",
                "target_id": "alex_keller",
                "display_name": "Alex Keller",
                "status": "needs_binding_review",
                "reason": "stale binding heuristic",
            },
            {
                "item_type": "missing_classification",
                "target_kind": "ability",
                "target_id": "armor_plates",
                "display_name": "Armor Plates",
                "status": "needs_population_review",
                "reason": "population issue should persist",
            },
        ]

        qa_rows = _build_qa_queue(detection_rows, candidates, bindings, existing_qa=existing_qa)

        item_types = [row["item_type"] for row in qa_rows]
        self.assertIn("binding_candidate", item_types)
        self.assertIn("missing_classification", item_types)
        self.assertNotIn("weak_name_match", item_types)
        self.assertNotIn("lower_trust_source_kind", item_types)
        self.assertNotIn("binding_image_kind_mismatch", item_types)

    def test_source_ingestion_reports_conflicting_source_identity_match(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
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
            first_path = repo_root / "operators_first.html"
            second_path = repo_root / "operators_second.html"
            first_path.write_text(first_html, encoding="utf-8")
            second_path.write_text(second_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="operators", url=first_path.resolve().as_uri()),
                    OnboardingSource(role="operators", url=second_path.resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(any(row["item_type"] == "conflicting_identity_match" for row in qa_rows))

    def test_source_ingestion_reports_ambiguous_structured_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            burst_png = image_root / "mystic_shift.png"
            burst_png.write_bytes(_ONE_BY_ONE_PNG)
            abilities_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Ultimate Passive Abilities</h2>
              <ul><li>Mystic Shift</li></ul>
              <img src="{burst_png.resolve().as_uri()}" alt="Mystic Shift icon" />
            </div></body></html>
            """
            abilities_source = repo_root / "abilities.html"
            abilities_source.write_text(abilities_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="abilities", url=abilities_source.resolve().as_uri())],
                repo_root=repo_root,
            )

            with (draft_root / "catalog" / "qa_queue.csv").open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(any(row["item_type"] == "ambiguous_structured_extraction" for row in rows))

    def test_source_ingestion_reports_source_seed_disagreement(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            abilities_path = repo_root / "starter_assets" / "marvel_rivals" / "abilities.yaml"
            abilities_path.write_text(
                "\n".join(
                    [
                        "abilities:",
                        "  - id: punisher_ult",
                        "    character_id: punisher",
                        '    display_name: "Final Judgment"',
                        '    aliases: ["final judgment"]',
                        "    class: passive",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            final_judgment = image_root / "final_judgment.png"
            final_judgment.write_bytes(_ONE_BY_ONE_PNG)
            abilities_html = f"""
            <html><body><div class="mw-parser-output">
              <h2>Ultimate Abilities</h2>
              <ul><li>Final Judgment</li></ul>
              <img src="{final_judgment.resolve().as_uri()}" alt="Final Judgment icon" />
            </div></body></html>
            """
            abilities_source = repo_root / "abilities.html"
            abilities_source.write_text(abilities_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [OnboardingSource(role="abilities", url=abilities_source.resolve().as_uri())],
                repo_root=repo_root,
            )

            with (draft_root / "catalog" / "qa_queue.csv").open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(any(row["item_type"] == "source_seed_disagreement" for row in rows))

    def test_source_ingestion_prefers_longer_source_canonical_name_for_same_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            schema_result = adapt_game_schema("marvel_rivals", repo_root=repo_root)
            first_html = """
            <html><body><div class="mw-parser-output">
              <h2>Roster</h2>
              <ul><li>Punisher</li></ul>
            </div></body></html>
            """
            second_html = """
            <html><body><div class="mw-parser-output">
              <h2>Roster</h2>
              <ul><li>The Punisher</li></ul>
            </div></body></html>
            """
            first_path = repo_root / "roster_first.html"
            second_path = repo_root / "roster_second.html"
            first_path.write_text(first_html, encoding="utf-8")
            second_path.write_text(second_html, encoding="utf-8")
            draft_root = Path(schema_result["draft_root"])
            ingest_onboarding_sources(
                draft_root,
                [
                    OnboardingSource(role="roster", url=first_path.resolve().as_uri()),
                    OnboardingSource(role="roster", url=second_path.resolve().as_uri()),
                ],
                repo_root=repo_root,
            )

            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            hero_row = next(row for row in entities["heroes"] if row["hero_id"] == "punisher")
            self.assertEqual(hero_row["display_name"], "The Punisher")
            self.assertTrue(any(row["item_type"] == "canonical_identity_preference_applied" for row in qa_rows))

    def test_invalid_override_structure_fails_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            overrides_path = repo_root / "starter_assets" / "marvel_rivals" / "game_detection_schema_overrides.yaml"
            overrides_path.parent.mkdir(parents=True, exist_ok=True)
            overrides_path.write_text("families:\n  hero_portrait: invalid\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                adapt_game_schema("marvel_rivals", repo_root=repo_root)

    def test_onboarding_run_creates_draft_bundle_with_candidates_and_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["counts"]["heroes"], 1)
            self.assertGreaterEqual(result["counts"]["abilities"], 1)
            self.assertGreaterEqual(result["counts"]["events"], 1)
            self.assertGreaterEqual(result["counts"]["detection_rows"], 3)
            self.assertGreaterEqual(result["counts"]["candidate_assets"], 3)
            self.assertGreaterEqual(result["counts"]["binding_candidates"], 3)

            draft_root = Path(result["draft_root"])
            self.assertTrue((draft_root / "entities.yaml").exists())
            self.assertTrue((draft_root / "manifests" / "assets_manifest.json").exists())
            self.assertTrue((draft_root / "manifests" / "detection_manifest.yaml").exists())
            self.assertTrue((draft_root / "catalog" / "qa_queue.csv").exists())
            self.assertTrue((draft_root / "catalog" / "detection_rows.csv").exists())

    def test_onboarding_accepts_plain_local_source_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            manifest_path = self._write_sources(repo_root, plain_local_paths=True)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            self.assertTrue(result["ok"])

    def test_publish_only_accepted_bindings_generate_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            with self.assertRaises(ValueError):
                publish_onboarding_draft(draft_root, repo_root=repo_root)

            self._accept_all_bindings(draft_root)
            second_publish = publish_onboarding_draft(draft_root, repo_root=repo_root)
            self.assertTrue(second_publish["ok"])
            self.assertGreater(second_publish["template_count"], 0)
            self.assertEqual(second_publish["contract_consistency"]["status"], "canonical")
            published_templates = Path(second_publish["artifacts"]["cv_templates"]).read_text(encoding="utf-8")
            self.assertIn("asset_id:", published_templates)
            self.assertIn("file_hash:", published_templates)
            self.assertIn("patch_tag:", published_templates)
            self.assertIn("qa_status:", published_templates)
            self.assertTrue(Path(second_publish["artifacts"]["runtime_cv_rules"]).exists())
            self.assertTrue(Path(second_publish["artifacts"]["fusion_rules"]).exists())
            published_detection_manifest = Path(second_publish["artifacts"]["detection_manifest"]).read_text(encoding="utf-8")
            self.assertIn("published_asset_id:", published_detection_manifest)
            published_assets_manifest = json.loads(Path(second_publish["artifacts"]["assets_manifest"]).read_text(encoding="utf-8"))
            published_asset = published_assets_manifest["published_assets"][0]
            self.assertTrue(str(published_asset["file_hash"]).startswith("sha256:"))
            self.assertEqual(published_asset["qa_status"], "verified")

    def test_publish_blocks_on_unresolved_required_derived_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "culling_turret",
                    "display_name": "Culling Turret",
                    "aliases": ["culling turret"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            with self.assertRaisesRegex(ValueError, "required derived detection row"):
                publish_onboarding_draft(draft_root, repo_root=repo_root)

    def test_report_unresolved_derived_rows_lists_required_unresolved_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "culling_turret",
                    "display_name": "Culling Turret",
                    "aliases": ["culling turret"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                    "class": "ability",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            report = report_unresolved_derived_rows(draft_root)
            self.assertTrue(report["ok"])
            self.assertGreaterEqual(report["unresolved_required_count"], 1)
            culling_row = next(row for row in report["rows"] if row["target_display_name"] == "Culling Turret")
            self.assertEqual(culling_row["asset_family"], "equipment_icon")
            self.assertEqual(culling_row["current_candidate_count"], 0)

    def test_fill_derived_detection_rows_rebuilds_selected_row_bindings_from_extra_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "culling_turret",
                    "display_name": "Culling Turret",
                    "aliases": ["culling turret"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                    "class": "ability",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            initial_report = report_unresolved_derived_rows(draft_root)
            detection_id = next(
                row["detection_id"]
                for row in initial_report["rows"]
                if row["target_display_name"] == "Culling Turret"
            )
            extra_manifest = self._write_extra_marvel_ability_source(repo_root, ability_name="Culling Turret")
            fill_result = fill_derived_detection_rows(
                draft_root,
                [detection_id],
                source_manifests=[extra_manifest],
                repo_root=repo_root,
            )
            self.assertTrue(fill_result["ok"])
            self.assertEqual(fill_result["counts"]["new_candidates_matched"], 1)
            self.assertEqual(fill_result["row_updates"][0]["before_status"], "unresolved")
            self.assertEqual(fill_result["row_updates"][0]["after_status"], "unresolved_pending_review")
            readiness = self._assert_phase_status_matches_publish_readiness(draft_root, repo_root=repo_root)
            self.assertFalse(readiness["can_publish"])

            bindings = self._read_csv(draft_root / "catalog" / "bindings.csv")
            self.assertTrue(
                any(
                    row["detection_id"] == detection_id
                    and row["candidate_display_name"] == "Culling Turret"
                    for row in bindings
                )
            )

    def test_fill_derived_detection_rows_rejects_optional_unsupported_selected_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            initial_report = report_unresolved_derived_rows(draft_root)
            detection_id = next(
                row["detection_id"]
                for row in initial_report["rows"]
                if row["target_display_name"] == "Final Judgment"
            )
            derived_manifest_path = draft_root / "manifests" / "derived_detection_manifest.yaml"
            derived_manifest = load_yaml_file(derived_manifest_path)
            selected_row = next(
                row
                for row in derived_manifest["rows"]
                if row["detection_id"] == detection_id
            )
            selected_row["status"] = "optional_unsupported"
            selected_row["required"] = False
            selected_row["blocking_publish"] = False
            selected_row["reason"] = "family is intentionally unsupported for this draft"
            dump_yaml_file(derived_manifest_path, derived_manifest)

            with patch(
                "pipeline.derived_detection_manifest.derive_game_detection_manifest",
                return_value={"manifest_path": str(derived_manifest_path)},
            ):
                with self.assertRaisesRegex(ValueError, "no selected detection rows are actionable for targeted fill"):
                    fill_derived_detection_rows(
                        draft_root,
                        [detection_id],
                        source_manifests=[manifest_path],
                        repo_root=repo_root,
                    )

    def test_prepare_derived_row_review_creates_one_review_file_per_selected_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "culling_turret",
                    "display_name": "Culling Turret",
                    "aliases": ["culling turret"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                    "class": "ability",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            fill_manifest = self._write_extra_marvel_ability_source(repo_root, ability_name="Culling Turret")
            fill_report = report_unresolved_derived_rows(draft_root)
            detection_id = next(
                row["detection_id"]
                for row in fill_report["rows"]
                if row["target_display_name"] == "Culling Turret"
            )
            fill_derived_detection_rows(
                draft_root,
                [detection_id],
                source_manifests=[fill_manifest],
                repo_root=repo_root,
            )

            prepared = prepare_derived_row_review(draft_root, [detection_id])
            self.assertTrue(prepared["ok"])
            self.assertEqual(prepared["item_count"], 1)
            review_file = Path(prepared["items"][0]["review_file_path"])
            self.assertTrue(review_file.exists())
            payload = json.loads(review_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "derived_row_review_v1")
            self.assertEqual(payload["candidate_option_count"], 1)
            self.assertEqual(payload["recommended_decision"], "accept_candidate")

    def test_apply_derived_row_review_accept_candidate_resolves_selected_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "culling_turret",
                    "display_name": "Culling Turret",
                    "aliases": ["culling turret"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                    "class": "ability",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            fill_manifest = self._write_extra_marvel_ability_source(repo_root, ability_name="Culling Turret")
            fill_report = report_unresolved_derived_rows(draft_root)
            detection_id = next(
                row["detection_id"]
                for row in fill_report["rows"]
                if row["target_display_name"] == "Culling Turret"
            )
            fill_derived_detection_rows(
                draft_root,
                [detection_id],
                source_manifests=[fill_manifest],
                repo_root=repo_root,
            )

            prepared = prepare_derived_row_review(draft_root, [detection_id])
            review_file = Path(prepared["items"][0]["review_file_path"])
            review_payload = json.loads(review_file.read_text(encoding="utf-8"))
            review_payload["review_status"] = "approved"
            review_payload["review_decision"] = "accept_candidate"
            review_payload["selected_candidate_id"] = review_payload["candidate_options"][0]["candidate_id"]
            review_payload["review_notes"] = "resolved via targeted row review"
            review_file.write_text(json.dumps(review_payload, indent=2), encoding="utf-8")

            applied = apply_derived_row_review(review_file)
            self.assertTrue(applied["ok"])
            self.assertEqual(applied["applied_count"], 1)

            bindings = self._read_csv(draft_root / "catalog" / "bindings.csv")
            accepted = [
                row
                for row in bindings
                if row["detection_id"] == detection_id and row["status"] == "accepted"
            ]
            self.assertEqual(len(accepted), 1)
            self.assertEqual(accepted[0]["derived_row_review_decision"], "accept_candidate")

            derived_manifest = load_yaml_file(draft_root / "manifests" / "derived_detection_manifest.yaml")
            selected_row = next(
                row
                for row in derived_manifest["rows"]
                if row["detection_id"] == detection_id
            )
            self.assertEqual(selected_row["status"], "resolved")

    def test_summarize_derived_row_review_from_draft_root_reports_auto_accept_eligibility(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "culling_turret",
                    "display_name": "Culling Turret",
                    "aliases": ["culling turret"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                    "class": "ability",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            fill_manifest = self._write_extra_marvel_ability_source(repo_root, ability_name="Culling Turret")
            fill_report = report_unresolved_derived_rows(draft_root)
            detection_id = next(
                row["detection_id"]
                for row in fill_report["rows"]
                if row["target_display_name"] == "Culling Turret"
            )
            fill_derived_detection_rows(
                draft_root,
                [detection_id],
                source_manifests=[fill_manifest],
                repo_root=repo_root,
            )
            prepare_derived_row_review(draft_root, [detection_id])

            summary = summarize_derived_row_review(draft_root)
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["review_file_count"], 1)
            self.assertEqual(summary["pending_count"], 1)
            self.assertEqual(summary["auto_accept_eligible_count"], 1)
            self.assertEqual(summary["rows"][0]["detection_id"], detection_id)
            self.assertTrue(summary["rows"][0]["auto_accept_eligible"])

    def test_apply_derived_row_review_accept_recommended_uses_single_recommended_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "culling_turret",
                    "display_name": "Culling Turret",
                    "aliases": ["culling turret"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                    "class": "ability",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            fill_manifest = self._write_extra_marvel_ability_source(repo_root, ability_name="Culling Turret")
            fill_report = report_unresolved_derived_rows(draft_root)
            detection_id = next(
                row["detection_id"]
                for row in fill_report["rows"]
                if row["target_display_name"] == "Culling Turret"
            )
            fill_derived_detection_rows(
                draft_root,
                [detection_id],
                source_manifests=[fill_manifest],
                repo_root=repo_root,
            )

            prepared = prepare_derived_row_review(draft_root, [detection_id])
            review_file = Path(prepared["items"][0]["review_file_path"])
            applied = apply_derived_row_review(review_file, accept_recommended=True)
            self.assertTrue(applied["ok"])
            self.assertEqual(applied["applied_count"], 1)
            self.assertTrue(applied["accept_recommended"])

            updated_review_payload = json.loads(review_file.read_text(encoding="utf-8"))
            self.assertEqual(updated_review_payload["review_status"], "approved")
            self.assertEqual(updated_review_payload["review_decision"], "accept_candidate")
            self.assertTrue(updated_review_payload["selected_candidate_id"])

            derived_manifest = load_yaml_file(draft_root / "manifests" / "derived_detection_manifest.yaml")
            selected_row = next(
                row
                for row in derived_manifest["rows"]
                if row["detection_id"] == detection_id
            )
            self.assertEqual(selected_row["status"], "resolved")
            readiness = self._assert_phase_status_matches_publish_readiness(draft_root, repo_root=repo_root)
            self.assertTrue(readiness["can_publish"])

    def test_apply_derived_row_review_only_auto_populated_skips_manual_approved_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].extend(
                [
                    {
                        "ability_id": "turret_overload",
                        "display_name": "Turret Overload",
                        "aliases": ["turret overload"],
                        "source_page_url": "https://example.com/abilities",
                        "source_role": "abilities",
                        "class": "ability",
                    },
                    {
                        "ability_id": "empty_row_ability",
                        "display_name": "Empty Row Ability",
                        "aliases": ["empty row ability"],
                        "source_page_url": "https://example.com/abilities",
                        "source_role": "abilities",
                        "class": "ability",
                    },
                ]
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            fill_manifest_b = self._write_extra_marvel_ability_source(repo_root, ability_name="Turret Overload")
            fill_report = report_unresolved_derived_rows(draft_root)
            overload_detection_id = next(
                row["detection_id"]
                for row in fill_report["rows"]
                if row["target_display_name"] == "Turret Overload"
            )
            empty_detection_id = next(
                row["detection_id"]
                for row in fill_report["rows"]
                if row["target_display_name"] == "Empty Row Ability"
            )
            fill_derived_detection_rows(
                draft_root,
                [overload_detection_id],
                source_manifests=[fill_manifest_b],
                repo_root=repo_root,
            )

            prepared = prepare_derived_row_review(draft_root, [empty_detection_id, overload_detection_id])
            overload_review_file = next(
                Path(item["review_file_path"])
                for item in prepared["items"]
                if item["detection_id"] == overload_detection_id
            )
            overload_review_payload = json.loads(overload_review_file.read_text(encoding="utf-8"))
            overload_review_payload["review_status"] = "approved"
            overload_review_payload["review_decision"] = "accept_candidate"
            overload_review_payload["selected_candidate_id"] = overload_review_payload["candidate_options"][0]["candidate_id"]
            overload_review_payload["review_notes"] = "manual approved review should be skipped"
            overload_review_file.write_text(json.dumps(overload_review_payload, indent=2), encoding="utf-8")

            applied = apply_derived_row_review(
                draft_root / "review" / "derived_row_reviews",
                reject_zero_candidate=True,
                only_auto_populated=True,
            )
            self.assertTrue(applied["ok"])
            self.assertEqual(applied["applied_count"], 1)
            self.assertEqual(applied["skipped_count"], 1)

            bindings = self._read_csv(draft_root / "catalog" / "bindings.csv")
            overload_accepted = [
                row
                for row in bindings
                if row["detection_id"] == overload_detection_id and row["status"] == "accepted"
            ]
            self.assertEqual(len(overload_accepted), 0)

            empty_review_file = next(
                Path(item["review_file_path"])
                for item in prepared["items"]
                if item["detection_id"] == empty_detection_id
            )
            empty_review_payload = json.loads(empty_review_file.read_text(encoding="utf-8"))
            self.assertEqual(empty_review_payload["review_decision"], "reject_all_candidates")
            self.assertEqual(empty_review_payload["apply_status"], "applied")

    def test_apply_derived_row_review_reject_zero_candidate_auto_populates_review(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            entities = load_yaml_file(draft_root / "entities.yaml")
            entities["abilities"].append(
                {
                    "ability_id": "empty_row_ability",
                    "display_name": "Empty Row Ability",
                    "aliases": ["empty row ability"],
                    "source_page_url": "https://example.com/abilities",
                    "source_role": "abilities",
                    "class": "ability",
                }
            )
            dump_yaml_file(draft_root / "entities.yaml", entities)

            fill_report = report_unresolved_derived_rows(draft_root)
            detection_id = next(
                row["detection_id"]
                for row in fill_report["rows"]
                if row["target_display_name"] == "Empty Row Ability"
            )
            prepared = prepare_derived_row_review(draft_root, [detection_id])
            review_file = Path(prepared["items"][0]["review_file_path"])

            applied = apply_derived_row_review(
                review_file,
                reject_zero_candidate=True,
                only_auto_populated=True,
            )
            self.assertTrue(applied["ok"])
            self.assertEqual(applied["applied_count"], 1)

            review_payload = json.loads(review_file.read_text(encoding="utf-8"))
            self.assertEqual(review_payload["review_decision"], "reject_all_candidates")
            self.assertEqual(review_payload["review_status"], "approved")

    def test_publish_fails_when_generated_contracts_drift_from_detection_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)

            with patch("pipeline.game_onboarding._build_runtime_cv_rules_manifest", return_value={"event_mappings": {}}):
                with self.assertRaises(ValueError):
                    publish_onboarding_draft(draft_root, repo_root=repo_root)

    def test_load_game_pack_supports_published_pack_structure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_marvel_starter_seed(repo_root)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])
            self._accept_all_bindings(draft_root)
            publish_onboarding_draft(draft_root, repo_root=repo_root)

            assets_root = repo_root / "assets" / "games"
            starter_root = repo_root / "starter_assets"
            starter_root.mkdir(parents=True, exist_ok=True)

            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root):
                pack = load_game_pack("marvel_rivals")
                summary = pack.summary()
                self.assertEqual(pack.pack_format, "published")
                self.assertGreater(summary["template_count"], 0)

    def test_runtime_detection_schema_fails_on_unknown_ontology_terms(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            starter = repo_root / "starter_assets"
            starter.mkdir(parents=True, exist_ok=True)
            (starter / "runtime_signal_event_ontology.yaml").write_text(
                "\n".join(
                    [
                        "schema_version: runtime_signal_event_ontology_v1",
                        'signal_types: ["character_identity"]',
                        'event_types: ["pov_character_identified"]',
                        'semantic_target_fields: ["entity_id"]',
                        'producer_families: ["runtime", "proxy"]',
                        'group_by_fields: ["entity_id"]',
                        "signal_event_targets:",
                        "  character_identity:",
                        '    event_types: ["pov_character_identified"]',
                        '    target_fields: ["entity_id"]',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (starter / "runtime_detection_schema.yaml").write_text(
                "\n".join(
                    [
                        "schema_version: runtime_detection_schema_v1",
                        "families:",
                        "  hero_portrait:",
                        "    ontology_collection: heroes",
                        "    target_kind: hero",
                        "    target_id_field: hero_id",
                        "    template_semantic_field: entity_id",
                        "    requires_asset: true",
                        "    roi_ref: hero_portrait",
                        "    match_method: TM_CCOEFF_NORMED",
                        "    threshold: 0.9",
                        "    scale_set: [1.0]",
                        "    temporal_window: 3",
                        "    runtime_rule:",
                        "      signal_type: unknown_signal",
                        "      event_type: pov_character_identified",
                        "      target_field: entity_id",
                        "      target_id_source: template_field",
                        "      target_value_field: entity_id",
                        "fusion_rules: {}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                _load_runtime_detection_schema(repo_root=repo_root)

    def test_call_of_duty_schema_adaptation_uses_adapter_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)

            result = adapt_game_schema("call_of_duty", repo_root=repo_root)

            self.assertTrue(result["ok"])
            schema = load_yaml_file(Path(result["artifacts"]["game_detection_schema"]))
            self.assertFalse(schema["families"]["ability_icon"]["enabled"])
            self.assertEqual(schema["families"]["equipment_icon"]["threshold"], 0.92)
            self.assertIn("equipment_icon", schema["active_asset_families"])
            self.assertNotIn("ability_icon", schema["active_asset_families"])

    def test_call_of_duty_source_ingestion_uses_adapter_roles_and_seed_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            hero_names = {row["display_name"] for row in entities["heroes"]}
            ability_rows = {row["display_name"]: row for row in entities["abilities"]}
            event_rows = {row["display_name"]: row for row in entities["events"]}
            self.assertEqual(hero_names, {"Farah", "Ghost"})
            self.assertEqual(ability_rows["Flash Grenade"]["ability_id"], "flash_grenade")
            self.assertEqual(ability_rows["Flash Grenade"]["class"], "equipment")
            self.assertEqual(event_rows["Triple Kill"]["event_id"], "triple_kill")
            self.assertEqual(event_rows["Triple Kill"]["category"], "combat")

            detection_rows = load_yaml_file(draft_root / "manifests" / "detection_manifest.yaml")["rows"]
            flash_row = next(row for row in detection_rows if row["target_display_name"] == "Flash Grenade")
            self.assertEqual(flash_row["asset_family"], "equipment_icon")
            self.assertTrue(any(row["target_display_name"] == "Ghost" for row in detection_rows))
            self.assertTrue(any(row["target_display_name"] == "Triple Kill" for row in detection_rows))

    def test_call_of_duty_plain_figure_layout_only_promotes_captioned_figure_to_ontology(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_plain_figure_source(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertEqual([row["display_name"] for row in entities["heroes"]], ["Ghost"])
            self.assertEqual({row["display_name"] for row in candidates}, {"Ghost", "Soap"})

    def test_call_of_duty_plain_paragraph_layout_only_promotes_explicit_adjacent_paragraph(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_plain_paragraph_source(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertEqual([row["display_name"] for row in entities["heroes"]], ["Ghost"])
            self.assertEqual({row["display_name"] for row in candidates}, {"Ghost", "soap"})

    def test_call_of_duty_ambiguous_plain_paragraph_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_ambiguous_plain_paragraph_source(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertEqual(entities["heroes"], [])
            self.assertEqual(len(candidates), 1)
            self.assertTrue(any(row["item_type"] == "ambiguous_paragraph_anchor" for row in qa_rows))

    def test_call_of_duty_cross_paragraph_ambiguous_anchor_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_cross_paragraph_ambiguous_source(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertEqual(entities["heroes"], [])
            self.assertEqual(len(candidates), 1)
            self.assertTrue(any(row["item_type"] == "cross_paragraph_ambiguous_anchor" for row in qa_rows))

    def test_call_of_duty_surrounding_paragraph_ambiguous_anchor_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_surrounding_paragraph_ambiguous_source(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertEqual(entities["heroes"], [])
            self.assertEqual(len(candidates), 1)
            self.assertTrue(any(row["item_type"] == "surrounding_paragraph_ambiguous_anchor" for row in qa_rows))

    def test_call_of_duty_referential_paragraph_anchor_stays_candidate_only_and_emits_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_referential_paragraph_source(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertEqual(entities["heroes"], [])
            self.assertEqual(len(candidates), 1)
            self.assertTrue(any(row["item_type"] == "referential_paragraph_anchor" for row in qa_rows))

    def test_call_of_duty_mixed_gallery_paragraph_promotes_only_explicit_gallery_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_mixed_gallery_paragraph_source(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertEqual([row["display_name"] for row in entities["heroes"]], ["Ghost"])
            self.assertEqual({row["display_name"] for row in candidates}, {"Ghost", "soap"})
            self.assertTrue(any(row["item_type"] == "referential_paragraph_anchor" for row in qa_rows))

    def test_call_of_duty_gallery_card_label_pages_work_in_onboarding(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_gallery_card_label_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertEqual([row["display_name"] for row in entities["heroes"]], ["Ghost", "Soap"])
            self.assertEqual({row["display_name"] for row in candidates}, {"Ghost", "Soap"})
            self.assertEqual({row["source_kind"] for row in candidates}, {"category_member_image"})

    def test_call_of_duty_directory_listing_pages_work_in_onboarding(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_listing_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertEqual([row["display_name"] for row in entities["heroes"]], ["Ghost", "Soap"])
            self.assertEqual({row["display_name"] for row in candidates}, {"Ghost", "Soap"})
            self.assertEqual({row["source_kind"] for row in candidates}, {"category_member_image"})

    def test_call_of_duty_directory_detail_pages_keep_listing_rows_primary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_detail_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertEqual([row["display_name"] for row in entities["heroes"]], ["Ghost", "Soap"])
            self.assertEqual({row["display_name"] for row in candidates}, {"Ghost", "Soap"})
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row["role"], "support")
            self.assertEqual(ghost_row["aliases"], ["Simon Riley"])
            self.assertFalse(any(row["display_name"] == "Price" for row in entities["heroes"]))

    def test_call_of_duty_directory_detail_ambiguous_pages_emit_qa_and_skip_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_detail_ambiguous_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            soap_row = next(row for row in entities["heroes"] if row["display_name"] == "Soap")
            self.assertEqual(ghost_row.get("role", ""), "operator")
            self.assertEqual(soap_row.get("role", ""), "")
            self.assertTrue(any(row["item_type"] == "ambiguous_listing_detail_enrichment" for row in qa_rows))

    def test_call_of_duty_directory_detail_conflicting_pages_emit_qa_and_skip_conflicting_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_detail_conflicting_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "operator")
            self.assertEqual(ghost_row.get("aliases", []), ["simon riley"])
            self.assertTrue(
                any(
                    row["item_type"] == "conflicting_listing_detail_enrichment"
                    and row.get("display_name", "") == "Ghost"
                    for row in qa_rows
                )
            )

    def test_call_of_duty_directory_detail_existing_row_conflict_emits_qa_and_keeps_existing_value(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_detail_existing_conflict_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "tank")
            self.assertEqual(ghost_row.get("aliases", []), ["Spectre", "simon riley"])
            self.assertTrue(
                any(
                    row["item_type"] == "existing_listing_detail_enrichment_conflict"
                    and row.get("display_name", "") == "Ghost"
                    and "tank" in row.get("reason", "")
                    for row in qa_rows
                )
            )

    def test_call_of_duty_directory_detail_canonical_alias_is_rejected_with_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_detail_canonical_alias_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "support")
            self.assertEqual(ghost_row.get("aliases", []), ["simon riley"])
            self.assertTrue(
                any(
                    row["item_type"] == "alias_equivalent_to_canonical_name"
                    and row.get("display_name", "") == "Ghost"
                    and "ghost" in row.get("reason", "")
                    for row in qa_rows
                )
            )

    def test_call_of_duty_directory_detail_existing_alias_equivalence_is_rejected_with_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_detail_existing_alias_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            ghost_row = next(row for row in entities["heroes"] if row["display_name"] == "Ghost")
            self.assertEqual(ghost_row.get("role", ""), "tank")
            self.assertEqual(ghost_row.get("aliases", []), ["Spectre", "simon riley"])
            self.assertTrue(
                any(
                    row["item_type"] == "alias_equivalent_to_existing_alias"
                    and row.get("display_name", "") == "Ghost"
                    and "spectre" in row.get("reason", "")
                    for row in qa_rows
                )
            )

    def test_call_of_duty_directory_detail_conflict_suppresses_aliases_with_qa(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_directory_detail_conflicting_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            qa_rows = self._read_csv(draft_root / "catalog" / "qa_queue.csv")
            self.assertTrue(
                any(
                    row["item_type"] == "alias_suppressed_by_detail_conflict"
                    and row.get("display_name", "") == "Ghost"
                    and ("Simon Riley" in row.get("reason", "") or "Spectre" in row.get("reason", ""))
                    for row in qa_rows
                )
            )

    def test_call_of_duty_invalid_source_role_fails_per_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = repo_root / "bad_call_of_duty_sources.yaml"
            manifest_path.write_text(
                "\n".join(
                    [
                        "game: call_of_duty",
                        "sources:",
                        "  - role: medals",
                        '    url: "https://example.com/bad"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

    def test_call_of_duty_category_pages_work_in_onboarding(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_category_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            self.assertTrue(any(row["display_name"] == "Ghost" for row in entities["heroes"]))
            self.assertTrue(any(row["display_name"] == "Flash Grenade" for row in entities["abilities"]))
            self.assertTrue(any(row["display_name"] == "Triple Kill" for row in entities["events"]))

    def test_call_of_duty_card_grid_pages_work_in_onboarding(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_card_grid_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertTrue(any(row["display_name"] == "Ghost" for row in entities["heroes"]))
            self.assertTrue(any(row["display_name"] == "Flash Grenade" for row in entities["abilities"]))
            self.assertTrue(any(row["display_name"] == "Triple Kill" for row in entities["events"]))
            self.assertEqual([row["display_name"] for row in candidates], ["Ghost", "Flash Grenade", "Triple Kill"])

    def test_call_of_duty_table_hybrid_pages_work_in_onboarding(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            manifest_path = self._write_callofduty_table_hybrid_sources(repo_root)

            result = onboard_game_from_manifest("call_of_duty", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            candidates = self._read_csv(draft_root / "catalog" / "asset_candidates.csv")
            self.assertTrue(any(row["display_name"] == "Ghost" for row in entities["heroes"]))
            self.assertTrue(any(row["display_name"] == "Flash Grenade" for row in entities["abilities"]))
            self.assertTrue(any(row["display_name"] == "Triple Kill" for row in entities["events"]))
            self.assertEqual([row["display_name"] for row in candidates], ["Ghost", "Flash Grenade", "Triple Kill"])

    def test_valorant_schema_adaptation_uses_adapter_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_valorant_starter_seed(repo_root)

            result = adapt_game_schema("valorant", repo_root=repo_root)

            self.assertTrue(result["ok"])
            schema = load_yaml_file(Path(result["artifacts"]["game_detection_schema"]))
            self.assertFalse(schema["families"]["ability_icon"]["enabled"])
            self.assertFalse(schema["families"]["medal_icon"]["enabled"])
            self.assertEqual(schema["families"]["equipment_icon"]["threshold"], 0.92)
            self.assertIn("equipment_icon", schema["active_asset_families"])
            self.assertNotIn("ability_icon", schema["active_asset_families"])
            self.assertNotIn("medal_icon", schema["active_asset_families"])

    def test_valorant_full_onboarding_validation_and_batch_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_valorant_starter_seed(repo_root)
            manifest_path = self._write_valorant_sources(repo_root)

            result = onboard_game_from_manifest("valorant", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            draft_root = Path(result["draft_root"])
            entities = load_yaml_file(draft_root / "entities.yaml")
            hero_names = {row["display_name"] for row in entities["heroes"]}
            ability_rows = {row["display_name"]: row for row in entities["abilities"]}
            event_rows = {row["display_name"]: row for row in entities["events"]}
            self.assertEqual(hero_names, {"Jett", "Sage"})
            self.assertEqual(ability_rows["Vandal"]["class"], "equipment")
            self.assertEqual(event_rows["Radiant"]["category"], "outcome")

            detection_rows = load_yaml_file(draft_root / "manifests" / "detection_manifest.yaml")["rows"]
            self.assertTrue(any(row["target_display_name"] == "Jett" for row in detection_rows))
            self.assertTrue(any(row["target_display_name"] == "Vandal" and row["asset_family"] == "equipment_icon" for row in detection_rows))
            self.assertFalse(any(row["target_display_name"] == "Radiant" for row in detection_rows))

            readiness = validate_onboarding_publish(draft_root, repo_root=repo_root)
            self.assertTrue(readiness["ok"])
            self.assertIn(readiness["readiness"], {"needs_binding_review", "ready_to_publish"})

            batch = publish_onboarding_batch(repo_root / "assets" / "games", game="valorant", apply=False)
            self.assertTrue(batch["ok"])
            self.assertEqual(batch["draft_count"], 1)
            self.assertEqual(batch["selected_count"], 1)
            self.assertFalse(batch["published"])
            self.assertEqual(batch["summary"]["failed"], 0)

    def test_valorant_invalid_source_role_fails_per_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_valorant_starter_seed(repo_root)
            manifest_path = repo_root / "bad_valorant_sources.yaml"
            manifest_path.write_text(
                "\n".join(
                    [
                        "game: valorant",
                        "sources:",
                        "  - role: medals",
                        '    url: "https://example.com/bad"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                onboard_game_from_manifest("valorant", manifest_path, repo_root=repo_root)

    def test_onboarding_keeps_successful_sources_when_one_source_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            self._write_callofduty_starter_seed(repo_root)
            schema_result = adapt_game_schema("call_of_duty", repo_root=repo_root)
            image_root = repo_root / "images"
            image_root.mkdir(parents=True, exist_ok=True)
            ghost = image_root / "ghost.png"
            ghost.write_bytes(_ONE_BY_ONE_PNG)
            operators_path = repo_root / "operators.html"
            operators_path.write_text(
                f'<html><body><div class="mw-parser-output"><h2>Operators</h2><ul><li>Ghost</li></ul><img src="{ghost.resolve().as_uri()}" alt="Ghost operator portrait" /></div></body></html>',
                encoding="utf-8",
            )

            result = ingest_onboarding_sources(
                Path(schema_result["draft_root"]),
                [
                    OnboardingSource(role="operators", url=operators_path.resolve().as_uri()),
                    OnboardingSource(role="events", url="https://example.com/missing"),
                ],
                repo_root=repo_root,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["counts"]["source_failures"], 1)
            fetch_log = self._read_csv(Path(result["artifacts"]["source_fetch_log_csv"]))
            self.assertTrue(any(row["status"] == "fetch_failed" for row in fetch_log))
