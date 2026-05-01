from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.game_onboarding import onboard_game_from_manifest, publish_onboarding_draft
from pipeline.game_pack import load_game_pack


_ONE_BY_ONE_PNG = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\rIDATx\x9cc`\x00\x00\x00\x02\x00\x01\xe2!\xbc3"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


class GameOnboardingTests(unittest.TestCase):
    def _write_sources(self, root: Path, *, plain_local_paths: bool = False) -> Path:
        image_root = root / "images"
        image_root.mkdir(parents=True, exist_ok=True)
        punisher = image_root / "punisher.png"
        final_judgment = image_root / "final_judgment.png"
        triple_ko = image_root / "triple_ko.png"
        for path in (punisher, final_judgment, triple_ko):
            path.write_bytes(_ONE_BY_ONE_PNG)

        roster_html = f"""
        <html><body><div class="mw-parser-output">
          <h2>Roster</h2>
          <ul>
            <li>The Punisher</li>
            <li>Mantis</li>
          </ul>
          <img src="{punisher.resolve().as_uri()}" alt="The Punisher portrait" />
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
                    f"  - role: abilities",
                    f"    url: \"{_ref(abilities_path)}\"",
                    f"  - role: medals",
                    f"    url: \"{_ref(medals_path)}\"",
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

    def test_onboarding_manifest_requires_valid_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            manifest_path = repo_root / "bad_sources.yaml"
            manifest_path.write_text("game: marvel_rivals\nsources: {}\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)

    def test_onboarding_run_creates_draft_bundle_with_candidates_and_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["counts"]["heroes"], 1)
            self.assertGreaterEqual(result["counts"]["abilities"], 1)
            self.assertGreaterEqual(result["counts"]["events"], 1)
            self.assertGreaterEqual(result["counts"]["candidate_assets"], 3)
            self.assertGreaterEqual(result["counts"]["binding_candidates"], 3)

            draft_root = Path(result["draft_root"])
            self.assertTrue((draft_root / "entities.yaml").exists())
            self.assertTrue((draft_root / "manifests" / "assets_manifest.json").exists())
            self.assertTrue((draft_root / "catalog" / "qa_queue.csv").exists())

    def test_onboarding_accepts_plain_local_source_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            manifest_path = self._write_sources(repo_root, plain_local_paths=True)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            self.assertTrue(result["ok"])

    def test_publish_only_accepted_bindings_generate_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            manifest_path = self._write_sources(repo_root)
            result = onboard_game_from_manifest("marvel_rivals", manifest_path, repo_root=repo_root)
            draft_root = Path(result["draft_root"])

            first_publish = publish_onboarding_draft(draft_root, repo_root=repo_root)
            self.assertTrue(first_publish["ok"])
            cv_templates_path = Path(first_publish["artifacts"]["cv_templates"])
            self.assertIn("templates: []", cv_templates_path.read_text(encoding="utf-8"))

            self._accept_all_bindings(draft_root)
            second_publish = publish_onboarding_draft(draft_root, repo_root=repo_root)
            self.assertTrue(second_publish["ok"])
            self.assertGreater(second_publish["template_count"], 0)
            published_templates = Path(second_publish["artifacts"]["cv_templates"]).read_text(encoding="utf-8")
            self.assertIn("asset_id:", published_templates)

    def test_load_game_pack_supports_published_pack_structure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
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
