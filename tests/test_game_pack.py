from __future__ import annotations

import unittest

from pipeline.game_pack import list_games, load_game_pack, validate_game_pack


class GamePackTests(unittest.TestCase):
    def test_list_games_includes_starter_asset_pack(self) -> None:
        self.assertIn("marvel_rivals", list_games())

    def test_validate_game_pack_uses_starter_assets_when_repo_assets_missing(self) -> None:
        result = validate_game_pack("marvel_rivals")
        self.assertTrue(result["ok"])
        self.assertEqual(result["source"], "starter_assets")

    def test_load_game_pack_returns_summary(self) -> None:
        pack = load_game_pack("marvel_rivals")
        summary = pack.summary()
        self.assertEqual(summary["game_id"], "marvel_rivals")
        self.assertGreaterEqual(summary["character_count"], 1)

