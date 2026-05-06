from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.contract_audit import audit_pipeline_contracts


class ContractAuditTests(unittest.TestCase):
    def _write_pack(self, root: Path, *, game: str, legacy: bool = False) -> None:
        game_root = root / "assets" / "games" / game
        (game_root / "manifests").mkdir(parents=True, exist_ok=True)
        (game_root / "templates" / "heroes").mkdir(parents=True, exist_ok=True)
        (game_root / "game.yaml").write_text(
            "\n".join(
                [
                    f"game_id: {game}",
                    'display_name: "Test Game"',
                    "resolution_profiles:",
                    '  normalize_to: "64x36"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "entities.yaml").write_text("heroes: []\nabilities: []\nevents: []\n", encoding="utf-8")
        (game_root / "medals.yaml").write_text("medals: []\n", encoding="utf-8")
        (game_root / "hud.yaml").write_text(
            "\n".join(
                [
                    "rois:",
                    "  hero_portrait:",
                    "    x_pct: 0.0",
                    "    y_pct: 0.0",
                    "    w_pct: 0.5",
                    "    h_pct: 0.5",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "weights.yaml").write_text("weights: {}\nthresholds: {}\ngates: {}\n", encoding="utf-8")
        (game_root / "manifests" / "assets_manifest.json").write_text(
            json.dumps({"game_id": game, "published_assets": []}, indent=2),
            encoding="utf-8",
        )
        (game_root / "manifests" / "cv_templates.yaml").write_text(
            "\n".join(
                [
                    "templates:",
                    f"  - asset_id: {game}.punisher.hero_portrait",
                    "    asset_family: hero_portrait",
                    '    display_name: "Punisher"',
                    "    entity_id: punisher",
                    "    roi_ref: hero_portrait",
                    '    template_path: "templates/heroes/punisher.png"',
                    '    match_method: "TM_CCOEFF_NORMED"',
                    "    threshold: 0.9",
                    "    temporal_window: 3",
                    "    scale_set: [1.0]",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        target_id_source = "asset_id_suffix" if legacy else "template_field"
        runtime_lines = [
            "event_mappings:",
            "  hero_portrait:",
            "    signal_type: character_identity",
            "    event_type: pov_character_identified",
            "    target_field: entity_id",
            f"    target_id_source: {target_id_source}",
            "    identity_competition: strongest_overlap",
        ]
        if not legacy:
            runtime_lines.append("    target_value_field: entity_id")
        (game_root / "manifests" / "runtime_cv_rules.yaml").write_text("\n".join(runtime_lines) + "\n", encoding="utf-8")
        (game_root / "manifests" / "fusion_rules.yaml").write_text(
            "\n".join(
                [
                    "schema_version: fusion_rules_v1",
                    "rules:",
                    "  - rule_id: character_identity_atomic",
                    "    event_type: pov_character_identified",
                    '    signal_types: ["character_identity"]',
                    '    required_signal_types: ["character_identity"]',
                    "    window_seconds: 0.5",
                    "    min_signal_count: 1",
                    "    confidence_method: max",
                    '    group_by: ["entity_id"]',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "manifests" / "detection_manifest.yaml").write_text(
            "\n".join(
                [
                    "schema_version: game_detection_manifest_v1",
                    "baseline_schema_version: runtime_detection_schema_v1",
                    f"game_id: {game}",
                    "row_count: 1",
                    "required_row_count: 1",
                    "ready_row_count: 1",
                    "rows_needing_assets: 0",
                    "rows:",
                    f"  - detection_id: {game}.punisher.hero_portrait",
                    f"    game_id: {game}",
                    "    target_kind: hero",
                    "    target_id: punisher",
                    '    target_display_name: "Punisher"',
                    "    ontology_collection: heroes",
                    "    asset_family: hero_portrait",
                    "    requires_asset: true",
                    '    required_semantic_fields: ["entity_id"]',
                    "    template_semantics:",
                    "      entity_id: punisher",
                    "    runtime_rule:",
                    "      signal_type: character_identity",
                    "      event_type: pov_character_identified",
                    "      target_field: entity_id",
                    "      target_id_source: template_field",
                    "      target_value_field: entity_id",
                    "      identity_competition: strongest_overlap",
                    '    fusion_rule_ids: ["character_identity_atomic"]',
                    "    status: ready_for_binding",
                    "    binding_status: accepted",
                    "    asset_status: published",
                    f"    published_asset_id: {game}.punisher.hero_portrait",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "templates" / "heroes" / "punisher.png").write_bytes(b"template")

    def test_contract_audit_identifies_canonical_and_legacy_packs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            self._write_pack(root, game="marvel_rivals", legacy=False)
            self._write_pack(root, game="legacy_game", legacy=True)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.roi_matcher._template_dimensions", return_value=(10, 10)):
                result = audit_pipeline_contracts(
                    repo_root=root,
                    config_payload={"proxy_scanner": {"signals": {"audio_prepass": {"enabled": True}}}},
                )
        self.assertTrue(result["ok"])
        by_game = {row["game"]: row for row in result["pack_contracts"] if "game" in row}
        self.assertEqual(by_game["marvel_rivals"]["contract_status"], "canonical")
        self.assertEqual(by_game["legacy_game"]["contract_status"], "legacy_assisted")
        self.assertEqual(by_game["marvel_rivals"]["ontology_status"], "ok")
        self.assertTrue(any(row["status"] == "legacy_proxy_signals_config" for row in result["legacy_usage"]))
