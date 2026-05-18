from __future__ import annotations

import json
import os
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

    def test_contract_audit_reports_governance_surface_health(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            self._write_pack(root, game="marvel_rivals", legacy=False)
            (root / "AGENTS.md").write_text(
                "\n".join(
                    [
                        "# AGENTS.md",
                        "## Decision Hierarchy",
                        "## Heuristic Placement Rules",
                        "## Anti-Bloat Rules",
                        "## Validation Requirements",
                        "## Escalation Rules",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            docs_root = root / "docs" / "v2"
            docs_root.mkdir(parents=True, exist_ok=True)
            (docs_root / "INDEX.md").write_text(
                "\n".join(
                    [
                        "# V2 Source of Truth Index",
                        "ENGINEERING_GOVERNANCE.md",
                        "QUALITY_MAINTENANCE.md",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (docs_root / "ENGINEERING_GOVERNANCE.md").write_text(
                "\n".join(
                    [
                        "# Engineering Governance",
                        "## Decision Classes",
                        "## Heuristic Placement Model",
                        "## Acceptable Placement",
                        "## Unacceptable Placement",
                        "## Validation Spine",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (docs_root / "QUALITY_MAINTENANCE.md").write_text(
                "\n".join(
                    [
                        "# Quality Maintenance",
                        "## Maintenance Classes",
                        "## Recurring Checks",
                        "## Drift Signals",
                        "## Triggered Actions",
                        "## Long-Run Data-Quality Preservation",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.roi_matcher._template_dimensions", return_value=(10, 10)):
                result = audit_pipeline_contracts(repo_root=root)

        by_surface = {row["surface"]: row for row in result["governance_surfaces"]}
        self.assertEqual(by_surface["agents"]["status"], "ok")
        self.assertEqual(by_surface["v2_index"]["status"], "ok")
        self.assertEqual(by_surface["engineering_governance"]["status"], "ok")
        self.assertEqual(by_surface["quality_maintenance"]["status"], "ok")
        self.assertFalse(any(row["status"].endswith("governance_surface") for row in result["warnings"]))

    def test_contract_audit_warns_when_governance_surface_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            self._write_pack(root, game="marvel_rivals", legacy=False)
            (root / "AGENTS.md").write_text("# AGENTS.md\n## Decision Hierarchy\n", encoding="utf-8")
            docs_root = root / "docs" / "v2"
            docs_root.mkdir(parents=True, exist_ok=True)
            (docs_root / "INDEX.md").write_text("# V2 Source of Truth Index\n", encoding="utf-8")
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.roi_matcher._template_dimensions", return_value=(10, 10)):
                result = audit_pipeline_contracts(repo_root=root)

        warnings = {row["status"] for row in result["warnings"]}
        self.assertIn("incomplete_governance_surface", warnings)
        self.assertIn("missing_governance_surface", warnings)

    def test_contract_audit_flags_draft_only_manifest_in_published_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            self._write_pack(root, game="marvel_rivals", legacy=False)
            (
                root / "assets" / "games" / "marvel_rivals" / "manifests" / "derived_detection_manifest.yaml"
            ).write_text("schema_version: derived_game_detection_manifest_v1\n", encoding="utf-8")
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.roi_matcher._template_dimensions", return_value=(10, 10)):
                result = audit_pipeline_contracts(repo_root=root)

        self.assertTrue(
            any(row["status"] == "draft_only_manifest_in_published_pack" for row in result["manifest_authority_findings"])
        )
        self.assertTrue(any(row["status"] == "draft_only_manifest_in_published_pack" for row in result["warnings"]))

    def test_contract_audit_warns_when_manifest_contract_doc_misses_schema_versions(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            self._write_pack(root, game="marvel_rivals", legacy=False)
            docs_root = root / "docs" / "v2"
            docs_root.mkdir(parents=True, exist_ok=True)
            (docs_root / "MANIFEST_CONTRACTS.md").write_text(
                "# Manifest Contracts / Game Packs\n\nNo schema version catalog here.\n",
                encoding="utf-8",
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.roi_matcher._template_dimensions", return_value=(10, 10)):
                result = audit_pipeline_contracts(repo_root=root)

        self.assertEqual(result["schema_documentation_findings"][0]["status"], "missing_schema_version_docs")
        self.assertTrue(any(row["status"] == "undocumented_canonical_schema_version" for row in result["warnings"]))

    def test_contract_audit_warns_when_schema_version_is_neither_registry_managed_nor_local_only(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            self._write_pack(root, game="marvel_rivals", legacy=False)
            pipeline_root = root / "pipeline"
            pipeline_root.mkdir(parents=True, exist_ok=True)
            (pipeline_root / "clip_registry.py").write_text(
                '\n'.join(
                    [
                        'PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (pipeline_root / "rogue_surface.py").write_text(
                '\n'.join(
                    [
                        'ROGUE_SURFACE_SCHEMA_VERSION = "rogue_surface_v1"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            docs_root = root / "docs" / "v2"
            docs_root.mkdir(parents=True, exist_ok=True)
            (docs_root / "REGISTRY_ORCHESTRATION_STATE.md").write_text(
                "\n".join(
                    [
                        "# Registry / Orchestration / State",
                        "## Registry-Managed Schema Ownership",
                        "## Explicit Local-Only Schema Scopes",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.roi_matcher._template_dimensions", return_value=(10, 10)):
                result = audit_pipeline_contracts(repo_root=root)

        self.assertTrue(any(row["status"] == "unscoped_schema_version" for row in result["registry_schema_findings"]))
        self.assertTrue(any(row["status"] == "unscoped_registry_schema_version" for row in result["warnings"]))

    def test_contract_audit_accepts_documented_local_only_schema_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            self._write_pack(root, game="marvel_rivals", legacy=False)
            pipeline_root = root / "pipeline"
            pipeline_root.mkdir(parents=True, exist_ok=True)
            (pipeline_root / "clip_registry.py").write_text(
                '\n'.join(
                    [
                        'PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (pipeline_root / "local_helper.py").write_text(
                '\n'.join(
                    [
                        'RESEARCH_RUNTIME_TRACE_SCHEMA_VERSION = "research_runtime_turn_trace_v1"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            docs_root = root / "docs" / "v2"
            docs_root.mkdir(parents=True, exist_ok=True)
            (docs_root / "REGISTRY_ORCHESTRATION_STATE.md").write_text(
                "\n".join(
                    [
                        "# Registry / Orchestration / State",
                        "## Registry-Managed Schema Ownership",
                        "## Explicit Local-Only Schema Scopes",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.roi_matcher._template_dimensions", return_value=(10, 10)):
                result = audit_pipeline_contracts(repo_root=root)

        self.assertFalse(any(row["status"] == "unscoped_schema_version" for row in result["registry_schema_findings"]))

    def test_contract_audit_reports_stale_fixture_freshness_as_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            fixtures_root = root / "tests" / "fixtures" / "fusion_goldsets" / "marvel_rivals"
            fixtures_root.mkdir(parents=True, exist_ok=True)
            fixture_path = fixtures_root / "sample.fusion_goldset.json"
            fixture_path.write_text('{"schema_version":"fusion_goldset_clip_v1"}\n', encoding="utf-8")
            assets_root = root / "assets" / "games" / "marvel_rivals" / "manifests"
            assets_root.mkdir(parents=True, exist_ok=True)
            detection_manifest = assets_root / "detection_manifest.yaml"
            detection_manifest.write_text("schema_version: game_detection_manifest_v1\n", encoding="utf-8")
            fusion_rules = assets_root / "fusion_rules.yaml"
            fusion_rules.write_text("schema_version: fusion_rules_v1\n", encoding="utf-8")
            pipeline_root = root / "pipeline"
            pipeline_root.mkdir(parents=True, exist_ok=True)
            validation_surface = pipeline_root / "fusion_validation.py"
            validation_surface.write_text("SUPPORTED_GOLDSET_SCHEMA_VERSION = 'fusion_goldset_clip_v1'\n", encoding="utf-8")
            os.utime(fixture_path, (1000, 1000))
            os.utime(detection_manifest, (2000, 2000))
            os.utime(fusion_rules, (2000, 2000))
            os.utime(validation_surface, (2000, 2000))

            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = audit_pipeline_contracts(repo_root=root)

        self.assertTrue(any(row["status"] == "fixture_refresh_recommended" for row in result["quality_maintenance_findings"]))
        self.assertTrue(
            any(row["status"] == "quality_maintenance_fixture_refresh_recommended" for row in result["warnings"])
        )

    def test_contract_audit_reports_review_backlog_severity(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            review_root = root / "assets" / "games" / "test_game" / "drafts" / "onboarding" / "draft1" / "review" / "derived_row_reviews"
            review_root.mkdir(parents=True, exist_ok=True)
            for index in range(10):
                payload = {"review_status": "approved" if index < 7 else "unreviewed"}
                (review_root / f"row_{index}.json").write_text(json.dumps(payload), encoding="utf-8")

            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = audit_pipeline_contracts(repo_root=root)

        backlog_rows = [row for row in result["quality_maintenance_findings"] if row["surface"] == "review_backlog"]
        self.assertEqual(len(backlog_rows), 1)
        self.assertEqual(backlog_rows[0]["status"], "review_backlog_high")
        self.assertEqual(backlog_rows[0]["severity"], "blocking")
        self.assertTrue(any(row["status"] == "quality_maintenance_review_backlog_high" for row in result["warnings"]))

    def test_contract_audit_reports_stale_review_files_when_detection_rows_disappear(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "starter_assets").mkdir(parents=True, exist_ok=True)
            review_root = root / "assets" / "games" / "test_game" / "drafts" / "onboarding" / "draft1" / "review" / "derived_row_reviews"
            review_root.mkdir(parents=True, exist_ok=True)
            detection_manifest_root = root / "assets" / "games" / "test_game" / "drafts" / "onboarding" / "draft1" / "manifests"
            detection_manifest_root.mkdir(parents=True, exist_ok=True)
            (detection_manifest_root / "derived_detection_manifest.yaml").write_text(
                "\n".join(
                    [
                        "rows:",
                        "  - detection_id: test_game.live_row.hero_portrait",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (review_root / "stale.review.json").write_text(
                json.dumps(
                    {
                        "detection_id": "test_game.removed_row.hero_portrait",
                        "review_status": "approved",
                        "review_decision": "defer_row",
                        "candidate_option_count": 0,
                    }
                ),
                encoding="utf-8",
            )

            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = audit_pipeline_contracts(repo_root=root)

        drift_rows = [row for row in result["quality_maintenance_findings"] if row["surface"] == "review_file_drift"]
        self.assertEqual(len(drift_rows), 1)
        self.assertEqual(drift_rows[0]["status"], "stale_review_files_present")
        self.assertEqual(drift_rows[0]["stale_review_count"], 1)
        self.assertTrue(any(row["status"] == "quality_maintenance_stale_review_files_present" for row in result["warnings"]))
