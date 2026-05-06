from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.fusion_analysis import (
    FusionAnalysisError,
    fuse_analysis,
    load_fusion_rules,
    normalize_fusion_signals,
    normalize_proxy_signals,
    normalize_runtime_signals,
)
from run import main as run_main
from run import run_fuse_clip_signals


class FusionAnalysisTests(unittest.TestCase):
    def _write_published_pack(self, root: Path, *, rules_text: str) -> None:
        game_root = root / "assets" / "games" / "marvel_rivals"
        (game_root / "manifests").mkdir(parents=True, exist_ok=True)
        (game_root / "game.yaml").write_text(
            "\n".join(
                [
                    "game_id: marvel_rivals",
                    'display_name: "Marvel Rivals"',
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
            json.dumps({"game_id": "marvel_rivals", "published_assets": []}, indent=2),
            encoding="utf-8",
        )
        (game_root / "manifests" / "cv_templates.yaml").write_text(
            "\n".join(
                [
                    "templates:",
                    "  - asset_id: marvel_rivals.punisher.hero_portrait",
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
        (game_root / "manifests" / "runtime_cv_rules.yaml").write_text(
            "\n".join(
                [
                    "event_mappings:",
                    "  hero_portrait:",
                    "    signal_type: character_identity",
                    "    event_type: pov_character_identified",
                    "    target_field: entity_id",
                    "    target_id_source: template_field",
                    "    target_value_field: entity_id",
                    "    identity_competition: strongest_overlap",
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
                    "game_id: marvel_rivals",
                    "row_count: 1",
                    "required_row_count: 1",
                    "ready_row_count: 1",
                    "rows_needing_assets: 0",
                    "rows:",
                    "  - detection_id: marvel_rivals.punisher.hero_portrait",
                    "    game_id: marvel_rivals",
                    "    target_kind: hero",
                    "    target_id: punisher",
                    '    target_display_name: "Punisher"',
                    "    ontology_collection: heroes",
                    "    asset_family: hero_portrait",
                    "    requires_asset: true",
                    '    required_semantic_fields: ["entity_id"]',
                    "    template_semantics:",
                    "      entity_id: punisher",
                    "    status: ready_for_binding",
                    "    binding_status: accepted",
                    "    asset_status: published",
                    "    published_asset_id: marvel_rivals.punisher.hero_portrait",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "manifests" / "fusion_rules.yaml").write_text(rules_text, encoding="utf-8")
        template_path = game_root / "templates" / "heroes" / "punisher.png"
        template_path.parent.mkdir(parents=True, exist_ok=True)
        template_path.write_bytes(b"template")

    def _proxy_sidecar(self) -> dict[str, object]:
        return {
            "schema_version": "proxy_scan_v1",
            "scan_id": "scan-001",
            "ok": True,
            "game": "marvel_rivals",
            "source": "/tmp/example.mp4",
            "signal_count": 2,
            "window_count": 1,
            "signals": [
                {
                    "source": "audio_spike",
                    "source_family": "audio_prepass",
                    "timestamp": 5.1,
                    "strength": 0.8,
                    "confidence": 0.7,
                    "reason": "peak",
                },
                {
                    "source": "chat_spike",
                    "source_family": "chat_velocity",
                    "timestamp": 5.3,
                    "strength": 0.6,
                    "confidence": 0.62,
                    "reason": "burst",
                },
            ],
            "windows": [
                {
                    "start_seconds": 4.5,
                    "end_seconds": 6.0,
                    "proxy_score": 0.82,
                    "signal_count": 2,
                    "sources": ["audio_spike", "chat_spike"],
                    "source_families": ["audio_prepass", "chat_velocity"],
                    "recommended_action": "inspect",
                    "signals": [],
                    "explanation": [],
                }
            ],
            "source_results": {
                "audio_prepass": {"status": "ok", "signal_count": 1},
                "chat_velocity": {"status": "ok", "signal_count": 1},
            },
            "sidecar_path": "/tmp/example.proxy_scan.json",
        }

    def _runtime_sidecar(self) -> dict[str, object]:
        return {
            "schema_version": "runtime_analysis_v1",
            "analysis_id": "runtime-001",
            "ok": True,
            "game": "marvel_rivals",
            "source": "/tmp/example.mp4",
            "matcher": {
                "status": "ok",
                "signals": [
                    {
                        "signal_id": "sig-medal",
                        "signal_type": "medal_visibility",
                        "event_type": "medal_seen",
                        "timestamp": 5.05,
                        "start_timestamp": 4.95,
                        "end_timestamp": 5.15,
                        "asset_id": "marvel_rivals.double_kill.medal_icon",
                        "asset_family": "medal_icon",
                        "roi_ref": "center_badge",
                        "confidence": 0.92,
                        "event_row_id": "double_kill",
                        "evidence": {"peak_score": 0.92, "supporting_frames": 4},
                        "source_detection_count": 4,
                        "producer": "runtime_cv_template_matcher",
                        "producer_family": "runtime",
                        "source_ref": "/tmp/example.mp4",
                    },
                    {
                        "signal_id": "sig-ability",
                        "signal_type": "ability_visibility",
                        "event_type": "ability_seen",
                        "timestamp": 4.9,
                        "start_timestamp": 4.8,
                        "end_timestamp": 5.0,
                        "asset_id": "marvel_rivals.punisher.ultimate.ability_icon",
                        "asset_family": "ability_icon",
                        "roi_ref": "ability_bar",
                        "confidence": 0.88,
                        "ability_id": "punisher_ultimate",
                        "evidence": {"peak_score": 0.88, "supporting_frames": 3},
                        "source_detection_count": 3,
                        "producer": "runtime_cv_template_matcher",
                        "producer_family": "runtime",
                        "source_ref": "/tmp/example.mp4",
                    },
                ],
            },
            "events": {
                "status": "ok",
                "signal_count": 2,
                "event_count": 2,
                "rows": [],
            },
            "sidecar_path": "/tmp/example.runtime_analysis.json",
        }

    def test_proxy_signals_normalize_into_shared_contract(self) -> None:
        rows = normalize_proxy_signals(self._proxy_sidecar())

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["producer_family"], "proxy")
        self.assertEqual(rows[0]["producer"], "audio_prepass")
        self.assertEqual(rows[0]["signal_type"], "audio_spike")
        self.assertEqual(rows[0]["start_timestamp"], rows[0]["timestamp"])
        self.assertIn("source_ref", rows[0])
        self.assertIsInstance(rows[0]["evidence"], dict)
        self.assertEqual(rows[0]["evidence"]["matching_windows"][0]["recommended_action"], "inspect")

    def test_runtime_signals_normalize_preserving_semantic_targets(self) -> None:
        rows = normalize_runtime_signals(self._runtime_sidecar())

        self.assertEqual(len(rows), 2)
        medal_row = next(row for row in rows if row["signal_type"] == "medal_visibility")
        ability_row = next(row for row in rows if row["signal_type"] == "ability_visibility")
        self.assertEqual(medal_row["event_row_id"], "double_kill")
        self.assertEqual(ability_row["ability_id"], "punisher_ultimate")
        self.assertEqual(ability_row["producer_family"], "runtime")
        self.assertEqual(ability_row["producer"], "runtime_cv_template_matcher")
        self.assertEqual(ability_row["source_ref"], "/tmp/example.mp4")
        self.assertEqual(ability_row["source_family"], "ability_icon")
        self.assertTrue(float(ability_row["start_timestamp"]) <= float(ability_row["timestamp"]) <= float(ability_row["end_timestamp"]))

    def test_normalize_fusion_signals_sorts_rows_using_shared_contract(self) -> None:
        rows = normalize_fusion_signals(
            proxy_sidecar=self._proxy_sidecar(),
            runtime_sidecar=self._runtime_sidecar(),
        )

        self.assertEqual([row["signal_type"] for row in rows], ["ability_visibility", "medal_visibility", "audio_spike", "chat_spike"])
        for row in rows:
            self.assertIn("producer", row)
            self.assertIn("source_ref", row)
            self.assertIsInstance(row["evidence"], dict)
            self.assertLessEqual(float(row["start_timestamp"]), float(row["end_timestamp"]))
            self.assertLessEqual(float(row["start_timestamp"]), float(row["timestamp"]))
            self.assertLessEqual(float(row["timestamp"]), float(row["end_timestamp"]))

    def test_runtime_signals_reject_invalid_normalized_time_window(self) -> None:
        runtime_sidecar = self._runtime_sidecar()
        runtime_sidecar["matcher"]["signals"][0]["start_timestamp"] = 5.2
        runtime_sidecar["matcher"]["signals"][0]["end_timestamp"] = 5.1

        with self.assertRaises(FusionAnalysisError) as exc:
            normalize_runtime_signals(runtime_sidecar)
        self.assertEqual(exc.exception.status, "invalid_normalized_signal_contract")

    def test_load_fusion_rules_rejects_unknown_signal_type(self) -> None:
        rules_text = (
            "rules:\n"
            "  - rule_id: bad\n"
            "    event_type: impossible\n"
            '    signal_types: ["unknown_signal"]\n'
            "    window_seconds: 1.0\n"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                with self.assertRaises(FusionAnalysisError) as exc:
                    load_fusion_rules("marvel_rivals")
        self.assertEqual(exc.exception.status, "invalid_fusion_rules")

    def test_load_fusion_rules_rejects_unknown_event_type(self) -> None:
        rules_text = (
            "rules:\n"
            "  - rule_id: bad\n"
            "    event_type: unknown_event\n"
            '    signal_types: ["medal_visibility"]\n'
            "    window_seconds: 1.0\n"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                with self.assertRaises(FusionAnalysisError) as exc:
                    load_fusion_rules("marvel_rivals")
        self.assertEqual(exc.exception.status, "invalid_fusion_rules")

    def test_load_fusion_rules_rejects_invalid_group_by_field(self) -> None:
        rules_text = (
            "rules:\n"
            "  - rule_id: bad\n"
            "    event_type: medal_seen\n"
            '    signal_types: ["medal_visibility"]\n'
            '    required_signal_types: ["medal_visibility"]\n'
            "    window_seconds: 1.0\n"
            '    group_by: ["unknown_key"]\n'
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                with self.assertRaises(FusionAnalysisError) as exc:
                    load_fusion_rules("marvel_rivals")
        self.assertEqual(exc.exception.status, "invalid_fusion_rules")

    def test_fuse_analysis_emits_atomic_and_composite_events(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: medal_atomic",
                "    event_type: medal_seen",
                '    signal_types: ["medal_visibility"]',
                '    required_signal_types: ["medal_visibility"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 1",
                "    confidence_method: max",
                '    group_by: ["event_row_id"]',
                "  - rule_id: ability_medal_combo",
                "    event_type: ability_plus_medal_combo",
                '    signal_types: ["ability_visibility", "medal_visibility", "audio_spike"]',
                '    required_signal_types: ["ability_visibility", "medal_visibility"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 2",
                "    confidence_method: mean",
                "    corroboration_bonus_per_extra_signal: 0.05",
                "    max_bonus: 0.10",
                "    metadata:",
                "      kind: composite",
            ]
        ) + "\n"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            output_root = root / "outputs" / "fused_analysis"
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.fusion_analysis.DEFAULT_OUTPUT_ROOT", output_root):
                result = fuse_analysis(
                    "/tmp/example.mp4",
                    "marvel_rivals",
                    proxy_sidecar=self._proxy_sidecar(),
                    runtime_sidecar=self._runtime_sidecar(),
                )
                self.assertTrue(Path(result["sidecar_path"]).exists())
        event_types = {row["event_type"] for row in result["fused_events"]}
        self.assertIn("medal_seen", event_types)
        self.assertIn("ability_plus_medal_combo", event_types)
        combo_row = next(row for row in result["fused_events"] if row["event_type"] == "ability_plus_medal_combo")
        self.assertIn("sig-medal", combo_row["contributing_signals"])
        self.assertIn("sig-ability", combo_row["contributing_signals"])
        self.assertIn("audio_prepass", combo_row["contributing_sources"])
        self.assertEqual(combo_row["gate_status"], "not_applicable")
        self.assertEqual(combo_row["metadata"]["rule_id"], "ability_medal_combo")
        self.assertEqual(combo_row["metadata"]["rule_parameters"]["window_seconds"], 0.5)
        self.assertEqual(combo_row["metadata"]["rule_parameters"]["confidence_method"], "mean")
        self.assertIn("matched_signal_types", combo_row["metadata"])
        self.assertIsInstance(combo_row["metadata"]["matched_signal_types"], list)
        self.assertLessEqual(combo_row["start_timestamp"], combo_row["anchor_timestamp"])
        self.assertLessEqual(combo_row["anchor_timestamp"], combo_row["end_timestamp"])
        self.assertLessEqual(combo_row["suggested_start_timestamp"], combo_row["suggested_end_timestamp"])
        self.assertEqual(result["fusion_summary"]["rule_parameters_by_id"]["ability_medal_combo"]["window_seconds"], 0.5)
        self.assertEqual(result["fusion_summary"]["rule_parameters_by_id"]["ability_medal_combo"]["confidence_method"], "mean")

    def test_penalty_logic_reduces_confidence_for_low_confidence_evidence(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: noisy_combo",
                "    event_type: high_action_sequence",
                '    signal_types: ["ability_visibility", "medal_visibility", "audio_spike"]',
                '    required_signal_types: ["ability_visibility", "medal_visibility"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 2",
                "    confidence_method: mean",
                "    low_confidence_threshold: 0.75",
                "    low_confidence_penalty: 0.10",
            ]
        ) + "\n"
        proxy_sidecar = self._proxy_sidecar()
        proxy_sidecar["signals"][0]["confidence"] = 0.4
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            gate_matches_path = root / "debug" / "gate_matches.json"
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = fuse_analysis(
                    "/tmp/example.mp4",
                    "marvel_rivals",
                    proxy_sidecar=proxy_sidecar,
                    runtime_sidecar=self._runtime_sidecar(),
                    output_path=root / "fusion.json",
                )
        row = result["fused_events"][0]
        self.assertLess(row["confidence"], 0.75)
        self.assertGreater(row["entropy"], 0.0)
        self.assertEqual(row["penalties"][0]["type"], "low_confidence_signals")

    def test_gated_rule_confirms_and_emits_clip_boundaries(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: medal_reaction_gate",
                "    event_type: medal_seen",
                '    signal_types: ["medal_visibility"]',
                '    required_signal_types: ["medal_visibility"]',
                '    anchor_signal_types: ["medal_visibility"]',
                '    dependent_signal_types: ["chat_spike", "audio_spike"]',
                "    window_seconds: 0.5",
                "    lag_window_seconds: 0.4",
                "    min_signal_count: 1",
                "    confidence_method: max",
                "    confirm_multiplier: 1.2",
                "    ambiguous_confidence_multiplier: 0.8",
                "    require_for_confirm: true",
                "    clip_start_lead_seconds: 6.0",
                "    clip_end_lag_seconds: 1.5",
                '    group_by: ["event_row_id"]',
            ]
        ) + "\n"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            gate_matches_path = root / "debug" / "gate_matches.json"
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = fuse_analysis(
                    "/tmp/example.mp4",
                    "marvel_rivals",
                    proxy_sidecar=self._proxy_sidecar(),
                    runtime_sidecar=self._runtime_sidecar(),
                    debug_output_dir=root / "debug",
                )
                self.assertTrue(gate_matches_path.exists())
        row = result["fused_events"][0]
        self.assertEqual(row["gate_status"], "confirmed")
        self.assertEqual(row["dependent_signal_ids"], ["scan-001:proxy:0", "scan-001:proxy:1"])
        self.assertAlmostEqual(row["base_confidence"], 0.92, places=5)
        self.assertAlmostEqual(row["confidence"], 1.0, places=5)
        self.assertAlmostEqual(row["multiplier_applied"], 1.2, places=5)
        self.assertAlmostEqual(row["suggested_start_timestamp"], 0.0, places=5)
        self.assertAlmostEqual(row["suggested_end_timestamp"], 6.8, places=5)
        self.assertEqual(row["metadata"]["rule_parameters"]["lag_window_seconds"], 0.4)
        self.assertEqual(row["metadata"]["rule_parameters"]["clip_start_lead_seconds"], 6.0)
        self.assertEqual(row["metadata"]["rule_parameters"]["clip_end_lag_seconds"], 1.5)
        self.assertEqual(row["metadata"]["rule_parameters"]["confirm_multiplier"], 1.2)
        self.assertEqual(row["metadata"]["rule_parameters"]["ambiguous_confidence_multiplier"], 0.8)
        rule_match = result["rule_matches"][0]
        self.assertEqual(rule_match["rule_parameters"]["lag_window_seconds"], 0.4)
        self.assertEqual(rule_match["rule_parameters"]["clip_start_lead_seconds"], 6.0)
        self.assertEqual(rule_match["rule_parameters"]["clip_end_lag_seconds"], 1.5)
        self.assertEqual(result["fusion_summary"]["gate_status_counts"]["confirmed"], 1)
        self.assertEqual(result["fusion_summary"]["rule_parameters_by_id"]["medal_reaction_gate"]["confirm_multiplier"], 1.2)
        self.assertEqual(result["fusion_summary"]["rule_parameters_by_id"]["medal_reaction_gate"]["ambiguous_confidence_multiplier"], 0.8)

    def test_gated_rule_becomes_ambiguous_without_dependent_signal(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: medal_reaction_gate",
                "    event_type: medal_seen",
                '    signal_types: ["medal_visibility"]',
                '    required_signal_types: ["medal_visibility"]',
                '    anchor_signal_types: ["medal_visibility"]',
                '    dependent_signal_types: ["chat_spike"]',
                "    window_seconds: 0.5",
                "    lag_window_seconds: 0.1",
                "    min_signal_count: 1",
                "    confidence_method: max",
                "    confirm_multiplier: 1.5",
                "    ambiguous_confidence_multiplier: 0.5",
                "    require_for_confirm: true",
                "    clip_start_lead_seconds: 2.0",
                "    clip_end_lag_seconds: 4.0",
                '    group_by: ["event_row_id"]',
            ]
        ) + "\n"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = fuse_analysis(
                    "/tmp/example.mp4",
                    "marvel_rivals",
                    proxy_sidecar=self._proxy_sidecar(),
                    runtime_sidecar=self._runtime_sidecar(),
                    output_path=root / "fusion.json",
                )
        row = result["fused_events"][0]
        self.assertEqual(row["gate_status"], "ambiguous")
        self.assertEqual(row["dependent_signal_ids"], [])
        self.assertAlmostEqual(row["confidence"], 0.46, places=5)
        self.assertAlmostEqual(row["suggested_start_timestamp"], 2.95, places=5)
        self.assertAlmostEqual(row["suggested_end_timestamp"], 8.95, places=5)

    def test_fuse_analysis_rejects_invalid_fused_event_contract(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: medal_atomic",
                "    event_type: medal_seen",
                '    signal_types: ["medal_visibility"]',
                '    required_signal_types: ["medal_visibility"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 1",
                "    confidence_method: max",
                '    group_by: ["event_row_id"]',
            ]
        ) + "\n"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.fusion_analysis._build_fused_event") as build_event:
                valid_event = {
                    "event_id": "event-1",
                    "event_type": "medal_seen",
                    "start_timestamp": 5.2,
                    "end_timestamp": 5.1,
                    "timestamp": 5.15,
                    "base_confidence": 0.92,
                    "post_gate_confidence": 0.92,
                    "confidence": 0.92,
                    "final_score": 0.92,
                    "entropy": 0.0,
                    "gate_status": "not_applicable",
                    "anchor_timestamp": 5.15,
                    "lag_window_seconds": None,
                    "multiplier_applied": 1.0,
                    "dependent_signal_ids": [],
                    "dependent_signal_types": [],
                    "synergy_applied": False,
                    "synergy_score": 0.0,
                    "synergy_multiplier": 1.0,
                    "synergy_matches": [],
                    "minimum_required_signals_met": True,
                    "suggested_start_timestamp": 5.0,
                    "suggested_end_timestamp": 5.3,
                    "contributing_signals": ["sig-medal"],
                    "contributing_sources": ["medal_icon"],
                    "penalties": [],
                    "bonuses": [],
                    "metadata": {
                        "rule_id": "medal_atomic",
                        "group_key": ["double_kill"],
                        "matched_signal_types": ["medal_visibility"],
                    },
                }
                build_event.return_value = valid_event
                with self.assertRaises(FusionAnalysisError) as exc:
                    fuse_analysis(
                        "/tmp/example.mp4",
                        "marvel_rivals",
                        proxy_sidecar=self._proxy_sidecar(),
                        runtime_sidecar=self._runtime_sidecar(),
                    )
        self.assertEqual(exc.exception.status, "invalid_fused_event_contract")

    def test_fuse_analysis_rejects_invalid_fusion_summary_contract(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: medal_atomic",
                "    event_type: medal_seen",
                '    signal_types: ["medal_visibility"]',
                '    required_signal_types: ["medal_visibility"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 1",
                "    confidence_method: max",
                '    group_by: ["event_row_id"]',
            ]
        ) + "\n"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.fusion_analysis._fusion_summary") as build_summary:
                build_summary.return_value = {
                    "normalized_signal_count": 3,
                    "fused_event_count": 1,
                    "signals_by_producer_family": {},
                    "signals_by_type": {},
                    "events_by_type": {},
                    "gate_status_counts": {},
                    "synergy_applied_count": 0,
                    "synergy_rule_counts": {},
                    "average_synergy_multiplier": 1.0,
                    "contract_summary": {},
                    "rule_count": 1,
                    "rule_ids": ["medal_atomic"],
                    "rule_parameters_by_id": {},
                }
                with self.assertRaises(FusionAnalysisError) as exc:
                    fuse_analysis(
                        "/tmp/example.mp4",
                        "marvel_rivals",
                        proxy_sidecar=self._proxy_sidecar(),
                        runtime_sidecar=self._runtime_sidecar(),
                    )
        self.assertEqual(exc.exception.status, "invalid_fusion_summary_contract")

    def test_load_fusion_rules_rejects_malformed_gate_config(self) -> None:
        rules_text = (
            "rules:\n"
            "  - rule_id: bad_gate\n"
            "    event_type: medal_seen\n"
            '    signal_types: ["medal_visibility"]\n'
            '    required_signal_types: ["medal_visibility"]\n'
            '    anchor_signal_types: ["medal_visibility"]\n'
            "    window_seconds: 1.0\n"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                with self.assertRaises(FusionAnalysisError) as exc:
                    load_fusion_rules("marvel_rivals")
        self.assertEqual(exc.exception.status, "invalid_fusion_rules")

    def test_synergy_rules_apply_multiplier_when_interactions_are_present(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: medal_combo_synergy",
                "    event_type: high_action_sequence",
                '    signal_types: ["ability_visibility", "medal_visibility", "audio_spike"]',
                '    required_signal_types: ["ability_visibility", "medal_visibility"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 2",
                "    confidence_method: mean",
                "    synergy:",
                "      enabled: true",
                "      minimum_required_signals: 3",
                "      interactions:",
                '        - signal_type: "audio_spike"',
                "          multiplier: 1.15",
                '        - signal_type: "medal_visibility"',
                "          multiplier: 1.10",
            ]
        ) + "\n"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            synergy_matches_path = root / "debug" / "synergy_matches.json"
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = fuse_analysis(
                    "/tmp/example.mp4",
                    "marvel_rivals",
                    proxy_sidecar=self._proxy_sidecar(),
                    runtime_sidecar=self._runtime_sidecar(),
                    debug_output_dir=root / "debug",
                )
                synergy_matches_exists = synergy_matches_path.exists()
        row = result["fused_events"][0]
        self.assertTrue(row["synergy_applied"])
        self.assertTrue(row["minimum_required_signals_met"])
        self.assertGreater(row["synergy_multiplier"], 1.0)
        self.assertGreater(row["final_score"], row["post_gate_confidence"])
        self.assertEqual(len(row["synergy_matches"]), 2)
        self.assertIn("audio_spike", row["metadata"]["matched_signal_types"])
        self.assertTrue(synergy_matches_exists)
        self.assertEqual(result["fusion_summary"]["synergy_applied_count"], 1)

    def test_synergy_minimum_required_signals_blocks_uplift(self) -> None:
        rules_text = "\n".join(
            [
                "rules:",
                "  - rule_id: medal_combo_synergy",
                "    event_type: medal_seen",
                '    signal_types: ["medal_visibility"]',
                '    required_signal_types: ["medal_visibility"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 1",
                "    confidence_method: max",
                "    synergy:",
                "      enabled: true",
                "      minimum_required_signals: 2",
                "      interactions:",
                '        - signal_type: "medal_visibility"',
                "          multiplier: 1.2",
            ]
        ) + "\n"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = fuse_analysis(
                    "/tmp/example.mp4",
                    "marvel_rivals",
                    proxy_sidecar=self._proxy_sidecar(),
                    runtime_sidecar=self._runtime_sidecar(),
                    output_path=root / "fusion.json",
                )
        row = result["fused_events"][0]
        self.assertFalse(row["synergy_applied"])
        self.assertFalse(row["minimum_required_signals_met"])
        self.assertEqual(row["synergy_multiplier"], 1.0)
        self.assertEqual(row["final_score"], row["post_gate_confidence"])

    def test_load_fusion_rules_rejects_malformed_synergy_config(self) -> None:
        rules_text = (
            "rules:\n"
            "  - rule_id: bad_synergy\n"
            "    event_type: medal_seen\n"
            '    signal_types: ["medal_visibility"]\n'
            "    window_seconds: 1.0\n"
            "    synergy:\n"
            "      enabled: true\n"
            "      interactions:\n"
            '        - signal_type: "chat_spike"\n'
            "          multiplier: 1.2\n"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root, rules_text=rules_text)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                with self.assertRaises(FusionAnalysisError) as exc:
                    load_fusion_rules("marvel_rivals")
        self.assertEqual(exc.exception.status, "invalid_fusion_rules")

    def test_run_fuse_clip_signals_uses_sidecars_without_rerunning_analysis(self) -> None:
        with patch("run.load_proxy_sidecar", return_value=self._proxy_sidecar()) as mock_proxy_loader, patch(
            "run.load_runtime_sidecar", return_value=self._runtime_sidecar()
        ) as mock_runtime_loader, patch(
            "run.run_scan_vod"
        ) as mock_scan_vod, patch(
            "run.run_analyze_roi_runtime"
        ) as mock_runtime_analysis, patch(
            "run.fuse_analysis",
            return_value={"ok": True, "status": "ok", "sidecar_path": "/tmp/example.fused_analysis.json"},
        ) as mock_fuse:
            result = run_fuse_clip_signals(
                "/tmp/example.mp4",
                "marvel_rivals",
                proxy_sidecar="/tmp/proxy.json",
                runtime_sidecar="/tmp/runtime.json",
            )
        mock_proxy_loader.assert_called_once()
        mock_runtime_loader.assert_called_once()
        mock_scan_vod.assert_not_called()
        mock_runtime_analysis.assert_not_called()
        mock_fuse.assert_called_once()
        self.assertTrue(result["ok"])

    def test_cli_routes_to_fuse_clip_signals(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--fuse-clip-signals", "/tmp/example.mp4", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_fuse_clip_signals",
                return_value={"ok": True, "status": "ok", "sidecar_path": "/tmp/example.fused_analysis.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv
