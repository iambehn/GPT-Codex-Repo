from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from run import main as run_main


def _fused_sidecar(source: Path, *, review_status: str = "approved", final_score: float = 0.91) -> dict[str, object]:
    return {
        "schema_version": "fused_analysis_v1",
        "fusion_id": "fused-123abc",
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "normalized_signals": [
            {"signal_id": "signal-1", "signal_type": "character_identity", "producer_family": "runtime"},
            {"signal_id": "signal-2", "signal_type": "chat_spike", "producer_family": "proxy"},
        ],
        "fused_events": [
            {
                "event_id": "fused-1",
                "event_type": "ability_plus_medal_combo",
                "confidence": max(0.0, final_score - 0.05),
                "final_score": final_score,
                "gate_status": "confirmed",
                "synergy_applied": True,
                "minimum_required_signals_met": True,
                "suggested_start_timestamp": 0.5,
                "suggested_end_timestamp": 3.0,
                "contributing_signals": ["signal-1", "signal-2"],
                "metadata": {"entity_id": "punisher", "ability_id": "ult", "matched_signal_types": ["character_identity", "chat_spike"]},
            }
        ],
        "fused_review": {"events": {"fused-1": {"review_status": review_status}}},
    }


def _synthetic_other_sidecar(source: Path, *, review_status: str = "approved", final_score: float = 0.91) -> dict[str, object]:
    return {
        "schema_version": "fused_analysis_v1",
        "fusion_id": "fused-synthetic-other",
        "ok": True,
        "game": "call_of_duty",
        "source": str(source.resolve()),
        "normalized_signals": [
            {
                "signal_id": "signal-1",
                "signal_type": "equipment_visibility",
                "producer_family": "runtime",
                "timestamp": 2.5,
                "start_timestamp": 2.0,
                "end_timestamp": 4.0,
            },
            {
                "signal_id": "signal-2",
                "signal_type": "equipment_visibility",
                "producer_family": "runtime",
                "timestamp": 3.0,
                "start_timestamp": 2.0,
                "end_timestamp": 4.0,
            },
        ],
        "fused_events": [
            {
                "event_id": "fused-1",
                "event_type": "ability_seen",
                "confidence": final_score,
                "final_score": final_score,
                "gate_status": "not_applicable",
                "synergy_applied": False,
                "minimum_required_signals_met": True,
                "suggested_start_timestamp": 2.0,
                "suggested_end_timestamp": 2.0,
                    "contributing_signals": ["signal-1", "signal-2"],
                    "metadata": {"matched_signal_types": ["equipment_visibility"]},
                }
            ],
            "fused_review": {"events": {"fused-1": {"review_status": review_status}}},
        }


def _bounded_cod_archetype_sidecar(source: Path, *, review_status: str = "approved", final_score: float = 0.91) -> dict[str, object]:
    payload = _synthetic_other_sidecar(source, review_status=review_status, final_score=final_score)
    payload["fused_events"][0]["metadata"]["equipment_id"] = "redeploy_extraction_token"
    return payload


def _bounded_marvel_team_wipe_sidecar(source: Path, *, review_status: str = "approved", final_score: float = 0.91) -> dict[str, object]:
    return {
        "schema_version": "fused_analysis_v1",
        "fusion_id": "fused-team-wipe",
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "normalized_signals": [
            {
                "signal_id": "signal-1",
                "signal_type": "team_wipe_visibility",
                "producer_family": "runtime",
                "timestamp": 1.5,
                "start_timestamp": 0.0,
                "end_timestamp": 2.5,
            },
            {
                "signal_id": "signal-2",
                "signal_type": "round_state_visibility",
                "producer_family": "runtime",
                "timestamp": 2.0,
                "start_timestamp": 0.0,
                "end_timestamp": 2.5,
            },
        ],
        "fused_events": [
            {
                "event_id": "fused-1",
                "event_type": "team_wipe_seen",
                "confidence": final_score,
                "final_score": final_score,
                "gate_status": "not_applicable",
                "synergy_applied": False,
                "minimum_required_signals_met": True,
                "suggested_start_timestamp": 0.0,
                "suggested_end_timestamp": 0.0,
                "contributing_signals": ["signal-1"],
                "metadata": {"matched_signal_types": ["team_wipe_visibility"]},
            }
        ],
        "fused_review": {"events": {"fused-1": {"review_status": review_status}}},
    }


class HookCandidateExportTests(unittest.TestCase):
    def test_derive_hook_candidates_from_approved_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_fused_sidecar(media), indent=2), encoding="utf-8")
            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            result = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "hook_candidate_v1")
            self.assertEqual(manifest["hook_candidate_count"], 1)
            row = manifest["hook_candidates"][0]
            self.assertEqual(row["lifecycle_state"], "approved")
            self.assertIn(row["hook_archetype"], {"flex", "domination", "other"})
            self.assertIn(row["hook_mode"], {"natural", "synthetic", "reject"})

    def test_derive_hook_candidates_from_selected_for_export_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_fused_sidecar(media), indent=2), encoding="utf-8")
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "alpha.highlight_selection.json")
            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            result = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            row = manifest["hook_candidates"][0]
            self.assertEqual(row["lifecycle_state"], "selected_for_export")
            self.assertEqual(row["context_expansion_policy"], "signal_aware_bounded_v1")
            self.assertGreater(row["context_expansion_seconds"], 0.0)
            self.assertGreaterEqual(row["context_signal_count"], 1)
            self.assertEqual(row["highlight_selection_manifest_path"], str((root / "alpha.highlight_selection.json").resolve()))
            self.assertGreater(row["context_sufficiency_score"], 0.5)
            self.assertLess(row["authenticity_risk_score"], 0.6)
            self.assertEqual(row["hook_mode"], "natural")
            self.assertIsNone(row["synthetic_subtype"])
            self.assertIsNone(row["synthetic_packaging_rationale"])

    def test_synthetic_subtype_routing_is_additive_and_does_not_change_hook_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_synthetic_other_sidecar(media), indent=2), encoding="utf-8")
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "alpha.highlight_selection.json")
            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            result = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            row = manifest["hook_candidates"][0]
            self.assertEqual(row["hook_mode"], "synthetic")
            self.assertEqual(row["synthetic_subtype"], "archetype_salvageable")
            self.assertEqual(row["packaging_strategy"], "archetype_probe_then_context_card")
            self.assertIn("archetype", row["synthetic_packaging_rationale"])
            self.assertEqual(row["hook_archetype"], "other")
            self.assertIsNone(row.get("archetype_cue_match"))
            self.assertIsNone(row.get("archetype_rationale"))
            self.assertIsNone(row["rejection_reason"])

    def test_bounded_call_of_duty_archetype_extension_is_additive(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_bounded_cod_archetype_sidecar(media), indent=2), encoding="utf-8")
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "alpha.highlight_selection.json")
            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            result = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            row = manifest["hook_candidates"][0]
            self.assertEqual(row["hook_mode"], "synthetic")
            self.assertEqual(row["hook_archetype"], "chaos")
            self.assertEqual(row["archetype_cue_match"], "ability_seen + equipment_visibility + equipment_id")
            self.assertIn("utility", row["archetype_rationale"])
            self.assertEqual(row["synthetic_subtype"], "context_salvageable")
            self.assertEqual(row["packaging_strategy"], "low_claim_post_payoff")

    def test_bounded_marvel_team_wipe_archetype_extension_is_additive(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_bounded_marvel_team_wipe_sidecar(media), indent=2), encoding="utf-8")
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "alpha.highlight_selection.json")
            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            result = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            row = manifest["hook_candidates"][0]
            self.assertEqual(row["hook_mode"], "synthetic")
            self.assertEqual(row["hook_archetype"], "chaos")
            self.assertEqual(row["archetype_cue_match"], "team_wipe_seen + team_wipe_visibility + round_state_visibility")
            self.assertIn("team-wipe", row["archetype_rationale"])
            self.assertEqual(row["synthetic_subtype"], "context_salvageable")
            self.assertEqual(row["packaging_strategy"], "low_claim_post_payoff")

    def test_context_salvageable_with_pre_context_keeps_setup_first_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_bounded_cod_archetype_sidecar(media), indent=2), encoding="utf-8")
            selection_path = root / "alpha.highlight_selection.json"
            export_highlight_selection(fused_sidecar=fused_path, output_path=selection_path)

            selection_payload = json.loads(selection_path.read_text(encoding="utf-8"))
            selection_row = selection_payload["selected_highlights"][0]
            selection_row["context_pre_signal_types"] = ["character_identity"]
            selection_row["context_post_signal_types"] = []
            selection_row["context_signal_count"] = 2
            selection_path.write_text(json.dumps(selection_payload, indent=2), encoding="utf-8")

            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            result = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            row = manifest["hook_candidates"][0]
            self.assertEqual(row["hook_mode"], "synthetic")
            self.assertEqual(row["synthetic_subtype"], "context_salvageable")
            self.assertEqual(row["context_pre_signal_types"], ["character_identity"])
            self.assertEqual(row["packaging_strategy"], "setup_then_payoff_with_context_card")

    def test_derive_hook_candidates_skips_ineligible_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_fused_sidecar(media, review_status="rejected"), indent=2), encoding="utf-8")
            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            result = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")

            self.assertTrue(result["ok"])
            self.assertEqual(result["hook_candidate_count"], 0)
            self.assertEqual(result["ineligible_lifecycle_count"], 1)

    def test_hook_row_identity_is_deterministic_and_registry_query_ingests_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_fused_sidecar(media), indent=2), encoding="utf-8")
            registry_path = root / "registry.sqlite"
            refresh_clip_registry(root, registry_path=registry_path)

            first = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")
            second = derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "alpha.hook_candidates.json")
            first_manifest = json.loads(Path(first["manifest_path"]).read_text(encoding="utf-8"))
            second_manifest = json.loads(Path(second["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(first_manifest["hook_candidates"][0]["hook_id"], second_manifest["hook_candidates"][0]["hook_id"])

            refresh_clip_registry(root, registry_path=registry_path)
            query = query_clip_registry(
                mode="hook-candidates",
                game="marvel_rivals",
                hook_mode=first_manifest["hook_candidates"][0]["hook_mode"],
                candidate_id=first_manifest["hook_candidates"][0]["candidate_id"],
                registry_path=registry_path,
            )
            self.assertTrue(query["ok"])
            self.assertEqual(query["row_count"], 1)
            self.assertEqual(query["rows"][0]["event_id"], "fused-1")

    def test_cli_routes_to_derive_hook_candidates(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--derive-hook-candidates",
                "/tmp/example.fused_analysis.json",
                "--registry-path",
                "/tmp/registry.sqlite",
            ]
            with patch("run.run_derive_hook_candidates", return_value={"ok": True, "hook_candidate_count": 1}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()
