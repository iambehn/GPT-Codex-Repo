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
            self.assertEqual(manifest["hook_candidates"][0]["lifecycle_state"], "selected_for_export")

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
