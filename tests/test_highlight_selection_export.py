from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline import highlight_selection_export
from pipeline.highlight_selection_export import export_highlight_selection


def _proxy_sidecar(source: Path, *, schema_version: str = "proxy_scan_v1") -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "scan_id": "proxy-123abc",
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "windows": [
            {
                "start_seconds": 0.0,
                "end_seconds": 5.0,
                "proxy_score": 0.91,
                "recommended_action": "download_candidate",
                "source_families": ["hf_multimodal"],
                "sources": ["hf_rerank_highlight"],
                "signal_count": 2,
            },
            {
                "start_seconds": 5.0,
                "end_seconds": 8.0,
                "proxy_score": 0.55,
                "recommended_action": "inspect",
                "source_families": ["hf_multimodal"],
                "sources": ["hf_semantic_match"],
                "signal_count": 1,
            },
            {
                "start_seconds": 8.0,
                "end_seconds": 10.0,
                "proxy_score": 0.10,
                "recommended_action": "skip",
                "source_families": ["audio_prepass"],
                "sources": ["audio_spike"],
                "signal_count": 1,
            },
        ],
    }


def _fused_sidecar(source: Path, *, schema_version: str = "fused_analysis_v1") -> dict[str, object]:
    return {
        "schema_version": schema_version,
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
                "confidence": 0.9,
                "final_score": 0.91,
                "gate_status": "confirmed",
                "suggested_start_timestamp": 0.5,
                "suggested_end_timestamp": 3.0,
                "contributing_signals": ["signal-1", "signal-2"],
                "metadata": {"entity_id": "punisher", "matched_signal_types": ["character_identity", "chat_spike"]},
            },
            {
                "event_id": "fused-2",
                "event_type": "low_signal_noise",
                "confidence": 0.2,
                "final_score": 0.2,
                "gate_status": "rejected",
                "suggested_start_timestamp": 4.0,
                "suggested_end_timestamp": 5.0,
                "contributing_signals": [],
                "metadata": {},
            },
        ],
        "fused_review": {"events": {"fused-1": {"review_status": "approved"}}},
    }


class HighlightSelectionExportTests(unittest.TestCase):
    def test_export_highlight_selection_writes_manifest_and_otio_skeleton(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(media), indent=2), encoding="utf-8")

            with patch.object(highlight_selection_export, "DEFAULT_OUTPUT_ROOT", root / "exports"):
                result = export_highlight_selection(sidecar)

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["selection_basis"], "proxy")
            self.assertEqual(manifest["selected_highlight_count"], 2)
            self.assertEqual(manifest["selected_highlights"][0]["recommended_action"], "download_candidate")
            otio_payload = json.loads(Path(result["otio_skeleton_path"]).read_text(encoding="utf-8"))
            self.assertEqual(otio_payload["OTIO_SCHEMA"], "Timeline.1")

    def test_export_highlight_selection_supports_fused_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.fused_analysis.json"
            sidecar.write_text(json.dumps(_fused_sidecar(media), indent=2), encoding="utf-8")

            with patch.object(highlight_selection_export, "DEFAULT_OUTPUT_ROOT", root / "exports"):
                result = export_highlight_selection(fused_sidecar=sidecar)

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["selection_basis"], "fused")
            self.assertEqual(manifest["selected_highlight_count"], 1)
            selected = manifest["selected_highlights"][0]
            self.assertEqual(selected["fusion_id"], "fused-123abc")
            self.assertEqual(selected["event_id"], "fused-1")
            self.assertTrue(str(selected["candidate_id"]).startswith("candidate-"))
            self.assertEqual(selected["contributing_producer_families"], ["proxy", "runtime"])
            otio_payload = json.loads(Path(result["otio_skeleton_path"]).read_text(encoding="utf-8"))
            self.assertEqual(otio_payload["metadata"]["selection_basis"], "fused")
            self.assertEqual(
                otio_payload["tracks"]["children"][0]["children"][0]["metadata"]["contributing_producer_families"],
                ["proxy", "runtime"],
            )

    def test_export_highlight_selection_rejects_invalid_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "bad.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(media, schema_version="proxy_scan_v0"), indent=2), encoding="utf-8")

            result = export_highlight_selection(sidecar)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_proxy_sidecar")

    def test_export_highlight_selection_rejects_invalid_fused_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "bad.fused_analysis.json"
            sidecar.write_text(json.dumps(_fused_sidecar(media, schema_version="fused_analysis_v0"), indent=2), encoding="utf-8")

            result = export_highlight_selection(fused_sidecar=sidecar)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_fused_sidecar")


if __name__ == "__main__":
    unittest.main()
