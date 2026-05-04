from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline import fused_export
from run import main as run_main
from run import run_export_fused_analysis


def _fused_sidecar(
    *,
    fusion_id: str,
    game: str,
    source: str,
    fused_events: list[dict[str, object]],
    normalized_signals: list[dict[str, object]],
    ok: bool = True,
    schema_version: str = "fused_analysis_v1",
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "fusion_id": fusion_id,
        "ok": ok,
        "status": "ok" if ok else "failed",
        "game": game,
        "source": source,
        "sidecar_path": f"/tmp/{fusion_id}.fused_analysis.json",
        "proxy": {"status": "ok"},
        "runtime": {"status": "ok"},
        "normalized_signals": normalized_signals,
        "fused_events": fused_events,
        "fusion_summary": {"event_count": len(fused_events)},
        "rule_matches": [],
    }


def _fused_event(
    *,
    event_id: str,
    event_type: str,
    final_score: float,
    contributing_signals: list[str],
    gate_status: str = "confirmed",
    synergy_applied: bool = True,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "start_timestamp": 10.0,
        "end_timestamp": 12.0,
        "confidence": round(final_score - 0.05, 4),
        "final_score": final_score,
        "gate_status": gate_status,
        "synergy_applied": synergy_applied,
        "synergy_multiplier": 1.15 if synergy_applied else 1.0,
        "minimum_required_signals_met": synergy_applied,
        "suggested_start_timestamp": 9.5,
        "suggested_end_timestamp": 12.8,
        "contributing_signals": contributing_signals,
        "metadata": {
            "matched_signal_types": ["medal_visibility", "chat_spike"],
            "entity_id": "hero_001",
            "ability_id": "ability_001",
            "event_row_id": "runtime-row-1",
        },
    }


def _signal(*, signal_id: str, signal_type: str, producer_family: str, asset_id: str, roi_ref: str) -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "signal_type": signal_type,
        "producer_family": producer_family,
        "asset_id": asset_id,
        "roi_ref": roi_ref,
        "source_family": producer_family,
    }


class FusedExportTests(unittest.TestCase):
    def test_export_fused_analysis_writes_candidate_event_and_signal_reference_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as export_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "marvel_rivals" / "alpha.fused_analysis.json",
                _fused_sidecar(
                    fusion_id="fusion-a",
                    game="marvel_rivals",
                    source="fixture-a.mp4",
                    fused_events=[
                        _fused_event(
                            event_id="event-1",
                            event_type="medal_seen",
                            final_score=0.91,
                            contributing_signals=["signal-1", "signal-2"],
                        )
                    ],
                    normalized_signals=[
                        _signal(
                            signal_id="signal-1",
                            signal_type="medal_visibility",
                            producer_family="runtime",
                            asset_id="medal_asset",
                            roi_ref="medal_area",
                        ),
                        _signal(
                            signal_id="signal-2",
                            signal_type="chat_spike",
                            producer_family="proxy",
                            asset_id="chat_asset",
                            roi_ref="chat_lane",
                        ),
                    ],
                ),
            )
            self._write_sidecar(
                root / "bad" / "failed.fused_analysis.json",
                _fused_sidecar(
                    fusion_id="fusion-b",
                    game="marvel_rivals",
                    source="fixture-b.mp4",
                    fused_events=[],
                    normalized_signals=[],
                    ok=False,
                ),
            )
            malformed_path = root / "bad" / "malformed.fused_analysis.json"
            malformed_path.parent.mkdir(parents=True, exist_ok=True)
            malformed_path.write_text("{not-json", encoding="utf-8")

            with patch.object(fused_export, "DEFAULT_OUTPUT_ROOT", Path(export_root)):
                result = run_export_fused_analysis(root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["candidate_row_count"], 1)
            self.assertEqual(result["event_row_count"], 1)
            self.assertEqual(result["signal_reference_row_count"], 2)
            self.assertEqual(result["scanned_sidecar_count"], 3)
            self.assertEqual(result["exported_sidecar_count"], 1)
            self.assertEqual(result["skipped_sidecar_count"], 2)

            candidates_jsonl = Path(result["candidates_jsonl_path"])
            events_jsonl = Path(result["events_jsonl_path"])
            signal_refs_jsonl = Path(result["signal_references_jsonl_path"])
            manifest_path = Path(result["manifest_path"])
            self.assertTrue(candidates_jsonl.is_file())
            self.assertTrue(events_jsonl.is_file())
            self.assertTrue(signal_refs_jsonl.is_file())
            self.assertTrue(manifest_path.is_file())

            candidate_rows = [json.loads(line) for line in candidates_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(candidate_rows), 1)
            self.assertEqual(candidate_rows[0]["event_id"], "event-1")
            self.assertEqual(candidate_rows[0]["recommended_action"], "highlight_candidate")
            self.assertEqual(candidate_rows[0]["segment_duration_seconds"], 3.3)
            self.assertEqual(candidate_rows[0]["entity_id"], "hero_001")

            event_rows = [json.loads(line) for line in events_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(event_rows), 1)
            self.assertEqual(event_rows[0]["recommended_action"], "highlight_candidate")

            signal_rows = [json.loads(line) for line in signal_refs_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(signal_rows), 2)
            self.assertEqual(signal_rows[0]["event_id"], "event-1")

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["candidate_row_count"], 1)
            self.assertEqual(manifest["event_row_count"], 1)
            self.assertEqual(manifest["signal_reference_row_count"], 2)
            self.assertEqual(manifest["skipped_failed_analysis_count"], 1)
            self.assertEqual(manifest["skipped_malformed_count"], 1)

    def test_export_fused_analysis_game_filter_limits_rows(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as export_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "marvel" / "marvel.fused_analysis.json",
                _fused_sidecar(
                    fusion_id="fusion-marvel",
                    game="marvel_rivals",
                    source="fixture-a.mp4",
                    fused_events=[],
                    normalized_signals=[],
                ),
            )
            self._write_sidecar(
                root / "cod" / "cod.fused_analysis.json",
                _fused_sidecar(
                    fusion_id="fusion-cod",
                    game="call_of_duty",
                    source="fixture-b.mp4",
                    fused_events=[],
                    normalized_signals=[],
                ),
            )

            with patch.object(fused_export, "DEFAULT_OUTPUT_ROOT", Path(export_root)):
                result = run_export_fused_analysis(root, game="marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["candidate_row_count"], 0)
            self.assertEqual(result["event_row_count"], 0)
            self.assertEqual(result["signal_reference_row_count"], 0)
            self.assertEqual(result["skipped_sidecar_count"], 1)

    def test_cli_routes_to_export_fused_analysis(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--export-fused-analysis", "/tmp/fused-sidecars"]
            with patch("run.run_export_fused_analysis", return_value={"ok": True, "candidate_row_count": 1}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv

    @staticmethod
    def _write_sidecar(path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
