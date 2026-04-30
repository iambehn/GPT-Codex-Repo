from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline import training_export
from run import run_export_training_data


def _window(
    start_seconds: float,
    end_seconds: float,
    proxy_score: float,
    recommended_action: str,
    sources: list[str],
    source_families: list[str],
    signals: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "start_seconds": start_seconds,
        "end_seconds": end_seconds,
        "proxy_score": proxy_score,
        "signal_count": len(signals),
        "sources": sources,
        "source_families": source_families,
        "recommended_action": recommended_action,
        "signals": signals,
        "explanation": [],
    }


def _signal(
    source: str,
    source_family: str,
    timestamp: float,
    strength: float,
    confidence: float,
    reason: str,
) -> dict[str, object]:
    return {
        "source": source,
        "source_family": source_family,
        "timestamp": timestamp,
        "strength": strength,
        "confidence": confidence,
        "reason": reason,
    }


def _sidecar(
    *,
    scan_id: str,
    game: str,
    source: str,
    windows: list[dict[str, object]],
    ok: bool = True,
    schema_version: str = "proxy_scan_v1",
) -> dict[str, object]:
    signals: list[dict[str, object]] = []
    for window in windows:
        signals.extend(window.get("signals", []))
    return {
        "schema_version": schema_version,
        "scan_id": scan_id,
        "ok": ok,
        "game": game,
        "source": source,
        "game_pack": {"game_id": game},
        "source_results": {
            "playlist_hls": {"status": "ok", "signal_count": 1},
            "audio_prepass": {"status": "skipped", "signal_count": 0, "reason": "not attempted"},
            "chat_velocity": {"status": "ok", "signal_count": 1},
        },
        "signal_count": len(signals),
        "window_count": len(windows),
        "signals": signals,
        "windows": windows,
    }


class TrainingExportTests(unittest.TestCase):
    def test_export_training_data_writes_jsonl_csv_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as export_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "game_a" / "multi.proxy_scan.json",
                _sidecar(
                    scan_id="scan-a",
                    game="marvel_rivals",
                    source="fixture-a",
                    windows=[
                        _window(
                            0.0,
                            30.0,
                            0.82,
                            "download_candidate",
                            ["chat_spike", "playlist_spike"],
                            ["chat_velocity", "playlist_hls"],
                            [
                                _signal("chat_spike", "chat_velocity", 5.0, 1.0, 0.7, "chat"),
                                _signal("playlist_spike", "playlist_hls", 8.0, 0.9, 0.65, "playlist"),
                            ],
                        ),
                        _window(
                            60.0,
                            80.0,
                            0.35,
                            "skip",
                            ["chat_spike"],
                            ["chat_velocity"],
                            [
                                _signal("chat_spike", "chat_velocity", 65.0, 0.5, 0.6, "chat"),
                            ],
                        ),
                    ],
                ),
            )
            self._write_sidecar(
                root / "game_a" / "single.proxy_scan.json",
                _sidecar(
                    scan_id="scan-b",
                    game="marvel_rivals",
                    source="fixture-b",
                    windows=[
                        _window(
                            10.0,
                            25.0,
                            0.72,
                            "inspect",
                            ["audio_spike"],
                            ["audio_prepass"],
                            [
                                _signal("audio_spike", "audio_prepass", 12.5, 0.95, 0.72, "audio"),
                            ],
                        )
                    ],
                ),
            )
            self._write_sidecar(
                root / "bad" / "failed.proxy_scan.json",
                _sidecar(scan_id="scan-failed", game="marvel_rivals", source="bad", windows=[], ok=False),
            )
            self._write_sidecar(
                root / "bad" / "empty.proxy_scan.json",
                _sidecar(scan_id="scan-empty", game="marvel_rivals", source="empty", windows=[]),
            )
            self._write_sidecar(
                root / "bad" / "schema.proxy_scan.json",
                _sidecar(
                    scan_id="scan-schema",
                    game="marvel_rivals",
                    source="schema",
                    windows=[],
                    schema_version="proxy_scan_v0",
                ),
            )
            malformed_path = root / "bad" / "malformed.proxy_scan.json"
            malformed_path.parent.mkdir(parents=True, exist_ok=True)
            malformed_path.write_text("{not-json", encoding="utf-8")

            with patch.object(training_export, "DEFAULT_OUTPUT_ROOT", Path(export_root)):
                result = run_export_training_data(root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 3)
            self.assertEqual(result["scanned_sidecar_count"], 6)
            self.assertEqual(result["exported_sidecar_count"], 2)
            self.assertEqual(result["skipped_sidecar_count"], 4)

            jsonl_path = Path(result["jsonl_path"])
            csv_path = Path(result["csv_path"])
            manifest_path = Path(result["manifest_path"])
            self.assertTrue(jsonl_path.is_file())
            self.assertTrue(csv_path.is_file())
            self.assertTrue(manifest_path.is_file())

            jsonl_rows = [
                json.loads(line)
                for line in jsonl_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(jsonl_rows), 3)
            self.assertIn("sources", jsonl_rows[0])
            self.assertIn("source_results", jsonl_rows[0])
            self.assertIn("signal_features", jsonl_rows[0])
            self.assertIsNone(jsonl_rows[0]["label"])
            self.assertIn("source_families", jsonl_rows[0])
            self.assertIn("signal_counts", jsonl_rows[0]["signal_features"])

            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                csv_rows = list(csv.DictReader(handle))
            self.assertEqual(len(csv_rows), 3)
            self.assertIn("source_status_playlist_hls", csv_rows[0])
            self.assertIn("signal_count_chat_spike", csv_rows[0])
            self.assertIn("max_strength_playlist_spike", csv_rows[0])
            self.assertIn("max_confidence_audio_spike", csv_rows[0])
            self.assertEqual(csv_rows[0]["label"], "")
            self.assertEqual(csv_rows[0]["label_source"], "")
            self.assertEqual(csv_rows[0]["label_notes"], "")

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["row_count"], 3)
            self.assertEqual(manifest["scanned_sidecar_count"], 6)
            self.assertEqual(manifest["exported_sidecar_count"], 2)
            self.assertEqual(manifest["skipped_malformed_count"], 1)
            self.assertEqual(manifest["skipped_schema_mismatch_count"], 1)
            self.assertEqual(manifest["skipped_failed_scan_count"], 1)
            self.assertEqual(manifest["skipped_empty_scan_count"], 1)
            self.assertEqual(len(manifest["warnings"]), 4)

    def test_export_training_data_game_filter_limits_rows(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as export_root:
            root = Path(sidecar_root)
            self._write_sidecar(
                root / "marvel" / "marvel.proxy_scan.json",
                _sidecar(
                    scan_id="scan-marvel",
                    game="marvel_rivals",
                    source="fixture-marvel",
                    windows=[
                        _window(
                            0.0,
                            10.0,
                            0.81,
                            "download_candidate",
                            ["chat_spike", "playlist_spike"],
                            ["chat_velocity", "playlist_hls"],
                            [
                                _signal("chat_spike", "chat_velocity", 2.0, 1.0, 0.7, "chat"),
                                _signal("playlist_spike", "playlist_hls", 2.0, 0.8, 0.65, "playlist"),
                            ],
                        )
                    ],
                ),
            )
            self._write_sidecar(
                root / "other" / "other.proxy_scan.json",
                _sidecar(
                    scan_id="scan-other",
                    game="overwatch",
                    source="fixture-other",
                    windows=[
                        _window(
                            5.0,
                            20.0,
                            0.75,
                            "inspect",
                            ["audio_spike"],
                            ["audio_prepass"],
                            [
                                _signal("audio_spike", "audio_prepass", 10.0, 0.9, 0.72, "audio"),
                            ],
                        )
                    ],
                ),
            )

            with patch.object(training_export, "DEFAULT_OUTPUT_ROOT", Path(export_root)):
                result = run_export_training_data(root, game="marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["game_filter"], "marvel_rivals")
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(result["scanned_sidecar_count"], 2)
            self.assertEqual(result["exported_sidecar_count"], 1)
            self.assertEqual(result["skipped_sidecar_count"], 1)

            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["game_filter"], "marvel_rivals")
            self.assertEqual(manifest["skipped_game_filter_mismatch_count"], 1)

    def _write_sidecar(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
