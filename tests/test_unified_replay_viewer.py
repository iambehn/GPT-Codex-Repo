from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline import unified_replay_viewer
from run import main as run_main
from run import run_render_unified_replay_viewer


def _proxy_sidecar(*, source: Path, game: str = "marvel_rivals", schema_version: str = "proxy_scan_v1") -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "scan_id": "proxy-123abc",
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source.resolve()),
        "sidecar_path": "/tmp/example.proxy_scan.json",
        "source_results": {
            "hf_multimodal": {
                "status": "ok",
                "signal_count": 3,
                "metadata": {
                    "pipeline": {
                        "duration_seconds": 10.0,
                        "shortlist_count": 1,
                        "stage_weights": {"proposal": 0.35, "transcript": 0.2, "semantic": 0.25, "novelty": 0.2},
                        "signal_thresholds": {"proposal": 0.55, "transcript": 0.6, "semantic": 0.6, "novelty": 0.6, "rerank": 0.65},
                    },
                    "stage_statuses": {
                        "shot_detector": "ok",
                        "asr": "ok",
                        "semantic": "ok",
                        "keyframes": "ok",
                        "reranker": "ok",
                    },
                    "stages": {
                        "shot_detector": {"status": "ok", "duration_ms": 12.5, "output_counts": {"proposal_count": 1}},
                        "asr": {"status": "ok", "duration_ms": 18.0, "output_counts": {"segment_count": 1}},
                        "semantic": {"status": "ok", "duration_ms": 22.0, "output_counts": {"segment_count": 1}},
                        "keyframes": {"status": "ok", "duration_ms": 16.0, "output_counts": {"segment_count": 1}},
                        "reranker": {"status": "ok", "duration_ms": 25.0, "output_counts": {"candidate_count": 1}},
                    },
                    "structured_outputs": {
                        "segment_boundaries": [{"timestamp_seconds": 2.0, "boundary_score": 0.9}],
                        "segment_proposals": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.9}],
                        "transcript_features": [{"start_seconds": 0.0, "end_seconds": 5.0, "text": "insane clutch", "keyword_hits": ["clutch"], "salience_score": 0.7}],
                        "semantic_scores": [{"start_seconds": 0.0, "end_seconds": 5.0, "query_scores": {"highlight moment": 0.8}, "top_query": "highlight moment", "semantic_score": 0.8}],
                        "keyframe_features": [{"start_seconds": 0.0, "end_seconds": 5.0, "keyframe_timestamp_seconds": 2.5, "novelty_score": 0.72, "cluster_id": 1}],
                        "reranked_candidates": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.9, "transcript_score": 0.7, "semantic_score": 0.8, "novelty_score": 0.72, "base_score": 0.794, "rerank_score": 0.91, "reason": "clear clutch swing", "reason_codes": ["clutch_moment"]}],
                    },
                },
            }
        },
        "config": {"proxy_scanner": {}},
        "signal_count": 3,
        "window_count": 1,
        "signals": [],
        "windows": [
            {
                "start_seconds": 0.0,
                "end_seconds": 5.0,
                "proxy_score": 0.91,
                "signal_count": 3,
                "sources": ["hf_shot_boundary", "hf_semantic_match", "hf_rerank_highlight"],
                "source_families": ["hf_multimodal"],
                "recommended_action": "download_candidate",
                "signals": [
                    {"source": "hf_shot_boundary", "strength": 0.9, "confidence": 0.7},
                    {"source": "hf_semantic_match", "strength": 0.8, "confidence": 0.76},
                    {"source": "hf_rerank_highlight", "strength": 0.91, "confidence": 0.8},
                ],
                "explanation": ["hf_rerank_highlight: strength=0.91", "proxy_score=0.91"],
            }
        ],
        "proxy_review": {"review_status": "approved"},
    }


def _runtime_sidecar(*, source: Path, game: str = "marvel_rivals", schema_version: str = "runtime_analysis_v1") -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "analysis_id": "runtime-123abc",
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source.resolve()),
        "sidecar_path": "/tmp/runtime.runtime_analysis.json",
        "game_pack": {"game_id": game},
        "matcher": {
            "status": "ok",
            "confirmed_detections": [
                {
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.0,
                    "last_timestamp": 1.5,
                    "peak_score": 0.98,
                }
            ],
            "signals": [
                {
                    "signal_id": "signal-1",
                    "signal_type": "character_identity",
                    "start_timestamp": 1.0,
                    "end_timestamp": 1.5,
                    "confidence": 0.98,
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "entity_id": "punisher",
                }
            ],
        },
        "events": {
            "status": "ok",
            "rows": [
                {
                    "event_id": "runtime-1",
                    "event_type": "pov_character_identified",
                    "start_timestamp": 1.0,
                    "end_timestamp": 1.5,
                    "confidence": 0.98,
                    "entity_id": "punisher",
                }
            ],
        },
        "runtime_review": {"review_status": "approved"},
    }


def _fused_sidecar(*, source: Path, game: str = "marvel_rivals", schema_version: str = "fused_analysis_v1") -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "fusion_id": "fused-123abc",
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source.resolve()),
        "sidecar_path": "/tmp/fused.fused_analysis.json",
        "normalized_signals": [
            {
                "signal_id": "proxy-1",
                "signal_type": "chat_spike",
                "producer_family": "proxy",
                "start_timestamp": 1.8,
                "end_timestamp": 2.0,
                "evidence": {
                    "matching_windows": [
                        {"window_index": 0, "proxy_score": 0.91},
                    ]
                },
            },
            {
                "signal_id": "signal-1",
                "signal_type": "character_identity",
                "producer_family": "runtime",
                "start_timestamp": 1.0,
                "end_timestamp": 1.5,
            },
        ],
        "fused_events": [
            {
                "event_id": "fused-1",
                "event_type": "ability_plus_medal_combo",
                "start_timestamp": 1.0,
                "end_timestamp": 2.0,
                "confidence": 0.82,
                "final_score": 0.91,
                "gate_status": "confirmed",
                "synergy_applied": True,
                "minimum_required_signals_met": True,
                "suggested_start_timestamp": 0.5,
                "suggested_end_timestamp": 3.0,
                "contributing_signals": ["signal-1", "proxy-1"],
                "metadata": {"entity_id": "punisher"},
            }
        ],
        "fusion_summary": {"event_count": 1},
        "fused_review": {"events": {"fused-1": {"review_status": "approved"}}},
    }


def _proxy_rejected_sidecar(*, source: Path, game: str = "marvel_rivals") -> dict[str, object]:
    payload = _proxy_sidecar(source=source, game=game)
    payload["proxy_review"] = {"review_status": "rejected"}
    return payload


def _proxy_calibration_report() -> dict[str, object]:
    return {
        "ok": True,
        "diagnostics": {
            "reviewed_clips": [
                {
                    "scan_id": "proxy-123abc",
                    "proxy_score": 0.91,
                    "recommended_action": "download_candidate",
                    "review_status": "approved",
                }
            ]
        },
        "recommendations": {
            "threshold_observations": ["proposal threshold leaves one approved clip below threshold"],
            "weight_observations": ["semantic weight appears too low on approved clips"],
        },
        "current_scoring": {
            "hf_multimodal": {
                "stage_weights": {"proposal": 0.35, "semantic": 0.25},
                "signal_thresholds": {"proposal": 0.55, "semantic": 0.6},
            }
        },
    }


def _proxy_replay_report() -> dict[str, object]:
    return {
        "ok": True,
        "comparison": {
            "reviewed_comparisons": [
                {
                    "scan_id": "proxy-123abc",
                    "current_proxy_score": 0.91,
                    "trial_proxy_score": 0.95,
                    "score_delta": 0.04,
                    "current_action": "download_candidate",
                    "trial_action": "download_candidate",
                }
            ]
        },
        "recommendation": {
            "decision": "prefer_trial",
            "reason": "Trial routing improves approved-clip handling without worsening rejected routing.",
        },
        "current_proxy_scoring": {
            "hf_multimodal": {
                "stage_weights": {"proposal": 0.35, "semantic": 0.25},
                "signal_thresholds": {"proposal": 0.55, "semantic": 0.6},
            }
        },
        "trial_proxy_scoring": {
            "hf_multimodal": {
                "stage_weights": {"proposal": 0.30, "semantic": 0.30},
                "signal_thresholds": {"proposal": 0.5, "semantic": 0.55},
            }
        },
    }


def _runtime_calibration_report() -> dict[str, object]:
    return {
        "ok": True,
        "diagnostics": {
            "reviewed_clips": [
                {
                    "analysis_id": "runtime-123abc",
                    "highlight_score": 0.88,
                    "recommended_action": "highlight_candidate",
                    "review_status": "approved",
                }
            ]
        },
        "recommendations": {
            "threshold_observations": ["highlight threshold may be conservative"],
            "weight_observations": ["medal_seen weight is doing most of the work"],
        },
        "current_scoring": {"action_thresholds": {"inspect": 0.25, "highlight_candidate": 0.60}},
    }


def _runtime_replay_report() -> dict[str, object]:
    return {
        "ok": True,
        "comparison": {
            "reviewed_comparisons": [
                {
                    "analysis_id": "runtime-123abc",
                    "current_highlight_score": 0.88,
                    "trial_highlight_score": 0.92,
                    "score_delta": 0.04,
                    "current_action": "highlight_candidate",
                    "trial_action": "highlight_candidate",
                }
            ]
        },
        "recommendation": {
            "decision": "prefer_trial",
            "reason": "Trial scoring improves approved clip routing without increasing rejected clips.",
        },
        "current_scoring": {"action_thresholds": {"inspect": 0.25, "highlight_candidate": 0.60}},
        "trial_scoring": {"action_thresholds": {"inspect": 0.22, "highlight_candidate": 0.58}},
    }


def _fixture_comparison_report(proxy_path: Path, runtime_path: Path) -> dict[str, object]:
    return {
        "ok": True,
        "comparison": {
            "fixture_rows": [
                {
                    "fixture_id": "action-gameplay-001",
                    "artifact_layer": "proxy",
                    "coverage_status": "both",
                    "review_status": "approved",
                    "baseline_sidecar_path": str(proxy_path.resolve()),
                    "trial_sidecar_path": "/tmp/trial/action-gameplay-001.proxy_scan.json",
                    "baseline_action": "inspect",
                    "trial_action": "download_candidate",
                    "score_delta": 0.12,
                    "shortlist_changed": True,
                    "rerank_changed": False,
                    "stage_latency_deltas": {"shot_detector": -3.0},
                    "recommendation_signal": "trial_better",
                },
                {
                    "fixture_id": "action-gameplay-001",
                    "artifact_layer": "runtime",
                    "coverage_status": "both",
                    "review_status": "approved",
                    "baseline_sidecar_path": str(runtime_path.resolve()),
                    "trial_sidecar_path": "/tmp/trial/action-gameplay-001.runtime_analysis.json",
                    "baseline_action": "inspect",
                    "trial_action": "highlight_candidate",
                    "score_delta": 0.08,
                    "shortlist_changed": False,
                    "rerank_changed": False,
                    "stage_latency_deltas": {},
                    "recommendation_signal": "trial_better",
                },
            ]
        },
        "recommendation": {
            "decision": "prefer_trial",
            "reason": "Trial sidecars improve more reviewed fixtures than baseline.",
        },
    }


def _fixture_trial_batch_manifest(comparison_report_path: Path) -> dict[str, object]:
    return {
        "ok": True,
        "schema_version": "fixture_trial_batch_v1",
        "batch_name": "nightly",
        "baseline_trial_name": "baseline",
        "overall_recommendation": {"decision": "adopt_trial", "trial_name": "distil-whisper"},
        "trial_comparisons": [
            {
                "trial_name": "distil-whisper",
                "comparison_status": "ok",
                "comparison_report_path": str(comparison_report_path.resolve()),
                "artifact_layer": "proxy",
                "recommendation": {"decision": "prefer_trial"},
            }
        ],
    }


class UnifiedReplayViewerTests(unittest.TestCase):
    def test_render_unified_replay_viewer_proxy_only(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "alpha.proxy_scan.json"
            proxy_path.write_text(json.dumps(_proxy_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(unified_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_unified_replay_viewer(proxy_sidecar=proxy_path)

            self.assertTrue(result["ok"])
            self.assertTrue(result["proxy_available"])
            self.assertFalse(result["runtime_available"])
            self.assertFalse(result["fused_available"])
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("Unified Replay Viewer - marvel_rivals", html_text)
            self.assertIn("download_candidate", html_text)
            self.assertIn("HF stage", html_text)
            self.assertIn("clutch_moment", html_text)
            self.assertIn("Provenance", html_text)
            self.assertIn("Recommendation Summary", html_text)
            self.assertIn("raw-proxy-sidecar", html_text)

    def test_render_unified_replay_viewer_runtime_and_fused(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            runtime_path = root / "alpha.runtime_analysis.json"
            fused_path = root / "alpha.fused_analysis.json"
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media), indent=2), encoding="utf-8")
            fused_path.write_text(json.dumps(_fused_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(unified_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_unified_replay_viewer(runtime_sidecar=runtime_path, fused_sidecar=fused_path)

            self.assertTrue(result["ok"])
            self.assertFalse(result["proxy_available"])
            self.assertTrue(result["runtime_available"])
            self.assertTrue(result["fused_available"])
            self.assertGreaterEqual(result["cross_link_count"], 1)
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("pov_character_identified", html_text)
            self.assertIn("ability_plus_medal_combo", html_text)
            self.assertIn("Provenance And Linked Evidence", html_text)
            self.assertIn("Disagreements", html_text)
            self.assertIn("Runtime: approved", html_text)
            self.assertIn("Fused: approved", html_text)

    def test_render_unified_replay_viewer_proxy_runtime_fused(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "alpha.proxy_scan.json"
            runtime_path = root / "alpha.runtime_analysis.json"
            fused_path = root / "alpha.fused_analysis.json"
            proxy_path.write_text(json.dumps(_proxy_sidecar(source=media), indent=2), encoding="utf-8")
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media), indent=2), encoding="utf-8")
            fused_path.write_text(json.dumps(_fused_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(unified_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_unified_replay_viewer(
                    proxy_sidecar=proxy_path,
                    runtime_sidecar=runtime_path,
                    fused_sidecar=fused_path,
                )

            self.assertTrue(result["ok"])
            self.assertTrue(result["proxy_available"])
            self.assertTrue(result["runtime_available"])
            self.assertTrue(result["fused_available"])
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("Proxy: approved", html_text)
            self.assertIn("Runtime: approved", html_text)
            self.assertIn("Fused: approved", html_text)
            self.assertIn("Unified Timeline", html_text)
            self.assertIn("viewer-search", html_text)
            self.assertIn("Review status snapshot", html_text)
            self.assertIn("raw-fused-sidecar", html_text)

    def test_render_unified_replay_viewer_includes_report_overlays(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "alpha.proxy_scan.json"
            runtime_path = root / "alpha.runtime_analysis.json"
            proxy_report_path = root / "proxy_calibration.json"
            proxy_replay_path = root / "proxy_replay.json"
            runtime_report_path = root / "runtime_calibration.json"
            runtime_replay_path = root / "runtime_replay.json"
            fixture_report_path = root / "fixture_comparison.json"
            batch_manifest_path = root / "fixture_trial_batch_manifest.json"
            proxy_path.write_text(json.dumps(_proxy_sidecar(source=media), indent=2), encoding="utf-8")
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media), indent=2), encoding="utf-8")
            proxy_report_path.write_text(json.dumps(_proxy_calibration_report(), indent=2), encoding="utf-8")
            proxy_replay_path.write_text(json.dumps(_proxy_replay_report(), indent=2), encoding="utf-8")
            runtime_report_path.write_text(json.dumps(_runtime_calibration_report(), indent=2), encoding="utf-8")
            runtime_replay_path.write_text(json.dumps(_runtime_replay_report(), indent=2), encoding="utf-8")
            fixture_report_path.write_text(json.dumps(_fixture_comparison_report(proxy_path, runtime_path), indent=2), encoding="utf-8")
            batch_manifest_path.write_text(json.dumps(_fixture_trial_batch_manifest(fixture_report_path), indent=2), encoding="utf-8")

            with patch.object(unified_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_unified_replay_viewer(
                    proxy_sidecar=proxy_path,
                    runtime_sidecar=runtime_path,
                    fixture_comparison_report=fixture_report_path,
                    fixture_trial_batch_manifest=batch_manifest_path,
                    proxy_calibration_report=proxy_report_path,
                    proxy_replay_report=proxy_replay_path,
                    runtime_calibration_report=runtime_report_path,
                    runtime_replay_report=runtime_replay_path,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["report_overlay_count"], 4)
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("Evaluation Overlay", html_text)
            self.assertIn("prefer_trial", html_text)
            self.assertIn("action-gameplay-001", html_text)
            self.assertIn("distil-whisper", html_text)
            self.assertIn("Artifact role", html_text)
            self.assertIn("Recommendation reason", html_text)

    def test_render_unified_replay_viewer_fused_only(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "alpha.fused_analysis.json"
            fused_path.write_text(json.dumps(_fused_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(unified_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_unified_replay_viewer(fused_sidecar=fused_path)

            self.assertTrue(result["ok"])
            self.assertFalse(result["proxy_available"])
            self.assertFalse(result["runtime_available"])
            self.assertTrue(result["fused_available"])
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("ability_plus_medal_combo", html_text)
            self.assertIn("Structured evidence is partial or missing", html_text)
            self.assertIn("Raw JSON Inspector", html_text)

    def test_render_unified_replay_viewer_flags_downstream_weak_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "alpha.proxy_scan.json"
            runtime_path = root / "alpha.runtime_analysis.json"
            fused_path = root / "alpha.fused_analysis.json"
            proxy_path.write_text(json.dumps(_proxy_rejected_sidecar(source=media), indent=2), encoding="utf-8")
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media), indent=2), encoding="utf-8")
            fused_path.write_text(json.dumps(_fused_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(unified_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_unified_replay_viewer(
                    proxy_sidecar=proxy_path,
                    runtime_sidecar=runtime_path,
                    fused_sidecar=fused_path,
                )

            self.assertTrue(result["ok"])
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("Layer disagreement detected", html_text)
            self.assertIn("Downstream fused approval depends on rejected upstream proxy evidence.", html_text)

    def test_render_unified_replay_viewer_rejects_invalid_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            proxy_path = root / "bad.proxy_scan.json"
            proxy_path.write_text(json.dumps(_proxy_sidecar(source=media, schema_version="proxy_scan_v0"), indent=2), encoding="utf-8")

            result = run_render_unified_replay_viewer(proxy_sidecar=proxy_path)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_proxy_sidecar")

    def test_render_unified_replay_viewer_rejects_mismatched_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            proxy_path = root / "alpha.proxy_scan.json"
            runtime_path = root / "alpha.runtime_analysis.json"
            proxy_path.write_text(json.dumps(_proxy_sidecar(source=media, game="marvel_rivals"), indent=2), encoding="utf-8")
            runtime_path.write_text(json.dumps(_runtime_sidecar(source=media, game="overwatch"), indent=2), encoding="utf-8")

            result = run_render_unified_replay_viewer(proxy_sidecar=proxy_path, runtime_sidecar=runtime_path)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "mismatched_sidecars")

    def test_render_unified_replay_viewer_warns_when_media_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "missing.mp4"
            proxy_path = root / "alpha.proxy_scan.json"
            proxy_path.write_text(json.dumps(_proxy_sidecar(source=media), indent=2), encoding="utf-8")

            with patch.object(unified_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_unified_replay_viewer(proxy_sidecar=proxy_path)

            self.assertTrue(result["ok"])
            self.assertFalse(result["media_embed_available"])
            self.assertEqual(result["warnings"][0]["status"], "missing_media_source")
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("Local media source not available", html_text)

    def test_cli_routes_to_render_unified_replay_viewer(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--render-unified-replay-viewer",
                "--proxy-sidecar",
                "/tmp/example.proxy_scan.json",
            ]
            with patch("run.run_render_unified_replay_viewer", return_value={"ok": True, "viewer_path": "/tmp/viewer.html"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()
