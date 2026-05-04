from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline import proxy_replay_viewer
from run import main as run_main
from run import run_render_proxy_replay_viewer


def _proxy_sidecar(
    *,
    source: Path,
    include_hf: bool = True,
    review_status: str | None = None,
    asr_status: str = "ok",
    asr_reason: str | None = None,
    schema_version: str = "proxy_scan_v1",
) -> dict[str, object]:
    source_results: dict[str, object] = {
        "audio_prepass": {"status": "skipped", "signal_count": 0},
        "visual_prepass": {"status": "skipped", "signal_count": 0},
        "playlist_hls": {"status": "skipped", "signal_count": 0},
        "chat_velocity": {"status": "skipped", "signal_count": 0},
    }
    if include_hf:
        source_results["hf_multimodal"] = {
            "status": "ok",
            "signal_count": 3,
            "metadata": {
                "pipeline": {
                    "duration_seconds": 10.0,
                    "shortlist_count": 2,
                    "stage_weights": {"proposal": 0.35, "transcript": 0.2, "semantic": 0.25, "novelty": 0.2},
                    "signal_thresholds": {"proposal": 0.55, "transcript": 0.6, "semantic": 0.6, "novelty": 0.6, "rerank": 0.65},
                },
                "stage_statuses": {
                    "shot_detector": "ok",
                    "asr": asr_status,
                    "semantic": "ok",
                    "keyframes": "ok",
                    "reranker": "ok",
                },
                "stages": {
                    "shot_detector": {"status": "ok", "duration_ms": 12.5, "output_counts": {"proposal_count": 1, "boundary_count": 1}},
                    "asr": {"status": asr_status, "reason": asr_reason, "duration_ms": 18.0, "output_counts": {"segment_count": 1}},
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
    payload: dict[str, object] = {
        "schema_version": schema_version,
        "scan_id": "marvel-rivals-proxy-123abc",
        "ok": True,
        "status": "ok",
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "source_results": source_results,
        "config": {"proxy_scanner": {}},
        "signal_count": 3 if include_hf else 1,
        "window_count": 2,
        "signals": [],
        "windows": [
            {
                "start_seconds": 0.0,
                "end_seconds": 5.0,
                "proxy_score": 0.91,
                "signal_count": 3 if include_hf else 1,
                "sources": ["hf_shot_boundary", "hf_semantic_match", "hf_rerank_highlight"] if include_hf else ["audio_spike"],
                "source_families": ["hf_multimodal"] if include_hf else ["audio_prepass"],
                "recommended_action": "download_candidate",
                "signals": [
                    {"source": "hf_shot_boundary", "strength": 0.9, "confidence": 0.7},
                    {"source": "hf_semantic_match", "strength": 0.8, "confidence": 0.76},
                    {"source": "hf_rerank_highlight", "strength": 0.91, "confidence": 0.8},
                ] if include_hf else [{"source": "audio_spike", "source_family": "audio_prepass", "strength": 1.0, "confidence": 0.72, "reason": "audio spike"}],
                "explanation": ["hf_rerank_highlight: strength=0.91", "proxy_score=0.91"],
            },
            {
                "start_seconds": 5.0,
                "end_seconds": 10.0,
                "proxy_score": 0.42,
                "signal_count": 1,
                "sources": ["hf_semantic_match"] if include_hf else ["audio_spike"],
                "source_families": ["hf_multimodal"] if include_hf else ["audio_prepass"],
                "recommended_action": "inspect",
                "signals": [{"source": "hf_semantic_match", "strength": 0.42, "confidence": 0.51}] if include_hf else [{"source": "audio_spike", "source_family": "audio_prepass", "strength": 0.6, "confidence": 0.5}],
                "explanation": ["proxy_score=0.42"],
            },
        ],
        "sidecar_path": "/tmp/example.proxy_scan.json",
        "proxy_review": {"review_status": review_status} if review_status is not None else None,
    }
    return payload


class ProxyReplayViewerTests(unittest.TestCase):
    def test_render_proxy_replay_viewer_writes_html_from_hf_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            sidecar_path = root / "alpha.proxy_scan.json"
            sidecar_path.write_text(json.dumps(_proxy_sidecar(source=media, review_status="approved"), indent=2), encoding="utf-8")

            with patch.object(proxy_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_proxy_replay_viewer(sidecar_path)

            self.assertTrue(result["ok"])
            self.assertTrue(result["has_hf_multimodal"])
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("Proxy Replay Viewer - marvel_rivals", html_text)
            self.assertIn("download_candidate", html_text)
            self.assertIn("hf_rerank_highlight", html_text)
            self.assertIn("clutch_moment", html_text)
            self.assertIn("Viewer Controls", html_text)
            self.assertIn("HF Stage Status", html_text)
            self.assertIn("Show JSON - HF Structured Outputs", html_text)

    def test_render_proxy_replay_viewer_shows_failed_stage_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            sidecar_path = root / "alpha.proxy_scan.json"
            sidecar_path.write_text(
                json.dumps(
                    _proxy_sidecar(source=media, review_status="rejected", asr_status="failed", asr_reason="missing optional dependency: transformers"),
                    indent=2,
                ),
                encoding="utf-8",
            )

            with patch.object(proxy_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_proxy_replay_viewer(sidecar_path)

            self.assertTrue(result["ok"])
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("missing optional dependency: transformers", html_text)
            self.assertIn("rejected", html_text)
            self.assertIn("High score plus rejected review", html_text)

    def test_render_proxy_replay_viewer_handles_non_hf_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            sidecar_path = root / "alpha.proxy_scan.json"
            sidecar_path.write_text(json.dumps(_proxy_sidecar(source=media, include_hf=False), indent=2), encoding="utf-8")

            with patch.object(proxy_replay_viewer, "DEFAULT_OUTPUT_ROOT", root / "viewer"):
                result = run_render_proxy_replay_viewer(sidecar_path)

            self.assertTrue(result["ok"])
            self.assertFalse(result["has_hf_multimodal"])
            html_text = Path(result["viewer_path"]).read_text(encoding="utf-8")
            self.assertIn("HF multimodal not present", html_text)
            self.assertIn("audio_prepass", html_text)
            self.assertIn("download_candidate", html_text)

    def test_render_proxy_replay_viewer_rejects_invalid_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar_path = root / "bad.proxy_scan.json"
            sidecar_path.write_text(json.dumps(_proxy_sidecar(source=media, schema_version="proxy_scan_v0"), indent=2), encoding="utf-8")

            result = run_render_proxy_replay_viewer(sidecar_path)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_proxy_sidecar")

    def test_cli_routes_to_render_proxy_replay_viewer(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--render-proxy-replay-viewer", "/tmp/example.proxy_scan.json"]
            with patch("run.run_render_proxy_replay_viewer", return_value={"ok": True, "viewer_path": "/tmp/viewer.html"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()
