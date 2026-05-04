from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from run import main as run_main
from run import run_calibrate_proxy_review


def _proxy_sidecar(
    *,
    scan_id: str,
    game: str,
    source: str,
    proxy_score: float,
    action: str,
    review_status: str | None = None,
    ok: bool = True,
    schema_version: str = "proxy_scan_v1",
    include_hf: bool = True,
    asr_status: str = "ok",
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
                    "asr": {"status": asr_status, "duration_ms": 18.0, "output_counts": {"segment_count": 1}},
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
        "scan_id": scan_id,
        "ok": ok,
        "game": game,
        "source": source,
        "source_results": source_results,
        "config": {"proxy_scanner": {}},
        "signal_count": 3 if include_hf else 0,
        "window_count": 1,
        "signals": [],
        "windows": [
            {
                "start_seconds": 0.0,
                "end_seconds": 10.0,
                "proxy_score": proxy_score,
                "signal_count": 3 if include_hf else 0,
                "sources": ["hf_shot_boundary", "hf_semantic_match", "hf_rerank_highlight"] if include_hf else [],
                "source_families": ["hf_multimodal"] if include_hf else ["audio_prepass"],
                "recommended_action": action,
                "signals": [
                    {"source": "hf_shot_boundary", "strength": 0.9, "confidence": 0.7},
                    {"source": "hf_semantic_match", "strength": 0.8, "confidence": 0.76},
                    {"source": "hf_rerank_highlight", "strength": 0.91, "confidence": 0.8},
                ]
                if include_hf
                else [],
                "explanation": [],
            }
        ],
        "sidecar_path": f"/tmp/{scan_id}.proxy_scan.json",
    }
    if review_status is not None:
        payload["proxy_review"] = {"review_status": review_status}
    return payload


class ProxyCalibrationTests(unittest.TestCase):
    def test_calibrate_proxy_review_reports_hf_diagnostics_from_reviewed_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root:
            root = Path(sidecar_root)
            (root / "approved.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        scan_id="approved",
                        game="marvel_rivals",
                        source="clip-approved.mp4",
                        proxy_score=0.92,
                        action="download_candidate",
                        review_status="approved",
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (root / "rejected.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        scan_id="rejected",
                        game="marvel_rivals",
                        source="clip-rejected.mp4",
                        proxy_score=0.41,
                        action="inspect",
                        review_status="rejected",
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            result = run_calibrate_proxy_review(root, game="marvel_rivals", min_reviewed=2)
            self.assertTrue(result["ok"])
            self.assertEqual(result["reviewed_sidecar_count"], 2)
            self.assertEqual(result["approved_count"], 1)
            self.assertEqual(result["rejected_count"], 1)
            self.assertIn("stage_latency_ms", result["diagnostics"]["stage_coverage"])
            self.assertIn("proposal", result["diagnostics"]["threshold_diagnostics"])
            self.assertIn("stage_weight_observations", result["recommendations"])

    def test_calibrate_proxy_review_skips_invalid_and_non_hf_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root:
            root = Path(sidecar_root)
            (root / "valid.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        scan_id="valid",
                        game="marvel_rivals",
                        source="clip-valid.mp4",
                        proxy_score=0.88,
                        action="download_candidate",
                        review_status="approved",
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (root / "non-hf.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        scan_id="non-hf",
                        game="marvel_rivals",
                        source="clip-non-hf.mp4",
                        proxy_score=0.5,
                        action="inspect",
                        review_status="rejected",
                        include_hf=False,
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (root / "wrong.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        scan_id="wrong",
                        game="marvel_rivals",
                        source="clip-wrong.mp4",
                        proxy_score=0.3,
                        action="skip",
                        review_status="rejected",
                        schema_version="proxy_scan_v0",
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (root / "malformed.proxy_scan.json").write_text("{bad-json", encoding="utf-8")
            result = run_calibrate_proxy_review(root, game="marvel_rivals", min_reviewed=1)
            reasons = {warning["reason"] for warning in result["warnings"]}
            self.assertIn("non_hf_source", reasons)
            self.assertIn("unsupported_schema_version", reasons)
            self.assertIn("malformed_json", reasons)

    def test_cli_routes_to_proxy_calibration(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--calibrate-proxy-review", "/tmp/proxy-sidecars", "--game", "marvel_rivals"]
            stdout = io.StringIO()
            with patch("run.run_calibrate_proxy_review", return_value={"ok": True, "reviewed_sidecar_count": 2}):
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(stdout.getvalue())["reviewed_sidecar_count"], 2)
        finally:
            sys.argv = original_argv
