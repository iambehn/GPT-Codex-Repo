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
from run import run_replay_proxy_scoring


def _proxy_sidecar(
    *,
    scan_id: str,
    game: str,
    source: str,
    review_status: str,
    proposal_score: float,
    transcript_score: float,
    semantic_score: float,
    novelty_score: float,
    rerank_score: float,
) -> dict[str, object]:
    return {
        "schema_version": "proxy_scan_v1",
        "scan_id": scan_id,
        "ok": True,
        "game": game,
        "source": source,
        "proxy_review": {"review_status": review_status},
        "source_results": {
            "hf_multimodal": {
                "status": "ok",
                "signal_count": 4,
                "metadata": {
                    "pipeline": {
                        "duration_seconds": 10.0,
                        "shortlist_count": 2,
                        "stage_weights": {"proposal": 0.35, "transcript": 0.2, "semantic": 0.25, "novelty": 0.2},
                        "signal_thresholds": {"proposal": 0.55, "transcript": 0.6, "semantic": 0.6, "novelty": 0.6, "rerank": 0.65},
                    },
                    "structured_outputs": {
                        "segment_boundaries": [{"timestamp_seconds": 2.0, "boundary_score": 0.9}],
                        "segment_proposals": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": proposal_score}],
                        "transcript_features": [{"start_seconds": 0.0, "end_seconds": 5.0, "text": "insane clutch", "keyword_hits": ["clutch"], "salience_score": transcript_score}],
                        "semantic_scores": [{"start_seconds": 0.0, "end_seconds": 5.0, "query_scores": {"highlight moment": semantic_score}, "top_query": "highlight moment", "semantic_score": semantic_score}],
                        "keyframe_features": [{"start_seconds": 0.0, "end_seconds": 5.0, "keyframe_timestamp_seconds": 2.5, "novelty_score": novelty_score, "cluster_id": 1}],
                        "reranked_candidates": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": proposal_score, "transcript_score": transcript_score, "semantic_score": semantic_score, "novelty_score": novelty_score, "base_score": 0.5, "rerank_score": rerank_score, "reason": "clear clutch swing", "reason_codes": ["clutch_moment"]}],
                    },
                },
            }
        },
        "config": {"proxy_scanner": {}},
        "signal_count": 4,
        "window_count": 1,
        "signals": [],
        "windows": [
            {
                "start_seconds": 0.0,
                "end_seconds": 10.0,
                "proxy_score": 0.75,
                "signal_count": 4,
                "sources": ["hf_shot_boundary", "hf_transcript_salience", "hf_semantic_match", "hf_rerank_highlight"],
                "source_families": ["hf_multimodal"],
                "recommended_action": "inspect",
                "signals": [],
                "explanation": [],
            }
        ],
        "sidecar_path": f"/tmp/{scan_id}.proxy_scan.json",
    }


def _current_proxy_config() -> dict[str, object]:
    return {
        "hf_multimodal": {
            "shortlist_count": 2,
            "stage_weights": {"proposal": 0.35, "transcript": 0.2, "semantic": 0.25, "novelty": 0.2},
            "signal_thresholds": {"proposal": 0.55, "transcript": 0.6, "semantic": 0.9, "novelty": 0.6, "rerank": 0.95},
        },
        "weights": {
            "hf_shot_boundary": 2.4,
            "hf_transcript_salience": 2.2,
            "hf_semantic_match": 2.6,
            "hf_keyframe_novelty": 2.0,
            "hf_rerank_highlight": 3.2,
        },
        "candidate_selection": {
            "dedupe_gap_seconds": 3,
            "merge_gap_seconds": 30,
            "audio_only_merge_gap_seconds": 8,
            "window_pre_seconds": 10,
            "window_post_seconds": 25,
            "audio_only_window_pre_seconds": 3,
            "audio_only_window_post_seconds": 6,
            "min_proxy_score": 0.30,
            "max_windows": 20,
            "agreement_bonus_per_extra_source": 0.10,
            "max_agreement_bonus": 0.25,
        },
        "cost_gates": {
            "inspect_min_score": 0.40,
            "download_candidate_min_score": 0.75,
            "download_candidate_min_sources": 2,
        },
    }


class ProxyTuningTests(unittest.TestCase):
    def test_replay_proxy_scoring_prefers_trial_when_semantic_threshold_improves_approved_routing(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as config_root:
            root = Path(sidecar_root)
            config_dir = Path(config_root)
            (root / "approved.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        scan_id="approved",
                        game="marvel_rivals",
                        source="clip-approved.mp4",
                        review_status="approved",
                        proposal_score=0.5,
                        transcript_score=0.0,
                        semantic_score=0.82,
                        novelty_score=0.2,
                        rerank_score=0.6,
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
                        review_status="rejected",
                        proposal_score=0.4,
                        transcript_score=0.0,
                        semantic_score=0.45,
                        novelty_score=0.2,
                        rerank_score=0.35,
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            trial_config = config_dir / "trial.yaml"
            trial_config.write_text("signal_thresholds:\n  semantic: 0.75\n", encoding="utf-8")
            with patch("run.load_config", return_value={"proxy_scanner": {"sources": {"hf_multimodal": _current_proxy_config()["hf_multimodal"]}, "weights": _current_proxy_config()["weights"], "candidate_selection": _current_proxy_config()["candidate_selection"], "cost_gates": _current_proxy_config()["cost_gates"]}}):
                result = run_replay_proxy_scoring(
                    root,
                    trial_config,
                    game="marvel_rivals",
                    min_reviewed=2,
                )
            self.assertTrue(result["ok"])
            self.assertEqual(result["trial_name"], "trial")
            self.assertEqual(result["recommendation"]["decision"], "prefer_trial")
            self.assertEqual(result["comparison"]["action_quality"]["current"]["skip"]["approved"], 1)
            self.assertEqual(result["comparison"]["action_quality"]["trial"]["inspect"]["approved"], 1)

    def test_replay_proxy_scoring_rejects_unsupported_trial_keys(self) -> None:
        with tempfile.TemporaryDirectory() as sidecar_root, tempfile.TemporaryDirectory() as config_root:
            root = Path(sidecar_root)
            config_dir = Path(config_root)
            (root / "approved.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        scan_id="approved",
                        game="marvel_rivals",
                        source="clip-approved.mp4",
                        review_status="approved",
                        proposal_score=0.9,
                        transcript_score=0.7,
                        semantic_score=0.82,
                        novelty_score=0.72,
                        rerank_score=0.91,
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            bad_trial = config_dir / "bad.yaml"
            bad_trial.write_text("components:\n  reranker:\n    enabled: false\n", encoding="utf-8")
            with patch("run.load_config", return_value={"proxy_scanner": {"sources": {"hf_multimodal": _current_proxy_config()["hf_multimodal"]}, "weights": _current_proxy_config()["weights"], "candidate_selection": _current_proxy_config()["candidate_selection"], "cost_gates": _current_proxy_config()["cost_gates"]}}):
                result = run_replay_proxy_scoring(root, bad_trial, game="marvel_rivals", min_reviewed=1)
            self.assertFalse(result["ok"])
            self.assertIn("unsupported keys", result["error"])

    def test_cli_routes_to_proxy_tuning(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--replay-proxy-scoring", "/tmp/proxy-sidecars", "--trial-proxy-config", "/tmp/trial.yaml"]
            stdout = io.StringIO()
            with patch("run.run_replay_proxy_scoring", return_value={"ok": True, "trial_name": "trial"}):
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertEqual(json.loads(stdout.getvalue())["trial_name"], "trial")
        finally:
            sys.argv = original_argv
