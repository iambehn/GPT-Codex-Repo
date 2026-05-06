from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.highlight_export_batch import (
    create_highlight_export_batch,
    record_post_ledger,
    record_posted_metrics_snapshot,
)
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from pipeline.shadow_ranking_replay import compare_shadow_ranking_replay, run_shadow_ranking_replay
from pipeline.v2_training_export import export_v2_training_datasets
from pipeline.workflow_run_state import create_workflow_run


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fused_sidecar(path: Path, *, game: str, source: Path, fusion_id: str, event_id: str, final_score: float) -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_analysis_v1",
            "fusion_id": fusion_id,
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "normalized_signals": [
                {
                    "signal_id": f"{event_id}-signal",
                    "signal_type": "character_identity",
                    "producer_family": "runtime",
                }
            ],
            "fused_events": [
                {
                    "event_id": event_id,
                    "event_type": "ability_plus_medal_combo",
                    "confidence": 0.84,
                    "final_score": final_score,
                    "gate_status": "confirmed",
                    "synergy_applied": True,
                    "minimum_required_signals_met": True,
                    "suggested_start_timestamp": 0.5,
                    "suggested_end_timestamp": 3.2,
                    "contributing_signals": [f"{event_id}-signal"],
                    "metadata": {"entity_id": "punisher", "ability_id": "ult"},
                }
            ],
            "fused_review": {
                "session_id": f"{fusion_id}-review",
                "reviewed_event_count": 1,
                "events": {event_id: {"review_status": "approved"}},
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _prepare_dataset(root: Path) -> tuple[dict, Path]:
    registry_path = root / "registry.sqlite"
    alpha_media = root / "media" / "alpha.mp4"
    beta_media = root / "media" / "beta.mp4"
    alpha_media.parent.mkdir(parents=True, exist_ok=True)
    alpha_media.write_bytes(b"alpha-video")
    beta_media.write_bytes(b"beta-video")

    alpha_fused = root / "fused" / "alpha.fused_analysis.json"
    beta_fused = root / "fused" / "beta.fused_analysis.json"
    _fused_sidecar(alpha_fused, game="marvel_rivals", source=alpha_media, fusion_id="fusion-alpha", event_id="fused-alpha", final_score=0.86)
    _fused_sidecar(beta_fused, game="marvel_rivals", source=beta_media, fusion_id="fusion-beta", event_id="fused-beta", final_score=0.94)
    refresh_clip_registry(root, registry_path=registry_path)

    export_highlight_selection(
        fused_sidecar=alpha_fused,
        output_path=root / "selection" / "alpha.highlight_selection.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    derive_hook_candidates(
        alpha_fused,
        registry_path=registry_path,
        output_path=root / "hooks" / "alpha.hook_candidates.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    workflow = create_workflow_run(
        "export_queue",
        registry_path=registry_path,
        output_path=root / "workflow" / "export.workflow_run.json",
    )
    export_batch = create_highlight_export_batch(
        registry_path=registry_path,
        workflow_run_id=workflow["workflow_run_id"],
        output_path=root / "exports" / "batch.highlight_export_batch.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    ledger = record_post_ledger(
        export_batch["manifest_path"],
        workflow_run_id=workflow["workflow_run_id"],
        platform="youtube",
        account_id="acct-1",
        output_path=root / "posted" / "ledger.posted_highlight_ledger.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)

    record_posted_metrics_snapshot(
        ledger["manifest_path"],
        workflow_run_id=workflow["workflow_run_id"],
        platform="youtube",
        account_id="acct-1",
        output_path=root / "metrics" / "snapshot.posted_highlight_metrics_snapshot.json",
        view_count=420,
        like_count=36,
        comment_count=8,
        share_count=5,
        save_count=4,
        watch_time_seconds=120.0,
        average_watch_time_seconds=8.0,
        completion_rate=0.56,
        engagement_rate=0.13,
    )
    refresh_clip_registry(root, registry_path=registry_path)

    dataset = export_v2_training_datasets(
        registry_path=registry_path,
        output_root=root / "dataset_exports",
        game="marvel_rivals",
    )
    return dataset, registry_path


class ShadowRankingReplayTests(unittest.TestCase):
    def test_run_shadow_ranking_replay_and_compare(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            dataset, registry_path = _prepare_dataset(root)

            replay = run_shadow_ranking_replay(
                dataset["manifest_path"],
                output_path=root / "shadow" / "replay.shadow_ranking_replay.json",
            )
            self.assertTrue(replay["ok"])
            self.assertEqual(replay["schema_version"], "shadow_ranking_replay_v1")
            self.assertEqual(replay["row_count"], 2)
            replay_rows = _read_jsonl(Path(replay["csv_path"]).with_suffix(".jsonl")) if False else replay["rows"]
            self.assertEqual(replay_rows[0]["candidate_id"] is not None, True)
            posted_row = next(row for row in replay_rows if row["post_present"])
            self.assertTrue(posted_row["latest_post_performance_label_eligible"])
            self.assertIsNotNone(posted_row["latest_post_performance_target_score"])
            self.assertEqual(posted_row["latest_post_performance_coverage_tier"], "posted_usable_metrics")

            comparison = compare_shadow_ranking_replay(
                replay["manifest_path"],
                output_path=root / "shadow" / "comparison.shadow_ranking_comparison.json",
            )
            self.assertTrue(comparison["ok"])
            self.assertEqual(comparison["schema_version"], "shadow_ranking_comparison_v1")
            self.assertEqual(comparison["recommendation"]["decision"], "prefer_shadow")

            first_refresh = refresh_clip_registry(root, registry_path=registry_path)
            second_refresh = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(first_refresh["ok"])
            self.assertTrue(second_refresh["ok"])
            self.assertEqual(first_refresh["shadow_ranking_replay_manifest_count"], 1)
            self.assertEqual(first_refresh["shadow_ranking_replay_row_count"], 2)
            self.assertEqual(first_refresh["shadow_ranking_comparison_report_count"], 1)
            self.assertEqual(first_refresh["shadow_ranking_comparison_row_count"], 2)

            replay_query = query_clip_registry(
                mode="shadow-ranking-replays",
                registry_path=registry_path,
                game="marvel_rivals",
            )
            self.assertEqual(replay_query["row_count"], 2)
            self.assertEqual(replay_query["rows"][0]["model_family"], "deterministic_shadow_baseline")

            comparison_query = query_clip_registry(
                mode="shadow-ranking-comparisons",
                registry_path=registry_path,
                recommendation_decision="prefer_shadow",
            )
            self.assertEqual(comparison_query["row_count"], 2)
            self.assertEqual(comparison_query["rows"][0]["recommendation_decision"], "prefer_shadow")

    def test_compare_shadow_ranking_replay_returns_keep_current_and_inconclusive(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            keep_current_replay = {
                "schema_version": "shadow_ranking_replay_v1",
                "replay_id": "keep-current",
                "model_family": "deterministic_shadow_baseline",
                "model_version": "v1",
                "dataset_manifest_path": "/tmp/dataset.json",
                "rows": [
                    {
                        "candidate_id": "a",
                        "event_id": "ea",
                        "game": "marvel_rivals",
                        "fixture_id": "fixture-a",
                        "heuristic_final_score": 0.95,
                        "predicted_candidate_score": 0.20,
                        "review_outcome": "approved",
                        "export_present": True,
                        "post_present": True,
                        "label_positive": True,
                        "label_score": 0.9,
                    },
                    {
                        "candidate_id": "b",
                        "event_id": "eb",
                        "game": "marvel_rivals",
                        "fixture_id": "fixture-b",
                        "heuristic_final_score": 0.20,
                        "predicted_candidate_score": 0.90,
                        "review_outcome": "approved",
                        "export_present": False,
                        "post_present": False,
                        "label_positive": False,
                        "label_score": 0.1,
                    },
                ],
            }
            keep_current_path = root / "keep-current.shadow_ranking_replay.json"
            _write_json(keep_current_path, keep_current_replay)
            keep_current = compare_shadow_ranking_replay(keep_current_path)
            self.assertEqual(keep_current["recommendation"]["decision"], "keep_current")

            inconclusive_replay = {
                "schema_version": "shadow_ranking_replay_v1",
                "replay_id": "inconclusive",
                "model_family": "deterministic_shadow_baseline",
                "model_version": "v1",
                "dataset_manifest_path": "/tmp/dataset.json",
                "rows": [
                    {
                        "candidate_id": "a",
                        "event_id": "ea",
                        "game": "marvel_rivals",
                        "fixture_id": "fixture-a",
                        "heuristic_final_score": 0.80,
                        "predicted_candidate_score": 0.79,
                        "review_outcome": "approved",
                        "export_present": True,
                        "post_present": False,
                        "label_positive": True,
                        "label_score": 0.7,
                    },
                    {
                        "candidate_id": "b",
                        "event_id": "eb",
                        "game": "marvel_rivals",
                        "fixture_id": "fixture-b",
                        "heuristic_final_score": 0.60,
                        "predicted_candidate_score": 0.61,
                        "review_outcome": "approved",
                        "export_present": False,
                        "post_present": False,
                        "label_positive": True,
                        "label_score": 0.65,
                    },
                ],
            }
            inconclusive_path = root / "inconclusive.shadow_ranking_replay.json"
            _write_json(inconclusive_path, inconclusive_replay)
            inconclusive = compare_shadow_ranking_replay(inconclusive_path)
            self.assertEqual(inconclusive["recommendation"]["decision"], "inconclusive")
