from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import (
    _candidate_id,
    load_candidate_lifecycle_details,
    load_workflow_run_details,
    query_clip_registry,
    refresh_clip_registry,
    transition_candidate_lifecycle,
)
from pipeline.workflow_run_state import create_workflow_run, query_workflow_queue


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _proxy_sidecar(path: Path, *, game: str, source: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "proxy_scan_v1",
            "scan_id": "proxy-001",
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "windows": [
                {
                    "start_seconds": 2.0,
                    "end_seconds": 8.0,
                    "proxy_score": 0.83,
                    "signal_count": 2,
                    "recommended_action": "download_candidate",
                    "sources": ["audio_spike", "chat_spike"],
                    "source_families": ["audio_prepass", "chat_velocity"],
                }
            ],
            "proxy_review": {"review_status": "approved"},
            "sidecar_path": str(path.resolve()),
        },
    )


def _runtime_sidecar(path: Path, *, game: str, source: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "runtime_analysis_v1",
            "analysis_id": "runtime-001",
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "matcher": {
                "frame_count": 42,
                "confirmed_detections": [
                    {
                        "asset_id": "marvel_rivals.punisher.hero_portrait",
                        "roi_ref": "hero_portrait",
                        "entity_id": "punisher",
                        "first_timestamp": 1.0,
                        "last_timestamp": 1.5,
                        "peak_score": 0.98,
                    }
                ],
            },
            "events": {
                "event_count": 1,
                "rows": [
                    {
                        "event_id": "runtime-event-1",
                        "event_type": "pov_character_identified",
                        "confidence": 0.98,
                        "start_timestamp": 1.0,
                        "end_timestamp": 1.5,
                        "entity_id": "punisher",
                    }
                ],
            },
            "runtime_review": {
                "session_id": "runtime-session-1",
                "review_status": "approved",
                "recommended_action": "highlight_candidate",
                "highlight_score": 0.88,
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _fused_sidecar(path: Path, *, game: str, source: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_analysis_v1",
            "fusion_id": "fusion-001",
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "normalized_signals": [
                {
                    "signal_id": "signal-1",
                    "signal_type": "character_identity",
                    "producer_family": "runtime",
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                }
            ],
            "fused_events": [
                {
                    "event_id": "fused-event-1",
                    "event_type": "ability_plus_medal_combo",
                    "confidence": 0.84,
                    "final_score": 0.92,
                    "gate_status": "confirmed",
                    "synergy_applied": True,
                    "synergy_multiplier": 1.15,
                    "minimum_required_signals_met": True,
                    "suggested_start_timestamp": 0.5,
                    "suggested_end_timestamp": 3.2,
                    "contributing_signals": ["signal-1"],
                    "metadata": {"entity_id": "punisher"},
                }
            ],
            "fused_review": {
                "session_id": "fused-session-1",
                "reviewed_event_count": 1,
                "events": {
                    "fused-event-1": {"review_status": "approved"},
                },
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _runtime_review_session(path: Path, *, game: str, sidecar_path: Path, source: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "runtime_review_session_v1",
            "session_id": "runtime-session-1",
            "game": game,
            "selection_source": "/tmp/runtime-selection",
            "selection_action_filter": "highlight_candidate",
            "created_at": "2026-05-03T00:00:00+00:00",
            "item_count": 1,
            "items": [
                {
                    "sidecar_path": str(sidecar_path.resolve()),
                    "source": str(source.resolve()),
                    "analysis_id": "runtime-001",
                    "review_status": "approved",
                    "apply_status": "applied",
                    "highlight_score": 0.88,
                    "recommended_action": "highlight_candidate",
                    "gpt_meta_path": "/tmp/runtime.meta.json",
                }
            ],
            "manifest_path": str(path.resolve()),
        },
    )


def _fused_review_session(path: Path, *, game: str, sidecar_path: Path, source: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_review_session_v1",
            "session_id": "fused-session-1",
            "game": game,
            "selection_source": "/tmp/fused-selection",
            "selection_action_filter": "review_default",
            "selection_event_type_filter": None,
            "created_at": "2026-05-03T00:00:00+00:00",
            "item_count": 1,
            "items": [
                {
                    "sidecar_path": str(sidecar_path.resolve()),
                    "source": str(source.resolve()),
                    "fusion_id": "fusion-001",
                    "event_id": "fused-event-1",
                    "event_type": "ability_plus_medal_combo",
                    "review_status": "approved",
                    "apply_status": "applied",
                    "final_score": 0.92,
                    "recommended_action": "highlight_candidate",
                    "gate_status": "confirmed",
                    "gpt_meta_path": "/tmp/fused.meta.json",
                }
            ],
            "manifest_path": str(path.resolve()),
        },
    )


def _fixture_comparison_report(path: Path, *, proxy_sidecar_path: Path, runtime_sidecar_path: Path, game: str, source: Path) -> None:
    _write_json(
        path,
        {
            "ok": True,
            "schema_version": "fixture_sidecar_comparison_v1",
            "comparison": {
                "fixture_rows": [
                    {
                        "fixture_id": "fixture-a",
                        "label": "fixture-a",
                        "artifact_layer": "proxy",
                        "game": game,
                        "source": str(source.resolve()),
                        "coverage_status": "both",
                        "review_status": "approved",
                        "baseline_sidecar_path": str(proxy_sidecar_path.resolve()),
                        "trial_sidecar_path": str(proxy_sidecar_path.resolve()),
                        "baseline_action": "inspect",
                        "trial_action": "download_candidate",
                        "baseline_score": 0.74,
                        "trial_score": 0.83,
                        "score_delta": 0.09,
                        "shortlist_changed": True,
                        "rerank_changed": False,
                        "stage_latency_deltas": {"shot_detector": -3.0},
                        "recommendation_signal": "trial_better",
                    },
                    {
                        "fixture_id": "fixture-a",
                        "label": "fixture-a",
                        "artifact_layer": "runtime",
                        "game": game,
                        "source": str(source.resolve()),
                        "coverage_status": "both",
                        "review_status": "approved",
                        "baseline_sidecar_path": str(runtime_sidecar_path.resolve()),
                        "trial_sidecar_path": str(runtime_sidecar_path.resolve()),
                        "baseline_action": "inspect",
                        "trial_action": "highlight_candidate",
                        "baseline_score": 0.82,
                        "trial_score": 0.88,
                        "score_delta": 0.06,
                        "shortlist_changed": False,
                        "rerank_changed": False,
                        "stage_latency_deltas": {},
                        "recommendation_signal": "trial_better",
                    },
                ]
            },
            "recommendation": {"decision": "prefer_trial", "reason": "Trial improved reviewed fixtures."},
        },
    )


def _fixture_trial_run_manifest(path: Path, *, trial_name: str, game: str, source: Path, proxy_sidecar_path: Path, runtime_sidecar_path: Path, fused_sidecar_path: Path) -> None:
    _write_json(
        path,
        {
            "ok": True,
            "status": "ok",
            "schema_version": "fixture_trial_run_v1",
            "trial_name": trial_name,
            "fixture_manifest_path": "/tmp/fixtures.json",
            "fixture_source_manifest_path": "/tmp/fixture_sources.json",
            "trial_root": str(path.parent.resolve()),
            "proxy_sidecar_root": str((path.parent / "proxy").resolve()),
            "runtime_sidecar_root": str((path.parent / "runtime").resolve()),
            "fused_sidecar_root": str((path.parent / "fused").resolve()),
            "effective_overrides": {"proposal_backend": "pyscenedetect", "asr_backend": "distil_whisper"},
            "completed_fixture_count": 1,
            "failed_fixture_count": 0,
            "fixtures": [
                {
                    "fixture_id": "fixture-a",
                    "game": game,
                    "source_path": str(source.resolve()),
                    "status": "ok",
                    "layers": {
                        "proxy": {"sidecar_path": str(proxy_sidecar_path.resolve())},
                        "runtime": {"sidecar_path": str(runtime_sidecar_path.resolve())},
                        "fused": {"sidecar_path": str(fused_sidecar_path.resolve())},
                    },
                }
            ],
            "warnings": [],
        },
    )


def _fixture_trial_batch_manifest(path: Path, *, comparison_report_path: Path) -> None:
    _write_json(
        path,
        {
            "ok": True,
            "schema_version": "fixture_trial_batch_v1",
            "batch_name": "nightly",
            "baseline_trial_name": "baseline",
            "overall_recommendation": {"decision": "adopt_trial", "trial_name": "distil-whisper"},
            "selected_trials": ["baseline", "distil-whisper"],
            "trial_comparisons": [
                {
                    "trial_name": "distil-whisper",
                    "comparison_status": "ok",
                    "comparison_report_path": str(comparison_report_path.resolve()),
                    "artifact_layer": "proxy",
                    "recommendation": {"decision": "prefer_trial"},
                }
            ],
        },
    )


def _highlight_selection_manifest(
    path: Path,
    *,
    game: str,
    source: Path,
    proxy_sidecar_path: Path | None = None,
    fused_sidecar_path: Path | None = None,
    candidate_id: str | None = None,
    event_id: str | None = None,
) -> None:
    selection_basis = "fused" if fused_sidecar_path is not None else "proxy"
    _write_json(
        path,
        {
            "ok": True,
            "schema_version": "highlight_selection_v1",
            "game": game,
            "source": str(source.resolve()),
            "selection_basis": selection_basis,
            "proxy_sidecar_path": str(proxy_sidecar_path.resolve()) if proxy_sidecar_path is not None else None,
            "fused_sidecar_path": str(fused_sidecar_path.resolve()) if fused_sidecar_path is not None else None,
            "selected_highlight_count": 1,
            "selected_highlights": [
                (
                    {
                        "highlight_id": "highlight-0",
                        "candidate_id": candidate_id,
                        "fusion_id": "fusion-001",
                        "event_id": event_id or "fused-event-1",
                        "start_seconds": 0.5,
                        "end_seconds": 3.2,
                        "final_score": 0.92,
                        "recommended_action": "highlight_candidate",
                        "gate_status": "confirmed",
                        "event_type": "ability_plus_medal_combo",
                        "contributing_producer_families": ["runtime"],
                    }
                    if selection_basis == "fused"
                    else {
                        "highlight_id": "highlight-0",
                        "start_seconds": 0.5,
                        "end_seconds": 3.2,
                        "proxy_score": 0.83,
                        "recommended_action": "download_candidate",
                    }
                )
            ],
        },
    )


def _highlight_export_batch_manifest(
    path: Path,
    *,
    game: str,
    source: Path,
    fused_sidecar_path: Path,
    selection_manifest_path: Path,
    candidate_id: str,
    event_id: str,
) -> None:
    _write_json(
        path,
        {
            "schema_version": "highlight_export_batch_v1",
            "export_batch_id": "export-batch-1",
            "game": game,
            "workflow_run_id": "workflow-export-1",
            "selection_manifest_path": str(selection_manifest_path.resolve()),
            "linked_inputs": {
                "fused_sidecar_paths": [str(fused_sidecar_path.resolve())],
                "selection_manifest_paths": [str(selection_manifest_path.resolve())],
                "hook_manifest_paths": [],
            },
            "export_count": 1,
            "created_at": "2026-05-06T00:00:00+00:00",
            "exports": [
                {
                    "export_id": "export-1",
                    "candidate_id": candidate_id,
                    "event_id": event_id,
                    "hook_id": "hook-1",
                    "fixture_id": "fixture-a",
                    "source": str(source.resolve()),
                    "fused_sidecar_path": str(fused_sidecar_path.resolve()),
                    "highlight_selection_manifest_path": str(selection_manifest_path.resolve()),
                    "start_seconds": 0.5,
                    "end_seconds": 3.2,
                    "final_score": 0.92,
                    "hook_archetype": "character",
                    "hook_mode": "natural",
                    "packaging_strategy": "single_clip",
                    "export_status": "exported",
                    "export_artifact_path": str((path.parent / "rendered" / "export-1.mp4").resolve()),
                    "otio_path": str((path.parent / "rendered" / "export-1.otio.json").resolve()),
                }
            ],
        },
    )


def _posted_highlight_ledger(
    path: Path,
    *,
    export_batch_manifest_path: Path,
    candidate_id: str,
    event_id: str,
) -> None:
    _write_json(
        path,
        {
            "schema_version": "posted_highlight_ledger_v1",
            "ledger_id": "ledger-1",
            "platform": "tiktok",
            "account_id": "acct-1",
            "workflow_run_id": "workflow-post-1",
            "posted_count": 1,
            "created_at": "2026-05-06T00:00:00+00:00",
            "posted_records": [
                {
                    "post_record_id": "post-1",
                    "export_id": "export-1",
                    "candidate_id": candidate_id,
                    "event_id": event_id,
                    "hook_id": "hook-1",
                    "export_batch_manifest_path": str(export_batch_manifest_path.resolve()),
                    "posted_at": "2026-05-06T01:00:00+00:00",
                    "post_status": "posted",
                    "external_post_id": "ext-1",
                    "external_url": "https://example.com/post/1",
                    "caption_text": "caption",
                    "duration_seconds": 2.7,
                    "media_asset_path": str((path.parent / "posted" / "export-1.mp4").resolve()),
                }
            ],
        },
    )


def _posted_metrics_snapshot(
    path: Path,
    *,
    post_ledger_manifest_path: Path,
) -> None:
    _write_json(
        path,
        {
            "schema_version": "posted_highlight_metrics_snapshot_v1",
            "snapshot_id": "metrics-1",
            "platform": "tiktok",
            "account_id": "acct-1",
            "workflow_run_id": "workflow-metrics-1",
            "captured_at": "2026-05-06T02:00:00+00:00",
            "snapshot_count": 1,
            "snapshots": [
                {
                    "snapshot_row_id": "snapshot-row-1",
                    "post_record_id": "post-1",
                    "export_id": "export-1",
                    "candidate_id": None,
                    "post_ledger_manifest_path": str(post_ledger_manifest_path.resolve()),
                    "external_post_id": "ext-1",
                    "external_url": "https://example.com/post/1",
                    "view_count": 100,
                    "like_count": 20,
                    "comment_count": 5,
                    "share_count": 3,
                    "save_count": 2,
                    "watch_time_seconds": 50.0,
                    "average_watch_time_seconds": 5.5,
                    "completion_rate": 0.7,
                    "engagement_rate": 0.3,
                }
            ],
        },
    )


class ClipRegistryTests(unittest.TestCase):
    def test_refresh_ingests_sidecars_and_review_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")

            proxy_path = root / "proxy" / "alpha.proxy_scan.json"
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            runtime_session_path = root / "review" / "runtime.runtime_review_session.json"
            fused_session_path = root / "review" / "fused.fused_review_session.json"
            registry_path = root / "registry.sqlite"

            _proxy_sidecar(proxy_path, game="marvel_rivals", source=media)
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            _runtime_review_session(runtime_session_path, game="marvel_rivals", sidecar_path=runtime_path, source=media)
            _fused_review_session(fused_session_path, game="marvel_rivals", sidecar_path=fused_path, source=media)

            result = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(result["ok"])
            self.assertEqual(result["clip_row_count"], 1)
            self.assertEqual(result["proxy_window_row_count"], 1)
            self.assertEqual(result["runtime_event_row_count"], 1)
            self.assertEqual(result["fused_event_row_count"], 1)
            self.assertEqual(result["runtime_review_item_row_count"], 1)
            self.assertEqual(result["fused_review_item_row_count"], 1)

            connection = sqlite3.connect(str(registry_path))
            try:
                clip_row = connection.execute(
                    "SELECT proxy_review_status, runtime_review_status, fused_review_status FROM clips"
                ).fetchone()
                self.assertEqual(tuple(clip_row), ("approved", "approved", "approved"))
            finally:
                connection.close()

    def test_query_fused_events_returns_ranked_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            result = query_clip_registry(
                mode="fused-events",
                game="marvel_rivals",
                gate_status="confirmed",
                registry_path=root / "registry.sqlite",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["event_type"], "ability_plus_medal_combo")
            self.assertEqual(row["review_status"], "approved")
            self.assertEqual(row["gate_status"], "confirmed")
            self.assertGreater(row["final_score"], 0.9)

    def test_query_runtime_events_returns_review_linkage(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            result = query_clip_registry(
                mode="runtime-events",
                game="marvel_rivals",
                review_status="approved",
                registry_path=root / "registry.sqlite",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["analysis_id"], "runtime-001")
            self.assertEqual(row["event_type"], "pov_character_identified")
            self.assertEqual(row["review_status"], "approved")

    def test_refresh_rebuilds_without_duplicate_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            registry_path = root / "registry.sqlite"

            refresh_clip_registry(root, registry_path=registry_path)
            refresh_clip_registry(root, registry_path=registry_path)

            connection = sqlite3.connect(str(registry_path))
            try:
                runtime_count = connection.execute("SELECT COUNT(*) FROM runtime_analyses").fetchone()[0]
                fused_count = connection.execute("SELECT COUNT(*) FROM fused_events").fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(runtime_count, 1)
            self.assertEqual(fused_count, 1)

    def test_missing_review_sidecar_emits_warning_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            session_path = root / "review" / "runtime.runtime_review_session.json"
            _runtime_review_session(
                session_path,
                game="marvel_rivals",
                sidecar_path=root / "runtime" / "missing.runtime_analysis.json",
                source=media,
            )
            before_text = session_path.read_text(encoding="utf-8")

            result = refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["warning_count"], 1)
            self.assertEqual(session_path.read_text(encoding="utf-8"), before_text)

    def test_refresh_ingests_fixture_artifacts_and_updates_clip_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "proxy" / "alpha.proxy_scan.json"
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            comparison_path = root / "comparisons" / "baseline-vs-trial.json"
            run_manifest_path = root / "trial" / "fixture_trial_run_manifest.json"
            batch_manifest_path = root / "trial" / "fixture_trial_batch_manifest.json"
            _proxy_sidecar(proxy_path, game="marvel_rivals", source=media)
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            _fixture_comparison_report(comparison_path, proxy_sidecar_path=proxy_path, runtime_sidecar_path=runtime_path, game="marvel_rivals", source=media)
            _fixture_trial_run_manifest(run_manifest_path, trial_name="distil-whisper", game="marvel_rivals", source=media, proxy_sidecar_path=proxy_path, runtime_sidecar_path=runtime_path, fused_sidecar_path=fused_path)
            _fixture_trial_batch_manifest(batch_manifest_path, comparison_report_path=comparison_path)

            result = refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            self.assertTrue(result["ok"])
            self.assertEqual(result["fixture_comparison_report_count"], 1)
            self.assertEqual(result["fixture_trial_run_manifest_count"], 1)
            self.assertEqual(result["fixture_trial_batch_manifest_count"], 1)
            connection = sqlite3.connect(str(root / "registry.sqlite"))
            try:
                clip_row = connection.execute(
                    "SELECT fixture_ids_json, top_proxy_action, top_fused_action, has_trial_preference, has_proxy_sidecar, has_fused_sidecar FROM clips"
                ).fetchone()
                self.assertIn("fixture-a", str(clip_row[0]))
                self.assertEqual(clip_row[1], "download_candidate")
                self.assertEqual(clip_row[2], "highlight_candidate")
                self.assertEqual(clip_row[3], 1)
                self.assertEqual(clip_row[4], 1)
                self.assertEqual(clip_row[5], 1)
            finally:
                connection.close()

    def test_query_fixture_comparisons_filters_by_fixture_and_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "proxy" / "alpha.proxy_scan.json"
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            _proxy_sidecar(proxy_path, game="marvel_rivals", source=media)
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            _fixture_comparison_report(root / "fixture_comparison.json", proxy_sidecar_path=proxy_path, runtime_sidecar_path=runtime_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            result = query_clip_registry(
                mode="fixture-comparisons",
                fixture_id="fixture-a",
                artifact_layer="proxy",
                recommendation_decision="prefer_trial",
                registry_path=root / "registry.sqlite",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["fixture_id"], "fixture-a")
            self.assertEqual(row["artifact_layer"], "proxy")
            self.assertEqual(row["recommendation_decision"], "prefer_trial")
            self.assertEqual(row["coverage_status"], "both")

    def test_query_fixture_trials_and_batch_comparisons(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "proxy" / "alpha.proxy_scan.json"
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            comparison_path = root / "comparisons" / "comparison.json"
            _proxy_sidecar(proxy_path, game="marvel_rivals", source=media)
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            _fixture_comparison_report(comparison_path, proxy_sidecar_path=proxy_path, runtime_sidecar_path=runtime_path, game="marvel_rivals", source=media)
            _fixture_trial_run_manifest(root / "trial" / "fixture_trial_run_manifest.json", trial_name="distil-whisper", game="marvel_rivals", source=media, proxy_sidecar_path=proxy_path, runtime_sidecar_path=runtime_path, fused_sidecar_path=fused_path)
            _fixture_trial_batch_manifest(root / "trial" / "fixture_trial_batch_manifest.json", comparison_report_path=comparison_path)
            refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            trial_result = query_clip_registry(
                mode="fixture-trials",
                fixture_id="fixture-a",
                trial_name="distil-whisper",
                registry_path=root / "registry.sqlite",
            )
            batch_result = query_clip_registry(
                mode="batch-comparisons",
                trial_name="distil-whisper",
                artifact_layer="proxy",
                recommendation_decision="prefer_trial",
                registry_path=root / "registry.sqlite",
            )

            self.assertTrue(trial_result["ok"])
            self.assertEqual(trial_result["row_count"], 1)
            self.assertEqual(trial_result["rows"][0]["trial_name"], "distil-whisper")
            self.assertEqual(trial_result["rows"][0]["fixture_id"], "fixture-a")
            self.assertTrue(batch_result["ok"])
            self.assertEqual(batch_result["row_count"], 1)
            self.assertEqual(batch_result["rows"][0]["batch_name"], "nightly")
            self.assertEqual(batch_result["rows"][0]["recommendation_decision"], "prefer_trial")

    def test_query_clips_and_fused_events_support_disagreement_and_fixture_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            proxy_path = root / "proxy" / "alpha.proxy_scan.json"
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            _proxy_sidecar(proxy_path, game="marvel_rivals", source=media)
            _runtime_sidecar(runtime_path, game="marvel_rivals", source=media)
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            proxy_payload = json.loads(proxy_path.read_text(encoding="utf-8"))
            proxy_payload["proxy_review"]["review_status"] = "rejected"
            proxy_path.write_text(json.dumps(proxy_payload, indent=2), encoding="utf-8")
            _fixture_comparison_report(root / "fixture_comparison.json", proxy_sidecar_path=proxy_path, runtime_sidecar_path=runtime_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            clip_result = query_clip_registry(
                mode="clips",
                fixture_id="fixture-a",
                has_disagreement=True,
                registry_path=root / "registry.sqlite",
            )
            fused_result = query_clip_registry(
                mode="fused-events",
                fixture_id="fixture-a",
                has_disagreement=True,
                registry_path=root / "registry.sqlite",
            )

            self.assertTrue(clip_result["ok"])
            self.assertEqual(clip_result["row_count"], 1)
            self.assertEqual(clip_result["rows"][0]["has_review_disagreement"], 1)
            self.assertTrue(fused_result["ok"])
            self.assertEqual(fused_result["row_count"], 1)

    def test_malformed_fixture_trial_manifest_emits_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            trial_manifest = root / "trial" / "fixture_trial_run_manifest.json"
            trial_manifest.parent.mkdir(parents=True, exist_ok=True)
            trial_manifest.write_text("{not-json", encoding="utf-8")

            result = refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["warning_count"], 1)

    def test_refresh_creates_candidate_lifecycles_and_preserves_derived_transitions(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            candidate_id = _candidate_id(
                game="marvel_rivals",
                source=str(media.resolve()),
                fused_sidecar_path=str(fused_path.resolve()),
                event_id="fused-event-1",
            )
            _highlight_selection_manifest(
                root / "exports" / "alpha.highlight_selection.json",
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )

            first = refresh_clip_registry(root, registry_path=registry_path)
            second = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            query = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="selected_for_export",
                registry_path=registry_path,
            )
            self.assertEqual(query["row_count"], 1)
            row = query["rows"][0]
            self.assertEqual(row["latest_review_status"], "approved")
            self.assertEqual(row["selection_basis"], "fused")
            selected_details = json.loads(row["selected_highlight_details_json"])
            self.assertEqual(selected_details["candidate_id"], candidate_id)
            self.assertEqual(selected_details["event_id"], "fused-event-1")
            transitions = json.loads(row["transitions_json"])
            self.assertEqual(len(transitions), 1)
            self.assertEqual(transitions[0]["to_state"], "selected_for_export")
            self.assertEqual(first["candidate_lifecycle_row_count"], 1)

    def test_refresh_preserves_selected_highlight_details_in_export_and_post_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            candidate_id = _candidate_id(
                game="marvel_rivals",
                source=str(media.resolve()),
                fused_sidecar_path=str(fused_path.resolve()),
                event_id="fused-event-1",
            )
            selection_manifest_path = root / "exports" / "alpha.highlight_selection.json"
            _highlight_selection_manifest(
                selection_manifest_path,
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            export_batch_path = root / "exports" / "alpha.highlight_export_batch.json"
            _highlight_export_batch_manifest(
                export_batch_path,
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                selection_manifest_path=selection_manifest_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            _posted_highlight_ledger(
                root / "posted" / "alpha.posted_highlight_ledger.json",
                export_batch_manifest_path=export_batch_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )

            result = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(result["ok"])
            export_rows = query_clip_registry(
                mode="highlight-exports",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(export_rows["row_count"], 1)
            export_selected = json.loads(export_rows["rows"][0]["selected_highlight_details_json"])
            self.assertEqual(export_selected["candidate_id"], candidate_id)
            self.assertEqual(export_selected["event_id"], "fused-event-1")

            posted_rows = query_clip_registry(
                mode="post-ledger-records",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(posted_rows["row_count"], 1)
            posted_selected = json.loads(posted_rows["rows"][0]["selected_highlight_details_json"])
            self.assertEqual(posted_selected["candidate_id"], candidate_id)
            self.assertEqual(posted_selected["event_id"], "fused-event-1")

    def test_refresh_preserves_selected_highlight_details_in_posted_metrics_and_rollups(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            candidate_id = _candidate_id(
                game="marvel_rivals",
                source=str(media.resolve()),
                fused_sidecar_path=str(fused_path.resolve()),
                event_id="fused-event-1",
            )
            selection_manifest_path = root / "exports" / "alpha.highlight_selection.json"
            _highlight_selection_manifest(
                selection_manifest_path,
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            export_batch_path = root / "exports" / "alpha.highlight_export_batch.json"
            _highlight_export_batch_manifest(
                export_batch_path,
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                selection_manifest_path=selection_manifest_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            post_ledger_path = root / "posted" / "alpha.posted_highlight_ledger.json"
            _posted_highlight_ledger(
                post_ledger_path,
                export_batch_manifest_path=export_batch_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            _posted_metrics_snapshot(
                root / "posted" / "alpha.posted_highlight_metrics_snapshot.json",
                post_ledger_manifest_path=post_ledger_path,
            )

            result = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(result["ok"])
            metrics_rows = query_clip_registry(
                mode="posted-metrics",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(metrics_rows["row_count"], 1)
            selected_details = json.loads(metrics_rows["rows"][0]["selected_highlight_details_json"])
            self.assertEqual(selected_details["candidate_id"], candidate_id)
            self.assertEqual(selected_details["event_type"], "ability_plus_medal_combo")

            rollups = query_clip_registry(
                mode="posted-performance-rollups",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(rollups["row_count"], 1)
            row = rollups["rows"][0]
            self.assertEqual(json.loads(row["by_selected_event_type_json"]), {"ability_plus_medal_combo": 1})
            self.assertEqual(json.loads(row["by_selected_producer_family_json"]), {"runtime": 1})

    def test_transition_candidate_lifecycle_updates_state_and_history(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)

            lifecycle_rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)
            candidate_id = lifecycle_rows["rows"][0]["candidate_id"]
            result = transition_candidate_lifecycle(
                candidate_id,
                "selected_for_export",
                reason="Operator selected the event for export.",
                actor="tester",
                registry_path=registry_path,
            )

            self.assertTrue(result["ok"])
            updated = query_clip_registry(
                mode="candidate-lifecycles",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(updated["rows"][0]["lifecycle_state"], "selected_for_export")
            transitions = json.loads(updated["rows"][0]["transitions_json"])
            self.assertEqual(transitions[-1]["to_state"], "selected_for_export")
            self.assertEqual(transitions[-1]["actor"], "tester")

    def test_invalid_candidate_lifecycle_transition_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)

            lifecycle_rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)
            candidate_id = lifecycle_rows["rows"][0]["candidate_id"]
            result = transition_candidate_lifecycle(
                candidate_id,
                "posted",
                registry_path=registry_path,
            )

            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "invalid lifecycle transition")

    def test_load_candidate_lifecycle_details_filters_by_fused_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)

            rows = load_candidate_lifecycle_details(
                game="marvel_rivals",
                source=str(media.resolve()),
                fused_sidecar_path=fused_path,
                registry_path=registry_path,
            )

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event_id"], "fused-event-1")

    def test_create_workflow_run_writes_deterministic_selection_queue_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)

            first = create_workflow_run("selection_queue", registry_path=registry_path, output_path=root / "workflow" / "selection.workflow_run.json")
            second = create_workflow_run("selection_queue", registry_path=registry_path, output_path=root / "workflow" / "selection.workflow_run.json")

            self.assertTrue(first["ok"])
            self.assertEqual(first["workflow_run_id"], second["workflow_run_id"])
            manifest = json.loads(Path(first["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "workflow_run_v1")
            self.assertEqual(manifest["workflow_type"], "selection_queue")
            self.assertEqual(manifest["stage"], "approved")
            self.assertEqual(manifest["item_counts"]["total"], 1)
            self.assertEqual(manifest["items"][0]["candidate_id"][:10], "candidate-")

    def test_refresh_ingests_workflow_runs_without_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            create_workflow_run("selection_queue", registry_path=registry_path, output_path=root / "workflow" / "selection.workflow_run.json")

            first = refresh_clip_registry(root, registry_path=registry_path)
            second = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["workflow_run_manifest_count"], 1)
            query = query_clip_registry(mode="workflow-runs", registry_path=registry_path)
            self.assertEqual(query["row_count"], 1)
            self.assertEqual(query["rows"][0]["workflow_type"], "selection_queue")
            details = load_workflow_run_details(registry_path=registry_path)
            self.assertEqual(len(details), 1)

    def test_query_workflow_queue_maps_all_phase_one_queue_classes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)

            payload = json.loads(fused_path.read_text(encoding="utf-8"))
            payload["fused_review"] = {"session_id": "fused-session-1", "reviewed_event_count": 0, "events": {}}
            fused_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            refresh_clip_registry(root, registry_path=registry_path)

            review_queue = query_workflow_queue("review_queue", registry_path=registry_path)
            self.assertEqual(review_queue["row_count"], 1)
            self.assertEqual(review_queue["rows"][0]["lifecycle_state"], "pending_review")

            payload["fused_review"] = {
                "session_id": "fused-session-1",
                "reviewed_event_count": 1,
                "events": {"fused-event-1": {"review_status": "approved"}},
            }
            fused_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            refresh_clip_registry(root, registry_path=registry_path)
            selection_queue = query_workflow_queue("selection_queue", registry_path=registry_path)
            self.assertEqual(selection_queue["row_count"], 1)
            self.assertEqual(selection_queue["rows"][0]["lifecycle_state"], "approved")

            lifecycle_rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)
            candidate_id = lifecycle_rows["rows"][0]["candidate_id"]
            _highlight_selection_manifest(
                root / "exports" / "alpha.highlight_selection.json",
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            export_queue = query_workflow_queue("export_queue", registry_path=registry_path)
            self.assertEqual(export_queue["row_count"], 1)
            self.assertEqual(export_queue["rows"][0]["lifecycle_state"], "selected_for_export")

            transition_candidate_lifecycle(candidate_id, "exported", registry_path=registry_path)
            post_queue = query_workflow_queue("post_queue", registry_path=registry_path)
            self.assertEqual(post_queue["row_count"], 1)
            self.assertEqual(post_queue["rows"][0]["lifecycle_state"], "exported")

    def test_workflow_run_query_preserves_candidate_and_artifact_linkage(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            lifecycle_rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)
            candidate_id = lifecycle_rows["rows"][0]["candidate_id"]
            _highlight_selection_manifest(
                root / "exports" / "alpha.highlight_selection.json",
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            create_workflow_run("export_queue", registry_path=registry_path, output_path=root / "workflow" / "export.workflow_run.json")
            refresh_clip_registry(root, registry_path=registry_path)

            result = query_clip_registry(
                mode="workflow-runs",
                workflow_type="export_queue",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )

            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["candidate_id"], candidate_id)
            self.assertEqual(row["lifecycle_state"], "selected_for_export")
            self.assertTrue(row["highlight_selection_manifest_path"].endswith(".highlight_selection.json"))

    def test_query_workflow_queue_prefers_current_lifecycle_over_stale_workflow_history(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            create_workflow_run("selection_queue", registry_path=registry_path, output_path=root / "workflow" / "selection.workflow_run.json")
            refresh_clip_registry(root, registry_path=registry_path)

            lifecycle_rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)
            candidate_id = lifecycle_rows["rows"][0]["candidate_id"]
            _highlight_selection_manifest(
                root / "exports" / "alpha.highlight_selection.json",
                game="marvel_rivals",
                source=media,
                fused_sidecar_path=fused_path,
                candidate_id=candidate_id,
                event_id="fused-event-1",
            )
            refresh_clip_registry(root, registry_path=registry_path)

            selection_queue = query_workflow_queue("selection_queue", registry_path=registry_path)
            export_queue = query_workflow_queue("export_queue", registry_path=registry_path)

            self.assertEqual(selection_queue["row_count"], 0)
            self.assertEqual(export_queue["row_count"], 1)
            self.assertEqual(export_queue["rows"][0]["latest_workflow_run_id"][:9], "workflow-")

    def test_malformed_downstream_artifacts_emit_warnings_without_corrupting_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)

            _write_json(
                root / "workflow" / "broken.workflow_run.json",
                {
                    "schema_version": "workflow_run_v1",
                    "workflow_run_id": "broken-run",
                    "items": [{"item_status": "ready"}],
                },
            )
            _write_json(
                root / "posted" / "broken.posted_highlight_ledger.json",
                {
                    "schema_version": "posted_highlight_ledger_v1",
                    "ledger_id": "broken-ledger",
                    "posted_count": 1,
                    "posted_records": [
                        {
                            "post_record_id": "post-broken",
                            "export_id": "missing-export",
                            "candidate_id": "candidate-broken",
                            "export_batch_manifest_path": str(root / "exports" / "missing.highlight_export_batch.json"),
                        }
                    ],
                },
            )

            result = refresh_clip_registry(root, registry_path=registry_path)
            clips = query_clip_registry(mode="clips", registry_path=registry_path)

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["warning_count"], 2)
            self.assertEqual(clips["row_count"], 1)

    def test_refresh_and_query_real_artifact_intake_dashboards(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = root / "registry.sqlite"
            reports = root / "reports"
            _write_json(
                reports / "older.real_artifact_intake.dashboard.json",
                {
                    "ok": True,
                    "status": "ok",
                    "schema_version": "real_artifact_intake_dashboard_v1",
                    "generated_at": "2026-05-05T09:00:00+00:00",
                    "intake_root": str((root / "intake").resolve()),
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "headline_status": "warning",
                    "current_intake": {
                        "intake_status": "warning",
                        "bundle_count": 2,
                        "warning_count": 1,
                        "bundle_readiness_rollups": {"readiness_status_counts": {"benchmark_ready": 1}},
                        "coverage_inventory": {"eligible_real_post_performance_label_count": 1},
                    },
                    "preflight_trends": {"trend_status": "stable", "entry_count": 1},
                    "refresh_outcome_trends": {"trend_status": "stable", "entry_count": 1},
                    "history_comparison": {
                        "history_alignment": {
                            "preflight_to_refresh_status": "aligned",
                            "real_vs_synthetic_status": "diverged",
                            "next_focus": "expand_real_evidence",
                        }
                    },
                },
            )
            _write_json(
                reports / "latest.real_artifact_intake.dashboard.json",
                {
                    "ok": True,
                    "status": "ok",
                    "schema_version": "real_artifact_intake_dashboard_v1",
                    "generated_at": "2026-05-05T10:00:00+00:00",
                    "intake_root": str((root / "intake").resolve()),
                    "filters": {"game": "marvel_rivals", "platform": "youtube"},
                    "headline_status": "ready",
                    "current_intake": {
                        "intake_status": "ready",
                        "bundle_count": 3,
                        "warning_count": 0,
                        "bundle_readiness_rollups": {"readiness_status_counts": {"benchmark_ready": 2}},
                        "coverage_inventory": {"eligible_real_post_performance_label_count": 4},
                    },
                    "preflight_trends": {"trend_status": "improving", "entry_count": 3},
                    "refresh_outcome_trends": {"trend_status": "improving", "entry_count": 2},
                    "history_comparison": {
                        "history_alignment": {
                            "preflight_to_refresh_status": "aligned",
                            "real_vs_synthetic_status": "narrowing",
                            "next_focus": "run_real_only_refresh",
                        }
                    },
                },
            )

            result = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(result["ok"])
            self.assertEqual(result["real_artifact_intake_dashboard_manifest_count"], 2)

            dashboards = query_clip_registry(
                mode="real-artifact-intake-dashboards",
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
            )
            self.assertEqual(dashboards["row_count"], 2)
            self.assertEqual(dashboards["rows"][0]["headline_status"], "ready")
            self.assertEqual(dashboards["rows"][0]["preflight_trend_status"], "improving")
            self.assertEqual(dashboards["rows"][0]["benchmark_ready_bundle_count"], 2)
            self.assertEqual(dashboards["rows"][0]["eligible_real_post_performance_label_count"], 4)

            ready_only = query_clip_registry(
                mode="real-artifact-intake-dashboards",
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                status="ready",
                limit=1,
            )
            self.assertEqual(ready_only["row_count"], 1)
            self.assertEqual(ready_only["rows"][0]["headline_status"], "ready")
            self.assertEqual(ready_only["rows"][0]["next_focus"], "run_real_only_refresh")


if __name__ == "__main__":
    unittest.main()
