from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry


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


if __name__ == "__main__":
    unittest.main()
