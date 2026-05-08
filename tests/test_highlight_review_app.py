from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import refresh_clip_registry
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from pipeline.highlight_review_app import (
    _write_proxy_review_session_decision,
    launch_highlight_review_app,
    load_highlight_review_records,
)


def _proxy_sidecar(source: Path) -> dict[str, object]:
    return {
        "schema_version": "proxy_scan_v1",
        "scan_id": "proxy-123abc",
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "proxy_review": {"review_status": "approved"},
        "windows": [],
    }


def _fused_sidecar(source: Path) -> dict[str, object]:
    return {
        "schema_version": "fused_analysis_v1",
        "fusion_id": "fused-123abc",
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "normalized_signals": [
            {
                "signal_id": "signal-runtime-1",
                "producer_family": "runtime",
            }
        ],
        "fused_events": [
            {
                "event_id": "fused-1",
                "event_type": "ability_plus_medal_combo",
                "final_score": 0.91,
                "confidence": 0.91,
                "gate_status": "confirmed",
                "minimum_required_signals_met": True,
                "suggested_start_timestamp": 0.5,
                "suggested_end_timestamp": 3.0,
                "contributing_signals": ["signal-runtime-1"],
                "metadata": {},
            }
        ],
        "fused_review": {"events": {"fused-1": {"review_status": "approved"}}},
    }


def _fixture_comparison_report(sidecar_path: Path) -> dict[str, object]:
    return {
        "ok": True,
        "comparison": {
            "fixture_rows": [
                {
                    "fixture_id": "commentary-heavy-001",
                    "artifact_layer": "proxy",
                    "coverage_status": "both",
                    "review_status": "approved",
                    "baseline_sidecar_path": str(sidecar_path.resolve()),
                    "trial_sidecar_path": str(sidecar_path.resolve()),
                    "baseline_action": "inspect",
                    "trial_action": "download_candidate",
                    "score_delta": 0.1,
                    "shortlist_changed": False,
                    "rerank_changed": False,
                    "stage_latency_deltas": {},
                    "recommendation_signal": "trial_better",
                }
            ]
        },
        "recommendation": {"decision": "prefer_trial"},
    }


def _fixture_trial_batch_manifest(report_path: Path) -> dict[str, object]:
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
                "comparison_report_path": str(report_path.resolve()),
                "artifact_layer": "proxy",
                "recommendation": {"decision": "prefer_trial"},
            }
        ],
    }


class _FakeBlocks:
    def __init__(self, *args, **kwargs) -> None:
        self.loaded = False
        self.launch_kwargs: dict[str, object] | None = None

    def __enter__(self) -> "_FakeBlocks":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def load(self, *args, **kwargs) -> None:
        self.loaded = True

    def launch(self, **kwargs) -> None:
        self.launch_kwargs = kwargs
        return None


class _FakeLayout:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs

    def __enter__(self) -> "_FakeLayout":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeComponent:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs

    def change(self, *args, **kwargs) -> None:
        return None

    def click(self, *args, **kwargs) -> None:
        return None


def _proxy_review_session(root: Path, source: Path, sidecar: Path, *, review_status: str | None = None) -> dict[str, object]:
    processing_root = root / "gpt" / "processing" / "marvel_rivals"
    inbox_root = root / "gpt" / "inbox" / "marvel_rivals"
    processing_root.mkdir(parents=True, exist_ok=True)
    inbox_root.mkdir(parents=True, exist_ok=True)
    processed_path = processing_root / "proxy-review-001.mp4"
    processed_path.write_bytes(b"video")
    meta_path = inbox_root / "proxy-review-001.meta.json"
    meta_payload = {
        "clip_id": "proxy-review-001",
        "game": "marvel_rivals",
        "clip_path": str(processed_path),
        "processed_path": str(processed_path),
        "meta_path": str(meta_path),
        "status": "queue",
        "selected_template_id": "proxy_review_bridge",
    }
    if review_status is not None:
        meta_payload["review_status"] = review_status
    meta_path.write_text(json.dumps(meta_payload, indent=2), encoding="utf-8")
    (inbox_root / f"{source.stem}.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nhello\n", encoding="utf-8")
    (inbox_root / f"{source.stem}.whisper.json").write_text(json.dumps({"segments": []}), encoding="utf-8")
    return {
        "schema_version": "proxy_review_session_v1",
        "session_id": "proxy-session-123",
        "game": "marvel_rivals",
        "gpt_repo": str((root / "gpt").resolve()),
        "selection_source": str(root / "batch.json"),
        "selection_action_filter": "download_candidate",
        "limit": None,
        "created_at": "2026-05-08T00:00:00+00:00",
        "materialization_mode": "copy",
        "item_count": 1,
        "items": [
            {
                "clip_id": "proxy-review-001",
                "sidecar_path": str(sidecar.resolve()),
                "source": str(source.resolve()),
                "gpt_processed_path": str(processed_path.resolve()),
                "gpt_meta_path": str(meta_path.resolve()),
                "top_proxy_score": 0.81,
                "top_recommended_action": "download_candidate",
                "sources": ["audio_spike", "visual_flash_spike"],
                "source_families": ["audio_prepass", "visual_prepass"],
                "materialization_mode": "copy",
                "bridge_owned": True,
                "apply_status": "pending",
                "review_status": review_status or "unreviewed",
            }
        ],
        "manifest_path": str((root / "proxy_review_session.json").resolve()),
    }


class HighlightReviewAppTests(unittest.TestCase):
    def test_load_highlight_review_records_reads_fixture_and_sidecar_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(media), indent=2), encoding="utf-8")
            report = root / "fixture_comparison.json"
            batch_manifest = root / "fixture_trial_batch_manifest.json"
            report.write_text(json.dumps(_fixture_comparison_report(sidecar), indent=2), encoding="utf-8")
            batch_manifest.write_text(json.dumps(_fixture_trial_batch_manifest(report), indent=2), encoding="utf-8")

            records = load_highlight_review_records(
                sidecar_root=root,
                fixture_manifest_path="assets/evaluation/fixture_manifest.json",
                fixture_comparison_report=report,
                fixture_trial_batch_manifest=batch_manifest,
            )

            self.assertGreaterEqual(len(records), 6)
            self.assertTrue(any(row["kind"] == "fixture" for row in records))
            self.assertTrue(any(row["kind"] == "sidecar" and row.get("proxy_review_status") == "approved" for row in records))
            fixture_row = next(row for row in records if row["kind"] == "fixture" and row["record_id"] == "commentary-heavy-001")
            self.assertEqual(len(fixture_row["fixture_comparison_rows"]), 1)
            self.assertEqual(len(fixture_row["fixture_trial_batch_rows"]), 1)

    def test_launch_highlight_review_app_builds_without_mutating_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.proxy_scan.json"
            original_text = json.dumps(_proxy_sidecar(media), indent=2)
            sidecar.write_text(original_text, encoding="utf-8")
            report = root / "fixture_comparison.json"
            batch_manifest = root / "fixture_trial_batch_manifest.json"
            report.write_text(json.dumps(_fixture_comparison_report(sidecar), indent=2), encoding="utf-8")
            batch_manifest.write_text(json.dumps(_fixture_trial_batch_manifest(report), indent=2), encoding="utf-8")

            fake_gradio = type(
                "FakeGradio",
                (),
                {
                    "Blocks": _FakeBlocks,
                    "Row": _FakeLayout,
                    "Column": _FakeLayout,
                    "Markdown": _FakeComponent,
                    "Dropdown": _FakeComponent,
                    "Video": _FakeComponent,
                    "Textbox": _FakeComponent,
                    "Code": _FakeComponent,
                    "Button": _FakeComponent,
                },
            )()
            with patch("pipeline.highlight_review_app.importlib.import_module", return_value=fake_gradio):
                result = launch_highlight_review_app(
                    sidecar_root=root,
                    fixture_manifest_path="assets/evaluation/fixture_manifest.json",
                    fixture_comparison_report=report,
                    fixture_trial_batch_manifest=batch_manifest,
                    launch=False,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(sidecar.read_text(encoding="utf-8"), original_text)

    def test_load_highlight_review_records_includes_candidate_lifecycle_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            fused_sidecar = root / "alpha.fused_analysis.json"
            fused_sidecar.write_text(json.dumps(_fused_sidecar(media), indent=2), encoding="utf-8")
            registry_path = root / "registry.sqlite"
            export_highlight_selection(fused_sidecar=fused_sidecar, output_path=root / "exports" / "alpha.highlight_selection.json")
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(fused_sidecar, registry_path=registry_path, output_path=root / "exports" / "alpha.hook_candidates.json")
            refresh_clip_registry(root, registry_path=registry_path)

            records = load_highlight_review_records(
                sidecar_root=root,
                registry_path=registry_path,
            )

            sidecar_record = next(row for row in records if row["kind"] == "sidecar")
            self.assertEqual(sidecar_record["candidate_lifecycle_count"], 1)
            self.assertIn("selected_for_export", sidecar_record["candidate_lifecycle_states"])
            self.assertEqual(sidecar_record["selected_highlight_event_types"], ["ability_plus_medal_combo"])
            self.assertEqual(sidecar_record["selected_highlight_producer_families"], ["runtime"])
            self.assertEqual(sidecar_record["selected_highlight_fusion_ids"], ["fused-123abc"])
            self.assertEqual(sidecar_record["hook_candidate_count"], 1)
            self.assertTrue(sidecar_record["strongest_hook_archetype"])

    def test_load_highlight_review_records_includes_proxy_review_session_items(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source = root / "alpha.mp4"
            source.write_bytes(b"source")
            sidecar = root / "alpha.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(source), indent=2), encoding="utf-8")
            session_path = root / "proxy_review_session.json"
            session_path.write_text(
                json.dumps(_proxy_review_session(root, source, sidecar, review_status="approved"), indent=2),
                encoding="utf-8",
            )

            records = load_highlight_review_records(proxy_review_session_manifest=session_path)

            self.assertEqual(len(records), 1)
            row = records[0]
            self.assertEqual(row["kind"], "proxy_review_session_item")
            self.assertEqual(row["review_status"], "approved")
            self.assertEqual(row["bridge_sources"], ["audio_spike", "visual_flash_spike"])
            self.assertTrue(row["transcript_available"])
            self.assertTrue(str(row["transcript_srt_path"]).endswith(".srt"))
            self.assertTrue(str(row["transcript_whisper_json_path"]).endswith(".whisper.json"))

    def test_write_proxy_review_session_decision_updates_meta_status(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source = root / "alpha.mp4"
            source.write_bytes(b"source")
            sidecar = root / "alpha.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(source), indent=2), encoding="utf-8")
            session = _proxy_review_session(root, source, sidecar)
            meta_path = Path(str(session["items"][0]["gpt_meta_path"]))

            approved = _write_proxy_review_session_decision(meta_path, "approved")
            self.assertTrue(approved["ok"])
            approved_payload = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(approved_payload["review_status"], "approved")
            self.assertIn("reviewed_at", approved_payload)

            unreviewed = _write_proxy_review_session_decision(meta_path, "unreviewed")
            self.assertTrue(unreviewed["ok"])
            unreviewed_payload = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(unreviewed_payload["review_status"], "unreviewed")
            self.assertNotIn("reviewed_at", unreviewed_payload)

    def test_launch_highlight_review_app_allows_proxy_review_media_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source = root / "gpt" / "accepted" / "marvel_rivals" / "alpha.mp4"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(b"source")
            sidecar = root / "alpha.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(source), indent=2), encoding="utf-8")
            session_path = root / "proxy_review_session.json"
            session_path.write_text(
                json.dumps(_proxy_review_session(root, source, sidecar, review_status="approved"), indent=2),
                encoding="utf-8",
            )

            fake_blocks = _FakeBlocks()
            fake_gradio = type(
                "FakeGradio",
                (),
                {
                    "Blocks": lambda *args, **kwargs: fake_blocks,
                    "Row": _FakeLayout,
                    "Column": _FakeLayout,
                    "Markdown": _FakeComponent,
                    "Dropdown": _FakeComponent,
                    "Video": _FakeComponent,
                    "Textbox": _FakeComponent,
                    "Code": _FakeComponent,
                    "Button": _FakeComponent,
                },
            )()
            with patch("pipeline.highlight_review_app.importlib.import_module", return_value=fake_gradio):
                result = launch_highlight_review_app(
                    proxy_review_session_manifest=session_path,
                    launch=True,
                )

            self.assertTrue(result["ok"])
            self.assertIsNotNone(fake_blocks.launch_kwargs)
            allowed_paths = fake_blocks.launch_kwargs.get("allowed_paths")
            self.assertIsInstance(allowed_paths, list)
            self.assertIn(str((root / "gpt" / "processing" / "marvel_rivals").resolve()), allowed_paths)
            self.assertIn(str((root / "gpt" / "accepted" / "marvel_rivals").resolve()), allowed_paths)
            self.assertIn(str((root / "gpt" / "inbox" / "marvel_rivals").resolve()), allowed_paths)


if __name__ == "__main__":
    unittest.main()
