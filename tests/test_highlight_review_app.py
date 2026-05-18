from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import refresh_clip_registry
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from pipeline import highlight_review_app as highlight_review_app_module
from pipeline.highlight_review_app import (
    _calibration_followup_counts,
    _active_review_records,
    _finalize_fused_review_session_decision,
    _finalize_proxy_review_session_decision,
    _followup_toggle_label,
    _render_calibration_handoff_summary,
    _render_calibration_followup_summary,
    _render_calibration_followup_status,
    _resolve_active_record_id,
    _sort_calibration_followup_rows,
    _top_calibration_followup_payload,
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


def _proxy_review_session_with_multiple_items(root: Path, sidecar: Path) -> dict[str, object]:
    processing_root = root / "gpt" / "processing" / "marvel_rivals"
    inbox_root = root / "gpt" / "inbox" / "marvel_rivals"
    accepted_root = root / "gpt" / "accepted" / "marvel_rivals"
    processing_root.mkdir(parents=True, exist_ok=True)
    inbox_root.mkdir(parents=True, exist_ok=True)
    accepted_root.mkdir(parents=True, exist_ok=True)
    items: list[dict[str, object]] = []
    for index, stem in enumerate(("alpha", "bravo")):
        source_path = accepted_root / f"{stem}.mp4"
        source_path.write_bytes(b"source")
        processed_path = processing_root / f"proxy-review-{index:03d}.mp4"
        processed_path.write_bytes(b"video")
        meta_path = inbox_root / f"proxy-review-{index:03d}.meta.json"
        meta_path.write_text(
            json.dumps(
                {
                    "clip_id": f"proxy-review-{index:03d}",
                    "game": "marvel_rivals",
                    "clip_path": str(processed_path),
                    "processed_path": str(processed_path),
                    "meta_path": str(meta_path),
                    "status": "queue",
                    "selected_template_id": "proxy_review_bridge",
                    "review_status": "unreviewed",
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        items.append(
            {
                "clip_id": f"proxy-review-{index:03d}",
                "sidecar_path": str(sidecar.resolve()),
                "source": str(source_path.resolve()),
                "gpt_processed_path": str(processed_path.resolve()),
                "gpt_meta_path": str(meta_path.resolve()),
                "top_proxy_score": 0.81,
                "top_recommended_action": "download_candidate",
                "sources": ["audio_spike"],
                "source_families": ["audio_prepass"],
                "materialization_mode": "copy",
                "bridge_owned": True,
                "apply_status": "pending",
                "review_status": "unreviewed",
            }
        )
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
        "item_count": len(items),
        "items": items,
        "manifest_path": str((root / "proxy_review_session.json").resolve()),
    }


def _fused_review_session(root: Path, source: Path, sidecar: Path, *, review_status: str | None = None) -> dict[str, object]:
    processing_root = root / "gpt" / "processing" / "marvel_rivals"
    inbox_root = root / "gpt" / "inbox" / "marvel_rivals"
    processing_root.mkdir(parents=True, exist_ok=True)
    inbox_root.mkdir(parents=True, exist_ok=True)
    processed_path = processing_root / "fused-review-001.mp4"
    processed_path.write_bytes(b"video")
    meta_path = inbox_root / "fused-review-001.meta.json"
    meta_payload = {
        "clip_id": "fused-review-001",
        "game": "marvel_rivals",
        "clip_path": str(processed_path),
        "processed_path": str(processed_path),
        "meta_path": str(meta_path),
        "status": "queue",
        "selected_template_id": "fused_review_bridge",
    }
    if review_status is not None:
        meta_payload["review_status"] = review_status
    meta_path.write_text(json.dumps(meta_payload, indent=2), encoding="utf-8")
    return {
        "schema_version": "fused_review_session_v1",
        "session_id": "fused-session-123",
        "game": "marvel_rivals",
        "gpt_repo": str((root / "gpt").resolve()),
        "selection_source": str(root / "fused"),
        "selection_action_filter": "review_default",
        "selection_event_type_filter": None,
        "limit": None,
        "created_at": "2026-05-09T00:00:00+00:00",
        "materialization_mode": "trim",
        "item_count": 1,
        "items": [
            {
                "clip_id": "fused-review-001",
                "sidecar_path": str(sidecar.resolve()),
                "source": str(source.resolve()),
                "event_id": "fused-1",
                "event_type": "ability_plus_medal_combo",
                "gpt_processed_path": str(processed_path.resolve()),
                "gpt_meta_path": str(meta_path.resolve()),
                "final_score": 0.91,
                "recommended_action": "highlight_candidate",
                "gate_status": "confirmed",
                "synergy_applied": False,
                "suggested_start_timestamp": 0.5,
                "suggested_end_timestamp": 3.0,
                "materialization_mode": "trim",
                "bridge_owned": True,
                "apply_status": "pending",
                "review_status": review_status or "unreviewed",
            }
        ],
        "manifest_path": str((root / "fused_review_session.json").resolve()),
    }


class HighlightReviewAppTests(unittest.TestCase):
    def test_sort_calibration_followup_rows_prefers_higher_absolute_delta(self) -> None:
        rows = [
            {"candidate_id": "low", "absolute_delta_iou": 0.05},
            {"candidate_id": "high", "absolute_delta_iou": 0.25},
        ]
        sorted_rows = _sort_calibration_followup_rows(rows)
        self.assertEqual([row["candidate_id"] for row in sorted_rows], ["high", "low"])

    def test_sort_calibration_followup_rows_breaks_ties_by_newer_replay(self) -> None:
        rows = [
            {"candidate_id": "older", "absolute_delta_iou": 0.25, "replay_created_at": "2026-05-14T01:00:00+00:00"},
            {"candidate_id": "newer", "absolute_delta_iou": 0.25, "replay_created_at": "2026-05-14T02:00:00+00:00"},
        ]
        sorted_rows = _sort_calibration_followup_rows(rows)
        self.assertEqual([row["candidate_id"] for row in sorted_rows], ["newer", "older"])

    def test_top_calibration_followup_payload_uses_strongest_sorted_row(self) -> None:
        payload = _top_calibration_followup_payload(
            _sort_calibration_followup_rows(
                [
                    {
                        "run_id": "replay-001",
                        "runtime_sidecar_path": "/tmp/weaker.runtime_analysis.json",
                        "review_record_path": "/tmp/weaker.json",
                        "difference_summary": "weaker",
                        "primary_source": "crop_candidate",
                        "secondary_source": "roi_fallback",
                        "primary_reference_crop": "10,10,80,30",
                        "secondary_reference_crop": "0,0,100,40",
                        "primary_reference_source": "localized_match",
                        "secondary_reference_source": "roi_fallback",
                        "primary_iou": 0.9,
                        "secondary_iou": 0.8,
                        "delta_iou": 0.1,
                        "absolute_delta_iou": 0.1,
                        "replay_created_at": "2026-05-14T01:00:00+00:00",
                    },
                    {
                        "run_id": "replay-003",
                        "runtime_sidecar_path": "/tmp/stronger.runtime_analysis.json",
                        "review_record_path": "/tmp/stronger.json",
                        "difference_summary": "stronger",
                        "primary_source": "replay_run",
                        "secondary_source": "crop_candidate",
                        "primary_reference_crop": "557,191,829,175",
                        "secondary_reference_crop": "499,172,921,194",
                        "primary_reference_source": "localized_match",
                        "secondary_reference_source": "roi_fallback",
                        "primary_iou": 1.0,
                        "secondary_iou": 0.811954,
                        "delta_iou": 0.188046,
                        "absolute_delta_iou": 0.188046,
                        "replay_created_at": "2026-05-14T02:00:00+00:00",
                    },
                ]
            )
        )
        self.assertEqual(
            payload,
            {
                "review_record_path": "/tmp/stronger.json",
                "run_id": "replay-003",
                "runtime_sidecar_path": "/tmp/stronger.runtime_analysis.json",
                "difference_summary": "stronger",
                "delta_iou": 0.188046,
                "absolute_delta_iou": 0.188046,
                "primary_iou": 1.0,
                "secondary_iou": 0.811954,
                "primary_source": "replay_run",
                "secondary_source": "crop_candidate",
                "primary_reference_crop": "557,191,829,175",
                "secondary_reference_crop": "499,172,921,194",
                "primary_reference_source": "localized_match",
                "secondary_reference_source": "roi_fallback",
            },
        )

    def test_top_calibration_followup_payload_absent_for_empty_rows(self) -> None:
        self.assertIsNone(_top_calibration_followup_payload([]))

    def test_calibration_followup_counts_shared_helper(self) -> None:
        matched_count, total_count = _calibration_followup_counts(
            [
                {"record_id": "a", "kind": "sidecar", "runtime_sidecar_path": "/tmp/a.runtime_analysis.json"},
                {"record_id": "b", "kind": "sidecar", "runtime_sidecar_path": "/tmp/b.runtime_analysis.json"},
            ],
            manifest_loaded=True,
            followup_rows=[{"runtime_sidecar_path": "/tmp/a.runtime_analysis.json"}],
        )
        self.assertEqual((matched_count, total_count), (1, 2))

    def test_followup_toggle_label_uses_shared_counts(self) -> None:
        label = _followup_toggle_label(
            [
                {"record_id": "a", "kind": "sidecar", "runtime_sidecar_path": "/tmp/a.runtime_analysis.json"},
                {"record_id": "b", "kind": "sidecar", "runtime_sidecar_path": "/tmp/b.runtime_analysis.json"},
            ],
            manifest_loaded=True,
            followup_rows=[{"runtime_sidecar_path": "/tmp/a.runtime_analysis.json"}],
        )
        self.assertEqual(label, "Show only calibration follow-up records (1/2)")

    def test_active_review_records_followup_filter_preserves_order(self) -> None:
        records = [
            {"record_id": "a", "kind": "sidecar", "runtime_sidecar_path": "/tmp/a.runtime_analysis.json"},
            {"record_id": "b", "kind": "sidecar", "runtime_sidecar_path": "/tmp/b.runtime_analysis.json"},
            {"record_id": "c", "kind": "fixture"},
        ]
        filtered = _active_review_records(
            records,
            manifest_loaded=True,
            followup_rows=[{"runtime_sidecar_path": "/tmp/b.runtime_analysis.json"}],
            show_only_followup_records=True,
        )
        self.assertEqual([row["record_id"] for row in filtered], ["b"])

    def test_resolve_active_record_id_falls_to_first_filtered_record(self) -> None:
        resolved = _resolve_active_record_id("missing", ["record-2", "record-3"])
        self.assertEqual(resolved, "record-2")
        self.assertIsNone(_resolve_active_record_id("missing", []))

    def test_render_calibration_handoff_summary_uses_viewer_payload(self) -> None:
        viewer_payload = {
            "selected_item_id": "runtime-event-0",
            "detector_calibration_handoffs": {
                "by_item_id": {
                    "runtime-event-0": {
                        "available": True,
                        "asset_id": "marvel_rivals.punisher.hero_portrait",
                        "roi_ref": "hero_portrait",
                        "timestamp_seconds": 1.0,
                        "suggested_judgment": "false_positive",
                        "suggested_suspected_cause": "template_crop_quality",
                        "suggested_crop": "0,0,100,40",
                        "suggested_crop_source": "roi_fallback",
                        "suggested_crop_placeholder": "x,y,w,h",
                        "template_comparison": {
                            "status": "ok",
                            "published_dimensions": {"width": 100, "height": 40},
                            "revised_dimensions": {"width": 96, "height": 38},
                            "delta": {"width": -4, "height": -2},
                            "dimensions_match": False,
                            "spatial_overlap": {
                                "reference_crop": "4,6,22,18",
                                "reference_source": "localized_match",
                                "intersection": {"x": 5, "y": 6, "w": 21, "h": 18},
                                "intersection_area": 378,
                                "revised_area": 2700,
                                "reference_area": 396,
                                "iou": 0.13913,
                                "revised_coverage_ratio": 0.14,
                                "reference_coverage_ratio": 0.954545,
                            },
                        },
                        "template_comparison_source": "replay_run",
                        "template_comparison_difference_summary": "Replay changed overlap source from roi_fallback to localized_match and changed IoU from 0.675 to 0.13913.",
                        "template_comparison_secondary": {
                            "status": "ok",
                            "published_dimensions": {"width": 100, "height": 40},
                            "revised_dimensions": {"width": 96, "height": 38},
                            "delta": {"width": -4, "height": -2},
                            "dimensions_match": False,
                            "spatial_overlap": {
                                "reference_crop": "0,0,100,40",
                                "reference_source": "roi_fallback",
                                "intersection": {"x": 5, "y": 5, "w": 90, "h": 30},
                                "intersection_area": 2700,
                                "revised_area": 2700,
                                "reference_area": 4000,
                                "iou": 0.675,
                                "revised_coverage_ratio": 1.0,
                                "reference_coverage_ratio": 0.675,
                            },
                        },
                        "template_comparison_secondary_source": "crop_candidate",
                        "init_command": "python tools/detector_calibration_session.py init --asset-id marvel_rivals.punisher.hero_portrait",
                        "create_crop_command_template": "python tools/detector_calibration_session.py create-crop --session-root <SESSION_ROOT> --crop 0,0,100,40",
                        "replay_command_template": "python tools/detector_calibration_session.py replay --session-root <SESSION_ROOT>",
                        "command": "python tools/detector_calibration_session.py init --asset-id marvel_rivals.punisher.hero_portrait",
                    }
                }
            },
        }

        summary = _render_calibration_handoff_summary(viewer_payload)

        self.assertIn("Asset id: marvel_rivals.punisher.hero_portrait", summary)
        self.assertIn("ROI: hero_portrait", summary)
        self.assertIn("Suggested judgment: false_positive", summary)
        self.assertIn("Suggested crop: 0,0,100,40", summary)
        self.assertIn("Suggested crop source: roi_fallback", summary)
        self.assertIn("Crop comparison source: replay_run", summary)
        self.assertIn("Crop comparison: published 100x40 -> revised 96x38", summary)
        self.assertIn("Crop comparison delta: -4x / -2y", summary)
        self.assertIn("Crop comparison dimensions match: False", summary)
        self.assertIn("Difference summary: Replay changed overlap source from roi_fallback to localized_match and changed IoU from 0.675 to 0.13913.", summary)
        self.assertIn("Spatial overlap reference: localized_match 4,6,22,18", summary)
        self.assertIn("Spatial overlap intersection: 21x18 @ 5,6", summary)
        self.assertIn("Spatial overlap IoU: 0.13913", summary)
        self.assertIn("Spatial overlap revised coverage: 0.14", summary)
        self.assertIn("Spatial overlap reference coverage: 0.954545", summary)
        self.assertIn("Candidate-time baseline source: crop_candidate", summary)
        self.assertIn("Candidate-time baseline delta: -4x / -2y", summary)
        self.assertIn("Candidate-time baseline overlap reference: roi_fallback 0,0,100,40", summary)
        self.assertIn("Candidate-time baseline IoU: 0.675", summary)
        self.assertIn("Workflow note: Run Init first. The session tool will return concrete Create Crop and Replay commands with the real session root.", summary)
        self.assertIn("Init:", summary)
        self.assertIn("Create Crop:", summary)
        self.assertIn("Replay:", summary)
        self.assertIn("tools/detector_calibration_session.py init", summary)

    def test_render_calibration_handoff_summary_explicit_empty_state(self) -> None:
        summary = _render_calibration_handoff_summary(
            {
                "selected_item_id": "window-0",
                "detector_calibration_handoffs": {"by_item_id": {"window-0": {"available": False}}},
            }
        )
        self.assertEqual(summary, "No detector calibration handoff available for this record.")

    def test_render_calibration_followup_summary_uses_matching_runtime_sidecar(self) -> None:
        row = {
            "kind": "sidecar",
            "runtime_sidecar_path": "/tmp/runtime_analysis.json",
        }
        summary = _render_calibration_followup_summary(
            row,
            manifest_loaded=True,
            followup_rows=[
                {
                    "runtime_sidecar_path": "/tmp/runtime_analysis.json",
                    "run_id": "replay-001",
                    "candidate_id": "weaker",
                    "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                    "difference_summary": "weaker",
                    "primary_source": "replay_run",
                    "secondary_source": "crop_candidate",
                    "primary_iou": 0.9,
                    "secondary_iou": 0.8,
                    "delta_iou": 0.1,
                    "absolute_delta_iou": 0.1,
                    "review_record_path": "/tmp/weaker.json",
                    "replay_created_at": "2026-05-14T01:00:00+00:00",
                },
                {
                    "runtime_sidecar_path": "/tmp/runtime_analysis.json",
                    "run_id": "replay-003",
                    "candidate_id": "rev-002",
                    "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                    "difference_summary": "Replay changed overlap source from roi_fallback to localized_match and changed IoU from 0.811954 to 1.0.",
                    "primary_source": "replay_run",
                    "secondary_source": "crop_candidate",
                    "primary_iou": 1.0,
                    "secondary_iou": 0.811954,
                    "delta_iou": 0.188046,
                    "absolute_delta_iou": 0.188046,
                    "review_record_path": "/tmp/review_record.json",
                    "replay_created_at": "2026-05-14T02:00:00+00:00",
                }
            ],
        )
        self.assertIn("Top follow-up", summary)
        self.assertIn("Top follow-up review record path: /tmp/review_record.json", summary)
        self.assertIn("Top follow-up replay run id: replay-003", summary)
        self.assertIn("Top follow-up runtime sidecar path: /tmp/runtime_analysis.json", summary)
        self.assertIn("Additional follow-ups", summary)
        self.assertLess(summary.find("Candidate id: rev-002"), summary.find("Candidate id: weaker"))
        self.assertIn("Replay run id: replay-003", summary)
        self.assertIn("Candidate id: rev-002", summary)
        self.assertIn("Asset id: marvel_rivals.ace.team_wipe_announcement", summary)
        self.assertIn("Primary IoU: 1.0", summary)
        self.assertIn("Delta IoU: 0.188046", summary)

    def test_render_calibration_followup_summary_single_row_uses_top_followup_heading_only(self) -> None:
        row = {
            "kind": "sidecar",
            "runtime_sidecar_path": "/tmp/runtime_analysis.json",
        }
        summary = _render_calibration_followup_summary(
            row,
            manifest_loaded=True,
            followup_rows=[
                {
                    "runtime_sidecar_path": "/tmp/runtime_analysis.json",
                    "run_id": "replay-003",
                    "candidate_id": "rev-002",
                    "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                    "difference_summary": "Replay changed overlap source from roi_fallback to localized_match and changed IoU from 0.811954 to 1.0.",
                    "primary_source": "replay_run",
                    "secondary_source": "crop_candidate",
                    "primary_iou": 1.0,
                    "secondary_iou": 0.811954,
                    "delta_iou": 0.188046,
                    "absolute_delta_iou": 0.188046,
                    "review_record_path": "/tmp/review_record.json",
                    "replay_created_at": "2026-05-14T02:00:00+00:00",
                }
            ],
        )
        self.assertIn("Top follow-up", summary)
        self.assertIn("Top follow-up review record path: /tmp/review_record.json", summary)
        self.assertIn("Top follow-up replay run id: replay-003", summary)
        self.assertIn("Top follow-up runtime sidecar path: /tmp/runtime_analysis.json", summary)
        self.assertNotIn("Additional follow-ups", summary)

    def test_render_calibration_followup_summary_no_manifest_state(self) -> None:
        summary = _render_calibration_followup_summary(
            {"kind": "sidecar", "runtime_sidecar_path": "/tmp/runtime_analysis.json"},
            manifest_loaded=False,
            followup_rows=[],
        )
        self.assertEqual(summary, "No calibration follow-up manifest loaded.")

    def test_render_calibration_followup_status_no_manifest(self) -> None:
        status = _render_calibration_followup_status(
            [{"record_id": "a", "kind": "sidecar", "runtime_sidecar_path": "/tmp/a.runtime_analysis.json"}],
            manifest_loaded=False,
            followup_rows=[],
            show_only_followup_records=False,
        )
        self.assertEqual(status, "Calibration follow-up manifest not loaded.")

    def test_render_calibration_followup_status_manifest_loaded_toggle_off(self) -> None:
        status = _render_calibration_followup_status(
            [
                {"record_id": "a", "kind": "sidecar", "runtime_sidecar_path": "/tmp/a.runtime_analysis.json"},
                {"record_id": "b", "kind": "sidecar", "runtime_sidecar_path": "/tmp/b.runtime_analysis.json"},
            ],
            manifest_loaded=True,
            followup_rows=[{"runtime_sidecar_path": "/tmp/a.runtime_analysis.json"}],
            show_only_followup_records=False,
        )
        self.assertEqual(status, "1 of 2 records have calibration follow-up.")

    def test_render_calibration_followup_status_manifest_loaded_toggle_on(self) -> None:
        status = _render_calibration_followup_status(
            [
                {"record_id": "a", "kind": "sidecar", "runtime_sidecar_path": "/tmp/a.runtime_analysis.json"},
                {"record_id": "b", "kind": "sidecar", "runtime_sidecar_path": "/tmp/b.runtime_analysis.json"},
            ],
            manifest_loaded=True,
            followup_rows=[{"runtime_sidecar_path": "/tmp/a.runtime_analysis.json"}],
            show_only_followup_records=True,
        )
        self.assertEqual(status, "Showing 1 of 2 records with calibration follow-up.")

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
                    "Accordion": _FakeLayout,
                    "Markdown": _FakeComponent,
                    "Dropdown": _FakeComponent,
                    "Checkbox": _FakeComponent,
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

    def test_launch_highlight_review_app_sidecar_path_reuses_viewer_handoff_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.fused_analysis.json"
            sidecar.write_text(json.dumps(_fused_sidecar(media), indent=2), encoding="utf-8")
            followup_manifest = root / "followup.json"
            followup_manifest.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_followup_manifest_v1",
                        "rows": [
                            {
                                "runtime_sidecar_path": str(sidecar.resolve()),
                                "run_id": "replay-003",
                                "candidate_id": "rev-002",
                                "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                                "difference_summary": "Replay changed overlap source from roi_fallback to localized_match and changed IoU from 0.811954 to 1.0.",
                                "primary_source": "replay_run",
                                "secondary_source": "crop_candidate",
                                "primary_iou": 1.0,
                                "secondary_iou": 0.811954,
                                "delta_iou": 0.188046,
                                "review_record_path": str(root / "review_record.json"),
                            }
                        ],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            fake_gradio = type(
                "FakeGradio",
                (),
                {
                    "Blocks": _FakeBlocks,
                    "Row": _FakeLayout,
                    "Column": _FakeLayout,
                    "Accordion": _FakeLayout,
                    "Markdown": _FakeComponent,
                    "Dropdown": _FakeComponent,
                    "Checkbox": _FakeComponent,
                    "Video": _FakeComponent,
                    "Textbox": _FakeComponent,
                    "Code": _FakeComponent,
                    "Button": _FakeComponent,
                },
            )()
            viewer_result = {
                "ok": True,
                "viewer_path": str(root / "viewer.html"),
                "viewer_payload": {
                    "selected_item_id": "fused-event-0",
                    "detector_calibration_handoffs": {
                        "by_item_id": {
                            "fused-event-0": {
                                "available": True,
                                "asset_id": "marvel_rivals.punisher.hero_portrait",
                                "roi_ref": "hero_portrait",
                                "timestamp_seconds": 1.0,
                                "suggested_judgment": "false_positive",
                                "suggested_suspected_cause": "template_crop_quality",
                                "suggested_crop_placeholder": "x,y,w,h",
                                "init_command": "python tools/detector_calibration_session.py init --asset-id marvel_rivals.punisher.hero_portrait",
                                "create_crop_command_template": "python tools/detector_calibration_session.py create-crop --session-root <SESSION_ROOT> --crop x,y,w,h",
                                "replay_command_template": "python tools/detector_calibration_session.py replay --session-root <SESSION_ROOT>",
                                "command": "python tools/detector_calibration_session.py init --asset-id marvel_rivals.punisher.hero_portrait",
                            }
                        }
                    },
                },
            }
            with patch.object(highlight_review_app_module.importlib, "import_module", return_value=fake_gradio):
                with patch.object(
                    highlight_review_app_module,
                    "render_unified_replay_viewer",
                    return_value=viewer_result,
                ):
                    result = launch_highlight_review_app(
                        sidecar_root=root,
                        detector_calibration_followup_manifest=followup_manifest,
                        launch=False,
                    )

            self.assertTrue(result["ok"])
            self.assertTrue(viewer_result["viewer_payload"]["detector_calibration_handoffs"]["by_item_id"]["fused-event-0"]["available"])

    def test_launch_highlight_review_app_fails_on_invalid_followup_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.fused_analysis.json"
            sidecar.write_text(json.dumps(_fused_sidecar(media), indent=2), encoding="utf-8")
            followup_manifest = root / "followup.json"
            followup_manifest.write_text("{not-json", encoding="utf-8")

            result = launch_highlight_review_app(
                sidecar_root=root,
                detector_calibration_followup_manifest=followup_manifest,
                launch=False,
            )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_detector_calibration_followup_manifest")

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
                json.dumps(_proxy_review_session(root, source, sidecar), indent=2),
                encoding="utf-8",
            )

            records = load_highlight_review_records(proxy_review_session_manifest=session_path)

            self.assertEqual(len(records), 1)
            row = records[0]
            self.assertEqual(row["kind"], "proxy_review_session_item")
            self.assertEqual(row["review_status"], "unreviewed")
            self.assertEqual(row["bridge_sources"], ["audio_spike", "visual_flash_spike"])
            self.assertTrue(row["transcript_available"])
            self.assertTrue(str(row["transcript_srt_path"]).endswith(".srt"))
            self.assertTrue(str(row["transcript_whisper_json_path"]).endswith(".whisper.json"))

    def test_load_highlight_review_records_skips_reviewed_proxy_session_items(self) -> None:
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

            self.assertEqual(records, [])

    def test_load_highlight_review_records_includes_fused_review_session_items(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source = root / "alpha.mp4"
            source.write_bytes(b"source")
            sidecar = root / "alpha.fused_analysis.json"
            sidecar.write_text(json.dumps(_fused_sidecar(source), indent=2), encoding="utf-8")
            session_path = root / "fused_review_session.json"
            session_path.write_text(
                json.dumps(_fused_review_session(root, source, sidecar), indent=2),
                encoding="utf-8",
            )

            records = load_highlight_review_records(fused_review_session_manifest=session_path)

            self.assertEqual(len(records), 1)
            row = records[0]
            self.assertEqual(row["kind"], "fused_review_session_item")
            self.assertEqual(row["review_status"], "unreviewed")
            self.assertEqual(row["event_id"], "fused-1")
            self.assertEqual(row["event_type"], "ability_plus_medal_combo")
            self.assertEqual(row["final_score"], 0.91)
            self.assertEqual(row["label"], "alpha | unknown_entity | 0.5s-3.0s")

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
                json.dumps(_proxy_review_session(root, source, sidecar), indent=2),
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
                    "Accordion": _FakeLayout,
                    "Markdown": _FakeComponent,
                    "Dropdown": _FakeComponent,
                    "Checkbox": _FakeComponent,
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

    def test_finalize_proxy_review_session_decision_moves_reviewed_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source = root / "alpha.mp4"
            source.write_bytes(b"source")
            sidecar = root / "alpha.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(source), indent=2), encoding="utf-8")
            session = _proxy_review_session(root, source, sidecar)
            session_path = root / "proxy_review_session.json"
            session_path.write_text(json.dumps(session, indent=2), encoding="utf-8")

            records = load_highlight_review_records(proxy_review_session_manifest=session_path)
            self.assertEqual(len(records), 1)
            row = records[0]

            result = _finalize_proxy_review_session_decision(row, "approved")

            self.assertTrue(result["ok"])
            final_clip_path = Path(str(result["gpt_final_path"]))
            final_meta_path = Path(str(result["gpt_meta_path"]))
            self.assertTrue(final_clip_path.exists())
            self.assertTrue(final_meta_path.exists())
            self.assertFalse(Path(row["gpt_processed_path"]).exists())
            self.assertFalse((root / "gpt" / "inbox" / "marvel_rivals" / "proxy-review-001.meta.json").exists())

            final_meta_payload = json.loads(final_meta_path.read_text(encoding="utf-8"))
            self.assertEqual(final_meta_payload["status"], "accepted")
            self.assertEqual(final_meta_payload["final_path"], str(final_clip_path))

            updated_session = json.loads(session_path.read_text(encoding="utf-8"))
            updated_item = updated_session["items"][0]
            self.assertEqual(updated_item["review_status"], "approved")
            self.assertEqual(updated_item["apply_status"], "reviewed")
            self.assertEqual(updated_item["gpt_meta_path"], str(final_meta_path))
            self.assertEqual(updated_item["gpt_final_path"], str(final_clip_path))

    def test_finalize_fused_review_session_decision_moves_reviewed_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            source = root / "alpha.mp4"
            source.write_bytes(b"source")
            sidecar = root / "alpha.fused_analysis.json"
            sidecar.write_text(json.dumps(_fused_sidecar(source), indent=2), encoding="utf-8")
            session = _fused_review_session(root, source, sidecar)
            session_path = root / "fused_review_session.json"
            session_path.write_text(json.dumps(session, indent=2), encoding="utf-8")

            records = load_highlight_review_records(fused_review_session_manifest=session_path)
            self.assertEqual(len(records), 1)
            row = records[0]

            result = _finalize_fused_review_session_decision(row, "approved")

            self.assertTrue(result["ok"])
            final_clip_path = Path(str(result["gpt_final_path"]))
            final_meta_path = Path(str(result["gpt_meta_path"]))
            self.assertTrue(final_clip_path.exists())
            self.assertTrue(final_meta_path.exists())
            self.assertFalse(Path(row["gpt_processed_path"]).exists())
            self.assertFalse((root / "gpt" / "inbox" / "marvel_rivals" / "fused-review-001.meta.json").exists())

            final_meta_payload = json.loads(final_meta_path.read_text(encoding="utf-8"))
            self.assertEqual(final_meta_payload["status"], "accepted")
            self.assertEqual(final_meta_payload["final_path"], str(final_clip_path))

            updated_session = json.loads(session_path.read_text(encoding="utf-8"))
            updated_item = updated_session["items"][0]
            self.assertEqual(updated_item["review_status"], "approved")
            self.assertEqual(updated_item["apply_status"], "reviewed")
            self.assertEqual(updated_item["gpt_meta_path"], str(final_meta_path))
            self.assertEqual(updated_item["gpt_final_path"], str(final_clip_path))

    def test_review_decision_advances_to_next_record_and_stops_at_last(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar = root / "alpha.proxy_scan.json"
            accepted_source = root / "seed.mp4"
            accepted_source.write_bytes(b"source")
            sidecar.write_text(json.dumps(_proxy_sidecar(accepted_source), indent=2), encoding="utf-8")
            session_path = root / "proxy_review_session.json"
            session_path.write_text(
                json.dumps(_proxy_review_session_with_multiple_items(root, sidecar), indent=2),
                encoding="utf-8",
            )

            fake_components: list[_FakeComponent] = []

            class _TrackingComponent(_FakeComponent):
                def __init__(self, *args, **kwargs) -> None:
                    super().__init__(*args, **kwargs)
                    self.click_fn = None
                    fake_components.append(self)

                def click(self, fn=None, *args, **kwargs) -> None:
                    self.click_fn = fn
                    return None

            fake_gradio = type(
                "FakeGradio",
                (),
                {
                    "Blocks": _FakeBlocks,
                    "Row": _FakeLayout,
                    "Column": _FakeLayout,
                    "Accordion": _FakeLayout,
                    "Markdown": _TrackingComponent,
                    "Dropdown": _TrackingComponent,
                    "Checkbox": _TrackingComponent,
                    "Video": _TrackingComponent,
                    "Textbox": _TrackingComponent,
                    "Code": _TrackingComponent,
                    "Button": _TrackingComponent,
                },
            )()
            with patch("pipeline.highlight_review_app.importlib.import_module", return_value=fake_gradio):
                result = launch_highlight_review_app(
                    proxy_review_session_manifest=session_path,
                    launch=False,
                )

            self.assertTrue(result["ok"])
            buttons = [component for component in fake_components if component.args and component.args[0] in {"Approve", "Reject", "Leave unreviewed"}]
            approve_button = next(component for component in buttons if component.args[0] == "Approve")
            self.assertIsNotNone(approve_button.click_fn)

            first_record = "proxy-session::proxy-session-123::000"
            second_record = "proxy-session::proxy-session-123::001"
            first_result = approve_button.click_fn(first_record, False)
            self.assertEqual(first_result[0]["value"], second_record)
            self.assertEqual(first_result[1]["label"], "Show only calibration follow-up records (0/2)")
            self.assertIn("Moved to next item", first_result[-1])

            second_result = approve_button.click_fn(second_record, False)
            self.assertEqual(second_result[0]["value"], second_record)
            self.assertEqual(second_result[1]["label"], "Show only calibration follow-up records (0/2)")
            self.assertIn("Reached last item", second_result[-1])


if __name__ == "__main__":
    unittest.main()
