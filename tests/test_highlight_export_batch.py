from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.highlight_export_batch import create_highlight_export_batch, materialize_synthetic_post_coverage, record_post_ledger
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from pipeline.workflow_run_state import create_workflow_run, query_workflow_queue
from run import main as run_main
from run import run_calibrate_runtime_review


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fused_sidecar(path: Path, *, game: str, source: Path, review_status: str = "approved") -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_analysis_v1",
            "fusion_id": f"fusion-{path.stem}",
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "normalized_signals": [
                {
                    "signal_id": "signal-1",
                    "signal_type": "character_identity",
                    "producer_family": "runtime",
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
                    "minimum_required_signals_met": True,
                    "suggested_start_timestamp": 0.5,
                    "suggested_end_timestamp": 3.2,
                    "contributing_signals": ["signal-1"],
                    "metadata": {"entity_id": "punisher", "ability_id": "ult"},
                }
            ],
            "fused_review": {
                "session_id": "fused-session-1",
                "reviewed_event_count": 1,
                "events": {"fused-event-1": {"review_status": review_status}},
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _hook_reject_fused_sidecar(path: Path, *, game: str, source: Path, review_status: str = "approved") -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_analysis_v1",
            "fusion_id": f"fusion-{path.stem}",
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "normalized_signals": [
                {
                    "signal_id": "signal-1",
                    "signal_type": "equipment_visibility",
                    "producer_family": "runtime",
                }
            ],
            "fused_events": [
                {
                    "event_id": "fused-event-1",
                    "event_type": "ability_seen",
                    "confidence": 0.97369,
                    "final_score": 0.97369,
                    "gate_status": "not_applicable",
                    "synergy_applied": False,
                    "minimum_required_signals_met": True,
                    "suggested_start_timestamp": 2.0,
                    "suggested_end_timestamp": 2.0,
                    "contributing_signals": ["signal-1"],
                    "metadata": {
                        "equipment_id": "redeploy_extraction_token",
                        "matched_signal_types": ["equipment_visibility"],
                    },
                }
            ],
            "fused_review": {
                "session_id": "fused-session-1",
                "reviewed_event_count": 1,
                "events": {"fused-event-1": {"review_status": review_status}},
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _runtime_sidecar(path: Path, *, game: str, source: Path, review_status: str = "approved") -> None:
    _runtime_sidecar_with_rows(path, game=game, source=source, review_status=review_status)


def _runtime_event(event_type: str, *, confidence: float = 0.95) -> dict[str, object]:
    return {
        "event_id": f"{event_type}-event",
        "event_type": event_type,
        "timestamp": 1.0,
        "start_timestamp": 1.0,
        "end_timestamp": 1.5,
        "asset_id": f"{event_type}-asset",
        "roi_ref": "hero_portrait",
        "confidence": confidence,
        "evidence": {"peak_score": confidence},
        "source_detection_count": 3,
    }


def _runtime_detection(*, roi_ref: str = "hero_portrait", asset_family: str = "hero_portrait") -> dict[str, object]:
    return {
        "asset_id": f"{asset_family}-asset",
        "roi_ref": roi_ref,
        "asset_family": asset_family,
        "first_timestamp": 1.0,
        "last_timestamp": 1.5,
        "peak_score": 0.98,
        "supporting_frames": 4,
        "temporal_window": 3,
    }


def _runtime_sidecar_with_rows(
    path: Path,
    *,
    game: str,
    source: Path,
    review_status: str = "approved",
    events: list[dict[str, object]] | None = None,
    detections: list[dict[str, object]] | None = None,
) -> None:
    event_rows = events if events is not None else [_runtime_event("pov_character_identified")]
    detection_rows = detections if detections is not None else [_runtime_detection()]
    detection_rois: dict[str, int] = {}
    detection_asset_families: dict[str, int] = {}
    for row in detection_rows:
        roi_ref = str(row.get("roi_ref") or "").strip()
        asset_family = str(row.get("asset_family") or "").strip()
        if roi_ref:
            detection_rois[roi_ref] = detection_rois.get(roi_ref, 0) + 1
        if asset_family:
            detection_asset_families[asset_family] = detection_asset_families.get(asset_family, 0) + 1

    _write_json(
        path,
        {
            "schema_version": "runtime_analysis_v1",
            "analysis_id": f"runtime-{path.stem}",
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "matcher": {
                "frame_count": 24,
                "confirmed_detections": detection_rows,
                "summary": {
                    "total_confirmed_detections": len(detection_rows),
                    "detections_by_roi": detection_rois,
                    "detections_by_asset_family": detection_asset_families,
                },
            },
            "events": {
                "event_count": len(event_rows),
                "rows": event_rows,
            },
            "runtime_review": {
                "session_id": "runtime-session-1",
                "review_status": review_status,
                "recommended_action": "highlight_candidate",
                "highlight_score": 0.88,
            },
            "sidecar_path": str(path.resolve()),
        },
    )


class HighlightExportBatchTests(unittest.TestCase):
    def test_create_highlight_export_batch_writes_deterministic_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "selection" / "alpha.highlight_selection.json")
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "hooks" / "alpha.hook_candidates.json")
            refresh_clip_registry(root, registry_path=registry_path)
            workflow = create_workflow_run(
                "export_queue",
                registry_path=registry_path,
                output_path=root / "workflow" / "export.workflow_run.json",
            )

            first = create_highlight_export_batch(
                registry_path=registry_path,
                workflow_run_id=workflow["workflow_run_id"],
                output_path=root / "exports" / "batch.highlight_export_batch.json",
            )
            second = create_highlight_export_batch(
                registry_path=registry_path,
                workflow_run_id=workflow["workflow_run_id"],
                output_path=root / "exports" / "batch.highlight_export_batch.json",
            )

            self.assertTrue(first["ok"])
            self.assertEqual(first["export_batch_id"], second["export_batch_id"])
            manifest = json.loads(Path(first["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "highlight_export_batch_v1")
            self.assertEqual(manifest["workflow_run_id"], workflow["workflow_run_id"])
            self.assertEqual(manifest["export_count"], 1)
            row = manifest["exports"][0]
            self.assertEqual(row["export_status"], "exported")
            self.assertTrue(str(row["hook_mode"]))
            self.assertTrue(Path(row["otio_path"]).exists())
            self.assertEqual(len(workflow["export_ready_snapshot_paths"]), 1)
            snapshot_payload = json.loads(Path(workflow["export_ready_snapshot_paths"][0]).read_text(encoding="utf-8"))
            self.assertEqual(snapshot_payload["schema_version"], "export_ready_snapshot_v1")
            self.assertEqual(snapshot_payload["workflow_run_id"], workflow["workflow_run_id"])

    def test_create_highlight_export_batch_replays_from_export_ready_snapshot_after_export(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="call_of_duty", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(
                fused_sidecar=fused_path,
                output_path=root / "selection" / "alpha.highlight_selection.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(
                fused_path,
                registry_path=registry_path,
                output_path=root / "hooks" / "alpha.hook_candidates.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            workflow = create_workflow_run(
                "export_queue",
                registry_path=registry_path,
                output_path=root / "workflow" / "export.workflow_run.json",
            )

            first_export = create_highlight_export_batch(
                registry_path=registry_path,
                workflow_run_id=workflow["workflow_run_id"],
                output_path=root / "exports" / "first.highlight_export_batch.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            export_queue = query_workflow_queue("export_queue", registry_path=registry_path)
            self.assertEqual(export_queue["row_count"], 0)

            replay_export = create_highlight_export_batch(
                registry_path=registry_path,
                workflow_run_id=workflow["workflow_run_id"],
                output_path=root / "exports" / "replayed.highlight_export_batch.json",
            )

            self.assertTrue(first_export["ok"])
            self.assertTrue(replay_export["ok"])
            self.assertTrue(replay_export["replayed_from_export_ready_snapshot"])
            replay_manifest = json.loads(Path(replay_export["manifest_path"]).read_text(encoding="utf-8"))
            self.assertTrue(replay_manifest["replayed_from_export_ready_snapshot"])
            self.assertEqual(replay_manifest["workflow_run_id"], workflow["workflow_run_id"])
            self.assertEqual(replay_manifest["export_count"], 1)

    def test_local_export_boundary_keeps_post_ledger_unset_until_posting(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="call_of_duty", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(
                fused_sidecar=fused_path,
                output_path=root / "selection" / "alpha.highlight_selection.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(
                fused_path,
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
            refresh_result = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(export_batch["ok"])
            self.assertTrue(refresh_result["ok"])
            manifest = json.loads(Path(export_batch["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "highlight_export_batch_v1")
            self.assertEqual(manifest["export_count"], 1)
            candidate_id = manifest["exports"][0]["candidate_id"]

            exported = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="exported",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(exported["row_count"], 1)
            self.assertEqual(exported["rows"][0]["lifecycle_state"], "exported")
            self.assertTrue(exported["rows"][0]["export_artifact_path"].endswith(".otio.json"))
            self.assertIsNone(exported["rows"][0]["post_ledger_path"])

            export_query = query_clip_registry(
                mode="highlight-exports",
                workflow_run_id=workflow["workflow_run_id"],
                export_status="exported",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(export_query["row_count"], 1)
            self.assertEqual(export_query["rows"][0]["candidate_id"], candidate_id)

    def test_export_batch_can_include_hook_rejected_candidate_when_hook_is_advisory(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _hook_reject_fused_sidecar(fused_path, game="call_of_duty", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(
                fused_sidecar=fused_path,
                output_path=root / "selection" / "alpha.highlight_selection.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            hook_result = derive_hook_candidates(
                fused_path,
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

            self.assertTrue(hook_result["ok"])
            hook_manifest = json.loads(Path(hook_result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(hook_manifest["hook_candidate_count"], 1)
            hook_row = hook_manifest["hook_candidates"][0]
            self.assertEqual(hook_row["lifecycle_state"], "selected_for_export")
            self.assertEqual(hook_row["hook_mode"], "reject")
            self.assertEqual(hook_row["rejection_reason"], "authenticity_risk_too_high")
            self.assertGreaterEqual(hook_row["authenticity_risk_score"], 0.6)
            self.assertIsNone(hook_row["packaging_strategy"])

            self.assertTrue(export_batch["ok"])
            manifest = json.loads(Path(export_batch["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["export_count"], 1)
            export_row = manifest["exports"][0]
            self.assertEqual(export_row["hook_mode"], "reject")
            self.assertIsNone(export_row["packaging_strategy"])
            self.assertEqual(export_row["export_status"], "exported")
            self.assertEqual(export_row["anchor_start_seconds"], 2.0)
            self.assertEqual(export_row["anchor_end_seconds"], 2.0)
            self.assertEqual(export_row["context_start_seconds"], 1.5)
            self.assertEqual(export_row["context_end_seconds"], 2.75)
            self.assertGreater(export_row["context_expansion_seconds"], 0.0)
            self.assertEqual(export_row["context_expansion_policy"], "signal_aware_bounded_v1")
            self.assertEqual(export_row["context_pre_signal_types"], [])
            self.assertEqual(export_row["context_post_signal_types"], [])
            self.assertEqual(export_row["context_signal_count"], 0)
            self.assertIn("fallback_pre_pad", export_row["context_expansion_reasons"])
            self.assertIn("fallback_post_pad", export_row["context_expansion_reasons"])

    def test_runtime_only_reviewed_artifacts_are_not_export_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            runtime_path = root / "runtime" / "alpha.runtime_analysis.json"
            registry_path = root / "registry.sqlite"
            _runtime_sidecar(runtime_path, game="call_of_duty", source=media)

            refresh_result = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(refresh_result["ok"])
            selected = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="selected_for_export",
                registry_path=registry_path,
            )
            export_queue = query_workflow_queue("export_queue", registry_path=registry_path)
            export_batch = create_highlight_export_batch(registry_path=registry_path)

            self.assertEqual(selected["row_count"], 0)
            self.assertEqual(export_queue["row_count"], 0)
            self.assertFalse(export_batch["ok"])
            self.assertEqual(export_batch["status"], "no_selected_candidates")

    def test_bounded_local_test_workspace_supports_runtime_calibration_and_local_export(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")

            runtime_root = root / "runtime"
            registry_path = root / "registry.sqlite"
            fused_path = root / "fused" / "alpha.fused_analysis.json"

            _runtime_sidecar_with_rows(
                runtime_root / "approved-a.runtime_analysis.json",
                game="call_of_duty",
                source=media,
                review_status="approved",
                events=[_runtime_event("medal_seen"), _runtime_event("ability_seen")],
                detections=[_runtime_detection(roi_ref="medal_area", asset_family="medal_icon")],
            )
            _runtime_sidecar_with_rows(
                runtime_root / "approved-b.runtime_analysis.json",
                game="call_of_duty",
                source=media,
                review_status="approved",
                events=[_runtime_event("medal_seen"), _runtime_event("ability_seen")],
                detections=[_runtime_detection(roi_ref="medal_area", asset_family="medal_icon")],
            )
            _runtime_sidecar_with_rows(
                runtime_root / "rejected-a.runtime_analysis.json",
                game="call_of_duty",
                source=media,
                review_status="rejected",
                events=[_runtime_event("pov_character_identified")],
                detections=[_runtime_detection()],
            )
            _runtime_sidecar_with_rows(
                runtime_root / "rejected-b.runtime_analysis.json",
                game="call_of_duty",
                source=media,
                review_status="rejected",
                events=[_runtime_event("pov_character_identified")],
                detections=[_runtime_detection()],
            )

            calibration = run_calibrate_runtime_review(runtime_root, game="call_of_duty", min_reviewed=4)
            _fused_sidecar(fused_path, game="call_of_duty", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(
                fused_sidecar=fused_path,
                output_path=root / "selection" / "alpha.highlight_selection.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(
                fused_path,
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
            refresh_result = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(calibration["ok"])
            self.assertEqual(calibration["status"], "ok")
            self.assertEqual(calibration["reviewed_sidecar_count"], 4)
            self.assertEqual(calibration["approved_count"], 2)
            self.assertEqual(calibration["rejected_count"], 2)
            self.assertEqual(calibration["release_gate_summary"]["status"], "pass")

            self.assertTrue(export_batch["ok"])
            self.assertTrue(refresh_result["ok"])
            manifest = json.loads(Path(export_batch["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "highlight_export_batch_v1")
            self.assertEqual(manifest["export_count"], 1)
            candidate_id = manifest["exports"][0]["candidate_id"]

            exported = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="exported",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(exported["row_count"], 1)
            self.assertTrue(exported["rows"][0]["export_artifact_path"].endswith(".otio.json"))
            self.assertIsNone(exported["rows"][0]["post_ledger_path"])

    def test_record_post_ledger_writes_generic_posted_records(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "selection" / "alpha.highlight_selection.json")
            refresh_clip_registry(root, registry_path=registry_path)
            export_batch = create_highlight_export_batch(
                registry_path=registry_path,
                output_path=root / "exports" / "batch.highlight_export_batch.json",
            )

            result = record_post_ledger(
                export_batch["manifest_path"],
                platform="youtube",
                account_id="acct-1",
                output_path=root / "posted" / "ledger.posted_highlight_ledger.json",
            )

            self.assertTrue(result["ok"])
            ledger = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(ledger["schema_version"], "posted_highlight_ledger_v1")
            self.assertEqual(ledger["posted_count"], 1)
            record = ledger["posted_records"][0]
            self.assertEqual(record["platform"], "youtube")
            self.assertEqual(record["account_id"], "acct-1")
            self.assertEqual(record["post_status"], "posted")

    def test_registry_refresh_promotes_exported_and_posted_and_supports_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "selection" / "alpha.highlight_selection.json")
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "hooks" / "alpha.hook_candidates.json")
            refresh_clip_registry(root, registry_path=registry_path)
            workflow = create_workflow_run("export_queue", registry_path=registry_path, output_path=root / "workflow" / "export.workflow_run.json")
            export_batch = create_highlight_export_batch(
                registry_path=registry_path,
                workflow_run_id=workflow["workflow_run_id"],
                output_path=root / "exports" / "batch.highlight_export_batch.json",
            )

            first = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(first["ok"])
            exported = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="exported",
                registry_path=registry_path,
            )
            self.assertEqual(exported["row_count"], 1)
            candidate_id = exported["rows"][0]["candidate_id"]
            self.assertTrue(exported["rows"][0]["export_artifact_path"].endswith(".otio.json"))

            ledger = record_post_ledger(
                export_batch["manifest_path"],
                workflow_run_id=workflow["workflow_run_id"],
                platform="youtube",
                account_id="acct-1",
                output_path=root / "posted" / "ledger.posted_highlight_ledger.json",
            )
            second = refresh_clip_registry(root, registry_path=registry_path)
            third = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(second["ok"])
            self.assertTrue(third["ok"])
            posted = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="posted",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(posted["row_count"], 1)
            transitions = json.loads(posted["rows"][0]["transitions_json"])
            self.assertEqual(len(transitions), 4)
            export_query = query_clip_registry(
                mode="highlight-exports",
                workflow_run_id=workflow["workflow_run_id"],
                export_status="exported",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            post_query = query_clip_registry(
                mode="post-ledger-records",
                workflow_run_id=workflow["workflow_run_id"],
                post_status="posted",
                platform="youtube",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(export_query["row_count"], 1)
            self.assertEqual(post_query["row_count"], 1)
            self.assertEqual(post_query["rows"][0]["candidate_id"], candidate_id)
            self.assertTrue(posted["rows"][0]["post_ledger_path"].endswith(".posted_highlight_ledger.json"))
            self.assertEqual(ledger["posted_count"], 1)

    def test_materialize_synthetic_post_coverage_creates_downstream_artifacts_and_refreshes_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = root / "registry.sqlite"
            for name in ("alpha", "beta"):
                media = root / "media" / f"{name}.mp4"
                media.parent.mkdir(parents=True, exist_ok=True)
                media.write_bytes(b"video")
                fused_path = root / "fused" / f"{name}.fused_analysis.json"
                _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(root / "fused" / "alpha.fused_analysis.json", registry_path=registry_path, output_path=root / "hooks" / "alpha.hook_candidates.json")
            derive_hook_candidates(root / "fused" / "beta.fused_analysis.json", registry_path=registry_path, output_path=root / "hooks" / "beta.hook_candidates.json")
            refresh_clip_registry(root, registry_path=registry_path)

            result = materialize_synthetic_post_coverage(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct",
                output_root=root / "synthetic_post_coverage",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["candidate_count"], 2)
            self.assertEqual(result["created_selection_count"], 2)
            self.assertEqual(result["posted_candidate_count_after_refresh"], 2)
            self.assertTrue(Path(result["export_manifest_path"]).exists())
            self.assertTrue(Path(result["post_ledger_manifest_path"]).exists())
            self.assertTrue(Path(result["metrics_snapshot_manifest_path"]).exists())

            posted = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="posted",
                registry_path=registry_path,
                game="marvel_rivals",
            )
            metrics = query_clip_registry(
                mode="posted-metrics",
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct",
            )
            self.assertEqual(posted["row_count"], 2)
            self.assertEqual(metrics["row_count"], 2)
            self.assertTrue(all(row["post_performance_label_eligible"] for row in metrics["rows"]))
            self.assertTrue(all(row["post_performance_coverage_tier"] == "posted_usable_metrics" for row in metrics["rows"]))
            self.assertTrue(all(json.loads(row["metadata_json"])["synthetic_benchmark"] for row in metrics["rows"]))

    def test_materialize_synthetic_post_coverage_can_include_rejected_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = root / "registry.sqlite"

            approved_media = root / "media" / "approved.mp4"
            approved_media.parent.mkdir(parents=True, exist_ok=True)
            approved_media.write_bytes(b"video")
            rejected_media = root / "media" / "rejected.mp4"
            rejected_media.write_bytes(b"video")

            _fused_sidecar(root / "fused" / "approved.fused_analysis.json", game="marvel_rivals", source=approved_media, review_status="approved")
            _fused_sidecar(root / "fused" / "rejected.fused_analysis.json", game="marvel_rivals", source=rejected_media, review_status="rejected")
            refresh_clip_registry(root, registry_path=registry_path)

            baseline = materialize_synthetic_post_coverage(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct",
                output_root=root / "synthetic_approved_only",
            )
            self.assertTrue(baseline["ok"])
            self.assertEqual(baseline["candidate_count"], 1)

            rejected_candidate_id = next(
                row["candidate_id"]
                for row in query_clip_registry(
                    mode="candidate-lifecycles",
                    registry_path=registry_path,
                    game="marvel_rivals",
                )["rows"]
                if row["latest_review_status"] == "rejected"
            )
            rejected_post_before = query_clip_registry(
                mode="candidate-lifecycles",
                registry_path=registry_path,
                game="marvel_rivals",
                candidate_id=rejected_candidate_id,
            )
            self.assertEqual(rejected_post_before["rows"][0]["lifecycle_state"], "rejected")

            result = materialize_synthetic_post_coverage(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct-rejected",
                output_root=root / "synthetic_with_rejected",
                include_rejected=True,
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["include_rejected"])
            self.assertEqual(result["candidate_count"], 1)

            rejected_post_after = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="posted",
                registry_path=registry_path,
                game="marvel_rivals",
                candidate_id=rejected_candidate_id,
            )
            self.assertEqual(rejected_post_after["row_count"], 1)
            metrics = query_clip_registry(
                mode="posted-metrics",
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct-rejected",
                candidate_id=rejected_candidate_id,
            )
            self.assertEqual(metrics["row_count"], 1)
            self.assertTrue(metrics["rows"][0]["post_performance_label_eligible"])

    def test_cli_routes_to_export_batch_and_post_ledger(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--create-highlight-export-batch",
                "--workflow-run-id",
                "workflow-123",
            ]
            with patch("run.run_create_highlight_export_batch", return_value={"ok": True, "manifest_path": "/tmp/export.json"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())

            __import__("sys").argv = [
                "run.py",
                "--record-post-ledger",
                "--export-manifest",
                "/tmp/export.json",
                "--platform",
                "youtube",
            ]
            with patch("run.run_record_post_ledger", return_value={"ok": True, "manifest_path": "/tmp/post.json"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()
