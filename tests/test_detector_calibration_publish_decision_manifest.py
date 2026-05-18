from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.detector_calibration_publish_decision_manifest import generate_detector_calibration_publish_decision_manifest


def _triage_row(
    *,
    status: str,
    asset_id: str,
    promotion_record_path: str,
    promotion_id: str,
    delta_iou: float,
    created_at: str = "2026-05-14T01:00:00Z",
    validation_status: str = "ok",
) -> dict:
    return {
        "triage_status": status,
        "triage_reason": "reason",
        "promotion_record_path": promotion_record_path,
        "promotion_root": str(Path(promotion_record_path).parent.parent),
        "promotion_id": promotion_id,
        "created_at": created_at,
        "asset_id": asset_id,
        "candidate_id": "rev-001",
        "run_id": "replay-001",
        "delta_iou": delta_iou,
        "absolute_delta_iou": abs(delta_iou),
        "difference_summary": "summary",
        "draft_validation_status": validation_status,
    }


def _triage_manifest(*, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_promotion_triage_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-14T01:00:00+00:00",
        "publish_ready_threshold_delta_iou": 0.15,
        "source_promotion_count": len(rows),
        "row_count": len(rows),
        "status_counts": {
            "publish_ready": sum(1 for row in rows if row.get("triage_status") == "publish_ready"),
            "needs_more_replay": sum(1 for row in rows if row.get("triage_status") == "needs_more_replay"),
            "hold": sum(1 for row in rows if row.get("triage_status") == "hold"),
        },
        "rows": rows,
    }


def _promotion_record(*, promotion_id: str, asset_id: str, source: str | None = None, runtime_sidecar_path: str | None = None, session_root: str | None = None) -> dict:
    return {
        "schema_version": "detector_calibration_revised_crop_promotion_v1",
        "promotion_id": promotion_id,
        "promotion_root": f"/tmp/{promotion_id}",
        "created_at": "2026-05-14T01:00:00Z",
        "game": "marvel_rivals",
        "source_evidence": {
            "source": source,
            "runtime_sidecar_path": runtime_sidecar_path,
            "session_root": session_root,
        },
        "published_asset": {
            "asset_id": asset_id,
        },
    }


class DetectorCalibrationPublishDecisionManifestTests(unittest.TestCase):
    def test_single_publish_ready_row_becomes_needs_broader_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            promotion_path = _write_json(
                root / "promotions" / "hero" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(
                    promotion_id="hero",
                    asset_id="marvel_rivals.human_torch.hero_portrait",
                    source="/tmp/clip-a.mp4",
                ),
            )
            triage_path = _write_json(
                root / "triage.json",
                _triage_manifest(
                    rows=[
                        _triage_row(
                            status="publish_ready",
                            asset_id="marvel_rivals.human_torch.hero_portrait",
                            promotion_record_path=str(promotion_path),
                            promotion_id="hero",
                            delta_iou=0.95,
                        )
                    ]
                ),
            )
            output_path = root / "decision.json"
            result = generate_detector_calibration_publish_decision_manifest(
                triage_manifest=triage_path,
                output_path=output_path,
            )
            self.assertTrue(result["ok"])
            row = result["rows"][0]
            self.assertEqual(row["decision_status"], "needs_broader_evidence")
            self.assertEqual(row["decision_reason"], "publish_ready_but_single_evidence_base")
            self.assertEqual(row["replay_count_for_asset"], 1)
            self.assertEqual(row["distinct_source_count_for_asset"], 1)
            self.assertEqual(row["source_keys_for_asset"], ["/tmp/clip-a.mp4"])

    def test_multi_evidence_asset_becomes_ready_to_publish(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            asset_id = "marvel_rivals.ace.team_wipe_announcement"
            promotion_a = _write_json(
                root / "promotions" / "ace-a" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(promotion_id="ace-a", asset_id=asset_id, source="/tmp/clip-a.mp4"),
            )
            promotion_b = _write_json(
                root / "promotions" / "ace-b" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(promotion_id="ace-b", asset_id=asset_id, source="/tmp/clip-b.mp4"),
            )
            triage_path = _write_json(
                root / "triage.json",
                _triage_manifest(
                    rows=[
                        _triage_row(
                            status="publish_ready",
                            asset_id=asset_id,
                            promotion_record_path=str(promotion_a),
                            promotion_id="ace-a",
                            delta_iou=0.4,
                            created_at="2026-05-14T02:00:00Z",
                        ),
                        _triage_row(
                            status="publish_ready",
                            asset_id=asset_id,
                            promotion_record_path=str(promotion_b),
                            promotion_id="ace-b",
                            delta_iou=0.2,
                            created_at="2026-05-14T01:00:00Z",
                        ),
                    ]
                ),
            )
            result = generate_detector_calibration_publish_decision_manifest(
                triage_manifest=triage_path,
                output_path=root / "decision.json",
            )
            self.assertEqual([row["decision_status"] for row in result["rows"]], ["ready_to_publish", "ready_to_publish"])
            self.assertEqual(result["rows"][0]["replay_count_for_asset"], 2)
            self.assertEqual(result["rows"][0]["distinct_source_count_for_asset"], 2)
            self.assertEqual(result["rows"][0]["source_keys_for_asset"], ["/tmp/clip-a.mp4", "/tmp/clip-b.mp4"])

    def test_non_publish_ready_row_becomes_defer(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            promotion_path = _write_json(
                root / "promotions" / "hold" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(
                    promotion_id="hold",
                    asset_id="marvel_rivals.ace.team_wipe_announcement",
                    source="/tmp/clip-a.mp4",
                ),
            )
            triage_path = _write_json(
                root / "triage.json",
                _triage_manifest(
                    rows=[
                        _triage_row(
                            status="hold",
                            asset_id="marvel_rivals.ace.team_wipe_announcement",
                            promotion_record_path=str(promotion_path),
                            promotion_id="hold",
                            delta_iou=-0.2,
                        )
                    ]
                ),
            )
            result = generate_detector_calibration_publish_decision_manifest(
                triage_manifest=triage_path,
                output_path=root / "decision.json",
            )
            self.assertEqual(result["rows"][0]["decision_status"], "defer")
            self.assertEqual(result["rows"][0]["decision_reason"], "source_triage_not_publish_ready")

    def test_missing_promotion_record_keeps_row_and_defers(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            triage_path = _write_json(
                root / "triage.json",
                _triage_manifest(
                    rows=[
                        _triage_row(
                            status="publish_ready",
                            asset_id="marvel_rivals.ace.team_wipe_announcement",
                            promotion_record_path=str(root / "missing.json"),
                            promotion_id="missing",
                            delta_iou=0.2,
                        )
                    ]
                ),
            )
            result = generate_detector_calibration_publish_decision_manifest(
                triage_manifest=triage_path,
                output_path=root / "decision.json",
            )
            self.assertEqual(result["rows"][0]["decision_status"], "defer")
            self.assertEqual(result["rows"][0]["decision_reason"], "missing_promotion_record_for_breadth_count")
            self.assertEqual(result["rows"][0]["replay_count_for_asset"], 0)
            self.assertEqual(result["rows"][0]["distinct_source_count_for_asset"], 0)

    def test_ordering_and_emitted_summary_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            asset_ready = "marvel_rivals.ace.team_wipe_announcement"
            asset_single = "marvel_rivals.human_torch.hero_portrait"
            ready_a = _write_json(
                root / "promotions" / "ready-a" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(promotion_id="ready-a", asset_id=asset_ready, source="/tmp/clip-a.mp4"),
            )
            ready_b = _write_json(
                root / "promotions" / "ready-b" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(promotion_id="ready-b", asset_id=asset_ready, source="/tmp/clip-b.mp4"),
            )
            single = _write_json(
                root / "promotions" / "single" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(promotion_id="single", asset_id=asset_single, runtime_sidecar_path="/tmp/runtime.json"),
            )
            hold = _write_json(
                root / "promotions" / "hold" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(promotion_id="hold", asset_id="marvel_rivals.defeat.end_match_result_banner", session_root="/tmp/session"),
            )
            triage_path = _write_json(
                root / "triage.json",
                _triage_manifest(
                    rows=[
                        _triage_row(status="publish_ready", asset_id=asset_ready, promotion_record_path=str(ready_a), promotion_id="ready-a", delta_iou=0.5, created_at="2026-05-14T03:00:00Z"),
                        _triage_row(status="publish_ready", asset_id=asset_ready, promotion_record_path=str(ready_b), promotion_id="ready-b", delta_iou=0.3, created_at="2026-05-14T02:00:00Z"),
                        _triage_row(status="publish_ready", asset_id=asset_single, promotion_record_path=str(single), promotion_id="single", delta_iou=0.9, created_at="2026-05-14T04:00:00Z"),
                        _triage_row(status="hold", asset_id="marvel_rivals.defeat.end_match_result_banner", promotion_record_path=str(hold), promotion_id="hold", delta_iou=-0.1, created_at="2026-05-14T01:00:00Z"),
                    ]
                ),
            )
            output_path = root / "decision.json"
            result = generate_detector_calibration_publish_decision_manifest(
                triage_manifest=triage_path,
                output_path=output_path,
            )
            self.assertEqual(
                [row["promotion_id"] for row in result["rows"]],
                ["ready-a", "ready-b", "single", "hold"],
            )
            self.assertEqual(
                result["status_counts"],
                {"ready_to_publish": 2, "needs_broader_evidence": 1, "defer": 1},
            )
            self.assertEqual(
                result["emitted_manifest"],
                {
                    "path": str(output_path.resolve()),
                    "row_count": 4,
                    "source_row_count": 4,
                    "status_counts": {"ready_to_publish": 2, "needs_broader_evidence": 1, "defer": 1},
                    "top_row": result["rows"][0],
                },
            )


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
