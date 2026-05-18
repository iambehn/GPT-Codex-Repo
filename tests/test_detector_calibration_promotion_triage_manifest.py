from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.detector_calibration_promotion_triage_manifest import generate_detector_calibration_promotion_triage_manifest


def _promotion_record(
    *,
    promotion_id: str,
    asset_id: str = "marvel_rivals.ace.team_wipe_announcement",
    created_at: str = "2026-05-14T01:00:00Z",
    eligibility: dict | None = None,
) -> dict:
    return {
        "schema_version": "detector_calibration_revised_crop_promotion_v1",
        "promotion_id": promotion_id,
        "promotion_root": f"/tmp/{promotion_id}",
        "created_at": created_at,
        "game": "marvel_rivals",
        "source_evidence": {
            "candidate_id": "rev-001",
            "run_id": "replay-001",
        },
        "published_asset": {
            "asset_id": asset_id,
        },
        "replay_evidence": {
            "eligibility": eligibility,
        },
    }


def _eligibility(
    *,
    delta_iou: float,
    primary_source: str = "replay_run",
    primary_reference_source: str = "localized_match",
    primary_iou: float = 1.0,
    secondary_iou: float = 0.8,
) -> dict:
    return {
        "status": "ok",
        "candidate_id": "rev-001",
        "run_id": "replay-001",
        "primary_source": primary_source,
        "secondary_source": "crop_candidate",
        "primary_reference_source": primary_reference_source,
        "secondary_reference_source": "roi_fallback",
        "primary_iou": primary_iou,
        "secondary_iou": secondary_iou,
        "delta_iou": delta_iou,
        "difference_summary": "summary",
    }


class DetectorCalibrationPromotionTriageManifestTests(unittest.TestCase):
    def test_generate_manifest_classifies_publish_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            promotions_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "detector_calibration_promotions"
            record_path = _write_promotion_record(
                promotions_root / "ready-a" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(
                    promotion_id="ready-a",
                    eligibility=_eligibility(delta_iou=0.188046),
                ),
            )
            output_path = root / "triage.json"
            with patch(
                "tools.detector_calibration_promotion_triage_manifest.validate_revised_crop_promotion_draft",
                return_value={"ok": True, "status": "ok"},
            ):
                result = generate_detector_calibration_promotion_triage_manifest(
                    game="marvel_rivals",
                    draft_promotions_root=promotions_root,
                    output_path=output_path,
                )
            self.assertTrue(result["ok"])
            self.assertEqual(result["row_count"], 1)
            row = result["rows"][0]
            self.assertEqual(row["triage_status"], "publish_ready")
            self.assertEqual(row["triage_reason"], "validated_replay_improvement_meets_publish_threshold")
            self.assertEqual(row["delta_iou"], 0.188046)
            self.assertEqual(row["promotion_record_path"], str(record_path.resolve()))
            self.assertEqual(
                result["emitted_manifest"],
                {
                    "path": str(output_path.resolve()),
                    "row_count": 1,
                    "source_promotion_count": 1,
                    "status_counts": {"publish_ready": 1, "needs_more_replay": 0, "hold": 0},
                    "top_row": row,
                },
            )

    def test_generate_manifest_classifies_needs_more_replay_below_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            promotions_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "detector_calibration_promotions"
            _write_promotion_record(
                promotions_root / "replay-more" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(
                    promotion_id="replay-more",
                    eligibility=_eligibility(delta_iou=0.05),
                ),
            )
            with patch(
                "tools.detector_calibration_promotion_triage_manifest.validate_revised_crop_promotion_draft",
                return_value={"ok": True, "status": "ok"},
            ):
                result = generate_detector_calibration_promotion_triage_manifest(
                    game="marvel_rivals",
                    draft_promotions_root=promotions_root,
                    output_path=root / "triage.json",
                )
            self.assertEqual(result["rows"][0]["triage_status"], "needs_more_replay")
            self.assertEqual(result["rows"][0]["triage_reason"], "positive_replay_improvement_below_publish_threshold")

    def test_generate_manifest_classifies_hold_on_validation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            promotions_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "detector_calibration_promotions"
            _write_promotion_record(
                promotions_root / "broken" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(
                    promotion_id="broken",
                    eligibility=_eligibility(delta_iou=0.2),
                ),
            )
            with patch(
                "tools.detector_calibration_promotion_triage_manifest.validate_revised_crop_promotion_draft",
                return_value={"ok": False, "status": "incomplete_provenance"},
            ):
                result = generate_detector_calibration_promotion_triage_manifest(
                    game="marvel_rivals",
                    draft_promotions_root=promotions_root,
                    output_path=root / "triage.json",
                )
            self.assertEqual(result["rows"][0]["triage_status"], "hold")
            self.assertEqual(result["rows"][0]["triage_reason"], "draft_promotion_validation_failed")
            self.assertEqual(result["rows"][0]["draft_validation_status"], "incomplete_provenance")

    def test_generate_manifest_classifies_hold_on_non_positive_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            promotions_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "detector_calibration_promotions"
            _write_promotion_record(
                promotions_root / "non-positive" / "manifests" / "revised_crop_promotion.json",
                _promotion_record(
                    promotion_id="non-positive",
                    eligibility=_eligibility(delta_iou=0.0),
                ),
            )
            with patch(
                "tools.detector_calibration_promotion_triage_manifest.validate_revised_crop_promotion_draft",
                return_value={"ok": True, "status": "ok"},
            ):
                result = generate_detector_calibration_promotion_triage_manifest(
                    game="marvel_rivals",
                    draft_promotions_root=promotions_root,
                    output_path=root / "triage.json",
                )
            self.assertEqual(result["rows"][0]["triage_status"], "hold")
            self.assertEqual(result["rows"][0]["triage_reason"], "non_positive_replay_improvement")

    def test_generate_manifest_sorts_by_status_then_delta_then_recency(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            promotions_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "detector_calibration_promotions"
            fixtures = [
                (
                    "ready-low",
                    _promotion_record(
                        promotion_id="ready-low",
                        created_at="2026-05-14T01:00:00Z",
                        eligibility=_eligibility(delta_iou=0.2),
                    ),
                ),
                (
                    "replay-more-newer",
                    _promotion_record(
                        promotion_id="replay-more-newer",
                        created_at="2026-05-14T03:00:00Z",
                        eligibility=_eligibility(delta_iou=0.1),
                    ),
                ),
                (
                    "hold-largest",
                    _promotion_record(
                        promotion_id="hold-largest",
                        created_at="2026-05-14T04:00:00Z",
                        eligibility=_eligibility(delta_iou=-0.4),
                    ),
                ),
                (
                    "ready-high",
                    _promotion_record(
                        promotion_id="ready-high",
                        created_at="2026-05-14T02:00:00Z",
                        eligibility=_eligibility(delta_iou=0.5),
                    ),
                ),
                (
                    "replay-more-older",
                    _promotion_record(
                        promotion_id="replay-more-older",
                        created_at="2026-05-14T01:30:00Z",
                        eligibility=_eligibility(delta_iou=0.1),
                    ),
                ),
            ]
            for name, payload in fixtures:
                _write_promotion_record(promotions_root / name / "manifests" / "revised_crop_promotion.json", payload)
            with patch(
                "tools.detector_calibration_promotion_triage_manifest.validate_revised_crop_promotion_draft",
                return_value={"ok": True, "status": "ok"},
            ), patch(
                "tools.detector_calibration_promotion_triage_manifest._utc_now",
                return_value="2026-05-14T05:00:00+00:00",
            ):
                result = generate_detector_calibration_promotion_triage_manifest(
                    game="marvel_rivals",
                    draft_promotions_root=promotions_root,
                    output_path=root / "triage.json",
                )
            ordered_ids = [row["promotion_id"] for row in result["rows"]]
            self.assertEqual(
                ordered_ids,
                ["ready-high", "ready-low", "replay-more-newer", "replay-more-older", "hold-largest"],
            )
            manifest = json.loads((root / "triage.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["generated_at"], "2026-05-14T05:00:00+00:00")
            self.assertEqual(
                manifest["status_counts"],
                {"publish_ready": 2, "needs_more_replay": 2, "hold": 1},
            )


def _write_promotion_record(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
