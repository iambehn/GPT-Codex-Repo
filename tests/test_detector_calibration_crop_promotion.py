from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.roi_matcher import PublishedRuntimePack, RoiBounds, TemplateSpec
from tools.detector_calibration_crop_promotion import (
    prepare_revised_crop_promotion,
    validate_revised_crop_promotion_draft,
)


def _runtime_pack(game_root: Path, template_path: Path) -> PublishedRuntimePack:
    return PublishedRuntimePack(
        game="marvel_rivals",
        root=game_root,
        pack_summary={"game": "marvel_rivals"},
        templates=[
            TemplateSpec(
                asset_id="marvel_rivals.ace.team_wipe_announcement",
                roi_ref="team_wipe_announcement",
                template_path=template_path,
                mask_path=None,
                threshold=0.93,
                scale_set=[1.0],
                temporal_window=2,
                match_method="TM_CCOEFF_NORMED",
                asset_family="team_wipe_announcement",
                event_row_id="ace",
            )
        ],
        rois={"team_wipe_announcement": RoiBounds(x=499, y=172, width=921, height=194)},
        width=1920,
        height=1080,
        template_manifest={},
        detection_manifest={},
        runtime_rules_manifest={},
        fusion_rules_manifest={},
        runtime_rules={},
    )


class DetectorCalibrationCropPromotionTests(unittest.TestCase):
    def test_prepare_rejects_missing_replay_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            session_root, game_root = _write_session_fixture(root, replay_comparison=None)

            with patch("tools.detector_calibration_crop_promotion.load_published_runtime_pack", return_value=_runtime_pack(game_root, game_root / "templates" / "team_wipes" / "ace.png")), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = prepare_revised_crop_promotion(
                    session_root,
                    candidate_id="rev-001",
                    run_id="replay-001",
                    approved_by="tj",
                )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "missing_replay_derived_comparison")

    def test_prepare_rejects_non_localized_replay_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            replay_comparison = _comparison("roi_fallback", "499,172,921,194", 0.9)
            candidate_comparison = _comparison("roi_fallback", "499,172,921,194", 0.8)
            session_root, game_root = _write_session_fixture(root, replay_comparison=replay_comparison, candidate_comparison=candidate_comparison)

            with patch("tools.detector_calibration_crop_promotion.load_published_runtime_pack", return_value=_runtime_pack(game_root, game_root / "templates" / "team_wipes" / "ace.png")), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = prepare_revised_crop_promotion(
                    session_root,
                    candidate_id="rev-001",
                    run_id="replay-001",
                    approved_by="tj",
                )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "ineligible_reference_source")

    def test_prepare_rejects_non_positive_delta_iou(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            replay_comparison = _comparison("localized_match", "557,191,829,175", 0.8)
            candidate_comparison = _comparison("roi_fallback", "499,172,921,194", 0.8)
            session_root, game_root = _write_session_fixture(root, replay_comparison=replay_comparison, candidate_comparison=candidate_comparison)

            with patch("tools.detector_calibration_crop_promotion.load_published_runtime_pack", return_value=_runtime_pack(game_root, game_root / "templates" / "team_wipes" / "ace.png")), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = prepare_revised_crop_promotion(
                    session_root,
                    candidate_id="rev-001",
                    run_id="replay-001",
                    approved_by="tj",
                )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "ineligible_delta_iou")

    def test_prepare_materializes_draft_promotion_and_validation_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            replay_comparison = _comparison("localized_match", "557,191,829,175", 1.0)
            candidate_comparison = _comparison("roi_fallback", "499,172,921,194", 0.811954)
            session_root, game_root = _write_session_fixture(root, replay_comparison=replay_comparison, candidate_comparison=candidate_comparison)
            promotion_root = root / "assets" / "games" / "marvel_rivals" / "drafts" / "detector_calibration_promotions" / "manual"

            with patch("tools.detector_calibration_crop_promotion.load_published_runtime_pack", return_value=_runtime_pack(game_root, game_root / "templates" / "team_wipes" / "ace.png")), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = prepare_revised_crop_promotion(
                    session_root,
                    candidate_id="rev-001",
                    run_id="replay-001",
                    approved_by="tj",
                    promotion_root=promotion_root,
                )

            self.assertTrue(result["ok"])
            record_path = Path(result["promotion_record_path"])
            self.assertTrue(record_path.is_file())
            draft_template_path = Path(result["draft_template_path"])
            self.assertTrue(draft_template_path.is_file())
            self.assertEqual(draft_template_path.read_bytes(), b"revised-crop")
            record = json.loads(record_path.read_text(encoding="utf-8"))
            self.assertEqual(record["operator_approval"]["approved_by"], "tj")
            self.assertEqual(record["replay_evidence"]["eligibility"]["primary_source"], "replay_run")
            self.assertEqual(record["replay_evidence"]["eligibility"]["primary_reference_source"], "localized_match")
            self.assertEqual(record["replay_evidence"]["eligibility"]["delta_iou"], 0.188046)
            with patch("tools.detector_calibration_crop_promotion.REPO_ROOT", root):
                validation = validate_revised_crop_promotion_draft(record_path)
            self.assertTrue(validation["ok"])

    def test_validation_rejects_incomplete_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path = root / "promotion" / "manifests" / "revised_crop_promotion.json"
            record_path.parent.mkdir(parents=True, exist_ok=True)
            record_path.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_revised_crop_promotion_v1",
                        "promotion_root": str(root / "promotion"),
                        "game": "marvel_rivals",
                        "source_evidence": {},
                        "published_asset": {},
                        "draft_update": {},
                        "operator_approval": {},
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = validate_revised_crop_promotion_draft(record_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "incomplete_provenance")

    def test_validation_rejects_inconsistent_asset_linkage(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            game_root = _write_game_root(root)
            draft_template = root / "promotion" / "templates" / "team_wipes" / "ace.png"
            draft_template.parent.mkdir(parents=True, exist_ok=True)
            draft_template.write_bytes(b"draft")
            record_path = root / "promotion" / "manifests" / "revised_crop_promotion.json"
            record_path.parent.mkdir(parents=True, exist_ok=True)
            record_path.write_text(
                json.dumps(
                    {
                        "schema_version": "detector_calibration_revised_crop_promotion_v1",
                        "promotion_root": str(root / "promotion"),
                        "game": "marvel_rivals",
                        "source_evidence": {
                            "review_record_path": str(root / "session" / "review_record.json"),
                            "session_root": str(root / "session"),
                            "candidate_id": "rev-001",
                            "run_id": "replay-001",
                            "replay_result_path": str(root / "session" / "replay_results" / "replay-001" / "replay_result.json"),
                        },
                        "published_asset": {
                            "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                            "template_path": str((game_root / "templates" / "wrong" / "ace.png").resolve()),
                        },
                        "draft_update": {
                            "template_path": str(draft_template),
                        },
                        "operator_approval": {
                            "approved_by": "tj",
                        },
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            with patch("tools.detector_calibration_crop_promotion.REPO_ROOT", root):
                result = validate_revised_crop_promotion_draft(record_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "inconsistent_asset_linkage")


def _comparison(reference_source: str, reference_crop: str, iou: float) -> dict[str, object]:
    return {
        "status": "ok",
        "published_dimensions": {"width": 921, "height": 194},
        "revised_dimensions": {"width": 829, "height": 175},
        "delta": {"width": -92, "height": -19},
        "dimensions_match": False,
        "spatial_overlap": {
            "reference_source": reference_source,
            "reference_crop": reference_crop,
            "intersection": {"x": 557, "y": 191, "w": 829, "h": 175},
            "intersection_area": 145075,
            "revised_area": 145075,
            "reference_area": 178674,
            "iou": iou,
            "revised_coverage_ratio": 1.0,
            "reference_coverage_ratio": iou,
        },
    }


def _write_game_root(root: Path) -> Path:
    game_root = root / "assets" / "games" / "marvel_rivals"
    manifests_root = game_root / "manifests"
    templates_root = game_root / "templates" / "team_wipes"
    masters_root = game_root / "masters" / "team_wipes"
    manifests_root.mkdir(parents=True, exist_ok=True)
    templates_root.mkdir(parents=True, exist_ok=True)
    masters_root.mkdir(parents=True, exist_ok=True)
    (templates_root / "ace.png").write_bytes(b"published-template")
    (masters_root / "ace.png").write_bytes(b"published-master")
    (manifests_root / "assets_manifest.json").write_text(
        json.dumps(
            {
                "game_id": "marvel_rivals",
                "published_assets": [
                    {
                        "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                        "candidate_id": "candidate_manual_ace_team_wipe_announcement_20260512",
                        "target_id": "ace",
                        "display_name": "Ace Team Wipe Announcement",
                        "asset_family": "team_wipe_announcement",
                        "master_path": "masters/team_wipes/ace.png",
                        "template_path": "templates/team_wipes/ace.png",
                        "detection_id": "marvel_rivals.ace.team_wipe_announcement",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return game_root


def _write_session_fixture(root: Path, *, replay_comparison: dict[str, object] | None, candidate_comparison: dict[str, object] | None = None) -> tuple[Path, Path]:
    game_root = _write_game_root(root)
    session_root = root / "outputs" / "detector_calibration" / "marvel_rivals" / "session-001"
    candidate_root = session_root / "crop_candidates" / "rev-001"
    replay_root = session_root / "replay_results" / "replay-001"
    candidate_root.mkdir(parents=True, exist_ok=True)
    replay_root.mkdir(parents=True, exist_ok=True)
    crop_png = candidate_root / "rev-001.png"
    crop_png.write_bytes(b"revised-crop")
    replay_result_path = replay_root / "replay_result.json"
    replay_result_path.write_text(
        json.dumps(
            {
                "schema_version": "detector_calibration_replay_v1",
                "ok": True,
                "status": "ok",
                "run_id": "replay-001",
                "candidate_id": "rev-001",
                "derived_template_comparison": replay_comparison,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    review_record = {
        "schema_version": "detector_calibration_review_v1",
        "session_id": "session-001",
        "session_root": str(session_root),
        "game": "marvel_rivals",
        "target": {
            "asset_id": "marvel_rivals.ace.team_wipe_announcement",
            "roi_ref": "team_wipe_announcement",
            "source": str((root / "clip.mp4").resolve()),
            "runtime_sidecar_path": str((root / "runtime.runtime_analysis.json").resolve()),
            "event_type": "team_wipe_seen",
            "event_row_id": "ace",
        },
        "published_template": {
            "template_path": str((game_root / "templates" / "team_wipes" / "ace.png").resolve()),
        },
        "crop_candidates": [
            {
                "candidate_id": "rev-001",
                "crop_png_path": str(crop_png),
                "crop": {"x": 557, "y": 191, "w": 829, "h": 175},
                "template_comparison": candidate_comparison,
            }
        ],
        "replay_runs": [
            {
                "run_id": "replay-001",
                "candidate_id": "rev-001",
                "replay_result_path": str(replay_result_path),
                "trial_runtime_sidecar_path": str((replay_root / "trial" / "runtime_analysis.json").resolve()),
                "current_runtime_sidecar_path": str((replay_root / "current" / "runtime_analysis.json").resolve()),
            }
        ],
    }
    (session_root / "review_record.json").write_text(json.dumps(review_record, indent=2), encoding="utf-8")
    return session_root, game_root
