from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.roi_matcher import PublishedRuntimePack, RoiBounds, TemplateSpec
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from tools.detector_calibration_published_pack_promotion import (
    main as published_promotion_main,
    promote_revised_crop_to_published_pack,
    rollback_revised_crop_published_pack_promotion,
    validate_published_calibration_promotion,
    validate_rolled_back_calibration_promotion,
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
                threshold=0.92,
                scale_set=[0.9, 1.0],
                temporal_window=1,
                match_method="TM_CCORR_NORMED",
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


class DetectorCalibrationPublishedPackPromotionTests(unittest.TestCase):
    def test_promote_rejects_invalid_draft_promotion_record(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path = root / "missing.json"
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = promote_revised_crop_to_published_pack(record_path, promoted_by="tj")
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_draft_promotion_record")

    def test_promote_rejects_missing_published_asset_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, game_root = _write_promotion_fixture(root)
            manifest_path = game_root / "manifests" / "assets_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["published_assets"] = []
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = promote_revised_crop_to_published_pack(record_path, promoted_by="tj")
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_draft_promotion_record")

    def test_promote_rejects_missing_template_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, game_root = _write_promotion_fixture(root)
            dump_yaml_file(game_root / "manifests" / "cv_templates.yaml", {"templates": []})
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = promote_revised_crop_to_published_pack(record_path, promoted_by="tj")
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "missing_template_row")

    def test_promote_rejects_published_linkage_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, game_root = _write_promotion_fixture(root)
            manifest_path = game_root / "manifests" / "assets_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["published_assets"][0]["template_path"] = "templates/team_wipes/drifted.png"
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = promote_revised_crop_to_published_pack(record_path, promoted_by="tj")
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_draft_promotion_record")

    def test_promote_overwrites_template_updates_manifests_and_validates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, game_root = _write_promotion_fixture(root)
            template_path = game_root / "templates" / "team_wipes" / "ace.png"
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ), patch(
                "tools.detector_calibration_published_pack_promotion.validate_published_pack",
                return_value={"ok": True, "status": "ok"},
            ):
                result = promote_revised_crop_to_published_pack(record_path, promoted_by="tj")

            self.assertTrue(result["ok"])
            self.assertEqual(template_path.read_bytes(), b"revised-template-bytes")
            backup_path = Path(result["backup_path"])
            self.assertTrue(backup_path.is_file())
            self.assertEqual(backup_path.read_bytes(), b"published-template")
            assets_manifest = json.loads((game_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
            asset_row = assets_manifest["published_assets"][0]
            self.assertEqual(asset_row["calibration_promotion_source"], "detector_calibration_revised_crop_promotion_v1")
            self.assertEqual(asset_row["calibration_promotion_record_path"], str(record_path.resolve()))
            self.assertEqual(asset_row["calibration_promotion_run_id"], "replay-003")
            self.assertEqual(asset_row["calibration_promotion_promoted_by"], "tj")
            self.assertTrue(str(asset_row["file_hash"]).startswith("sha256:"))
            cv_templates = load_yaml_file(game_root / "manifests" / "cv_templates.yaml")
            template_row = cv_templates["templates"][0]
            self.assertEqual(template_row["calibration_promotion_record_path"], str(record_path.resolve()))
            self.assertEqual(template_row["calibration_promotion_run_id"], "replay-003")
            self.assertEqual(template_row["calibration_promotion_promoted_by"], "tj")
            self.assertEqual(template_row["file_hash"], asset_row["file_hash"])

    def test_validate_rejects_missing_template_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, game_root = _write_promotion_fixture(root)
            cv_templates = load_yaml_file(game_root / "manifests" / "cv_templates.yaml")
            actual_hash = _sha256(game_root / "templates" / "team_wipes" / "ace.png")
            cv_templates["templates"][0]["file_hash"] = actual_hash
            dump_yaml_file(game_root / "manifests" / "cv_templates.yaml", cv_templates)
            assets_manifest = json.loads((game_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
            assets_manifest["published_assets"][0]["file_hash"] = actual_hash
            for key in (
                "calibration_promotion_source",
                "calibration_promotion_record_path",
                "calibration_promotion_session_root",
                "calibration_promotion_candidate_id",
                "calibration_promotion_run_id",
                "calibration_promotion_revised_crop_path",
                "calibration_promotion_approved_by",
                "calibration_promotion_promoted_at",
                "calibration_promotion_promoted_by",
            ):
                assets_manifest["published_assets"][0][key] = "x"
            (game_root / "manifests" / "assets_manifest.json").write_text(json.dumps(assets_manifest, indent=2) + "\n", encoding="utf-8")
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = validate_published_calibration_promotion(promotion_record_path=record_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "missing_template_provenance")

    def test_rollback_rejects_missing_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, _, _ = _write_promoted_fixture(root)
            backup_path = root / "assets" / "games" / "marvel_rivals" / "templates" / "team_wipes" / "missing.png"
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = rollback_revised_crop_published_pack_promotion(
                    record_path,
                    backup_path=backup_path,
                    rolled_back_by="tj",
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "missing_backup")

    def test_rollback_rejects_when_rows_no_longer_point_at_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, game_root, backup_path = _write_promoted_fixture(root)
            assets_manifest_path = game_root / "manifests" / "assets_manifest.json"
            assets_manifest = json.loads(assets_manifest_path.read_text(encoding="utf-8"))
            assets_manifest["published_assets"][0]["calibration_promotion_record_path"] = str(root / "other.json")
            assets_manifest_path.write_text(json.dumps(assets_manifest, indent=2) + "\n", encoding="utf-8")
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ):
                result = rollback_revised_crop_published_pack_promotion(
                    record_path,
                    backup_path=backup_path,
                    rolled_back_by="tj",
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "asset_record_path_mismatch")

    def test_rollback_restores_backup_removes_provenance_and_validates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, game_root, backup_path = _write_promoted_fixture(root)
            template_path = game_root / "templates" / "team_wipes" / "ace.png"
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ), patch(
                "tools.detector_calibration_published_pack_promotion.validate_published_pack",
                return_value={"ok": True, "status": "ok"},
            ):
                result = rollback_revised_crop_published_pack_promotion(
                    record_path,
                    backup_path=backup_path,
                    rolled_back_by="tj",
                )

            self.assertTrue(result["ok"])
            self.assertEqual(template_path.read_bytes(), b"published-template")
            assets_manifest = json.loads((game_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
            asset_row = assets_manifest["published_assets"][0]
            for key in (
                "calibration_promotion_source",
                "calibration_promotion_record_path",
                "calibration_promotion_session_root",
                "calibration_promotion_candidate_id",
                "calibration_promotion_run_id",
                "calibration_promotion_revised_crop_path",
                "calibration_promotion_approved_by",
                "calibration_promotion_promoted_at",
                "calibration_promotion_promoted_by",
            ):
                self.assertNotIn(key, asset_row)
            cv_templates = load_yaml_file(game_root / "manifests" / "cv_templates.yaml")
            template_row = cv_templates["templates"][0]
            for key in (
                "calibration_promotion_record_path",
                "calibration_promotion_run_id",
                "calibration_promotion_promoted_at",
                "calibration_promotion_promoted_by",
            ):
                self.assertNotIn(key, template_row)
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ), patch(
                "tools.detector_calibration_published_pack_promotion.validate_published_pack",
                return_value={"ok": True, "status": "ok"},
            ):
                validation = validate_rolled_back_calibration_promotion(
                    promotion_record_path=record_path,
                    backup_path=backup_path,
                )
            self.assertTrue(validation["ok"])

    def test_cli_validate_rollback_routes_to_rollback_validator(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            record_path, _, backup_path = _write_promoted_fixture(root)
            stdout = io.StringIO()
            with patch("tools.detector_calibration_published_pack_promotion.REPO_ROOT", root), patch(
                "tools.detector_calibration_crop_promotion.REPO_ROOT",
                root,
            ), patch(
                "tools.detector_calibration_published_pack_promotion.validate_rolled_back_calibration_promotion",
                return_value={"ok": True, "status": "ok", "published_template_path": "/tmp/ace.png", "file_hash": "sha256:test", "pack_validation_status": "ok"},
            ), redirect_stdout(stdout):
                code = published_promotion_main(
                    [
                        "validate-rollback",
                        "--promotion-record-path",
                        str(record_path),
                        "--backup-path",
                        str(backup_path),
                    ]
                )
            self.assertEqual(code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])


def _write_promotion_fixture(root: Path) -> tuple[Path, Path]:
    game_root = root / "assets" / "games" / "marvel_rivals"
    manifests_root = game_root / "manifests"
    templates_root = game_root / "templates" / "team_wipes"
    masters_root = game_root / "masters" / "team_wipes"
    manifests_root.mkdir(parents=True, exist_ok=True)
    templates_root.mkdir(parents=True, exist_ok=True)
    masters_root.mkdir(parents=True, exist_ok=True)
    published_template_path = templates_root / "ace.png"
    published_template_path.write_bytes(b"published-template")
    (masters_root / "ace.png").write_bytes(b"published-master")
    dump_yaml_file(
        manifests_root / "cv_templates.yaml",
        {
            "templates": [
                {
                    "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                    "game_id": "marvel_rivals",
                    "asset_family": "team_wipe_announcement",
                    "display_name": "Ace Team Wipe Announcement",
                    "template_path": "templates/team_wipes/ace.png",
                    "mask_path": "",
                    "roi_ref": "team_wipe_announcement",
                    "match_method": "TM_CCORR_NORMED",
                    "threshold": 0.92,
                    "scale_set": [0.9, 1.0],
                    "temporal_window": 1,
                    "source_url": "https://example.com/clip",
                    "source_page_url": "https://example.com/clip",
                    "source_kind": "public_gameplay_clip_frame",
                    "source_license_note": "note",
                    "license_note": "note",
                    "patch_tag": "2026-04-launch",
                    "file_hash": _sha256(published_template_path),
                    "qa_status": "verified",
                    "binding_status": "accepted",
                    "event_row_id": "ace",
                }
            ]
        },
    )
    assets_manifest = {
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
                "source_url": "https://example.com/clip",
                "source_page_url": "https://example.com/clip",
                "source_kind": "public_gameplay_clip_frame",
                "source_license_note": "note",
                "license_note": "note",
                "patch_tag": "2026-04-launch",
                "file_hash": _sha256(published_template_path),
                "qa_status": "verified",
                "detection_id": "marvel_rivals.ace.team_wipe_announcement",
            }
        ],
    }
    (manifests_root / "assets_manifest.json").write_text(json.dumps(assets_manifest, indent=2) + "\n", encoding="utf-8")

    revised_crop = root / "drafts" / "detector_calibration_promotions" / "20260513T225849Z-team-wipe-announcement" / "templates" / "team_wipes" / "ace.png"
    revised_crop.parent.mkdir(parents=True, exist_ok=True)
    revised_crop.write_bytes(b"revised-template-bytes")
    promotion_record_path = root / "drafts" / "detector_calibration_promotions" / "20260513T225849Z-team-wipe-announcement" / "manifests" / "revised_crop_promotion.json"
    promotion_record_path.parent.mkdir(parents=True, exist_ok=True)
    promotion_record_path.write_text(
        json.dumps(
            {
                "schema_version": "detector_calibration_revised_crop_promotion_v1",
                "promotion_id": "20260513T225849Z-team-wipe-announcement",
                "promotion_root": str(promotion_record_path.parents[1]),
                "created_at": "2026-05-13T22:58:49Z",
                "game": "marvel_rivals",
                "operator_approval": {
                    "approved_by": "tj",
                    "notes": None,
                },
                "source_evidence": {
                    "session_root": str(root / "outputs" / "detector_calibration" / "marvel_rivals" / "session-001"),
                    "review_record_path": str(root / "outputs" / "detector_calibration" / "marvel_rivals" / "session-001" / "review_record.json"),
                    "candidate_id": "rev-002",
                    "run_id": "replay-003",
                    "replay_result_path": str(root / "outputs" / "detector_calibration" / "marvel_rivals" / "session-001" / "replay_results" / "replay-003" / "replay_result.json"),
                    "source": str(root / "clip.mp4"),
                    "runtime_sidecar_path": str(root / "runtime.runtime_analysis.json"),
                    "fused_sidecar_path": None,
                    "proxy_sidecar_path": None,
                    "event_type": "team_wipe_seen",
                    "event_row_id": "ace",
                },
                "published_asset": {
                    "asset_id": "marvel_rivals.ace.team_wipe_announcement",
                    "candidate_id": "candidate_manual_ace_team_wipe_announcement_20260512",
                    "detection_id": "marvel_rivals.ace.team_wipe_announcement",
                    "asset_family": "team_wipe_announcement",
                    "roi_ref": "team_wipe_announcement",
                    "template_path": str(published_template_path.resolve()),
                    "master_path": str((masters_root / "ace.png").resolve()),
                },
                "revised_crop": {
                    "candidate_id": "rev-002",
                    "crop_png_path": str(revised_crop.resolve()),
                    "crop": {"x": 557, "y": 191, "w": 829, "h": 175},
                    "template_comparison": {},
                },
                "replay_evidence": {
                    "trial_runtime_sidecar_path": str(root / "trial.runtime_analysis.json"),
                    "trial_fused_sidecar_path": None,
                    "current_runtime_sidecar_path": str(root / "current.runtime_analysis.json"),
                    "current_fused_sidecar_path": None,
                    "eligibility": {
                        "ok": True,
                        "status": "ok",
                        "candidate_id": "rev-002",
                        "run_id": "replay-003",
                        "replay_result_path": str(root / "outputs" / "detector_calibration" / "marvel_rivals" / "session-001" / "replay_results" / "replay-003" / "replay_result.json"),
                        "primary_source": "replay_run",
                        "secondary_source": "crop_candidate",
                        "primary_reference_source": "localized_match",
                        "primary_reference_crop": "557,191,829,175",
                        "secondary_reference_source": "roi_fallback",
                        "secondary_reference_crop": "499,172,921,194",
                        "primary_iou": 1.0,
                        "secondary_iou": 0.811954,
                        "delta_iou": 0.188046,
                        "difference_summary": "Replay changed overlap source from roi_fallback to localized_match and changed IoU from 0.811954 to 1.0.",
                    },
                },
                "draft_update": {
                    "template_path": str(revised_crop.resolve()),
                    "template_relpath": "templates/team_wipes/ace.png",
                    "copied_from_revised_crop_path": str(revised_crop.resolve()),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return promotion_record_path, game_root


def _write_promoted_fixture(root: Path) -> tuple[Path, Path, Path]:
    record_path, game_root = _write_promotion_fixture(root)
    template_path = game_root / "templates" / "team_wipes" / "ace.png"
    backup_path = game_root / "templates" / "team_wipes" / "ace.calibration-backup-20260513T230510Z-team_wipe_announcement.png"
    backup_path.write_bytes(b"published-template")
    template_path.write_bytes(b"revised-template-bytes")
    promoted_hash = _sha256(template_path)

    assets_manifest_path = game_root / "manifests" / "assets_manifest.json"
    assets_manifest = json.loads(assets_manifest_path.read_text(encoding="utf-8"))
    assets_manifest["published_assets"][0]["file_hash"] = promoted_hash
    assets_manifest["published_assets"][0].update(
        {
            "calibration_promotion_source": "detector_calibration_revised_crop_promotion_v1",
            "calibration_promotion_record_path": str(record_path.resolve()),
            "calibration_promotion_session_root": str(root / "outputs" / "detector_calibration" / "marvel_rivals" / "session-001"),
            "calibration_promotion_candidate_id": "rev-002",
            "calibration_promotion_run_id": "replay-003",
            "calibration_promotion_revised_crop_path": str((root / "drafts" / "detector_calibration_promotions" / "20260513T225849Z-team-wipe-announcement" / "templates" / "team_wipes" / "ace.png").resolve()),
            "calibration_promotion_approved_by": "tj",
            "calibration_promotion_promoted_at": "2026-05-13T23:05:10Z",
            "calibration_promotion_promoted_by": "tj",
        }
    )
    assets_manifest_path.write_text(json.dumps(assets_manifest, indent=2) + "\n", encoding="utf-8")

    cv_templates_path = game_root / "manifests" / "cv_templates.yaml"
    cv_templates = load_yaml_file(cv_templates_path)
    cv_templates["templates"][0]["file_hash"] = promoted_hash
    cv_templates["templates"][0].update(
        {
            "calibration_promotion_record_path": str(record_path.resolve()),
            "calibration_promotion_run_id": "replay-003",
            "calibration_promotion_promoted_at": "2026-05-13T23:05:10Z",
            "calibration_promotion_promoted_by": "tj",
        }
    )
    dump_yaml_file(cv_templates_path, cv_templates)
    return record_path, game_root, backup_path


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return f"sha256:{digest.hexdigest()}"
