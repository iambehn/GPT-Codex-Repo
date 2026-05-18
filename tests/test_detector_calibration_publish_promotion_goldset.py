from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.test_detector_calibration_publish_decision_manifest import (
    _promotion_record,
    _triage_manifest,
    _triage_row,
    _write_json,
)
from tests.test_detector_calibration_published_pack_promotion import (
    _write_promoted_fixture,
    _write_promotion_fixture,
)
from tools.detector_calibration_publish_decision_manifest import generate_detector_calibration_publish_decision_manifest
from tools.detector_calibration_published_pack_promotion import (
    promote_revised_crop_to_published_pack,
    rollback_revised_crop_published_pack_promotion,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "detector_calibration_publish_promotion_goldsets"
    / "detector_calibration_publish_promotion_goldset.json"
)


class DetectorCalibrationPublishPromotionGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "detector_calibration_publish_promotion_goldset_v1")
        self.assertGreaterEqual(len(payload["cases"]), 4)

    def test_detector_calibration_publish_and_promotion_cases_match_expectations(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                scenario = str(case["scenario"])
                if scenario == "publish_decision":
                    self._assert_publish_decision_case(case)
                elif scenario == "promotion":
                    self._assert_promotion_case(case)
                elif scenario == "rollback":
                    self._assert_rollback_case(case)
                else:
                    self.fail(f"unsupported scenario: {scenario}")

    def _assert_publish_decision_case(self, case: dict[str, object]) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            asset_id = str(case["asset_id"])
            rows = []
            for row in list(case.get("triage_rows", [])):
                promotion_id = str(row["promotion_id"])
                promotion_path = _write_json(
                    root / "promotions" / promotion_id / "manifests" / "revised_crop_promotion.json",
                    _promotion_record(
                        promotion_id=promotion_id,
                        asset_id=asset_id,
                        source=row.get("source"),
                    ),
                )
                rows.append(
                    _triage_row(
                        status=str(row["status"]),
                        asset_id=asset_id,
                        promotion_record_path=str(promotion_path),
                        promotion_id=promotion_id,
                        delta_iou=float(row["delta_iou"]),
                        created_at=str(row["created_at"]),
                    )
                )
            triage_path = _write_json(root / "triage.json", _triage_manifest(rows=rows))
            result = generate_detector_calibration_publish_decision_manifest(
                triage_manifest=triage_path,
                output_path=root / "decision.json",
            )
            self.assertTrue(result["ok"])
            self.assertEqual([row["decision_status"] for row in result["rows"]], list(case["expected_statuses"]))
            if "expected_replay_count" in case:
                self.assertEqual(result["rows"][0]["replay_count_for_asset"], case["expected_replay_count"])
            if "expected_distinct_source_count" in case:
                self.assertEqual(result["rows"][0]["distinct_source_count_for_asset"], case["expected_distinct_source_count"])
            if "expected_reason" in case:
                self.assertEqual(result["rows"][0]["decision_reason"], case["expected_reason"])

    def _assert_promotion_case(self, case: dict[str, object]) -> None:
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
                result = promote_revised_crop_to_published_pack(record_path, promoted_by=str(case["promoted_by"]))

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], case["expected_status"])
            self.assertEqual(template_path.read_bytes(), b"revised-template-bytes")
            assets_manifest = json.loads((game_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
            asset_row = assets_manifest["published_assets"][0]
            self.assertEqual(asset_row["calibration_promotion_source"], case["expected_source"])
            self.assertEqual(asset_row["calibration_promotion_run_id"], case["expected_run_id"])

    def _assert_rollback_case(self, case: dict[str, object]) -> None:
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
                    rolled_back_by=str(case["rolled_back_by"]),
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], case["expected_status"])
            self.assertEqual(template_path.read_bytes(), b"published-template")


if __name__ == "__main__":
    unittest.main()
