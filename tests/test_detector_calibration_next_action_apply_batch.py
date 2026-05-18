from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools.detector_calibration_next_action_apply_batch import (
    apply_detector_calibration_next_actions_batch,
)


def _next_actions_row(
    *,
    asset_id: str,
    action_status: str = "collect_more_evidence",
) -> dict:
    return {
        "asset_id": asset_id,
        "action_status": action_status,
        "action_reason": "progress_row_requires_broader_evidence",
        "progress_status": "actively_collecting",
        "queue_status": "in_progress",
        "decision_status": "needs_broader_evidence",
        "triage_status": "publish_ready",
        "remaining_replay_gap": 1,
        "remaining_distinct_source_gap": 1,
        "linked_session_count": 1,
        "linked_promotion_count": 0,
        "recommended_action_note": "Need more evidence.",
    }


def _next_actions_manifest(*, rows: list[dict]) -> dict:
    return {
        "schema_version": "detector_calibration_next_actions_manifest_v1",
        "game": "marvel_rivals",
        "generated_at": "2026-05-15T01:00:00+00:00",
        "source_progress_manifest_path": "/tmp/progress.json",
        "row_count": len(rows),
        "status_counts": {
            "review_for_publish": sum(1 for row in rows if row.get("action_status") == "review_for_publish"),
            "collect_more_evidence": sum(1 for row in rows if row.get("action_status") == "collect_more_evidence"),
            "investigate_state_gap": sum(1 for row in rows if row.get("action_status") == "investigate_state_gap"),
        },
        "rows": rows,
    }


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


class DetectorCalibrationNextActionApplyBatchTests(unittest.TestCase):
    def test_all_rows_reused(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(
                    rows=[
                        _next_actions_row(asset_id="asset.a"),
                        _next_actions_row(asset_id="asset.b"),
                    ]
                ),
            )
            with mock.patch(
                "tools.detector_calibration_next_action_apply_batch.apply_detector_calibration_next_action",
                side_effect=[
                    {
                        "ok": True,
                        "status": "ok",
                        "asset_id": "asset.a",
                        "action_status": "collect_more_evidence",
                        "applied_action": "reuse_existing_expansion",
                        "expansion_manifest_path": "/tmp/a.json",
                        "reuse_mode": "reused_existing",
                    },
                    {
                        "ok": True,
                        "status": "ok",
                        "asset_id": "asset.b",
                        "action_status": "collect_more_evidence",
                        "applied_action": "reuse_existing_expansion",
                        "expansion_manifest_path": "/tmp/b.json",
                        "reuse_mode": "reused_existing",
                    },
                ],
            ) as mock_apply:
                result = apply_detector_calibration_next_actions_batch(next_actions_manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["selected_row_count"], 2)
            self.assertEqual(result["applied_row_count"], 2)
            self.assertEqual(result["reused_count"], 2)
            self.assertEqual(result["created_count"], 0)
            self.assertEqual(result["failed_count"], 0)
            self.assertEqual([row["row_index"] for row in result["results"]], [0, 1])
            self.assertEqual(result["emitted_batch"]["selected_row_count"], 2)
            self.assertEqual(
                result["emitted_batch"]["top_result"],
                {
                    "asset_id": "asset.a",
                    "applied_action": "reuse_existing_expansion",
                    "reuse_mode": "reused_existing",
                    "expansion_manifest_path": "/tmp/a.json",
                },
            )
            self.assertEqual(result["emitted_batch"]["failed_asset_ids"], [])
            self.assertEqual(mock_apply.call_count, 2)

    def test_mixed_reused_and_created_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(
                    rows=[
                        _next_actions_row(asset_id="asset.a"),
                        _next_actions_row(asset_id="asset.b"),
                    ]
                ),
            )
            with mock.patch(
                "tools.detector_calibration_next_action_apply_batch.apply_detector_calibration_next_action",
                side_effect=[
                    {
                        "ok": True,
                        "status": "ok",
                        "asset_id": "asset.a",
                        "action_status": "collect_more_evidence",
                        "applied_action": "create_new_expansion",
                        "expansion_manifest_path": "/tmp/a.json",
                        "reuse_mode": "created_new",
                    },
                    {
                        "ok": True,
                        "status": "ok",
                        "asset_id": "asset.b",
                        "action_status": "collect_more_evidence",
                        "applied_action": "reuse_existing_expansion",
                        "expansion_manifest_path": "/tmp/b.json",
                        "reuse_mode": "reused_existing",
                    },
                ],
            ):
                result = apply_detector_calibration_next_actions_batch(next_actions_manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["created_count"], 1)
            self.assertEqual(result["reused_count"], 1)
            self.assertEqual(result["applied_row_count"], 2)
            self.assertEqual(result["emitted_batch"]["top_result"]["asset_id"], "asset.a")

    def test_per_row_failure_continues_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(
                    rows=[
                        _next_actions_row(asset_id="asset.a"),
                        _next_actions_row(asset_id="asset.b"),
                    ]
                ),
            )
            with mock.patch(
                "tools.detector_calibration_next_action_apply_batch.apply_detector_calibration_next_action",
                side_effect=[
                    {
                        "ok": False,
                        "status": "stale_publish_decision_status",
                        "asset_id": "asset.a",
                        "action_status": "collect_more_evidence",
                    },
                    {
                        "ok": True,
                        "status": "ok",
                        "asset_id": "asset.b",
                        "action_status": "collect_more_evidence",
                        "applied_action": "reuse_existing_expansion",
                        "expansion_manifest_path": "/tmp/b.json",
                        "reuse_mode": "reused_existing",
                    },
                ],
            ):
                result = apply_detector_calibration_next_actions_batch(next_actions_manifest=manifest_path)
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "partial_failure")
            self.assertEqual(result["failed_count"], 1)
            self.assertEqual(result["applied_row_count"], 1)
            self.assertEqual(result["results"][0]["status"], "stale_publish_decision_status")
            self.assertEqual(result["results"][1]["status"], "ok")
            self.assertEqual(result["emitted_batch"]["failed_asset_ids"], ["asset.a"])
            self.assertEqual(
                result["emitted_batch"]["top_result"],
                {
                    "asset_id": "asset.b",
                    "applied_action": "reuse_existing_expansion",
                    "reuse_mode": "reused_existing",
                    "expansion_manifest_path": "/tmp/b.json",
                },
            )

    def test_no_actionable_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(
                    rows=[_next_actions_row(asset_id="asset.a", action_status="review_for_publish")]
                ),
            )
            result = apply_detector_calibration_next_actions_batch(next_actions_manifest=manifest_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "no_actionable_rows")
            self.assertEqual(result["selected_row_count"], 0)
            self.assertEqual(result["results"], [])
            self.assertIsNone(result["emitted_batch"]["top_result"])
            self.assertEqual(result["emitted_batch"]["failed_asset_ids"], [])

    def test_invalid_next_actions_manifest_aborts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = root / "next-actions.json"
            manifest_path.write_text("{not-json", encoding="utf-8")
            result = apply_detector_calibration_next_actions_batch(next_actions_manifest=manifest_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_next_actions_manifest")
            self.assertEqual(result["selected_row_count"], 0)
            self.assertIsNone(result["emitted_batch"]["top_result"])
            self.assertEqual(result["emitted_batch"]["failed_asset_ids"], [])

    def test_all_failed_selected_rows_emit_failed_asset_ids_without_top_result(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(
                    rows=[
                        _next_actions_row(asset_id="asset.a"),
                        _next_actions_row(asset_id="asset.b"),
                    ]
                ),
            )
            with mock.patch(
                "tools.detector_calibration_next_action_apply_batch.apply_detector_calibration_next_action",
                side_effect=[
                    {
                        "ok": False,
                        "status": "stale_publish_decision_status",
                        "asset_id": "asset.a",
                        "action_status": "collect_more_evidence",
                    },
                    {
                        "ok": False,
                        "status": "stale_publish_decision_status",
                        "asset_id": "asset.b",
                        "action_status": "collect_more_evidence",
                    },
                ],
            ):
                result = apply_detector_calibration_next_actions_batch(next_actions_manifest=manifest_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "partial_failure")
            self.assertEqual(result["failed_count"], 2)
            self.assertIsNone(result["emitted_batch"]["top_result"])
            self.assertEqual(result["emitted_batch"]["failed_asset_ids"], ["asset.a", "asset.b"])

    def test_record_ledger_creates_new_ledger_with_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(rows=[_next_actions_row(asset_id="asset.a")]),
            )
            ledger_path = root / "ledger.json"
            with mock.patch(
                "tools.detector_calibration_next_action_apply_batch.apply_detector_calibration_next_action",
                return_value={
                    "ok": True,
                    "status": "ok",
                    "asset_id": "asset.a",
                    "action_status": "collect_more_evidence",
                    "applied_action": "reuse_existing_expansion",
                    "expansion_manifest_path": "/tmp/a.json",
                    "reuse_mode": "reused_existing",
                },
            ):
                result = apply_detector_calibration_next_actions_batch(
                    next_actions_manifest=manifest_path,
                    record_ledger=True,
                    ledger_path=ledger_path,
                )
            self.assertTrue(result["ok"])
            self.assertTrue(result["ledger_recorded"])
            self.assertEqual(result["ledger_path"], str(ledger_path.resolve()))
            self.assertTrue(result["ledger_run_id"])
            payload = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], "detector_calibration_next_action_apply_ledger_v1")
            self.assertEqual(payload["row_count"], 1)
            self.assertEqual(payload["rows"][0]["top_asset_id"], "asset.a")
            self.assertEqual(payload["rows"][0]["top_expansion_manifest_path"], "/tmp/a.json")

    def test_record_ledger_appends_second_row(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(rows=[_next_actions_row(asset_id="asset.a")]),
            )
            ledger_path = root / "ledger.json"
            with mock.patch(
                "tools.detector_calibration_next_action_apply_batch.apply_detector_calibration_next_action",
                return_value={
                    "ok": True,
                    "status": "ok",
                    "asset_id": "asset.a",
                    "action_status": "collect_more_evidence",
                    "applied_action": "reuse_existing_expansion",
                    "expansion_manifest_path": "/tmp/a.json",
                    "reuse_mode": "reused_existing",
                },
            ):
                first = apply_detector_calibration_next_actions_batch(
                    next_actions_manifest=manifest_path,
                    record_ledger=True,
                    ledger_path=ledger_path,
                )
                second = apply_detector_calibration_next_actions_batch(
                    next_actions_manifest=manifest_path,
                    record_ledger=True,
                    ledger_path=ledger_path,
                )
            payload = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["row_count"], 2)
            self.assertEqual(len(payload["rows"]), 2)
            self.assertEqual(payload["rows"][0]["run_id"], first["ledger_run_id"])
            self.assertEqual(payload["rows"][1]["run_id"], second["ledger_run_id"])

    def test_ledger_write_failure_returns_ledger_record_failed_and_preserves_batch_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            manifest_path = _write_json(
                root / "next-actions.json",
                _next_actions_manifest(rows=[_next_actions_row(asset_id="asset.a")]),
            )
            ledger_parent = root / "blocked"
            ledger_parent.write_text("not-a-directory", encoding="utf-8")
            ledger_path = ledger_parent / "ledger.json"
            with mock.patch(
                "tools.detector_calibration_next_action_apply_batch.apply_detector_calibration_next_action",
                return_value={
                    "ok": True,
                    "status": "ok",
                    "asset_id": "asset.a",
                    "action_status": "collect_more_evidence",
                    "applied_action": "reuse_existing_expansion",
                    "expansion_manifest_path": "/tmp/a.json",
                    "reuse_mode": "reused_existing",
                },
            ):
                result = apply_detector_calibration_next_actions_batch(
                    next_actions_manifest=manifest_path,
                    record_ledger=True,
                    ledger_path=ledger_path,
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "ledger_record_failed")
            self.assertEqual(result["selected_row_count"], 1)
            self.assertEqual(result["applied_row_count"], 1)
            self.assertEqual(result["reused_count"], 1)
            self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
