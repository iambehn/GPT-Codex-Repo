from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_detector_calibration_next_action_apply_ledger import (
    inspect_detector_calibration_next_action_apply_ledger,
    main as inspect_main,
)


def _ledger_payload(*, rows: list[dict] | None = None) -> dict:
    rows = rows or []
    return {
        "schema_version": "detector_calibration_next_action_apply_ledger_v1",
        "game": "marvel_rivals",
        "updated_at": "2026-05-15T21:44:56.138925Z",
        "row_count": len(rows),
        "rows": rows,
    }


def _ledger_row(
    *,
    run_id: str = "20260515T214456Z",
    status: str = "ok",
    ok: bool = True,
    selected_row_count: int = 1,
    applied_row_count: int = 1,
    created_count: int = 0,
    reused_count: int = 1,
    failed_count: int = 0,
    top_asset_id: str | None = "marvel_rivals.human_torch.hero_portrait",
) -> dict:
    return {
        "run_id": run_id,
        "recorded_at": "2026-05-15T21:44:56.138925Z",
        "source_next_actions_manifest_path": "/tmp/next-actions.json",
        "status": status,
        "ok": ok,
        "selected_row_count": selected_row_count,
        "applied_row_count": applied_row_count,
        "created_count": created_count,
        "reused_count": reused_count,
        "failed_count": failed_count,
        "top_asset_id": top_asset_id,
        "failed_asset_ids": [],
        "top_expansion_manifest_path": "/tmp/expansion.json",
    }


class InspectDetectorCalibrationNextActionApplyLedgerTests(unittest.TestCase):
    def test_empty_ledger_renders_explicit_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps(_ledger_payload(), indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_apply_ledger(ledger=ledger_path)
            self.assertTrue(result["ok"])
            self.assertIn("Latest run", result["rendered_output"])
            self.assertIn("None", result["rendered_output"])
            self.assertIn("Runs", result["rendered_output"])

    def test_non_empty_ledger_renders_expected_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            payload = _ledger_payload(
                rows=[
                    _ledger_row(run_id="20260515T214455Z", top_asset_id="marvel_rivals.ace.team_wipe_announcement"),
                    _ledger_row(run_id="20260515T214456Z"),
                ]
            )
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_apply_ledger(ledger=ledger_path)
            self.assertIn("Game: marvel_rivals", result["rendered_output"])
            self.assertIn("Row count: 2", result["rendered_output"])
            self.assertIn("Run id: 20260515T214456Z", result["rendered_output"])
            self.assertIn("Top asset id: marvel_rivals.human_torch.hero_portrait", result["rendered_output"])
            self.assertIn(
                "- 20260515T214455Z | ok | applied/selected=1/1 | top_asset_id=marvel_rivals.ace.team_wipe_announcement",
                result["rendered_output"],
            )
            self.assertIn(
                "- 20260515T214456Z | ok | applied/selected=1/1 | top_asset_id=marvel_rivals.human_torch.hero_portrait",
                result["rendered_output"],
            )

    def test_json_mode_returns_original_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            payload = _ledger_payload(rows=[_ledger_row()])
            ledger_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            result = inspect_detector_calibration_next_action_apply_ledger(
                ledger=ledger_path,
                emit_json=True,
            )
            self.assertEqual(json.loads(result["rendered_output"]), payload)

    def test_invalid_ledger_shape_fails_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "ledger missing required fields"):
                inspect_detector_calibration_next_action_apply_ledger(ledger=ledger_path)

    def test_main_returns_error_code_for_invalid_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ledger_path = root / "ledger.json"
            ledger_path.write_text(json.dumps({"game": "marvel_rivals"}, indent=2), encoding="utf-8")
            exit_code = inspect_main(["--ledger", str(ledger_path)])
            self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
