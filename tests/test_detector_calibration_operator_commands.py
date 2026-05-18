from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from pipeline.commands.detector_calibration_operator import dispatch_detector_calibration_operator_commands


def _base_args() -> SimpleNamespace:
    return SimpleNamespace(
        inspect_detector_calibration_followup_report=None,
        inspect_detector_calibration_promotion_triage_manifest=None,
        inspect_detector_calibration_publish_decision_manifest=None,
        inspect_detector_calibration_evidence_expansion_queue_manifest=None,
        inspect_detector_calibration_evidence_expansion_progress_manifest=None,
        inspect_detector_calibration_next_actions_manifest=None,
        inspect_detector_calibration_next_action_apply_ledger=None,
        inspect_detector_calibration_next_action_apply_history_summary=None,
        inspect_detector_calibration_next_action_apply_history_trend=None,
        inspect_detector_calibration_next_action_cross_game_ledger_comparison=None,
        inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger=None,
        inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary=None,
        apply_detector_calibration_next_action=None,
        apply_detector_calibration_next_actions=None,
        asset_id=None,
        json=False,
    )


def _dispatch(args: SimpleNamespace) -> int | None:
    return dispatch_detector_calibration_operator_commands(
        args,
        run_inspect_detector_calibration_followup_report_fn=lambda *a, **k: {"ok": True, "rendered_output": "followup ok"},
        run_inspect_detector_calibration_promotion_triage_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "triage ok"},
        run_inspect_detector_calibration_publish_decision_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "publish ok"},
        run_inspect_detector_calibration_evidence_expansion_queue_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "queue ok"},
        run_inspect_detector_calibration_evidence_expansion_progress_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "progress ok"},
        run_inspect_detector_calibration_next_actions_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "next actions ok"},
        run_inspect_detector_calibration_next_action_apply_ledger_fn=lambda *a, **k: {"ok": True, "rendered_output": "apply ledger ok"},
        run_inspect_detector_calibration_next_action_apply_history_summary_fn=lambda *a, **k: {"ok": True, "rendered_output": "history summary ok"},
        run_inspect_detector_calibration_next_action_apply_history_trend_fn=lambda *a, **k: {"ok": True, "rendered_output": "history trend ok"},
        run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_fn=lambda *a, **k: {"ok": True, "rendered_output": "comparison ok"},
        run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger_fn=lambda *a, **k: {"ok": True, "rendered_output": "history ledger ok"},
        run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary_fn=lambda *a, **k: {"ok": True, "rendered_output": "history summary ok"},
        run_apply_detector_calibration_next_action_fn=lambda *a, **k: {"ok": True, "status": "ok", "asset_id": k["asset_id"]},
        run_apply_detector_calibration_next_actions_batch_fn=lambda *a, **k: {"ok": True, "status": "ok", "selected_row_count": 2},
    )


class DetectorCalibrationOperatorCommandTests(unittest.TestCase):
    def test_routes_followup_inspector(self) -> None:
        args = _base_args()
        args.inspect_detector_calibration_followup_report = "/tmp/report.json"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        self.assertIn("followup ok", stdout.getvalue())

    def test_returns_error_when_inspector_raises(self) -> None:
        args = _base_args()
        args.inspect_detector_calibration_publish_decision_manifest = "/tmp/publish.json"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = dispatch_detector_calibration_operator_commands(
                args,
                run_inspect_detector_calibration_followup_report_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_promotion_triage_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_publish_decision_manifest_fn=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
                run_inspect_detector_calibration_evidence_expansion_queue_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_evidence_expansion_progress_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_next_actions_manifest_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_next_action_apply_ledger_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_next_action_apply_history_summary_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_next_action_apply_history_trend_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_ledger_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_inspect_detector_calibration_next_action_cross_game_ledger_comparison_history_summary_fn=lambda *a, **k: {"ok": True, "rendered_output": "ok"},
                run_apply_detector_calibration_next_action_fn=lambda *a, **k: {"ok": True},
                run_apply_detector_calibration_next_actions_batch_fn=lambda *a, **k: {"ok": True},
            )
        self.assertEqual(exit_code, 1)
        self.assertIn("Error: boom", stdout.getvalue())

    def test_apply_single_requires_asset_id(self) -> None:
        args = _base_args()
        args.apply_detector_calibration_next_action = "/tmp/actions.json"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 1)
        self.assertIn("--apply-detector-calibration-next-action requires --asset-id", stdout.getvalue())

    def test_apply_batch_prints_json(self) -> None:
        args = _base_args()
        args.apply_detector_calibration_next_actions = "/tmp/actions.json"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["selected_row_count"], 2)

    def test_returns_none_when_no_route_matches(self) -> None:
        self.assertIsNone(_dispatch(_base_args()))
