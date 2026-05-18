from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from pipeline.commands.review_calibration import dispatch_review_calibration_commands


class _ParserStub:
    def error(self, message: str) -> None:
        raise RuntimeError(message)


def _base_args() -> SimpleNamespace:
    return SimpleNamespace(
        calibrate_proxy_review=None,
        replay_proxy_scoring=None,
        calibrate_runtime_review=None,
        replay_runtime_scoring=None,
        validate_fusion_goldset=None,
        replay_fusion_rules=None,
        replay_template_thresholds=None,
        replay_runtime_event_rules=None,
        promote_runtime_scoring=None,
        rollback_runtime_scoring=None,
        prepare_proxy_review=None,
        prepare_runtime_review=None,
        prepare_fused_review=None,
        apply_proxy_review=None,
        apply_runtime_review=None,
        apply_fused_review=None,
        cleanup_proxy_review=None,
        cleanup_runtime_review=None,
        cleanup_fused_review=None,
        prepare_onboarding_identity_review=None,
        apply_onboarding_identity_review=None,
        cleanup_onboarding_identity_review=None,
        render_replay_viewer=None,
        render_proxy_replay_viewer=None,
        render_unified_replay_viewer=False,
        launch_highlight_review_app=None,
        game="marvel_rivals",
        output_path=None,
        min_reviewed=3,
        include_unreviewed=False,
        debug_output_dir=None,
        trial_proxy_config=None,
        trial_config=None,
        trial_rules=None,
        trial_templates=None,
        trial_runtime_rules=None,
        trial_name=None,
        media_root=None,
        sample_fps=None,
        limit_frames=None,
        proxy_sidecar_root=None,
        runtime_sidecar_root=None,
        fused_sidecar_root=None,
        sidecar_root=None,
        force=False,
        rollback_name=None,
        batch_report=None,
        action=None,
        limit=None,
        gpt_repo=None,
        session_name=None,
        event_type=None,
        fused_sidecar=None,
        proxy_sidecar=None,
        runtime_sidecar=None,
        fixture_comparison_report=None,
        fixture_trial_batch_manifest=None,
        proxy_calibration_report=None,
        proxy_replay_report=None,
        runtime_calibration_report=None,
        runtime_replay_report=None,
        registry_path=None,
        fixture_manifest=None,
        proxy_review_session_manifest=None,
        fused_review_session_manifest=None,
        json=False,
    )


def _dispatch(args: SimpleNamespace, *, phase: str = "all") -> int | None:
    return dispatch_review_calibration_commands(
        args,
        parser=_ParserStub(),
        phase=phase,
        run_calibrate_proxy_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_replay_proxy_scoring_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_calibrate_runtime_review_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_replay_runtime_scoring_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_validate_fusion_goldset_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_replay_fusion_rules_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_replay_template_thresholds_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_replay_runtime_event_rules_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_promote_runtime_scoring_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_rollback_runtime_scoring_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_prepare_proxy_review_fn=lambda *a, **k: {"ok": True, "item_count": 1},
        run_prepare_runtime_review_fn=lambda *a, **k: {"ok": True, "item_count": 1},
        run_prepare_fused_review_fn=lambda *a, **k: {"ok": True, "item_count": 1},
        run_apply_proxy_review_fn=lambda *a, **k: {"ok": True, "applied_count": 1},
        run_apply_runtime_review_fn=lambda *a, **k: {"ok": True, "applied_count": 1},
        run_apply_fused_review_fn=lambda *a, **k: {"ok": True, "applied_count": 1},
        run_cleanup_proxy_review_fn=lambda *a, **k: {"ok": True, "cleanup_count": 1},
        run_cleanup_runtime_review_fn=lambda *a, **k: {"ok": True, "cleanup_count": 1},
        run_cleanup_fused_review_fn=lambda *a, **k: {"ok": True, "cleanup_count": 1},
        run_prepare_onboarding_identity_review_fn=lambda *a, **k: {"ok": True, "item_count": 1},
        run_apply_onboarding_identity_review_fn=lambda *a, **k: {"ok": True, "resolved_count": 1},
        run_cleanup_onboarding_identity_review_fn=lambda *a, **k: {"ok": True, "cleanup_count": 1},
        run_render_replay_viewer_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_render_proxy_replay_viewer_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_render_unified_replay_viewer_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_launch_highlight_review_app_fn=lambda *a, **k: {"ok": True, "launch_url": "http://127.0.0.1:7860", "app": object()},
    )


class ReviewCalibrationCommandTests(unittest.TestCase):
    def test_pre_phase_requires_trial_proxy_config_for_proxy_replay(self) -> None:
        args = _base_args()
        args.replay_proxy_scoring = "/tmp/proxy-sidecars"
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args, phase="pre")
        self.assertEqual(str(ctx.exception), "--replay-proxy-scoring requires --trial-proxy-config")

    def test_post_phase_routes_prepare_proxy_review(self) -> None:
        args = _base_args()
        args.prepare_proxy_review = "marvel_rivals"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args, phase="post")
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["item_count"], 1)

    def test_post_phase_strips_app_from_highlight_review_output(self) -> None:
        args = _base_args()
        args.launch_highlight_review_app = "/tmp/sidecars"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args, phase="post")
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertNotIn("app", payload)
        self.assertEqual(payload["launch_url"], "http://127.0.0.1:7860")

    def test_returns_none_when_no_route_matches_phase(self) -> None:
        args = _base_args()
        self.assertIsNone(_dispatch(args, phase="post"))
