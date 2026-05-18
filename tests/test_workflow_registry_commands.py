from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from pipeline.commands.workflow_registry import dispatch_workflow_registry_commands


class _ParserStub:
    def error(self, message: str) -> None:
        raise RuntimeError(message)


def _base_args() -> SimpleNamespace:
    return SimpleNamespace(
        compare_fixture_sidecars=None,
        run_fixture_trial=None,
        compare_fixture_trials=None,
        run_fixture_trial_batch=None,
        query_workflow_queue=False,
        refresh_clip_registry=None,
        query_clip_registry=False,
        transition_candidate_lifecycle=False,
        baseline_sidecar_root=None,
        trial_sidecar_root=None,
        artifact_layer=None,
        game=None,
        output_path=None,
        fixture_source_manifest=None,
        trial_name=None,
        output_root=None,
        pattern=None,
        limit=None,
        proposal_backend=None,
        asr_backend=None,
        emit_runtime=False,
        emit_fused=False,
        baseline_run_root=None,
        trial_run_root=None,
        trial=None,
        batch_name=None,
        workflow_type=None,
        registry_path=None,
        fixture_id=None,
        debug_output_dir=None,
        mode=None,
        event_type=None,
        action=None,
        review_status=None,
        gate_status=None,
        recommendation_decision=None,
        coverage_status=None,
        has_disagreement=False,
        candidate_id=None,
        lifecycle_state=None,
        hook_archetype=None,
        hook_mode=None,
        comparison_status=None,
        export_status=None,
        post_status=None,
        platform=None,
        account_id=None,
        evidence_mode=None,
        model_family=None,
        training_target=None,
        workflow_run_id=None,
        stage=None,
        status=None,
        full_json=False,
        to_state=None,
        reason=None,
        source_artifact=None,
        actor=None,
    )


def _dispatch(args: SimpleNamespace, *, phase: str = "all") -> int | None:
    return dispatch_workflow_registry_commands(
        args,
        parser=_ParserStub(),
        phase=phase,
        print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
        run_compare_fixture_sidecars_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_fixture_trial_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_compare_fixture_trials_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_fixture_trial_batch_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_query_workflow_queue_fn=lambda *a, **k: {"ok": True, "row_count": 2, "rows": [{"id": 1}, {"id": 2}]},
        run_refresh_clip_registry_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_query_clip_registry_fn=lambda *a, **k: {"ok": True, "row_count": 2, "rows": [{"id": 1}, {"id": 2}]},
        run_transition_candidate_lifecycle_fn=lambda *a, **k: {"ok": True, "status": "ok"},
    )


class WorkflowRegistryCommandTests(unittest.TestCase):
    def test_pre_export_requires_sidecar_roots_for_fixture_comparison(self) -> None:
        args = _base_args()
        args.compare_fixture_sidecars = "/tmp/fixtures.json"
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args, phase="pre_export")
        self.assertEqual(str(ctx.exception), "--compare-fixture-sidecars requires --baseline-sidecar-root and --trial-sidecar-root")

    def test_pre_export_routes_fixture_trial_batch(self) -> None:
        args = _base_args()
        args.run_fixture_trial_batch = "/tmp/fixtures.json"
        args.fixture_source_manifest = "/tmp/source.json"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args, phase="pre_export")
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])

    def test_post_export_requires_workflow_type_for_queue_query(self) -> None:
        args = _base_args()
        args.query_workflow_queue = True
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args, phase="post_export")
        self.assertEqual(str(ctx.exception), "--query-workflow-queue requires --workflow-type")

    def test_post_export_routes_query_clip_registry_through_renderer(self) -> None:
        args = _base_args()
        args.query_clip_registry = True
        args.mode = "fused-events"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args, phase="post_export")
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["kwargs"]["command_name"], "query_clip_registry")
        self.assertTrue(payload["result"]["ok"])

    def test_returns_none_when_no_route_matches_phase(self) -> None:
        self.assertIsNone(_dispatch(_base_args(), phase="pre_export"))
