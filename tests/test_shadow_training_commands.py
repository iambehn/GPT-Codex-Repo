from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from pipeline.commands.shadow_training import dispatch_shadow_training_commands


class _ParserStub:
    def error(self, message: str) -> None:
        raise RuntimeError(message)


def _base_args() -> SimpleNamespace:
    return SimpleNamespace(
        run_shadow_ranking_replay=False,
        compare_shadow_ranking_replay=None,
        train_shadow_ranking_model=False,
        compare_shadow_model_families=None,
        run_shadow_benchmark_matrix=False,
        summarize_shadow_benchmark_matrix=None,
        review_shadow_benchmark_results=None,
        compare_shadow_benchmark_evidence_modes=None,
        summarize_shadow_target_readiness=None,
        evaluate_shadow_ranking_model=False,
        evaluate_shadow_experiment_policy=False,
        run_shadow_operator=False,
        summarize_shadow_experiment_ledger=False,
        dataset_manifest=None,
        model_path=None,
        model_family=None,
        model_version=None,
        output_path=None,
        game=None,
        fixture_id=None,
        candidate_id=None,
        platform=None,
        full_json=False,
        model_output_path=None,
        training_target=None,
        split_key=None,
        train_fraction=None,
        policy_path=None,
        recommendation_decision=None,
        registry_path=None,
        target=None,
        experiment_manifest=None,
        mode=None,
        output_root=None,
    )


def _dispatch(args: SimpleNamespace) -> int | None:
    return dispatch_shadow_training_commands(
        args,
        parser=_ParserStub(),
        print_cli_result_fn=lambda result, **kwargs: print(json.dumps({"result": result, "kwargs": kwargs}, indent=2)),
        run_run_shadow_ranking_replay_fn=lambda *a, **k: {"ok": True, "status": "ok", "kwargs": k},
        run_compare_shadow_ranking_replay_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_train_shadow_ranking_model_fn=lambda *a, **k: {"ok": True, "status": "ok", "kwargs": k},
        run_compare_shadow_model_families_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_run_shadow_benchmark_matrix_fn=lambda *a, **k: {"ok": True, "status": "ok", "kwargs": k},
        run_summarize_shadow_benchmark_matrix_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_review_shadow_benchmark_results_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_compare_shadow_benchmark_evidence_modes_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_summarize_shadow_target_readiness_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_evaluate_shadow_ranking_model_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_evaluate_shadow_experiment_policy_fn=lambda *a, **k: {"ok": True, "status": "ok"},
        run_run_shadow_operator_fn=lambda *a, **k: {"ok": True, "status": "ok", "kwargs": k},
        run_summarize_shadow_experiment_ledger_fn=lambda *a, **k: {"ok": True, "status": "ok"},
    )


class ShadowTrainingCommandTests(unittest.TestCase):
    def test_run_shadow_ranking_replay_requires_dataset_manifest(self) -> None:
        args = _base_args()
        args.run_shadow_ranking_replay = True
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args)
        self.assertEqual(str(ctx.exception), "--run-shadow-ranking-replay requires --dataset-manifest")

    def test_train_shadow_ranking_model_prints_json_and_uses_defaults(self) -> None:
        args = _base_args()
        args.train_shadow_ranking_model = True
        args.dataset_manifest = "/tmp/dataset.manifest.json"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = _dispatch(args)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["kwargs"]["model_family"], "linear_shadow_ranker")
        self.assertEqual(payload["kwargs"]["training_target"], "approved_or_selected_probability")
        self.assertEqual(payload["kwargs"]["split_key"], "fixture_id")
        self.assertEqual(payload["kwargs"]["train_fraction"], 0.8)

    def test_run_shadow_operator_requires_experiment_manifest_for_govern_mode(self) -> None:
        args = _base_args()
        args.run_shadow_operator = True
        args.mode = "govern"
        with self.assertRaises(RuntimeError) as ctx:
            _dispatch(args)
        self.assertEqual(str(ctx.exception), "--run-shadow-operator with --mode govern requires --experiment-manifest")

    def test_summarize_shadow_benchmark_matrix_normalizes_empty_string_to_none(self) -> None:
        args = _base_args()
        args.summarize_shadow_benchmark_matrix = ""
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = dispatch_shadow_training_commands(
                args,
                parser=_ParserStub(),
                print_cli_result_fn=lambda result, **kwargs: print(json.dumps(result)),
                run_run_shadow_ranking_replay_fn=lambda *a, **k: {"ok": True},
                run_compare_shadow_ranking_replay_fn=lambda *a, **k: {"ok": True},
                run_train_shadow_ranking_model_fn=lambda *a, **k: {"ok": True},
                run_compare_shadow_model_families_fn=lambda *a, **k: {"ok": True},
                run_run_shadow_benchmark_matrix_fn=lambda *a, **k: {"ok": True},
                run_summarize_shadow_benchmark_matrix_fn=lambda manifest, **kwargs: {"ok": True, "manifest": manifest, "kwargs": kwargs},
                run_review_shadow_benchmark_results_fn=lambda *a, **k: {"ok": True},
                run_compare_shadow_benchmark_evidence_modes_fn=lambda *a, **k: {"ok": True},
                run_summarize_shadow_target_readiness_fn=lambda *a, **k: {"ok": True},
                run_evaluate_shadow_ranking_model_fn=lambda *a, **k: {"ok": True},
                run_evaluate_shadow_experiment_policy_fn=lambda *a, **k: {"ok": True},
                run_run_shadow_operator_fn=lambda *a, **k: {"ok": True},
                run_summarize_shadow_experiment_ledger_fn=lambda *a, **k: {"ok": True},
            )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertIsNone(payload["manifest"])

    def test_returns_none_when_no_route_matches(self) -> None:
        self.assertIsNone(_dispatch(_base_args()))
