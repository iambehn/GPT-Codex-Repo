from __future__ import annotations

import argparse
import json
from typing import Any, Callable


def dispatch_shadow_training_commands(
    args: argparse.Namespace,
    *,
    parser: argparse.ArgumentParser,
    print_cli_result_fn: Callable[..., None],
    run_run_shadow_ranking_replay_fn: Callable[..., dict[str, Any]],
    run_compare_shadow_ranking_replay_fn: Callable[..., dict[str, Any]],
    run_train_shadow_ranking_model_fn: Callable[..., dict[str, Any]],
    run_compare_shadow_model_families_fn: Callable[..., dict[str, Any]],
    run_run_shadow_benchmark_matrix_fn: Callable[..., dict[str, Any]],
    run_summarize_shadow_benchmark_matrix_fn: Callable[..., dict[str, Any]],
    run_review_shadow_benchmark_results_fn: Callable[..., dict[str, Any]],
    run_compare_shadow_benchmark_evidence_modes_fn: Callable[..., dict[str, Any]],
    run_summarize_shadow_target_readiness_fn: Callable[..., dict[str, Any]],
    run_evaluate_shadow_ranking_model_fn: Callable[..., dict[str, Any]],
    run_evaluate_shadow_experiment_policy_fn: Callable[..., dict[str, Any]],
    run_run_shadow_operator_fn: Callable[..., dict[str, Any]],
    run_summarize_shadow_experiment_ledger_fn: Callable[..., dict[str, Any]],
) -> int | None:
    if args.run_shadow_ranking_replay:
        if not args.dataset_manifest:
            parser.error("--run-shadow-ranking-replay requires --dataset-manifest")
        print_cli_result_fn(
            run_run_shadow_ranking_replay_fn(
                args.dataset_manifest,
                model_path=args.model_path,
                model_family=args.model_family,
                model_version=args.model_version,
                output_path=args.output_path,
                game=args.game,
                fixture_id=args.fixture_id,
                candidate_id=args.candidate_id,
                platform=args.platform,
            ),
            command_name="run_shadow_ranking_replay",
            full_json=args.full_json,
        )
        return 0

    if args.compare_shadow_ranking_replay:
        print_cli_result_fn(
            run_compare_shadow_ranking_replay_fn(
                args.compare_shadow_ranking_replay,
                output_path=args.output_path,
            ),
            command_name="compare_shadow_ranking_replay",
            full_json=args.full_json,
        )
        return 0

    if args.train_shadow_ranking_model:
        if not args.dataset_manifest:
            parser.error("--train-shadow-ranking-model requires --dataset-manifest")
        result = run_train_shadow_ranking_model_fn(
            args.dataset_manifest,
            model_output_path=args.model_output_path,
            model_family=args.model_family or "linear_shadow_ranker",
            training_target=args.training_target or "approved_or_selected_probability",
            split_key=args.split_key or "fixture_id",
            train_fraction=args.train_fraction if args.train_fraction is not None else 0.8,
            game=args.game,
            fixture_id=args.fixture_id,
            candidate_id=args.candidate_id,
            platform=args.platform,
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.compare_shadow_model_families:
        print_cli_result_fn(
            run_compare_shadow_model_families_fn(
                args.compare_shadow_model_families,
                output_path=args.output_path,
                training_target=args.training_target,
                game=args.game,
                platform=args.platform,
            ),
            full_json=args.full_json,
        )
        return 0

    if args.run_shadow_benchmark_matrix:
        if not args.dataset_manifest:
            parser.error("--run-shadow-benchmark-matrix requires --dataset-manifest")
        print_cli_result_fn(
            run_run_shadow_benchmark_matrix_fn(
                args.dataset_manifest,
                policy_path=args.policy_path,
                model_family=args.model_family,
                training_target=args.training_target,
                split_key=args.split_key or "fixture_id",
                train_fraction=args.train_fraction if args.train_fraction is not None else 0.8,
                game=args.game,
                platform=args.platform,
                output_path=args.output_path,
            ),
            command_name="run_shadow_benchmark_matrix",
            full_json=args.full_json,
        )
        return 0

    if args.summarize_shadow_benchmark_matrix is not None:
        print_cli_result_fn(
            run_summarize_shadow_benchmark_matrix_fn(
                None if args.summarize_shadow_benchmark_matrix == "" else args.summarize_shadow_benchmark_matrix,
                registry_path=args.registry_path,
                training_target=args.training_target,
                game=args.game,
                platform=args.platform,
                recommendation_decision=args.recommendation_decision,
                model_family=args.model_family,
            ),
            command_name="summarize_shadow_benchmark_matrix",
            full_json=args.full_json,
        )
        return 0

    if args.review_shadow_benchmark_results:
        print_cli_result_fn(
            run_review_shadow_benchmark_results_fn(
                args.review_shadow_benchmark_results,
                output_path=args.output_path,
                training_target=args.training_target,
                model_family=args.model_family,
                game=args.game,
                platform=args.platform,
            ),
            command_name="review_shadow_benchmark_results",
            full_json=args.full_json,
        )
        return 0

    if args.compare_shadow_benchmark_evidence_modes:
        print_cli_result_fn(
            run_compare_shadow_benchmark_evidence_modes_fn(
                args.compare_shadow_benchmark_evidence_modes[0],
                args.compare_shadow_benchmark_evidence_modes[1],
                output_path=args.output_path,
                training_target=args.training_target,
                game=args.game,
                platform=args.platform,
            ),
            command_name="compare_shadow_benchmark_evidence_modes",
            full_json=args.full_json,
        )
        return 0

    if args.summarize_shadow_target_readiness is not None:
        print_cli_result_fn(
            run_summarize_shadow_target_readiness_fn(
                None if args.summarize_shadow_target_readiness == "" else args.summarize_shadow_target_readiness,
                registry_path=args.registry_path,
                training_target=args.training_target,
                game=args.game,
                platform=args.platform,
                model_family=args.model_family,
            ),
            command_name="summarize_shadow_target_readiness",
            full_json=args.full_json,
        )
        return 0

    if args.evaluate_shadow_ranking_model:
        if not args.model_path:
            parser.error("--evaluate-shadow-ranking-model requires --model-path")
        print_cli_result_fn(
            run_evaluate_shadow_ranking_model_fn(
                model_path=args.model_path,
                dataset_manifest=args.dataset_manifest,
                output_path=args.output_path,
                game=args.game,
                fixture_id=args.fixture_id,
                candidate_id=args.candidate_id,
                platform=args.platform,
            ),
            full_json=args.full_json,
        )
        return 0

    if args.evaluate_shadow_experiment_policy:
        if not args.experiment_manifest:
            parser.error("--evaluate-shadow-experiment-policy requires --experiment-manifest")
        print_cli_result_fn(
            run_evaluate_shadow_experiment_policy_fn(
                args.experiment_manifest,
                policy_path=args.policy_path,
                target=args.target,
                output_path=args.output_path,
                game=args.game,
                platform=args.platform,
            ),
            command_name="evaluate_shadow_experiment_policy",
            full_json=args.full_json,
        )
        return 0

    if args.run_shadow_operator:
        if not args.mode:
            parser.error("--run-shadow-operator requires --mode")
        normalized_mode = str(args.mode).strip().lower()
        if normalized_mode not in {"train", "benchmark", "govern", "full"}:
            parser.error("--run-shadow-operator requires --mode train|benchmark|govern|full")
        if normalized_mode in {"train", "benchmark", "full"} and not args.dataset_manifest:
            parser.error(f"--run-shadow-operator with --mode {normalized_mode} requires --dataset-manifest")
        if normalized_mode == "govern" and not args.experiment_manifest:
            parser.error("--run-shadow-operator with --mode govern requires --experiment-manifest")
        print_cli_result_fn(
            run_run_shadow_operator_fn(
                mode=normalized_mode,
                dataset_manifest=args.dataset_manifest,
                model_path=args.model_path,
                model_family=args.model_family,
                model_version=args.model_version,
                experiment_manifest=args.experiment_manifest,
                training_target=args.training_target,
                target=args.target,
                policy_path=args.policy_path,
                game=args.game,
                platform=args.platform,
                output_root=args.output_root,
                output_path=args.output_path,
                split_key=args.split_key,
                train_fraction=args.train_fraction,
            ),
            command_name="run_shadow_operator",
            full_json=args.full_json,
        )
        return 0

    if args.summarize_shadow_experiment_ledger:
        print_cli_result_fn(
            run_summarize_shadow_experiment_ledger_fn(
                registry_path=args.registry_path,
                target=args.target,
                game=args.game,
                platform=args.platform,
                recommendation_decision=args.recommendation_decision,
                training_target=args.training_target,
            ),
            command_name="summarize_shadow_experiment_ledger",
            full_json=args.full_json,
        )
        return 0

    return None
