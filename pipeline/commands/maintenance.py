from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

from pipeline.contract_audit import audit_pipeline_contracts
from pipeline.roi_matcher import RoiMatcherError, check_roi_runtime, list_pack_templates, validate_published_pack
from tools.inspect_quality_maintenance_findings import inspect_quality_maintenance_findings
from tools.run_decision_regression_goldsets import run_decision_regression_goldsets
from tools.run_repo_quality_health import run_repo_quality_health


def run_inspect_quality_maintenance_findings(
    *,
    repo_root: str | Path,
    game: str | None = None,
    emit_json: bool = False,
    inspector: Callable[..., dict[str, Any]] = inspect_quality_maintenance_findings,
) -> dict[str, Any]:
    return inspector(
        repo_root=repo_root,
        game=game,
        emit_json=emit_json,
    )


def run_decision_regression_goldsets(
    *,
    emit_json: bool = False,
    runner: Callable[..., dict[str, Any]] = run_decision_regression_goldsets,
) -> dict[str, Any]:
    return runner(
        emit_json=emit_json,
    )


def run_repo_quality_health(
    *,
    emit_json: bool = False,
    runner: Callable[..., dict[str, Any]] = run_repo_quality_health,
) -> dict[str, Any]:
    return runner(
        emit_json=emit_json,
    )


def run_check_roi_runtime(
    *,
    checker: Callable[[], dict[str, Any]] = check_roi_runtime,
) -> dict[str, Any]:
    return checker()


def run_validate_published_pack(
    game: str,
    *,
    validator: Callable[[str], dict[str, Any]] = validate_published_pack,
) -> dict[str, Any]:
    try:
        return validator(game)
    except RoiMatcherError as exc:
        return exc.to_dict(game=game)


def run_audit_pipeline_contracts(
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    repo_root: str | Path,
    config_payload: dict[str, Any],
    auditor: Callable[..., dict[str, Any]] = audit_pipeline_contracts,
) -> dict[str, Any]:
    result = auditor(game=game, repo_root=repo_root, config_payload=config_payload)
    if output_path is not None:
        target = _resolve_output_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        debug_root = _resolve_output_path(debug_output_dir)
        debug_root.mkdir(parents=True, exist_ok=True)
        (debug_root / "pipeline_contract_audit.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def run_list_pack_templates(
    game: str,
    *,
    lister: Callable[[str], dict[str, Any]] = list_pack_templates,
) -> dict[str, Any]:
    try:
        return lister(game)
    except RoiMatcherError as exc:
        return exc.to_dict(game=game)
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "status": "missing_game_pack",
            "game": game,
            "error": str(exc),
        }


def dispatch_maintenance_commands(
    args: argparse.Namespace,
    *,
    run_audit_pipeline_contracts_fn: Callable[..., dict[str, Any]],
    run_inspect_quality_maintenance_findings_fn: Callable[..., dict[str, Any]],
    run_decision_regression_goldsets_fn: Callable[..., dict[str, Any]],
    run_repo_quality_health_fn: Callable[..., dict[str, Any]],
    run_check_roi_runtime_fn: Callable[..., dict[str, Any]],
    run_validate_published_pack_fn: Callable[[str], dict[str, Any]],
    run_list_pack_templates_fn: Callable[[str], dict[str, Any]],
) -> int | None:
    if args.audit_pipeline_contracts:
        result = run_audit_pipeline_contracts_fn(
            game=args.game,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.inspect_quality_maintenance_findings:
        return _rendered_command_exit(
            lambda: run_inspect_quality_maintenance_findings_fn(
                game=args.game,
                emit_json=bool(args.json),
            )
        )

    if args.run_decision_regression_goldsets:
        return _rendered_command_exit(
            lambda: run_decision_regression_goldsets_fn(
                emit_json=bool(args.json),
            )
        )

    if args.run_repo_quality_health:
        return _rendered_command_exit(
            lambda: run_repo_quality_health_fn(
                emit_json=bool(args.json),
            )
        )

    if args.check_roi_runtime:
        result = run_check_roi_runtime_fn()
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.validate_published_pack:
        result = run_validate_published_pack_fn(args.validate_published_pack)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.list_pack_templates:
        result = run_list_pack_templates_fn(args.list_pack_templates)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    return None


def _resolve_output_path(path_value: str | Path) -> Path:
    target = Path(path_value).expanduser()
    if not target.is_absolute():
        return (Path.cwd() / target).resolve()
    return target.resolve()


def _rendered_command_exit(command: Callable[[], dict[str, Any]]) -> int:
    try:
        result = command()
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    print(result["rendered_output"])
    return 0 if result.get("ok") else 1
