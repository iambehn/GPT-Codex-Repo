from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.runtime_tuning import replay_runtime_scoring
from pipeline.simple_yaml import dump_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HISTORY_ROOT = REPO_ROOT / "outputs" / "runtime_scoring_history"
REQUIRED_PROMOTION_RESULT_FIELDS = (
    "ok",
    "status",
    "trial_name",
    "config_path",
    "config_changed",
    "force_used",
    "applied_scoring",
    "previous_scoring",
    "replay_recommendation",
    "snapshot_paths",
    "warnings",
)


def promote_runtime_scoring(
    trial_config_path: str | Path,
    *,
    sidecar_root: str | Path | None,
    game: str | None = None,
    current_scoring_config: dict[str, Any],
    config_path: str | Path,
    config_data: dict[str, Any] | None,
    default_config: dict[str, Any],
    min_reviewed: int = 3,
    force: bool = False,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    if sidecar_root is None:
        return {
            "ok": False,
            "status": "missing_sidecar_root",
            "error": "sidecar root is required for runtime scoring promotion",
        }

    replay_result = replay_runtime_scoring(
        sidecar_root,
        trial_config_path,
        game=game,
        current_scoring_config=current_scoring_config,
        min_reviewed=min_reviewed,
        trial_name=trial_name,
    )
    replay_validation_error = _validate_runtime_replay_result(replay_result)
    if replay_validation_error is not None:
        return {
            "ok": False,
            "status": "invalid_replay_result",
            "error": replay_validation_error,
            "replay_result": replay_result,
        }
    if not replay_result.get("ok", False):
        if str(replay_result.get("status")) == "insufficient_review_data":
            if not force:
                return {
                    "ok": False,
                    "status": "promotion_blocked_by_replay",
                    "trial_name": replay_result.get("trial_name"),
                    "replay_recommendation": replay_result.get("recommendation"),
                    "warnings": replay_result.get("warnings", []),
                }
        else:
            return {
                "ok": False,
                "status": "replay_failed",
                "error": "runtime replay validation failed",
                "replay_result": replay_result,
            }

    replay_decision = str(replay_result.get("recommendation", {}).get("decision", "inconclusive"))
    if replay_decision != "prefer_trial" and not force:
        return {
            "ok": False,
            "status": "promotion_blocked_by_replay",
            "trial_name": replay_result.get("trial_name"),
            "replay_recommendation": replay_result.get("recommendation"),
            "warnings": replay_result.get("warnings", []),
        }

    config_target = _resolve_path(config_path)
    previous_scoring = deepcopy(current_scoring_config)
    applied_scoring = deepcopy(replay_result["trial_scoring"])
    next_config = _next_config_payload(config_data, default_config, applied_scoring)

    try:
        snapshot_paths = _write_snapshot_bundle(
            previous_scoring=previous_scoring,
            applied_scoring=applied_scoring,
            config_path=config_target,
            replay_result=replay_result,
            force=force,
        )
    except OSError as exc:
        return {
            "ok": False,
            "status": "snapshot_write_failed",
            "error": str(exc),
            "config_path": str(config_target),
        }

    try:
        config_target.parent.mkdir(parents=True, exist_ok=True)
        dump_yaml_file(config_target, next_config)
    except OSError as exc:
        return {
            "ok": False,
            "status": "config_write_failed",
            "error": str(exc),
            "config_path": str(config_target),
            "snapshot_paths": snapshot_paths,
        }

    result = {
        "ok": True,
        "status": "ok",
        "trial_name": replay_result.get("trial_name"),
        "config_path": str(config_target),
        "config_changed": previous_scoring != applied_scoring,
        "force_used": force,
        "applied_scoring": applied_scoring,
        "previous_scoring": previous_scoring,
        "replay_recommendation": replay_result.get("recommendation"),
        "snapshot_paths": snapshot_paths,
        "warnings": replay_result.get("warnings", []),
    }
    if game is not None:
        result["game_filter"] = game

    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")

    if debug_output_dir is not None:
        _write_debug_bundle(_resolve_path(debug_output_dir), result, replay_result)

    _validate_promotion_result_contract(result)
    return result


def _next_config_payload(
    config_data: dict[str, Any] | None,
    default_config: dict[str, Any],
    applied_scoring: dict[str, Any],
) -> dict[str, Any]:
    if config_data:
        next_config = deepcopy(config_data)
    else:
        next_config = deepcopy(default_config)
    runtime_analysis = next_config.get("runtime_analysis")
    if not isinstance(runtime_analysis, dict):
        runtime_analysis = {}
        next_config["runtime_analysis"] = runtime_analysis
    runtime_analysis["scoring"] = deepcopy(applied_scoring)
    return next_config


def _write_snapshot_bundle(
    *,
    previous_scoring: dict[str, Any],
    applied_scoring: dict[str, Any],
    config_path: Path,
    replay_result: dict[str, Any],
    force: bool,
) -> dict[str, str]:
    snapshot_dir = DEFAULT_HISTORY_ROOT / _snapshot_id(replay_result.get("trial_name"))
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    previous_path = snapshot_dir / "previous_scoring.yaml"
    applied_path = snapshot_dir / "applied_scoring.yaml"
    record_path = snapshot_dir / "promotion_record.json"
    replay_path = snapshot_dir / "replay_report.json"

    dump_yaml_file(previous_path, previous_scoring)
    dump_yaml_file(applied_path, applied_scoring)
    replay_path.write_text(json.dumps(replay_result, indent=2), encoding="utf-8")
    record_path.write_text(
        json.dumps(
            {
                "created_at": _utc_now(),
                "trial_name": replay_result.get("trial_name"),
                "config_path": str(config_path),
                "force_used": force,
                "replay_decision": replay_result.get("recommendation", {}).get("decision"),
                "replay_reason": replay_result.get("recommendation", {}).get("reason"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "snapshot_dir": str(snapshot_dir),
        "previous_scoring_path": str(previous_path),
        "applied_scoring_path": str(applied_path),
        "promotion_record_path": str(record_path),
        "replay_report_path": str(replay_path),
    }


def _write_debug_bundle(debug_root: Path, result: dict[str, Any], replay_result: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "runtime_promotion_result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (debug_root / "replay_report.json").write_text(json.dumps(replay_result, indent=2), encoding="utf-8")
    (debug_root / "warnings.json").write_text(json.dumps(result.get("warnings", []), indent=2), encoding="utf-8")


def _validate_runtime_replay_result(replay_result: dict[str, Any]) -> str | None:
    if not isinstance(replay_result, dict):
        return "runtime replay result must be a dict"
    if "recommendation" not in replay_result or not isinstance(replay_result.get("recommendation"), dict):
        return "runtime replay result is missing recommendation"
    recommendation = replay_result["recommendation"]
    for field in ("decision", "reason", "supporting_metrics", "data_quality_notes", "follow_up"):
        if field not in recommendation:
            return f"runtime replay recommendation missing field: {field}"
    if str(recommendation.get("decision")) not in {"prefer_trial", "keep_current", "inconclusive"}:
        return "runtime replay recommendation.decision is invalid"
    if "trial_scoring" not in replay_result or not isinstance(replay_result.get("trial_scoring"), dict):
        return "runtime replay result is missing trial_scoring"
    if "warnings" in replay_result and not isinstance(replay_result.get("warnings"), list):
        return "runtime replay warnings must be a list"
    return None


def _validate_promotion_result_contract(result: dict[str, Any]) -> None:
    missing_fields = [field for field in REQUIRED_PROMOTION_RESULT_FIELDS if field not in result]
    if missing_fields:
        raise ValueError(f"invalid_runtime_promotion_result_contract: missing fields: {', '.join(missing_fields)}")
    if not isinstance(result.get("applied_scoring"), dict):
        raise ValueError("invalid_runtime_promotion_result_contract: applied_scoring must be a dict")
    if not isinstance(result.get("previous_scoring"), dict):
        raise ValueError("invalid_runtime_promotion_result_contract: previous_scoring must be a dict")
    if not isinstance(result.get("replay_recommendation"), dict):
        raise ValueError("invalid_runtime_promotion_result_contract: replay_recommendation must be a dict")
    if not isinstance(result.get("snapshot_paths"), dict):
        raise ValueError("invalid_runtime_promotion_result_contract: snapshot_paths must be a dict")
    if not isinstance(result.get("warnings"), list):
        raise ValueError("invalid_runtime_promotion_result_contract: warnings must be a list")


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _snapshot_id(trial_name: Any) -> str:
    slug = str(trial_name or "trial").strip().lower().replace(" ", "-")
    slug = "".join(char if char.isalnum() or char == "-" else "-" for char in slug).strip("-") or "trial"
    return f"{_utc_now_compact()}-{slug}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
