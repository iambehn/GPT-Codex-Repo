from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.runtime_export import merged_scoring_config
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HISTORY_ROOT = REPO_ROOT / "outputs" / "runtime_scoring_history"


def rollback_runtime_scoring(
    snapshot_dir: str | Path,
    *,
    config_path: str | Path,
    config_data: dict[str, Any] | None,
    default_config: dict[str, Any],
    current_scoring_config: dict[str, Any],
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    rollback_name: str | None = None,
) -> dict[str, Any]:
    source_dir = _resolve_path(snapshot_dir)
    if not source_dir.exists() or not source_dir.is_dir():
        return {
            "ok": False,
            "status": "invalid_snapshot_dir",
            "error": "snapshot directory does not exist or is not a directory",
            "restore_source": str(source_dir),
        }

    required_paths = {
        "previous_scoring_path": source_dir / "previous_scoring.yaml",
        "applied_scoring_path": source_dir / "applied_scoring.yaml",
        "promotion_record_path": source_dir / "promotion_record.json",
    }
    for key, path in required_paths.items():
        if not path.exists() or not path.is_file():
            return {
                "ok": False,
                "status": "missing_snapshot_file",
                "error": f"required snapshot file is missing: {path.name}",
                "restore_source": str(source_dir),
                "missing_path": str(path),
                "missing_key": key,
            }

    try:
        restore_payload = load_yaml_file(required_paths["previous_scoring_path"])
        promotion_record = json.loads(required_paths["promotion_record_path"].read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_restore_payload",
            "error": str(exc),
            "restore_source": str(source_dir),
        }

    if not isinstance(restore_payload, dict):
        return {
            "ok": False,
            "status": "invalid_restore_payload",
            "error": "previous_scoring.yaml must contain a mapping",
            "restore_source": str(source_dir),
        }

    restored_scoring = merged_scoring_config(restore_payload)
    current_scoring = merged_scoring_config(current_scoring_config)
    next_config = _next_config_payload(config_data, default_config, restored_scoring)
    config_target = _resolve_path(config_path)

    try:
        rollback_snapshot_paths = _write_rollback_snapshot(
            current_scoring=current_scoring,
            restored_scoring=restored_scoring,
            source_dir=source_dir,
            config_path=config_target,
            rollback_name=rollback_name,
            promotion_record=promotion_record,
        )
    except OSError as exc:
        return {
            "ok": False,
            "status": "snapshot_write_failed",
            "error": str(exc),
            "config_path": str(config_target),
            "restore_source": str(source_dir),
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
            "restore_source": str(source_dir),
            "rollback_snapshot_paths": rollback_snapshot_paths,
        }

    result = {
        "ok": True,
        "status": "ok",
        "config_path": str(config_target),
        "config_changed": current_scoring != restored_scoring,
        "restore_source": str(source_dir),
        "restored_scoring": restored_scoring,
        "current_scoring_before_restore": current_scoring,
        "source_snapshot_paths": {key: str(path) for key, path in required_paths.items()},
        "rollback_snapshot_paths": rollback_snapshot_paths,
        "warnings": [],
    }

    if output_path is not None:
        target = _resolve_path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")

    if debug_output_dir is not None:
        _write_debug_bundle(_resolve_path(debug_output_dir), result, promotion_record)

    return result


def _next_config_payload(
    config_data: dict[str, Any] | None,
    default_config: dict[str, Any],
    restored_scoring: dict[str, Any],
) -> dict[str, Any]:
    if config_data:
        next_config = deepcopy(config_data)
    else:
        next_config = deepcopy(default_config)
    runtime_analysis = next_config.get("runtime_analysis")
    if not isinstance(runtime_analysis, dict):
        runtime_analysis = {}
        next_config["runtime_analysis"] = runtime_analysis
    runtime_analysis["scoring"] = deepcopy(restored_scoring)
    return next_config


def _write_rollback_snapshot(
    *,
    current_scoring: dict[str, Any],
    restored_scoring: dict[str, Any],
    source_dir: Path,
    config_path: Path,
    rollback_name: str | None,
    promotion_record: dict[str, Any],
) -> dict[str, str]:
    snapshot_dir = DEFAULT_HISTORY_ROOT / _snapshot_id(rollback_name or "rollback")
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    previous_path = snapshot_dir / "previous_scoring.yaml"
    applied_path = snapshot_dir / "applied_scoring.yaml"
    record_path = snapshot_dir / "rollback_record.json"
    source_record_path = snapshot_dir / "source_promotion_record.json"

    dump_yaml_file(previous_path, current_scoring)
    dump_yaml_file(applied_path, restored_scoring)
    source_record_path.write_text(json.dumps(promotion_record, indent=2), encoding="utf-8")
    record_path.write_text(
        json.dumps(
            {
                "created_at": _utc_now(),
                "rollback_name": rollback_name or "rollback",
                "config_path": str(config_path),
                "source_snapshot_dir": str(source_dir),
                "source_promotion_record_path": str(source_dir / "promotion_record.json"),
                "restore_source_path": str(source_dir / "previous_scoring.yaml"),
                "restored_from_trial_name": promotion_record.get("trial_name"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "snapshot_dir": str(snapshot_dir),
        "previous_scoring_path": str(previous_path),
        "applied_scoring_path": str(applied_path),
        "rollback_record_path": str(record_path),
        "source_promotion_record_copy_path": str(source_record_path),
    }


def _write_debug_bundle(debug_root: Path, result: dict[str, Any], promotion_record: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "runtime_rollback_result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (debug_root / "source_promotion_record.json").write_text(json.dumps(promotion_record, indent=2), encoding="utf-8")
    (debug_root / "warnings.json").write_text(json.dumps(result.get("warnings", []), indent=2), encoding="utf-8")


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _snapshot_id(label: str) -> str:
    slug = str(label or "rollback").strip().lower().replace(" ", "-")
    slug = "".join(char if char.isalnum() or char == "-" else "-" for char in slug).strip("-") or "rollback"
    return f"{_utc_now_compact()}-{slug}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
