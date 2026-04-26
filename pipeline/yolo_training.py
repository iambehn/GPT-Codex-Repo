from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.game_pack import get_yolo_model_dir, load_game_pack, validate_game_pack
from utils.logger import get_logger

logger = get_logger(__name__)

_SCHEMA_VERSION = "yolo_training.v1"


def train_yolo_model(game: str, config: dict, dry_run: bool = False) -> dict[str, Any]:
    game_pack = load_game_pack(game, config, create_missing=True)
    validation = validate_game_pack(game, config)
    if not validation.get("valid", False):
        return {
            "ok": False,
            "status": "failed",
            "game": game,
            "errors": list(validation.get("errors") or []),
            "warnings": list(validation.get("warnings") or []),
        }

    model_dir = get_yolo_model_dir(game, config, game_pack)
    dataset_path = model_dir / "dataset.yaml"
    label_map_path = model_dir / "label_map.json"
    dataset_manifest_path = model_dir / "dataset_manifest.json"
    weights_dir = model_dir / "weights"
    training_manifest_path = model_dir / "training_manifest.json"
    training_cfg = _training_config(config, game_pack)

    errors: list[str] = []
    warnings: list[str] = list(validation.get("warnings") or [])
    if not dataset_path.exists():
        errors.append(f"dataset.yaml not found: {dataset_path}")
    if not label_map_path.exists():
        warnings.append(f"label_map.json not found yet: {label_map_path}")

    dataset_manifest = _load_json(dataset_manifest_path)
    exported_examples = int(((dataset_manifest.get("summary") or {}).get("exported_examples") or 0))
    if exported_examples <= 0:
        errors.append("No exported YOLO examples found. Build the dataset first and add labeled samples.")
    elif exported_examples < int(training_cfg.get("minimum_examples", 1)):
        warnings.append(
            f"Only {exported_examples} exported examples found; recommended minimum is "
            f"{int(training_cfg.get('minimum_examples', 1))}."
        )

    if errors:
        return {
            "ok": False,
            "status": "failed",
            "game": game,
            "model_dir": str(model_dir),
            "errors": errors,
            "warnings": warnings,
        }

    run_name = _run_name(training_cfg.get("run_name_prefix", "train"))
    run_root = model_dir / "runs"
    run_dir = run_root / run_name
    summary = {
        "schema_version": _SCHEMA_VERSION,
        "game": game,
        "generated_at": _now_iso(),
        "model_dir": str(model_dir),
        "dataset_path": str(dataset_path),
        "dataset_manifest_path": str(dataset_manifest_path) if dataset_manifest_path.exists() else None,
        "weights_dir": str(weights_dir),
        "training_config": training_cfg,
        "exported_examples": exported_examples,
        "dry_run": dry_run,
    }

    if dry_run:
        summary["status"] = "dry_run"
        summary["expected_run_dir"] = str(run_dir)
        return {"ok": True, **summary, "warnings": warnings, "errors": []}

    try:
        results = _run_training(dataset_path, run_root, run_name, training_cfg)
    except ImportError as e:
        return {
            "ok": False,
            "status": "missing_dependency",
            "game": game,
            "model_dir": str(model_dir),
            "errors": [str(e)],
            "warnings": warnings,
        }
    except Exception as e:
        logger.warning(f"[yolo_training] Failed for {game}: {e}")
        return {
            "ok": False,
            "status": "error",
            "game": game,
            "model_dir": str(model_dir),
            "errors": [str(e)],
            "warnings": warnings,
        }

    save_dir = Path(str(getattr(results, "save_dir", run_dir)))
    best_weights = save_dir / "weights" / "best.pt"
    last_weights = save_dir / "weights" / "last.pt"
    promoted_best = weights_dir / "best.pt"
    promoted_last = weights_dir / "last.pt"
    weights_dir.mkdir(parents=True, exist_ok=True)

    if best_weights.exists():
        shutil.copy2(best_weights, promoted_best)
    else:
        warnings.append(f"Training completed without best.pt at {best_weights}")
    if last_weights.exists():
        shutil.copy2(last_weights, promoted_last)

    metrics = _results_metrics(results)
    manifest = {
        **summary,
        "status": "ok",
        "completed_at": _now_iso(),
        "run_dir": str(save_dir),
        "promoted_best_weights": str(promoted_best) if promoted_best.exists() else None,
        "promoted_last_weights": str(promoted_last) if promoted_last.exists() else None,
        "metrics": metrics,
        "warnings": warnings,
    }
    training_manifest_path.write_text(json.dumps(manifest, indent=2))
    return {"ok": True, **manifest, "errors": []}


def _training_config(config: dict, game_pack: dict) -> dict[str, Any]:
    global_cfg = dict(config.get("yolo_training") or {})
    hud = game_pack.get("hud") or {}
    detector_cfg = dict(((hud.get("detectors") or {}).get("yolo")) or {})
    training_overrides = dict(detector_cfg.get("training") or {})

    merged = {
        "base_model": str(global_cfg.get("base_model", "yolov8s.pt")),
        "epochs": int(global_cfg.get("epochs", 80)),
        "imgsz": int(global_cfg.get("imgsz", 640)),
        "batch": int(global_cfg.get("batch", 16)),
        "patience": int(global_cfg.get("patience", 15)),
        "device": global_cfg.get("device", "cpu"),
        "workers": int(global_cfg.get("workers", 2)),
        "project_subdir": str(global_cfg.get("project_subdir", "runs")),
        "run_name_prefix": str(global_cfg.get("run_name_prefix", "train")),
        "minimum_examples": int(global_cfg.get("minimum_examples", 30)),
    }
    merged.update(training_overrides)
    return merged


def _run_training(dataset_path: Path, run_root: Path, run_name: str, training_cfg: dict[str, Any]) -> Any:
    try:
        from ultralytics import YOLO
    except ImportError as e:
        raise ImportError("ultralytics is not installed; install it to enable YOLO training") from e

    model = YOLO(str(training_cfg.get("base_model", "yolov8s.pt")))
    return model.train(
        data=str(dataset_path),
        project=str(run_root),
        name=run_name,
        epochs=int(training_cfg.get("epochs", 80)),
        imgsz=int(training_cfg.get("imgsz", 640)),
        batch=int(training_cfg.get("batch", 16)),
        patience=int(training_cfg.get("patience", 15)),
        device=training_cfg.get("device", "cpu"),
        workers=int(training_cfg.get("workers", 2)),
        exist_ok=False,
    )


def _results_metrics(results: Any) -> dict[str, Any]:
    metrics = getattr(results, "results_dict", None)
    if isinstance(metrics, dict):
        return {str(key): _safe_scalar(value) for key, value in metrics.items()}
    return {}


def _safe_scalar(value: Any) -> Any:
    try:
        if hasattr(value, "item"):
            value = value.item()
    except Exception:
        return value
    if isinstance(value, float):
        return round(value, 6)
    return value


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _run_name(prefix: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}-{stamp}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
