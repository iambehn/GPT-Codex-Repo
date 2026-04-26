from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from pipeline.game_pack import (
    get_game_pack_dir,
    get_primary_entities,
    get_yolo_detector_game_config,
    get_yolo_model_dir,
    load_game_pack,
    resolve_asset_path,
    validate_game_pack,
)
from utils.logger import get_logger

logger = get_logger(__name__)

_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp"}
_SCHEMA_VERSION = "yolo_dataset.v1"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TRAIN_RATIO = 0.8


def build_yolo_dataset(game: str, config: dict) -> dict[str, Any]:
    """Generate the per-game YOLO registry and dataset/export files.

    This stage does not train a model. It creates a stable directory layout,
    exports class metadata from the game pack, and scans existing icon/template
    assets as annotation seeds.
    """
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
    _ensure_model_dirs(model_dir)

    yolo_cfg = get_yolo_detector_game_config(game, config, game_pack)
    label_entries = _label_entries(game_pack, yolo_cfg)
    if not label_entries:
        return {
            "ok": False,
            "status": "failed",
            "game": game,
            "model_dir": str(model_dir),
            "errors": ["hud.yaml detectors.yolo.labels is empty; define concrete YOLO classes first."],
            "warnings": list(validation.get("warnings") or []),
        }

    dataset_payload = {
        "path": str(model_dir),
        "train": "images/train",
        "val": "images/val",
        "names": [entry["label"] for entry in label_entries],
    }
    dataset_path = model_dir / "dataset.yaml"
    labels_path = model_dir / "labels.txt"
    label_map_path = model_dir / "label_map.json"
    seed_manifest_path = model_dir / "seed_manifest.json"
    dataset_manifest_path = model_dir / "dataset_manifest.json"

    dataset_path.write_text(yaml.safe_dump(dataset_payload, sort_keys=False, allow_unicode=False))
    labels_path.write_text("".join(f"{entry['label']}\n" for entry in label_entries))
    label_map_path.write_text(json.dumps({
        "schema_version": _SCHEMA_VERSION,
        "game": game,
        "generated_at": _now_iso(),
        "classes": label_entries,
    }, indent=2))

    seed_manifest = _build_seed_manifest(game, config, game_pack, label_entries, model_dir)
    seed_manifest_path.write_text(json.dumps(seed_manifest, indent=2))
    dataset_manifest = _export_roi_crop_dataset(game, config, game_pack, label_entries, model_dir, seed_manifest, yolo_cfg)
    dataset_manifest_path.write_text(json.dumps(dataset_manifest, indent=2))
    validation = validate_game_pack(game, config)

    logger.info(
        f"[yolo_dataset] Built dataset registry for {game}: "
        f"{len(label_entries)} class(es), "
        f"{seed_manifest['summary']['seed_assets']} seed asset(s), "
        f"{dataset_manifest['summary']['exported_examples']} exported example(s)"
    )
    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "model_dir": str(model_dir),
        "dataset_path": str(dataset_path),
        "labels_path": str(labels_path),
        "label_map_path": str(label_map_path),
        "seed_manifest_path": str(seed_manifest_path),
        "dataset_manifest_path": str(dataset_manifest_path),
        "classes": len(label_entries),
        "seed_assets": seed_manifest["summary"]["seed_assets"],
        "exported_examples": dataset_manifest["summary"]["exported_examples"],
        "warnings": list(validation.get("warnings") or []) + list(dataset_manifest.get("warnings") or []),
        "errors": [],
    }


def _ensure_model_dirs(model_dir: Path) -> None:
    for subdir in (
        "images/train",
        "images/val",
        "labels/train",
        "labels/val",
        "weights",
        "seed_assets/icons",
        "seed_assets/roi_templates",
        "seed_assets/reference_frames",
    ):
        path = model_dir / subdir
        path.mkdir(parents=True, exist_ok=True)
        keep = path / ".gitkeep"
        if not keep.exists():
            keep.write_text("")


def _clear_export_dirs(model_dir: Path) -> None:
    for subdir in ("images/train", "images/val", "labels/train", "labels/val"):
        path = model_dir / subdir
        if not path.exists():
            continue
        for child in path.iterdir():
            if child.name == ".gitkeep":
                continue
            if child.is_file() or child.is_symlink():
                child.unlink()
            elif child.is_dir():
                shutil.rmtree(child)


def _label_entries(game_pack: dict, yolo_cfg: dict) -> list[dict[str, Any]]:
    labels = yolo_cfg.get("labels") or {}
    if not isinstance(labels, dict):
        return []

    primary_kind, primary_entities = get_primary_entities(game_pack)
    moments = {item.get("id"): item for item in ((game_pack.get("moments") or {}).get("moments") or [])}

    entries: list[dict[str, Any]] = []
    for class_id, (raw_label, mapping) in enumerate(labels.items()):
        maps_to = None
        kind = None
        if isinstance(mapping, str):
            maps_to = mapping
        elif isinstance(mapping, dict):
            maps_to = mapping.get("maps_to") or mapping.get("entity_id") or mapping.get("event_id")
            kind = mapping.get("kind")

        if not kind:
            if maps_to in primary_entities:
                kind = "entity"
            elif maps_to in moments:
                kind = "event"

        display_name = maps_to or raw_label
        if kind == "entity" and maps_to in primary_entities:
            display_name = primary_entities[maps_to].get("display_name") or display_name
        elif kind == "event" and maps_to in moments:
            display_name = moments[maps_to].get("id") or display_name

        entries.append({
            "class_id": class_id,
            "label": str(raw_label),
            "kind": kind,
            "maps_to": maps_to,
            "display_name": display_name,
            "entity_kind": primary_kind if kind == "entity" else None,
        })
    return entries


def _build_seed_manifest(
    game: str,
    config: dict,
    game_pack: dict,
    label_entries: list[dict[str, Any]],
    model_dir: Path,
) -> dict[str, Any]:
    pack_dir = get_game_pack_dir(game, config)
    hud = game_pack.get("hud") or {}
    detectors = hud.get("detectors") or {}
    roi_template_meta = {
        str(item.get("id")): item
        for item in (hud.get("roi_templates") or [])
        if isinstance(item, dict) and item.get("id")
    }

    weapon_detector = detectors.get("weapon_detector") or {}
    icon_dir_raw = weapon_detector.get("icon_dir") or f"assets/weapon_icons/{game}"
    icon_dir = resolve_asset_path(icon_dir_raw, pack_dir)
    roi_dir = pack_dir / "roi_templates"
    reference_dir = pack_dir / "examples" / "reference_frames"

    icons = _icon_seed_assets(icon_dir, label_entries)
    roi_templates = _roi_seed_assets(roi_dir, label_entries, roi_template_meta)
    reference_frames = _reference_frame_assets(reference_dir)

    return {
        "schema_version": _SCHEMA_VERSION,
        "game": game,
        "generated_at": _now_iso(),
        "model_dir": str(model_dir),
        "classes": label_entries,
        "sources": {
            "icons": icons,
            "roi_templates": roi_templates,
            "reference_frames": reference_frames,
        },
        "summary": {
            "seed_assets": len(icons) + len(roi_templates) + len(reference_frames),
            "icons": len(icons),
            "roi_templates": len(roi_templates),
            "reference_frames": len(reference_frames),
        },
    }


def _export_roi_crop_dataset(
    game: str,
    config: dict,
    game_pack: dict,
    label_entries: list[dict[str, Any]],
    model_dir: Path,
    seed_manifest: dict[str, Any],
    yolo_cfg: dict[str, Any],
) -> dict[str, Any]:
    _clear_export_dirs(model_dir)

    cv2, np = _load_cv2_numpy()
    warnings: list[str] = []
    samples: list[dict[str, Any]] = []
    label_map = {entry["label"]: entry for entry in label_entries}
    entity_labels = {
        entry["maps_to"]: entry
        for entry in label_entries
        if entry.get("kind") == "entity" and entry.get("maps_to")
    }

    roi_map = _roi_map(game_pack)
    weapon_roi = roi_map.get("weapon_detector")
    if weapon_roi is None:
        warnings.append("weapon_detector ROI is missing; entity ROI-crop export is limited.")
    asset_training_offsets = _asset_training_offsets(yolo_cfg)
    pseudo_label_confidence = _safe_float(yolo_cfg.get("weapon_pseudo_label_min_confidence"), 0.92)

    dataset_cfg = dict(config.get("yolo_dataset") or {})
    n_augment = max(0, int(dataset_cfg.get("augment_per_seed", 0)))
    rng = np.random.default_rng(int(dataset_cfg.get("augment_seed", 42))) if n_augment > 0 else None

    for sample in _scan_asset_training_examples(game, config, game_pack, entity_labels, weapon_roi, cv2, asset_training_offsets):
        if sample.get("warning"):
            warnings.append(sample["warning"])
            continue
        samples.append(sample)

    for sample in _scan_weapon_detector_examples(game, config, entity_labels, weapon_roi, cv2, pseudo_label_confidence):
        if sample.get("warning"):
            warnings.append(sample["warning"])
            continue
        samples.append(sample)

    for icon_sample in _icon_seed_export_samples(seed_manifest, entity_labels, weapon_roi, cv2, np, n_augment=n_augment, rng=rng):
        if icon_sample.get("warning"):
            warnings.append(icon_sample["warning"])
            continue
        samples.append(icon_sample)

    for template_sample in _roi_template_export_samples(seed_manifest, label_map, roi_map, cv2, np):
        if template_sample.get("warning"):
            warnings.append(template_sample["warning"])
            continue
        samples.append(template_sample)

    reference_frames = (seed_manifest.get("sources") or {}).get("reference_frames") or []
    if reference_frames:
        warnings.append("reference_frames are tracked in the seed manifest but not exported without annotations.")

    if not samples:
        warnings.append("No ROI-crop samples were exported; add quarantine asset-training entries or valid seed assets.")

    samples = _assign_splits(samples)
    exported_samples = _write_exported_samples(samples, model_dir, cv2)
    class_counts = _class_counts(exported_samples, label_entries)
    missing_labels = [entry["label"] for entry in label_entries if class_counts.get(entry["label"], 0) == 0]
    for label in missing_labels:
        warnings.append(f"YOLO class '{label}' has no exported examples yet.")

    return {
        "schema_version": _SCHEMA_VERSION,
        "game": game,
        "generated_at": _now_iso(),
        "export_mode": "roi_crop",
        "model_dir": str(model_dir),
        "samples": exported_samples,
        "summary": {
            "exported_examples": len(exported_samples),
            "train_examples": sum(1 for sample in exported_samples if sample["split"] == "train"),
            "val_examples": sum(1 for sample in exported_samples if sample["split"] == "val"),
            "clip_asset_training_examples": sum(1 for sample in exported_samples if sample["source_type"] == "clip_asset_training"),
            "weapon_detector_pseudo_examples": sum(1 for sample in exported_samples if sample["source_type"] == "weapon_detector_pseudo"),
            "icon_seed_examples": sum(1 for sample in exported_samples if sample["source_type"] == "icon_seed"),
            "icon_seed_augmented_examples": sum(1 for sample in exported_samples if sample["source_type"] == "icon_seed_augmented"),
            "roi_template_examples": sum(1 for sample in exported_samples if sample["source_type"] == "roi_template_seed"),
            "classes_with_examples": sum(1 for entry in label_entries if class_counts.get(entry["label"], 0) > 0),
            "augment_per_seed": n_augment,
        },
        "class_counts": class_counts,
        "asset_training_time_offsets_seconds": asset_training_offsets,
        "weapon_pseudo_label_min_confidence": pseudo_label_confidence,
        "warnings": _dedupe_warnings(warnings),
    }


def _icon_seed_assets(icon_dir: Path, label_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not icon_dir.exists():
        return []

    assets: list[dict[str, Any]] = []
    for path in sorted(icon_dir.iterdir()):
        if path.suffix.lower() not in _IMAGE_SUFFIXES or not path.is_file():
            continue
        entity_id = path.stem
        suggested_labels = [
            entry["label"]
            for entry in label_entries
            if entry.get("kind") == "entity" and entry.get("maps_to") == entity_id
        ]
        assets.append({
            "source_type": "icon",
            "source_path": str(path),
            "entity_id": entity_id,
            "suggested_labels": suggested_labels,
        })
    return assets


def _roi_seed_assets(
    roi_dir: Path,
    label_entries: list[dict[str, Any]],
    roi_template_meta: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if not roi_dir.exists():
        return []

    assets: list[dict[str, Any]] = []
    for path in sorted(roi_dir.iterdir()):
        if path.suffix.lower() not in _IMAGE_SUFFIXES or not path.is_file():
            continue
        template_id = path.stem
        template_meta = roi_template_meta.get(template_id, {})
        suggested_labels = [
            entry["label"]
            for entry in label_entries
            if entry["label"] == template_id or entry.get("maps_to") == template_id
        ]
        assets.append({
            "source_type": "roi_template",
            "source_path": str(path),
            "template_id": template_id,
            "semantic_type": template_meta.get("semantic_type"),
            "roi_ref": template_meta.get("roi_ref"),
            "suggested_labels": suggested_labels,
        })
    return assets


def _reference_frame_assets(reference_dir: Path) -> list[dict[str, Any]]:
    if not reference_dir.exists():
        return []

    assets: list[dict[str, Any]] = []
    for path in sorted(reference_dir.iterdir()):
        if path.name.startswith(".") or path.suffix.lower() not in _IMAGE_SUFFIXES or not path.is_file():
            continue
        assets.append({
            "source_type": "reference_frame",
            "source_path": str(path),
            "suggested_labels": [],
        })
    return assets


def _load_cv2_numpy():
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
    except ImportError as e:
        raise RuntimeError("opencv-python-headless and numpy are required for ROI-crop dataset export") from e
    return cv2, np


def _roi_map(game_pack: dict) -> dict[str, dict[str, int]]:
    rois = ((game_pack.get("hud") or {}).get("rois") or {})
    normalized: dict[str, dict[str, int]] = {}
    for name, roi in rois.items():
        if not isinstance(roi, dict):
            continue
        try:
            normalized[name] = {
                "x": int(round(float(roi["x"]))),
                "y": int(round(float(roi["y"]))),
                "w": int(round(float(roi["w"]))),
                "h": int(round(float(roi["h"]))),
            }
        except (KeyError, TypeError, ValueError):
            continue
    return normalized


def _scan_asset_training_examples(
    game: str,
    config: dict,
    game_pack: dict,
    entity_labels: dict[str, dict[str, Any]],
    weapon_roi: dict[str, int] | None,
    cv2: Any,
    frame_offsets: list[float],
) -> list[dict[str, Any]]:
    if not weapon_roi:
        return []

    examples: list[dict[str, Any]] = []
    for meta_path in _iter_meta_paths(config, game):
        meta = _load_json(meta_path)
        clip_path = _resolve_source_clip(meta.get("clip_path"))
        clip_id = str(meta.get("clip_id", meta_path.stem))
        for index, entry in enumerate(meta.get("asset_training") or []):
            if not isinstance(entry, dict):
                continue
            entity_id = str(entry.get("entity_id", "")).strip()
            label_entry = entity_labels.get(entity_id)
            if not label_entry:
                continue

            crop_box = entry.get("crop_box") or {}
            try:
                crop_box = {
                    "x": int(round(float(crop_box["x"]))),
                    "y": int(round(float(crop_box["y"]))),
                    "w": int(round(float(crop_box["w"]))),
                    "h": int(round(float(crop_box["h"]))),
                }
            except (KeyError, TypeError, ValueError):
                examples.append({"warning": f"{meta_path}: invalid crop_box for asset_training entry {index}."})
                continue

            source_clip = _resolve_source_clip(entry.get("source_clip")) or clip_path
            if source_clip is None or not source_clip.exists():
                examples.append({"warning": f"{meta_path}: source clip for asset_training entry {index} is missing."})
                continue

            anchor_frame_time = _safe_float(entry.get("frame_time_seconds"), 0.0)
            seen_frame_times: set[float] = set()
            for frame_offset in frame_offsets:
                sampled_frame_time = round(max(0.0, anchor_frame_time + float(frame_offset)), 3)
                if sampled_frame_time in seen_frame_times:
                    continue
                seen_frame_times.add(sampled_frame_time)

                frame = _read_video_frame(source_clip, sampled_frame_time, cv2)
                if frame is None:
                    examples.append({"warning": f"{source_clip}: could not read frame at {sampled_frame_time}s."})
                    continue

                normalized = _normalize_frame_1920(frame, cv2)
                scale_x = 1920.0 / frame.shape[1]
                scale_y = 1080.0 / frame.shape[0]
                scaled_box = {
                    "x": int(round(crop_box["x"] * scale_x)),
                    "y": int(round(crop_box["y"] * scale_y)),
                    "w": int(round(crop_box["w"] * scale_x)),
                    "h": int(round(crop_box["h"] * scale_y)),
                }
                roi_image, bbox_norm = _crop_roi_with_bbox(normalized, weapon_roi, scaled_box)
                if roi_image is None or bbox_norm is None:
                    examples.append({"warning": f"{meta_path}: crop_box did not overlap the configured weapon ROI."})
                    continue

                offset_slug = _offset_slug(frame_offset)
                examples.append({
                    "sample_id": f"{label_entry['label']}__clip__{clip_id}__{index}__{offset_slug}",
                    "source_type": "clip_asset_training",
                    "label": label_entry["label"],
                    "class_id": label_entry["class_id"],
                    "maps_to": label_entry.get("maps_to"),
                    "display_name": label_entry.get("display_name"),
                    "split_key": f"{clip_id}:{index}:{offset_slug}",
                    "image_array": roi_image,
                    "bbox_norm": bbox_norm,
                    "image_ext": ".png",
                    "source_path": str(source_clip),
                    "source_meta_path": str(meta_path),
                    "frame_time_seconds": sampled_frame_time,
                    "anchor_frame_time_seconds": anchor_frame_time,
                    "frame_offset_seconds": round(float(frame_offset), 3),
                    "roi_box": {
                        **weapon_roi,
                        "base_width": 1920,
                        "base_height": 1080,
                    },
                    "crop_box": scaled_box,
                })
    return examples


def _scan_weapon_detector_examples(
    game: str,
    config: dict,
    entity_labels: dict[str, dict[str, Any]],
    weapon_roi: dict[str, int] | None,
    cv2: Any,
    min_confidence: float,
) -> list[dict[str, Any]]:
    if not weapon_roi:
        return []

    examples: list[dict[str, Any]] = []
    for meta_path in _iter_meta_paths(config, game):
        meta = _load_json(meta_path)
        weapon_detection = meta.get("weapon_detection") or {}
        if str(weapon_detection.get("method") or "").strip() != "template_match":
            continue

        source_clip = _resolve_source_clip(meta.get("clip_path"))
        if source_clip is None or not source_clip.exists():
            continue

        clip_id = str(meta.get("clip_id", meta_path.stem))
        observations = weapon_detection.get("frame_observations") or []
        if isinstance(observations, list) and observations:
            for index, observation in enumerate(observations):
                if not isinstance(observation, dict):
                    continue
                sample = _weapon_detector_example_from_observation(
                    observation,
                    weapon_detection,
                    entity_labels,
                    weapon_roi,
                    source_clip,
                    meta_path,
                    clip_id,
                    index,
                    min_confidence,
                    cv2,
                )
                if sample:
                    examples.append(sample)
            continue

        sample = _weapon_detector_example_from_observation(
            {
                "weapon_id": weapon_detection.get("weapon_id"),
                "display_name": weapon_detection.get("display_name"),
                "confidence": weapon_detection.get("confidence"),
                "timestamp": weapon_detection.get("frame_time"),
                "match_box": weapon_detection.get("best_match_box"),
            },
            weapon_detection,
            entity_labels,
            weapon_roi,
            source_clip,
            meta_path,
            clip_id,
            0,
            min_confidence,
            cv2,
        )
        if sample:
            examples.append(sample)
    return examples


def _weapon_detector_example_from_observation(
    observation: dict[str, Any],
    weapon_detection: dict[str, Any],
    entity_labels: dict[str, dict[str, Any]],
    weapon_roi: dict[str, int],
    source_clip: Path,
    meta_path: Path,
    clip_id: str,
    index: int,
    min_confidence: float,
    cv2: Any,
) -> dict[str, Any] | None:
    weapon_id = str(observation.get("weapon_id") or weapon_detection.get("weapon_id") or "").strip()
    label_entry = entity_labels.get(weapon_id)
    if not label_entry:
        return None

    confidence = _safe_float(observation.get("confidence"), _safe_float(weapon_detection.get("confidence"), 0.0))
    if confidence < min_confidence:
        return None

    raw_box = observation.get("match_box") or weapon_detection.get("best_match_box") or {}
    scaled_box = _scaled_match_box(raw_box)
    if not scaled_box:
        return None

    frame_time = _safe_float(observation.get("timestamp"), _safe_float(weapon_detection.get("frame_time"), 0.0))
    frame = _read_video_frame(source_clip, frame_time, cv2)
    if frame is None:
        return {"warning": f"{source_clip}: could not read pseudo-label frame at {frame_time}s."}

    normalized = _normalize_frame_1920(frame, cv2)
    roi_image, bbox_norm = _crop_roi_with_bbox(normalized, weapon_roi, scaled_box)
    if roi_image is None or bbox_norm is None:
        return {"warning": f"{meta_path}: pseudo-label match_box did not overlap the configured weapon ROI."}

    return {
        "sample_id": f"{label_entry['label']}__pseudo__{clip_id}__{index}",
        "source_type": "weapon_detector_pseudo",
        "label": label_entry["label"],
        "class_id": label_entry["class_id"],
        "maps_to": label_entry.get("maps_to"),
        "display_name": label_entry.get("display_name"),
        "split_key": f"{clip_id}:pseudo:{index}",
        "image_array": roi_image,
        "bbox_norm": bbox_norm,
        "image_ext": ".png",
        "source_path": str(source_clip),
        "source_meta_path": str(meta_path),
        "frame_time_seconds": frame_time,
        "pseudo_confidence": confidence,
        "roi_box": {
            **weapon_roi,
            "base_width": 1920,
            "base_height": 1080,
        },
        "crop_box": scaled_box,
    }


def _scaled_match_box(raw_box: Any) -> dict[str, int] | None:
    if not isinstance(raw_box, dict):
        return None
    try:
        x = float(raw_box["x"])
        y = float(raw_box["y"])
        w = float(raw_box["w"])
        h = float(raw_box["h"])
    except (KeyError, TypeError, ValueError):
        return None

    base_width = _safe_float(raw_box.get("base_width"), 1920.0) or 1920.0
    base_height = _safe_float(raw_box.get("base_height"), 1080.0) or 1080.0
    scale_x = 1920.0 / float(base_width)
    scale_y = 1080.0 / float(base_height)
    return {
        "x": int(round(x * scale_x)),
        "y": int(round(y * scale_y)),
        "w": int(round(w * scale_x)),
        "h": int(round(h * scale_y)),
    }


def _iter_meta_paths(config: dict, game: str) -> list[Path]:
    paths = config.get("paths") or {}
    stage_keys = ("inbox", "quarantine", "processing", "accepted", "rejected")
    meta_paths: list[Path] = []
    for key in stage_keys:
        raw_root = paths.get(key)
        if not raw_root:
            continue
        root = Path(str(raw_root))
        if not root.is_absolute():
            root = (_PROJECT_ROOT / root).resolve()
        else:
            root = root.resolve()
        game_root = root / game
        if not game_root.exists():
            continue
        meta_paths.extend(sorted(game_root.rglob("*.meta.json")))
    return meta_paths


def _resolve_source_clip(raw_path: Any) -> Path | None:
    text = str(raw_path).strip() if raw_path is not None else ""
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path.resolve()
    return (_PROJECT_ROOT / path).resolve()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return round(default, 3)


def _asset_training_offsets(yolo_cfg: dict[str, Any]) -> list[float]:
    raw = yolo_cfg.get("asset_training_time_offsets_seconds", [0.0])
    if isinstance(raw, (int, float)):
        raw = [raw]
    if not isinstance(raw, list):
        return [0.0]

    offsets: list[float] = []
    seen: set[float] = set()
    for value in raw:
        try:
            offset = round(float(value), 3)
        except (TypeError, ValueError):
            continue
        if offset in seen:
            continue
        seen.add(offset)
        offsets.append(offset)
    return offsets or [0.0]


def _offset_slug(value: float) -> str:
    rounded = round(float(value), 3)
    if rounded == 0.0:
        return "base"
    sign = "p" if rounded > 0 else "m"
    millis = int(round(abs(rounded) * 1000))
    return f"{sign}{millis:04d}"


def _read_video_frame(clip_path: Path, frame_time_seconds: float, cv2: Any):
    cap = cv2.VideoCapture(str(clip_path))
    try:
        if not cap.isOpened():
            return None
        cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, float(frame_time_seconds)) * 1000.0)
        ok, frame = cap.read()
        return frame if ok else None
    finally:
        cap.release()


def _normalize_frame_1920(frame: Any, cv2: Any):
    height, width = frame.shape[:2]
    if width == 1920 and height == 1080:
        return frame
    return cv2.resize(frame, (1920, 1080), interpolation=cv2.INTER_LINEAR)


def _crop_roi_with_bbox(
    frame: Any,
    roi: dict[str, int],
    crop_box: dict[str, int],
) -> tuple[Any | None, dict[str, float] | None]:
    rx, ry, rw, rh = roi["x"], roi["y"], roi["w"], roi["h"]
    roi_image = frame[ry:ry + rh, rx:rx + rw]
    if getattr(roi_image, "size", 0) == 0:
        return None, None

    left = crop_box["x"] - rx
    top = crop_box["y"] - ry
    right = left + crop_box["w"]
    bottom = top + crop_box["h"]
    if right <= 0 or bottom <= 0 or left >= rw or top >= rh:
        return None, None

    left = max(0, left)
    top = max(0, top)
    right = min(rw, right)
    bottom = min(rh, bottom)
    box_w = max(1, right - left)
    box_h = max(1, bottom - top)
    bbox_norm = {
        "cx": round((left + box_w / 2) / rw, 6),
        "cy": round((top + box_h / 2) / rh, 6),
        "w": round(box_w / rw, 6),
        "h": round(box_h / rh, 6),
    }
    return roi_image, bbox_norm


def _icon_seed_export_samples(
    seed_manifest: dict[str, Any],
    entity_labels: dict[str, dict[str, Any]],
    weapon_roi: dict[str, int] | None,
    cv2: Any,
    np: Any,
    n_augment: int = 0,
    rng: Any = None,
) -> list[dict[str, Any]]:
    if not weapon_roi:
        return []

    samples: list[dict[str, Any]] = []
    for item in ((seed_manifest.get("sources") or {}).get("icons") or []):
        entity_id = str(item.get("entity_id", "")).strip()
        label_entry = entity_labels.get(entity_id)
        if not label_entry:
            continue
        image = cv2.imread(str(item.get("source_path")), cv2.IMREAD_UNCHANGED)
        if image is None:
            samples.append({"warning": f"{item.get('source_path')}: could not load icon seed image."})
            continue
        canvas, bbox_norm = _place_seed_on_canvas(image, weapon_roi["w"], weapon_roi["h"], cv2, np)
        roi_box = {**weapon_roi, "base_width": weapon_roi["w"], "base_height": weapon_roi["h"]}
        source_path_str = str(item.get("source_path"))
        samples.append({
            "sample_id": f"{label_entry['label']}__icon_seed",
            "source_type": "icon_seed",
            "label": label_entry["label"],
            "class_id": label_entry["class_id"],
            "maps_to": label_entry.get("maps_to"),
            "display_name": label_entry.get("display_name"),
            "split_key": source_path_str,
            "image_array": canvas,
            "bbox_norm": bbox_norm,
            "image_ext": ".png",
            "source_path": source_path_str,
            "roi_box": roi_box,
        })

        if n_augment > 0 and rng is not None:
            for aug_idx, (aug_canvas, aug_bbox) in enumerate(
                _augment_seed_variants(image, weapon_roi["w"], weapon_roi["h"], n_augment, cv2, np, rng)
            ):
                samples.append({
                    "sample_id": f"{label_entry['label']}__icon_seed_aug{aug_idx:03d}",
                    "source_type": "icon_seed_augmented",
                    "label": label_entry["label"],
                    "class_id": label_entry["class_id"],
                    "maps_to": label_entry.get("maps_to"),
                    "display_name": label_entry.get("display_name"),
                    "split_key": source_path_str,
                    "image_array": aug_canvas,
                    "bbox_norm": aug_bbox,
                    "image_ext": ".png",
                    "source_path": source_path_str,
                    "roi_box": roi_box,
                })
    return samples


def _roi_template_export_samples(
    seed_manifest: dict[str, Any],
    label_map: dict[str, dict[str, Any]],
    roi_map: dict[str, dict[str, int]],
    cv2: Any,
    np: Any,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for item in ((seed_manifest.get("sources") or {}).get("roi_templates") or []):
        template_id = str(item.get("template_id", "")).strip()
        label_entry = next(
            (entry for entry in label_map.values() if entry["label"] == template_id or entry.get("maps_to") == template_id),
            None,
        )
        if not label_entry:
            continue
        roi_ref = str(item.get("roi_ref") or "").strip()
        roi = roi_map.get(roi_ref)
        if not roi:
            samples.append({"warning": f"{item.get('source_path')}: roi_ref '{roi_ref}' is missing for ROI template export."})
            continue
        image = cv2.imread(str(item.get("source_path")), cv2.IMREAD_UNCHANGED)
        if image is None:
            samples.append({"warning": f"{item.get('source_path')}: could not load ROI template seed image."})
            continue
        canvas, bbox_norm = _place_seed_on_canvas(image, roi["w"], roi["h"], cv2, np)
        samples.append({
            "sample_id": f"{label_entry['label']}__roi_template_seed",
            "source_type": "roi_template_seed",
            "label": label_entry["label"],
            "class_id": label_entry["class_id"],
            "maps_to": label_entry.get("maps_to"),
            "display_name": label_entry.get("display_name"),
            "split_key": str(item.get("source_path")),
            "image_array": canvas,
            "bbox_norm": bbox_norm,
            "image_ext": ".png",
            "source_path": str(item.get("source_path")),
            "roi_box": {
                **roi,
                "base_width": roi["w"],
                "base_height": roi["h"],
            },
            "roi_ref": roi_ref,
        })
    return samples


def _place_seed_on_canvas(
    image: Any,
    canvas_w: int,
    canvas_h: int,
    cv2: Any,
    np: Any,
    scale_jitter: float = 1.0,
    pos_jitter_x: int = 0,
    pos_jitter_y: int = 0,
) -> tuple[Any, dict[str, float]]:
    if image.ndim == 2:
        image = cv2.cvtColor(image, cv2.COLOR_GRAY2BGRA)
    elif image.shape[2] == 3:
        image = cv2.cvtColor(image, cv2.COLOR_BGR2BGRA)

    margin = 8
    max_w = max(1, canvas_w - margin * 2)
    max_h = max(1, canvas_h - margin * 2)
    src_h, src_w = image.shape[:2]
    scale = min(max_w / max(src_w, 1), max_h / max(src_h, 1), 1.0)
    scale = scale * max(0.6, min(float(scale_jitter), 1.4))
    target_w = max(1, int(round(src_w * scale)))
    target_h = max(1, int(round(src_h * scale)))
    resized = cv2.resize(image, (target_w, target_h), interpolation=cv2.INTER_AREA if scale < 1.0 else cv2.INTER_LINEAR)

    canvas = np.zeros((canvas_h, canvas_w, 4), dtype=np.uint8)
    cx = (canvas_w - target_w) // 2 + int(pos_jitter_x)
    cy = (canvas_h - target_h) // 2 + int(pos_jitter_y)
    x = max(0, min(cx, max(0, canvas_w - target_w)))
    y = max(0, min(cy, max(0, canvas_h - target_h)))
    alpha = resized[:, :, 3:4].astype(np.float32) / 255.0
    canvas_region = canvas[y:y + target_h, x:x + target_w, :3].astype(np.float32)
    image_region = resized[:, :, :3].astype(np.float32)
    canvas[y:y + target_h, x:x + target_w, :3] = ((alpha * image_region) + ((1.0 - alpha) * canvas_region)).astype(np.uint8)
    canvas[y:y + target_h, x:x + target_w, 3] = 255

    bbox_norm = {
        "cx": round((x + target_w / 2) / canvas_w, 6),
        "cy": round((y + target_h / 2) / canvas_h, 6),
        "w": round(target_w / canvas_w, 6),
        "h": round(target_h / canvas_h, 6),
    }
    return canvas[:, :, :3], bbox_norm


def _augment_seed_variants(
    image: Any,
    canvas_w: int,
    canvas_h: int,
    n_variants: int,
    cv2: Any,
    np: Any,
    rng: Any,
) -> list[tuple[Any, dict[str, float]]]:
    """Generate n_variants augmented icon-on-canvas images from a single source icon.

    Augmentations simulate real capture variability: brightness, noise, compression
    blur, and slight scale/position shifts within the HUD crop region.
    """
    results = []
    for _ in range(n_variants):
        aug = image.copy().astype(np.float32)

        # Brightness + contrast jitter
        brightness = rng.uniform(0.72, 1.28)
        contrast = rng.uniform(0.85, 1.15)
        aug = aug * brightness
        mean = aug.mean()
        aug = (aug - mean) * contrast + mean
        aug = np.clip(aug, 0, 255).astype(np.uint8)

        # Gaussian noise (simulates sensor/compression noise)
        noise_sigma = rng.uniform(0, 9)
        if noise_sigma > 0.5:
            noise = rng.normal(0, noise_sigma, aug.shape).astype(np.float32)
            aug = np.clip(aug.astype(np.float32) + noise, 0, 255).astype(np.uint8)

        # Slight blur (simulates video encoding artifacts)
        blur_k = int(rng.choice([0, 0, 1, 1, 2]))
        if blur_k > 0:
            aug = cv2.GaussianBlur(aug, (blur_k * 2 + 1, blur_k * 2 + 1), 0)

        # Scale and position jitter when placing on canvas
        scale_jitter = float(rng.uniform(0.88, 1.10))
        pos_jitter_x = int(rng.integers(-5, 6))
        pos_jitter_y = int(rng.integers(-5, 6))

        canvas, bbox_norm = _place_seed_on_canvas(
            aug, canvas_w, canvas_h, cv2, np,
            scale_jitter=scale_jitter,
            pos_jitter_x=pos_jitter_x,
            pos_jitter_y=pos_jitter_y,
        )
        results.append((canvas, bbox_norm))
    return results


def _assign_splits(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not samples:
        return []

    ordered = sorted(samples, key=lambda item: (item["label"], item["source_type"], item["split_key"]))
    for index, sample in enumerate(ordered):
        sample["split"] = "val" if (index % 5 == 0) else "train"

    if all(sample["split"] == "val" for sample in ordered):
        ordered[0]["split"] = "train"
    return ordered


def _write_exported_samples(samples: list[dict[str, Any]], model_dir: Path, cv2: Any) -> list[dict[str, Any]]:
    exported: list[dict[str, Any]] = []
    for sample in samples:
        split = sample["split"]
        image_name = f"{sample['sample_id']}{sample.get('image_ext', '.png')}"
        label_name = f"{sample['sample_id']}.txt"
        image_path = model_dir / "images" / split / image_name
        label_path = model_dir / "labels" / split / label_name
        cv2.imwrite(str(image_path), sample["image_array"])
        bbox = sample["bbox_norm"]
        label_path.write_text(
            f"{sample['class_id']} {bbox['cx']:.6f} {bbox['cy']:.6f} {bbox['w']:.6f} {bbox['h']:.6f}\n"
        )
        exported.append({
            key: value
            for key, value in sample.items()
            if key not in {"image_array", "image_ext"}
        } | {
            "image_path": str(image_path),
            "label_path": str(label_path),
        })
    return exported


def _class_counts(samples: list[dict[str, Any]], label_entries: list[dict[str, Any]]) -> dict[str, int]:
    counts = {entry["label"]: 0 for entry in label_entries}
    for sample in samples:
        counts[sample["label"]] = counts.get(sample["label"], 0) + 1
    return counts


def _dedupe_warnings(warnings: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for warning in warnings:
        text = str(warning).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
