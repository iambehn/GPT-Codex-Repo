from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.game_pack import get_primary_entities, load_game_pack, resolve_asset_path
from utils.logger import get_logger

logger = get_logger(__name__)


def run_yolo_detector(
    clip_path: str | Path,
    game: str,
    config: dict,
    game_pack: dict | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Run optional YOLOv8 inference and write normalized detections to meta."""
    clip = Path(clip_path)
    meta_path = clip.with_suffix(".meta.json")

    if meta_path.exists() and not force:
        existing = _load_meta(meta_path)
        if "yolo_detection" in existing:
            logger.debug(f"[yolo_detector] Already processed: {clip.name}")
            return existing["yolo_detection"]

    game_pack = game_pack or load_game_pack(game, config)
    cfg = _merged_config(config, game_pack)
    base = _base_result(cfg, game_pack)
    if not cfg.get("enabled", False):
        return _write_and_return(meta_path, {**base, "status": "disabled"})

    weights_path = base.get("weights_path", "")
    if not weights_path or not Path(weights_path).exists():
        return _write_and_return(meta_path, {
            **base,
            "status": "missing_weights",
            "reason": f"YOLO weights not found: {weights_path or 'not configured'}",
        })

    try:
        raw_detections = _run_model_inference(clip, Path(weights_path), cfg)
    except ImportError as e:
        return _write_and_return(meta_path, {**base, "status": "missing_dependency", "reason": str(e)})
    except Exception as e:
        logger.warning(f"[yolo_detector] Failed for {clip.name}: {e}")
        return _write_and_return(meta_path, {**base, "status": "error", "reason": str(e)})

    detections = _map_detections(raw_detections, cfg, game_pack)
    top_entity = _top_entity(detections)
    event_candidates = _event_candidates(detections)
    result = {
        **base,
        "status": "ok",
        "detections": detections,
        "top_entity": top_entity,
        "event_candidates": event_candidates,
        "context_confidence": _context_confidence(top_entity, event_candidates),
    }
    return _write_and_return(meta_path, result)


def _merged_config(config: dict, game_pack: dict) -> dict:
    global_cfg = dict(config.get("yolo_detector") or {})
    hud = game_pack.get("hud") or {}
    detector_cfg = dict(((hud.get("detectors") or {}).get("yolo")) or {})
    merged = {
        "enabled": bool(global_cfg.get("enabled", False)),
        "confidence_threshold": float(global_cfg.get("confidence_threshold", 0.60)),
        "iou_threshold": float(global_cfg.get("iou_threshold", 0.45)),
        "imgsz": int(global_cfg.get("imgsz", 640)),
        "max_det": int(global_cfg.get("max_det", 100)),
        "verbose": bool(global_cfg.get("verbose", False)),
    }
    merged.update(detector_cfg)
    merged["enabled"] = bool(global_cfg.get("enabled", False)) and bool(detector_cfg.get("enabled", False))
    return merged


def _base_result(cfg: dict, game_pack: dict) -> dict:
    pack_root = Path(game_pack.get("pack_root", "."))
    weights_path = resolve_asset_path(str(cfg.get("weights_path", "")), pack_root) if cfg.get("weights_path") else None
    return {
        "enabled": bool(cfg.get("enabled", False)),
        "status": "disabled",
        "weights_path": str(weights_path) if weights_path else None,
        "confidence_threshold": float(cfg.get("confidence_threshold", 0.60)),
        "detections": [],
        "top_entity": None,
        "event_candidates": [],
        "context_confidence": 0.0,
    }


def _run_model_inference(clip: Path, weights_path: Path, cfg: dict) -> list[dict[str, Any]]:
    try:
        from ultralytics import YOLO
    except ImportError as e:
        raise ImportError("ultralytics is not installed; install it to enable YOLOv8 inference") from e

    model = YOLO(str(weights_path))
    results = model.predict(
        source=str(clip),
        conf=float(cfg.get("confidence_threshold", 0.60)),
        iou=float(cfg.get("iou_threshold", 0.45)),
        imgsz=int(cfg.get("imgsz", 640)),
        max_det=int(cfg.get("max_det", 100)),
        verbose=bool(cfg.get("verbose", False)),
    )
    return _extract_result_detections(results)


def _extract_result_detections(results: Any) -> list[dict[str, Any]]:
    detections: list[dict[str, Any]] = []
    for frame_index, result in enumerate(results or []):
        names = getattr(result, "names", {}) or {}
        boxes = getattr(result, "boxes", None)
        if boxes is None:
            continue

        iterable = boxes if isinstance(boxes, list) else list(boxes)
        for box in iterable:
            class_id = _scalar(getattr(box, "cls", None), default=0, as_int=True)
            confidence = _scalar(getattr(box, "conf", None), default=0.0, as_int=False)
            label = names.get(class_id, str(class_id)) if isinstance(names, dict) else str(class_id)
            detections.append({
                "label": label,
                "class_id": class_id,
                "confidence": round(float(confidence), 3),
                "box": _xyxy(getattr(box, "xyxy", None)),
                "frame_index": frame_index,
                "timestamp": 0.0,
            })
    return detections


def _map_detections(raw_detections: list[dict[str, Any]], cfg: dict, game_pack: dict) -> list[dict[str, Any]]:
    label_map = cfg.get("labels") or {}
    primary_kind, primary_entities = get_primary_entities(game_pack)
    moment_ids = {item.get("id") for item in ((game_pack.get("moments") or {}).get("moments") or [])}

    mapped: list[dict[str, Any]] = []
    for raw in raw_detections:
        label = str(raw.get("label", raw.get("class_id", "")))
        mapping = _normalize_label_mapping(label_map.get(label), primary_entities, moment_ids)
        item = dict(raw)
        item["label"] = label
        item["kind"] = mapping.get("kind")
        item["maps_to"] = mapping.get("maps_to")
        if item["kind"] == "entity":
            item["entity_id"] = item["maps_to"]
            item["entity_kind"] = primary_kind
        elif item["kind"] == "event":
            item["event_id"] = item["maps_to"]
        mapped.append(item)
    mapped.sort(key=lambda item: float(item.get("confidence", 0.0)), reverse=True)
    return mapped


def _normalize_label_mapping(mapping: Any, entities: dict, moment_ids: set[str | None]) -> dict[str, str | None]:
    if isinstance(mapping, dict):
        maps_to = mapping.get("maps_to") or mapping.get("entity_id") or mapping.get("event_id")
        kind = mapping.get("kind")
        if not kind:
            kind = "entity" if maps_to in entities else "event" if maps_to in moment_ids else None
        return {"kind": kind, "maps_to": maps_to}
    if isinstance(mapping, str):
        kind = "entity" if mapping in entities else "event" if mapping in moment_ids else None
        return {"kind": kind, "maps_to": mapping}
    return {"kind": None, "maps_to": None}


def _top_entity(detections: list[dict[str, Any]]) -> dict[str, Any] | None:
    for detection in detections:
        if detection.get("kind") == "entity" and detection.get("entity_id"):
            return {
                "entity_id": detection["entity_id"],
                "label": detection.get("label"),
                "confidence": detection.get("confidence", 0.0),
                "box": detection.get("box"),
            }
    return None


def _event_candidates(detections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = []
    for detection in detections:
        if detection.get("kind") != "event" or not detection.get("event_id"):
            continue
        events.append({
            "event_id": detection["event_id"],
            "label": detection.get("label"),
            "confidence": detection.get("confidence", 0.0),
            "timestamp": detection.get("timestamp", 0.0),
            "box": detection.get("box"),
        })
    return events


def _context_confidence(top_entity: dict | None, event_candidates: list[dict]) -> float:
    scores = []
    if top_entity:
        scores.append(float(top_entity.get("confidence", 0.0)))
    if event_candidates:
        scores.append(max(float(event.get("confidence", 0.0)) for event in event_candidates))
    if not scores:
        return 0.0
    return round(sum(scores) / len(scores), 3)


def _write_and_return(meta_path: Path, result: dict) -> dict:
    meta = _load_meta(meta_path)
    meta["yolo_detection"] = result
    meta_path.write_text(json.dumps(meta, indent=2))
    return result


def _load_meta(meta_path: Path) -> dict:
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _scalar(value: Any, default: float, as_int: bool) -> int | float:
    try:
        if hasattr(value, "tolist"):
            value = value.tolist()
        while isinstance(value, list):
            value = value[0]
        return int(value) if as_int else float(value)
    except (TypeError, ValueError, IndexError):
        return int(default) if as_int else float(default)


def _xyxy(value: Any) -> list[float]:
    try:
        if hasattr(value, "tolist"):
            value = value.tolist()
        while value and isinstance(value[0], list):
            value = value[0]
        return [round(float(v), 3) for v in value[:4]]
    except (TypeError, ValueError, IndexError):
        return []
