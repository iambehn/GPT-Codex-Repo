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
        raw_detections = _run_model_inference(clip, Path(weights_path), cfg, game_pack)
    except ImportError as e:
        return _write_and_return(meta_path, {**base, "status": "missing_dependency", "reason": str(e)})
    except Exception as e:
        logger.warning(f"[yolo_detector] Failed for {clip.name}: {e}")
        return _write_and_return(meta_path, {**base, "status": "error", "reason": str(e)})

    detections = _map_detections(raw_detections, cfg, game_pack)
    top_entity = _top_entity(detections)
    event_candidates = _event_candidates(detections)
    timing = _video_timing(clip)
    timing["vid_stride"] = int(cfg.get("vid_stride", 1))
    timing["timestamp_source"] = _dominant_timestamp_source(detections, timing)
    result = {
        **base,
        "status": "ok",
        "detections": detections,
        "top_entity": top_entity,
        "event_candidates": event_candidates,
        "context_confidence": _context_confidence(top_entity, event_candidates),
        "timing": timing,
    }
    return _write_and_return(meta_path, result)


def _merged_config(config: dict, game_pack: dict) -> dict:
    global_cfg = dict(config.get("yolo_detector") or {})
    hud = game_pack.get("hud") or {}
    detector_cfg = dict(((hud.get("detectors") or {}).get("yolo")) or {})
    merged = {
        "enabled": bool(global_cfg.get("enabled", False)),
        "inference_mode": str(global_cfg.get("inference_mode", "video")),
        "confidence_threshold": float(global_cfg.get("confidence_threshold", 0.60)),
        "iou_threshold": float(global_cfg.get("iou_threshold", 0.45)),
        "imgsz": int(global_cfg.get("imgsz", 640)),
        "max_det": int(global_cfg.get("max_det", 100)),
        "vid_stride": int(global_cfg.get("vid_stride", 1)),
        "frame_sample": str(global_cfg.get("frame_sample", "middle")),
        "max_samples": int(global_cfg.get("max_samples", 24)),
        "roi_ref": global_cfg.get("roi_ref"),
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
        "inference_mode": str(cfg.get("inference_mode", "video")),
        "roi_ref": cfg.get("roi_ref"),
        "weights_path": str(weights_path) if weights_path else None,
        "confidence_threshold": float(cfg.get("confidence_threshold", 0.60)),
        "detections": [],
        "top_entity": None,
        "event_candidates": [],
        "context_confidence": 0.0,
        "timing": {
            "fps": None,
            "duration_seconds": None,
            "frame_count": None,
            "vid_stride": int(cfg.get("vid_stride", 1)),
            "timestamp_source": "unknown",
        },
    }


def _run_model_inference(clip: Path, weights_path: Path, cfg: dict, game_pack: dict) -> list[dict[str, Any]]:
    if str(cfg.get("inference_mode", "video")).strip().lower() == "roi_crop":
        return _run_roi_crop_inference(clip, weights_path, cfg, game_pack)
    return _run_video_inference(clip, weights_path, cfg)


def _run_video_inference(clip: Path, weights_path: Path, cfg: dict) -> list[dict[str, Any]]:
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
        vid_stride=max(1, int(cfg.get("vid_stride", 1))),
        verbose=bool(cfg.get("verbose", False)),
    )
    timing = _video_timing(clip)
    timing["vid_stride"] = max(1, int(cfg.get("vid_stride", 1)))
    return _extract_result_detections(results, timing)


def _run_roi_crop_inference(clip: Path, weights_path: Path, cfg: dict, game_pack: dict) -> list[dict[str, Any]]:
    try:
        import cv2
        from ultralytics import YOLO
    except ImportError as e:
        raise ImportError("opencv-python-headless and ultralytics are required for ROI-crop YOLO inference") from e

    roi = _resolve_inference_roi(cfg, game_pack)
    samples, timing = _sample_roi_frames(clip, cfg, roi, cv2)
    if not samples:
        return []

    model = YOLO(str(weights_path))
    detections: list[dict[str, Any]] = []
    for sample in samples:
        results = model.predict(
            source=sample["image"],
            conf=float(cfg.get("confidence_threshold", 0.60)),
            iou=float(cfg.get("iou_threshold", 0.45)),
            imgsz=int(cfg.get("imgsz", 640)),
            max_det=int(cfg.get("max_det", 100)),
            verbose=bool(cfg.get("verbose", False)),
        )
        raw = _extract_result_detections(results, {"vid_stride": 1})
        for item in raw:
            mapped_box = _map_roi_box_to_frame(item.get("box"), sample["roi_box"])
            item["frame_index"] = sample["frame_index"]
            item["timestamp"] = sample["timestamp"]
            item["timestamp_source"] = sample["timestamp_source"]
            item["box"] = mapped_box
            item["roi_box"] = dict(sample["roi_box"])
            item["inference_mode"] = "roi_crop"
            detections.append(item)

    if timing:
        for item in detections:
            item.setdefault("timestamp_source", _timestamp_source(timing, len(samples)))
    return detections


def _extract_result_detections(results: Any, timing: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    detections: list[dict[str, Any]] = []
    frames = list(results or [])
    timing = timing or {}
    timestamp_source = _timestamp_source(timing, len(frames))

    for frame_index, result in enumerate(frames):
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
                "timestamp": _estimate_timestamp(frame_index, timing, len(frames)),
                "timestamp_source": timestamp_source,
            })
    return detections


def _resolve_inference_roi(cfg: dict, game_pack: dict) -> dict[str, Any]:
    hud = game_pack.get("hud") or {}
    rois = hud.get("rois") or {}
    roi_ref = str(cfg.get("roi_ref") or "").strip()
    if not roi_ref:
        weapon_detector = (hud.get("detectors") or {}).get("weapon_detector") or {}
        roi_ref = str(weapon_detector.get("roi_ref") or "weapon_detector")

    roi = rois.get(roi_ref)
    if not isinstance(roi, dict):
        raise RuntimeError(f"YOLO ROI '{roi_ref}' is not defined in hud.yaml")

    try:
        return {
            "roi_ref": roi_ref,
            "x": int(round(float(roi["x"]))),
            "y": int(round(float(roi["y"]))),
            "w": int(round(float(roi["w"]))),
            "h": int(round(float(roi["h"]))),
            "base_width": 1920,
            "base_height": 1080,
        }
    except (KeyError, TypeError, ValueError) as e:
        raise RuntimeError(f"YOLO ROI '{roi_ref}' is invalid") from e


def _sample_roi_frames(clip: Path, cfg: dict, roi: dict[str, Any], cv2: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    timing = _video_timing(clip)
    timing["vid_stride"] = max(1, int(cfg.get("vid_stride", 1)))
    samples: list[dict[str, Any]] = []

    cap = cv2.VideoCapture(str(clip))
    try:
        if not cap.isOpened():
            raise RuntimeError(f"Could not open clip for ROI-crop inference: {clip}")

        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        indices = _select_frame_indices(frame_count, cfg)
        if not indices:
            indices = [0]

        for frame_index in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, int(frame_index))
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            normalized = _normalize_frame_1920(frame, cv2)
            crop = _crop_roi_image(normalized, roi)
            if crop is None:
                continue
            samples.append({
                "image": crop,
                "frame_index": int(frame_index),
                "timestamp": _frame_index_timestamp(int(frame_index), timing, frame_count),
                "timestamp_source": _timestamp_source(timing, max(frame_count, len(indices))),
                "roi_box": roi,
            })
    finally:
        cap.release()

    return samples, timing


def _select_frame_indices(frame_count: int, cfg: dict) -> list[int]:
    if frame_count <= 0:
        return [0]

    mode = str(cfg.get("frame_sample", "middle")).strip().lower()
    vid_stride = max(1, int(cfg.get("vid_stride", 1)))
    max_samples = max(1, int(cfg.get("max_samples", 24)))

    if mode == "middle":
        return [max(0, frame_count // 2)]

    if mode in {"first", "start"}:
        return [0]

    indices = list(range(0, frame_count, vid_stride))
    if not indices:
        indices = [0]
    if len(indices) > max_samples:
        step = max(1, len(indices) // max_samples)
        indices = indices[::step][:max_samples]
    return sorted(set(max(0, min(frame_count - 1, idx)) for idx in indices))


def _normalize_frame_1920(frame: Any, cv2: Any):
    height, width = frame.shape[:2]
    if width == 1920 and height == 1080:
        return frame
    return cv2.resize(frame, (1920, 1080), interpolation=cv2.INTER_LINEAR)


def _crop_roi_image(frame: Any, roi: dict[str, Any]):
    x, y, w, h = int(roi["x"]), int(roi["y"]), int(roi["w"]), int(roi["h"])
    crop = frame[y:y + h, x:x + w]
    if getattr(crop, "size", 0) == 0:
        return None
    return crop


def _frame_index_timestamp(frame_index: int, timing: dict[str, Any], frame_total: int) -> float:
    fps = timing.get("fps")
    if isinstance(fps, (int, float)) and fps and fps > 0:
        return round(frame_index / float(fps), 3)
    if frame_total > 0:
        return _estimate_timestamp(frame_index, {**timing, "vid_stride": 1}, frame_total)
    return 0.0


def _map_roi_box_to_frame(box: Any, roi: dict[str, Any]) -> list[float]:
    try:
        x1, y1, x2, y2 = [float(value) for value in list(box)[:4]]
    except (TypeError, ValueError):
        return []
    return [
        round(x1 + float(roi["x"]), 3),
        round(y1 + float(roi["y"]), 3),
        round(x2 + float(roi["x"]), 3),
        round(y2 + float(roi["y"]), 3),
    ]


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
    if not result.get("timing"):
        result["timing"] = {
            "fps": None,
            "duration_seconds": None,
            "frame_count": None,
            "vid_stride": None,
            "timestamp_source": "unknown",
        }
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


def _dominant_timestamp_source(detections: list[dict[str, Any]], timing: dict[str, Any]) -> str:
    for detection in detections:
        source = detection.get("timestamp_source")
        if source:
            return str(source)
    return _timestamp_source(timing, 0)


def _video_timing(clip: Path) -> dict[str, Any]:
    timing = {
        "fps": None,
        "duration_seconds": None,
        "frame_count": None,
    }

    try:
        import cv2
    except ImportError:
        cv2 = None

    if cv2 is not None:
        cap = cv2.VideoCapture(str(clip))
        try:
            if cap.isOpened():
                fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
                frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
                if fps > 0:
                    timing["fps"] = round(float(fps), 3)
                if frame_count > 0:
                    timing["frame_count"] = frame_count
                if fps > 0 and frame_count > 0:
                    timing["duration_seconds"] = round(frame_count / fps, 3)
        finally:
            cap.release()

    if timing["duration_seconds"] is None:
        meta = _load_meta(clip.with_suffix(".meta.json"))
        try:
            duration = float(meta.get("duration_seconds", 0.0))
        except (TypeError, ValueError):
            duration = 0.0
        if duration > 0:
            timing["duration_seconds"] = round(duration, 3)

    return timing


def _timestamp_source(timing: dict[str, Any], frame_total: int) -> str:
    fps = timing.get("fps")
    duration = timing.get("duration_seconds")
    if isinstance(fps, (int, float)) and fps and fps > 0:
        return "video_fps"
    if isinstance(duration, (int, float)) and duration and duration > 0 and frame_total > 1:
        return "duration_interpolation"
    return "unknown"


def _estimate_timestamp(frame_index: int, timing: dict[str, Any], frame_total: int) -> float:
    fps = timing.get("fps")
    duration = timing.get("duration_seconds")
    vid_stride = max(1, int(timing.get("vid_stride", 1) or 1))

    if isinstance(fps, (int, float)) and fps and fps > 0:
        return round((frame_index * vid_stride) / float(fps), 3)

    if isinstance(duration, (int, float)) and duration and duration > 0:
        if frame_total <= 1:
            return 0.0
        return round((frame_index / max(frame_total - 1, 1)) * float(duration), 3)

    return 0.0
