from __future__ import annotations

import json
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from pipeline.game_pack import get_game_pack_dir

_VALID_FEEDBACK_TYPES = {
    "false_positive",
    "false_negative",
    "needs_roi_template",
    "recommend_retrain",
    "weight_adjustment",
}

_VALID_SOURCE_STAGES = {"queue", "quarantine", "manual"}
_YAML_FLOAT_FIELDS = ("accept", "reject", "quarantine")


def get_feedback_dir(game: str, config: dict) -> Path:
    path = get_game_pack_dir(game, config) / "feedback"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_feedback_log_path(game: str, config: dict) -> Path:
    return get_feedback_dir(game, config) / "review_feedback.jsonl"


def get_feedback_report_dir(game: str, config: dict) -> Path:
    path = get_game_pack_dir(game, config) / "reports" / "feedback"
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_feedback_entries(game: str, config: dict) -> list[dict[str, Any]]:
    path = get_feedback_log_path(game, config)
    if not path.exists():
        return []

    entries: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


def record_feedback(
    game: str,
    config: dict,
    *,
    clip_id: str,
    feedback_type: str,
    source_stage: str,
    clip_path: str | Path | None = None,
    meta_path: str | Path | None = None,
    detector: str | None = None,
    note: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if feedback_type not in _VALID_FEEDBACK_TYPES:
        raise ValueError(f"Unsupported feedback_type '{feedback_type}'")
    if source_stage not in _VALID_SOURCE_STAGES:
        raise ValueError(f"Unsupported source_stage '{source_stage}'")

    clip_path_obj = Path(clip_path) if clip_path else None
    meta_path_obj = Path(meta_path) if meta_path else (clip_path_obj.with_suffix(".meta.json") if clip_path_obj else None)
    meta = _load_json(meta_path_obj) if meta_path_obj else {}

    entry = {
        "feedback_id": uuid.uuid4().hex[:12],
        "recorded_at": _now_iso(),
        "game": game,
        "clip_id": clip_id,
        "clip_path": str(clip_path_obj) if clip_path_obj else meta.get("clip_path"),
        "source_stage": source_stage,
        "feedback_type": feedback_type,
        "detector": detector or "unspecified",
        "note": str(note or "").strip()[:500],
        "decision_status": ((meta.get("decision") or {}).get("status")),
        "quarantine_reason": ((meta.get("quarantine") or {}).get("reason")) or meta.get("quarantine_reason"),
        "context": {
            "player_entity": ((meta.get("context") or {}).get("player_entity")),
            "detected_event": ((meta.get("context") or {}).get("detected_event")),
        },
        "niceshot_profile": ((meta.get("niceshot_detection") or {}).get("profile")),
        "yolo_top_entity": (((meta.get("yolo_detection") or {}).get("top_entity") or {}).get("entity_id")),
        "details": details or {},
    }

    log_path = get_feedback_log_path(game, config)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")

    if meta_path_obj:
        meta.setdefault("review_feedback", [])
        meta["review_feedback"].append({
            "feedback_id": entry["feedback_id"],
            "recorded_at": entry["recorded_at"],
            "feedback_type": feedback_type,
            "source_stage": source_stage,
            "detector": entry["detector"],
            "note": entry["note"],
        })
        _write_json(meta_path_obj, meta)

    return entry


def summarize_feedback(game: str, config: dict, *, limit: int = 20) -> dict[str, Any]:
    pack_dir = get_game_pack_dir(game, config)
    entries = load_feedback_entries(game, config)
    counts = Counter(entry.get("feedback_type", "unknown") for entry in entries)
    detector_counts = Counter(entry.get("detector", "unspecified") for entry in entries)
    quarantine_counts = Counter(
        entry.get("quarantine_reason")
        for entry in entries
        if entry.get("quarantine_reason")
    )
    source_stage_counts = Counter(entry.get("source_stage", "manual") for entry in entries)

    roi_requests = Counter()
    for entry in entries:
        if entry.get("feedback_type") != "needs_roi_template":
            continue
        entity = ((entry.get("context") or {}).get("player_entity")) or "unknown"
        detector = entry.get("detector", "unspecified")
        roi_requests[f"{detector}:{entity}"] += 1

    feedback_cfg = _load_feedback_cfg(game, config)
    threshold_cfg = _load_threshold_cfg(game, config)
    weight_cfg = _load_weight_cfg(game, config)
    weight_update = _recommend_weight_update(threshold_cfg, weight_cfg, counts, quarantine_counts, feedback_cfg)
    retrain = _recommend_retrain(entries, counts, detector_counts, feedback_cfg)

    return {
        "game": game,
        "pack_dir": str(pack_dir),
        "total_entries": len(entries),
        "counts": dict(counts),
        "detector_counts": dict(detector_counts),
        "quarantine_reason_counts": dict(quarantine_counts),
        "source_stage_counts": dict(source_stage_counts),
        "roi_template_requests": [
            {"target": target, "count": count}
            for target, count in roi_requests.most_common(10)
        ],
        "weight_update": weight_update,
        "retrain_recommendation": retrain,
        "recent_entries": list(reversed(entries[-limit:])),
    }


def apply_feedback_updates(game: str, config: dict, *, dry_run: bool = False) -> dict[str, Any]:
    pack_dir = get_game_pack_dir(game, config)
    weights_path = pack_dir / "weights.yaml"
    weights_payload = _load_yaml(weights_path)
    clip_judge = weights_payload.setdefault("clip_judge", {})
    feedback_cfg = clip_judge.setdefault("feedback", {})

    summary = summarize_feedback(game, config, limit=20)
    weight_update = summary["weight_update"]
    retrain = summary["retrain_recommendation"]

    result = {
        "game": game,
        "dry_run": dry_run,
        "weight_update": weight_update,
        "retrain_recommendation": retrain,
        "weights_path": str(weights_path),
        "report_path": None,
        "applied": False,
    }

    report = {
        "generated_at": _now_iso(),
        "game": game,
        "dry_run": dry_run,
        "summary": summary,
    }
    report_path = get_feedback_report_dir(game, config) / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    result["report_path"] = str(report_path)

    if dry_run:
        report_path.write_text(json.dumps(report, indent=2))
        return result

    if weight_update.get("would_change"):
        clip_judge["thresholds"] = weight_update["thresholds"]
        clip_judge["composite_weights"] = weight_update["composite_weights"]
        result["applied"] = True

    feedback_cfg["last_reviewed_at"] = _now_iso()
    feedback_cfg["last_feedback_summary"] = {
        "total_entries": summary["total_entries"],
        "counts": summary["counts"],
        "detector_counts": summary["detector_counts"],
        "quarantine_reason_counts": summary["quarantine_reason_counts"],
    }
    feedback_cfg["pending_roi_template_requests"] = summary["roi_template_requests"]
    feedback_cfg["retrain_recommendation"] = retrain

    weights_path.write_text(yaml.safe_dump(weights_payload, sort_keys=False))
    report_path.write_text(json.dumps(report, indent=2))
    return result


def _load_feedback_cfg(game: str, config: dict) -> dict[str, Any]:
    pack_dir = get_game_pack_dir(game, config)
    weights = _load_yaml(pack_dir / "weights.yaml")
    return (((weights.get("clip_judge") or {}).get("feedback")) or {})


def _load_threshold_cfg(game: str, config: dict) -> dict[str, float]:
    pack_dir = get_game_pack_dir(game, config)
    weights = _load_yaml(pack_dir / "weights.yaml")
    thresholds = ((weights.get("clip_judge") or {}).get("thresholds")) or {}
    return {
        key: float(thresholds.get(key, default))
        for key, default in (("accept", 0.7), ("reject", 0.25), ("quarantine", 0.45))
    }


def _load_weight_cfg(game: str, config: dict) -> dict[str, float]:
    pack_dir = get_game_pack_dir(game, config)
    weights = _load_yaml(pack_dir / "weights.yaml")
    composite = ((weights.get("clip_judge") or {}).get("composite_weights")) or {}
    if not composite:
        composite = {
            "ai_clip_score": 0.35,
            "ai_hook_score": 0.25,
            "kill_feed_score": 0.20,
            "audio_score": 0.10,
            "context_score": 0.10,
        }
    return {key: float(value) for key, value in composite.items()}


def _recommend_weight_update(
    thresholds: dict[str, float],
    composite_weights: dict[str, float],
    counts: Counter,
    quarantine_counts: Counter,
    feedback_cfg: dict[str, Any],
) -> dict[str, Any]:
    false_positive = int(counts.get("false_positive", 0))
    false_negative = int(counts.get("false_negative", 0))
    roi_requests = int(counts.get("needs_roi_template", 0))

    accept = thresholds["accept"]
    reject = thresholds["reject"]
    quarantine = thresholds["quarantine"]
    updated_weights = dict(composite_weights)
    changes: list[str] = []

    threshold_step = float(feedback_cfg.get("threshold_step", 0.01))
    weight_step = float(feedback_cfg.get("weight_step", 0.05))

    if false_positive > false_negative:
        delta = min(0.03, threshold_step * min(false_positive - false_negative, 3))
        accept = min(0.9, accept + delta)
        reject = min(0.5, reject + (delta / 2.0))
        changes.append(f"Raised accept threshold by {round(delta, 3)} due to false positives.")
    elif false_negative > false_positive:
        delta = min(0.03, threshold_step * min(false_negative - false_positive, 3))
        accept = max(0.5, accept - delta)
        reject = max(0.1, reject - (delta / 2.0))
        changes.append(f"Lowered accept threshold by {round(delta, 3)} due to false negatives.")

    context_pressure = int(quarantine_counts.get("missing_context", 0)) + roi_requests
    if context_pressure >= 2:
        _shift_weight(updated_weights, "context_score", "ai_clip_score", weight_step)
        changes.append("Shifted composite weight toward context_score because context failures are recurring.")

    if int(quarantine_counts.get("hook_not_resolved", 0)) >= 2:
        _shift_weight(updated_weights, "ai_hook_score", "audio_score", weight_step)
        changes.append("Shifted composite weight toward ai_hook_score because hook failures are recurring.")

    normalized_weights = _normalize_weights(updated_weights)
    updated_thresholds = {
        "accept": round(accept, 3),
        "reject": round(reject, 3),
        "quarantine": round(quarantine, 3),
    }

    would_change = (
        any(round(thresholds[key], 3) != updated_thresholds[key] for key in _YAML_FLOAT_FIELDS)
        or normalized_weights != _normalize_weights(composite_weights)
    )

    return {
        "would_change": would_change,
        "changes": changes,
        "thresholds": updated_thresholds,
        "composite_weights": normalized_weights,
    }


def _recommend_retrain(
    entries: list[dict[str, Any]],
    counts: Counter,
    detector_counts: Counter,
    feedback_cfg: dict[str, Any],
) -> dict[str, Any]:
    explicit = int(counts.get("recommend_retrain", 0))
    threshold = int(feedback_cfg.get("retrain_threshold", 3))
    yolo_related = sum(
        1 for entry in entries
        if entry.get("detector") in {"yolo_detector", "weapon_detector"}
        and entry.get("feedback_type") in {"false_negative", "needs_roi_template", "recommend_retrain"}
    )

    if explicit > 0:
        return {
            "recommended": True,
            "reason": "Reviewer explicitly requested YOLO retraining.",
            "related_feedback_count": explicit,
            "threshold": threshold,
        }

    recommended = yolo_related >= threshold
    if recommended:
        reason = "YOLO-related feedback crossed the retrain threshold."
    elif detector_counts.get("yolo_detector", 0):
        reason = "YOLO feedback exists but has not crossed the retrain threshold yet."
    else:
        reason = "No YOLO-specific retrain pressure recorded yet."

    return {
        "recommended": recommended,
        "reason": reason,
        "related_feedback_count": yolo_related,
        "threshold": threshold,
    }


def _shift_weight(weights: dict[str, float], gain_key: str, loss_key: str, amount: float) -> None:
    gain = float(weights.get(gain_key, 0.0))
    loss = float(weights.get(loss_key, 0.0))
    shift = min(amount, max(loss - 0.05, 0.0))
    if shift <= 0:
        return
    weights[gain_key] = gain + shift
    weights[loss_key] = loss - shift


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    total = sum(max(float(value), 0.0) for value in weights.values())
    if total <= 0:
        return {key: 0.0 for key in weights}
    normalized = {
        key: round(max(float(value), 0.0) / total, 3)
        for key, value in weights.items()
    }
    # Keep the sum at exactly 1.0 by compensating in the largest bucket.
    drift = round(1.0 - sum(normalized.values()), 3)
    if drift:
        top_key = max(normalized, key=normalized.get)
        normalized[top_key] = round(normalized[top_key] + drift, 3)
    return normalized


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
