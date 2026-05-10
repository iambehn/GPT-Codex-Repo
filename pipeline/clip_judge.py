from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import anthropic

from pipeline.game_pack import get_primary_entities, resolve_asset_path
from utils.logger import get_logger

logger = get_logger(__name__)

_FALLBACK_AI = {
    "worthiness_score": 0.50,
    "hook_score": 0.50,
    "confidence": 0.40,
    "hook_pass": False,
    "narrative_type": "unknown",
    "reason": "AI clip judge unavailable; used deterministic fallback only.",
}


def evaluate(clip_path: str | Path, game_pack: dict, config: dict, force: bool = False) -> dict[str, Any]:
    """Evaluate a clip before transcription and processing.

    Reads earlier detector results from the clip sidecar, builds candidate
    moments and context, then writes a structured decision manifest into the
    clip's .meta.json under:
      - candidate_moments
      - context
      - detector_outputs
      - decision
      - quarantine
      - clip_judge
    """
    clip = Path(clip_path)
    meta_path = clip.with_suffix(".meta.json")

    if not meta_path.exists():
        logger.error(f"[clip_judge] No meta.json found for {clip.name}")
        return {}

    meta = json.loads(meta_path.read_text())
    if not force and meta.get("clip_judge"):
        logger.debug(f"[clip_judge] Already evaluated: {clip.name}")
        return meta["clip_judge"]

    weights_cfg = ((game_pack.get("weights") or {}).get("clip_judge") or {})
    thresholds = weights_cfg.get("thresholds") or {}
    composite_weights = weights_cfg.get("composite_weights") or {}
    hard_gates = weights_cfg.get("hard_gates") or {}
    hook_window_seconds = float(hard_gates.get("hook_window_seconds", 1.5))
    required_context_fields = hard_gates.get("require_context_fields")
    if required_context_fields is None:
        required_context_fields = ["player_entity", "detected_event"]

    candidate_moments = _build_candidate_moments(meta, hook_window_seconds)
    context = _derive_context(meta, game_pack, candidate_moments)
    detector_outputs = _collect_detector_outputs(meta, game_pack)

    ai_result = _run_ai_clip_judge(meta, game_pack, candidate_moments, context, config)
    kill_feed_score = _normalize_kill_feed(meta)
    audio_score = _normalize_audio(meta)
    context_score = _normalize_context(context, required_context_fields)

    hook_gate = _resolve_hook_gate(meta, candidate_moments, ai_result, hook_window_seconds)
    hook_gate_passed = hook_gate["passed"]

    composite_inputs = {
        "ai_clip_score": float(ai_result.get("worthiness_score", 0.0)),
        "ai_hook_score": float(ai_result.get("hook_score", 0.0)),
        "kill_feed_score": kill_feed_score,
        "audio_score": audio_score,
        "context_score": context_score,
        "niceshot_score": _normalize_niceshot(meta),
        "yolo_event_score": _normalize_yolo(meta),
    }
    composite_score = _weighted_score(composite_inputs, composite_weights)

    quarantine_reason = None
    missing_fields = [field for field in required_context_fields if not context.get(field)]
    needs_roi_template = _needs_roi_template(game_pack, context)
    ui_drift = _suspect_ui_drift(meta, context, needs_roi_template)

    if needs_roi_template:
        decision_status = "quarantine"
        quarantine_reason = "needs_roi_template"
    elif ui_drift:
        decision_status = "quarantine"
        quarantine_reason = "ui_drift"
    elif missing_fields:
        decision_status = "quarantine"
        quarantine_reason = "missing_context"
    elif not hook_gate_passed:
        decision_status = "quarantine"
        quarantine_reason = "hook_not_resolved"
    elif composite_score >= float(thresholds.get("accept", 0.70)):
        decision_status = "accept"
    elif composite_score < float(thresholds.get("reject", 0.25)) and ai_result.get("confidence", 0.0) >= 0.60:
        decision_status = "reject"
    else:
        decision_status = "quarantine"
        quarantine_reason = "low_confidence"

    explanation = _build_explanation(
        ai_result=ai_result,
        composite_score=composite_score,
        hook_gate_passed=hook_gate_passed,
        candidate_moments=candidate_moments,
        missing_fields=missing_fields,
        quarantine_reason=quarantine_reason,
        context=context,
        hook_gate=hook_gate,
    )

    result = {
        "candidate_moments": candidate_moments,
        "context": context,
        "detector_outputs": {
            **detector_outputs,
            "ai_clip_judge": ai_result,
            "composite_inputs": {
                key: (round(value, 3) if value is not None else None)
                for key, value in composite_inputs.items()
            },
        },
        "decision": {
            "status": decision_status,
            "composite_score": round(composite_score, 3),
            "hook_gate_passed": hook_gate_passed,
            "top_hook_timestamp": hook_gate.get("expected_hook_timestamp"),
            "original_hook_timestamp": hook_gate.get("original_hook_timestamp"),
            "expected_final_hook_timestamp": hook_gate.get("expected_hook_timestamp"),
            "hook_alignment": hook_gate,
            "explanation": explanation,
        },
        "quarantine": {
            "reason": quarantine_reason,
            "missing_fields": missing_fields,
        } if decision_status == "quarantine" else {},
    }

    meta["candidate_moments"] = result["candidate_moments"]
    meta["context"] = result["context"]
    meta["detector_outputs"] = result["detector_outputs"]
    meta["decision"] = result["decision"]
    meta["quarantine"] = result["quarantine"]
    meta["clip_judge"] = result
    meta_path.write_text(json.dumps(meta, indent=2))

    logger.info(
        f"[clip_judge] {clip.name}: {decision_status.upper()} "
        f"(score={result['decision']['composite_score']}, hook={hook_gate_passed})"
    )
    return result


def _build_candidate_moments(meta: dict, hook_window_seconds: float) -> list[dict[str, Any]]:
    moments: list[dict[str, Any]] = []

    kill_feed = meta.get("kill_feed", {})
    kill_feed_events = kill_feed.get("events") or []
    if kill_feed_events:
        for event in kill_feed_events:
            if not isinstance(event, dict):
                continue
            kind = str(event.get("kind") or "kill")
            method = str(event.get("method") or kill_feed.get("method") or "unknown")
            moments.append({
                "timestamp": _safe_timestamp(event.get("timestamp", 0.0)),
                "source": "kill_feed",
                "kind": kind,
                "confidence": _clamp_float(event.get("confidence", 0.0)) or (0.92 if kind == "headshot" else 0.78),
                "hook_candidate": True,
                "hook_window_seconds": hook_window_seconds,
                "detail": f"method={method}",
                "evidence": {
                    "method": method,
                    "sweat_score": kill_feed.get("sweat_score"),
                },
            })
    else:
        for ts in kill_feed.get("kill_timestamps", []):
            moments.append({
                "timestamp": round(float(ts), 3),
                "source": "kill_feed",
                "kind": "kill",
                "confidence": 0.78,
                "hook_candidate": True,
                "hook_window_seconds": hook_window_seconds,
                "detail": f"method={kill_feed.get('method', 'unknown')}",
                "evidence": {
                    "method": kill_feed.get("method"),
                    "sweat_score": kill_feed.get("sweat_score"),
                },
            })
        for ts in kill_feed.get("headshot_timestamps", []):
            moments.append({
                "timestamp": round(float(ts), 3),
                "source": "kill_feed",
                "kind": "headshot",
                "confidence": 0.92,
                "hook_candidate": True,
                "hook_window_seconds": hook_window_seconds,
                "detail": f"method={kill_feed.get('method', 'unknown')}",
                "evidence": {
                    "method": kill_feed.get("method"),
                    "sweat_score": kill_feed.get("sweat_score"),
                },
            })

    audio_events = meta.get("audio_events", {})
    if isinstance(audio_events.get("events"), list) and audio_events.get("events"):
        for event in audio_events.get("events", []):
            if not isinstance(event, dict):
                continue
            moments.append({
                "timestamp": _safe_timestamp(event.get("timestamp", 0.0)),
                "source": "audio_detector",
                "kind": str(event.get("type", "audio_event")),
                "confidence": 0.6 if str(event.get("type")) == "multi_kill" else 0.55,
                "hook_candidate": False,
                "hook_window_seconds": hook_window_seconds,
                "detail": f"spike_count={event.get('spike_count', 1)}",
                "evidence": dict(event),
            })
    else:
        for ts in audio_events.get("spike_timestamps", []):
            moments.append({
                "timestamp": round(float(ts), 3),
                "source": "audio_detector",
                "kind": "audio_spike",
                "confidence": 0.55,
                "hook_candidate": False,
                "hook_window_seconds": hook_window_seconds,
                "detail": "spike",
            })

    niceshot = meta.get("niceshot_detection", {})
    if niceshot.get("status") == "ok":
        for moment in niceshot.get("moments", []):
            moments.append({
                "timestamp": _safe_timestamp(moment.get("timestamp", 0.0)),
                "source": "niceshot",
                "kind": str(moment.get("kind", "action_spike")),
                "confidence": _clamp_float(moment.get("confidence", niceshot.get("confidence", 0.0))),
                "hook_candidate": bool(moment.get("hook_candidate", True)),
                "hook_window_seconds": hook_window_seconds,
                "detail": "hook candidate" if moment.get("hook_candidate", True) else "",
                "evidence": {
                    "profile": niceshot.get("profile"),
                    "normalized_composite": (niceshot.get("normalized_scores") or {}).get("composite"),
                },
            })

    yolo = meta.get("yolo_detection", {})
    if yolo.get("status") == "ok":
        for event in yolo.get("event_candidates", []):
            moments.append({
                "timestamp": _safe_timestamp(event.get("timestamp", 0.0)),
                "source": "yolo_detector",
                "kind": str(event.get("event_id") or event.get("label") or "visual_event"),
                "confidence": _clamp_float(event.get("confidence", 0.0)),
                "hook_candidate": True,
                "hook_window_seconds": hook_window_seconds,
                "detail": str(event.get("label") or event.get("event_id") or "visual_event"),
                "evidence": {
                    "box": event.get("box"),
                    "label": event.get("label"),
                    "timestamp_source": event.get("timestamp_source") or (yolo.get("timing") or {}).get("timestamp_source"),
                },
            })

    deduped: dict[tuple[float, str, str], dict[str, Any]] = {}
    for moment in moments:
        key = (moment["timestamp"], moment["kind"], moment["source"])
        existing = deduped.get(key)
        if existing is None or moment["confidence"] > existing["confidence"]:
            deduped[key] = moment

    return sorted(deduped.values(), key=lambda item: (-item["confidence"], item["timestamp"]))


def _resolve_hook_gate(
    meta: dict,
    candidate_moments: list[dict[str, Any]],
    ai_result: dict[str, Any],
    hook_window_seconds: float,
) -> dict[str, Any]:
    hook_enforcer = meta.get("hook_enforcer") or {}
    trim_plan = hook_enforcer.get("trim_plan") or {}
    anchor = hook_enforcer.get("anchor_moment") or {}
    expected = trim_plan.get("expected_hook_timestamp")

    if hook_enforcer.get("status") == "ok":
        if hook_enforcer.get("early_hook_passed"):
            anchor_timestamp = anchor.get("timestamp")
            return {
                "passed": True,
                "mode": "early",
                "original_hook_timestamp": anchor_timestamp,
                "expected_hook_timestamp": anchor_timestamp,
                "trim_plan": trim_plan,
                "source": "hook_enforcer",
            }

        if (
            trim_plan.get("strategy") == "hard_trim"
            and anchor
            and _clamp_float(anchor.get("confidence", 0.0)) >= 0.5
            and expected is not None
            and _safe_timestamp(expected) <= hook_window_seconds
        ):
            return {
                "passed": True,
                "mode": "hard_trim",
                "original_hook_timestamp": anchor.get("timestamp"),
                "expected_hook_timestamp": _safe_timestamp(expected),
                "trim_plan": trim_plan,
                "source": "hook_enforcer",
            }

        return {
            "passed": False,
            "mode": trim_plan.get("strategy") or "unresolved",
            "original_hook_timestamp": anchor.get("timestamp") if anchor else None,
            "expected_hook_timestamp": expected,
            "trim_plan": trim_plan,
            "source": "hook_enforcer",
        }

    early_candidates = [
        moment for moment in candidate_moments
        if moment.get("hook_candidate", True) and _safe_timestamp(moment.get("timestamp")) <= hook_window_seconds
    ]
    passed = bool(early_candidates) and (
        ai_result.get("hook_pass", False) or ai_result.get("hook_score", 0.0) >= 0.65
    )
    top = early_candidates[0] if early_candidates else None
    return {
        "passed": passed,
        "mode": "legacy_early" if passed else "unresolved",
        "original_hook_timestamp": top.get("timestamp") if top else None,
        "expected_hook_timestamp": top.get("timestamp") if top else None,
        "trim_plan": {"strategy": "none" if passed else "unresolved"},
        "source": "candidate_moments",
    }


def _derive_context(meta: dict, game_pack: dict, candidate_moments: list[dict[str, Any]]) -> dict[str, Any]:
    game_meta = game_pack.get("game") or {}
    primary_kind, primary_entities = get_primary_entities(game_pack)
    weapon_detection = meta.get("weapon_detection", {})
    yolo_detection = meta.get("yolo_detection", {})
    detected_id = weapon_detection.get("weapon_id")

    detected_event = "action_spike"
    kill_feed = meta.get("kill_feed", {})
    audio_events = meta.get("audio_events", {})
    yolo_events = yolo_detection.get("event_candidates") or []
    if int(kill_feed.get("headshot_count", 0)) >= 2:
        detected_event = "multi_headshot"
    elif int(kill_feed.get("kill_count", 0)) >= 4:
        detected_event = "ace_candidate"
    elif int(kill_feed.get("kill_count", 0)) >= 3:
        detected_event = "team_wipe_candidate"
    elif int(kill_feed.get("kill_count", 0)) >= 2:
        detected_event = "multi_kill"
    elif int(kill_feed.get("kill_count", 0)) == 1:
        detected_event = "single_pick"
    elif audio_events.get("multi_kill_detected"):
        detected_event = "audio_multi_kill"
    elif yolo_events:
        detected_event = yolo_events[0].get("event_id") or yolo_events[0].get("label") or "visual_event"

    yolo_top_entity = yolo_detection.get("top_entity") or {}
    if not detected_id and yolo_top_entity.get("entity_id"):
        detected_id = yolo_top_entity["entity_id"]

    entity_display = None
    if detected_id and detected_id in primary_entities:
        entity_display = primary_entities[detected_id].get("display_name")
    elif weapon_detection.get("display_name"):
        entity_display = weapon_detection.get("display_name")
    elif yolo_top_entity.get("entity_id"):
        entity_display = str(yolo_top_entity.get("entity_id", "")).replace("_", " ").title()

    hook_enforcer = meta.get("hook_enforcer") or {}
    anchor = hook_enforcer.get("anchor_moment") or {}
    trim_plan = hook_enforcer.get("trim_plan") or {}
    return {
        "game_id": game_meta.get("game_id") or meta.get("game"),
        "display_name": game_meta.get("display_name") or meta.get("game", "").replace("_", " ").title(),
        "ui_version": game_meta.get("ui_version", "legacy-2026-04"),
        "primary_entity_kind": primary_kind,
        "player_entity": detected_id,
        "player_entity_name": entity_display,
        "detected_event": detected_event if candidate_moments else None,
        "hook_anchor_timestamp": anchor.get("timestamp") if anchor else (candidate_moments[0]["timestamp"] if candidate_moments else None),
        "expected_final_hook_timestamp": trim_plan.get("expected_hook_timestamp"),
        "context_confidence": _context_confidence(weapon_detection, yolo_detection),
        "match_type": None,
    }


def _collect_detector_outputs(meta: dict, game_pack: dict) -> dict[str, Any]:
    return {
        "audio_detector": meta.get("audio_events", {}),
        "kill_feed": meta.get("kill_feed", {}),
        "weapon_detector": meta.get("weapon_detection", {}),
        "niceshot": meta.get("niceshot_detection", {}),
        "yolo": meta.get("yolo_detection", {}),
        "hook_enforcer": meta.get("hook_enforcer", {}),
    }


def _run_ai_clip_judge(
    meta: dict,
    game_pack: dict,
    candidate_moments: list[dict[str, Any]],
    context: dict[str, Any],
    config: dict,
) -> dict[str, Any]:
    judge_cfg = config.get("clip_judge", {})
    if not judge_cfg.get("enabled", True):
        return _fallback_ai_from_meta(meta, candidate_moments, context)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return _fallback_ai_from_meta(meta, candidate_moments, context)

    model = judge_cfg.get("model", "claude-haiku-4-5-20251001")
    max_tokens = int(judge_cfg.get("max_tokens", 400))

    prompt = _build_ai_prompt(meta, game_pack, candidate_moments, context)
    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_content = message.content[0].text
        parsed = _parse_ai_response(raw_content)
        if parsed:
            return parsed
    except Exception as e:
        logger.warning(f"[clip_judge] AI judge failed: {e}")

    return _fallback_ai_from_meta(meta, candidate_moments, context)


def _build_ai_prompt(meta: dict, game_pack: dict, candidate_moments: list[dict[str, Any]], context: dict[str, Any]) -> str:
    audio_events = meta.get("audio_events", {})
    kill_feed = meta.get("kill_feed", {})
    weapon_detection = meta.get("weapon_detection", {})
    niceshot_detection = meta.get("niceshot_detection", {})
    yolo_detection = meta.get("yolo_detection", {})
    game_meta = game_pack.get("game") or {}

    return f"""You are judging whether a gaming clip should be kept for short-form social posting.
You do NOT have raw frames here. You are making the best possible pre-processing judgment from signal metadata.

Game: {game_meta.get("display_name", context.get("display_name", "Unknown"))}
Genre: {game_meta.get("genre", "fps")}
Duration seconds: {meta.get("duration_seconds", 0)}
Quality tag: {meta.get("quality_tag", "unknown")}

Candidate moments:
{json.dumps(candidate_moments[:8], indent=2)}

Context:
{json.dumps(context, indent=2)}

Signals:
{json.dumps({
    "audio_events": {
        "spike_count": audio_events.get("spike_count", 0),
        "multi_kill_detected": audio_events.get("multi_kill_detected", False),
        "events": audio_events.get("events", []),
    },
    "kill_feed": {
        "passed": kill_feed.get("passed"),
        "sweat_score": kill_feed.get("sweat_score"),
        "kill_count": kill_feed.get("kill_count"),
        "headshot_count": kill_feed.get("headshot_count"),
        "method": kill_feed.get("method"),
    },
    "weapon_detection": weapon_detection,
    "niceshot_detection": niceshot_detection,
    "yolo_detection": yolo_detection,
}, indent=2)}

Rules:
- Heavily reward clips with a clear anchor moment that can open the final edit instantly.
- A good hook means the final trimmed video can show payoff, or a very clear promise of payoff, inside the first 1.5 seconds.
- Penalize missing context and weak evidence.
- Prefer honest uncertainty over guessing.

Return ONLY valid JSON:
{{
  "worthiness_score": <float 0.0-1.0>,
  "hook_score": <float 0.0-1.0>,
  "confidence": <float 0.0-1.0>,
  "hook_pass": <true|false>,
  "narrative_type": "<short label>",
  "reason": "<1-2 sentence explanation>"
}}"""


def _parse_ai_response(content: str) -> dict[str, Any] | None:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(line for line in lines if not line.strip().startswith("```")).strip()

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start < 0 or end <= start:
            return None
        try:
            result = json.loads(text[start:end])
        except json.JSONDecodeError:
            return None

    return {
        "worthiness_score": _clamp_float(result.get("worthiness_score", 0.5)),
        "hook_score": _clamp_float(result.get("hook_score", 0.5)),
        "confidence": _clamp_float(result.get("confidence", 0.5)),
        "hook_pass": bool(result.get("hook_pass", False)),
        "narrative_type": str(result.get("narrative_type", "unknown"))[:80],
        "reason": str(result.get("reason", ""))[:280],
    }


def _fallback_ai_from_meta(meta: dict, candidate_moments: list[dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
    kill_feed = meta.get("kill_feed", {})
    audio_events = meta.get("audio_events", {})
    sweat_score = min(float(kill_feed.get("sweat_score", 0.0)) / 80.0, 1.0)
    hook_score = 0.80 if candidate_moments else 0.20
    confidence = 0.70 if candidate_moments else 0.35

    if not context.get("player_entity"):
        confidence = min(confidence, 0.45)
    if audio_events.get("multi_kill_detected"):
        sweat_score = min(1.0, sweat_score + 0.10)

    return {
        "worthiness_score": round(max(sweat_score, 0.25), 3),
        "hook_score": round(hook_score, 3),
        "confidence": round(confidence, 3),
        "hook_pass": bool(candidate_moments),
        "narrative_type": context.get("detected_event") or "unknown",
        "reason": _FALLBACK_AI["reason"],
    }


def _normalize_kill_feed(meta: dict) -> float:
    kill_feed = meta.get("kill_feed", {})
    score = min(float(kill_feed.get("sweat_score", 0.0)) / 80.0, 1.0)
    if int(kill_feed.get("headshot_count", 0)) > 0:
        score = min(1.0, score + 0.05)
    return round(score, 3)


def _normalize_audio(meta: dict) -> float:
    audio_events = meta.get("audio_events", {})
    spike_count = int(audio_events.get("spike_count", 0))
    score = min(spike_count / 4.0, 1.0)
    if audio_events.get("multi_kill_detected"):
        score = min(1.0, score + 0.15)
    return round(score, 3)


def _normalize_niceshot(meta: dict) -> float | None:
    niceshot = meta.get("niceshot_detection", {})
    # Return None (not zero) when unavailable or in stub mode — keeps the weight slot out
    # of the composite denominator so it doesn't dilute scores until the integration is live.
    if niceshot.get("status") != "ok" or niceshot.get("mode") == "stub":
        return None
    normalized = niceshot.get("normalized_scores") or {}
    if normalized.get("composite") is not None:
        return _clamp_float(normalized.get("composite", 0.0))
    action = _clamp_float(niceshot.get("action_score", 0.0))
    hook = _clamp_float(niceshot.get("hook_score", 0.0))
    confidence = _clamp_float(niceshot.get("confidence", 0.0))
    return round(((action + hook) / 2.0) * confidence, 3)


def _normalize_yolo(meta: dict) -> float | None:
    yolo = meta.get("yolo_detection", {})
    # Return None when model isn't configured or inference failed — excluded from denominator
    # rather than penalized as a zero score.
    if yolo.get("status") != "ok":
        return None
    candidates = yolo.get("event_candidates") or []
    if candidates:
        return round(max(_clamp_float(item.get("confidence", 0.0)) for item in candidates), 3)
    return _clamp_float(yolo.get("context_confidence", 0.0))


def _normalize_context(context: dict, required_fields: list[str]) -> float:
    if not required_fields:
        return 1.0
    resolved = sum(1 for field in required_fields if context.get(field))
    return round(resolved / len(required_fields), 3)


def _context_confidence(weapon_detection: dict, yolo_detection: dict) -> float:
    scores = []
    if weapon_detection.get("weapon_id"):
        scores.append(float(weapon_detection.get("confidence", 0.0)))
    if yolo_detection.get("context_confidence"):
        scores.append(float(yolo_detection.get("context_confidence", 0.0)))
    if not scores:
        return 0.0
    return round(max(scores), 3)


def _weighted_score(values: dict[str, float | None], weights: dict[str, float]) -> float:
    if not weights:
        active = {k: v for k, v in values.items() if v is not None}
        return sum(active.values()) / max(len(active), 1)
    total_weight = 0.0
    weighted = 0.0
    for key, weight in weights.items():
        value = values.get(key)
        if value is None:
            continue  # detector not active — excluded from denominator, not penalized as zero
        weighted += value * float(weight)
        total_weight += float(weight)
    if total_weight <= 0:
        return 0.0
    return weighted / total_weight


def _needs_roi_template(game_pack: dict, context: dict) -> bool:
    hud = game_pack.get("hud") or {}
    detector = (hud.get("detectors") or {}).get("weapon_detector") or {}
    if context.get("player_entity"):
        return False
    icon_dir = detector.get("icon_dir")
    if not icon_dir:
        return False

    pack_root = Path(game_pack.get("pack_root", "."))
    resolved = resolve_asset_path(icon_dir, pack_root)
    if not resolved.exists():
        return True
    return not any(p.suffix.lower() in {".png", ".jpg", ".jpeg"} for p in resolved.iterdir())


def _suspect_ui_drift(meta: dict, context: dict, needs_roi_template: bool) -> bool:
    if needs_roi_template:
        return False
    audio_events = meta.get("audio_events", {})
    kill_feed = meta.get("kill_feed", {})
    weapon_detection = meta.get("weapon_detection", {})
    if int(audio_events.get("spike_count", 0)) == 0:
        return False
    if context.get("player_entity"):
        return False

    weak_kf = kill_feed.get("method") in {"no_events", "disabled"} or not kill_feed.get("kill_count", 0)
    weak_weapon = weapon_detection.get("method") in {"no_match", "disabled"} or not weapon_detection.get("weapon_id")
    return weak_kf and weak_weapon


def _build_explanation(
    ai_result: dict[str, Any],
    composite_score: float,
    hook_gate_passed: bool,
    candidate_moments: list[dict[str, Any]],
    missing_fields: list[str],
    quarantine_reason: str | None,
    context: dict[str, Any],
    hook_gate: dict[str, Any],
) -> list[str]:
    explanation = [
        f"Composite score {round(composite_score, 3)} with AI confidence {round(ai_result.get('confidence', 0.0), 3)}.",
        ai_result.get("reason", _FALLBACK_AI["reason"]),
    ]
    if hook_gate_passed:
        explanation.append(
            f"Hook gate satisfied via {hook_gate.get('mode')} "
            f"(original={hook_gate.get('original_hook_timestamp')}s, "
            f"final={hook_gate.get('expected_hook_timestamp')}s)."
        )
    elif candidate_moments:
        explanation.append(
            f"Top unresolved hook candidate: {candidate_moments[0]['kind']} at {candidate_moments[0]['timestamp']}s."
        )
    if context.get("player_entity_name"):
        explanation.append(f"Resolved player context as {context['player_entity_name']}.")
    if missing_fields:
        explanation.append(f"Missing required context fields: {', '.join(missing_fields)}.")
    if quarantine_reason:
        explanation.append(f"Quarantine reason: {quarantine_reason}.")
    return explanation


def _clamp_float(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def _safe_timestamp(value: Any) -> float:
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return 0.0
