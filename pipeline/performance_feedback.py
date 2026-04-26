"""
Social performance feedback loop.

Reads post metrics from Google Sheets, links them back to each clip's
composite_inputs from the clip_judge detector pipeline, and recommends
weight adjustments based on which signals predicted high engagement.

Correlation method: top-vs-bottom quartile comparison. More robust than
Pearson correlation at small sample sizes and easier to interpret — you can
read "kill_feed_score is 0.18 higher in top performers" directly.

Minimum thresholds prevent premature tuning on noisy early data:
  - min_clips (default 10): correlated clips with real metrics before any
    recommendation is generated
  - min_age_hours (default 48): clips must be old enough to have accumulated
    meaningful engagement signal

Entry points:
  apply_performance_updates(game, config)         — full loop, dry_run=True by default
  fetch_performance_signals(game, config)         — data collection only
  compute_weight_recommendations(signals, ...)    — correlation → recommendation
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from pipeline.game_pack import get_game_pack_dir
from utils.analytics import compute_decision_rows, read_analytics_tables
from utils.logger import get_logger

logger = get_logger(__name__)

_COMPOSITE_INPUT_KEYS = [
    "ai_clip_score",
    "ai_hook_score",
    "kill_feed_score",
    "audio_score",
    "context_score",
    "niceshot_score",
    "yolo_event_score",
]

_MIN_CLIPS = 10
_MIN_AGE_HOURS = 48.0
_MIN_DELTA = 0.05   # signal delta must exceed this to generate a recommendation
_MAX_SHIFT = 0.05   # hard cap on weight shift per run (overridden by weights.yaml weight_step)


def fetch_performance_signals(
    game: str,
    config: dict,
    min_age_hours: float = _MIN_AGE_HOURS,
) -> dict[str, Any]:
    """Pull analytics tables, score posts, and link each post back to its clip's
    composite_inputs from the detector pipeline.

    Returns:
        signals:      list of correlated records (post score + composite_inputs)
        uncorrelated: posts with metrics but no locatable meta.json
        total_posts:  total game posts in the Posts sheet
        configured:   whether analytics is set up
    """
    tables = read_analytics_tables(config)
    if not tables.get("configured"):
        return {
            "ok": False,
            "error": tables.get("error", "analytics not configured"),
            "signals": [],
            "uncorrelated": [],
            "total_posts": 0,
            "configured": False,
        }

    all_posts = tables.get("posts") or []
    all_metrics = tables.get("metrics") or []

    game_posts = [p for p in all_posts if str(p.get("game", "")).strip().lower() == game.lower()]
    post_by_id = {str(p.get("post_id", "")): p for p in game_posts}

    decision_rows = compute_decision_rows(game_posts, all_metrics, config)
    decision_by_id = {str(row.get("post_id", "")): row for row in decision_rows}

    latest_views = _latest_metric_field(all_metrics, "views")
    latest_retention = _latest_metric_field(all_metrics, "retention")

    signals: list[dict[str, Any]] = []
    uncorrelated: list[dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for post_id, post in post_by_id.items():
        decision = decision_by_id.get(post_id)
        if not decision:
            continue

        age_hours = _safe_float(decision.get("post_age_hours"))
        if age_hours is None or age_hours < min_age_hours:
            continue

        views = _safe_int(latest_views.get(post_id))
        if not views:
            continue

        score = _safe_float(decision.get("score")) or 0.0
        clip_id = str(post.get("clip_id", "")).strip()

        record: dict[str, Any] = {
            "clip_id": clip_id,
            "post_id": post_id,
            "platform": str(post.get("platform", "")),
            "performance_score": round(score, 4),
            "age_hours": round(age_hours, 1),
            "views": views,
            "retention": _safe_float(latest_retention.get(post_id)) or 0.0,
            "decision_reason": decision.get("decision_reason", ""),
        }

        composite_inputs = _find_composite_inputs(clip_id, game, config)
        if composite_inputs:
            record["composite_inputs"] = composite_inputs
            signals.append(record)
        else:
            uncorrelated.append(record)

    logger.info(
        f"[perf_feedback] {game}: {len(signals)} correlated, "
        f"{len(uncorrelated)} uncorrelated, {len(game_posts)} total posts"
    )

    return {
        "ok": True,
        "error": None,
        "game": game,
        "signals": signals,
        "uncorrelated": uncorrelated,
        "total_posts": len(game_posts),
        "configured": True,
    }


def compute_weight_recommendations(
    signals: list[dict[str, Any]],
    game: str,
    config: dict,
    min_clips: int = _MIN_CLIPS,
) -> dict[str, Any]:
    """Compare composite_inputs between top and bottom performance quartiles.
    Recommend weight shifts for signals that clearly differentiate the two groups.
    """
    eligible = [s for s in signals if s.get("composite_inputs")]

    if len(eligible) < min_clips:
        return {
            "ok": False,
            "reason": (
                f"Only {len(eligible)} correlated clips (need {min_clips} with ≥48h of data). "
                "Keep posting — recommendations unlock automatically."
            ),
            "clips_used": len(eligible),
            "min_clips": min_clips,
            "recommendations": [],
            "would_change": False,
        }

    sorted_clips = sorted(eligible, key=lambda s: s["performance_score"])
    n = len(sorted_clips)
    q = max(1, n // 4)
    bottom_group = sorted_clips[:q]
    top_group = sorted_clips[n - q:]

    top_avgs = _avg_inputs(top_group)
    bottom_avgs = _avg_inputs(bottom_group)
    weight_step = _load_weight_step(game, config)

    recommendations: list[dict[str, Any]] = []
    for key in _COMPOSITE_INPUT_KEYS:
        t = top_avgs.get(key)
        b = bottom_avgs.get(key)
        if t is None or b is None:
            continue
        delta = t - b
        if abs(delta) < _MIN_DELTA:
            continue
        direction = "increase" if delta > 0 else "decrease"
        shift = round(min(abs(delta) * 0.4, weight_step), 3)
        recommendations.append({
            "signal": key,
            "direction": direction,
            "delta": round(delta, 4),
            "suggested_shift": shift if direction == "increase" else -shift,
            "top_quartile_avg": round(t, 4),
            "bottom_quartile_avg": round(b, 4),
            "reason": (
                f"{key} is {delta:+.3f} {'higher' if delta > 0 else 'lower'} in top performers — "
                f"{'increase' if delta > 0 else 'reduce'} its weight by {shift}."
            ),
        })

    recommendations.sort(key=lambda r: abs(r["delta"]), reverse=True)

    return {
        "ok": True,
        "clips_used": n,
        "top_quartile_size": q,
        "bottom_quartile_size": q,
        "top_avg_performance": round(sum(s["performance_score"] for s in top_group) / q, 4),
        "bottom_avg_performance": round(sum(s["performance_score"] for s in bottom_group) / q, 4),
        "weight_step": weight_step,
        "recommendations": recommendations,
        "would_change": bool(recommendations),
    }


def apply_performance_updates(
    game: str,
    config: dict,
    *,
    dry_run: bool = False,
    min_clips: int = _MIN_CLIPS,
    min_age_hours: float = _MIN_AGE_HOURS,
) -> dict[str, Any]:
    """Full feedback loop: fetch signals → correlate → recommend → optionally write weights.yaml."""
    fetch_result = fetch_performance_signals(game, config, min_age_hours=min_age_hours)

    if not fetch_result.get("ok"):
        return {
            "ok": False,
            "game": game,
            "dry_run": dry_run,
            "error": fetch_result.get("error"),
            "signals_found": 0,
            "recommendations": None,
            "applied": False,
        }

    signals = fetch_result["signals"]
    rec = compute_weight_recommendations(signals, game, config, min_clips=min_clips)

    result: dict[str, Any] = {
        "ok": True,
        "game": game,
        "dry_run": dry_run,
        "signals_found": len(signals),
        "uncorrelated": len(fetch_result["uncorrelated"]),
        "total_posts": fetch_result["total_posts"],
        "recommendations": rec,
        "applied": False,
    }

    if not rec.get("ok"):
        logger.info(f"[perf_feedback] {game}: {rec.get('reason')}")
        return result

    _log_summary(game, fetch_result, rec)

    if dry_run or not rec.get("would_change"):
        return result

    weights_path = get_game_pack_dir(game, config) / "weights.yaml"
    weights_payload = _load_yaml(weights_path)
    clip_judge = weights_payload.setdefault("clip_judge", {})
    composite_weights = dict(clip_judge.get("composite_weights") or {})

    for r in rec["recommendations"]:
        key = r["signal"]
        shift = float(r["suggested_shift"])
        current = float(composite_weights.get(key, 0.0))
        composite_weights[key] = max(0.0, round(current + shift, 3))

    composite_weights = _normalize_weights(composite_weights)
    clip_judge["composite_weights"] = composite_weights

    feedback_cfg = clip_judge.setdefault("feedback", {})
    feedback_cfg["last_performance_update_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    feedback_cfg["last_performance_clips_used"] = rec["clips_used"]

    weights_path.write_text(yaml.safe_dump(weights_payload, sort_keys=False))
    result["applied"] = True
    result["updated_weights"] = composite_weights
    logger.info(f"[perf_feedback] {game}: applied weight updates → {weights_path}")
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_composite_inputs(clip_id: str, game: str, config: dict) -> dict[str, float] | None:
    if not clip_id:
        return None
    paths = config.get("paths") or {}
    search_roots = [
        Path(paths.get("inbox", "inbox")) / game,
        Path(paths.get("accepted", "accepted")) / game,
        Path(paths.get("quarantine", "quarantine")) / game,
    ]
    for root in search_roots:
        if not root.exists():
            continue
        candidate = root / f"{clip_id}.meta.json"
        if candidate.exists():
            result = _extract_composite_inputs(_read_json(candidate))
            if result is not None:
                return result
        for meta_path in root.glob("*.meta.json"):
            try:
                meta = _read_json(meta_path)
            except Exception:
                continue
            if str(meta.get("clip_id", "")) == clip_id:
                result = _extract_composite_inputs(meta)
                if result is not None:
                    return result
    return None


def _extract_composite_inputs(meta: dict) -> dict[str, float] | None:
    raw = (meta.get("detector_outputs") or {}).get("composite_inputs")
    if not isinstance(raw, dict):
        return None
    result = {}
    for key in _COMPOSITE_INPUT_KEYS:
        val = raw.get(key)
        if val is not None:
            try:
                result[key] = float(val)
            except (TypeError, ValueError):
                pass
    return result if result else None


def _avg_inputs(clips: list[dict[str, Any]]) -> dict[str, float]:
    buckets: dict[str, list[float]] = {}
    for clip in clips:
        for key, val in (clip.get("composite_inputs") or {}).items():
            buckets.setdefault(key, []).append(float(val))
    return {key: sum(vals) / len(vals) for key, vals in buckets.items()}


def _latest_metric_field(metrics: list[dict], field: str) -> dict[str, Any]:
    """Return the most recent value of `field` per post_id."""
    latest: dict[str, tuple[str, Any]] = {}
    for row in metrics:
        pid = str(row.get("post_id", ""))
        ts = str(row.get("snapshot_at", ""))
        val = row.get(field)
        if val is None:
            continue
        existing_ts, _ = latest.get(pid, ("", None))
        if ts >= existing_ts:
            latest[pid] = (ts, val)
    return {pid: v for pid, (_, v) in latest.items()}


def _load_weight_step(game: str, config: dict) -> float:
    try:
        weights_path = get_game_pack_dir(game, config) / "weights.yaml"
        weights = _load_yaml(weights_path)
        step = ((weights.get("clip_judge") or {}).get("feedback") or {}).get("weight_step", _MAX_SHIFT)
        return float(step)
    except Exception:
        return _MAX_SHIFT


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    total = sum(max(v, 0.0) for v in weights.values())
    if total <= 0:
        return weights
    normalized = {k: round(max(v, 0.0) / total, 3) for k, v in weights.items()}
    drift = round(1.0 - sum(normalized.values()), 3)
    if drift:
        top_key = max(normalized, key=normalized.__getitem__)
        normalized[top_key] = round(normalized[top_key] + drift, 3)
    return normalized


def _log_summary(game: str, fetch_result: dict, rec: dict) -> None:
    logger.info(
        f"[perf_feedback] {game}: {rec['clips_used']} clips — "
        f"top avg {rec['top_avg_performance']:.3f} vs bottom avg {rec['bottom_avg_performance']:.3f}"
    )
    for r in rec["recommendations"]:
        logger.info(f"[perf_feedback]   {r['reason']}")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0
