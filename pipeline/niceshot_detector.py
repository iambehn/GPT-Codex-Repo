from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.game_pack import load_game_pack
from utils.logger import get_logger

logger = get_logger(__name__)

<<<<<<< HEAD
_DEFAULT_PROFILES = {
    "cod_like_default": {
        "score_multipliers": {"action": 1.0, "hook": 1.0, "confidence": 1.0},
        "score_weights": {"action": 0.45, "hook": 0.35, "confidence": 0.20},
        "moment_boosts": {
            "flick": 0.05,
            "headshot": 0.06,
            "entry_frag": 0.05,
            "killstreak": 0.04,
        },
        "hook_kinds": ["flick", "headshot", "entry_frag", "killstreak", "precision_pick"],
        "non_hook_kinds": ["audio_spike", "ambient_action"],
        "kind_aliases": {},
    },
    "hero_shooter_default": {
        "score_multipliers": {"action": 1.0, "hook": 1.05, "confidence": 1.0},
        "score_weights": {"action": 0.40, "hook": 0.40, "confidence": 0.20},
        "moment_boosts": {
            "ultimate_swing": 0.08,
            "team_wipe_candidate": 0.06,
            "multi_kill_swing": 0.05,
            "entry_frag": 0.05,
        },
        "hook_kinds": ["ultimate_swing", "team_wipe_candidate", "multi_kill_swing", "entry_frag", "precision_pick"],
        "non_hook_kinds": ["ambient_action"],
        "kind_aliases": {
            "ult_swing": "ultimate_swing",
            "teamwipe": "team_wipe_candidate",
        },
    },
}

=======
>>>>>>> origin/main

def run_niceshot_detector(
    clip_path: str | Path,
    game: str,
    config: dict,
    game_pack: dict | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Adapter contract for NiceShot-style candidate moment extraction.

    V1 intentionally supports only stub and fixture JSON modes because the
    project does not yet have confirmed NiceShot API/CLI details.
    """
    clip = Path(clip_path)
    meta_path = clip.with_suffix(".meta.json")

    if meta_path.exists() and not force:
        existing = _load_meta(meta_path)
        if "niceshot_detection" in existing:
            logger.debug(f"[niceshot_detector] Already processed: {clip.name}")
            return existing["niceshot_detection"]

    game_pack = game_pack or load_game_pack(game, config)
    cfg = _merged_config(game, config, game_pack)
    if not cfg.get("enabled", False):
        return _write_and_return(meta_path, _base_result(cfg, "disabled"))

    mode = str(cfg.get("mode") or "stub").strip().lower()
    try:
        if mode == "stub":
            result = _stub_result(cfg)
        elif mode == "fixture_json":
            result = _fixture_result(clip, cfg)
        elif mode in {"cli", "api"}:
            result = _base_result(cfg, "not_configured", f"{mode} mode is reserved for a future NiceShot integration")
        else:
            result = _base_result(cfg, "not_configured", f"unsupported NiceShot mode '{mode}'")
    except Exception as e:
        logger.warning(f"[niceshot_detector] Failed for {clip.name}: {e}")
        result = _base_result(cfg, "error", str(e))

    return _write_and_return(meta_path, result)


def _merged_config(game: str, config: dict, game_pack: dict) -> dict:
    global_cfg = dict(config.get("niceshot_detector") or {})
    game_cfg = dict((((game_pack.get("game") or {}).get("detectors") or {}).get("niceshot")) or {})
<<<<<<< HEAD
    profile_name = str(game_cfg.get("profile") or global_cfg.get("profile", "cod_like_default"))
    profiles = dict(_DEFAULT_PROFILES)
    profiles.update(global_cfg.get("profiles") or {})
    profile_cfg = dict(profiles.get(profile_name) or {})
    profile_overrides = dict(game_cfg.get("profile_overrides") or {})

    merged = {
        "enabled": bool(global_cfg.get("enabled", False)),
        "provider": global_cfg.get("provider", "niceshot_ai"),
        "profile": profile_name,
=======
    merged = {
        "enabled": bool(global_cfg.get("enabled", False)),
        "provider": global_cfg.get("provider", "niceshot_ai"),
        "profile": global_cfg.get("profile", "cod_like_default"),
>>>>>>> origin/main
        "mode": global_cfg.get("mode", "stub"),
        "fixture_dir": global_cfg.get("fixture_dir"),
        "fixture_path": global_cfg.get("fixture_path"),
        "stub": dict(global_cfg.get("stub") or {}),
<<<<<<< HEAD
        "score_multipliers": dict(profile_cfg.get("score_multipliers") or {}),
        "score_weights": dict(profile_cfg.get("score_weights") or {}),
        "moment_boosts": dict(profile_cfg.get("moment_boosts") or {}),
        "hook_kinds": list(profile_cfg.get("hook_kinds") or []),
        "non_hook_kinds": list(profile_cfg.get("non_hook_kinds") or []),
        "kind_aliases": dict(profile_cfg.get("kind_aliases") or {}),
        "profile_overrides_applied": sorted(profile_overrides.keys()),
    }
    merged.update(game_cfg)
    merged["score_multipliers"] = _merge_mapping(profile_cfg.get("score_multipliers"), profile_overrides.get("score_multipliers"))
    merged["score_weights"] = _merge_mapping(profile_cfg.get("score_weights"), profile_overrides.get("score_weights"))
    merged["moment_boosts"] = _merge_mapping(profile_cfg.get("moment_boosts"), profile_overrides.get("moment_boosts"))
    merged["kind_aliases"] = _merge_mapping(profile_cfg.get("kind_aliases"), profile_overrides.get("kind_aliases"))
    merged["hook_kinds"] = _merge_list(profile_cfg.get("hook_kinds"), profile_overrides.get("hook_kinds"))
    merged["non_hook_kinds"] = _merge_list(profile_cfg.get("non_hook_kinds"), profile_overrides.get("non_hook_kinds"))
=======
    }
    merged.update(game_cfg)
>>>>>>> origin/main
    merged["enabled"] = bool(global_cfg.get("enabled", False)) and bool(game_cfg.get("enabled", False))
    merged["game"] = game
    return merged


def _stub_result(cfg: dict) -> dict:
    stub = cfg.get("stub") or {}
    result = _base_result(cfg, "ok")
<<<<<<< HEAD
    raw_moments = stub.get("moments", [])
    normalized_moments = _normalize_moments(raw_moments, source="niceshot", cfg=cfg)
    raw_action = _clamp(stub.get("action_score", 0.0))
    raw_hook = _clamp(stub.get("hook_score", 0.0))
    raw_confidence = _clamp(stub.get("confidence", 0.0))
    normalized_scores = _normalize_scores(raw_action, raw_hook, raw_confidence, normalized_moments, cfg)
    result.update({
        "mode": "stub",
        "raw_action_score": raw_action,
        "raw_hook_score": raw_hook,
        "raw_confidence": raw_confidence,
        "action_score": normalized_scores["action"],
        "hook_score": normalized_scores["hook"],
        "confidence": normalized_scores["confidence"],
        "normalized_scores": normalized_scores,
        "moments": normalized_moments,
        "moment_summary": _moment_summary(normalized_moments),
        "explanation": _build_explanation(cfg, normalized_scores, normalized_moments),
=======
    result.update({
        "mode": "stub",
        "action_score": _clamp(stub.get("action_score", 0.0)),
        "hook_score": _clamp(stub.get("hook_score", 0.0)),
        "confidence": _clamp(stub.get("confidence", 0.0)),
        "moments": _normalize_moments(stub.get("moments", []), source="niceshot"),
>>>>>>> origin/main
    })
    return result


def _fixture_result(clip: Path, cfg: dict) -> dict:
    fixture_path = _resolve_fixture_path(clip, cfg)
    if fixture_path is None or not fixture_path.exists():
        return _base_result(cfg, "not_configured", "fixture JSON not found")

    try:
        payload = json.loads(fixture_path.read_text())
    except json.JSONDecodeError as e:
        return _base_result(cfg, "error", f"fixture JSON is malformed: {e}")

<<<<<<< HEAD
    normalized_moments = _normalize_moments(payload.get("moments", []), source="niceshot", cfg=cfg)
    raw_action = _clamp(payload.get("action_score", 0.0))
    raw_hook = _clamp(payload.get("hook_score", 0.0))
    raw_confidence = _clamp(payload.get("confidence", 0.0))
    normalized_scores = _normalize_scores(raw_action, raw_hook, raw_confidence, normalized_moments, cfg)
=======
>>>>>>> origin/main
    result = _base_result(cfg, str(payload.get("status", "ok")))
    result.update({
        "mode": "fixture_json",
        "fixture_path": str(fixture_path),
<<<<<<< HEAD
        "raw_action_score": raw_action,
        "raw_hook_score": raw_hook,
        "raw_confidence": raw_confidence,
        "action_score": normalized_scores["action"],
        "hook_score": normalized_scores["hook"],
        "confidence": normalized_scores["confidence"],
        "normalized_scores": normalized_scores,
        "moments": normalized_moments,
        "moment_summary": _moment_summary(normalized_moments),
        "explanation": _build_explanation(cfg, normalized_scores, normalized_moments),
=======
        "action_score": _clamp(payload.get("action_score", 0.0)),
        "hook_score": _clamp(payload.get("hook_score", 0.0)),
        "confidence": _clamp(payload.get("confidence", 0.0)),
        "moments": _normalize_moments(payload.get("moments", []), source="niceshot"),
>>>>>>> origin/main
    })
    return result


def _resolve_fixture_path(clip: Path, cfg: dict) -> Path | None:
    if cfg.get("fixture_path"):
        return Path(str(cfg["fixture_path"])).expanduser().resolve()
    if cfg.get("fixture_dir"):
        return (Path(str(cfg["fixture_dir"])).expanduser().resolve() / f"{clip.stem}.json")
    return None


<<<<<<< HEAD
def _normalize_moments(raw_moments: Any, source: str, cfg: dict) -> list[dict[str, Any]]:
=======
def _normalize_moments(raw_moments: Any, source: str) -> list[dict[str, Any]]:
>>>>>>> origin/main
    moments: list[dict[str, Any]] = []
    if not isinstance(raw_moments, list):
        return moments

<<<<<<< HEAD
    hook_kinds = set(cfg.get("hook_kinds") or [])
    non_hook_kinds = set(cfg.get("non_hook_kinds") or [])
    kind_aliases = cfg.get("kind_aliases") or {}
    moment_boosts = cfg.get("moment_boosts") or {}

=======
>>>>>>> origin/main
    for item in raw_moments:
        if not isinstance(item, dict):
            continue
        try:
            timestamp = round(float(item.get("timestamp", item.get("time", 0.0))), 3)
        except (TypeError, ValueError):
            timestamp = 0.0
<<<<<<< HEAD
        raw_kind = str(item.get("kind", item.get("label", "action_spike")))
        kind = str(kind_aliases.get(raw_kind, raw_kind))
        confidence = _clamp(item.get("confidence", item.get("score", 0.0)))
        confidence = _clamp(confidence + float(moment_boosts.get(kind, 0.0)))
        hook_candidate = bool(item.get("hook_candidate", True))
        if kind in hook_kinds:
            hook_candidate = True
        elif kind in non_hook_kinds:
            hook_candidate = False
        moments.append({
            "timestamp": timestamp,
            "source": str(item.get("source", source)),
            "kind": kind,
            "confidence": confidence,
            "hook_candidate": hook_candidate,
=======
        moments.append({
            "timestamp": timestamp,
            "source": str(item.get("source", source)),
            "kind": str(item.get("kind", item.get("label", "action_spike"))),
            "confidence": _clamp(item.get("confidence", item.get("score", 0.0))),
            "hook_candidate": bool(item.get("hook_candidate", True)),
>>>>>>> origin/main
        })
    return moments


def _base_result(cfg: dict, status: str, reason: str | None = None) -> dict:
    result = {
        "enabled": bool(cfg.get("enabled", False)),
        "status": status,
        "provider": cfg.get("provider", "niceshot_ai"),
        "profile": cfg.get("profile", "cod_like_default"),
        "mode": cfg.get("mode", "stub"),
<<<<<<< HEAD
        "profile_overrides_applied": cfg.get("profile_overrides_applied", []),
        "raw_action_score": 0.0,
        "raw_hook_score": 0.0,
        "raw_confidence": 0.0,
        "action_score": 0.0,
        "hook_score": 0.0,
        "confidence": 0.0,
        "normalized_scores": {"action": 0.0, "hook": 0.0, "confidence": 0.0, "composite": 0.0},
        "moments": [],
        "moment_summary": {"total": 0, "hook_candidates": 0, "top_kind": None, "earliest_hook_timestamp": None},
        "explanation": [],
=======
        "action_score": 0.0,
        "hook_score": 0.0,
        "confidence": 0.0,
        "moments": [],
>>>>>>> origin/main
    }
    if reason:
        result["reason"] = reason
    return result


def _write_and_return(meta_path: Path, result: dict) -> dict:
    meta = _load_meta(meta_path)
    meta["niceshot_detection"] = result
    meta_path.write_text(json.dumps(meta, indent=2))
    return result


def _load_meta(meta_path: Path) -> dict:
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _clamp(value: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return 0.0
<<<<<<< HEAD


def _normalize_scores(
    raw_action: float,
    raw_hook: float,
    raw_confidence: float,
    moments: list[dict[str, Any]],
    cfg: dict,
) -> dict[str, float]:
    multipliers = cfg.get("score_multipliers") or {}
    score_weights = cfg.get("score_weights") or {}

    action = _clamp(raw_action * float(multipliers.get("action", 1.0)))
    hook = _clamp(raw_hook * float(multipliers.get("hook", 1.0)))
    confidence = _clamp(raw_confidence * float(multipliers.get("confidence", 1.0)))
    if moments and confidence == 0.0:
        confidence = _clamp(max(moment.get("confidence", 0.0) for moment in moments) * 0.85)

    total_weight = sum(float(score_weights.get(key, 0.0)) for key in ("action", "hook", "confidence"))
    if total_weight <= 0:
        composite = round((action + hook + confidence) / 3.0, 3)
    else:
        composite = round(
            (
                action * float(score_weights.get("action", 0.0))
                + hook * float(score_weights.get("hook", 0.0))
                + confidence * float(score_weights.get("confidence", 0.0))
            ) / total_weight,
            3,
        )

    return {
        "action": action,
        "hook": hook,
        "confidence": confidence,
        "composite": composite,
    }


def _moment_summary(moments: list[dict[str, Any]]) -> dict[str, Any]:
    if not moments:
        return {"total": 0, "hook_candidates": 0, "top_kind": None, "earliest_hook_timestamp": None}

    hook_candidates = [moment for moment in moments if moment.get("hook_candidate")]
    top = max(moments, key=lambda item: item.get("confidence", 0.0))
    earliest_hook = min(
        (moment.get("timestamp") for moment in hook_candidates),
        default=None,
    )
    return {
        "total": len(moments),
        "hook_candidates": len(hook_candidates),
        "top_kind": top.get("kind"),
        "earliest_hook_timestamp": earliest_hook,
    }


def _build_explanation(cfg: dict, normalized_scores: dict[str, float], moments: list[dict[str, Any]]) -> list[str]:
    explanation = [
        (
            f"Profile '{cfg.get('profile', 'cod_like_default')}' produced normalized scores "
            f"action={normalized_scores['action']}, hook={normalized_scores['hook']}, "
            f"confidence={normalized_scores['confidence']}."
        )
    ]
    if cfg.get("profile_overrides_applied"):
        explanation.append(
            "Applied game-specific NiceShot overrides: "
            + ", ".join(cfg["profile_overrides_applied"])
            + "."
        )
    if moments:
        explanation.append(
            f"Normalized {len(moments)} NiceShot moment(s); strongest signal is {max(moments, key=lambda item: item.get('confidence', 0.0)).get('kind')}."
        )
    else:
        explanation.append("No NiceShot moments were available for normalization.")
    return explanation


def _merge_mapping(base: Any, override: Any) -> dict[str, Any]:
    merged = dict(base or {})
    merged.update(override or {})
    return merged


def _merge_list(base: Any, override: Any) -> list[Any]:
    items: list[Any] = []
    for value in list(base or []) + list(override or []):
        if value not in items:
            items.append(value)
    return items
=======
>>>>>>> origin/main
