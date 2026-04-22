"""
Deterministic modular title engine.

The engine reads detector and judge metadata from a clip sidecar, builds a
small fact bundle, and assembles publishing metadata without calling an LLM.
Existing assets/titles.yaml files remain valid: old templates that use
{weapon} still work, while newer structured templates can use {subject},
{action}, {stakes}, {event}, {game}, {kill_count}, and {headshot_count}.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from pipeline.game_pack import get_game_metadata, get_primary_entities, load_game_pack
from utils.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_TITLES_PATH = "assets/titles.yaml"
_DEFAULT_HISTORY_PATH = "assets/title_history.json"
_DEFAULT_HISTORY_WINDOW = 20
_BARE_TITLE = "Gaming clip"
_CATEGORIES = ("performance_hype", "broken_balance", "reactionary_shock", "meta_authority")


def generate_title(clip_path: Path, game: str, config: dict) -> dict:
    """Generate canonical deterministic publishing metadata for one clip."""
    te_cfg = config.get("title_engine", {})
    meta_path = clip_path.with_suffix(".meta.json")

    if meta_path.exists():
        try:
            existing = json.loads(meta_path.read_text())
            if "title_engine" in existing:
                logger.debug(f"[title_engine] Already generated for {clip_path.name}")
                return existing["title_engine"]
        except (json.JSONDecodeError, OSError):
            pass

    if not te_cfg.get("enabled", False):
        return _write_result(meta_path, _empty("title_engine disabled"))

    clip_meta = _read_meta(meta_path)
    titles_data = _load_titles_data(te_cfg)
    if titles_data is None:
        return _write_result(meta_path, _empty(f"titles file not found: {te_cfg.get('titles_path', _DEFAULT_TITLES_PATH)}"))

    game_pack = load_game_pack(game, config)
    facts = _build_fact_bundle(clip_meta, game, config, game_pack)
    phrases = _build_phrase_parts(facts)
    variables = _build_variables(facts, phrases)
    category = _select_category(facts)
    history = _load_history(te_cfg, game)

    selected = _select_title_from_templates(titles_data, game, category, variables, history, te_cfg)
    explanation = list(facts["explanation"])
    fallback_level = "bare"
    template_used: str | None = None

    if selected:
        title = selected["title"]
        template_used = selected["template"]
        fallback_level = selected["fallback_level"]
        explanation.append(f"Selected {fallback_level} title template.")
    else:
        title, fallback_level = _generated_title(facts, variables)
        explanation.append(f"Used {fallback_level} title fallback.")

    if not title:
        title = _BARE_TITLE
        fallback_level = "bare"

    hashtags = _generate_hashtags(facts)
    caption = _generate_caption(title, hashtags)
    confidence = _calculate_title_confidence(facts, fallback_level)

    result = {
        "title": title,
        "caption": caption,
        "category": category,
        "template_used": template_used,
        "variables": {k: v for k, v in variables.items() if v not in ("", None)},
        "hashtags": hashtags,
        "confidence": confidence,
        "fallback_level": fallback_level,
        "explanation": explanation,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    _update_history(te_cfg, game, template_used or title, title)
    _write_result(meta_path, result)
    logger.info(f"[title_engine] '{title}' (category={category}, fallback={fallback_level})")
    return result


def _read_meta(meta_path: Path) -> dict:
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _load_titles_data(te_cfg: dict) -> dict | None:
    titles_path = Path(te_cfg.get("titles_path", _DEFAULT_TITLES_PATH))
    if not titles_path.exists():
        logger.warning(f"[title_engine] titles.yaml not found at {titles_path}")
        return None
    try:
        return yaml.safe_load(titles_path.read_text()) or {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning(f"[title_engine] Could not read titles file: {exc}")
        return {}


def _build_fact_bundle(clip_meta: dict, game: str, config: dict, game_pack: dict) -> dict[str, Any]:
    game_display = get_game_metadata(game, config, game_pack).get("display_name", game)
    entity_id, entity_name, entity_source, entity_confidence = _resolve_entity(clip_meta, game_pack)
    event_label, event_source = _resolve_event_label(clip_meta)
    moment_label, moment_source = _resolve_moment_label(clip_meta)

    kill_feed = clip_meta.get("kill_feed") or {}
    decision = clip_meta.get("decision") or {}
    context = clip_meta.get("context") or {}
    niceshot = clip_meta.get("niceshot_detection") or {}
    yolo = clip_meta.get("yolo_detection") or {}

    hook_timestamp = _first_number(
        decision.get("top_hook_timestamp"),
        decision.get("hook_timestamp"),
        context.get("hook_timestamp"),
        context.get("hook_anchor_timestamp"),
    )
    hook_passed = bool(
        decision.get("hook_gate_passed")
        or decision.get("hook_passed")
        or (decision.get("status") == "accept" and hook_timestamp is not None and hook_timestamp <= 1.5)
    )

    candidate_moments = _candidate_moments(clip_meta)
    yolo_labels = [
        str(det.get("label"))
        for det in (yolo.get("detections") or [])
        if isinstance(det, dict) and det.get("label")
    ]

    facts = {
        "entity_name": entity_name,
        "entity_id": entity_id,
        "entity_source": entity_source,
        "game": game,
        "game_name": game_display,
        "event_label": event_label,
        "event_source": event_source,
        "moment_label": moment_label,
        "moment_source": moment_source,
        "kill_count": _safe_int(kill_feed.get("kill_count")),
        "headshot_count": _safe_int(kill_feed.get("headshot_count")),
        "sweat_score": _safe_float(kill_feed.get("sweat_score")),
        "hook_timestamp": hook_timestamp,
        "hook_passed": hook_passed,
        "composite_score": _first_number(decision.get("composite_score"), clip_meta.get("composite_score"), 0.0),
        "niceshot_action_score": _safe_float(niceshot.get("action_score")),
        "niceshot_hook_score": _safe_float(niceshot.get("hook_score")),
        "niceshot_confidence": _safe_float(niceshot.get("confidence")),
        "yolo_labels": yolo_labels,
        "candidate_moments": candidate_moments,
        "context_confidence": _safe_float(
            context.get("confidence")
            or context.get("context_confidence")
            or yolo.get("context_confidence")
        ),
        "entity_confidence": entity_confidence,
    }
    facts["confidence"] = _aggregate_confidence(facts)
    facts["explanation"] = _fact_explanation(facts)
    return facts


def _resolve_entity(clip_meta: dict, game_pack: dict) -> tuple[str | None, str | None, str | None, float]:
    context = clip_meta.get("context") or {}
    if context.get("player_entity"):
        entity_id = str(context.get("player_entity"))
        return entity_id, _entity_display_name(entity_id, game_pack, context.get("player_entity_name")), "context", _safe_float(context.get("confidence") or context.get("context_confidence"), 0.7)

    weapon = clip_meta.get("weapon_detection") or {}
    weapon_id = weapon.get("weapon_id") or weapon.get("entity_id")
    weapon_method = str(weapon.get("method", "")).lower()
    if weapon_id and weapon_method not in {"no_match", "unresolved", "skipped"}:
        entity_id = str(weapon_id)
        name = weapon.get("display_name") or weapon.get("weapon_name")
        return entity_id, _entity_display_name(entity_id, game_pack, name), "opencv", _safe_float(weapon.get("confidence"), 0.75)

    yolo = clip_meta.get("yolo_detection") or {}
    top_entity = yolo.get("top_entity") or {}
    if isinstance(top_entity, dict) and top_entity.get("entity_id"):
        entity_id = str(top_entity.get("entity_id"))
        name = top_entity.get("display_name") or top_entity.get("name")
        return entity_id, _entity_display_name(entity_id, game_pack, name), "yolo", _safe_float(top_entity.get("confidence"), 0.65)

    return None, None, None, 0.0


def _entity_display_name(entity_id: str, game_pack: dict, preferred: Any = None) -> str:
    if preferred:
        return str(preferred)
    _kind, entities = get_primary_entities(game_pack)
    entity = entities.get(entity_id) if isinstance(entities, dict) else None
    if isinstance(entity, dict) and entity.get("display_name"):
        return str(entity["display_name"])
    return entity_id.replace("_", " ").title()


def _resolve_event_label(clip_meta: dict) -> tuple[str | None, str | None]:
    context = clip_meta.get("context") or {}
    if context.get("detected_event"):
        return _humanize_label(context.get("detected_event")), "context"

    yolo = clip_meta.get("yolo_detection") or {}
    candidates = yolo.get("event_candidates") or []
    if candidates:
        top = max((c for c in candidates if isinstance(c, dict)), key=lambda c: _safe_float(c.get("confidence")), default=None)
        if top:
            return _humanize_label(top.get("event_id") or top.get("label")), "yolo"

    moments = _candidate_moments(clip_meta)
    if moments:
        top = max(moments, key=lambda item: _safe_float(item.get("confidence")))
        return _humanize_label(top.get("kind") or top.get("event") or top.get("label")), "candidate_moment"

    kill_feed = clip_meta.get("kill_feed") or {}
    kill_count = _safe_int(kill_feed.get("kill_count"))
    headshot_count = _safe_int(kill_feed.get("headshot_count"))
    if kill_count >= 5:
        return "team wipe", "kill_feed"
    if kill_count >= 3:
        return "multi kill swing", "kill_feed"
    if headshot_count >= 2:
        return "headshot chain", "kill_feed"
    if headshot_count == 1:
        return "precision pick", "kill_feed"
    return None, None


def _resolve_moment_label(clip_meta: dict) -> tuple[str | None, str | None]:
    moments = _candidate_moments(clip_meta)
    if not moments:
        return None, None
    top = max(moments, key=lambda item: _safe_float(item.get("confidence")))
    return _humanize_label(top.get("label") or top.get("kind") or top.get("event")), str(top.get("source") or "candidate_moment")


def _candidate_moments(clip_meta: dict) -> list[dict]:
    moments: list[dict] = []
    for item in clip_meta.get("candidate_moments") or []:
        if isinstance(item, dict):
            moments.append(item)
    niceshot = clip_meta.get("niceshot_detection") or {}
    for item in niceshot.get("moments") or []:
        if isinstance(item, dict):
            merged = dict(item)
            merged.setdefault("source", "niceshot")
            moments.append(merged)
    return moments


def _build_phrase_parts(facts: dict[str, Any]) -> dict[str, str]:
    subject = facts.get("entity_name") or "this play"
    action = _action_phrase(facts)
    stakes = _stakes_phrase(facts)
    angle = _select_category(facts)
    return {
        "subject": subject,
        "action": action,
        "stakes": stakes,
        "angle": angle,
    }


def _action_phrase(facts: dict[str, Any]) -> str:
    event = str(facts.get("event_label") or "").lower()
    kill_count = _safe_int(facts.get("kill_count"))
    headshot_count = _safe_int(facts.get("headshot_count"))
    niceshot_score = _safe_float(facts.get("niceshot_action_score"))

    if "wipe" in event or kill_count >= 5:
        return "wipes the team"
    if "ultimate" in event or "ult" in event:
        return "turns the fight with an ultimate"
    if "clutch" in event:
        return "lands the clutch play"
    if "headshot" in event or headshot_count >= 2:
        return "chains the headshots"
    if "precision" in event or headshot_count == 1:
        return "lands the clutch pick"
    if "multi" in event or kill_count >= 3:
        return "turns the fight"
    if kill_count >= 2:
        return "chains a multi-kill"
    if niceshot_score >= 0.75:
        return "finds the opening"
    return "makes the play"


def _stakes_phrase(facts: dict[str, Any]) -> str:
    hook_timestamp = facts.get("hook_timestamp")
    composite_score = _safe_float(facts.get("composite_score"))
    kill_count = _safe_int(facts.get("kill_count"))

    if facts.get("hook_passed") and hook_timestamp is not None and float(hook_timestamp) <= 1.5:
        return "inside the first second"
    if composite_score >= 0.8:
        return "when the fight looked lost"
    if kill_count >= 3:
        return "before they can reset"
    if facts.get("hook_passed"):
        return "right off the opener"
    return ""


def _build_variables(facts: dict[str, Any], phrases: dict[str, str]) -> dict[str, Any]:
    event = facts.get("event_label") or facts.get("moment_label") or ""
    kill_count = _safe_int(facts.get("kill_count"))
    headshot_count = _safe_int(facts.get("headshot_count"))
    variables = {
        "subject": phrases["subject"],
        "action": phrases["action"],
        "stakes": phrases["stakes"],
        "angle": phrases["angle"],
        "event": event,
        "moment": facts.get("moment_label") or event,
        "game": facts.get("game_name") or "",
        "entity": facts.get("entity_name") or "",
        "entity_id": facts.get("entity_id") or "",
        "weapon": phrases["subject"],
        "hero": facts.get("entity_name") or "",
        "kill_count": str(kill_count) if kill_count else "",
        "headshot_count": str(headshot_count) if headshot_count else "",
        "enemy_count": str(kill_count) if kill_count > 1 else "",
    }
    return variables


def _select_category(facts: dict[str, Any]) -> str:
    kill_count = _safe_int(facts.get("kill_count"))
    headshot_count = _safe_int(facts.get("headshot_count"))
    niceshot_score = _safe_float(facts.get("niceshot_action_score"))
    composite_score = _safe_float(facts.get("composite_score"))
    event = str(facts.get("event_label") or facts.get("moment_label") or "").lower()

    if kill_count >= 3 or headshot_count >= 2 or niceshot_score >= 0.75:
        return "performance_hype"
    if "ultimate" in event or "wipe" in event or composite_score >= 0.78:
        return "broken_balance"
    if facts.get("hook_passed") or kill_count >= 1 or "clutch" in event:
        return "reactionary_shock"
    return "meta_authority"


def _select_title_from_templates(
    titles_data: dict,
    game: str,
    category: str,
    variables: dict,
    history: dict,
    te_cfg: dict,
) -> dict | None:
    for fallback_level, section in (("game_template", game), ("generic_template", "generic")):
        templates = _get_templates(titles_data, section, category)
        selected = _pick_template(templates, variables, history, te_cfg)
        if selected:
            selected["fallback_level"] = fallback_level
            return selected

    for fallback_level, section in (("game_template", game), ("generic_template", "generic")):
        for fallback_category in _CATEGORIES:
            if fallback_category == category:
                continue
            templates = _get_templates(titles_data, section, fallback_category)
            selected = _pick_template(templates, variables, history, te_cfg)
            if selected:
                selected["fallback_level"] = fallback_level
                return selected
    return None


def _get_templates(titles_data: dict, game: str, category: str) -> list[dict]:
    raw_templates = titles_data.get(game, {}).get(category, [])
    return [_normalize_template(item) for item in raw_templates if _normalize_template(item)]


def _normalize_template(item: Any) -> dict | None:
    if isinstance(item, str):
        return {"template": item, "required": []}
    if isinstance(item, dict):
        template = item.get("template") or item.get("text") or item.get("title")
        if not template:
            return None
        required = item.get("required") or item.get("requires") or []
        if isinstance(required, str):
            required = [required]
        return {
            "template": str(template),
            "required": [str(value) for value in required],
            "id": item.get("id"),
        }
    return None


def _pick_template(templates: list[dict], variables: dict, history: dict, te_cfg: dict) -> dict | None:
    if not templates:
        return None
    window = int(te_cfg.get("history_window", _DEFAULT_HISTORY_WINDOW))
    recent_templates = set((history.get("hashes") or [])[-window:])
    recent_titles = set((history.get("title_hashes") or [])[-window:])
    fallback: dict | None = None

    for item in templates:
        template = item["template"]
        if not _has_required_variables(item, variables):
            continue
        title = _clean_title(_safe_format(template, variables))
        if not title:
            continue
        candidate = {
            "title": title,
            "template": template,
            "template_hash": _hash(template),
            "title_hash": _hash(title.lower()),
        }
        if fallback is None:
            fallback = candidate
        if candidate["template_hash"] in recent_templates or candidate["title_hash"] in recent_titles:
            continue
        return candidate
    return fallback


def _has_required_variables(item: dict, variables: dict) -> bool:
    for key in item.get("required") or []:
        if variables.get(key) in ("", None):
            return False
    return True


def _safe_format(template: str, variables: dict) -> str:
    result = template
    for key in re.findall(r"\{(\w+)\}", template):
        result = result.replace(f"{{{key}}}", str(variables.get(key, "")))
    return result


def _clean_title(value: str) -> str:
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\s+([?!.,:;])", r"\1", value)
    value = value.strip(" -|")
    return value[:100].strip()


def _generated_title(facts: dict[str, Any], variables: dict) -> tuple[str, str]:
    subject = variables.get("subject") or "this play"
    action = variables.get("action") or ""
    stakes = variables.get("stakes") or ""
    game = variables.get("game") or ""

    if subject != "this play" and action:
        return _clean_title(" ".join(part for part in [subject, action, stakes] if part)), "generated"
    if facts.get("event_label"):
        return _clean_title(f"This {facts['event_label']} was too clean"), "generated"
    if game:
        return _clean_title(f"{game} clip"), "generated"
    return _BARE_TITLE, "bare"


def _generate_hashtags(facts: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    game_name = facts.get("game_name") or facts.get("game")
    entity_name = facts.get("entity_name")
    event = str(facts.get("event_label") or facts.get("moment_label") or "").lower()

    if game_name:
        game_tag = "#" + _tagify(str(game_name))
        tags.extend([game_tag, game_tag + "Clips"])
    if entity_name:
        tags.append("#" + _tagify(str(entity_name)))

    kill_count = _safe_int(facts.get("kill_count"))
    headshot_count = _safe_int(facts.get("headshot_count"))
    if kill_count >= 5:
        tags.append("#TeamWipe")
    elif kill_count >= 3:
        tags.append("#TripleKill")
    elif kill_count >= 2:
        tags.append("#MultiKill")
    elif kill_count == 1:
        tags.append("#Clutch")

    if headshot_count > 0 or "headshot" in event or "precision" in event:
        tags.append("#Headshot")
    if facts.get("hook_passed"):
        tags.append("#InstantHook")
    tags.extend(["#Gaming", "#GamingClips", "#FPS"])
    return _dedupe(tags)[:12]


def _generate_caption(title: str, hashtags: list[str]) -> str:
    caption = f"{title}\n\n{' '.join(hashtags)}".strip()
    return caption[:280].strip()


def _calculate_title_confidence(facts: dict[str, Any], fallback_level: str) -> float:
    base = _safe_float(facts.get("confidence"))
    if fallback_level == "game_template":
        base += 0.1
    elif fallback_level == "generic_template":
        base += 0.05
    elif fallback_level == "bare":
        base = min(base, 0.25)
    return round(max(0.0, min(1.0, base)), 3)


def _aggregate_confidence(facts: dict[str, Any]) -> float:
    values = [
        _safe_float(facts.get("entity_confidence")),
        _safe_float(facts.get("context_confidence")),
        _safe_float(facts.get("niceshot_confidence")),
        _safe_float(facts.get("composite_score")),
    ]
    values = [value for value in values if value > 0]
    if not values:
        return 0.0
    return round(sum(values) / len(values), 3)


def _fact_explanation(facts: dict[str, Any]) -> list[str]:
    explanation: list[str] = []
    if facts.get("entity_name"):
        explanation.append(f"Resolved subject from {facts.get('entity_source')}: {facts.get('entity_name')}.")
    else:
        explanation.append("No entity was resolved; using safe subject fallback.")
    if facts.get("event_label"):
        explanation.append(f"Resolved event from {facts.get('event_source')}: {facts.get('event_label')}.")
    if facts.get("moment_label"):
        explanation.append(f"Top moment source: {facts.get('moment_source')}.")
    if facts.get("hook_passed"):
        explanation.append("Hook gate passed for title framing.")
    return explanation


def _load_history(te_cfg: dict, game: str) -> dict:
    history_path = Path(te_cfg.get("history_path", _DEFAULT_HISTORY_PATH))
    try:
        if history_path.exists():
            data = json.loads(history_path.read_text())
            game_data = data.get(game, {})
            if isinstance(game_data, list):
                return {"hashes": game_data, "title_hashes": []}
            return {
                "hashes": list(game_data.get("hashes") or []),
                "title_hashes": list(game_data.get("title_hashes") or []),
                "titles": list(game_data.get("titles") or []),
                "last_fallback_category": game_data.get("last_fallback_category"),
            }
    except (json.JSONDecodeError, OSError):
        pass
    return {"hashes": [], "title_hashes": [], "titles": []}


def _update_history(te_cfg: dict, game: str, template: str, title: str) -> None:
    history_path = Path(te_cfg.get("history_path", _DEFAULT_HISTORY_PATH))
    window = int(te_cfg.get("history_window", _DEFAULT_HISTORY_WINDOW))
    try:
        data: dict = {}
        if history_path.exists():
            data = json.loads(history_path.read_text())
        game_data = data.get(game, {})
        if isinstance(game_data, list):
            game_data = {"hashes": game_data}
        hashes = list(game_data.get("hashes") or [])
        title_hashes = list(game_data.get("title_hashes") or [])
        titles = list(game_data.get("titles") or [])
        hashes.append(_hash(template))
        title_hashes.append(_hash(title.lower()))
        titles.append(title)
        game_data["hashes"] = hashes[-(window * 2):]
        game_data["title_hashes"] = title_hashes[-(window * 2):]
        game_data["titles"] = titles[-(window * 2):]
        data[game] = game_data
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(json.dumps(data, indent=2))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"[title_engine] Could not update history: {exc}")


def _write_result(meta_path: Path, result: dict) -> dict:
    try:
        existing = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        existing["title_engine"] = result
        meta_path.write_text(json.dumps(existing, indent=2))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"[title_engine] Could not write meta: {exc}")
    return result


def _empty(reason: str) -> dict:
    return {
        "title": None,
        "caption": "",
        "category": None,
        "template_used": None,
        "variables": {},
        "hashtags": [],
        "confidence": 0.0,
        "fallback_level": "bare",
        "explanation": [reason],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "reason": reason,
    }


def _humanize_label(value: Any) -> str | None:
    if value in ("", None):
        return None
    return str(value).replace("_", " ").replace("-", " ").strip().lower()


def _tagify(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "", value)
    return cleaned or "Gaming"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value in ("", None):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _hash(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()[:8]
