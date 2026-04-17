"""
Title Engine — context-aware clip title generation

Loads title templates from assets/titles.yaml (configurable via
title_engine.titles_path). Selects a category based on kill-feed and
weapon-detection results already written to the clip's .meta.json, then
substitutes detected variables ({weapon}, {kill_count}, {enemy_count}, etc.)
to produce a ready-to-use upload title.

Deduplication: recently used template hashes are tracked per game in
assets/title_history.json. Templates become eligible again after
history_window clips.

Category selection logic (see _select_category for full detail):
    High action (multi-kill + headshots, or 4+ kills)
        → random between performance_hype / broken_balance
    Medium action (multi-kill or kill + headshot)
        → random between broken_balance / reactionary_shock / performance_hype
    Some action (sweat_score > 50 or any kill)
        → random between broken_balance / reactionary_shock
    Low action (no kill signals)
        → cycles through meta_authority / reactionary_shock / broken_balance

Config block (config.yaml → title_engine):
    enabled: false
    mode: "template"              # "template" | "llm" (future)
    titles_path: "assets/titles.yaml"
    history_path: "assets/title_history.json"
    history_window: 20            # avoid repeating a template within last N clips

LLM mode (future):
    When mode: "llm", the engine sends the structured variable dict to Claude
    with a short prompt. The CV layer keeps the output factually accurate;
    Claude provides variety. Requires anthropic client already available.
"""

from __future__ import annotations

import hashlib
import json
import random
import re
from datetime import datetime
from pathlib import Path

import yaml

from utils.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_TITLES_PATH = "assets/titles.yaml"
_DEFAULT_HISTORY_PATH = "assets/title_history.json"
_DEFAULT_HISTORY_WINDOW = 20


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def generate_title(clip_path: Path, game: str, config: dict) -> dict:
    """Generate a context-aware title for a clip.

    Reads weapon_detection and kill_feed keys from the clip's .meta.json
    (written by earlier pipeline stages), selects a template, substitutes
    variables, and writes the result back under 'title_engine'.

    Idempotent: skips if 'title_engine' key already exists in meta.json.

    Returns:
        {
            'title':         str | None,
            'category':      str | None,
            'template_used': str | None,
            'variables':     dict,
            'generated_at':  str,
        }
    """
    te_cfg = config.get("title_engine", {})
    meta_path = clip_path.with_suffix(".meta.json")

    # Idempotency
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

    titles_path = Path(te_cfg.get("titles_path", _DEFAULT_TITLES_PATH))
    if not titles_path.exists():
        logger.warning(f"[title_engine] titles.yaml not found at {titles_path}")
        return _write_result(meta_path, _empty(f"titles file not found: {titles_path}"))

    with open(titles_path) as f:
        titles_data = yaml.safe_load(f) or {}

    # Load accumulated clip metadata from disk (kill_feed + weapon_detection)
    clip_meta: dict = {}
    if meta_path.exists():
        try:
            clip_meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    variables = _extract_variables(clip_meta, game, config)
    category = _select_category(clip_meta, game, te_cfg)
    templates = _get_templates(titles_data, game, category)

    if not templates:
        templates = _get_templates(titles_data, "generic", category)
    if not templates:
        logger.debug(f"[title_engine] No templates for game='{game}' category='{category}'")
        return _write_result(meta_path, _empty(f"no templates for {game}/{category}"))

    history = _load_history(te_cfg, game)
    template_str = _pick_template(templates, history, te_cfg)
    title = _safe_format(template_str, variables)
    hashtags = _generate_hashtags(clip_meta, game, config)

    result = {
        "title":         title,
        "category":      category,
        "template_used": template_str,
        "variables":     {k: v for k, v in variables.items() if v != ""},
        "hashtags":      hashtags,
        "generated_at":  datetime.now().isoformat(timespec="seconds"),
    }

    _update_history(te_cfg, game, template_str)
    _write_result(meta_path, result)

    logger.info(
        f"[title_engine] '{title}' (category={category}, game={game})\n"
        f"  hashtags: {' '.join(hashtags)}"
    )
    return result


# ---------------------------------------------------------------------------
# Variable extraction
# ---------------------------------------------------------------------------

def _extract_variables(clip_meta: dict, game: str, config: dict) -> dict:
    """Build substitution variables from accumulated clip metadata."""
    kf = clip_meta.get("kill_feed", {})
    wd = clip_meta.get("weapon_detection", {})
    game_display = config.get("games", {}).get(game, {}).get("display_name", game)

    kill_count = kf.get("kill_count", 0)
    headshot_count = kf.get("headshot_count", 0)
    enemy_count = str(kill_count) if kill_count > 1 else ""

    return {
        "weapon":        wd.get("display_name") or "this weapon",
        "game":          game_display,
        "kill_count":    str(kill_count) if kill_count else "",
        "headshot_count":str(headshot_count) if headshot_count else "",
        "enemy_count":   enemy_count,
        "hero":          "",    # future: character/hero detection via HUD icon
        "map":           "",    # future: minimap or loading screen detection
        "season":        "",    # future: config field or patch-notes lookup
    }


# ---------------------------------------------------------------------------
# Hashtag generation
# ---------------------------------------------------------------------------

def _generate_hashtags(clip_meta: dict, game: str, config: dict) -> list[str]:
    """Build a hashtag list from detected signals for use in post descriptions.

    Sources:
      - Game name and a condensed variant (e.g., #MarvelRivals #MarvelRivalsClips)
      - Detected weapon display name (e.g., #SniperRifle)
      - Kill-feed events (#TripleKill, #Headshot, #Clutch, #MultiKill)
    """
    tags: list[str] = []
    game_display = config.get("games", {}).get(game, {}).get("display_name", game)

    # Game tags
    game_tag = "#" + game_display.replace(" ", "")
    tags.append(game_tag)
    tags.append(game_tag + "Clips")

    # Weapon tag
    wd = clip_meta.get("weapon_detection", {})
    if wd.get("display_name"):
        weapon_tag = "#" + wd["display_name"].replace(" ", "")
        tags.append(weapon_tag)

    # Kill-feed event tags
    kf = clip_meta.get("kill_feed", {})
    kill_count = kf.get("kill_count", 0)
    headshot_count = kf.get("headshot_count", 0)

    if kill_count >= 5:
        tags.append("#Ace")
    elif kill_count >= 4:
        tags.append("#QuadKill")
    elif kill_count >= 3:
        tags.append("#TripleKill")
    elif kill_count >= 2:
        tags.append("#MultiKill")
    elif kill_count == 1:
        tags.append("#Clutch")

    if headshot_count > 0:
        tags.append("#Headshot")
    if headshot_count >= 2:
        tags.append("#MultiHeadshot")

    # Universal gaming tags
    tags.extend(["#Gaming", "#GamingClips", "#FPS"])

    # Deduplicate while preserving order
    seen: set[str] = set()
    result = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            result.append(t)
    return result


# ---------------------------------------------------------------------------
# Category selection
# ---------------------------------------------------------------------------

def _select_category(clip_meta: dict, game: str, te_cfg: dict) -> str:
    """Map clip metadata to a psychological hook category.

    Four categories:
        meta_authority   — "pros are doing this" / FOMO
        broken_balance   — "this is unfair/OP" / controversy
        reactionary_shock— "I wasn't expecting this" / curiosity
        performance_hype — "popping off" / action energy

    Multiple categories can fit the same clip signal level, so we
    randomise between applicable options to keep variety across posts.
    """
    kf = clip_meta.get("kill_feed", {})
    kill_count = kf.get("kill_count", 0)
    headshot_count = kf.get("headshot_count", 0)
    sweat_score = float(kf.get("sweat_score", 0.0))

    # High action: multi-kill with headshots, or ace-level kill count
    if (headshot_count > 0 and kill_count >= 3) or kill_count >= 4:
        return random.choice(["performance_hype", "broken_balance"])

    # Medium-high action: multi-kill, or single kill with headshot
    if kill_count >= 2 or (kill_count >= 1 and headshot_count > 0):
        return random.choice(["broken_balance", "reactionary_shock", "performance_hype"])

    # Some action: high sweat score or any kill
    if sweat_score > 50 or kill_count >= 1:
        return random.choice(["broken_balance", "reactionary_shock"])

    # Low action: rotate through meta_authority and reactionary_shock
    fallback_cycle = ["meta_authority", "reactionary_shock", "broken_balance", "meta_authority"]
    history_path = Path(te_cfg.get("history_path", _DEFAULT_HISTORY_PATH))
    try:
        if history_path.exists():
            data = json.loads(history_path.read_text())
            last = data.get(game, {}).get("last_fallback_category", "broken_balance")
            try:
                idx = (fallback_cycle.index(last) + 1) % len(fallback_cycle)
            except ValueError:
                idx = 0
            next_cat = fallback_cycle[idx]
            data.setdefault(game, {})["last_fallback_category"] = next_cat
            history_path.write_text(json.dumps(data, indent=2))
            return next_cat
    except (json.JSONDecodeError, OSError):
        pass

    return "meta_authority"


# ---------------------------------------------------------------------------
# Template selection and formatting
# ---------------------------------------------------------------------------

def _get_templates(titles_data: dict, game: str, category: str) -> list[str]:
    return list(titles_data.get(game, {}).get(category, []))


def _pick_template(templates: list[str], history: list[str], te_cfg: dict) -> str:
    """Return a template not in the recent history window. Falls back if all are used."""
    window = int(te_cfg.get("history_window", _DEFAULT_HISTORY_WINDOW))
    recent = set(history[-window:])
    candidates = [t for t in templates if _hash(t) not in recent] or templates
    return random.choice(candidates)


def _safe_format(template: str, variables: dict) -> str:
    """Format a template string; replace unknown {keys} with an empty string."""
    result = template
    for key in re.findall(r'\{(\w+)\}', template):
        result = result.replace(f'{{{key}}}', str(variables.get(key, "")))
    # Clean up double spaces left by empty substitutions
    result = re.sub(r'  +', ' ', result).strip()
    return result


# ---------------------------------------------------------------------------
# History management
# ---------------------------------------------------------------------------

def _load_history(te_cfg: dict, game: str) -> list[str]:
    history_path = Path(te_cfg.get("history_path", _DEFAULT_HISTORY_PATH))
    try:
        if history_path.exists():
            data = json.loads(history_path.read_text())
            return data.get(game, {}).get("hashes", [])
    except (json.JSONDecodeError, OSError):
        pass
    return []


def _update_history(te_cfg: dict, game: str, template: str) -> None:
    history_path = Path(te_cfg.get("history_path", _DEFAULT_HISTORY_PATH))
    window = int(te_cfg.get("history_window", _DEFAULT_HISTORY_WINDOW))
    try:
        data: dict = {}
        if history_path.exists():
            data = json.loads(history_path.read_text())
        game_data = data.get(game, {"hashes": [], "last_fallback_category": "engagement_bait"})
        hashes = game_data.get("hashes", [])
        hashes.append(_hash(template))
        game_data["hashes"] = hashes[-(window * 2):]
        data[game] = game_data
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(json.dumps(data, indent=2))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"[title_engine] Could not update history: {e}")


def _hash(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()[:8]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _write_result(meta_path: Path, result: dict) -> dict:
    try:
        existing = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        existing["title_engine"] = result
        meta_path.write_text(json.dumps(existing, indent=2))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"[title_engine] Could not write meta: {e}")
    return result


def _empty(reason: str) -> dict:
    return {
        "title":         None,
        "category":      None,
        "template_used": None,
        "variables":     {},
        "hashtags":      [],
        "generated_at":  datetime.now().isoformat(timespec="seconds"),
        "reason":        reason,
    }
