"""
Rank Looker — Stage 0.5 (pre-download skill filter)

Evaluates Twitch clip metadata (title, broadcaster follower count) BEFORE
anything is downloaded. Skips clips that show no skill signals, reducing
wasted bandwidth and downstream processing on low-quality content.

Two modes (configured under rank_looker in config.yaml):
    heuristic  — title NLP + Twitch follower count proxy (default; no extra API needed)
    api        — Tracker.gg in-game rank lookup (requires TRACKER_GG_API_KEY)

Heuristic logic (applied in order):
    1. Smurf keyword in title          → reject (overrides follower count)
    2. Follower count >= threshold     → accept (Verified Skill proxy)
    3. Sweat keyword in title          → accept
    4. No skill signals + strict=false → accept (permissive default)
    4. No skill signals + strict=true  → reject

API mode logic (Tracker.gg):
    1. Look up the broadcaster's rank by username on Tracker.gg
    2. If rank data unavailable (404, quota, network error) → accept (permissive)
    3. If game has no configured rank threshold             → accept (permissive)
    4. Compare rank against game_rank_thresholds in config
       - rank >= threshold → accept
       - rank <  threshold → reject

Result dict returned by check_clip():
    passed  bool   True = download this clip
    reason  str    Human-readable explanation for the decision
    method  str    "disabled" | "smurf" | "social_proxy" | "title_nlp" | "no_signal"
                   | "api_rank" | "api_no_data" | "api_no_threshold"
"""

import os

import requests

from utils.logger import get_logger

logger = get_logger(__name__)

_TRACKER_GG_BASE = "https://public-api.tracker.gg/v2"

# Tracker.gg game slug per config game key.
_TRACKER_GAME_SLUG: dict[str, str] = {
    "marvel_rivals": "marvel-rivals",
    "deadlock": "deadlock",
}

# Default platform identifier per game (what the username is treated as).
_TRACKER_PLATFORM: dict[str, str] = {
    "marvel_rivals": "ign",    # NetEase in-game name; often matches Twitch handle
    "deadlock": "steam",       # Steam username; often matches Twitch handle
}

# Rank tiers in ascending order (lowest first) per game.
# Prefix-matched, so "Platinum I" → "platinum" → index 3.
_RANK_ORDERS: dict[str, list[str]] = {
    "marvel_rivals": [
        "bronze", "silver", "gold", "platinum", "diamond",
        "grandmaster", "celestial", "eternity",
    ],
    "deadlock": [
        "seeker", "alchemist", "ritualist", "emissary",
        "archon", "oracle", "phantom", "ascendant", "eternus",
    ],
}


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def check_clip(clip_meta: dict, config: dict) -> dict:
    """Evaluate a clip's Twitch metadata for skill signals.

    Args:
        clip_meta: Dict containing at minimum:
                   title (str), broadcaster_name (str),
                   broadcaster_follower_count (int, 0 if unknown),
                   game (str, config game key e.g. 'deadlock')
        config:    Full parsed config.yaml dict.

    Returns:
        {'passed': bool, 'reason': str, 'method': str}
    """
    rl_cfg = config.get("rank_looker", {})

    if not rl_cfg.get("enabled", False):
        return {"passed": True, "reason": "rank_looker disabled", "method": "disabled"}

    mode = rl_cfg.get("mode", "heuristic")

    if mode == "api":
        api_key = rl_cfg.get("tracker_gg_api_key") or os.getenv("TRACKER_GG_API_KEY")
        if api_key:
            return _api_check(clip_meta, rl_cfg, api_key)
        else:
            logger.debug(
                "rank_looker mode=api but no TRACKER_GG_API_KEY — "
                "falling back to heuristic."
            )

    return _heuristic_check(clip_meta, rl_cfg)


# ---------------------------------------------------------------------------
# Heuristic mode
# ---------------------------------------------------------------------------

def _heuristic_check(clip_meta: dict, rl_cfg: dict) -> dict:
    """Title NLP + Twitch follower count heuristic check."""
    title = clip_meta.get("title", "").lower()
    broadcaster_name = clip_meta.get("broadcaster_name", "unknown")
    follower_count = int(clip_meta.get("broadcaster_follower_count", 0))

    smurf_keywords = [k.lower() for k in rl_cfg.get("smurf_keywords", [])]
    sweat_keywords = [k.lower() for k in rl_cfg.get("sweat_keywords", [])]
    min_followers = int(rl_cfg.get("min_follower_count", 50000))
    strict = rl_cfg.get("strict", False)

    # 1. Smurf detection — reject regardless of follower count.
    smurf_hits = [k for k in smurf_keywords if k in title]
    if smurf_hits:
        reason = f"smurf keyword(s) in title: {smurf_hits}"
        logger.info(f"[rank_looker] SKIP '{broadcaster_name}' — {reason}")
        return {"passed": False, "reason": reason, "method": "smurf"}

    # 2. Social rank proxy — high follower count treated as Verified Skill.
    if follower_count >= min_followers:
        reason = f"social proxy: {follower_count:,} followers ≥ {min_followers:,}"
        logger.debug(f"[rank_looker] PASS '{broadcaster_name}' — {reason}")
        return {"passed": True, "reason": reason, "method": "social_proxy"}

    # 3. Title NLP — any sweat keyword is a positive skill signal.
    sweat_hits = [k for k in sweat_keywords if k in title]
    if sweat_hits:
        reason = f"sweat keyword(s) in title: {sweat_hits}"
        logger.debug(f"[rank_looker] PASS '{broadcaster_name}' — {reason}")
        return {"passed": True, "reason": reason, "method": "title_nlp"}

    # 4. No positive signals.
    if strict:
        reason = "no skill signals found (strict mode)"
        logger.info(f"[rank_looker] SKIP '{broadcaster_name}' — {reason}")
        return {"passed": False, "reason": reason, "method": "no_signal"}

    reason = "no skill signals — permissive pass"
    logger.debug(f"[rank_looker] PASS '{broadcaster_name}' — {reason}")
    return {"passed": True, "reason": reason, "method": "no_signal"}


# ---------------------------------------------------------------------------
# API mode (Tracker.gg)
# ---------------------------------------------------------------------------

def _api_check(clip_meta: dict, rl_cfg: dict, api_key: str) -> dict:
    """Tracker.gg rank lookup for the clip's broadcaster.

    Design decisions (confirmed):
    - Look up the *streamer's* rank, not an opponent's.
    - If rank data is unavailable for any reason → permissive pass.
    - If the game has no configured threshold → permissive pass.
    """
    broadcaster_name = clip_meta.get("broadcaster_name", "unknown")
    game_key = clip_meta.get("game", "")

    threshold = rl_cfg.get("game_rank_thresholds", {}).get(game_key)
    if not threshold:
        reason = f"no rank threshold configured for '{game_key}' — permissive pass"
        logger.debug(f"[rank_looker] PASS '{broadcaster_name}' — {reason}")
        return {"passed": True, "reason": reason, "method": "api_no_threshold"}

    rank = _lookup_tracker_gg_rank(broadcaster_name, game_key, api_key)
    if rank is None:
        reason = f"rank data unavailable for '{broadcaster_name}' on Tracker.gg — permissive pass"
        logger.debug(f"[rank_looker] PASS '{broadcaster_name}' — {reason}")
        return {"passed": True, "reason": reason, "method": "api_no_data"}

    passed = _rank_passes_threshold(rank, threshold, game_key)
    if passed:
        reason = f"rank '{rank}' >= threshold '{threshold}' for {game_key}"
        logger.debug(f"[rank_looker] PASS '{broadcaster_name}' — {reason}")
        return {"passed": True, "reason": reason, "method": "api_rank"}

    reason = f"rank '{rank}' below threshold '{threshold}' for {game_key}"
    logger.info(f"[rank_looker] SKIP '{broadcaster_name}' — {reason}")
    return {"passed": False, "reason": reason, "method": "api_rank"}


def _lookup_tracker_gg_rank(username: str, game_key: str, api_key: str) -> str | None:
    """Return the player's current rank tier (lowercase) from Tracker.gg, or None.

    Uses the broadcaster's Twitch username as the platform identifier — many
    streamers keep consistent usernames across platforms. Returns None on any
    lookup failure (404, quota exceeded, parse error, network error).
    """
    slug = _TRACKER_GAME_SLUG.get(game_key)
    platform = _TRACKER_PLATFORM.get(game_key)
    if not slug or not platform:
        logger.debug(
            f"[rank_looker] Tracker.gg: no slug/platform configured for game '{game_key}'."
        )
        return None

    url = f"{_TRACKER_GG_BASE}/{slug}/standard/profile/{platform}/{username}"
    try:
        resp = requests.get(
            url,
            headers={
                "TRN-Api-Key": api_key,
                "Accept": "application/json",
            },
            timeout=10,
        )
        if resp.status_code == 404:
            logger.debug(
                f"[rank_looker] Tracker.gg: profile not found for '{username}' ({game_key})."
            )
            return None
        if resp.status_code == 429:
            logger.debug("[rank_looker] Tracker.gg: rate limited — permissive pass.")
            return None
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.debug(f"[rank_looker] Tracker.gg lookup failed for '{username}': {e}")
        return None

    # Parse rank tier from segments. Tracker.gg responses nest rank data under
    # the "overview" segment inside data.segments[].stats.
    for segment in data.get("data", {}).get("segments", []):
        if segment.get("type") != "overview":
            continue
        stats = segment.get("stats", {})
        # Stat key varies by game ("rank", "ranked", "rankScore", etc.)
        for stat_key in ("rank", "ranked", "rankScore", "seasonRank"):
            rank_stat = stats.get(stat_key)
            if not rank_stat:
                continue
            tier = (
                rank_stat.get("metadata", {}).get("tierName")
                or rank_stat.get("displayValue", "")
            )
            if tier and tier.strip():
                # Normalise "Platinum I" → "platinum", "Emissary 2" → "emissary"
                return tier.lower().split()[0]

    logger.debug(
        f"[rank_looker] Tracker.gg: no rank found in response for '{username}' ({game_key})."
    )
    return None


def _rank_passes_threshold(rank: str, threshold: str, game_key: str) -> bool:
    """Return True if rank >= threshold in the game's rank order.

    Both rank and threshold are matched as prefixes against the ordered tier list,
    so sub-tier suffixes ("I", "II", "2", etc.) are ignored.
    Unknown ranks or games → permissive True.
    """
    order = _RANK_ORDERS.get(game_key, [])
    if not order:
        logger.debug(
            f"[rank_looker] No rank order defined for '{game_key}' — permissive pass."
        )
        return True

    rank_lower = rank.lower()
    threshold_lower = threshold.lower()

    rank_idx = next((i for i, r in enumerate(order) if rank_lower.startswith(r)), None)
    threshold_idx = next((i for i, r in enumerate(order) if threshold_lower.startswith(r)), None)

    if rank_idx is None or threshold_idx is None:
        logger.debug(
            f"[rank_looker] Could not resolve rank='{rank}' or threshold='{threshold}' "
            f"in rank order for '{game_key}' — permissive pass."
        )
        return True

    return rank_idx >= threshold_idx
