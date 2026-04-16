"""
Rank Looker — Stage 0.5 (pre-download skill filter)

Evaluates Twitch clip metadata (title, broadcaster follower count) BEFORE
anything is downloaded. Skips clips that show no skill signals, reducing
wasted bandwidth and downstream processing on low-quality content.

Two modes (configured under rank_looker in config.yaml):
    heuristic  — title NLP + Twitch follower count proxy (default; no extra API needed)
    api        — Tracker.gg in-game rank lookup (not yet implemented; planned for Phase 2)

Heuristic logic (applied in order):
    1. Smurf keyword in title          → reject (overrides follower count)
    2. Follower count >= threshold     → accept (Verified Skill proxy)
    3. Sweat keyword in title          → accept
    4. No skill signals + strict=false → accept (permissive default)
    4. No skill signals + strict=true  → reject

Result dict returned by check_clip():
    passed  bool   True = download this clip
    reason  str    Human-readable explanation for the decision
    method  str    "disabled" | "smurf" | "social_proxy" | "title_nlp" | "no_signal"
"""

import os
from utils.logger import get_logger

logger = get_logger(__name__)


def check_clip(clip_meta: dict, config: dict) -> dict:
    """Evaluate a clip's Twitch metadata for skill signals.

    Args:
        clip_meta: Dict containing at minimum:
                   title (str), broadcaster_name (str),
                   broadcaster_follower_count (int, 0 if unknown)
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
            # Phase 2: implement Tracker.gg rank lookup here.
            # Until implemented, fall through to heuristic.
            logger.debug(
                "rank_looker mode=api not yet implemented — "
                "falling back to heuristic."
            )
        else:
            logger.debug(
                "rank_looker mode=api but no TRACKER_GG_API_KEY — "
                "falling back to heuristic."
            )

    return _heuristic_check(clip_meta, rl_cfg)


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
