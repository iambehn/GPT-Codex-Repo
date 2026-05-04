from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from pipeline.structured_source_fields import binding_key, clean_text


_PORTRAIT_TOKENS = ("portrait", "hero", "operator", "character", "roster", "duelist", "vanguard", "strategist")
_ICON_TOKENS = ("icon", "ability", "equipment", "skill", "loadout", "perk", "item")
_BADGE_TOKENS = ("badge", "medal", "emblem", "event", "objective")
_ARTWORK_TOKENS = ("artwork", "splash", "render", "banner", "poster", "key art")
_GENERIC_PAGE_ART_TOKENS = ("screenshot", "wallpaper", "background", "cinematic", "promo", "promotional")
_LOGO_TOKENS = ("site logo", "wiki logo", "logo")
_MAP_TOKENS = ("map", "verdansk", "rebirth", "arena", "stage")


def analyze_asset_candidate(
    *,
    display_name: str,
    asset_family: str,
    source_role: str,
    source_kind: str,
    source_url: str,
    section_heading: str = "",
    raw_label: str = "",
) -> dict[str, Any]:
    combined = " ".join(
        part
        for part in (
            clean_text(display_name),
            clean_text(raw_label),
            clean_text(section_heading),
            clean_text(source_role),
            clean_text(source_kind),
            clean_text(urlparse(source_url).path),
        )
        if part
    ).casefold()
    portrait_like = _has_any(combined, _PORTRAIT_TOKENS)
    icon_like = _has_any(combined, _ICON_TOKENS)
    badge_like = _has_any(combined, _BADGE_TOKENS)
    artwork_like = _has_any(combined, _ARTWORK_TOKENS)
    generic_page_art = _has_any(combined, _GENERIC_PAGE_ART_TOKENS)
    logo_like = _has_any(combined, _LOGO_TOKENS) or any(token in source_url.casefold() for token in ("site-logo", "wiki-logo"))
    map_like = _has_any(combined, _MAP_TOKENS)
    prose_like = len(clean_text(raw_label or display_name).split()) > 6 or "." in (raw_label or display_name)
    expected_kind = _expected_visual_kind(asset_family)

    quality_reasons: list[str] = []
    score = 0.42
    if source_kind == "direct_image":
        score += 0.20
        quality_reasons.append("direct image source")
    elif source_kind == "category_member_image":
        score += 0.10
        quality_reasons.append("category member image")
    elif source_kind == "gallery_image":
        score -= 0.04
        quality_reasons.append("gallery image source")
    elif source_kind == "infobox_image":
        score -= 0.03
        quality_reasons.append("infobox image source")
    else:
        quality_reasons.append("page image source")

    if expected_kind == "portrait" and portrait_like:
        score += 0.28
        quality_reasons.append("portrait-like label")
    elif expected_kind == "icon" and icon_like:
        score += 0.28
        quality_reasons.append("icon-like label")
    elif expected_kind == "badge" and badge_like:
        score += 0.28
        quality_reasons.append("badge-like label")

    if artwork_like:
        score -= 0.26
        quality_reasons.append("artwork-like image")
    if generic_page_art:
        score -= 0.18
        quality_reasons.append("generic promotional art")
    if map_like and expected_kind == "badge":
        score -= 0.22
        quality_reasons.append("map-like event image")
    if prose_like and expected_kind == "badge" and not badge_like:
        score -= 0.15
        quality_reasons.append("prose-like event label")

    hard_reject = False
    reject_reasons: list[str] = []
    if logo_like:
        hard_reject = True
        reject_reasons.append("site or wiki logo")
    if expected_kind in {"portrait", "icon"} and (artwork_like or generic_page_art) and not ((expected_kind == "portrait" and portrait_like) or (expected_kind == "icon" and icon_like)):
        hard_reject = True
        reject_reasons.append("artwork or screenshot mismatches expected asset kind")
    if expected_kind == "badge" and map_like:
        hard_reject = True
        reject_reasons.append("map-like image for medal or badge asset")
    if expected_kind == "badge" and prose_like and not badge_like:
        hard_reject = True
        reject_reasons.append("event image label looks like prose instead of badge or medal")

    score = max(0.0, min(score, 0.99))
    candidate_quality = "high" if score >= 0.75 else "medium" if score >= 0.48 else "low"
    return {
        "binding_key": binding_key(display_name),
        "candidate_quality": candidate_quality,
        "quality_score": round(score, 2),
        "quality_reasons": quality_reasons,
        "portrait_like": portrait_like,
        "icon_like": icon_like,
        "badge_like": badge_like,
        "artwork_like": artwork_like,
        "generic_page_art": generic_page_art,
        "logo_like": logo_like,
        "map_like": map_like,
        "prose_like": prose_like,
        "hard_reject": hard_reject,
        "reject_reasons": reject_reasons,
    }


def score_binding_candidate(
    *,
    target_display_name: str,
    target_aliases: list[str],
    asset_family: str,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    candidate_key = str(candidate.get("binding_key", "")).strip()
    target_keys = {binding_key(target_display_name)}
    target_keys.update(binding_key(alias) for alias in target_aliases if binding_key(alias))
    if not candidate_key or not any(target_keys):
        return {"score": 0.0, "reasons": [], "name_match_quality": "none", "flags": {}}

    reasons: list[str] = []
    flags = {
        "weak_name_match": False,
        "lower_trust_source_kind": False,
        "image_kind_mismatch": False,
    }
    score = 0.0
    name_match_quality = "none"
    if candidate_key in target_keys:
        score += 0.62
        reasons.append("exact normalized or alias match")
        name_match_quality = "exact"
    else:
        target_tokens = {token for key in target_keys for token in key.split("_") if token}
        candidate_tokens = {token for token in candidate_key.split("_") if token}
        overlap = target_tokens & candidate_tokens
        if overlap and overlap == target_tokens:
            score += 0.50
            reasons.append("target tokens fully contained in candidate name")
            name_match_quality = "strong"
        elif overlap and len(overlap) / max(len(target_tokens), 1) >= 0.6:
            score += 0.40
            reasons.append("strong token overlap")
            name_match_quality = "strong"
        elif target_display_name.casefold() in str(candidate.get("display_name", "")).casefold():
            score += 0.28
            reasons.append("target name appears in candidate label")
            name_match_quality = "weak"
            flags["weak_name_match"] = True
        else:
            return {"score": 0.0, "reasons": [], "name_match_quality": "none", "flags": flags}

    quality = str(candidate.get("candidate_quality", "low"))
    if quality == "high":
        score += 0.18
        reasons.append("high-quality candidate")
    elif quality == "medium":
        score += 0.08
        reasons.append("medium-quality candidate")
    else:
        score -= 0.05
        reasons.append("low-quality candidate")

    source_kind = str(candidate.get("source_kind", ""))
    if source_kind == "direct_image":
        score += 0.08
        reasons.append("direct image preferred")
    elif source_kind == "category_member_image":
        score += 0.04
        reasons.append("category member source")
    elif source_kind == "gallery_image":
        score -= 0.05
        reasons.append("lower-trust gallery image")
        flags["lower_trust_source_kind"] = True
    elif source_kind == "infobox_image":
        score -= 0.04
        reasons.append("lower-trust infobox image")
        flags["lower_trust_source_kind"] = True
    elif source_kind == "page_image":
        reasons.append("page image source")
    else:
        score -= 0.02
        reasons.append("lower-trust source kind")
        flags["lower_trust_source_kind"] = True

    expected_kind = _expected_visual_kind(asset_family)
    if expected_kind == "portrait":
        if candidate.get("portrait_like"):
            score += 0.08
            reasons.append("portrait-like image kind")
        elif candidate.get("artwork_like") or candidate.get("generic_page_art"):
            score -= 0.12
            reasons.append("portrait target but image looks like artwork")
            flags["image_kind_mismatch"] = True
    elif expected_kind == "icon":
        if candidate.get("icon_like"):
            score += 0.08
            reasons.append("icon-like image kind")
        elif candidate.get("artwork_like") or candidate.get("generic_page_art"):
            score -= 0.12
            reasons.append("icon target but image looks like artwork")
            flags["image_kind_mismatch"] = True
    elif expected_kind == "badge":
        if candidate.get("badge_like"):
            score += 0.08
            reasons.append("badge-like image kind")
        elif candidate.get("map_like") or candidate.get("generic_page_art"):
            score -= 0.12
            reasons.append("badge target but image looks like map or banner")
            flags["image_kind_mismatch"] = True

    score = max(0.0, min(score, 0.99))
    return {
        "score": round(score, 2),
        "reasons": reasons,
        "name_match_quality": name_match_quality,
        "flags": flags,
    }


def _expected_visual_kind(asset_family: str) -> str:
    if asset_family == "hero_portrait":
        return "portrait"
    if asset_family in {"ability_icon", "equipment_icon", "hud_icon"}:
        return "icon"
    if asset_family == "medal_icon":
        return "badge"
    return "generic"


def _has_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)
