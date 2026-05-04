from __future__ import annotations

import re
from typing import Any


_HERO_ROLE_TOKENS = {
    "duelist": "duelist",
    "vanguard": "vanguard",
    "strategist": "strategist",
    "support": "support",
    "tank": "tank",
}
_ABILITY_CLASS_TOKENS = {
    "ultimate": "ultimate",
    "ult": "ultimate",
    "utility": "utility",
    "passive": "passive",
    "mobility": "mobility",
    "movement": "mobility",
    "equipment": "equipment",
    "loadout": "equipment",
}
_EVENT_CATEGORY_TOKENS = {
    "combat": "combat",
    "kill": "combat",
    "headshot": "combat",
    "outcome": "outcome",
    "objective": "objective",
    "ability": "ability",
    "medal": "combat",
    "badge": "combat",
}


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "item"


def extract_event_name(text: str, *, section_heading: str) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    match = re.match(r"^(?:The\s+)?([A-Z][A-Za-z0-9'&().:/ -]{1,80}?)\s+event\b", cleaned)
    if match:
        return clean_text(match.group(1))
    if cleaned.count(" ") <= 5 and "." not in cleaned:
        return clean_text(re.sub(r"\s*[:\-–]\s*.*$", "", cleaned))
    if "events" in section_heading.casefold():
        return ""
    return clean_text(re.sub(r"\s*[:\-–]\s*.*$", "", cleaned))


def binding_key(value: str) -> str:
    tokens = [token for token in slugify(value).split("_") if token]
    return "_".join(tokens)


def aliases_equivalent(left: str, right: str) -> bool:
    left_key = binding_key(clean_text(left))
    right_key = binding_key(clean_text(right))
    return bool(left_key and right_key and left_key == right_key)


def identity_keys(
    display_name: str,
    *,
    canonical_id: str = "",
    aliases: list[str] | None = None,
) -> set[str]:
    keys = {
        binding_key(clean_text(display_name)),
        binding_key(clean_text(canonical_id)),
    }
    for alias in aliases or []:
        keys.add(binding_key(clean_text(alias)))
    keys.discard("")
    return keys


def reconcile_identity(
    existing: dict[str, Any],
    candidate: dict[str, Any],
    *,
    preferred_source: str,
) -> dict[str, Any]:
    existing_name = clean_text(str(existing.get("display_name", "")))
    existing_id = clean_text(str(existing.get("canonical_id", "")))
    existing_aliases = [clean_text(str(item)) for item in existing.get("aliases", []) if clean_text(str(item))]
    candidate_name = clean_text(str(candidate.get("display_name", "")))
    candidate_id = clean_text(str(candidate.get("canonical_id", "")))
    candidate_aliases = [clean_text(str(item)) for item in candidate.get("aliases", []) if clean_text(str(item))]

    existing_keys = identity_keys(existing_name, canonical_id=existing_id, aliases=existing_aliases)
    candidate_keys = identity_keys(candidate_name, canonical_id=candidate_id, aliases=candidate_aliases)
    shared_keys = sorted(existing_keys & candidate_keys)

    if not shared_keys:
        return {
            "match_status": "identity_match_rejected",
            "chosen_display_name": existing_name,
            "chosen_canonical_id": existing_id,
            "basis": "no_shared_identity_key",
            "blocking": True,
            "candidate_ids": [candidate_id] if candidate_id else [],
            "candidate_names": [candidate_name] if candidate_name else [],
        }

    existing_has_candidate_name_alias = any(aliases_equivalent(alias, candidate_name) for alias in existing_aliases)
    candidate_has_existing_name_alias = any(aliases_equivalent(alias, existing_name) for alias in candidate_aliases)

    if preferred_source == "starter_seed":
        chosen_name = candidate_name or existing_name
        chosen_id = candidate_id or existing_id
    elif aliases_equivalent(existing_name, candidate_name):
        chosen_name = _preferred_display_name(existing_name, candidate_name)
        chosen_id = candidate_id or existing_id
    elif existing_has_candidate_name_alias and not candidate_has_existing_name_alias:
        chosen_name = existing_name or candidate_name
        chosen_id = existing_id or candidate_id
    elif candidate_has_existing_name_alias and not existing_has_candidate_name_alias:
        chosen_name = candidate_name or existing_name
        chosen_id = candidate_id or existing_id
    else:
        return {
            "match_status": "conflicting_identity_match",
            "chosen_display_name": existing_name,
            "chosen_canonical_id": existing_id,
            "basis": "shared_alias_without_canonical_preference",
            "blocking": True,
            "candidate_ids": [candidate_id] if candidate_id else [],
            "candidate_names": [candidate_name] if candidate_name else [],
        }

    if not chosen_name:
        chosen_name = existing_name or candidate_name
    if not chosen_id:
        chosen_id = existing_id or candidate_id or binding_key(chosen_name)

    name_changed = bool(chosen_name and chosen_name != existing_name)
    id_changed = bool(chosen_id and chosen_id != existing_id)
    if name_changed and id_changed:
        match_status = "canonical_identity_preference_applied"
        basis = "display_name_and_id_preference"
    elif name_changed:
        match_status = "canonical_identity_preference_applied"
        basis = "display_name_preference"
    elif id_changed:
        match_status = "canonical_identity_preference_applied"
        basis = "id_only_preference"
    else:
        match_status = "exact_identity_match"
        basis = "shared_identity_key"

    return {
        "match_status": match_status,
        "chosen_display_name": chosen_name,
        "chosen_canonical_id": chosen_id,
        "basis": basis,
        "blocking": False,
        "candidate_ids": [candidate_id] if candidate_id else [],
        "candidate_names": [candidate_name] if candidate_name else [],
        "identity_source": preferred_source,
    }


def merge_aliases(
    existing_aliases: list[str],
    candidate_aliases: list[str],
    *,
    canonical_name: str,
) -> tuple[list[str], list[dict[str, str]]]:
    merged_aliases = [clean_text(alias) for alias in existing_aliases if clean_text(alias)]
    rejections: list[dict[str, str]] = []
    for alias in candidate_aliases:
        cleaned = clean_text(alias)
        if not cleaned:
            continue
        if aliases_equivalent(cleaned, canonical_name):
            rejections.append({"status": "alias_equivalent_to_canonical_name", "alias": cleaned})
            continue
        if any(aliases_equivalent(cleaned, existing_alias) for existing_alias in merged_aliases):
            rejections.append({"status": "alias_equivalent_to_existing_alias", "alias": cleaned})
            continue
        merged_aliases.append(cleaned)
    return merged_aliases, rejections


def extract_structured_fields(
    text: str,
    *,
    source_role: str,
    section_heading: str,
    record_type: str | None,
) -> dict[str, Any]:
    cleaned = clean_text(text)
    display_name = _normalize_display_name(cleaned, source_role=source_role, section_heading=section_heading, record_type=record_type)
    parenthetical_tokens = re.findall(r"\(([^)]+)\)", cleaned)
    role_candidates = set(_infer_field_candidates(cleaned, section_heading, _HERO_ROLE_TOKENS))
    class_candidates = set(_infer_field_candidates(cleaned, section_heading, _ABILITY_CLASS_TOKENS))
    category_candidates = set(_infer_field_candidates(cleaned, section_heading, _EVENT_CATEGORY_TOKENS))
    aliases, alias_ambiguity = _extract_aliases(cleaned, parenthetical_tokens, role_candidates | class_candidates | category_candidates)
    findings: list[dict[str, Any]] = []

    role_value = _resolve_single_value(role_candidates, field_name="role", findings=findings)
    class_value = _resolve_single_value(class_candidates, field_name="class", findings=findings)
    category_value = _resolve_single_value(category_candidates, field_name="category", findings=findings)
    if alias_ambiguity:
        findings.append({"status": "alias_ambiguity", "candidate_aliases": alias_ambiguity})

    if record_type == "ability_or_equipment" and not class_value:
        findings.append({"status": "weak_source_extraction", "field": "class"})
    if record_type == "event_badge_or_medal" and not category_value:
        findings.append({"status": "weak_source_extraction", "field": "category"})

    return {
        "display_name": display_name,
        "aliases": aliases,
        "role": role_value,
        "role_source": "source" if role_value else "",
        "class": class_value,
        "class_source": "source" if class_value else "",
        "category": category_value,
        "category_source": "source" if category_value else "",
        "aliases_source": "source" if aliases else "",
        "findings": findings,
    }


def find_explicit_listing_match(text: str, candidate_names: list[str]) -> str:
    matched_names = find_explicit_listing_matches(text, candidate_names)
    if len(matched_names) != 1:
        return ""
    return matched_names[0]


def find_explicit_listing_matches(text: str, candidate_names: list[str]) -> list[str]:
    cleaned = clean_text(text)
    if not cleaned:
        return []
    matches: list[str] = []
    for candidate in candidate_names:
        label = clean_text(candidate)
        if not label:
            continue
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(label)}(?![A-Za-z0-9])", cleaned, flags=re.IGNORECASE):
            matches.append(candidate)
    unique_matches: list[str] = []
    for match in matches:
        if match not in unique_matches:
            unique_matches.append(match)
    return unique_matches


def _normalize_display_name(text: str, *, source_role: str, section_heading: str, record_type: str | None) -> str:
    if record_type == "event_badge_or_medal" or source_role.strip().lower() in {"events", "medals"}:
        event_name = extract_event_name(text, section_heading=section_heading or "")
        if event_name:
            return event_name
    stripped = re.sub(r"\(([^)]+)\)", "", text)
    return clean_text(re.sub(r"\s*[:\-–]\s*.*$", "", stripped))


def _infer_field_candidates(text: str, section_heading: str, token_map: dict[str, str]) -> list[str]:
    candidates: list[str] = []
    lowered = f"{text} {section_heading}".casefold()
    for token, value in token_map.items():
        if re.search(rf"\b{re.escape(token)}s?\b", lowered):
            candidates.append(value)
    return candidates


def _resolve_single_value(candidates: set[str], *, field_name: str, findings: list[dict[str, Any]]) -> str:
    if not candidates:
        return ""
    if len(candidates) > 1:
        findings.append({"status": "ambiguous_structured_extraction", "field": field_name, "candidate_values": sorted(candidates)})
        return ""
    return next(iter(candidates))


def _extract_aliases(text: str, parenthetical_tokens: list[str], reserved_tokens: set[str]) -> tuple[list[str], list[str]]:
    aliases: list[str] = []
    ambiguous: list[str] = []
    lowered = text.casefold()
    aka_match = re.search(r"\baka\s+([A-Za-z0-9'&().:/ -]+)$", text, flags=re.IGNORECASE)
    if aka_match:
        alias = clean_text(aka_match.group(1))
        if alias:
            aliases.append(alias)
    for token in parenthetical_tokens:
        cleaned = clean_text(token)
        if not cleaned:
            continue
        if binding_key(cleaned) in {binding_key(item) for item in reserved_tokens if item}:
            continue
        if "/" in cleaned:
            parts = [clean_text(part) for part in cleaned.split("/") if clean_text(part)]
            if len(parts) > 1:
                ambiguous.extend(parts)
                continue
        aliases.append(cleaned)
    aliases = sorted({alias for alias in aliases if alias})
    return aliases, sorted({alias for alias in ambiguous if alias and alias not in aliases})


def _preferred_display_name(left: str, right: str) -> str:
    left_clean = clean_text(left)
    right_clean = clean_text(right)
    if len(right_clean) > len(left_clean):
        return right_clean
    return left_clean or right_clean
