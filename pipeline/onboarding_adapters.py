from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StarterSeedSpec:
    ontology_section: str
    file_name: str
    root_key: str
    source_kind: str
    id_field: str
    extra_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class GameOnboardingAdapter:
    game_id: str
    supported_source_roles: tuple[str, ...]
    role_to_kind: dict[str, str]
    role_asset_families: dict[str, str]
    disabled_families: tuple[str, ...] = ()
    family_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)
    starter_seed_specs: tuple[StarterSeedSpec, ...] = ()
    display_name_overrides: dict[str, str] = field(default_factory=dict)


_DEFAULT_SEED_SPECS = (
    StarterSeedSpec(
        ontology_section="heroes",
        file_name="characters.yaml",
        root_key="characters",
        source_kind="starter_characters",
        id_field="hero_id",
        extra_fields=("role",),
    ),
    StarterSeedSpec(
        ontology_section="abilities",
        file_name="abilities.yaml",
        root_key="abilities",
        source_kind="starter_abilities",
        id_field="ability_id",
        extra_fields=("class", "character_id"),
    ),
    StarterSeedSpec(
        ontology_section="events",
        file_name="action_moments.yaml",
        root_key="moments",
        source_kind="starter_action_moments",
        id_field="event_id",
        extra_fields=("category",),
    ),
)


_MARVEL_RIVALS_ADAPTER = GameOnboardingAdapter(
    game_id="marvel_rivals",
    supported_source_roles=("overview", "roster", "abilities", "events", "medals", "assets_reference"),
    role_to_kind={
        "roster": "character_or_operator",
        "abilities": "ability_or_equipment",
        "events": "event_badge_or_medal",
        "medals": "event_badge_or_medal",
    },
    role_asset_families={
        "roster": "hero_portrait",
        "abilities": "ability_icon",
        "events": "medal_icon",
        "medals": "medal_icon",
    },
    starter_seed_specs=_DEFAULT_SEED_SPECS,
)


_CALL_OF_DUTY_ADAPTER = GameOnboardingAdapter(
    game_id="call_of_duty",
    supported_source_roles=("overview", "operators", "equipment", "events", "assets_reference"),
    role_to_kind={
        "operators": "character_or_operator",
        "equipment": "ability_or_equipment",
        "events": "event_badge_or_medal",
    },
    role_asset_families={
        "operators": "hero_portrait",
        "equipment": "equipment_icon",
        "events": "medal_icon",
    },
    disabled_families=("ability_icon",),
    starter_seed_specs=_DEFAULT_SEED_SPECS,
)


_VALORANT_ADAPTER = GameOnboardingAdapter(
    game_id="valorant",
    supported_source_roles=("overview", "agents", "gear", "ranks", "assets_reference"),
    role_to_kind={
        "agents": "character_or_operator",
        "gear": "ability_or_equipment",
        "ranks": "event_badge_or_medal",
    },
    role_asset_families={
        "agents": "hero_portrait",
        "gear": "equipment_icon",
        "ranks": "medal_icon",
    },
    disabled_families=("ability_icon", "medal_icon"),
    starter_seed_specs=_DEFAULT_SEED_SPECS,
)


def get_onboarding_adapter(game: str) -> GameOnboardingAdapter:
    normalized = str(game).strip()
    if normalized == "marvel_rivals":
        return _MARVEL_RIVALS_ADAPTER
    if normalized == "call_of_duty":
        return _CALL_OF_DUTY_ADAPTER
    if normalized == "valorant":
        return _VALORANT_ADAPTER
    return GameOnboardingAdapter(
        game_id=normalized,
        supported_source_roles=_MARVEL_RIVALS_ADAPTER.supported_source_roles,
        role_to_kind=dict(_MARVEL_RIVALS_ADAPTER.role_to_kind),
        role_asset_families=dict(_MARVEL_RIVALS_ADAPTER.role_asset_families),
        starter_seed_specs=_DEFAULT_SEED_SPECS,
    )
