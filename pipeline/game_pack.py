from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

from utils.logger import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_REQUIRED_FILES = ("game.yaml", "entities.yaml", "moments.yaml", "hud.yaml", "weights.yaml")
_QUARANTINE_REASONS = [
    "missing_context",
    "hook_not_resolved",
    "low_confidence",
    "ui_drift",
    "needs_roi_template",
]


def get_game_pack_root(config: dict) -> Path:
    """Return the root folder that stores all game packs."""
    base_assets = Path(config.get("paths", {}).get("assets", "assets"))
    return (PROJECT_ROOT / base_assets / "games").resolve()


def get_game_pack_dir(game: str, config: dict) -> Path:
    """Return the folder for a single game pack."""
    return get_game_pack_root(config) / game


def list_supported_games(config: dict) -> list[str]:
    """Return the union of config-defined games and game-pack folders."""
    configured = set((config.get("games") or {}).keys())
    pack_root = get_game_pack_root(config)
    if pack_root.exists():
        configured.update(p.name for p in pack_root.iterdir() if p.is_dir())
    return sorted(configured)


def game_pack_exists(game: str, config: dict) -> bool:
    pack_dir = get_game_pack_dir(game, config)
    return pack_dir.exists() and all((pack_dir / name).exists() for name in _REQUIRED_FILES)


def load_game_pack(game: str, config: dict, create_missing: bool = False) -> dict[str, Any]:
    """Load a game pack from disk, optionally scaffolding it first."""
    if create_missing and not game_pack_exists(game, config):
        scaffold_game_pack(game, config, force=False)

    pack_dir = get_game_pack_dir(game, config)
    pack = {
        "pack_root": str(pack_dir),
        "exists": pack_dir.exists(),
        "game": _load_yaml(pack_dir / "game.yaml"),
        "entities": _load_yaml(pack_dir / "entities.yaml"),
        "moments": _load_yaml(pack_dir / "moments.yaml"),
        "hud": _load_yaml(pack_dir / "hud.yaml"),
        "weights": _load_yaml(pack_dir / "weights.yaml"),
    }
    return pack


def get_game_metadata(game: str, config: dict, game_pack: dict | None = None) -> dict[str, Any]:
    """Merge legacy config metadata with game-pack metadata."""
    legacy = dict((config.get("games") or {}).get(game, {}))
    if game_pack is None:
        game_pack = load_game_pack(game, config)
    pack_game = dict(game_pack.get("game") or {})

    display_name = (
        pack_game.get("display_name")
        or legacy.get("display_name")
        or game.replace("_", " ").title()
    )

    return {
        "game_id": pack_game.get("game_id", game),
        "display_name": display_name,
        "folder": pack_game.get("folder") or legacy.get("folder") or game,
        "twitch_url": pack_game.get("twitch_url") or legacy.get("twitch_url"),
        "genre": pack_game.get("genre", "fps"),
        "ui_version": pack_game.get("ui_version", "legacy-2026-04"),
        "detectors": pack_game.get("detectors", {}),
    }


def get_primary_entities(game_pack: dict) -> tuple[str, dict[str, dict]]:
    """Return the canonical primary entity map for the game pack."""
    entities = game_pack.get("entities") or {}
    kind = entities.get("primary_kind")
    if kind in ("heroes", "weapons", "characters"):
        items = entities.get(kind, {})
        if items:
            return kind, items

    for candidate in ("heroes", "weapons", "characters"):
        items = entities.get(candidate, {})
        if items:
            return candidate, items

    return "entities", {}


def get_kill_feed_game_config(game: str, config: dict, game_pack: dict | None = None) -> dict:
    """Return per-game kill-feed settings, preferring the game pack."""
    if game_pack is None:
        game_pack = load_game_pack(game, config)

    hud = game_pack.get("hud") or {}
    detector = (hud.get("detectors") or {}).get("kill_feed") or {}
    rois = hud.get("rois") or {}
    if detector:
        merged = dict(detector)
        roi_ref = detector.get("roi_ref", "kill_feed")
        if roi_ref in rois:
            merged["roi"] = rois[roi_ref]
        elif "kill_feed" in rois:
            merged["roi"] = rois["kill_feed"]
        return merged

    return dict((config.get("kill_feed") or {}).get("games", {}).get(game, {}))


def get_weapon_detector_game_config(game: str, config: dict, game_pack: dict | None = None) -> dict:
    """Return per-game weapon/entity detector settings, preferring the game pack."""
    if game_pack is None:
        game_pack = load_game_pack(game, config)

    hud = game_pack.get("hud") or {}
    detector = (hud.get("detectors") or {}).get("weapon_detector") or {}
    rois = hud.get("rois") or {}
    if detector:
        merged = dict(detector)
        roi_ref = detector.get("roi_ref", "weapon_detector")
        if roi_ref in rois:
            merged["roi"] = rois[roi_ref]
        elif "weapon_detector" in rois:
            merged["roi"] = rois["weapon_detector"]

        primary_kind, items = get_primary_entities(game_pack)
        merged["weapons"] = {
            entity_id: data.get("display_name", entity_id.replace("_", " ").title())
            for entity_id, data in items.items()
        }
        merged["entities_kind"] = detector.get("entities_kind", primary_kind)
        return merged

    return dict((config.get("weapon_detector") or {}).get("games", {}).get(game, {}))


def get_yolo_detector_game_config(game: str, config: dict, game_pack: dict | None = None) -> dict:
    """Return per-game YOLO detector settings, preferring the game pack."""
    if game_pack is None:
        game_pack = load_game_pack(game, config)

    hud = game_pack.get("hud") or {}
    detector = (hud.get("detectors") or {}).get("yolo") or {}
    merged = dict(config.get("yolo_detector") or {})
    merged.update(detector)
    merged["enabled"] = bool((config.get("yolo_detector") or {}).get("enabled", False)) and bool(detector.get("enabled", False))
    return merged


def get_yolo_model_dir(game: str, config: dict, game_pack: dict | None = None) -> Path:
    """Return the per-game YOLO model registry directory."""
    if game_pack is None:
        game_pack = load_game_pack(game, config)

    pack_root = Path(game_pack.get("pack_root", "."))
    yolo_cfg = get_yolo_detector_game_config(game, config, game_pack)
    raw_weights = yolo_cfg.get("weights_path") or f"models/yolo/{game}/weights/best.pt"
    resolved_weights = resolve_asset_path(str(raw_weights), pack_root)
    if resolved_weights.parent.name == "weights":
        return resolved_weights.parent.parent
    return resolved_weights.parent



def validate_game_pack(game: str, config: dict) -> dict[str, Any]:
    """Validate that a game pack has the minimum required files and references."""
    pack_dir = get_game_pack_dir(game, config)
    pack = load_game_pack(game, config)
    errors: list[str] = []
    warnings: list[str] = []

    if not pack_dir.exists():
        return {"valid": False, "errors": [f"missing pack directory: {pack_dir}"], "warnings": []}

    for required in _REQUIRED_FILES:
        if not (pack_dir / required).exists():
            errors.append(f"missing required file: {required}")

    game_meta = pack.get("game") or {}
    if game_meta.get("game_id") and game_meta.get("game_id") != game:
        errors.append(f"game.yaml game_id mismatch: expected '{game}', found '{game_meta.get('game_id')}'")

    entities = pack.get("entities") or {}
    primary_kind, primary_items = get_primary_entities(pack)
    if not primary_items:
        errors.append("entities.yaml must define at least one primary entity set")
    elif primary_kind not in ("heroes", "weapons", "characters"):
        warnings.append(f"unusual primary entity kind: {primary_kind}")

    hud = pack.get("hud") or {}
    rois = hud.get("rois") or {}
    for roi_name, roi in rois.items():
        for field in ("x", "y", "w", "h"):
            if field not in roi:
                errors.append(f"ROI '{roi_name}' is missing '{field}'")

    detectors = hud.get("detectors") or {}
    moment_ids = {item.get("id") for item in ((pack.get("moments") or {}).get("moments") or [])}
    for detector_name, detector in detectors.items():
        roi_ref = detector.get("roi_ref")
        if roi_ref and roi_ref not in rois:
            errors.append(f"{detector_name} references unknown roi_ref '{roi_ref}'")

        for key in ("icon_dir", "template_dir"):
            raw_path = detector.get(key)
            if raw_path:
                resolved = resolve_asset_path(raw_path, pack_dir)
                if not resolved.exists():
                    warnings.append(f"{detector_name} {key} not found yet: {resolved}")

        if detector_name == "yolo":
            _validate_yolo_labels(detector, primary_items, moment_ids, errors)
            if detector.get("enabled") and detector.get("weights_path"):
                resolved_weights = resolve_asset_path(detector.get("weights_path"), pack_dir)
                if not resolved_weights.exists():
                    warnings.append(f"yolo weights_path not found yet: {resolved_weights}")
            if detector.get("labels"):
                yolo_dir = get_yolo_model_dir(game, config, pack)
                dataset_yaml = yolo_dir / "dataset.yaml"
                labels_txt = yolo_dir / "labels.txt"
                if not dataset_yaml.exists():
                    warnings.append(f"yolo dataset.yaml not found yet: {dataset_yaml}")
                if not labels_txt.exists():
                    warnings.append(f"yolo labels.txt not found yet: {labels_txt}")

    weights = pack.get("weights") or {}
    judge_cfg = weights.get("clip_judge") or {}
    for threshold_key in ("accept", "reject", "quarantine"):
        if threshold_key not in (judge_cfg.get("thresholds") or {}):
            errors.append(f"weights.yaml clip_judge.thresholds missing '{threshold_key}'")

    reasons = judge_cfg.get("quarantine_reasons") or []
    for reason in _QUARANTINE_REASONS:
        if reason not in reasons:
            warnings.append(f"weights.yaml missing quarantine reason '{reason}'")

    return {"valid": not errors, "errors": errors, "warnings": warnings}


def scaffold_game_pack(game: str, config: dict, force: bool = False) -> dict[str, Any]:
    """Create or refresh a draft game pack using legacy config and assets."""
    pack_dir = get_game_pack_dir(game, config)
    pack_dir.mkdir(parents=True, exist_ok=True)
    model_dir = PROJECT_ROOT / "models" / "yolo" / game
    (pack_dir / "examples" / "positive_clips").mkdir(parents=True, exist_ok=True)
    (pack_dir / "examples" / "negative_clips").mkdir(parents=True, exist_ok=True)
    (pack_dir / "examples" / "reference_frames").mkdir(parents=True, exist_ok=True)
    (pack_dir / "examples" / "gold_set" / "clips").mkdir(parents=True, exist_ok=True)
    (pack_dir / "examples" / "gold_set" / "sidecars").mkdir(parents=True, exist_ok=True)
    for subdir in (
        "images/train",
        "images/val",
        "labels/train",
        "labels/val",
        "weights",
        "seed_assets/icons",
        "seed_assets/roi_templates",
        "seed_assets/reference_frames",
    ):
        (model_dir / subdir).mkdir(parents=True, exist_ok=True)
    for name in (".gitkeep",):
        for subdir in (
            "positive_clips",
            "negative_clips",
            "reference_frames",
            "gold_set/clips",
            "gold_set/sidecars",
        ):
            target = pack_dir / "examples" / subdir / name
            if not target.exists():
                target.write_text("")
        for subdir in (
            "images/train",
            "images/val",
            "labels/train",
            "labels/val",
            "weights",
            "seed_assets/icons",
            "seed_assets/roi_templates",
            "seed_assets/reference_frames",
        ):
            target = model_dir / subdir / name
            if not target.exists():
                target.write_text("")

    legacy_game = dict((config.get("games") or {}).get(game, {}))
    display_name = legacy_game.get("display_name") or game.replace("_", " ").title()
    primary_kind, entities = _legacy_entities(game, config)

    game_yaml = {
        "game_id": game,
        "display_name": display_name,
        "folder": legacy_game.get("folder", game),
        "twitch_url": legacy_game.get("twitch_url"),
        "genre": _infer_genre(game, display_name),
        "ui_version": "legacy-2026-04",
        "detectors": {
            "audio_detector": {"enabled": bool((config.get("audio_detector") or {}).get("enabled", False))},
            "kill_feed": {"enabled": bool((config.get("kill_feed") or {}).get("enabled", False))},
            "weapon_detector": {"enabled": bool((config.get("weapon_detector") or {}).get("enabled", False))},
            "clip_judge_ai": {"enabled": True, "provider": "anthropic"},
            "niceshot": {
                "enabled": False,
                "provider": "niceshot_ai",
                "profile": "hero_shooter_default",
                "profile_overrides": {
                    "score_multipliers": {},
                    "moment_boosts": {},
                    "hook_kinds": [],
                    "kind_aliases": {},
                },
            },
        },
    }

    entities_yaml = {
        "primary_kind": primary_kind,
        primary_kind: entities,
        "abilities": {},
        "aliases": {},
    }

    hud_yaml = {
        "ui_version": "legacy-2026-04",
        "rois": _legacy_rois(game, config),
        "detectors": _legacy_detector_hud(game, config, primary_kind),
        "roi_templates": [],
        "ui_drift": {"expected_hashes": []},
    }

    moments_yaml = {
        "moments": _default_moments(game),
        "hook_targets": {
            "hard_gate": True,
            "window_seconds": 1.5,
            "description": "The clip must contain a moment that can anchor the first 1.5 seconds of the final edit.",
        },
    }

    weights_yaml = {
        "clip_judge": {
            "ai_ratio": 0.6,
            "deterministic_ratio": 0.4,
            "thresholds": {"accept": 0.70, "quarantine": 0.45, "reject": 0.25},
            "hard_gates": {
                "hook_window_seconds": 1.5,
                "require_context_fields": ["player_entity", "detected_event"],
            },
            "hook_enforcer": {
                "window_seconds": 1.5,
                "acceptance_threshold": 0.50,
                "pre_event_padding_seconds": 0.50,
                "minimum_remaining_seconds": 6.0,
                "signal_weights": {
                    "kill_feed": 0.45,
                    "niceshot": 0.25,
                    "yolo": 0.20,
                    "audio": 0.10,
                },
            },
            "composite_weights": {
                "ai_clip_score": 0.35,
                "ai_hook_score": 0.25,
                "kill_feed_score": 0.20,
                "audio_score": 0.10,
                "context_score": 0.10,
            },
            "quarantine_reasons": _QUARANTINE_REASONS,
            "feedback": {
                "enabled": True,
                "signals": ["scroll_stop_rate", "retention", "rewatch_rate", "shares", "follows"],
            },
        }
    }

    files = {
        "game.yaml": game_yaml,
        "entities.yaml": entities_yaml,
        "moments.yaml": moments_yaml,
        "hud.yaml": hud_yaml,
        "weights.yaml": weights_yaml,
    }

    written: list[str] = []
    for filename, payload in files.items():
        path = pack_dir / filename
        if path.exists() and not force:
            continue
        _write_yaml(path, payload)
        written.append(filename)

    logger.info(f"[game_pack] Scaffolded {game}: {written or ['no file changes']}")
    return {"pack_dir": str(pack_dir), "written": written}


def resolve_asset_path(raw_path: str | None, pack_dir: Path | None = None) -> Path:
    """Resolve a repo-relative or absolute asset path."""
    if not raw_path:
        return PROJECT_ROOT
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    if pack_dir is not None:
        pack_relative = (pack_dir / raw_path).resolve()
        if pack_relative.exists():
            return pack_relative
    return (PROJECT_ROOT / raw_path).resolve()


def _legacy_entities(game: str, config: dict) -> tuple[str, dict[str, dict]]:
    roster_path = PROJECT_ROOT / "assets" / "rosters" / f"{game}.yaml"
    if roster_path.exists():
        data = yaml.safe_load(roster_path.read_text()) or {}
        if "heroes" in data:
            return "heroes", data.get("heroes", {})
        if "weapons" in data:
            return "weapons", data.get("weapons", {})

    legacy_weapon_cfg = ((config.get("weapon_detector") or {}).get("games") or {}).get(game, {})
    legacy_weapons = legacy_weapon_cfg.get("weapons", {})
    entities = {
        entity_id: {"display_name": display_name}
        for entity_id, display_name in legacy_weapons.items()
    }
    return "weapons", entities


def _legacy_rois(game: str, config: dict) -> dict[str, dict]:
    rois: dict[str, dict] = {}
    kill_cfg = ((config.get("kill_feed") or {}).get("games") or {}).get(game, {})
    if kill_cfg.get("roi"):
        rois["kill_feed"] = dict(kill_cfg["roi"])
    weapon_cfg = ((config.get("weapon_detector") or {}).get("games") or {}).get(game, {})
    if weapon_cfg.get("roi"):
        rois["weapon_detector"] = dict(weapon_cfg["roi"])
    return rois


def _legacy_detector_hud(game: str, config: dict, primary_kind: str) -> dict[str, dict]:
    detectors: dict[str, dict] = {}

    kill_cfg = ((config.get("kill_feed") or {}).get("games") or {}).get(game, {})
    if kill_cfg:
        detectors["kill_feed"] = {
            "roi_ref": "kill_feed",
            "template_dir": f"assets/kill_feed_templates/{game}",
            "kill_colors": kill_cfg.get("kill_colors", []),
            "headshot_colors": kill_cfg.get("headshot_colors", []),
            "pixel_spike_threshold": kill_cfg.get("pixel_spike_threshold", 50),
        }

    weapon_cfg = ((config.get("weapon_detector") or {}).get("games") or {}).get(game, {})
    if weapon_cfg:
        detectors["weapon_detector"] = {
            "roi_ref": "weapon_detector",
            "icon_dir": f"assets/weapon_icons/{game}",
            "entities_kind": primary_kind,
            "match_mode": (config.get("weapon_detector") or {}).get("match_mode", "color"),
        }

    detectors["yolo"] = {
        "enabled": False,
        "inference_mode": "video",
        "weights_path": f"models/yolo/{game}/weights/best.pt",
        "confidence_threshold": 0.60,
        "frame_sample": "middle",
        "max_samples": 24,
        "labels": {},
    }

    return detectors


def _validate_yolo_labels(
    detector: dict,
    primary_items: dict[str, dict],
    moment_ids: set[str | None],
    errors: list[str],
) -> None:
    labels = detector.get("labels") or {}
    if not isinstance(labels, dict):
        errors.append("yolo labels must be a mapping")
        return

    for raw_label, mapping in labels.items():
        target = None
        kind = None
        if isinstance(mapping, str):
            target = mapping
        elif isinstance(mapping, dict):
            target = mapping.get("maps_to") or mapping.get("entity_id") or mapping.get("event_id")
            kind = mapping.get("kind")
        else:
            errors.append(f"yolo label '{raw_label}' must map to a string or object")
            continue

        if not target:
            errors.append(f"yolo label '{raw_label}' is missing maps_to/entity_id/event_id")
            continue
        if kind == "entity" and target not in primary_items:
            errors.append(f"yolo label '{raw_label}' maps to unknown entity '{target}'")
        elif kind == "event" and target not in moment_ids:
            errors.append(f"yolo label '{raw_label}' maps to unknown moment '{target}'")
        elif not kind and target not in primary_items and target not in moment_ids:
            errors.append(f"yolo label '{raw_label}' maps to unknown target '{target}'")


def _default_moments(game: str) -> list[dict[str, Any]]:
    shared = [
        {
            "id": "multi_kill_swing",
            "labels": ["multi-kill", "swing", "burst"],
            "narrative": "A short burst of eliminations that changes the fight immediately.",
            "rarity_hint": "high",
        },
        {
            "id": "clutch_reversal",
            "labels": ["clutch", "outplay", "reversal"],
            "narrative": "A low-odds survival or comeback moment.",
            "rarity_hint": "high",
        },
        {
            "id": "precision_pick",
            "labels": ["headshot", "flick", "pick"],
            "narrative": "A fast precision elimination that reads instantly to viewers.",
            "rarity_hint": "medium",
        },
    ]

    extra_by_game = {
        "marvel_rivals": [
            {
                "id": "ultimate_swing",
                "labels": ["ultimate", "team wipe", "combo"],
                "narrative": "An ultimate or combo that visibly swings a team fight.",
                "rarity_hint": "high",
            }
        ],
        "deadlock": [
            {
                "id": "teamfight_pick",
                "labels": ["teamfight", "gank", "pick"],
                "narrative": "A pick or chain of eliminations that opens a lane or fight.",
                "rarity_hint": "medium",
            }
        ],
        "arc_raiders": [
            {
                "id": "extraction_clutch",
                "labels": ["extract", "squad wipe", "ambush"],
                "narrative": "A fight or escape that secures an extraction under pressure.",
                "rarity_hint": "high",
            }
        ],
    }

    return shared + extra_by_game.get(game, [])


def _infer_genre(game: str, display_name: str) -> str:
    text = f"{game} {display_name}".lower()
    if "rivals" in text or "deadlock" in text:
        return "hero_shooter"
    return "fps"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        logger.warning(f"[game_pack] Failed to parse {path}: {e}")
        return {}


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False))


def slugify_game_name(raw: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
    return slug or "new_game"


def print_validation_report(result: dict[str, Any]) -> str:
    return json.dumps(result, indent=2)
