from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.simple_yaml import load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
STARTER_ASSETS_ROOT = REPO_ROOT / "starter_assets"
ASSETS_ROOT = REPO_ROOT / "assets" / "games"
STARTER_REQUIRED_FILES = (
    "game.yaml",
    "characters.yaml",
    "abilities.yaml",
    "action_moments.yaml",
    "roi_profiles.yaml",
    "labels.yaml",
    "score_weights.yaml",
)
PUBLISHED_REQUIRED_FILES = (
    "game.yaml",
    "entities.yaml",
    "hud.yaml",
    "weights.yaml",
    "manifests/assets_manifest.json",
    "manifests/cv_templates.yaml",
    "manifests/detection_manifest.yaml",
    "manifests/runtime_cv_rules.yaml",
    "manifests/fusion_rules.yaml",
)


@dataclass
class GamePack:
    game_id: str
    root: Path
    source: str
    pack_format: str
    files: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        if self.pack_format == "published":
            entities = self.files.get("entities.yaml", {})
            templates = self.files.get("manifests/cv_templates.yaml", {}).get("templates", [])
            detection_manifest = self.files.get("manifests/detection_manifest.yaml", {})
            runtime_rules = self.files.get("manifests/runtime_cv_rules.yaml", {}).get("event_mappings", {})
            fusion_rules = self.files.get("manifests/fusion_rules.yaml", {}).get("rules", [])
            return {
                "game_id": self.game_id,
                "source": self.source,
                "root": str(self.root),
                "pack_format": self.pack_format,
                "character_count": len(entities.get("heroes", [])),
                "ability_count": len(entities.get("abilities", [])),
                "event_count": len(entities.get("events", [])),
                "template_count": len(templates),
                "detection_row_count": int(detection_manifest.get("row_count", len(detection_manifest.get("rows", [])) if isinstance(detection_manifest, dict) else 0) or 0),
                "runtime_rule_count": len(runtime_rules) if isinstance(runtime_rules, dict) else 0,
                "fusion_rule_count": len(fusion_rules) if isinstance(fusion_rules, list) else 0,
                "required_files": list(PUBLISHED_REQUIRED_FILES),
            }

        characters = self.files.get("characters.yaml", {}).get("characters", [])
        abilities = self.files.get("abilities.yaml", {}).get("abilities", [])
        moments = self.files.get("action_moments.yaml", {}).get("moments", [])
        return {
            "game_id": self.game_id,
            "source": self.source,
            "root": str(self.root),
            "pack_format": self.pack_format,
            "character_count": len(characters),
            "ability_count": len(abilities),
            "moment_count": len(moments),
            "required_files": list(STARTER_REQUIRED_FILES),
        }


def list_games() -> list[str]:
    names: set[str] = set()
    for root in (STARTER_ASSETS_ROOT, ASSETS_ROOT):
        if not root.exists():
            continue
        for child in root.iterdir():
            if child.is_dir():
                names.add(child.name)
    return sorted(names)


def game_root(game_id: str) -> tuple[Path, str]:
    repo_root = ASSETS_ROOT / game_id
    if repo_root.exists():
        return repo_root, "assets"
    starter_root = STARTER_ASSETS_ROOT / game_id
    if starter_root.exists():
        return starter_root, "starter_assets"
    raise FileNotFoundError(f"Unknown game pack: {game_id}")


def load_game_pack(game_id: str) -> GamePack:
    root, source = game_root(game_id)
    pack_format = _detect_pack_format(root)
    required_files = PUBLISHED_REQUIRED_FILES if pack_format == "published" else STARTER_REQUIRED_FILES
    files: dict[str, Any] = {}
    missing: list[str] = []
    for filename in required_files:
        path = root / filename
        if not path.exists():
            missing.append(filename)
            continue
        files[filename] = _load_pack_file(path)
    if missing:
        raise FileNotFoundError(f"Game pack {game_id} is missing required files: {missing}")
    return GamePack(game_id=game_id, root=root, source=source, pack_format=pack_format, files=files)


def validate_game_pack(game_id: str) -> dict[str, Any]:
    root, source = game_root(game_id)
    pack_format = _detect_pack_format(root)
    required_files = PUBLISHED_REQUIRED_FILES if pack_format == "published" else STARTER_REQUIRED_FILES
    existing = sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())
    missing = [filename for filename in required_files if not (root / filename).exists()]
    result: dict[str, Any] = {
        "ok": not missing,
        "game": game_id,
        "source": source,
        "root": str(root),
        "pack_format": pack_format,
        "existing_files": existing,
        "missing_files": missing,
    }
    if not missing:
        result["summary"] = load_game_pack(game_id).summary()
    return result


def init_game_pack(game_id: str, overwrite: bool = False) -> dict[str, Any]:
    starter_root = STARTER_ASSETS_ROOT / game_id
    if not starter_root.exists():
        raise FileNotFoundError(f"No starter asset pack exists for {game_id}")
    target_root = ASSETS_ROOT / game_id
    ASSETS_ROOT.mkdir(parents=True, exist_ok=True)
    if target_root.exists():
        if not overwrite:
            return {
                "ok": True,
                "game": game_id,
                "target_root": str(target_root),
                "copied": False,
                "message": "Game pack already exists in assets/games.",
            }
        shutil.rmtree(target_root)
    shutil.copytree(starter_root, target_root)
    return {
        "ok": True,
        "game": game_id,
        "target_root": str(target_root),
        "copied": True,
        "message": "Starter game pack copied into assets/games.",
    }


def _detect_pack_format(root: Path) -> str:
    published_markers = (
        "entities.yaml",
        "hud.yaml",
        "weights.yaml",
        "manifests/assets_manifest.json",
        "manifests/cv_templates.yaml",
    )
    if any((root / filename).exists() for filename in published_markers):
        return "published"
    return "starter"


def _load_pack_file(path: Path) -> Any:
    if path.suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    return load_yaml_file(path)
