from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.simple_yaml import load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
STARTER_ASSETS_ROOT = REPO_ROOT / "starter_assets"
ASSETS_ROOT = REPO_ROOT / "assets" / "games"
REQUIRED_FILES = (
    "game.yaml",
    "characters.yaml",
    "abilities.yaml",
    "action_moments.yaml",
    "roi_profiles.yaml",
    "labels.yaml",
    "score_weights.yaml",
)


@dataclass
class GamePack:
    game_id: str
    root: Path
    source: str
    files: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        characters = self.files.get("characters.yaml", {}).get("characters", [])
        abilities = self.files.get("abilities.yaml", {}).get("abilities", [])
        moments = self.files.get("action_moments.yaml", {}).get("moments", [])
        return {
            "game_id": self.game_id,
            "source": self.source,
            "root": str(self.root),
            "character_count": len(characters),
            "ability_count": len(abilities),
            "moment_count": len(moments),
            "required_files": list(REQUIRED_FILES),
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
    files: dict[str, Any] = {}
    missing: list[str] = []
    for filename in REQUIRED_FILES:
        path = root / filename
        if not path.exists():
            missing.append(filename)
            continue
        files[filename] = load_yaml_file(path)
    if missing:
        raise FileNotFoundError(f"Game pack {game_id} is missing required files: {missing}")
    return GamePack(game_id=game_id, root=root, source=source, files=files)


def validate_game_pack(game_id: str) -> dict[str, Any]:
    root, source = game_root(game_id)
    existing = sorted(path.name for path in root.glob("*.yaml"))
    missing = [filename for filename in REQUIRED_FILES if not (root / filename).exists()]
    result: dict[str, Any] = {
        "ok": not missing,
        "game": game_id,
        "source": source,
        "root": str(root),
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

