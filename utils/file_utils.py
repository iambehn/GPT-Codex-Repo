"""
File system helpers shared across pipeline stages.

Handles directory creation, clip movement between pipeline folders,
and deriving the game key from a clip's path.
"""

import json
import shutil
from pathlib import Path

from pipeline.game_pack import list_supported_games
from utils.logger import get_logger

logger = get_logger(__name__)

def ensure_dirs(config: dict) -> None:
    """Create all pipeline folders and per-game subfolders if they don't exist.

    Should be called once at startup before any stage runs.

    Args:
        config: Full parsed config.yaml dict.
    """
    stage_folders = [
        config["paths"]["inbox"],
        config["paths"]["quarantine"],
        config["paths"]["processing"],
        config["paths"]["accepted"],
        config["paths"]["rejected"],
    ]
    known_games = list_supported_games(config) or list((config.get("games") or {}).keys())
    for folder in stage_folders:
        for game in known_games:
            Path(folder, game).mkdir(parents=True, exist_ok=True)

    Path(config["paths"]["assets"], "music").mkdir(parents=True, exist_ok=True)
    Path(config["paths"]["logs"]).mkdir(parents=True, exist_ok=True)

    logger.debug("Pipeline directories verified.")


def get_game_from_path(clip_path: str | Path) -> str | None:
    """Derive the game key from a clip's file path.

    Looks for a known game folder name in the path components.

    Args:
        clip_path: Path to the clip file.

    Returns:
        Game key string (e.g. 'arc_raiders') or None if not found.
    """
    parts = Path(clip_path).parts
    stage_dirs = {"inbox", "quarantine", "processing", "accepted", "rejected"}
    for idx, part in enumerate(parts[:-1]):
        if part in stage_dirs and idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def move_to_quarantine(
    clip_path: str | Path,
    game: str,
    config: dict,
    reason: str | None = None,
    move_sidecar: bool = True,
) -> Path:
    """Move a clip to the quarantine folder for the given game.

    Args:
        clip_path: Current path to the clip file.
        game: Game key (e.g. 'arc_raiders').
        config: Full parsed config.yaml dict.

    Returns:
        New path of the clip in quarantine/{game}/ or quarantine/{game}/{reason}/.
    """
    src = Path(clip_path)
    dest_dir = Path(config["paths"]["quarantine"], game)
    if reason:
        dest_dir = dest_dir / reason
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.move(str(src), str(dest))

    meta_src = src.with_suffix(".meta.json")
    if move_sidecar and meta_src.exists():
        meta_dest = dest.with_suffix(".meta.json")
        shutil.move(str(meta_src), str(meta_dest))
        try:
            meta = json.loads(meta_dest.read_text())
            meta["clip_path"] = str(dest)
            meta["meta_path"] = str(meta_dest)
            if reason:
                meta["quarantine_reason"] = reason
            meta_dest.write_text(json.dumps(meta, indent=2))
        except Exception as e:
            logger.warning(f"Could not update quarantined sidecar {meta_dest.name}: {e}")

    logger.info(f"Quarantined: {src.name} → {dest}")
    return dest


def move_clip(clip_path: str | Path, destination_stage: str, game: str, config: dict) -> Path:
    """Move a clip between pipeline stage folders.

    Args:
        clip_path: Current path to the clip.
        destination_stage: Config path key for the target folder
                           (e.g. 'processing', 'accepted', 'rejected').
        game: Game key (e.g. 'arc_raiders').
        config: Full parsed config.yaml dict.

    Returns:
        New path of the clip in the destination folder.
    """
    src = Path(clip_path)
    dest_dir = Path(config["paths"][destination_stage], game)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.move(str(src), str(dest))
    logger.info(f"Moved {src.name}: → {destination_stage}/{game}/")
    return dest
