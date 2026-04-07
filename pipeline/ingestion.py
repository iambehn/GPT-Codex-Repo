"""
Stage 1 — Ingestion

Downloads clips from the Twitch game clips page for a given game using yt-dlp.
Each downloaded clip is classified by duration, resolution, and audio levels,
then assigned a quality tag (low / medium / high). Bad or unprocessable clips
are moved to quarantine. Good clips land in inbox/{game}/ with a sidecar
metadata JSON file.
"""

from utils.logger import get_logger

logger = get_logger(__name__)


def run_ingestion(game: str, config: dict) -> list[dict]:
    """Download and classify clips for a single game.

    Args:
        game: Game key matching a key in config['games'] (e.g. 'arc_raiders').
        config: Full parsed config.yaml dict.

    Returns:
        List of clip manifest dicts, one per successfully ingested clip.
        Each dict contains at minimum: clip_path, game, quality_tag, duration_seconds,
        resolution_height, has_audio.
    """
    pass
