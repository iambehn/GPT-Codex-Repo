"""
Stage 1 — Ingestion

Downloads clips from the Twitch game clips page for a given game using yt-dlp.
Each downloaded clip is probed with FFprobe to extract duration, resolution, fps,
and audio presence. Clips are then assigned a quality tag (high / medium / low)
or quarantined if they fall outside the configured thresholds.

Good clips land in inbox/{game}/ with a sidecar .meta.json file.
Bad clips are moved to quarantine/{game}/ and excluded from the returned list.

Sidecar .meta.json fields:
    clip_id, game, clip_path, meta_path, quality_tag,
    duration_seconds, resolution_width, resolution_height,
    fps, has_audio, downloaded_at
"""

import json
import subprocess
from datetime import datetime
from pathlib import Path

import yt_dlp

from utils.file_utils import move_to_quarantine
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _probe_clip(clip_path: Path) -> dict | None:
    """Run FFprobe on a clip and return parsed media info.

    Args:
        clip_path: Path to the video file.

    Returns:
        Dict with keys: duration_seconds, resolution_width, resolution_height,
        fps, has_audio. Returns None if FFprobe fails or no video stream found.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                "-show_format",
                str(clip_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        data = json.loads(result.stdout)
    except (subprocess.SubprocessError, json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"FFprobe failed for {clip_path.name}: {e}")
        return None

    streams = data.get("streams", [])
    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    if not video_stream:
        logger.warning(f"No video stream found in {clip_path.name}")
        return None

    # Duration: prefer format-level, fall back to stream-level
    fmt = data.get("format", {})
    duration = float(fmt.get("duration") or video_stream.get("duration", 0))

    # FPS: stored as a fraction string e.g. "60000/1001"
    fps_raw = video_stream.get("r_frame_rate", "30/1")
    try:
        num, den = fps_raw.split("/")
        fps = round(int(num) / int(den), 3)
    except (ValueError, ZeroDivisionError):
        fps = 30.0

    has_audio = any(s.get("codec_type") == "audio" for s in streams)

    return {
        "duration_seconds": round(duration, 3),
        "resolution_width": int(video_stream.get("width", 0)),
        "resolution_height": int(video_stream.get("height", 0)),
        "fps": fps,
        "has_audio": has_audio,
    }


def _assign_quality_tag(probe: dict, thresholds: dict) -> str | None:
    """Assign a quality tag or flag a clip for quarantine.

    Quarantine conditions (returns None):
      - duration < min_duration_seconds
      - duration > max_duration_seconds
      - resolution_height < min_resolution_height

    Quality tag (for clips that pass quarantine):
      - high:   resolution_height >= 1080
      - medium: resolution_height >= 720
      - low:    resolution_height >= 480

    Args:
        probe: Dict returned by _probe_clip.
        thresholds: config['ingestion']['quality_thresholds'] dict.

    Returns:
        'high', 'medium', 'low', or None (quarantine).
    """
    h = probe["resolution_height"]
    dur = probe["duration_seconds"]

    if dur < thresholds["min_duration_seconds"]:
        return None
    if dur > thresholds["max_duration_seconds"]:
        return None
    if h < thresholds["min_resolution_height"]:
        return None

    if h >= 1080:
        return "high"
    if h >= 720:
        return "medium"
    return "low"


def _write_meta(clip_path: Path, game: str, probe: dict, quality_tag: str) -> Path:
    """Write the sidecar .meta.json for a clip and return its path."""
    meta = {
        "clip_id": clip_path.stem,
        "game": game,
        "clip_path": str(clip_path),
        "meta_path": str(clip_path.with_suffix(".meta.json")),
        "quality_tag": quality_tag,
        "duration_seconds": probe["duration_seconds"],
        "resolution_width": probe["resolution_width"],
        "resolution_height": probe["resolution_height"],
        "fps": probe["fps"],
        "has_audio": probe["has_audio"],
        "downloaded_at": datetime.now().isoformat(timespec="seconds"),
    }
    meta_path = clip_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta_path


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_ingestion(game: str, config: dict) -> list[dict]:
    """Download and classify clips for a single game.

    Downloads up to config['ingestion']['max_clips_per_run'] clips from the
    game's Twitch clips page, probes each with FFprobe, assigns quality tags,
    quarantines bad clips, and writes a sidecar .meta.json per good clip.

    Args:
        game: Game key matching a key in config['games'] (e.g. 'arc_raiders').
        config: Full parsed config.yaml dict.

    Returns:
        List of clip manifest dicts for all successfully ingested clips.
        Each dict mirrors the fields written to the sidecar .meta.json.
    """
    game_cfg = config["games"][game]
    twitch_url = game_cfg["twitch_url"]
    output_dir = Path(config["paths"]["inbox"]) / game
    output_dir.mkdir(parents=True, exist_ok=True)

    thresholds = config["ingestion"]["quality_thresholds"]
    max_clips = config["ingestion"]["max_clips_per_run"]

    logger.info(f"[{game}] Downloading up to {max_clips} clips from {twitch_url}")

    ydl_opts = {
        "outtmpl": str(output_dir / "%(title)s_%(id)s.%(ext)s"),
        "format": "best[ext=mp4]/best",
        "max_downloads": max_clips,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "ignoreerrors": True,
    }

    downloaded_paths: list[Path] = []

    class _PathCollector:
        """yt-dlp post-processor hook that records downloaded file paths."""
        def __init__(self):
            self.paths = []

        def __call__(self, info: dict) -> None:
            path = info.get("filepath") or info.get("filename")
            if path:
                self.paths.append(Path(path))

    collector = _PathCollector()
    ydl_opts["progress_hooks"] = [
        lambda d: collector(d) if d.get("status") == "finished" else None
    ]

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([twitch_url])
        downloaded_paths = collector.paths
    except yt_dlp.utils.MaxDownloadsReached:
        downloaded_paths = collector.paths
    except Exception as e:
        logger.error(f"[{game}] yt-dlp download error: {e}")
        return []

    logger.info(f"[{game}] Downloaded {len(downloaded_paths)} file(s). Classifying...")

    manifests: list[dict] = []

    for clip_path in downloaded_paths:
        if not clip_path.exists():
            logger.warning(f"File missing after download: {clip_path}")
            continue

        # Skip if already processed in a previous run
        meta_path = clip_path.with_suffix(".meta.json")
        if meta_path.exists():
            logger.debug(f"Skipping already-processed clip: {clip_path.name}")
            manifests.append(json.loads(meta_path.read_text()))
            continue

        probe = _probe_clip(clip_path)
        if probe is None:
            logger.warning(f"[{game}] Quarantining (probe failed): {clip_path.name}")
            move_to_quarantine(clip_path, game, config)
            continue

        quality_tag = _assign_quality_tag(probe, thresholds)
        if quality_tag is None:
            reason = (
                f"duration={probe['duration_seconds']}s, "
                f"height={probe['resolution_height']}px"
            )
            logger.warning(f"[{game}] Quarantining (thresholds): {clip_path.name} ({reason})")
            move_to_quarantine(clip_path, game, config)
            continue

        _write_meta(clip_path, game, probe, quality_tag)
        manifest = json.loads(clip_path.with_suffix(".meta.json").read_text())
        manifests.append(manifest)
        logger.info(
            f"[{game}] Ingested [{quality_tag}] {clip_path.name} "
            f"({probe['duration_seconds']}s, {probe['resolution_height']}p)"
        )

    logger.info(f"[{game}] Ingestion complete: {len(manifests)} clip(s) ready.")
    return manifests
