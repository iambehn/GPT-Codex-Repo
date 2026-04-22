"""
Metadata Injector — embeds title, weapon, and description into MP4 file tags

Uses FFmpeg with -c copy (stream copy, no re-encode) to write the clip's
generated title and hashtags into the MP4 container metadata. Platforms
like YouTube can read these tags directly; they also make clips self-describing
for any downstream tooling.

Tags written:
    title       — the title_engine-generated title
    comment     — space-separated hashtag string
    description — title + newline + hashtags (for platforms that read this tag)
    artist      — detected weapon display name (doubles as a searchable label)

The output is written to a new file (<stem>.tagged.mp4) to preserve the
original processed clip. If FFmpeg is not available or the title is missing,
the function returns without error.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)


def inject_metadata(processed_path: Path, clip_path: Path, config: dict) -> Path | None:
    """Embed title and hashtag metadata into the processed MP4.

    Reads title_engine results from the source clip's .meta.json. If no
    title has been generated yet, returns None without modifying any file.

    Args:
        processed_path: Path to the FFmpeg-rendered output clip.
        clip_path:      Path to the original inbox clip whose meta.json
                        holds the title_engine results.
        config:         Full parsed config.yaml dict.

    Returns:
        Path to the tagged output file, or None if injection was skipped.
    """
    if not config.get("title_engine", {}).get("enabled", False):
        return None

    meta_path = clip_path.with_suffix(".meta.json")
    if not meta_path.exists():
        return None

    try:
        clip_meta = json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError):
        return None

    te = clip_meta.get("title_engine", {})
    title = te.get("title")
    if not title:
        logger.debug(f"[metadata_injector] No title in meta — skipping {processed_path.name}")
        return None

    hashtags = te.get("hashtags", [])
    hashtag_str = " ".join(hashtags)
    description = f"{title}\n\n{hashtag_str}".strip()

    wd = clip_meta.get("weapon_detection", {})
    artist = wd.get("display_name") or ""

    tagged_path = processed_path.with_name(processed_path.stem + ".tagged.mp4")

    cmd = [
        "ffmpeg",
        "-y",
        "-i", str(processed_path),
        "-c", "copy",
        "-metadata", f"title={title}",
        "-metadata", f"comment={hashtag_str}",
        "-metadata", f"description={description}",
    ]
    if artist:
        cmd += ["-metadata", f"artist={artist}"]
    cmd.append(str(tagged_path))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            logger.warning(
                f"[metadata_injector] FFmpeg returned {result.returncode} "
                f"for {processed_path.name}: {result.stderr[-200:]}"
            )
            return None
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        logger.warning(f"[metadata_injector] Failed to inject metadata: {e}")
        return None

    logger.info(
        f"[metadata_injector] Tagged: {tagged_path.name} "
        f"| title='{title}' | {len(hashtags)} hashtag(s)"
    )
    return tagged_path
