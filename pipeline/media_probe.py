from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse


def probe_media_duration(source: str | Path) -> float | None:
    source_text = str(source)
    parsed = urlparse(source_text)
    if parsed.scheme in {"http", "https"}:
        return None

    path = Path(source_text)
    if not path.exists() or path.suffix.lower() == ".m3u8":
        return None

    ffprobe = shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe"
    if not Path(ffprobe).exists():
        return None

    command = [
        ffprobe,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    try:
        result = subprocess.run(command, capture_output=True, check=False, timeout=15)
    except subprocess.TimeoutExpired:
        return None

    if result.returncode != 0:
        return None

    output = result.stdout.decode("utf-8", errors="ignore").strip()
    if not output:
        return None

    try:
        duration = float(output)
    except ValueError:
        return None
    if duration <= 0:
        return None
    return duration


def probe_has_video_stream(source: str | Path) -> bool | None:
    source_text = str(source)
    parsed = urlparse(source_text)
    if parsed.scheme in {"http", "https"}:
        return None

    path = Path(source_text)
    if not path.exists() or path.suffix.lower() == ".m3u8":
        return None

    ffprobe = shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe"
    if not Path(ffprobe).exists():
        return None

    command = [
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    try:
        result = subprocess.run(command, capture_output=True, check=False, timeout=15)
    except subprocess.TimeoutExpired:
        return None

    if result.returncode != 0:
        return None

    output = result.stdout.decode("utf-8", errors="ignore").strip()
    if not output:
        return False
    return "video" in output.splitlines()
