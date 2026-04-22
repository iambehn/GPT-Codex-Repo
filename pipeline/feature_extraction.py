"""
Stage 3 — Feature Extraction

Reads a clip and its Whisper transcript and produces a structured feature dict
that the Decision Engine uses to select a template.

All audio and motion analysis is done via FFmpeg subprocess calls (no extra
dependencies beyond what is already required).

New fields added to .meta.json (all others carried forward from Ingestion):
    audio_peak_db      — loudest point in the clip (dBFS)
    audio_avg_db       — mean loudness (dBFS)
    silence_ratio      — fraction of clip duration that is silent (0.0–1.0)
    scene_change_count — number of detected scene cuts
    motion_level       — 'low' | 'medium' | 'high'  (from scene change rate)
    audio_energy       — 'low' | 'medium' | 'high'  (from mean loudness)
    keywords           — list of FPS keywords matched in the transcript
"""

import json
import re
import subprocess
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# FPS keyword lists
# Shared set covers common FPS moments; per-game sets add title-specific terms.
# All matching is case-insensitive against the full transcript text.
# ---------------------------------------------------------------------------
_FPS_KEYWORDS: dict[str, list[str]] = {
    "shared": [
        "ace", "kill", "clutch", "headshot", "one tap", "multi-kill",
        "1v5", "streak", "insane", "insane play",
    ],
    "arc_raiders": [
        "wipe", "squad wipe", "extraction", "clutch extract",
        "solo", "ambush", "no scope", "raid", "down",
    ],
    "marvel_rivals": [
        "ult", "team wipe", "clutch", "wombo combo", "flank",
        "dive", "penta", "dominate", "shutdown", "combo",
    ],
    "deadlock": [
        "gank", "teamfight", "wipe", "carry", "clutch",
        "comeback", "steal", "outplay", "lane", "ambush",
    ],
}

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _analyze_audio(clip_path: Path) -> dict:
    """Extract audio loudness and silence ratio using FFmpeg.

    Uses two passes:
      1. volumedetect filter  → mean and peak loudness (dBFS)
      2. silencedetect filter → total silence duration → silence_ratio

    Args:
        clip_path: Path to the video file.

    Returns:
        Dict with keys: audio_peak_db, audio_avg_db, silence_ratio.
        Returns safe defaults (0.0 dB, 0.0 ratio) on failure.
    """
    defaults = {"audio_peak_db": 0.0, "audio_avg_db": -99.0, "silence_ratio": 0.0}

    # --- Pass 1: volumedetect ---
    try:
        r = subprocess.run(
            ["ffmpeg", "-i", str(clip_path), "-af", "volumedetect",
             "-f", "null", "/dev/null"],
            capture_output=True, text=True, timeout=60,
        )
        output = r.stderr

        peak_match = re.search(r"max_volume:\s*([-\d.]+)\s*dB", output)
        mean_match = re.search(r"mean_volume:\s*([-\d.]+)\s*dB", output)

        audio_peak_db = float(peak_match.group(1)) if peak_match else 0.0
        audio_avg_db = float(mean_match.group(1)) if mean_match else -99.0
    except (subprocess.SubprocessError, ValueError) as e:
        logger.warning(f"volumedetect failed for {clip_path.name}: {e}")
        return defaults

    # --- Pass 2: silencedetect ---
    silence_ratio = 0.0
    try:
        r2 = subprocess.run(
            ["ffmpeg", "-i", str(clip_path),
             "-af", "silencedetect=noise=-40dB:d=0.5",
             "-f", "null", "/dev/null"],
            capture_output=True, text=True, timeout=60,
        )
        # Lines: "silence_duration: N.NNN"
        durations = re.findall(r"silence_duration:\s*([\d.]+)", r2.stderr)
        total_silence = sum(float(d) for d in durations)

        # Get clip duration from format tag in the same output
        dur_match = re.search(r"Duration:\s*([\d:]+\.[\d]+)", r2.stderr)
        if dur_match:
            parts = dur_match.group(1).split(":")
            clip_dur = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
            if clip_dur > 0:
                silence_ratio = min(round(total_silence / clip_dur, 4), 1.0)
    except (subprocess.SubprocessError, ValueError) as e:
        logger.warning(f"silencedetect failed for {clip_path.name}: {e}")

    return {
        "audio_peak_db": round(audio_peak_db, 2),
        "audio_avg_db": round(audio_avg_db, 2),
        "silence_ratio": silence_ratio,
    }


def _detect_motion(clip_path: Path, duration: float) -> dict:
    """Count scene changes using FFmpeg's select filter.

    Selects frames where the scene change score exceeds 0.3, then counts
    how many were selected. The rate (changes/sec) maps to a motion level.

    Thresholds:
        low:    < 0.5 scene changes/sec
        medium: 0.5 – 2.0 scene changes/sec
        high:   > 2.0 scene changes/sec

    Args:
        clip_path: Path to the video file.
        duration:  Clip duration in seconds (from meta).

    Returns:
        Dict with keys: scene_change_count (int), motion_level (str).
    """
    try:
        r = subprocess.run(
            [
                "ffmpeg", "-i", str(clip_path),
                "-vf", "select='gt(scene,0.3)',showinfo",
                "-vsync", "vfr", "-f", "null", "/dev/null",
            ],
            capture_output=True, text=True, timeout=120,
        )
        # showinfo emits one tagged line per selected frame
        scene_change_count = r.stderr.count("[Parsed_showinfo")
    except subprocess.SubprocessError as e:
        logger.warning(f"Scene detection failed for {clip_path.name}: {e}")
        return {"scene_change_count": 0, "motion_level": "low"}

    rate = scene_change_count / duration if duration > 0 else 0.0
    if rate < 0.5:
        motion_level = "low"
    elif rate < 2.0:
        motion_level = "medium"
    else:
        motion_level = "high"

    return {"scene_change_count": scene_change_count, "motion_level": motion_level}


def _classify_audio_energy(audio_avg_db: float) -> str:
    """Derive an audio energy label from mean loudness.

    Thresholds (dBFS):
        low:    mean < -30
        medium: -30 to -18
        high:   > -18

    Args:
        audio_avg_db: Mean loudness in dBFS from volumedetect.

    Returns:
        'low', 'medium', or 'high'.
    """
    if audio_avg_db < -30.0:
        return "low"
    if audio_avg_db < -18.0:
        return "medium"
    return "high"


def _extract_keywords(transcript_text: str, game: str) -> list[str]:
    """Find FPS keyword matches in a transcript.

    Checks the shared keyword set plus the game-specific set (if known).
    Matching is case-insensitive. Multi-word phrases are matched as substrings.

    Args:
        transcript_text: Full transcript string from Whisper.
        game: Game key (e.g. 'arc_raiders').

    Returns:
        Sorted list of matched keyword strings (no duplicates).
    """
    text_lower = transcript_text.lower()
    keyword_sets = [_FPS_KEYWORDS["shared"]]
    if game in _FPS_KEYWORDS:
        keyword_sets.append(_FPS_KEYWORDS[game])

    matched = set()
    for kw_list in keyword_sets:
        for kw in kw_list:
            if kw.lower() in text_lower:
                matched.add(kw)

    return sorted(matched)


def _update_meta(meta_path: Path, updates: dict) -> dict:
    """Merge updates into an existing .meta.json and return the full dict."""
    meta = json.loads(meta_path.read_text())
    meta.update(updates)
    meta_path.write_text(json.dumps(meta, indent=2))
    return meta


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_feature_extraction(clip_path: str, transcript: dict, config: dict) -> dict:
    """Extract structured metadata from a clip and its transcript.

    Reads the existing .meta.json (written by Ingestion), runs audio and motion
    analysis via FFmpeg, matches FPS keywords from the transcript, then writes
    the enriched feature dict back to .meta.json.

    Idempotent: if 'motion_level' is already present in .meta.json the
    extraction is skipped and the existing metadata is returned.

    Args:
        clip_path: Path to the video file (in inbox/{game}/).
        transcript: Whisper result dict (output of run_transcription).
        config: Full parsed config.yaml dict.

    Returns:
        Full feature dict (all fields from Ingestion + Transcription + this stage).
    """
    clip = Path(clip_path)
    meta_path = clip.with_suffix(".meta.json")

    if not meta_path.exists():
        logger.error(f"No .meta.json found for {clip.name} — was Ingestion run first?")
        return {}

    meta = json.loads(meta_path.read_text())

    # Idempotency: skip if already extracted
    if "motion_level" in meta:
        logger.debug(f"Skipping already-extracted clip: {clip.name}")
        return meta

    game = meta.get("game", "")
    duration = meta.get("duration_seconds", 0.0)

    logger.info(f"Extracting features: {clip.name}")

    audio_features = _analyze_audio(clip)
    motion_features = _detect_motion(clip, duration)
    audio_energy = _classify_audio_energy(audio_features["audio_avg_db"])
    keywords = _extract_keywords(transcript.get("text", ""), game)

    updates = {
        **audio_features,
        **motion_features,
        "audio_energy": audio_energy,
        "keywords": keywords,
    }

    full_meta = _update_meta(meta_path, updates)

    logger.info(
        f"Features [{clip.name}]: motion={motion_features['motion_level']} "
        f"({motion_features['scene_change_count']} cuts), "
        f"audio_energy={audio_energy} ({audio_features['audio_avg_db']} dBFS), "
        f"keywords={keywords}"
    )

    return full_meta
