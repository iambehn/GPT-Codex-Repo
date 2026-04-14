"""
Stage 2 — Transcription

Runs OpenAI Whisper on a clip's audio track to produce a segment-level transcript.
Two sidecar files are written alongside the clip:

  <stem>.whisper.json  — full Whisper result (text, segments with timestamps, language)
  <stem>.srt           — SRT subtitle file consumed by FFmpeg's subtitles filter
                         in the Processing stage for caption burn-in

The Whisper model is loaded once and cached in memory across multiple clips to avoid
paying the model-load cost on every call.

The clip's .meta.json is updated with transcript_path and srt_path fields.

Future upgrade: replace with WhisperX for word-level timestamps when word-highlight
captions are implemented.
"""

import json
from datetime import timedelta
from pathlib import Path

import whisper

from utils.logger import get_logger

logger = get_logger(__name__)

# Module-level model cache: model_name → loaded whisper.Whisper instance
_model_cache: dict = {}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _load_model(model_name: str) -> whisper.Whisper:
    """Load a Whisper model, reusing a cached instance if already loaded.

    Args:
        model_name: Whisper model size (e.g. 'medium', 'large-v3').

    Returns:
        Loaded whisper.Whisper model instance.
    """
    if model_name not in _model_cache:
        logger.info(f"Loading Whisper model '{model_name}' (first use — may take a moment)...")
        _model_cache[model_name] = whisper.load_model(model_name)
        logger.info(f"Whisper model '{model_name}' loaded.")
    return _model_cache[model_name]


def _seconds_to_srt_timestamp(seconds: float) -> str:
    """Convert a float seconds value to SRT timestamp format HH:MM:SS,mmm.

    Args:
        seconds: Time in seconds (may have fractional component).

    Returns:
        Formatted string like '00:01:23,456'.
    """
    total_ms = int(round(seconds * 1000))
    millis = total_ms % 1000
    total_s = total_ms // 1000
    secs = total_s % 60
    total_m = total_s // 60
    mins = total_m % 60
    hours = total_m // 60
    return f"{hours:02d}:{mins:02d}:{secs:02d},{millis:03d}"


def _build_srt(segments: list[dict]) -> str:
    """Convert Whisper segments to SRT format string.

    Args:
        segments: List of Whisper segment dicts, each with 'start', 'end', 'text'.

    Returns:
        Full SRT file contents as a string.
    """
    lines = []
    for i, seg in enumerate(segments, start=1):
        start = _seconds_to_srt_timestamp(seg["start"])
        end = _seconds_to_srt_timestamp(seg["end"])
        text = seg["text"].strip()
        lines.append(f"{i}\n{start} --> {end}\n{text}\n")
    return "\n".join(lines)


def _update_meta(meta_path: Path, updates: dict) -> None:
    """Merge update fields into an existing .meta.json file.

    Args:
        meta_path: Path to the sidecar .meta.json file.
        updates: Dict of fields to add or overwrite.
    """
    meta = json.loads(meta_path.read_text())
    meta.update(updates)
    meta_path.write_text(json.dumps(meta, indent=2))


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_transcription(clip_path: str, config: dict) -> dict:
    """Transcribe a single clip using Whisper.

    Skips transcription if a .whisper.json sidecar already exists (idempotent).
    Writes both a .whisper.json (full result) and a .srt (for FFmpeg caption burn-in).
    Updates the clip's .meta.json with transcript_path and srt_path.

    If the clip has no audio track (has_audio=False in .meta.json), an empty
    transcript is written rather than running Whisper on silence.

    Args:
        clip_path: Path to the video file (in inbox/{game}/).
        config: Full parsed config.yaml dict.

    Returns:
        Whisper result dict with keys: text (str), segments (list), language (str).
    """
    clip = Path(clip_path)
    whisper_json_path = clip.with_suffix(".whisper.json")
    srt_path = clip.with_suffix(".srt")
    meta_path = clip.with_suffix(".meta.json")

    # Idempotency: skip if already transcribed
    if whisper_json_path.exists():
        logger.debug(f"Skipping already-transcribed clip: {clip.name}")
        return json.loads(whisper_json_path.read_text())

    # Check for audio — avoid running Whisper on silent/video-only clips
    has_audio = True
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        has_audio = meta.get("has_audio", True)

    if not has_audio:
        logger.info(f"No audio track in {clip.name} — writing empty transcript.")
        result = {"text": "", "segments": [], "language": "en"}
        whisper_json_path.write_text(json.dumps(result, indent=2))
        srt_path.write_text("")
        if meta_path.exists():
            _update_meta(meta_path, {
                "transcript_path": str(whisper_json_path),
                "srt_path": str(srt_path),
            })
        return result

    model_name = config["transcription"]["model"]
    language = config["transcription"]["language"]

    logger.info(f"Transcribing {clip.name} with Whisper [{model_name}]...")
    model = _load_model(model_name)

    result = model.transcribe(
        str(clip),
        language=language,
        verbose=False,
    )

    # Whisper returns numpy arrays in segments — convert to plain Python types
    clean_segments = [
        {
            "id": seg["id"],
            "start": float(seg["start"]),
            "end": float(seg["end"]),
            "text": seg["text"],
        }
        for seg in result.get("segments", [])
    ]
    clean_result = {
        "text": result.get("text", "").strip(),
        "segments": clean_segments,
        "language": result.get("language", language),
    }

    # Write .whisper.json
    whisper_json_path.write_text(json.dumps(clean_result, indent=2))

    # Write .srt for FFmpeg caption burn-in
    srt_path.write_text(_build_srt(clean_segments))

    # Update .meta.json with file references
    if meta_path.exists():
        _update_meta(meta_path, {
            "transcript_path": str(whisper_json_path),
            "srt_path": str(srt_path),
        })

    word_count = len(clean_result["text"].split())
    logger.info(
        f"Transcribed {clip.name}: {len(clean_segments)} segment(s), "
        f"~{word_count} word(s), language={clean_result['language']}"
    )

    # Language filter: skip clip if detected language doesn't match the filter
    language_filter = config["transcription"].get("language_filter")
    if language_filter and clean_result["language"] != language_filter:
        logger.warning(
            f"Language filter: {clip.name} detected as '{clean_result['language']}' "
            f"(expected '{language_filter}') — skipping clip."
        )
        return None

    return clean_result
