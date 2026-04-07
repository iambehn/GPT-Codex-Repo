"""
Stage 2 — Transcription

Runs OpenAI Whisper on a clip's audio track to produce a segment-level transcript.
Output is written as a sidecar .json file (Whisper JSON format) alongside the clip.
The transcript is used downstream by Feature Extraction (keyword matching) and
AI Scoring (context for the Claude prompt).

Future upgrade: replace with WhisperX for word-level timestamps when word-highlight
captions are implemented.
"""

from utils.logger import get_logger

logger = get_logger(__name__)


def run_transcription(clip_path: str, config: dict) -> dict:
    """Transcribe a single clip using Whisper.

    Args:
        clip_path: Absolute or relative path to the video file.
        config: Full parsed config.yaml dict.

    Returns:
        Whisper result dict with keys: text, segments, language.
        Also writes a sidecar <clip_path>.whisper.json to disk.
    """
    pass
