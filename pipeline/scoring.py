"""
Stage 6 — AI Scoring Engine

Sends a processed clip's transcript and feature metadata to the Claude API
and receives a structured virality score in return.

The prompt includes:
  - Game name, duration, template applied
  - Full transcript text (from .whisper.json)
  - Feature signals: motion_level, audio_energy, keywords, quality_tag

Claude returns a JSON object with:
  highlight_score    — integer 0-100 (primary virality estimate)
  clip_type          — categorical label for the moment type
  suggested_title    — ready-to-use video title (<100 chars)
  suggested_caption  — social media caption with hashtags (<280 chars)
  score_reasoning    — 1-2 sentence explanation of the score

The score is stored in .meta.json under the key 'scoring' and returned as a dict.
Idempotent: skips clips whose .meta.json already contains a 'scoring' key.

Model defaults to claude-haiku-4-5-20251001 (fast + cost-efficient for this task).
Override via config['scoring']['model'].
"""

import json
import os
from pathlib import Path

import anthropic

from utils.logger import get_logger

logger = get_logger(__name__)

# Clip type labels Claude may return
_VALID_CLIP_TYPES = {
    "clutch_play", "kill_streak", "funny_moment", "highlight_reel",
    "commentary", "tutorial", "reaction", "other",
}

_FALLBACK_SCORE = {
    "highlight_score": 50,
    "clip_type": "other",
    "suggested_title": "",
    "suggested_caption": "",
    "score_reasoning": "Scoring unavailable.",
}


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(metadata: dict, transcript_text: str) -> str:
    """Assemble the scoring prompt from clip metadata and transcript.

    Args:
        metadata: Full clip feature dict from .meta.json.
        transcript_text: Raw transcript text from Whisper.

    Returns:
        Prompt string to send as the user message to Claude.
    """
    game = metadata.get("game", "unknown").replace("_", " ").title()
    duration = metadata.get("duration_seconds", 0)
    template = metadata.get("selected_template_id", "unknown")
    quality = metadata.get("quality_tag", "unknown")
    motion = metadata.get("motion_level", "unknown")
    audio_energy = metadata.get("audio_energy", "unknown")
    keywords = metadata.get("keywords", [])
    decision = metadata.get("decision", {})
    keywords_str = ", ".join(keywords) if keywords else "none detected"
    transcript_str = transcript_text.strip() if transcript_text.strip() else "(no speech detected)"

    return f"""You are a gaming clip analyst for a social media channel focused on FPS games.
Analyze the following gaming clip and score its viral potential.

CLIP INFORMATION:
- Game: {game}
- Duration: {duration:.1f} seconds
- Template applied: {template}
- Source quality: {quality}
- Motion level: {motion}
- Audio energy: {audio_energy}
- FPS keywords detected: {keywords_str}
- Pre-judge composite score: {decision.get('composite_score', 'unknown')}
- Hook gate passed: {decision.get('hook_gate_passed', 'unknown')}

TRANSCRIPT:
\"\"\"{transcript_str}\"\"\"

SCORING CRITERIA — rate viral potential on TikTok, YouTube Shorts, and Instagram Reels from 0–100:
- Excitement and memorable moments (35%): Does this clip have a clear highlight moment?
- Keyword and gaming relevance (25%): Are strong FPS action words present?
- Energy and pacing (25%): Is the clip punchy and engaging throughout?
- Title and caption potential (15%): Is there a clear hook for a title?

Respond with ONLY a valid JSON object. No markdown, no explanation outside the JSON.

{{
  "highlight_score": <integer 0-100>,
  "clip_type": "<clutch_play|kill_streak|funny_moment|highlight_reel|commentary|tutorial|reaction|other>",
  "suggested_title": "<engaging video title under 100 characters>",
  "suggested_caption": "<social media caption with relevant hashtags, under 280 characters>",
  "score_reasoning": "<1-2 sentences explaining why this score was given>"
}}"""


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------

def _parse_response(content: str) -> dict:
    """Parse Claude's JSON response, with fallback for malformed output.

    Args:
        content: Raw text content from Claude's response.

    Returns:
        Scoring result dict. Falls back to _FALLBACK_SCORE on any parse error.
    """
    # Strip any accidental markdown fences
    text = content.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(
            line for line in lines
            if not line.strip().startswith("```")
        ).strip()

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON object between first { and last }
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                result = json.loads(text[start:end])
            except json.JSONDecodeError:
                logger.warning("Could not parse Claude scoring response as JSON.")
                return _FALLBACK_SCORE.copy()
        else:
            logger.warning("No JSON object found in Claude scoring response.")
            return _FALLBACK_SCORE.copy()

    # Validate and sanitise fields
    score = result.get("highlight_score", 50)
    if not isinstance(score, (int, float)):
        score = 50
    score = max(0, min(100, int(score)))

    clip_type = result.get("clip_type", "other")
    if clip_type not in _VALID_CLIP_TYPES:
        clip_type = "other"

    return {
        "highlight_score": score,
        "clip_type": clip_type,
        "suggested_title": str(result.get("suggested_title", ""))[:100],
        "suggested_caption": str(result.get("suggested_caption", ""))[:280],
        "score_reasoning": str(result.get("score_reasoning", "")),
    }


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_scoring(clip_path: str, metadata: dict, config: dict) -> dict:
    """Score a processed clip using the Claude API.

    Reads the transcript from the sidecar .whisper.json (falls back to
    metadata keywords if the file is missing). Sends a structured prompt
    to Claude and parses the JSON response.

    Idempotent: if 'scoring' already exists in .meta.json, returns it as-is.

    Args:
        clip_path: Path to the processed clip in processing/{game}/.
        metadata:  Full clip feature dict (includes template and feature fields).
        config:    Full parsed config.yaml dict.

    Returns:
        Scoring result dict with keys: highlight_score, clip_type,
        suggested_title, suggested_caption, score_reasoning.
    """
    clip = Path(clip_path)

    # Resolve .meta.json — prefer inbox copy (has srt/transcript paths)
    inbox_clip = Path(metadata.get("clip_path", clip_path))
    meta_path = inbox_clip.with_suffix(".meta.json")
    if not meta_path.exists():
        meta_path = clip.with_suffix(".meta.json")

    # Idempotency
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if "scoring" in meta:
            logger.debug(f"Skipping already-scored clip: {clip.name}")
            return meta["scoring"]

    # Load transcript text
    transcript_text = ""
    whisper_path_str = metadata.get("transcript_path")
    if whisper_path_str:
        whisper_path = Path(whisper_path_str)
        if whisper_path.exists():
            try:
                whisper_data = json.loads(whisper_path.read_text())
                transcript_text = whisper_data.get("text", "")
            except (json.JSONDecodeError, OSError):
                pass

    # API key check
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error(
            "ANTHROPIC_API_KEY not set. Add it to your .env file. "
            "Returning fallback score."
        )
        return _FALLBACK_SCORE.copy()

    scoring_cfg = config.get("scoring", {})
    model = scoring_cfg.get("model", "claude-haiku-4-5-20251001")
    max_tokens = int(scoring_cfg.get("max_tokens", 512))

    prompt = _build_prompt(metadata, transcript_text)

    logger.info(f"Scoring {clip.name} with {model}...")

    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_content = message.content[0].text
    except anthropic.APIConnectionError as e:
        logger.error(f"Claude API connection error: {e}. Returning fallback score.")
        return _FALLBACK_SCORE.copy()
    except anthropic.AuthenticationError:
        logger.error("Claude API authentication failed — check ANTHROPIC_API_KEY.")
        return _FALLBACK_SCORE.copy()
    except anthropic.RateLimitError:
        logger.warning("Claude API rate limit hit. Returning fallback score.")
        return _FALLBACK_SCORE.copy()
    except Exception as e:
        logger.error(f"Unexpected Claude API error: {e}. Returning fallback score.")
        return _FALLBACK_SCORE.copy()

    result = _parse_response(raw_content)

    logger.info(
        f"Score [{clip.name}]: {result['highlight_score']}/100 "
        f"({result['clip_type']}) — \"{result['suggested_title']}\""
    )

    # Persist into .meta.json
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        meta["scoring"] = result
        meta_path.write_text(json.dumps(meta, indent=2))

    return result
