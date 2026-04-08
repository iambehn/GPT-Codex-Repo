"""
Stage 7 — Manual Review UI

Flask web app for reviewing processed clips before distribution.
Clips sit in processing/{game}/ after AI Scoring. The reviewer watches
each clip, sees the virality score and Claude-generated metadata, then
approves or rejects.

Routes:
  GET  /                              — queue view (all pending clips, sorted by score)
  GET  /clip/<game>/<stem>            — single clip review page
  POST /clip/<game>/<stem>/approve    — approve → accepted/{game}/, load next
  POST /clip/<game>/<stem>/reject     — reject  → rejected/{game}/, load next
  GET  /video/<game>/<filename>       — stream the processed video file

Launch:
  python -m pipeline.review.app
  (or via run.py --review flag — future work)
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

import yaml
from flask import (
    Flask,
    abort,
    redirect,
    render_template,
    send_from_directory,
    url_for,
)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    config_path = Path(__file__).parent.parent.parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


CONFIG: dict = {}


@app.before_request
def _ensure_config():
    global CONFIG
    if not CONFIG:
        CONFIG = _load_config()


# ---------------------------------------------------------------------------
# Clip discovery helpers
# ---------------------------------------------------------------------------

def _get_pending_clips() -> list[dict]:
    """Scan inbox meta files to find clips that are processed but not yet reviewed.

    A clip is pending review when:
      - Its .meta.json has a non-empty 'processed_path'
      - That processed file still exists inside processing/

    Returns list of clip info dicts sorted by highlight_score descending.
    """
    clips = []
    inbox_root = Path(CONFIG["paths"]["inbox"])
    processing_root = Path(CONFIG["paths"]["processing"])

    for game in CONFIG["games"]:
        inbox_dir = inbox_root / game
        if not inbox_dir.exists():
            continue

        for meta_file in sorted(inbox_dir.glob("*.meta.json")):
            try:
                meta = json.loads(meta_file.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            processed_path_str = meta.get("processed_path", "")
            if not processed_path_str:
                continue

            processed = Path(processed_path_str)
            # Must still live inside the processing/ tree (not moved yet)
            if not processed.exists():
                continue
            try:
                processed.relative_to(processing_root)
            except ValueError:
                continue

            scoring = meta.get("scoring", {})
            clips.append({
                "meta": meta,
                "game": game,
                "stem": processed.stem,
                "filename": processed.name,
                "processed_path": str(processed),
                "clip_id": meta.get("clip_id", meta_file.stem),
                "score": scoring.get("highlight_score", 0),
                "clip_type": scoring.get("clip_type", "unknown"),
                "suggested_title": scoring.get("suggested_title", ""),
                "suggested_caption": scoring.get("suggested_caption", ""),
                "score_reasoning": scoring.get("score_reasoning", ""),
                "duration": round(meta.get("duration_seconds", 0), 1),
                "template": meta.get("selected_template_id", ""),
                "motion_level": meta.get("motion_level", ""),
                "audio_energy": meta.get("audio_energy", ""),
                "keywords": meta.get("keywords", []),
                "quality_tag": meta.get("quality_tag", ""),
            })

    clips.sort(key=lambda c: c["score"], reverse=True)
    return clips


def _find_clip(game: str, stem: str) -> dict | None:
    """Find a specific pending clip by game and filename stem."""
    for clip in _get_pending_clips():
        if clip["game"] == game and clip["stem"] == stem:
            return clip
    return None


def _next_clip(game: str, stem: str) -> dict | None:
    """Return the next pending clip after the given one, or None."""
    clips = _get_pending_clips()
    for i, clip in enumerate(clips):
        if clip["game"] == game and clip["stem"] == stem:
            if i + 1 < len(clips):
                return clips[i + 1]
            return None
    return clips[0] if clips else None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def queue():
    clips = _get_pending_clips()
    return render_template("queue.html", clips=clips)


@app.route("/clip/<game>/<stem>")
def review_clip(game: str, stem: str):
    clip = _find_clip(game, stem)
    if clip is None:
        abort(404)
    next_c = _next_clip(game, stem)
    video_url = url_for("serve_video", game=game, filename=clip["filename"])
    return render_template("review.html", clip=clip, next_clip=next_c, video_url=video_url)


@app.route("/clip/<game>/<stem>/approve", methods=["POST"])
def approve_clip(game: str, stem: str):
    return _handle_decision(game, stem, "accepted")


@app.route("/clip/<game>/<stem>/reject", methods=["POST"])
def reject_clip(game: str, stem: str):
    return _handle_decision(game, stem, "rejected")


def _handle_decision(game: str, stem: str, decision: str):
    """Move the processed clip to accepted/ or rejected/ and update meta."""
    clip = _find_clip(game, stem)
    if clip is None:
        abort(404)

    src = Path(clip["processed_path"])
    dest_dir = Path(CONFIG["paths"][decision]) / game
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name

    shutil.move(str(src), str(dest))

    # Update meta with review outcome
    inbox_root = Path(CONFIG["paths"]["inbox"])
    meta_path = inbox_root / game / f"{clip['clip_id']}.meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        meta["review_status"] = decision
        meta["reviewed_at"] = datetime.now().isoformat(timespec="seconds")
        meta["final_path"] = str(dest)
        meta_path.write_text(json.dumps(meta, indent=2))

    # Redirect to next pending clip (or back to queue if none)
    next_c = _next_clip(game, stem)
    if next_c:
        return redirect(url_for("review_clip", game=next_c["game"], stem=next_c["stem"]))
    return redirect(url_for("queue"))


@app.route("/video/<game>/<filename>")
def serve_video(game: str, filename: str):
    """Stream a processed video file safely."""
    video_dir = Path(CONFIG["paths"]["processing"]) / game
    return send_from_directory(str(video_dir), filename, mimetype="video/mp4")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cfg = _load_config()
    review_cfg = cfg.get("review", {})
    app.run(
        host=review_cfg.get("host", "127.0.0.1"),
        port=review_cfg.get("port", 5000),
        debug=review_cfg.get("debug", False),
    )
