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
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import yaml
from flask import (
    Flask,
    Response,
    abort,
    redirect,
    render_template,
    request,
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
    project_root = Path(__file__).parent.parent.parent
    inbox_root = (project_root / CONFIG["paths"]["inbox"]).resolve()
    processing_root = (project_root / CONFIG["paths"]["processing"]).resolve()

    for game in CONFIG["games"]:
        inbox_dir = inbox_root / game
        if not inbox_dir.exists():
            continue

        for meta_file in sorted(inbox_dir.glob("*.meta.json")):
            try:
                meta = json.loads(meta_file.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            # Skip clips that have already been reviewed
            if meta.get("review_status"):
                continue

            processed_path_str = meta.get("processed_path", "")
            if not processed_path_str:
                continue

            processed = Path(processed_path_str)
            # Resolve legacy relative paths written before absolute-path fix
            if not processed.is_absolute():
                processed = (project_root / processed).resolve()
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
    project_root = Path(__file__).parent.parent.parent
    video_dir = (project_root / CONFIG["paths"]["processing"] / game).resolve()
    return send_from_directory(str(video_dir), filename, mimetype="video/mp4")


@app.route("/thumb/<game>/<stem>")
def serve_thumb(game: str, stem: str):
    """Return a JPEG thumbnail for a clip in processing/.

    Extracts a frame at the 3-second mark using FFmpeg on first request,
    caches it as a .thumb.jpg sidecar next to the video, and serves it.
    Returns a 1x1 transparent GIF if the clip doesn't exist or FFmpeg fails.
    """
    project_root = Path(__file__).parent.parent.parent
    processing_dir = (project_root / CONFIG["paths"]["processing"] / game).resolve()
    clip_path = processing_dir / f"{stem}.mp4"
    thumb_path = processing_dir / f"{stem}.thumb.jpg"

    if not clip_path.exists():
        # Return a 1×1 transparent GIF placeholder
        return Response(
            b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!"
            b"\xf9\x04\x00\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02"
            b"\x02D\x01\x00;",
            mimetype="image/gif",
        )

    if not thumb_path.exists():
        try:
            subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-ss", "3",
                    "-i", str(clip_path),
                    "-frames:v", "1",
                    "-q:v", "4",
                    "-vf", "scale=160:-1",
                    str(thumb_path),
                ],
                capture_output=True,
                timeout=15,
            )
        except Exception:
            pass

    if thumb_path.exists():
        return send_from_directory(str(processing_dir), thumb_path.name, mimetype="image/jpeg")

    return Response(
        b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!"
        b"\xf9\x04\x00\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02"
        b"\x02D\x01\x00;",
        mimetype="image/gif",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Scout routes
# ---------------------------------------------------------------------------

@app.route("/scout")
def scout():
    from pipeline.scout.tracker import load_cache
    cache = load_cache()
    thresholds = CONFIG.get("scout", {}).get("thresholds", {})
    trend_min = thresholds.get("trend_score_min", 6)
    longevity_min = thresholds.get("longevity_score_min", 5)

    games_data = []
    for name, data in cache.get("games", {}).items():
        latest = data.get("latest", {})
        prev_score = data.get("previous_trend_score")
        curr_score = latest.get("trend_score", 0) if latest else 0

        if prev_score is None or not latest:
            direction = None
        elif curr_score > prev_score:
            direction = "up"
        elif curr_score < prev_score:
            direction = "down"
        else:
            direction = "flat"

        games_data.append({
            "name": name,
            "longevity_score": data.get("longevity_score", 5),
            "latest": latest or {},
            "previous_trend_score": prev_score,
            "flagged": latest.get("flagged", False) if latest else False,
            "direction": direction,
        })

    games_data.sort(key=lambda g: (-int(g["flagged"]), -g["latest"].get("trend_score", 0)))

    return render_template(
        "scout.html",
        games=games_data,
        last_poll=cache.get("last_poll"),
        trend_min=trend_min,
        longevity_min=longevity_min,
    )


@app.route("/scout/poll", methods=["POST"])
def scout_poll():
    """Trigger an immediate background poll of all tracked games."""
    import threading as _t
    from pipeline.scout.tracker import poll_all_games
    _t.Thread(target=poll_all_games, args=(CONFIG,), daemon=True).start()
    return redirect(url_for("scout"))


@app.route("/scout/game/add", methods=["POST"])
def scout_add_game():
    from pipeline.scout.tracker import add_game, poll_game
    name = request.form.get("game_name", "").strip()
    longevity = int(request.form.get("longevity_score", 5))
    if name:
        add_game(name, longevity)
        # Poll immediately so the row has data on first load
        import threading as _t
        _t.Thread(target=poll_game, args=(name, CONFIG), daemon=True).start()
    return redirect(url_for("scout"))


@app.route("/scout/game/remove", methods=["POST"])
def scout_remove_game():
    from pipeline.scout.tracker import remove_game
    name = request.form.get("game_name", "").strip()
    if name:
        remove_game(name)
    return redirect(url_for("scout"))


@app.route("/scout/game/longevity", methods=["POST"])
def scout_set_longevity():
    from pipeline.scout.tracker import set_longevity
    name = request.form.get("game_name", "").strip()
    score = request.form.get("longevity_score", 5)
    if name:
        set_longevity(name, score, CONFIG)
    return redirect(url_for("scout"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cfg = _load_config()
    review_cfg = cfg.get("review", {})
    debug = review_cfg.get("debug", False)

    # Start background scout polling.
    # In debug mode, Werkzeug runs the script twice (reloader parent + worker).
    # Only start the thread in the actual worker subprocess to avoid duplicates.
    if not debug or os.environ.get("WERKZEUG_RUN_MAIN"):
        from pipeline.scout.tracker import start_background_polling
        start_background_polling(cfg)

    app.run(
        host=review_cfg.get("host", "127.0.0.1"),
        port=review_cfg.get("port", 5000),
        debug=debug,
    )
