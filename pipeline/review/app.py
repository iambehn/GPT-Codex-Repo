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
  GET  /quarantine                    — asset-training queue for quarantined clips

Launch:
  python -m pipeline.review.app
  (or via run.py --review flag — future work)
"""

import base64
import binascii
import json
import os
import shutil
import struct
import subprocess
from datetime import datetime
from pathlib import Path

import yaml
from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from pipeline.clip_judge import evaluate as evaluate_clip
from pipeline.game_pack import (
    get_primary_entities,
    get_weapon_detector_game_config,
    list_supported_games,
    load_game_pack,
    resolve_asset_path,
)
from pipeline.weapon_detector import run_weapon_detector

app = Flask(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm"}
MAX_ICON_IMAGE_BYTES = 2 * 1024 * 1024
MAX_ICON_IMAGE_BASE64_CHARS = int(MAX_ICON_IMAGE_BYTES * 1.4)
MIN_ICON_CROP_SIDE = 8
MAX_ICON_CROP_SIDE = 1024

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

    for game in list_supported_games(CONFIG):
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


def _quarantine_root(game: str | None = None) -> Path:
    root = (PROJECT_ROOT / CONFIG["paths"]["quarantine"]).resolve()
    return root / game if game else root


def _inbox_root(game: str | None = None) -> Path:
    root = (PROJECT_ROOT / CONFIG["paths"]["inbox"]).resolve()
    return root / game if game else root


def _safe_relative(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError:
        abort(404)
    return resolved


def _get_quarantine_clips() -> list[dict]:
    """Return quarantined clips, including nested reason folders."""
    clips: list[dict] = []
    q_root = _quarantine_root()

    for game in list_supported_games(CONFIG):
        game_root = q_root / game
        if not game_root.exists():
            continue

        for clip_path in sorted(game_root.rglob("*")):
            if not clip_path.is_file() or clip_path.suffix.lower() not in VIDEO_EXTENSIONS:
                continue

            try:
                rel_path = clip_path.relative_to(game_root)
            except ValueError:
                continue

            meta_path = clip_path.with_suffix(".meta.json")
            meta = _load_json(meta_path)
            rel_parent = rel_path.parent.as_posix()
            reason = (
                (meta.get("quarantine") or {}).get("reason")
                or meta.get("quarantine_reason")
                or (rel_parent if rel_parent != "." else "legacy_root")
            )
            decision = meta.get("decision", {})

            clips.append({
                "game": game,
                "stem": rel_path.with_suffix("").as_posix(),
                "filename": clip_path.name,
                "video_relpath": rel_path.as_posix(),
                "clip_path": str(clip_path),
                "clip_id": meta.get("clip_id", clip_path.stem),
                "reason": reason,
                "decision_status": decision.get("status", ""),
                "composite_score": decision.get("composite_score", ""),
                "has_meta": meta_path.exists(),
                "mtime": clip_path.stat().st_mtime,
            })

    clips.sort(key=lambda c: (c["game"], c["reason"], -c["mtime"]))
    return clips


def _find_quarantine_clip(game: str, clip_stem: str) -> dict | None:
    for clip in _get_quarantine_clips():
        if clip["game"] == game and clip["stem"] == clip_stem:
            return clip
    return None


def _resolve_quarantine_clip(game: str, clip_stem: str) -> Path:
    clip = _find_quarantine_clip(game, clip_stem)
    if clip is None:
        abort(404)
    clip_path = Path(clip["clip_path"])
    return _safe_relative(clip_path, _quarantine_root(game))


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2))


def _ensure_quarantine_meta(clip_path: Path, game: str) -> tuple[Path, dict]:
    """Create a minimal sidecar for legacy quarantined clips if needed."""
    meta_path = clip_path.with_suffix(".meta.json")
    meta = _load_json(meta_path)
    if not meta:
        meta = {
            "clip_id": clip_path.stem,
            "game": game,
            "clip_path": str(clip_path),
            "meta_path": str(meta_path),
            "status": "quarantine",
            "quarantine_reason": "legacy_root",
            "created_from": "quarantine_review",
        }
    else:
        meta["clip_path"] = str(clip_path)
        meta["meta_path"] = str(meta_path)
        meta.setdefault("game", game)
        meta.setdefault("clip_id", clip_path.stem)
    _write_json(meta_path, meta)
    return meta_path, meta


def _entity_options(game: str) -> tuple[str, list[dict]]:
    game_pack = load_game_pack(game, CONFIG)
    primary_kind, entities = get_primary_entities(game_pack)
    options = [
        {
            "entity_id": entity_id,
            "display_name": data.get("display_name", entity_id.replace("_", " ").title()),
            "primary_kind": primary_kind,
        }
        for entity_id, data in sorted(entities.items())
    ]
    return primary_kind, options


def _icon_dir_for_game(game: str) -> Path:
    game_pack = load_game_pack(game, CONFIG)
    wd_cfg = get_weapon_detector_game_config(game, CONFIG, game_pack)
    raw_path = wd_cfg.get("icon_dir") or f"assets/weapon_icons/{game}"
    return resolve_asset_path(raw_path, Path(game_pack.get("pack_root", PROJECT_ROOT)))


def _png_dimensions(image_bytes: bytes) -> tuple[int, int]:
    if len(image_bytes) < 24 or not image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("image_b64 must be a PNG image")

    pos = 8
    width = height = None
    seen_iend = False
    first_chunk = True
    while pos + 8 <= len(image_bytes):
        length = struct.unpack(">I", image_bytes[pos:pos + 4])[0]
        chunk_type = image_bytes[pos + 4:pos + 8]
        pos += 8
        if pos + length + 4 > len(image_bytes):
            raise ValueError("image_b64 must be a complete PNG image")

        chunk_data = image_bytes[pos:pos + length]
        pos += length
        expected_crc = struct.unpack(">I", image_bytes[pos:pos + 4])[0]
        pos += 4

        actual_crc = binascii.crc32(chunk_type + chunk_data) & 0xFFFFFFFF
        if expected_crc != actual_crc:
            raise ValueError("image_b64 PNG checksum validation failed")
        if first_chunk and chunk_type != b"IHDR":
            raise ValueError("image_b64 PNG is missing an IHDR header")
        first_chunk = False

        if chunk_type == b"IHDR":
            if length != 13:
                raise ValueError("image_b64 PNG has an invalid IHDR header")
            width, height = struct.unpack(">II", chunk_data[:8])
            if width <= 0 or height <= 0:
                raise ValueError("image_b64 PNG dimensions must be positive")
        elif chunk_type == b"IEND":
            seen_iend = True
            break

    if width is None or height is None or not seen_iend:
        raise ValueError("image_b64 must be a complete PNG image")
    return width, height


def _decode_png(image_b64: str) -> bytes:
    if "," in image_b64:
        image_b64 = image_b64.split(",", 1)[1]
    if len(image_b64) > MAX_ICON_IMAGE_BASE64_CHARS:
        raise ValueError(f"image_b64 is too large; max PNG size is {MAX_ICON_IMAGE_BYTES} bytes")
    try:
        image_bytes = base64.b64decode(image_b64, validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("image_b64 is not valid base64")
    if len(image_bytes) > MAX_ICON_IMAGE_BYTES:
        raise ValueError(f"PNG payload is too large; max size is {MAX_ICON_IMAGE_BYTES} bytes")
    _png_dimensions(image_bytes)
    return image_bytes


def _validate_crop_box(crop_box: dict) -> dict:
    if not isinstance(crop_box, dict):
        raise ValueError("crop_box must be an object with x, y, w, and h")
    for field in ("x", "y", "w", "h"):
        if field not in crop_box:
            raise ValueError(f"crop_box is missing '{field}'")

    try:
        normalized = {
            field: int(round(float(crop_box[field])))
            for field in ("x", "y", "w", "h")
        }
    except (TypeError, ValueError):
        raise ValueError("crop_box values must be numeric") from None

    if normalized["x"] < 0 or normalized["y"] < 0:
        raise ValueError("crop_box x and y must be non-negative")
    if normalized["w"] < MIN_ICON_CROP_SIDE or normalized["h"] < MIN_ICON_CROP_SIDE:
        raise ValueError(f"crop_box width and height must be at least {MIN_ICON_CROP_SIDE}px")
    if normalized["w"] > MAX_ICON_CROP_SIDE or normalized["h"] > MAX_ICON_CROP_SIDE:
        raise ValueError(f"crop_box width and height must be no larger than {MAX_ICON_CROP_SIDE}px")
    return normalized


def _validate_png_matches_crop(image_bytes: bytes, crop_box: dict) -> None:
    width, height = _png_dimensions(image_bytes)
    if width != crop_box["w"] or height != crop_box["h"]:
        raise ValueError(
            "PNG dimensions must match crop_box width and height "
            f"({width}x{height} != {crop_box['w']}x{crop_box['h']})"
        )


def _asset_path_response(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _backup_existing_asset(asset_path: Path) -> Path | None:
    if not asset_path.exists():
        return None

    backup_dir = asset_path.parent / ".backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    candidate = backup_dir / f"{asset_path.stem}.{stamp}{asset_path.suffix}"
    for i in range(1, 1000):
        if not candidate.exists():
            shutil.copy2(asset_path, candidate)
            return candidate
        candidate = backup_dir / f"{asset_path.stem}.{stamp}.{i}{asset_path.suffix}"
    raise RuntimeError(f"Could not create backup for {asset_path.name}")


def _save_training_audit(
    meta_path: Path,
    meta: dict,
    entity_id: str,
    display_name: str,
    asset_path: Path,
    crop_box: dict,
    frame_time_seconds: float,
    overwrite: bool,
    backup_path: Path | None = None,
) -> None:
    entry = {
        "entity_id": entity_id,
        "display_name": display_name,
        "asset_path": _asset_path_response(asset_path),
        "crop_box": crop_box,
        "frame_time_seconds": frame_time_seconds,
        "source_clip": meta.get("clip_path", meta.get("clip_id", meta_path.stem)),
        "source_clip_id": meta.get("clip_id", meta_path.stem),
        "overwrite": overwrite,
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }
    if backup_path:
        entry["backup_path"] = _asset_path_response(backup_path)
    meta.setdefault("asset_training", [])
    meta["asset_training"].append(entry)
    _write_json(meta_path, meta)


def _unique_inbox_destination(src: Path, game: str) -> Path:
    dest_dir = _inbox_root(game)
    dest_dir.mkdir(parents=True, exist_ok=True)
    candidate = dest_dir / src.name
    if not candidate.exists():
        return candidate
    for i in range(1, 1000):
        next_candidate = dest_dir / f"{src.stem}_{i}{src.suffix}"
        if not next_candidate.exists():
            return next_candidate
    raise RuntimeError(f"Could not find unique inbox destination for {src.name}")


def _move_quarantine_clip_to_inbox(clip_path: Path, game: str, meta: dict) -> Path:
    dest = _unique_inbox_destination(clip_path, game)
    shutil.move(str(clip_path), str(dest))

    old_meta_path = clip_path.with_suffix(".meta.json")
    new_meta_path = dest.with_suffix(".meta.json")
    if old_meta_path.exists():
        shutil.move(str(old_meta_path), str(new_meta_path))

    meta["clip_path"] = str(dest)
    meta["meta_path"] = str(new_meta_path)
    meta["quarantine_recovered_at"] = datetime.now().isoformat(timespec="seconds")
    meta["status"] = "recovered_from_quarantine"
    _write_json(new_meta_path, meta)
    return dest


def _rescan_quarantine_clip(game: str, clip_stem: str) -> dict:
    clip_path = _resolve_quarantine_clip(game, clip_stem)
    meta_path, meta = _ensure_quarantine_meta(clip_path, game)

    weapon_detection = run_weapon_detector(clip_path, game, CONFIG, force=True)
    meta = _load_json(meta_path)
    game_pack = load_game_pack(game, CONFIG)
    judge = evaluate_clip(clip_path, game_pack, CONFIG, force=True)
    meta = _load_json(meta_path)

    moved_to_inbox = False
    inbox_path = None
    if (judge.get("decision") or {}).get("status") == "accept":
        inbox_path = _move_quarantine_clip_to_inbox(clip_path, game, meta)
        moved_to_inbox = True

    return {
        "weapon_detection": weapon_detection,
        "decision": judge.get("decision", {}),
        "quarantine": judge.get("quarantine", {}),
        "moved_to_inbox": moved_to_inbox,
        "inbox_path": str(inbox_path) if inbox_path else None,
    }


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
# Quarantine / asset-training routes
# ---------------------------------------------------------------------------

def _json_error(message: str, status: int = 400):
    response = jsonify({"ok": False, "error": message})
    response.status_code = status
    return response


@app.route("/quarantine")
def quarantine_queue():
    clips = _get_quarantine_clips()
    return render_template("quarantine_queue.html", clips=clips)


@app.route("/quarantine/<game>/<path:clip_stem>")
def quarantine_review(game: str, clip_stem: str):
    clip = _find_quarantine_clip(game, clip_stem)
    if clip is None:
        abort(404)

    clip_path = _resolve_quarantine_clip(game, clip_stem)
    meta = _load_json(clip_path.with_suffix(".meta.json"))
    primary_kind, entities = _entity_options(game)
    video_url = url_for("quarantine_video", game=game, filename=clip["video_relpath"])

    return render_template(
        "quarantine_review.html",
        clip=clip,
        meta=meta,
        primary_kind=primary_kind,
        entities=entities,
        video_url=video_url,
    )


@app.route("/quarantine/video/<game>/<path:filename>")
def quarantine_video(game: str, filename: str):
    game_root = _quarantine_root(game).resolve()
    clip_path = _safe_relative(game_root / filename, game_root)
    if not clip_path.exists() or clip_path.suffix.lower() not in VIDEO_EXTENSIONS:
        abort(404)
    rel_path = clip_path.relative_to(game_root).as_posix()
    return send_from_directory(str(game_root), rel_path)


@app.route("/api/quarantine/roster/<game>")
def api_quarantine_roster(game: str):
    primary_kind, entities = _entity_options(game)
    return jsonify({"ok": True, "game": game, "primary_kind": primary_kind, "entities": entities})


@app.route("/api/quarantine/save-icon", methods=["POST"])
def api_quarantine_save_icon():
    data = request.get_json(silent=True) or {}
    game = str(data.get("game", "")).strip()
    clip_stem = str(data.get("clip_stem", "")).strip()
    entity_id = str(data.get("entity_id", "")).strip()
    image_b64 = str(data.get("image_b64", "")).strip()
    crop_box = data.get("crop_box") or {}
    overwrite = bool(data.get("overwrite", False))

    if not game or not clip_stem or not entity_id or not image_b64:
        return _json_error("game, clip_stem, entity_id, and image_b64 are required")

    entity_map = {entity["entity_id"]: entity for entity in _entity_options(game)[1]}
    entity = entity_map.get(entity_id)
    if entity is None:
        return _json_error(f"Unknown entity_id '{entity_id}' for game '{game}'", 400)

    clip_path = _resolve_quarantine_clip(game, clip_stem)
    meta_path, meta = _ensure_quarantine_meta(clip_path, game)

    try:
        image_bytes = _decode_png(image_b64)
    except ValueError as e:
        return _json_error(str(e), 400)

    try:
        normalized_crop_box = _validate_crop_box(crop_box)
        _validate_png_matches_crop(image_bytes, normalized_crop_box)
    except ValueError as e:
        return _json_error(str(e), 400)

    try:
        frame_time_seconds = float(data.get("frame_time_seconds", 0.0))
    except (TypeError, ValueError):
        return _json_error("frame_time_seconds must be numeric", 400)

    icon_dir = _icon_dir_for_game(game)
    icon_dir.mkdir(parents=True, exist_ok=True)
    asset_path = icon_dir / f"{entity_id}.png"
    if asset_path.exists() and not overwrite:
        return _json_error(
            f"Icon already exists for '{entity_id}'. Enable overwrite to replace it.",
            409,
        )

    try:
        backup_path = _backup_existing_asset(asset_path) if overwrite else None
    except RuntimeError as e:
        return _json_error(str(e), 500)
    asset_path.write_bytes(image_bytes)
    _save_training_audit(
        meta_path=meta_path,
        meta=meta,
        entity_id=entity_id,
        display_name=entity["display_name"],
        asset_path=asset_path,
        crop_box=normalized_crop_box,
        frame_time_seconds=frame_time_seconds,
        overwrite=overwrite,
        backup_path=backup_path,
    )

    rescan = _rescan_quarantine_clip(game, clip_stem)
    return jsonify({
        "ok": True,
        "asset_path": _asset_path_response(asset_path),
        "backup_path": _asset_path_response(backup_path) if backup_path else None,
        "rescan": rescan,
    })


@app.route("/api/quarantine/rescan", methods=["POST"])
def api_quarantine_rescan():
    data = request.get_json(silent=True) or {}
    game = str(data.get("game", "")).strip()
    clip_stem = str(data.get("clip_stem", "")).strip()
    if not game or not clip_stem:
        return _json_error("game and clip_stem are required")
    rescan = _rescan_quarantine_clip(game, clip_stem)
    return jsonify({"ok": True, "rescan": rescan})

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
