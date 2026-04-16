"""
Montage Assembler — pipeline/montage.py

Assembles a short-form montage from accepted clips for a single game.
Selects clips by kill_feed.sweat_score (highest first), trims each to its
peak action window, and concatenates them with a hard cut or brief flash
transition until the target duration is reached.

Source:  accepted/{game}/*.mp4  (clips that have already passed manual review)
Output:  montage/{game}/montage_{game}_{date}_{id}.mp4 + .meta.json

Clip trimming:
    Uses kill_feed.kill_timestamps and kill_feed.headshot_timestamps from
    meta.json to locate the peak scoring 5-second window, then adds
    configurable padding on each side. Clips without kill_feed data are
    skipped when require_kill_feed: true (default); set false to include
    them trimmed to their midpoint instead.

Config block (config.yaml → montage):
    enabled: false
    target_duration_seconds: 45
    transition: "flash"            # "cut" | "flash"
    flash_color: "white"           # "white" | "black"
    flash_duration_seconds: 0.067  # ~2 frames at 30 fps
    clip_window_seconds: 5         # action window per clip
    clip_padding_seconds: 2        # context before/after window
    min_sweat_score: 0             # minimum kill_feed.sweat_score
    require_kill_feed: true        # skip clips without kill_feed data
    output_dir: "montage"
    max_clips: 10                  # safety cap
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

_NORM_FPS = "30"
_NORM_CODEC_V = "libx264"
_NORM_CRF = "18"
_NORM_PRESET = "fast"
_NORM_CODEC_A = "aac"
_NORM_AUDIO_RATE = "44100"


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_montage(game: str, config: dict) -> dict | None:
    """Assemble a montage from accepted clips for a game.

    Returns the montage manifest dict on success, or None if assembly
    failed or there were not enough eligible clips.
    """
    mc = config.get("montage", {})
    if not mc.get("enabled", False):
        logger.info("[montage] Disabled in config — set montage.enabled: true to activate.")
        return None

    accepted_dir = Path(config["paths"]["accepted"]) / game
    inbox_dir = Path(config["paths"]["inbox"]) / game

    if not accepted_dir.exists():
        logger.warning(f"[montage] No accepted clips found for '{game}'.")
        return None

    output_dir = Path(mc.get("output_dir", "montage")) / game
    output_dir.mkdir(parents=True, exist_ok=True)

    clips = _select_clips(accepted_dir, inbox_dir, game, mc)
    if len(clips) < 2:
        logger.warning(
            f"[montage] Need at least 2 eligible clips for '{game}', "
            f"found {len(clips)}. Run the pipeline and approve more clips first."
        )
        return None

    target = float(mc.get("target_duration_seconds", 45))
    window = float(mc.get("clip_window_seconds", 5))
    padding = float(mc.get("clip_padding_seconds", 2))
    max_clips = int(mc.get("max_clips", 10))

    # Greedy selection: keep adding clips until target duration is reached
    selected: list[dict] = []
    total_duration = 0.0

    for clip in clips:
        if total_duration >= target or len(selected) >= max_clips:
            break

        kf = clip["meta"].get("kill_feed", {})
        clip_duration = float(clip["meta"].get("duration_seconds", 30))

        start, end = _find_peak_window(
            kf.get("kill_timestamps", []),
            kf.get("headshot_timestamps", []),
            clip_duration,
            window,
            padding,
        )
        selected.append({
            "clip_path":        clip["clip_path"],
            "meta":             clip["meta"],
            "trim_start":       round(start, 3),
            "trim_end":         round(end, 3),
            "segment_duration": round(end - start, 3),
        })
        total_duration += end - start

    if not selected:
        logger.warning(f"[montage] No clips selected for '{game}'.")
        return None

    montage_id = uuid.uuid4().hex[:8]
    date_str = datetime.now().strftime("%Y%m%d")
    output_name = f"montage_{game}_{date_str}_{montage_id}.mp4"
    output_path = output_dir / output_name

    logger.info(
        f"[montage] Assembling {len(selected)} clip(s) for '{game}' "
        f"→ {output_name} (target={target}s, estimated={total_duration:.1f}s)"
    )

    with tempfile.TemporaryDirectory(prefix="montage_") as tmp_dir:
        success = _assemble(selected, output_path, Path(tmp_dir), mc)

    if not success or not output_path.exists():
        logger.error(f"[montage] Assembly failed for '{game}'.")
        return None

    manifest = _write_montage_meta(output_path, game, selected, total_duration, mc)
    logger.info(
        f"[montage] Complete: {output_path} "
        f"({total_duration:.1f}s, {len(selected)} clips, "
        f"transition={mc.get('transition', 'flash')})"
    )
    return manifest


# ---------------------------------------------------------------------------
# Clip selection
# ---------------------------------------------------------------------------

def _select_clips(
    accepted_dir: Path,
    inbox_dir: Path,
    game: str,
    mc: dict,
) -> list[dict]:
    """Load accepted clips, resolve their meta.json, filter, and sort by sweat_score."""
    require_kf = mc.get("require_kill_feed", True)
    min_score = float(mc.get("min_sweat_score", 0))
    clips = []

    for mp4 in sorted(accepted_dir.glob("*.mp4")):
        meta = _load_meta(mp4, inbox_dir)
        if meta is None:
            logger.debug(f"[montage] No meta found for {mp4.name} — skipping.")
            continue

        # Never include a montage within a montage
        if meta.get("is_montage"):
            continue

        kf = meta.get("kill_feed", {})
        has_events = bool(
            kf.get("kill_count", 0) > 0
            or kf.get("headshot_count", 0) > 0
            or float(kf.get("sweat_score", 0)) > 0
        )

        if require_kf and not has_events:
            logger.debug(f"[montage] Skipping {mp4.name} — no kill_feed data (require_kill_feed: true).")
            continue

        sweat = float(kf.get("sweat_score", 0)) if kf else 0.0
        if sweat < min_score:
            continue

        clips.append({"clip_path": mp4, "meta": meta, "sweat_score": sweat})

    clips.sort(key=lambda c: c["sweat_score"], reverse=True)
    return clips


def _load_meta(mp4_path: Path, inbox_dir: Path) -> dict | None:
    """Find and load the .meta.json for an accepted clip.

    Checks alongside the mp4 first, then searches inbox_dir by clip_id
    (the accepted filename format is {game}_{date}_{clip_id}).
    """
    # Check next to the mp4
    adjacent = mp4_path.with_suffix(".meta.json")
    if adjacent.exists():
        try:
            return json.loads(adjacent.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    # Derive clip_id from filename and search inbox
    parts = mp4_path.stem.split("_", 2)
    clip_id_guess = parts[2] if len(parts) == 3 else mp4_path.stem

    candidate = inbox_dir / f"{clip_id_guess}.meta.json"
    if candidate.exists():
        try:
            return json.loads(candidate.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    # Slow path: scan all inbox meta files
    for meta_file in inbox_dir.glob("*.meta.json"):
        try:
            meta = json.loads(meta_file.read_text())
            if meta.get("clip_id") == clip_id_guess:
                return meta
        except (json.JSONDecodeError, OSError):
            continue

    return None


# ---------------------------------------------------------------------------
# Peak window calculation
# ---------------------------------------------------------------------------

def _find_peak_window(
    kill_ts: list,
    headshot_ts: list,
    duration: float,
    window: float,
    padding: float,
    kill_score: float = 10,
    headshot_score: float = 20,
) -> tuple[float, float]:
    """Return (start, end) of the highest-scoring action window in the clip."""
    events = [(float(t), kill_score) for t in kill_ts] + \
             [(float(t), headshot_score) for t in headshot_ts]

    if not events:
        # No event data — centre on the midpoint of the clip
        mid = duration / 2
        start = max(0.0, mid - window / 2 - padding)
        end = min(duration, mid + window / 2 + padding)
        return start, end

    all_ts = sorted(set(t for t, _ in events))
    best_score = -1.0
    best_window_start = all_ts[0]

    for t in all_ts:
        score = sum(s for et, s in events if t <= et < t + window)
        if score > best_score:
            best_score = score
            best_window_start = t

    clip_start = max(0.0, best_window_start - padding)
    clip_end = min(duration, best_window_start + window + padding)
    return clip_start, clip_end


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def _assemble(
    selected: list[dict],
    output_path: Path,
    tmp: Path,
    mc: dict,
) -> bool:
    """Trim segments, generate flash frames, then concat everything."""
    transition = mc.get("transition", "flash")
    flash_color = mc.get("flash_color", "white")
    flash_dur = float(mc.get("flash_duration_seconds", 0.067))

    # 1. Trim and normalize each segment
    segment_paths: list[Path] = []
    for i, seg in enumerate(selected):
        out = tmp / f"seg_{i:02d}.mp4"
        ok = _trim_clip(seg["clip_path"], seg["trim_start"], seg["trim_end"], out)
        if ok:
            segment_paths.append(out)
        else:
            logger.warning(f"[montage] Trim failed for {seg['clip_path'].name} — skipping.")

    if not segment_paths:
        return False

    # 2. Interleave flash frames if requested
    if transition == "flash" and len(segment_paths) > 1:
        probe = _probe_video(segment_paths[0])
        flash_path = tmp / "flash.mp4"
        if _generate_flash(flash_color, probe["width"], probe["height"], probe["fps"], flash_dur, flash_path):
            interleaved: list[Path] = []
            for i, p in enumerate(segment_paths):
                interleaved.append(p)
                if i < len(segment_paths) - 1:
                    interleaved.append(flash_path)
            segment_paths = interleaved

    # 3. Write concat list and run final pass
    concat_list = tmp / "concat.txt"
    concat_list.write_text("".join(f"file '{p}'\n" for p in segment_paths))

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_list),
        "-c:v", _NORM_CODEC_V, "-crf", _NORM_CRF, "-preset", _NORM_PRESET,
        "-c:a", _NORM_CODEC_A,
        str(output_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"[montage] FFmpeg concat error: {result.stderr[-400:]}")
            return False
        return True
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        logger.error(f"[montage] FFmpeg concat exception: {e}")
        return False


def _trim_clip(clip_path: Path, start: float, end: float, output_path: Path) -> bool:
    """Trim a clip to [start, end] and normalize resolution/fps/codec."""
    # Probe actual resolution to set scale correctly
    probe = _probe_video(clip_path)
    scale_filter = f"scale={probe['width']}:{probe['height']},fps={_NORM_FPS}"

    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start), "-to", str(end),
        "-i", str(clip_path),
        "-vf", scale_filter,
        "-c:v", _NORM_CODEC_V, "-crf", _NORM_CRF, "-preset", _NORM_PRESET,
        "-c:a", _NORM_CODEC_A, "-ar", _NORM_AUDIO_RATE,
        str(output_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


def _generate_flash(
    color: str, width: int, height: int, fps: int, duration: float, output_path: Path
) -> bool:
    """Generate a solid-color flash frame clip for use as a transition."""
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"color={color}:size={width}x{height}:rate={fps}",
        "-f", "lavfi", "-i", "aevalsrc=0:c=stereo:s=44100",
        "-t", str(duration),
        "-c:v", _NORM_CODEC_V, "-crf", _NORM_CRF, "-preset", "ultrafast",
        "-c:a", _NORM_CODEC_A,
        str(output_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _probe_video(path: Path) -> dict:
    """Return width, height, and fps for a video file."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", str(path)],
            capture_output=True, text=True, timeout=15,
        )
        streams = json.loads(result.stdout).get("streams", [])
        vs = next((s for s in streams if s.get("codec_type") == "video"), {})
        fps_raw = vs.get("r_frame_rate", "30/1")
        try:
            n, d = fps_raw.split("/")
            fps = int(round(int(n) / int(d)))
        except (ValueError, ZeroDivisionError):
            fps = 30
        return {"width": int(vs.get("width", 1920)), "height": int(vs.get("height", 1080)), "fps": fps}
    except Exception:
        return {"width": 1920, "height": 1080, "fps": 30}


def _write_montage_meta(
    output_path: Path,
    game: str,
    selected: list[dict],
    total_duration: float,
    mc: dict,
) -> dict:
    manifest = {
        "montage_id":       output_path.stem,
        "game":             game,
        "is_montage":       True,
        "clip_path":        str(output_path),
        "meta_path":        str(output_path.with_suffix(".meta.json")),
        "clip_count":       len(selected),
        "total_duration_seconds": round(total_duration, 2),
        "transition":       mc.get("transition", "flash"),
        "flash_color":      mc.get("flash_color", "white") if mc.get("transition") == "flash" else None,
        "source_clip_ids":  [s["meta"].get("clip_id", s["clip_path"].stem) for s in selected],
        "source_sweat_scores": [round(s["meta"].get("kill_feed", {}).get("sweat_score", 0), 1) for s in selected],
        "assembled_at":     datetime.now().isoformat(timespec="seconds"),
        "review_status":    None,
    }
    output_path.with_suffix(".meta.json").write_text(json.dumps(manifest, indent=2))
    return manifest
