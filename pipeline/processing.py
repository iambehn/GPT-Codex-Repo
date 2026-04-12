"""
Stage 5 — Processing

Applies the template selected by the Decision Engine to a source clip using
FFmpeg. All operations are performed via subprocess calls — no Python video
libraries. Output files land in processing/{game}/.

Operations implemented (driven by template fields):
  - Duration:   trim_end (trim -c copy then re-encode) | speed_ramp (setpts + atempo)
  - Vertical fill: blur_pillarbox (split bg+fg composite) | center_crop
  - Zoom:       punch_in (static crop) | slow_push (zoompan) | none
  - Color:      eq filter (brightness / contrast / saturation) | LUT (lut3d)
  - Captions:   subtitles filter from .srt (transcript) or generated (static)
  - Effects:    vignette | film_grain (noise) | chromatic_aberration (rgbashift)
  - Audio:      volume + EBU R128 loudnorm | background music mix (amix)

NOT YET IMPLEMENTED (logs a warning and skips):
  - input_mode: "multi"   — requires multi-clip concatenation (recap_montage)
  - strategy: "hard_cut"  — part of multi-clip path
  - strategy: "pad_silence" — rare edge case

Output naming: processing/{game}/{game}_{YYYYMMDD}_{clip_id}.mp4
"""

import json
import subprocess
import tempfile
from datetime import date
from pathlib import Path

from utils.file_utils import get_game_from_path
from utils.logger import get_logger

logger = get_logger(__name__)

# ASS subtitle alignment codes (numpad layout)
_ASS_ALIGNMENT = {
    "bottom_left": 1,
    "bottom_center": 2,
    "bottom_right": 3,
    "center": 5,
    "top_left": 7,
    "top_center": 8,
    "top_right": 9,
}

_DEFAULT_CRF = 18


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _hex_to_ass(hex_color: str) -> str:
    """Convert #RRGGBB hex to ASS colour &H00BBGGRR."""
    h = hex_color.lstrip("#")
    if len(h) == 8:          # RRGGBBAA
        h = h[:6]
    return f"&H00{h[4:6]}{h[2:4]}{h[0:2]}".upper()


def _atempo_chain(speed: float) -> str:
    """Build a chain of atempo filters covering speed factors outside [0.5, 2.0]."""
    filters: list[str] = []
    remaining = speed
    while remaining > 2.0:
        filters.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        filters.append("atempo=0.5")
        remaining /= 0.5
    filters.append(f"atempo={remaining:.6f}")
    return ",".join(filters)


def _make_static_srt(static_lines: list[dict]) -> str:
    """Convert template static_lines array to SRT text."""
    def ts(s: float) -> str:
        ms = int(round(s * 1000))
        millis = ms % 1000
        total_s = ms // 1000
        return f"{total_s // 3600:02d}:{(total_s % 3600) // 60:02d}:{total_s % 60:02d},{millis:03d}"

    lines = []
    for i, entry in enumerate(static_lines, 1):
        text = entry.get("text", "").strip()
        if not text:
            continue
        end = entry.get("end_seconds") or (entry["start_seconds"] + 3.0)
        lines.append(f"{i}\n{ts(entry['start_seconds'])} --> {ts(end)}\n{text}\n")
    return "\n".join(lines)


def _escape_filter_path(path: str) -> str:
    """Escape a file path for use inside an FFmpeg filter string."""
    return path.replace("\\", "/").replace(":", "\\:").replace("'", "\\'")


# ---------------------------------------------------------------------------
# Filter-graph builders (each returns a list of filter_complex segments)
# ---------------------------------------------------------------------------

def _add_trim(parts: list[str], current: str, target_dur: float,
              actual_dur: float, strategy: str) -> tuple[str, list[str]]:
    """Trim or speed-ramp the video to match target_duration."""
    out = "vtrim"

    if strategy == "trim_end":
        if actual_dur > target_dur:
            parts.append(f"[{current}]trim=end={target_dur},setpts=PTS-STARTPTS[{out}]")
            return out, []
        # Clip is already shorter than target — keep as-is
        return current, []

    if strategy == "speed_ramp" and actual_dur > 0:
        speed = actual_dur / target_dur
        # Clamp to a reasonable range (0.5x – 2.5x) to avoid unnatural results
        speed = max(0.5, min(speed, 2.5))
        pts_factor = round(1.0 / speed, 6)
        parts.append(f"[{current}]setpts={pts_factor}*PTS[{out}]")
        return out, [_atempo_chain(speed)]   # audio speed filters returned separately

    # Fallback: just return unchanged
    return current, []


def _add_vertical_fill(parts: list[str], current: str,
                       vf_cfg: dict, out_w: int, out_h: int) -> str:
    """Apply vertical fill to convert source aspect ratio to output dimensions."""
    method = vf_cfg.get("method", "blur_pillarbox")

    if method == "center_crop":
        out = "vfill"
        parts.append(
            f"[{current}]scale={out_w}:{out_h}:force_original_aspect_ratio=increase,"
            f"crop={out_w}:{out_h}[{out}]"
        )
        return out

    if method == "blur_pillarbox":
        blur = int(vf_cfg.get("blur_strength", 30))
        # Split source into background path and foreground path
        parts.append(f"[{current}]split[bg_in][fg_in]")
        # Background: scale to fill output height, crop to output width, blur
        parts.append(
            f"[bg_in]scale=-1:{out_h},crop={out_w}:{out_h},"
            f"boxblur={blur}:{blur}[bg]"
        )
        # Foreground: scale to fit within output width, preserve aspect ratio
        parts.append(f"[fg_in]scale={out_w}:-2[fg]")
        # Composite: foreground centred over background
        out = "vfill"
        parts.append(f"[bg][fg]overlay=(W-w)/2:(H-h)/2[{out}]")
        return out

    # smart_crop — not yet implemented
    logger.warning(f"vertical_fill method '{method}' not implemented — using center_crop")
    out = "vfill"
    parts.append(
        f"[{current}]scale={out_w}:{out_h}:force_original_aspect_ratio=increase,"
        f"crop={out_w}:{out_h}[{out}]"
    )
    return out


def _add_zoom(parts: list[str], current: str, zoom_cfg: dict,
              out_w: int, out_h: int, fps: int, target_dur: float) -> str:
    """Apply zoom effect."""
    if not zoom_cfg.get("enabled", False):
        return current

    zoom_type = zoom_cfg.get("type", "none")
    intensity = float(zoom_cfg.get("intensity", 1.0))
    duration = float(zoom_cfg.get("duration_seconds", 1.0))
    out = "vzoom"

    if zoom_type == "none" or intensity <= 1.0:
        return current

    if zoom_type == "punch_in":
        # Static crop to simulate a punched-in zoom — fast, no per-frame processing
        crop_w = f"iw/{intensity:.4f}"
        crop_h = f"ih/{intensity:.4f}"
        cx = f"(iw-iw/{intensity:.4f})/2"
        cy = f"(ih-ih/{intensity:.4f})/2"
        parts.append(
            f"[{current}]crop={crop_w}:{crop_h}:{cx}:{cy},"
            f"scale={out_w}:{out_h}[{out}]"
        )
        return out

    if zoom_type == "slow_push":
        # Gradual zoom from 1.0 → intensity over duration_seconds, then hold
        frames = max(1, int(duration * fps))
        increment = round((intensity - 1.0) / frames, 8)
        parts.append(
            f"[{current}]zoompan="
            f"z='min(zoom+{increment},{intensity:.4f})':"
            f"d={int(target_dur * fps)}:"
            f"s={out_w}x{out_h}:"
            f"fps={fps}[{out}]"
        )
        return out

    if zoom_type == "dynamic":
        # Alternating zoom — treat as punch_in for single-clip mode
        crop_w = f"iw/{intensity:.4f}"
        crop_h = f"ih/{intensity:.4f}"
        cx = f"(iw-iw/{intensity:.4f})/2"
        cy = f"(ih-ih/{intensity:.4f})/2"
        parts.append(
            f"[{current}]crop={crop_w}:{crop_h}:{cx}:{cy},"
            f"scale={out_w}:{out_h}[{out}]"
        )
        return out

    return current


def _add_color_grade(parts: list[str], current: str, cg_cfg: dict) -> str:
    """Apply colour grade via FFmpeg eq filter and optional LUT."""
    if not cg_cfg.get("enabled", False):
        return current

    brightness = float(cg_cfg.get("brightness", 0.0))
    # eq filter uses 1.0 as neutral for contrast and saturation
    contrast = 1.0 + float(cg_cfg.get("contrast", 0.0))
    saturation = 1.0 + float(cg_cfg.get("saturation", 0.0))
    lut_path = cg_cfg.get("lut_path")
    out = "vcolor"

    eq_filter = (
        f"eq=brightness={brightness:.4f}:"
        f"contrast={contrast:.4f}:"
        f"saturation={saturation:.4f}"
    )

    if lut_path:
        parts.append(
            f"[{current}]{eq_filter},"
            f"lut3d='{_escape_filter_path(lut_path)}'[{out}]"
        )
    else:
        parts.append(f"[{current}]{eq_filter}[{out}]")

    return out


def _add_captions(parts: list[str], current: str, cap_cfg: dict,
                  srt_path: Path | None, tmp_dir: str) -> str:
    """Burn captions into the video via FFmpeg subtitles filter."""
    if not cap_cfg.get("enabled", False):
        return current

    source = cap_cfg.get("source", "none")
    effective_srt: Path | None = None

    if source == "transcript":
        if srt_path and srt_path.exists() and srt_path.stat().st_size > 0:
            effective_srt = srt_path
        else:
            logger.debug("No SRT file for transcript captions — skipping captions.")
            return current

    elif source == "static":
        static_lines = cap_cfg.get("static_lines", [])
        srt_content = _make_static_srt(static_lines)
        if not srt_content.strip():
            return current
        tmp_srt = Path(tmp_dir) / "static_captions.srt"
        tmp_srt.write_text(srt_content)
        effective_srt = tmp_srt

    else:
        return current  # source == "none"

    # FFmpeg subtitles filter cannot handle spaces in file paths even when quoted.
    # Copy to a guaranteed space-free temp path before building the filter string.
    if " " in str(effective_srt):
        safe_srt = Path(tmp_dir) / "captions.srt"
        safe_srt.write_bytes(effective_srt.read_bytes())
        effective_srt = safe_srt

    style = cap_cfg.get("style", {})
    font_family = style.get("font_family", "Arial")
    font_size = int(style.get("font_size_px", 48))
    font_color = _hex_to_ass(style.get("font_color", "#FFFFFF"))
    stroke_color = _hex_to_ass(style.get("stroke_color", "#000000"))
    stroke_width = int(style.get("stroke_width_px", 2))
    alignment_str = style.get("alignment", "bottom_center")
    alignment_val = _ASS_ALIGNMENT.get(alignment_str, 2)

    bg = style.get("background", {})
    bg_enabled = bg.get("enabled", False)

    force_style_parts = [
        f"FontName={font_family}",
        f"FontSize={font_size}",
        f"PrimaryColour={font_color}",
        f"OutlineColour={stroke_color}",
        f"Outline={stroke_width}",
        f"Alignment={alignment_val}",
        "MarginV=60",
    ]
    if bg_enabled:
        bg_color = _hex_to_ass(bg.get("color", "#000000CC"))
        force_style_parts += [f"BackColour={bg_color}", "BorderStyle=4"]

    force_style = ",".join(force_style_parts)
    escaped_path = _escape_filter_path(str(effective_srt))
    out = "vcap"

    parts.append(
        f"[{current}]subtitles='{escaped_path}':"
        f"force_style='{force_style}'[{out}]"
    )
    return out


def _add_post_effects(parts: list[str], current: str, vfx: dict) -> str:
    """Apply vignette, film grain, and chromatic aberration."""
    chain: list[str] = []

    vig = vfx.get("vignette", {})
    if vig.get("enabled") and float(vig.get("intensity", 0)) > 0:
        angle = round(3.14159 / 4 * float(vig["intensity"]), 4)
        chain.append(f"vignette=angle={angle}")

    grain = vfx.get("film_grain", {})
    if grain.get("enabled") and float(grain.get("intensity", 0)) > 0:
        strength = max(1, int(float(grain["intensity"]) * 60))
        chain.append(f"noise=c0s={strength}:c0f=t+u")

    ca = vfx.get("chromatic_aberration", {})
    if ca.get("enabled") and float(ca.get("intensity", 0)) > 0:
        shift = max(1, int(float(ca["intensity"]) * 5))
        chain.append(f"rgbashift=rh={shift}:bh=-{shift}")

    if not chain:
        return current

    out = "vfx"
    parts.append(f"[{current}]{','.join(chain)}[{out}]")
    return out


# ---------------------------------------------------------------------------
# Full command builder
# ---------------------------------------------------------------------------

def _build_ffmpeg_cmd(
    clip_path: Path,
    output_path: Path,
    template: dict,
    metadata: dict,
    tmp_dir: str,
) -> list[str]:
    """Assemble the complete FFmpeg command for a single-clip template."""

    tl = template["timeline"]
    vfx = template["visual_effects"]
    cap_cfg = template["captions"]
    audio_cfg = template["audio"]
    out_cfg = template["output"]

    out_w = out_cfg["resolution"]["width"]
    out_h = out_cfg["resolution"]["height"]
    out_fps = int(out_cfg.get("fps", 30))
    bitrate = int(out_cfg.get("bitrate_kbps", 6000))
    codec = out_cfg.get("codec", "h264")
    codec_lib = "libx264" if codec == "h264" else ("libx265" if codec == "h265" else "libvpx-vp9")

    target_dur = float(tl["target_duration_seconds"])
    actual_dur = float(metadata.get("duration_seconds", target_dur))
    strategy = tl.get("target_duration_strategy", "trim_end")
    has_audio = metadata.get("has_audio", True)

    srt_path = Path(metadata["srt_path"]) if metadata.get("srt_path") else None

    # ---- Build video filter graph ----------------------------------------
    video_parts: list[str] = []
    current = "0:v"
    audio_speed_filters: list[str] = []

    current, audio_speed_filters = _add_trim(
        video_parts, current, target_dur, actual_dur, strategy
    )
    current = _add_vertical_fill(video_parts, current, vfx["vertical_fill"], out_w, out_h)
    current = _add_zoom(video_parts, current, vfx["zoom"], out_w, out_h, out_fps, target_dur)
    current = _add_color_grade(video_parts, current, vfx["color_grade"])
    current = _add_captions(video_parts, current, cap_cfg, srt_path, tmp_dir)
    current = _add_post_effects(video_parts, current, vfx)
    final_video_label = current

    # ---- Build audio filter graph ----------------------------------------
    audio_parts: list[str] = []
    final_audio_label: str | None = None

    if has_audio:
        orig_audio = audio_cfg.get("original_audio", {})
        vol_db = float(orig_audio.get("volume_db", 0.0))
        normalize = orig_audio.get("normalize", True)

        a_filters: list[str] = []
        if vol_db != 0.0:
            a_filters.append(f"volume={vol_db:.1f}dB")
        if normalize:
            a_filters.append("loudnorm=I=-14:TP=-1.5:LRA=11")
        if audio_speed_filters:
            a_filters.append(audio_speed_filters[0] if len(audio_speed_filters) == 1
                              else ",".join(audio_speed_filters))

        bg_music = audio_cfg.get("background_music", {})
        if bg_music.get("enabled") and bg_music.get("asset_path"):
            # Background music mix — two audio inputs
            music_vol = float(bg_music.get("volume_db", -16.0))
            a_filters_str = ",".join(a_filters) if a_filters else "anull"
            audio_parts.append(f"[0:a]{a_filters_str}[a_orig]")
            audio_parts.append(f"[1:a]volume={music_vol:.1f}dB[a_music]")
            audio_parts.append("[a_orig][a_music]amix=inputs=2:duration=first[aout]")
            final_audio_label = "aout"
        else:
            if a_filters:
                audio_parts.append(f"[0:a]{','.join(a_filters)}[aout]")
                final_audio_label = "aout"
            # else: no audio processing — map 0:a directly

    # ---- Assemble filter_complex string ----------------------------------
    all_parts = video_parts + audio_parts
    filter_complex_str = ";".join(all_parts)

    # ---- Build command ---------------------------------------------------
    cmd = ["ffmpeg", "-y", "-hide_banner"]

    # Inputs
    cmd += ["-i", str(clip_path)]
    bg_music = audio_cfg.get("background_music", {})
    if bg_music.get("enabled") and bg_music.get("asset_path"):
        cmd += ["-i", bg_music["asset_path"]]

    # Filter complex
    if filter_complex_str:
        cmd += ["-filter_complex", filter_complex_str]

    # Video output mapping
    cmd += ["-map", f"[{final_video_label}]" if final_video_label != "0:v" else "0:v"]

    # Audio output mapping
    if has_audio:
        if final_audio_label:
            cmd += ["-map", f"[{final_audio_label}]"]
        elif not audio_parts:
            cmd += ["-map", "0:a"]
    else:
        cmd += ["-an"]

    # Encoding settings
    cmd += [
        "-c:v", codec_lib,
        "-crf", str(_DEFAULT_CRF),
        "-maxrate", f"{bitrate}k",
        "-bufsize", f"{bitrate * 2}k",
        "-preset", "fast",
        "-r", str(out_fps),
        "-pix_fmt", "yuv420p",
    ]
    if has_audio:
        cmd += ["-c:a", "aac", "-b:a", "192k", "-ar", "44100"]

    cmd.append(str(output_path))
    return cmd


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_processing(clip_path: str, template: dict, metadata: dict, config: dict) -> str:
    """Apply a template to a source clip and produce the processed output file.

    Writes output to processing/{game}/{game}_{YYYYMMDD}_{clip_id}.mp4.
    Updates .meta.json with processed_path.
    Idempotent: skips if output file already exists.

    Args:
        clip_path: Path to the source clip in inbox/{game}/.
        template:  Full template dict from select_template.
        metadata:  Feature dict from run_feature_extraction (with template fields).
        config:    Full parsed config.yaml dict.

    Returns:
        Path string of the processed output file, or empty string on failure.
    """
    clip = Path(clip_path)
    template_id = template.get("template_id", "unknown")

    # Reject multi-clip templates (not yet implemented)
    if template.get("timeline", {}).get("input_mode") == "multi":
        logger.warning(
            f"Template '{template_id}' uses input_mode='multi' which is not yet "
            f"implemented. Skipping processing for {clip.name}."
        )
        return ""

    game = metadata.get("game") or get_game_from_path(clip) or "unknown"
    clip_id = metadata.get("clip_id", clip.stem)
    today = date.today().strftime("%Y%m%d")

    output_dir = Path(config["paths"]["processing"]) / game
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{game}_{today}_{clip_id}.mp4"

    # Idempotency: skip if already processed
    if output_path.exists():
        logger.debug(f"Already processed: {output_path.name} — skipping.")
        return str(output_path)

    logger.info(
        f"Processing [{template_id}] → {output_path.name} "
        f"({metadata.get('duration_seconds', '?')}s source, "
        f"{template['timeline']['target_duration_seconds']}s target)"
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            cmd = _build_ffmpeg_cmd(clip, output_path, template, metadata, tmp_dir)
        except Exception as e:
            logger.error(f"Failed to build FFmpeg command for {clip.name}: {e}")
            return ""

        logger.debug(f"FFmpeg command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 min max for long clips
        )

    if result.returncode != 0:
        logger.error(
            f"FFmpeg failed for {clip.name} (exit {result.returncode}):\n"
            f"{result.stderr[-2000:]}"   # last 2000 chars of stderr
        )
        return ""

    if not output_path.exists():
        logger.error(f"FFmpeg exited 0 but output not found: {output_path}")
        return ""

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"Processed: {output_path.name} ({size_mb:.1f} MB)")

    # Persist output path into the clip's .meta.json
    meta_path = clip.with_suffix(".meta.json")
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        meta["processed_path"] = str(output_path)
        meta_path.write_text(json.dumps(meta, indent=2))

    return str(output_path)
