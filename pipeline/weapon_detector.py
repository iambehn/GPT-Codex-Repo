"""
Weapon Detector — HUD icon recognition via OpenCV matchTemplate

Identifies the active weapon by matching the weapon-icon ROI against a
library of 64×64 PNG reference images stored in:
    assets/weapon_icons/{game}/{weapon_id}.png

Resolution normalization: every frame is resized to 1920×1080 before the
ROI crop, keeping config coordinates stable across stream resolutions.

Config block (config.yaml → weapon_detector):
    enabled: false
    confidence_threshold: 0.80
    frame_sample: "middle"      # "middle" | "kill_timestamps" | "all"
    icon_dir: "assets/weapon_icons"
    games:
      deadlock:
        roi: {x: 1600, y: 950, w: 150, h: 80}
        weapons:
          sniper_rifle: "Sniper Rifle"   # weapon_id: display_name
          smg_01: "SMG"

Building the icon library:
    1. Run a clip through the pipeline with kill_feed.enabled: true so
       kill_timestamps are written to meta.json.
    2. Use frame_sample: "kill_timestamps" to land on action frames.
    3. Manually crop the weapon icon area from a frame and save it as:
           assets/weapon_icons/{game}/{weapon_id}.png
    4. Add the weapon_id → display_name mapping to config.yaml.
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.game_pack import get_weapon_detector_game_config, load_game_pack, resolve_asset_path
from utils.logger import get_logger

logger = get_logger(__name__)

try:
    import cv2
    import numpy as np
    _CV2_AVAILABLE = True
except ImportError:
    _CV2_AVAILABLE = False
    logger.warning(
        "OpenCV not installed — Weapon Detector will be skipped.\n"
        "  Fix: pip install opencv-python-headless"
    )

TARGET_WIDTH = 1920
TARGET_HEIGHT = 1080
_DEFAULT_CONFIDENCE = 0.80
_DEFAULT_ICON_DIR = "assets/weapon_icons"
<<<<<<< HEAD
_DEFAULT_TEMPLATE_SCALES = [0.9, 1.0, 1.1]
_CANDIDATE_LIMIT = 5
=======
>>>>>>> origin/main


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_weapon_detector(clip_path: Path, game: str, config: dict, force: bool = False) -> dict:
    """Detect the active weapon from the HUD weapon-icon ROI.

    Idempotent: skips if meta.json already contains a weapon_detection key.

    Returns:
        {
            'weapon_id':    str | None,
            'display_name': str | None,
            'confidence':   float,
            'method':       str,    # "template_match" | "no_match" | "disabled"
            'frame_time':   float,  # seconds into clip where the best frame was taken
        }
    """
    wd_cfg = config.get("weapon_detector", {})
    meta_path = clip_path.with_suffix(".meta.json")

    # Idempotency
    if meta_path.exists() and not force:
        try:
            existing = json.loads(meta_path.read_text())
            if "weapon_detection" in existing:
                logger.debug(f"[weapon_detector] Already processed: {clip_path.name}")
                return existing["weapon_detection"]
        except (json.JSONDecodeError, OSError):
            pass

    if not _CV2_AVAILABLE:
        return _write_and_return(meta_path, _disabled("opencv not installed"))

    if not wd_cfg.get("enabled", False):
        return _write_and_return(meta_path, _disabled("weapon_detector disabled"))

    game_pack = load_game_pack(game, config)
    game_cfg = get_weapon_detector_game_config(game, config, game_pack)
    if not game_cfg:
        return _write_and_return(meta_path, _disabled(f"no config for game '{game}'"))

    icon_dir = resolve_asset_path(
        game_cfg.get("icon_dir") or str(Path(wd_cfg.get("icon_dir", _DEFAULT_ICON_DIR)) / game),
        Path(game_pack.get("pack_root", ".")),
    )
    weapon_names = game_cfg.get("weapons", {})
<<<<<<< HEAD
    match_mode = game_cfg.get("match_mode") or wd_cfg.get("match_mode", "color")
    template_scales = _template_scales(game_cfg, wd_cfg)
    templates = _load_templates(icon_dir, weapon_names)
=======
    match_mode = wd_cfg.get("match_mode", "color")   # "color" | "grayscale"
    templates = _load_templates(icon_dir, weapon_names, match_mode)
>>>>>>> origin/main

    if not templates:
        logger.debug(f"[weapon_detector] No icons in {icon_dir} — skipping.")
        return _write_and_return(meta_path, _disabled("no weapon icon assets"))

    roi = game_cfg.get("roi", {})
    threshold = float(wd_cfg.get("confidence_threshold", _DEFAULT_CONFIDENCE))
    frame_sample = wd_cfg.get("frame_sample", "middle")

    # Optionally align with kill-feed event timestamps
    kill_timestamps: list[float] = []
    if frame_sample == "kill_timestamps" and meta_path.exists():
        try:
            existing = json.loads(meta_path.read_text())
            kf = existing.get("kill_feed", {})
            kill_timestamps = kf.get("kill_timestamps", []) + kf.get("headshot_timestamps", [])
        except (json.JSONDecodeError, OSError):
            pass

<<<<<<< HEAD
    result = _detect(clip_path, roi, templates, threshold, frame_sample, kill_timestamps, match_mode, template_scales)
=======
    result = _detect(clip_path, roi, templates, threshold, frame_sample, kill_timestamps, match_mode)
>>>>>>> origin/main
    _write_and_return(meta_path, result)

    if result["weapon_id"]:
        logger.info(
            f"[weapon_detector] {clip_path.name}: '{result['display_name']}' "
            f"(confidence={result['confidence']:.2f}, t={result['frame_time']}s)"
        )
    else:
        logger.debug(f"[weapon_detector] No weapon matched in {clip_path.name}.")

    return result


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def _detect(
    clip_path: Path,
    roi: dict,
    templates: list[dict],
    threshold: float,
    frame_sample: str,
    kill_timestamps: list[float],
    match_mode: str = "color",
<<<<<<< HEAD
    template_scales: list[float] | None = None,
=======
>>>>>>> origin/main
) -> dict:
    """Open the clip, extract frames, and return the best template match."""
    cap = cv2.VideoCapture(str(clip_path))
    if not cap.isOpened():
        return _disabled(f"could not open {clip_path.name}")

    try:
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 1
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        duration = total_frames / fps

        if frame_sample == "kill_timestamps" and kill_timestamps:
            sample_times = sorted(set(kill_timestamps))
        elif frame_sample == "all":
            sample_times = list(range(0, max(1, int(duration)), 2))
        else:
            sample_times = [duration / 2]

        rx = roi.get("x", 0)
        ry = roi.get("y", 0)
        rw = roi.get("w", 150)
        rh = roi.get("h", 80)

        best_weapon: dict | None = None
<<<<<<< HEAD
        best_conf = -1.0
        best_time = 0.0
        best_match_box: dict | None = None
        best_variant = None
        best_scale = None
        frame_observations: list[dict] = []
        top_candidates: dict[str, dict] = {}
=======
        best_conf = 0.0
        best_time = 0.0
>>>>>>> origin/main

        for t in sample_times:
            cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
            ok, frame = cap.read()
            if not ok:
                continue

            h, w = frame.shape[:2]
            if w != TARGET_WIDTH or h != TARGET_HEIGHT:
                frame = cv2.resize(frame, (TARGET_WIDTH, TARGET_HEIGHT), interpolation=cv2.INTER_LINEAR)

            roi_bgr = frame[ry:ry + rh, rx:rx + rw]
            if roi_bgr.size == 0:
                continue

<<<<<<< HEAD
            search_variants = _search_variants(roi_bgr)

            frame_best_weapon: dict | None = None
            frame_best_conf = -1.0
            frame_best_box: dict | None = None
            frame_best_variant: str | None = None
            frame_best_scale = 1.0

            for tmpl in templates:
                match = _best_template_match(search_variants, tmpl, match_mode, template_scales or _DEFAULT_TEMPLATE_SCALES)
                if match is None:
                    continue
                match_box = {
                    "x": int(rx + match["x"]),
                    "y": int(ry + match["y"]),
                    "w": int(match["w"]),
                    "h": int(match["h"]),
                    "base_width": TARGET_WIDTH,
                    "base_height": TARGET_HEIGHT,
                }
                max_val = float(match["confidence"])
                if max_val > frame_best_conf:
                    frame_best_conf = max_val
                    frame_best_weapon = tmpl
                    frame_best_box = match_box
                    frame_best_variant = str(match["variant"])
                    frame_best_scale = float(match["scale"])
=======
            # Grayscale mode: reduces sensitivity to background colour shifts
            # caused by semi-transparent or game-world-tinted HUD elements.
            search_frame = (
                cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)
                if match_mode == "grayscale"
                else roi_bgr
            )

            for tmpl in templates:
                img = tmpl["image"]
                if img.shape[0] > search_frame.shape[0] or img.shape[1] > search_frame.shape[1]:
                    continue
                res = cv2.matchTemplate(search_frame, img, cv2.TM_CCOEFF_NORMED)
                _, max_val, _, _ = cv2.minMaxLoc(res)
>>>>>>> origin/main
                if max_val > best_conf:
                    best_conf = max_val
                    best_weapon = tmpl
                    best_time = t
<<<<<<< HEAD
                    best_match_box = match_box
                    best_variant = str(match["variant"])
                    best_scale = float(match["scale"])

                candidate = top_candidates.get(tmpl["weapon_id"])
                candidate_payload = {
                    "weapon_id": tmpl["weapon_id"],
                    "display_name": tmpl["display_name"],
                    "confidence": round(max_val, 3),
                    "match_variant": str(match["variant"]),
                    "match_scale": round(float(match["scale"]), 3),
                    "match_box": match_box,
                }
                if candidate is None or float(candidate.get("confidence", 0.0)) < max_val:
                    top_candidates[tmpl["weapon_id"]] = candidate_payload

            if frame_best_weapon:
                frame_observations.append({
                    "timestamp": round(float(t), 3),
                    "weapon_id": frame_best_weapon["weapon_id"],
                    "display_name": frame_best_weapon["display_name"],
                    "confidence": round(float(max(frame_best_conf, 0.0)), 3),
                    "match_variant": frame_best_variant,
                    "match_scale": round(float(frame_best_scale), 3),
                    "match_box": frame_best_box,
                })

        debug_fields = {
            "roi": {
                "x": int(rx),
                "y": int(ry),
                "w": int(rw),
                "h": int(rh),
                "base_width": TARGET_WIDTH,
                "base_height": TARGET_HEIGHT,
            },
            "sample_times": [round(float(ts), 3) for ts in sample_times],
            "frame_observations": frame_observations,
            "best_match_box": best_match_box,
            "best_match_variant": best_variant,
            "best_match_scale": round(float(best_scale), 3) if best_scale is not None else None,
            "top_candidates": sorted(
                top_candidates.values(),
                key=lambda item: float(item.get("confidence", 0.0)),
                reverse=True,
            )[:_CANDIDATE_LIMIT],
        }
=======
>>>>>>> origin/main

        if best_weapon and best_conf >= threshold:
            return {
                "weapon_id":    best_weapon["weapon_id"],
                "display_name": best_weapon["display_name"],
                "confidence":   round(best_conf, 3),
                "method":       "template_match",
                "frame_time":   round(best_time, 2),
<<<<<<< HEAD
                **debug_fields,
=======
>>>>>>> origin/main
            }

        return {
            "weapon_id":    None,
            "display_name": None,
<<<<<<< HEAD
            "confidence":   round(max(best_conf, 0.0), 3),
            "method":       "no_match",
            "frame_time":   round(best_time, 2),
            **debug_fields,
=======
            "confidence":   round(best_conf, 3),
            "method":       "no_match",
            "frame_time":   round(best_time, 2),
>>>>>>> origin/main
        }
    finally:
        cap.release()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

<<<<<<< HEAD
def _load_templates(icon_dir: Path, weapon_name_map: dict) -> list[dict]:
    """Load weapon PNG reference images from the icon directory.

    Templates are normalized into reusable color / grayscale / edge variants
    so the detector can switch matching strategies per game without reloading
    the icon library.
    """
=======
def _load_templates(icon_dir: Path, weapon_name_map: dict, match_mode: str = "color") -> list[dict]:
    """Load weapon PNG reference images from the icon directory.

    When match_mode is "grayscale", templates are loaded as single-channel
    images to match the grayscale ROI crop used during detection.
    """
    read_flag = cv2.IMREAD_GRAYSCALE if match_mode == "grayscale" else cv2.IMREAD_COLOR
>>>>>>> origin/main
    templates = []
    if not icon_dir.exists():
        return templates
    for ext in ("*.png", "*.jpg", "*.jpeg"):
        for path in sorted(icon_dir.glob(ext)):
<<<<<<< HEAD
            img = cv2.imread(str(path), cv2.IMREAD_UNCHANGED)
            if img is None:
                continue
            variants = _template_variants(img)
            if not variants:
                continue
            weapon_id = path.stem
            display_name = weapon_name_map.get(weapon_id, weapon_id.replace("_", " ").title())
            templates.append({
                "weapon_id": weapon_id,
                "display_name": display_name,
                "variants": variants,
            })
            logger.debug(f"[weapon_detector] Loaded template variants: {path.name} → '{display_name}'")
    return templates


def _template_scales(game_cfg: dict, wd_cfg: dict) -> list[float]:
    raw = game_cfg.get("template_scales")
    if raw is None:
        raw = wd_cfg.get("template_scales", _DEFAULT_TEMPLATE_SCALES)
    if isinstance(raw, (int, float)):
        raw = [raw]
    if not isinstance(raw, list):
        return list(_DEFAULT_TEMPLATE_SCALES)
    scales: list[float] = []
    seen: set[float] = set()
    for value in raw:
        try:
            scale = round(float(value), 3)
        except (TypeError, ValueError):
            continue
        if scale <= 0:
            continue
        if scale in seen:
            continue
        seen.add(scale)
        scales.append(scale)
    return scales or list(_DEFAULT_TEMPLATE_SCALES)


def _template_variants(image) -> dict[str, np.ndarray]:
    if image.ndim == 2:
        color = cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
        alpha = None
    elif image.shape[2] == 4:
        alpha = image[:, :, 3]
        color = image[:, :, :3]
    else:
        alpha = None
        color = image[:, :, :3]

    color = _trim_transparent_padding(color, alpha)
    if color.size == 0:
        return {}
    grayscale = cv2.cvtColor(color, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(grayscale, 40, 120)
    return {
        "color": color,
        "grayscale": grayscale,
        "edges": edges,
    }


def _trim_transparent_padding(color: np.ndarray, alpha: np.ndarray | None):
    if alpha is None:
        return color
    ys, xs = np.where(alpha > 0)
    if len(xs) == 0 or len(ys) == 0:
        return color
    x1, x2 = xs.min(), xs.max() + 1
    y1, y2 = ys.min(), ys.max() + 1
    return color[y1:y2, x1:x2]


def _search_variants(roi_bgr: np.ndarray) -> dict[str, np.ndarray]:
    grayscale = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(grayscale, 40, 120)
    return {
        "color": roi_bgr,
        "grayscale": grayscale,
        "edges": edges,
    }


def _match_variants_for_mode(match_mode: str) -> list[str]:
    mode = str(match_mode or "color").strip().lower()
    if mode in {"grayscale", "gray"}:
        return ["grayscale"]
    if mode in {"edge", "edges"}:
        return ["edges"]
    if mode == "hybrid":
        return ["color", "grayscale", "edges"]
    return ["color"]


def _best_template_match(
    search_variants: dict[str, np.ndarray],
    template: dict,
    match_mode: str,
    template_scales: list[float],
) -> dict | None:
    best: dict | None = None
    for variant_name in _match_variants_for_mode(match_mode):
        search_frame = search_variants.get(variant_name)
        template_image = (template.get("variants") or {}).get(variant_name)
        if search_frame is None or template_image is None:
            continue

        for scale in template_scales:
            tmpl = _scaled_template(template_image, scale)
            if tmpl is None:
                continue
            if tmpl.shape[0] > search_frame.shape[0] or tmpl.shape[1] > search_frame.shape[1]:
                continue
            res = cv2.matchTemplate(search_frame, tmpl, cv2.TM_CCOEFF_NORMED)
            _, max_val, _, max_loc = cv2.minMaxLoc(res)
            candidate = {
                "confidence": float(max_val),
                "x": int(max_loc[0]),
                "y": int(max_loc[1]),
                "w": int(tmpl.shape[1]),
                "h": int(tmpl.shape[0]),
                "variant": variant_name,
                "scale": float(scale),
            }
            if best is None or float(candidate["confidence"]) > float(best["confidence"]):
                best = candidate
    return best


def _scaled_template(template_image: np.ndarray, scale: float):
    if abs(scale - 1.0) < 1e-6:
        return template_image
    width = max(1, int(round(template_image.shape[1] * scale)))
    height = max(1, int(round(template_image.shape[0] * scale)))
    if width < 1 or height < 1:
        return None
    interpolation = cv2.INTER_AREA if scale < 1.0 else cv2.INTER_LINEAR
    return cv2.resize(template_image, (width, height), interpolation=interpolation)


=======
            img = cv2.imread(str(path), read_flag)
            if img is None:
                continue
            weapon_id = path.stem
            display_name = weapon_name_map.get(weapon_id, weapon_id.replace("_", " ").title())
            templates.append({"weapon_id": weapon_id, "display_name": display_name, "image": img})
            logger.debug(f"[weapon_detector] Loaded ({match_mode}): {path.name} → '{display_name}'")
    return templates


>>>>>>> origin/main
def _write_and_return(meta_path: Path, result: dict) -> dict:
    try:
        existing = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        existing["weapon_detection"] = result
        meta_path.write_text(json.dumps(existing, indent=2))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"[weapon_detector] Could not write meta: {e}")
    return result


def _disabled(reason: str) -> dict:
    return {
        "weapon_id":    None,
        "display_name": None,
        "confidence":   0.0,
        "method":       "disabled",
        "frame_time":   0.0,
        "reason":       reason,
    }
