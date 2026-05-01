from __future__ import annotations

import csv
import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.game_pack import load_game_pack


class RoiMatcherError(RuntimeError):
    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message

    def to_dict(self, *, game: str | None = None, source: str | Path | None = None) -> dict[str, Any]:
        payload = {
            "ok": False,
            "status": self.status,
            "error": self.message,
        }
        if game is not None:
            payload["game"] = game
        if source is not None:
            payload["source"] = str(source)
        return payload


@dataclass(frozen=True)
class RoiBounds:
    x: int
    y: int
    width: int
    height: int


@dataclass(frozen=True)
class TemplateSpec:
    asset_id: str
    roi_ref: str
    template_path: Path
    mask_path: Path | None
    threshold: float
    scale_set: list[float]
    temporal_window: int
    match_method: str
    asset_family: str


@dataclass(frozen=True)
class FrameBundle:
    frame_index: int
    timestamp: float
    image: Any


def match_roi_templates(
    source: str | Path,
    game: str,
    *,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    min_score: float | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    pack_summary, templates, rois, width, height = _load_published_pack_runtime(game)
    if not templates:
        result = {
            "ok": True,
            "status": "no_templates",
            "game": game,
            "source": str(source),
            "frame_count": 0,
            "sample_fps": sample_fps or 0.0,
            "template_count": 0,
            "detections": [],
            "confirmed_detections": [],
            "summary": _summary_rows([], [], templates),
            "top_scores": {},
            "unseen_templates": [],
        }
        if output_path is not None:
            Path(output_path).write_text(json.dumps(result, indent=2), encoding="utf-8")
        if debug_output_dir is not None:
            _write_debug_bundle(
                debug_output_dir=debug_output_dir,
                result=result,
                pack_summary=pack_summary,
                confirmed_roi_images=[],
            )
        return result

    cv2_module, np_module = _load_cv_runtime()
    fps = float(sample_fps or 4.0)
    frames = _decode_video_frames(source, sample_fps=fps, width=width, height=height, limit_frames=limit_frames, np_module=np_module)

    detections: list[dict[str, Any]] = []
    confirmed_roi_images: list[tuple[str, int, float, Any]] = []
    for frame in frames:
        for template in templates:
            bounds = rois.get(template.roi_ref)
            if bounds is None:
                raise RoiMatcherError("invalid_roi_ref", f"template '{template.asset_id}' references unknown ROI '{template.roi_ref}'")
            roi_image = _crop_roi(frame.image, bounds)
            match = _best_match_for_template(
                roi_image=roi_image,
                template=template,
                cv2_module=cv2_module,
                np_module=np_module,
            )
            if match is None:
                continue
            threshold = max(template.threshold, float(min_score)) if min_score is not None else template.threshold
            if match["score"] < threshold:
                continue
            detections.append(
                {
                    "asset_id": template.asset_id,
                    "timestamp": frame.timestamp,
                    "roi_ref": template.roi_ref,
                    "score": round(float(match["score"]), 5),
                    "threshold": threshold,
                    "match_method": template.match_method,
                    "template_path": str(template.template_path),
                    "frame_index": frame.frame_index,
                    "asset_family": template.asset_family,
                }
            )

    confirmed = _confirm_detections(detections, templates)
    confirmed_keys = {(row["asset_id"], row["first_timestamp"], row["last_timestamp"]) for row in confirmed}
    for cluster in confirmed:
        matching_rows = [
            row for row in detections
            if row["asset_id"] == cluster["asset_id"]
            and row["roi_ref"] == cluster["roi_ref"]
            and cluster["first_timestamp"] <= row["timestamp"] <= cluster["last_timestamp"]
        ]
        if not matching_rows:
            continue
        best_row = max(matching_rows, key=lambda row: float(row["score"]))
        frame = next((item for item in frames if item.frame_index == int(best_row["frame_index"])), None)
        if frame is None:
            continue
        bounds = rois[cluster["roi_ref"]]
        confirmed_roi_images.append((cluster["asset_id"], frame.frame_index, float(best_row["timestamp"]), _crop_roi(frame.image, bounds)))

    top_scores = _top_scores(detections)
    unseen_templates = [template.asset_id for template in templates if template.asset_id not in top_scores]
    result = {
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source),
        "frame_count": len(frames),
        "sample_fps": fps,
        "template_count": len(templates),
        "detections": detections,
        "confirmed_detections": confirmed,
        "summary": _summary_rows(detections, confirmed, templates),
        "top_scores": top_scores,
        "unseen_templates": unseen_templates,
    }
    if output_path is not None:
        Path(output_path).write_text(json.dumps(result, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        _write_debug_bundle(
            debug_output_dir=debug_output_dir,
            result=result,
            pack_summary=pack_summary,
            confirmed_roi_images=confirmed_roi_images,
        )
    return result


def _load_template_specs(pack_root: Path, hud_payload: dict[str, Any], template_payload: dict[str, Any]) -> list[TemplateSpec]:
    rois = hud_payload.get("rois", {})
    raw_templates = template_payload.get("templates", [])
    if raw_templates is None:
        raw_templates = []
    if not isinstance(raw_templates, list):
        raise RoiMatcherError("invalid_template_manifest", "cv_templates.yaml must define a top-level 'templates' list")

    templates: list[TemplateSpec] = []
    for row in raw_templates:
        if not isinstance(row, dict):
            raise RoiMatcherError("invalid_template_manifest", "each template row must be an object")
        roi_ref = str(row.get("roi_ref", "")).strip()
        template_rel = str(row.get("template_path", "")).strip()
        if not roi_ref or not template_rel:
            raise RoiMatcherError("invalid_template_manifest", "template rows must include 'roi_ref' and 'template_path'")
        if roi_ref not in rois:
            raise RoiMatcherError("invalid_roi_ref", f"template '{row.get('asset_id', template_rel)}' references unknown ROI '{roi_ref}'")
        template_path = pack_root / template_rel
        if not template_path.exists():
            raise RoiMatcherError("missing_template_file", f"template file is missing: {template_rel}")
        mask_rel = str(row.get("mask_path", "")).strip()
        mask_path = None
        if mask_rel:
            mask_path = pack_root / mask_rel
            if not mask_path.exists():
                raise RoiMatcherError("missing_mask_file", f"mask file is missing: {mask_rel}")
        templates.append(
            TemplateSpec(
                asset_id=str(row.get("asset_id", template_path.stem)),
                roi_ref=roi_ref,
                template_path=template_path,
                mask_path=mask_path,
                threshold=float(row.get("threshold", 0.9)),
                scale_set=[float(item) for item in row.get("scale_set", [1.0])],
                temporal_window=max(1, int(row.get("temporal_window", 1))),
                match_method=str(row.get("match_method", "TM_CCOEFF_NORMED")),
                asset_family=str(row.get("asset_family", "")),
            )
        )
    return templates


def check_roi_runtime() -> dict[str, Any]:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    checks: dict[str, dict[str, Any]] = {}
    overall_ok = True
    for name in ("cv2", "numpy"):
        try:
            module = __import__(name)
            checks[name] = {
                "ok": True,
                "version": getattr(module, "__version__", "unknown"),
            }
        except Exception as exc:
            overall_ok = False
            checks[name] = {
                "ok": False,
                "error": str(exc),
            }
    ffmpeg_path = Path(ffmpeg)
    checks["ffmpeg"] = {
        "ok": ffmpeg_path.exists(),
        "path": str(ffmpeg_path),
    }
    if not ffmpeg_path.exists():
        overall_ok = False
        checks["ffmpeg"]["error"] = "ffmpeg binary was not found"
    return {
        "ok": overall_ok,
        "status": "ok" if overall_ok else "runtime_missing",
        "checks": checks,
    }


def validate_published_pack(game: str) -> dict[str, Any]:
    pack_summary, templates, rois, width, height = _load_published_pack_runtime(game)
    warnings: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    templates_by_roi: dict[str, int] = {}
    templates_by_asset_family: dict[str, int] = {}
    templates_missing_masks: list[str] = []
    templates_with_large_scale_sets: list[str] = []
    templates_with_roi_fit_warnings: list[dict[str, Any]] = []

    for template in templates:
        templates_by_roi[template.roi_ref] = templates_by_roi.get(template.roi_ref, 0) + 1
        templates_by_asset_family[template.asset_family] = templates_by_asset_family.get(template.asset_family, 0) + 1
        if template.mask_path is None:
            templates_missing_masks.append(template.asset_id)
        if len(template.scale_set) > 3:
            templates_with_large_scale_sets.append(template.asset_id)
        bounds = rois[template.roi_ref]
        max_scale = max(template.scale_set or [1.0])
        template_height, template_width = _template_dimensions(template)
        if int(template_width * max_scale) > bounds.width or int(template_height * max_scale) > bounds.height:
            warning = {
                "asset_id": template.asset_id,
                "roi_ref": template.roi_ref,
                "template_size": [template_width, template_height],
                "max_scale": max_scale,
                "roi_size": [bounds.width, bounds.height],
            }
            templates_with_roi_fit_warnings.append(warning)
            warnings.append({"status": "roi_fit_warning", **warning})

    return {
        "ok": not failures,
        "status": "ok" if not failures else "invalid_published_pack",
        "game": game,
        "pack_summary": pack_summary,
        "template_count": len(templates),
        "roi_count": len(rois),
        "frame_dimensions": {"width": width, "height": height},
        "templates_by_roi": templates_by_roi,
        "templates_by_asset_family": templates_by_asset_family,
        "templates_missing_masks": templates_missing_masks,
        "templates_with_large_scale_sets": templates_with_large_scale_sets,
        "templates_with_roi_fit_warnings": templates_with_roi_fit_warnings,
        "warnings": warnings,
        "failures": failures,
    }


def list_pack_templates(game: str) -> dict[str, Any]:
    pack_summary, templates, _, width, height = _load_published_pack_runtime(game)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for template in templates:
        grouped.setdefault(template.roi_ref, []).append(
            {
                "asset_id": template.asset_id,
                "asset_family": template.asset_family,
                "threshold": template.threshold,
                "scale_set": template.scale_set,
                "temporal_window": template.temporal_window,
                "match_method": template.match_method,
                "has_mask": template.mask_path is not None,
            }
        )
    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "pack_summary": pack_summary,
        "frame_dimensions": {"width": width, "height": height},
        "templates_by_roi": grouped,
    }


def _load_cv_runtime() -> tuple[Any, Any]:
    try:
        import cv2  # type: ignore
    except Exception as exc:
        raise RoiMatcherError("opencv_unavailable", f"OpenCV runtime is unavailable: {exc}") from exc
    try:
        import numpy  # type: ignore
    except Exception as exc:
        raise RoiMatcherError("numpy_unavailable", f"NumPy runtime is unavailable: {exc}") from exc
    return cv2, numpy


def _load_published_pack_runtime(game: str) -> tuple[dict[str, Any], list[TemplateSpec], dict[str, RoiBounds], int, int]:
    game_pack = load_game_pack(game)
    if game_pack.pack_format != "published":
        raise RoiMatcherError("published_pack_required", f"game pack '{game}' is not a published runtime pack")
    hud_payload = game_pack.files.get("hud.yaml", {})
    template_payload = game_pack.files.get("manifests/cv_templates.yaml", {})
    width, height = _target_dimensions(game_pack.files.get("game.yaml", {}))
    rois = _resolve_rois(hud_payload.get("rois", {}), width=width, height=height)
    templates = _load_template_specs(game_pack.root, hud_payload, template_payload)
    pack_summary = {
        "game_root": str(game_pack.root),
        "pack_format": game_pack.pack_format,
        "template_count": len(templates),
        "roi_count": len(rois),
    }
    return pack_summary, templates, rois, width, height


def _target_dimensions(game_payload: dict[str, Any]) -> tuple[int, int]:
    profiles = game_payload.get("resolution_profiles", {})
    normalize_to = str(profiles.get("normalize_to", "")).strip()
    if "x" in normalize_to:
        left, right = normalize_to.lower().split("x", 1)
        try:
            return int(left), int(right)
        except ValueError:
            pass
    return 1920, 1080


def _decode_video_frames(
    source: str | Path,
    *,
    sample_fps: float,
    width: int,
    height: int,
    limit_frames: int | None,
    np_module: Any,
) -> list[FrameBundle]:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    if not Path(ffmpeg).exists():
        raise RoiMatcherError("ffmpeg_unavailable", "ffmpeg is not available for frame decode")
    command = [
        ffmpeg,
        "-v",
        "error",
        "-i",
        str(source),
        "-vf",
        f"fps={sample_fps},scale={width}:{height}:flags=fast_bilinear",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "-",
    ]
    try:
        result = subprocess.run(command, capture_output=True, check=False, timeout=30)
    except subprocess.TimeoutExpired as exc:
        raise RoiMatcherError("decode_timeout", "video frame decode timed out") from exc
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="ignore").strip() or "video frame decode failed"
        raise RoiMatcherError("decode_failed", message)

    bytes_per_frame = width * height * 3
    frame_count = len(result.stdout) // bytes_per_frame
    if limit_frames is not None:
        frame_count = min(frame_count, max(0, int(limit_frames)))
    frames: list[FrameBundle] = []
    for index in range(frame_count):
        start = index * bytes_per_frame
        end = start + bytes_per_frame
        chunk = result.stdout[start:end]
        if len(chunk) != bytes_per_frame:
            continue
        image = np_module.frombuffer(chunk, dtype=np_module.uint8).reshape((height, width, 3))
        frames.append(
            FrameBundle(
                frame_index=index,
                timestamp=index / sample_fps,
                image=image,
            )
        )
    return frames


def _resolve_rois(raw_rois: dict[str, Any], *, width: int, height: int) -> dict[str, RoiBounds]:
    resolved: dict[str, RoiBounds] = {}
    for roi_name, row in raw_rois.items():
        if not isinstance(row, dict):
            continue
        x = max(0, min(width - 1, int(float(row.get("x_pct", 0.0)) * width)))
        y = max(0, min(height - 1, int(float(row.get("y_pct", 0.0)) * height)))
        roi_width = max(1, int(float(row.get("w_pct", 1.0)) * width))
        roi_height = max(1, int(float(row.get("h_pct", 1.0)) * height))
        roi_width = min(roi_width, width - x)
        roi_height = min(roi_height, height - y)
        resolved[str(roi_name)] = RoiBounds(x=x, y=y, width=roi_width, height=roi_height)
    return resolved


def _crop_roi(image: Any, bounds: RoiBounds) -> Any:
    return image[bounds.y : bounds.y + bounds.height, bounds.x : bounds.x + bounds.width]


def _best_match_for_template(*, roi_image: Any, template: TemplateSpec, cv2_module: Any, np_module: Any) -> dict[str, Any] | None:
    template_image = cv2_module.imread(str(template.template_path), cv2_module.IMREAD_UNCHANGED)
    if template_image is None:
        raise RoiMatcherError("template_decode_failed", f"failed to read template image: {template.template_path}")
    mask = None
    if template.mask_path is not None:
        mask = cv2_module.imread(str(template.mask_path), cv2_module.IMREAD_GRAYSCALE)
        if mask is None:
            raise RoiMatcherError("mask_decode_failed", f"failed to read mask image: {template.mask_path}")

    best_score = None
    best_scale = None
    method = getattr(cv2_module, template.match_method, None)
    if method is None:
        raise RoiMatcherError("invalid_match_method", f"unsupported match method: {template.match_method}")

    roi_height, roi_width = roi_image.shape[:2]
    for scale in template.scale_set:
        scaled_template = template_image
        scaled_mask = mask
        if scale != 1.0:
            scaled_template = cv2_module.resize(template_image, None, fx=scale, fy=scale, interpolation=cv2_module.INTER_LINEAR)
            if mask is not None:
                scaled_mask = cv2_module.resize(mask, None, fx=scale, fy=scale, interpolation=cv2_module.INTER_NEAREST)
        template_height, template_width = scaled_template.shape[:2]
        if template_height > roi_height or template_width > roi_width:
            continue
        prepared_roi = roi_image
        prepared_template = scaled_template
        prepared_mask = scaled_mask
        if len(prepared_template.shape) == 2 and len(prepared_roi.shape) == 3:
            prepared_roi = cv2_module.cvtColor(prepared_roi, cv2_module.COLOR_RGB2GRAY)
        elif len(prepared_template.shape) == 3 and len(prepared_roi.shape) == 2:
            prepared_template = cv2_module.cvtColor(prepared_template, cv2_module.COLOR_BGRA2BGR if prepared_template.shape[2] == 4 else cv2_module.COLOR_BGR2RGB)
        if len(prepared_template.shape) == 3 and prepared_template.shape[2] == 4:
            if prepared_mask is None:
                alpha_mask = prepared_template[:, :, 3]
                prepared_mask = alpha_mask
            prepared_template = prepared_template[:, :, :3]
        result = cv2_module.matchTemplate(prepared_roi, prepared_template, method, mask=prepared_mask)
        _, max_val, _, _ = cv2_module.minMaxLoc(result)
        if best_score is None or max_val > best_score:
            best_score = float(max_val)
            best_scale = scale
    if best_score is None:
        return None
    return {"score": best_score, "scale": best_scale}


def _confirm_detections(detections: list[dict[str, Any]], templates: list[TemplateSpec]) -> list[dict[str, Any]]:
    windows = {template.asset_id: template.temporal_window for template in templates}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in detections:
        grouped.setdefault((row["asset_id"], row["roi_ref"]), []).append(row)

    confirmed: list[dict[str, Any]] = []
    for (asset_id, roi_ref), rows in grouped.items():
        sorted_rows = sorted(rows, key=lambda row: int(row["frame_index"]))
        temporal_window = windows.get(asset_id, 1)
        cluster: list[dict[str, Any]] = []
        for row in sorted_rows:
            if not cluster:
                cluster = [row]
                continue
            if int(row["frame_index"]) <= int(cluster[-1]["frame_index"]) + temporal_window:
                cluster.append(row)
            else:
                _append_confirmed_cluster(confirmed, asset_id, roi_ref, temporal_window, cluster)
                cluster = [row]
        _append_confirmed_cluster(confirmed, asset_id, roi_ref, temporal_window, cluster)
    return confirmed


def _append_confirmed_cluster(
    confirmed: list[dict[str, Any]],
    asset_id: str,
    roi_ref: str,
    temporal_window: int,
    cluster: list[dict[str, Any]],
) -> None:
    if len(cluster) < temporal_window:
        return
    confirmed.append(
        {
            "asset_id": asset_id,
            "roi_ref": roi_ref,
            "first_timestamp": cluster[0]["timestamp"],
            "last_timestamp": cluster[-1]["timestamp"],
            "peak_score": max(float(row["score"]) for row in cluster),
            "supporting_frames": len(cluster),
            "temporal_window": temporal_window,
        }
    )


def _summary_rows(detections: list[dict[str, Any]], confirmed: list[dict[str, Any]], templates: list[TemplateSpec]) -> dict[str, Any]:
    detections_by_roi: dict[str, int] = {}
    detections_by_asset_family: dict[str, int] = {}
    template_families = {template.asset_id: template.asset_family for template in templates}
    for row in detections:
        detections_by_roi[row["roi_ref"]] = detections_by_roi.get(row["roi_ref"], 0) + 1
        family = row.get("asset_family") or template_families.get(row["asset_id"], "unknown")
        detections_by_asset_family[family] = detections_by_asset_family.get(family, 0) + 1
    return {
        "total_detections": len(detections),
        "total_confirmed_detections": len(confirmed),
        "unique_assets_detected": len({row["asset_id"] for row in detections}),
        "detections_by_roi": detections_by_roi,
        "detections_by_asset_family": detections_by_asset_family,
    }


def _top_scores(detections: list[dict[str, Any]]) -> dict[str, float]:
    top: dict[str, float] = {}
    for row in detections:
        score = float(row["score"])
        asset_id = row["asset_id"]
        if asset_id not in top or score > top[asset_id]:
            top[asset_id] = score
    return top


def _template_dimensions(template: TemplateSpec) -> tuple[int, int]:
    try:
        from PIL import Image  # type: ignore
    except Exception:
        # Fallback for validation in environments without Pillow: use matcher runtime when available.
        cv2_module, _ = _load_cv_runtime()
        image = cv2_module.imread(str(template.template_path), cv2_module.IMREAD_UNCHANGED)
        if image is None:
            raise RoiMatcherError("template_decode_failed", f"failed to read template image: {template.template_path}")
        return int(image.shape[0]), int(image.shape[1])
    with Image.open(template.template_path) as image:
        width, height = image.size
    return int(height), int(width)


def _write_debug_bundle(
    *,
    debug_output_dir: str | Path,
    result: dict[str, Any],
    pack_summary: dict[str, Any],
    confirmed_roi_images: list[tuple[str, int, float, Any]],
) -> None:
    debug_root = Path(debug_output_dir)
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "match_report.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (debug_root / "pack_summary.json").write_text(json.dumps(pack_summary, indent=2), encoding="utf-8")
    _write_csv(debug_root / "detections.csv", result.get("detections", []))
    _write_csv(debug_root / "confirmed_detections.csv", result.get("confirmed_detections", []))
    if confirmed_roi_images:
        crops_root = debug_root / "confirmed_roi_crops"
        crops_root.mkdir(parents=True, exist_ok=True)
        _write_confirmed_crops(crops_root, confirmed_roi_images)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        if not rows:
            writer.writerow({"empty": ""})
            return
        for row in rows:
            writer.writerow({key: json.dumps(value) if isinstance(value, (list, dict)) else value for key, value in row.items()})


def _write_confirmed_crops(crops_root: Path, confirmed_roi_images: list[tuple[str, int, float, Any]]) -> None:
    cv2_module, _ = _load_cv_runtime()
    for asset_id, frame_index, timestamp, image in confirmed_roi_images:
        safe_asset = asset_id.replace("/", "_").replace("\\", "_")
        filename = f"{safe_asset}-frame{frame_index:05d}-{timestamp:.3f}.png"
        output_path = crops_root / filename
        bgr_image = cv2_module.cvtColor(image, cv2_module.COLOR_BGR2RGB) if getattr(image, "shape", (0, 0, 0))[-1:] == (3,) else image
        cv2_module.imwrite(str(output_path), bgr_image)
