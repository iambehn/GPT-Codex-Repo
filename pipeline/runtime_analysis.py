from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from pipeline.event_mapper import EventMapperError, load_matcher_report, map_matcher_result, write_event_debug_bundle
from pipeline.game_pack import load_game_pack
from pipeline.roi_matcher import validate_published_pack
from pipeline.roi_matcher import RoiMatcherError, match_roi_templates


REPO_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "runtime_analysis"


class RuntimeAnalysisError(RuntimeError):
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


def analyze_roi_runtime(
    source: str | Path,
    game: str,
    *,
    matcher_report: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    template_overrides: dict[str, dict[str, Any]] | None = None,
    runtime_rule_overrides: dict[str, dict[str, Any]] | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    try:
        game_pack = load_game_pack(game)
    except FileNotFoundError as exc:
        raise RuntimeAnalysisError("missing_game_pack", str(exc)) from exc
    if game_pack.pack_format != "published":
        raise RuntimeAnalysisError("published_pack_required", f"game pack '{game}' is not a published runtime pack")

    if matcher_report is not None:
        matcher_result = load_matcher_report(matcher_report)
    else:
        matcher_result = match_roi_templates(
            source,
            game,
            sample_fps=sample_fps,
            limit_frames=limit_frames,
            template_overrides=template_overrides,
            debug_output_dir=debug_output_dir,
        )

    event_result = map_matcher_result(
        game,
        matcher_result,
        fallback_source=source,
        runtime_rule_overrides=runtime_rule_overrides,
    )
    if debug_output_dir is not None:
        write_event_debug_bundle(debug_output_dir, event_result)

    sidecar_path = _runtime_analysis_path(source, game, output_path)
    analysis_id = _analysis_id(game, source)
    payload = {
        "schema_version": RUNTIME_ANALYSIS_SCHEMA_VERSION,
        "analysis_id": analysis_id,
        "ok": bool(matcher_result.get("ok", False)) and bool(event_result.get("ok", False)),
        "status": str(event_result.get("status", matcher_result.get("status", "ok"))),
        "game": game,
        "source": str(matcher_result.get("source") or source),
        "sidecar_path": str(sidecar_path),
        "game_pack": game_pack.summary(),
        "contract_summary": _runtime_contract_summary(game),
        "matcher": {
            "status": matcher_result.get("status"),
            "frame_count": int(matcher_result.get("frame_count", 0) or 0),
            "sample_fps": float(matcher_result.get("sample_fps", 0.0) or 0.0),
            "template_count": int(matcher_result.get("template_count", 0) or 0),
            "summary": matcher_result.get("summary", {}),
            "top_scores": matcher_result.get("top_scores", {}),
            "unseen_templates": matcher_result.get("unseen_templates", []),
            "confirmed_detections": matcher_result.get("confirmed_detections", []),
            "signals": event_result.get("signals", []),
        },
        "events": {
            "status": event_result.get("status"),
            "signal_count": int(event_result.get("signal_count", 0) or 0),
            "event_count": int(event_result.get("event_count", 0) or 0),
            "event_summary": event_result.get("event_summary", {}),
            "rows": event_result.get("events", []),
        },
    }
    try:
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:
        raise RuntimeAnalysisError("sidecar_write_failed", str(exc)) from exc
    return payload


def _runtime_contract_summary(game: str) -> dict[str, Any]:
    try:
        validation = validate_published_pack(game)
    except RoiMatcherError as exc:
        return {
            "status": "missing",
            "error": exc.message,
            "active_legacy_modes": [],
        }
    return {
        "status": validation.get("contract_status", "canonical"),
        "active_legacy_modes": validation.get("active_legacy_modes", []),
        "canonical_contracts": validation.get("canonical_contracts", {}),
    }


def _runtime_analysis_path(source: str | Path, game: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        path = Path(output_path).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        else:
            path = path.resolve()
        return path

    source_slug = _source_slug(source)
    source_hash = hashlib.sha1(str(source).encode("utf-8")).hexdigest()[:12]
    filename = f"{source_slug}-{source_hash}.runtime_analysis.json"
    return DEFAULT_OUTPUT_ROOT / game / filename


def _source_slug(source: str | Path) -> str:
    source_text = str(source)
    stem = Path(source_text).stem
    if "://" in source_text:
        path_part = source_text.split("://", 1)[1].split("?", 1)[0]
        stem = Path(path_part).stem or stem
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", stem.lower()).strip("-")
    return slug or "analysis"


def _analysis_id(game: str, source: str | Path) -> str:
    digest = hashlib.sha1(f"{game}\n{source}".encode("utf-8")).hexdigest()[:12]
    return f"{game}-runtime-{digest}"
