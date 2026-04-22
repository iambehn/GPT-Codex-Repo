from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.game_pack import load_game_pack
from utils.logger import get_logger

logger = get_logger(__name__)


def run_niceshot_detector(
    clip_path: str | Path,
    game: str,
    config: dict,
    game_pack: dict | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Adapter contract for NiceShot-style candidate moment extraction.

    V1 intentionally supports only stub and fixture JSON modes because the
    project does not yet have confirmed NiceShot API/CLI details.
    """
    clip = Path(clip_path)
    meta_path = clip.with_suffix(".meta.json")

    if meta_path.exists() and not force:
        existing = _load_meta(meta_path)
        if "niceshot_detection" in existing:
            logger.debug(f"[niceshot_detector] Already processed: {clip.name}")
            return existing["niceshot_detection"]

    game_pack = game_pack or load_game_pack(game, config)
    cfg = _merged_config(game, config, game_pack)
    if not cfg.get("enabled", False):
        return _write_and_return(meta_path, _base_result(cfg, "disabled"))

    mode = str(cfg.get("mode") or "stub").strip().lower()
    try:
        if mode == "stub":
            result = _stub_result(cfg)
        elif mode == "fixture_json":
            result = _fixture_result(clip, cfg)
        elif mode in {"cli", "api"}:
            result = _base_result(cfg, "not_configured", f"{mode} mode is reserved for a future NiceShot integration")
        else:
            result = _base_result(cfg, "not_configured", f"unsupported NiceShot mode '{mode}'")
    except Exception as e:
        logger.warning(f"[niceshot_detector] Failed for {clip.name}: {e}")
        result = _base_result(cfg, "error", str(e))

    return _write_and_return(meta_path, result)


def _merged_config(game: str, config: dict, game_pack: dict) -> dict:
    global_cfg = dict(config.get("niceshot_detector") or {})
    game_cfg = dict((((game_pack.get("game") or {}).get("detectors") or {}).get("niceshot")) or {})
    merged = {
        "enabled": bool(global_cfg.get("enabled", False)),
        "provider": global_cfg.get("provider", "niceshot_ai"),
        "profile": global_cfg.get("profile", "cod_like_default"),
        "mode": global_cfg.get("mode", "stub"),
        "fixture_dir": global_cfg.get("fixture_dir"),
        "fixture_path": global_cfg.get("fixture_path"),
        "stub": dict(global_cfg.get("stub") or {}),
    }
    merged.update(game_cfg)
    merged["enabled"] = bool(global_cfg.get("enabled", False)) and bool(game_cfg.get("enabled", False))
    merged["game"] = game
    return merged


def _stub_result(cfg: dict) -> dict:
    stub = cfg.get("stub") or {}
    result = _base_result(cfg, "ok")
    result.update({
        "mode": "stub",
        "action_score": _clamp(stub.get("action_score", 0.0)),
        "hook_score": _clamp(stub.get("hook_score", 0.0)),
        "confidence": _clamp(stub.get("confidence", 0.0)),
        "moments": _normalize_moments(stub.get("moments", []), source="niceshot"),
    })
    return result


def _fixture_result(clip: Path, cfg: dict) -> dict:
    fixture_path = _resolve_fixture_path(clip, cfg)
    if fixture_path is None or not fixture_path.exists():
        return _base_result(cfg, "not_configured", "fixture JSON not found")

    try:
        payload = json.loads(fixture_path.read_text())
    except json.JSONDecodeError as e:
        return _base_result(cfg, "error", f"fixture JSON is malformed: {e}")

    result = _base_result(cfg, str(payload.get("status", "ok")))
    result.update({
        "mode": "fixture_json",
        "fixture_path": str(fixture_path),
        "action_score": _clamp(payload.get("action_score", 0.0)),
        "hook_score": _clamp(payload.get("hook_score", 0.0)),
        "confidence": _clamp(payload.get("confidence", 0.0)),
        "moments": _normalize_moments(payload.get("moments", []), source="niceshot"),
    })
    return result


def _resolve_fixture_path(clip: Path, cfg: dict) -> Path | None:
    if cfg.get("fixture_path"):
        return Path(str(cfg["fixture_path"])).expanduser().resolve()
    if cfg.get("fixture_dir"):
        return (Path(str(cfg["fixture_dir"])).expanduser().resolve() / f"{clip.stem}.json")
    return None


def _normalize_moments(raw_moments: Any, source: str) -> list[dict[str, Any]]:
    moments: list[dict[str, Any]] = []
    if not isinstance(raw_moments, list):
        return moments

    for item in raw_moments:
        if not isinstance(item, dict):
            continue
        try:
            timestamp = round(float(item.get("timestamp", item.get("time", 0.0))), 3)
        except (TypeError, ValueError):
            timestamp = 0.0
        moments.append({
            "timestamp": timestamp,
            "source": str(item.get("source", source)),
            "kind": str(item.get("kind", item.get("label", "action_spike"))),
            "confidence": _clamp(item.get("confidence", item.get("score", 0.0))),
            "hook_candidate": bool(item.get("hook_candidate", True)),
        })
    return moments


def _base_result(cfg: dict, status: str, reason: str | None = None) -> dict:
    result = {
        "enabled": bool(cfg.get("enabled", False)),
        "status": status,
        "provider": cfg.get("provider", "niceshot_ai"),
        "profile": cfg.get("profile", "cod_like_default"),
        "mode": cfg.get("mode", "stub"),
        "action_score": 0.0,
        "hook_score": 0.0,
        "confidence": 0.0,
        "moments": [],
    }
    if reason:
        result["reason"] = reason
    return result


def _write_and_return(meta_path: Path, result: dict) -> dict:
    meta = _load_meta(meta_path)
    meta["niceshot_detection"] = result
    meta_path.write_text(json.dumps(meta, indent=2))
    return result


def _load_meta(meta_path: Path) -> dict:
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _clamp(value: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 3)
    except (TypeError, ValueError):
        return 0.0
