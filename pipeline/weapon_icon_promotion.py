from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline.game_pack import get_weapon_detector_game_config, load_game_pack, resolve_asset_path
from pipeline.weapon_detector_audit import get_weapon_detector_report_dir


def promote_weapon_audit_crop(
    game: str,
    config: dict,
    *,
    report_path: str | Path | None = None,
    rank: int = 1,
    source: str = "auto",
    overwrite: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    report = _load_report(game, config, report_path)
    if "error" in report:
        return report

    ranked = list(report.get("ranked_candidates") or [])
    if rank < 1 or rank > len(ranked):
        return {
            "ok": False,
            "error": f"rank {rank} out of range (1-{len(ranked)})",
            "report_path": str(report["report_path"]),
        }

    item = ranked[rank - 1]
    entity_id = item.get("candidate_weapon_id")
    if not entity_id:
        return {
            "ok": False,
            "error": f"rank {rank} has no candidate_weapon_id",
            "report_path": str(report["report_path"]),
        }

    selected = _select_source_path(item, source)
    if "error" in selected:
        return {
            "ok": False,
            "error": selected["error"],
            "report_path": str(report["report_path"]),
        }

    game_pack = load_game_pack(game, config)
    wd_cfg = get_weapon_detector_game_config(game, config, game_pack)
    pack_root = Path(game_pack.get("pack_root", "."))
    icon_dir = resolve_asset_path(
        wd_cfg.get("icon_dir") or f"assets/weapon_icons/{game}",
        pack_root,
    )
    icon_dir.mkdir(parents=True, exist_ok=True)
    asset_path = icon_dir / f"{entity_id}.png"

    if asset_path.exists() and not overwrite:
        return {
            "ok": False,
            "error": f"asset already exists for '{entity_id}'. Enable overwrite to replace it.",
            "report_path": str(report["report_path"]),
            "asset_path": _response_path(asset_path),
            "source_path": _response_path(selected["path"]),
        }

    backup_path = _backup_existing_asset(asset_path) if asset_path.exists() and overwrite and not dry_run else None
    promotion = {
        "promoted_at": datetime.now().isoformat(timespec="seconds"),
        "game": game,
        "report_path": str(report["report_path"]),
        "rank": rank,
        "entity_id": entity_id,
        "display_name": item.get("candidate_display_name") or entity_id.replace("_", " ").title(),
        "source_type": selected["source_type"],
        "source_path": _response_path(selected["path"]),
        "asset_path": _response_path(asset_path),
        "backup_path": _response_path(backup_path) if backup_path else None,
        "overwrite": overwrite,
        "dry_run": dry_run,
        "candidate_confidence": round(float(item.get("candidate_confidence", 0.0) or 0.0), 3),
        "clip_stem": item.get("clip_stem"),
    }

    if not dry_run:
        shutil.copy2(selected["path"], asset_path)
        _append_promotion_log(game, config, promotion)

    return {"ok": True, **promotion}


def _load_report(game: str, config: dict, report_path: str | Path | None) -> dict[str, Any]:
    path = Path(report_path) if report_path else _latest_report_path(game, config)
    if path is None:
        return {"ok": False, "error": f"no weapon-detector audit reports found for {game}"}
    if not path.exists():
        return {"ok": False, "error": f"report not found: {path}"}
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {"ok": False, "error": f"report is not valid JSON: {path}"}
    payload["report_path"] = path
    return payload


def _latest_report_path(game: str, config: dict) -> Path | None:
    report_dir = get_weapon_detector_report_dir(game, config)
    reports = sorted(report_dir.glob("*.json"))
    return reports[-1] if reports else None


def _select_source_path(item: dict[str, Any], source: str) -> dict[str, Any]:
    exported = item.get("exported_assets") or {}
    source_key = str(source or "auto").strip().lower()
    candidates = []
    if source_key in {"auto", "candidate"}:
        path = exported.get("candidate_crop_path")
        if path:
            candidates.append(("candidate", Path(path)))
    if source_key in {"auto", "roi"}:
        path = exported.get("roi_crop_path")
        if path:
            candidates.append(("roi", Path(path)))

    for source_type, path in candidates:
        if path.exists():
            return {"source_type": source_type, "path": path}

    expected = "candidate_crop_path or roi_crop_path" if source_key == "auto" else f"{source_key}_crop_path"
    return {"error": f"missing exported asset for {expected}"}


def _backup_existing_asset(asset_path: Path) -> Path:
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


def _append_promotion_log(game: str, config: dict, promotion: dict[str, Any]) -> None:
    report_dir = get_weapon_detector_report_dir(game, config)
    log_path = report_dir / "promotion_log.jsonl"
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(promotion, sort_keys=True) + "\n")


def _response_path(path: Path | None) -> str | None:
    if path is None:
        return None
    return str(path)
