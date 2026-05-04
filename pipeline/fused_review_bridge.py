from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.fused_export import DEFAULT_ACTION_THRESHOLDS
from pipeline.simple_yaml import load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
FUSED_REVIEW_SESSION_SCHEMA_VERSION = "fused_review_session_v1"
DEFAULT_GPT_REPO = Path.home() / "GPT-Codex-Repo"
BRIDGE_TEMPLATE_ID = "fused_review_bridge"
BRIDGE_CLIP_TYPE = "fused_event_candidate"
DEFAULT_INSPECT_LIMIT = 3


def prepare_fused_review(
    game: str,
    *,
    sidecar_root: str | Path | None = None,
    action: str | None = None,
    limit: int | None = None,
    gpt_repo: str | Path | None = None,
    session_name: str | None = None,
    event_type: str | None = None,
) -> dict[str, Any]:
    gpt_repo_path = _resolve_gpt_repo(gpt_repo)
    candidates, selection_source, effective_action = _select_candidates(
        game=game,
        sidecar_root=sidecar_root,
        action=action,
        limit=limit,
        event_type=event_type,
    )
    created_at = _utc_now()
    session_id = _session_id(game, candidates, effective_action, session_name, created_at)
    manifest_path = _session_manifest_path(game, session_id)

    items: list[dict[str, Any]] = []
    materialization_modes: set[str] = set()
    for index, candidate in enumerate(candidates):
        item = _materialize_candidate(
            game=game,
            candidate=candidate,
            gpt_repo=gpt_repo_path,
            session_id=session_id,
            index=index,
        )
        materialization_modes.add(item["materialization_mode"])
        items.append(item)

    manifest = {
        "schema_version": FUSED_REVIEW_SESSION_SCHEMA_VERSION,
        "session_id": session_id,
        "game": game,
        "gpt_repo": str(gpt_repo_path),
        "selection_source": selection_source,
        "selection_action_filter": effective_action,
        "selection_event_type_filter": event_type,
        "limit": limit,
        "created_at": created_at,
        "materialization_mode": _top_level_materialization_mode(materialization_modes),
        "item_count": len(items),
        "items": items,
        "manifest_path": str(manifest_path),
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def apply_fused_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    manifest_path = _resolve_path(session_manifest)
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != FUSED_REVIEW_SESSION_SCHEMA_VERSION:
        return {
            "ok": False,
            "session_manifest": str(manifest_path),
            "error": "unsupported session manifest schema",
        }

    if gpt_repo is not None:
        manifest_gpt_repo = Path(str(manifest.get("gpt_repo", ""))).resolve()
        requested_gpt_repo = _resolve_path(gpt_repo)
        if manifest_gpt_repo != requested_gpt_repo:
            return {
                "ok": False,
                "session_manifest": str(manifest_path),
                "error": "session manifest gpt_repo does not match requested gpt_repo",
            }

    approved_count = 0
    rejected_count = 0
    unreviewed_count = 0

    for item in manifest.get("items", []):
        gpt_meta_path = Path(str(item["gpt_meta_path"])).resolve()
        sidecar_path = Path(str(item["sidecar_path"])).resolve()

        meta = _load_json(gpt_meta_path)
        review_status = _normalized_review_status(meta.get("review_status"))
        reviewed_at = meta.get("reviewed_at")
        final_path = str(meta.get("final_path")) if meta.get("final_path") else None

        sidecar = _load_json(sidecar_path)
        fused_review = sidecar.setdefault("fused_review", {})
        event_reviews = fused_review.setdefault("events", {})
        event_reviews[str(item["event_id"])] = {
            "session_id": manifest["session_id"],
            "review_status": review_status,
            "reviewed_at": reviewed_at,
            "review_app": "gpt_codex_review_app",
            "bridge_final_score": item.get("final_score"),
            "bridge_recommended_action": item.get("recommended_action"),
            "bridge_event_type": item.get("event_type"),
            "bridge_gate_status": item.get("gate_status"),
            "bridge_segment": {
                "start_timestamp": item.get("suggested_start_timestamp"),
                "end_timestamp": item.get("suggested_end_timestamp"),
            },
            "gpt_meta_path": str(gpt_meta_path),
            "gpt_processed_path": item.get("gpt_processed_path"),
            "gpt_final_path": final_path,
        }
        fused_review["session_id"] = manifest["session_id"]
        fused_review["reviewed_event_count"] = len(event_reviews)
        sidecar_path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")

        item["apply_status"] = "applied"
        item["review_status"] = review_status
        item["reviewed_at"] = reviewed_at
        item["gpt_final_path"] = final_path

        if review_status == "approved":
            approved_count += 1
        elif review_status == "rejected":
            rejected_count += 1
        else:
            unreviewed_count += 1

    manifest["applied_at"] = _utc_now()
    manifest["approved_count"] = approved_count
    manifest["rejected_count"] = rejected_count
    manifest["unreviewed_count"] = unreviewed_count
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "session_manifest": str(manifest_path),
        "session_id": manifest["session_id"],
        "approved_count": approved_count,
        "rejected_count": rejected_count,
        "unreviewed_count": unreviewed_count,
        "item_count": len(manifest.get("items", [])),
    }


def cleanup_fused_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    del gpt_repo
    manifest_path = _resolve_path(session_manifest)
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != FUSED_REVIEW_SESSION_SCHEMA_VERSION:
        return {
            "ok": False,
            "session_manifest": str(manifest_path),
            "error": "unsupported session manifest schema",
        }

    cleanup_count = 0
    for item in manifest.get("items", []):
        cleanup_count += int(_cleanup_session_item(item))

    manifest["cleanup_at"] = _utc_now()
    manifest["cleanup_count"] = cleanup_count
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "session_manifest": str(manifest_path),
        "session_id": manifest["session_id"],
        "cleanup_count": cleanup_count,
        "item_count": len(manifest.get("items", [])),
    }


def _select_candidates(
    *,
    game: str,
    sidecar_root: str | Path | None,
    action: str | None,
    limit: int | None,
    event_type: str | None,
) -> tuple[list[dict[str, Any]], str, str]:
    root = _default_sidecar_root(game, sidecar_root)
    sidecar_paths = sorted(root.rglob("*.fused_analysis.json"))
    selection_source = str(root)
    effective_action = str(action or "review_default")

    highlight_candidates: list[dict[str, Any]] = []
    inspect_candidates: list[dict[str, Any]] = []
    for sidecar_path in sidecar_paths:
        for candidate in _candidates_from_sidecar(sidecar_path, game=game, event_type=event_type):
            candidate_action = candidate["recommended_action"]
            if effective_action == "highlight_candidate":
                if candidate_action == "highlight_candidate":
                    highlight_candidates.append(candidate)
                continue
            if effective_action == "inspect":
                if candidate_action == "inspect":
                    inspect_candidates.append(candidate)
                continue
            if effective_action == "all_non_skip":
                if candidate_action == "highlight_candidate":
                    highlight_candidates.append(candidate)
                elif candidate_action == "inspect":
                    inspect_candidates.append(candidate)
                continue
            if candidate_action == "highlight_candidate":
                highlight_candidates.append(candidate)
            elif candidate_action == "inspect":
                inspect_candidates.append(candidate)

    highlight_candidates.sort(key=_candidate_sort_key)
    inspect_candidates.sort(key=_candidate_sort_key)

    if effective_action in {"highlight_candidate", "inspect", "all_non_skip"}:
        merged = highlight_candidates + inspect_candidates
        if limit is not None:
            merged = merged[:limit]
        return merged, selection_source, effective_action

    merged = list(highlight_candidates)
    if limit is not None:
        remaining = max(0, limit - len(merged))
        if remaining > 0:
            merged.extend(inspect_candidates[:remaining])
        merged = merged[:limit]
    else:
        merged.extend(inspect_candidates[:DEFAULT_INSPECT_LIMIT])
    return merged, selection_source, effective_action


def _candidates_from_sidecar(sidecar_path: Path, *, game: str, event_type: str | None) -> list[dict[str, Any]]:
    sidecar = _load_json(sidecar_path)
    if sidecar.get("schema_version") != "fused_analysis_v1":
        return []
    if sidecar.get("game") != game:
        return []
    if not sidecar.get("ok", False):
        return []

    source_path = Path(str(sidecar.get("source", ""))).expanduser()
    if not source_path.is_absolute():
        source_path = source_path.resolve()
    if not source_path.exists() or not source_path.is_file():
        return []

    candidates: list[dict[str, Any]] = []
    for row in sidecar.get("fused_events", []):
        if not isinstance(row, dict):
            continue
        if event_type and str(row.get("event_type")) != event_type:
            continue
        final_score = float(row.get("final_score", row.get("confidence", 0.0)) or 0.0)
        candidate = {
            "sidecar_path": str(sidecar_path.resolve()),
            "source": str(source_path.resolve()),
            "fusion_id": sidecar.get("fusion_id"),
            "event_id": row.get("event_id"),
            "event_type": row.get("event_type"),
            "final_score": final_score,
            "recommended_action": _recommended_action(final_score),
            "gate_status": row.get("gate_status"),
            "synergy_applied": bool(row.get("synergy_applied", False)),
            "suggested_start_timestamp": float(row.get("suggested_start_timestamp", 0.0) or 0.0),
            "suggested_end_timestamp": float(row.get("suggested_end_timestamp", 0.0) or 0.0),
            "entity_id": row.get("metadata", {}).get("entity_id") if isinstance(row.get("metadata"), dict) else None,
            "ability_id": row.get("metadata", {}).get("ability_id") if isinstance(row.get("metadata"), dict) else None,
            "equipment_id": row.get("metadata", {}).get("equipment_id") if isinstance(row.get("metadata"), dict) else None,
            "event_row_id": row.get("metadata", {}).get("event_row_id") if isinstance(row.get("metadata"), dict) else None,
            "matched_signal_types": list(row.get("metadata", {}).get("matched_signal_types", []))
            if isinstance(row.get("metadata"), dict)
            else [],
        }
        candidates.append(candidate)
    return candidates


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[float, int, int, str]:
    gate_bonus = 0 if str(candidate.get("gate_status")) == "confirmed" else 1
    synergy_bonus = 0 if bool(candidate.get("synergy_applied", False)) else 1
    return (-float(candidate.get("final_score", 0.0)), gate_bonus, synergy_bonus, str(candidate.get("source", "")))


def _materialize_candidate(
    *,
    game: str,
    candidate: dict[str, Any],
    gpt_repo: Path,
    session_id: str,
    index: int,
) -> dict[str, Any]:
    gpt_paths = _gpt_paths(gpt_repo)
    processing_dir = gpt_paths["processing"] / game
    inbox_dir = gpt_paths["inbox"] / game
    processing_dir.mkdir(parents=True, exist_ok=True)
    inbox_dir.mkdir(parents=True, exist_ok=True)

    bridge_stem = f"fused-review-{session_id.split('-')[-1]}-{index:03d}-{_source_slug(candidate['source'])}"
    gpt_processed_path = processing_dir / f"{bridge_stem}.mp4"
    gpt_meta_path = inbox_dir / f"{bridge_stem}.meta.json"

    _materialize_segment(
        Path(str(candidate["source"])).resolve(),
        gpt_processed_path,
        start_seconds=float(candidate["suggested_start_timestamp"]),
        end_seconds=float(candidate["suggested_end_timestamp"]),
    )

    gpt_meta = {
        "clip_id": bridge_stem,
        "game": game,
        "clip_path": str(gpt_processed_path),
        "processed_path": str(gpt_processed_path),
        "meta_path": str(gpt_meta_path),
        "status": "queue",
        "created_from": "fused_review_bridge",
        "selected_template_id": BRIDGE_TEMPLATE_ID,
        "scoring": {
            "highlight_score": _highlight_score(candidate["final_score"]),
            "clip_type": BRIDGE_CLIP_TYPE,
            "suggested_title": f"{Path(str(candidate['source'])).stem}-{candidate['event_type']}",
            "suggested_caption": f"fused score {candidate['final_score']:.4f}",
            "score_reasoning": _score_reasoning(candidate),
        },
        "fused_review_bridge": {
            "bridge_owned": True,
            "session_id": session_id,
            "source_sidecar_path": candidate["sidecar_path"],
            "source_clip_path": candidate["source"],
            "event_id": candidate["event_id"],
            "event_type": candidate["event_type"],
            "final_score": candidate["final_score"],
            "gate_status": candidate["gate_status"],
            "synergy_applied": candidate["synergy_applied"],
            "suggested_start_timestamp": candidate["suggested_start_timestamp"],
            "suggested_end_timestamp": candidate["suggested_end_timestamp"],
            "entity_id": candidate.get("entity_id"),
            "ability_id": candidate.get("ability_id"),
            "equipment_id": candidate.get("equipment_id"),
            "event_row_id": candidate.get("event_row_id"),
            "matched_signal_types": list(candidate.get("matched_signal_types", [])),
        },
    }
    gpt_meta_path.write_text(json.dumps(gpt_meta, indent=2), encoding="utf-8")

    return {
        "clip_id": bridge_stem,
        "sidecar_path": candidate["sidecar_path"],
        "source": candidate["source"],
        "event_id": candidate["event_id"],
        "event_type": candidate["event_type"],
        "gpt_processed_path": str(gpt_processed_path),
        "gpt_meta_path": str(gpt_meta_path),
        "final_score": candidate["final_score"],
        "recommended_action": candidate["recommended_action"],
        "gate_status": candidate["gate_status"],
        "synergy_applied": candidate["synergy_applied"],
        "suggested_start_timestamp": candidate["suggested_start_timestamp"],
        "suggested_end_timestamp": candidate["suggested_end_timestamp"],
        "entity_id": candidate.get("entity_id"),
        "ability_id": candidate.get("ability_id"),
        "equipment_id": candidate.get("equipment_id"),
        "event_row_id": candidate.get("event_row_id"),
        "materialization_mode": "trim",
        "bridge_owned": True,
        "apply_status": "pending",
        "review_status": "unreviewed",
    }


def _materialize_segment(source_path: Path, output_path: Path, *, start_seconds: float, end_seconds: float) -> None:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    ffmpeg_path = Path(ffmpeg)
    if not ffmpeg_path.exists():
        raise ValueError(f"ffmpeg not found for fused review materialization: {ffmpeg_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    duration = max(0.01, float(end_seconds) - float(start_seconds))
    command = [
        str(ffmpeg_path),
        "-v",
        "error",
        "-y",
        "-ss",
        f"{max(0.0, float(start_seconds)):.3f}",
        "-i",
        str(source_path),
        "-t",
        f"{duration:.3f}",
        "-c",
        "copy",
        str(output_path),
    ]
    subprocess.run(command, check=True, capture_output=True)


def _cleanup_session_item(item: dict[str, Any]) -> bool:
    removed_any = False
    meta_path = Path(str(item["gpt_meta_path"]))
    meta = _load_json(meta_path) if meta_path.exists() else {}
    final_path_value = item.get("gpt_final_path") or meta.get("final_path")
    for key in ("gpt_processed_path", "gpt_meta_path", "gpt_final_path"):
        path_value = final_path_value if key == "gpt_final_path" else item.get(key)
        if not path_value:
            continue
        path = Path(str(path_value))
        if path.exists() and path.is_file():
            path.unlink()
            removed_any = True
    processed_path = Path(str(item["gpt_processed_path"]))
    thumb_path = processed_path.with_suffix(".thumb.jpg")
    if thumb_path.exists():
        thumb_path.unlink()
        removed_any = True
    item["cleanup_status"] = "removed" if removed_any else "already_clean"
    return removed_any


def _recommended_action(final_score: float) -> str:
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["highlight_candidate"]):
        return "highlight_candidate"
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["inspect"]):
        return "inspect"
    return "skip"


def _resolve_gpt_repo(gpt_repo: str | Path | None) -> Path:
    candidate = _resolve_path(gpt_repo) if gpt_repo is not None else DEFAULT_GPT_REPO.resolve()
    if not candidate.exists() or not candidate.is_dir():
        raise ValueError(f"gpt review repo does not exist or is not a directory: {candidate}")
    return candidate


def _gpt_paths(gpt_repo: Path) -> dict[str, Path]:
    config_path = gpt_repo / "config.yaml"
    paths_config = {}
    if config_path.exists():
        loaded = load_yaml_file(config_path)
        if isinstance(loaded.get("paths"), dict):
            paths_config = loaded["paths"]

    return {
        "inbox": _gpt_repo_path(gpt_repo, paths_config.get("inbox", "inbox")),
        "processing": _gpt_repo_path(gpt_repo, paths_config.get("processing", "processing")),
        "accepted": _gpt_repo_path(gpt_repo, paths_config.get("accepted", "accepted")),
        "rejected": _gpt_repo_path(gpt_repo, paths_config.get("rejected", "rejected")),
    }


def _gpt_repo_path(gpt_repo: Path, path_value: str) -> Path:
    path = Path(str(path_value))
    if path.is_absolute():
        return path.resolve()
    return (gpt_repo / path).resolve()


def _default_sidecar_root(game: str, sidecar_root: str | Path | None) -> Path:
    if sidecar_root is None:
        return (REPO_ROOT / "outputs" / "fused_analysis" / game).resolve()
    return _resolve_path(sidecar_root)


def _session_manifest_path(game: str, session_id: str) -> Path:
    return REPO_ROOT / "outputs" / "fused_review_sessions" / game / f"{session_id}.fused_review_session.json"


def _session_id(game: str, candidates: list[dict[str, Any]], action: str, session_name: str | None, created_at: str) -> str:
    slug = _slug(session_name or "session")
    payload = "\n".join([game, action, created_at, *[f"{item['sidecar_path']}|{item['event_id']}|{item['final_score']}" for item in candidates]])
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"{game}-fused-review-{slug}-{digest}"


def _top_level_materialization_mode(modes: set[str]) -> str:
    if not modes:
        return "none"
    if len(modes) == 1:
        return next(iter(modes))
    return "mixed"


def _highlight_score(score: float) -> int:
    return max(0, min(100, int(round(float(score) * 100))))


def _score_reasoning(candidate: dict[str, Any]) -> str:
    signal_text = ", ".join(candidate.get("matched_signal_types", [])) or "none"
    return (
        f"Fused review bridge candidate. Score={candidate['final_score']:.4f}. "
        f"Action={candidate['recommended_action']}. Event={candidate['event_type']}. "
        f"Gate={candidate['gate_status']}. Signals={signal_text}."
    )


def _normalized_review_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "accepted":
        return "approved"
    if normalized == "rejected":
        return "rejected"
    return "unreviewed"


def _resolve_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _source_slug(source: str | Path) -> str:
    return _slug(Path(str(source)).stem or "clip")


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", str(value).lower()).strip("-") or "item"
