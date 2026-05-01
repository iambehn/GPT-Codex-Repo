from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.simple_yaml import load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_REVIEW_SESSION_SCHEMA_VERSION = "runtime_review_session_v1"
DEFAULT_GPT_REPO = Path.home() / "GPT-Codex-Repo"
BRIDGE_TEMPLATE_ID = "runtime_review_bridge"
BRIDGE_CLIP_TYPE = "runtime_candidate"
DEFAULT_INSPECT_LIMIT = 3


def prepare_runtime_review(
    game: str,
    *,
    sidecar_root: str | Path | None = None,
    action: str | None = None,
    limit: int | None = None,
    gpt_repo: str | Path | None = None,
    session_name: str | None = None,
) -> dict[str, Any]:
    gpt_repo_path = _resolve_gpt_repo(gpt_repo)
    candidates, selection_source, effective_action = _select_candidates(
        game=game,
        sidecar_root=sidecar_root,
        action=action,
        limit=limit,
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
        "schema_version": RUNTIME_REVIEW_SESSION_SCHEMA_VERSION,
        "session_id": session_id,
        "game": game,
        "gpt_repo": str(gpt_repo_path),
        "selection_source": selection_source,
        "selection_action_filter": effective_action,
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


def apply_runtime_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    manifest_path = _resolve_path(session_manifest)
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != RUNTIME_REVIEW_SESSION_SCHEMA_VERSION:
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
        sidecar["runtime_review"] = {
            "session_id": manifest["session_id"],
            "review_status": review_status,
            "reviewed_at": reviewed_at,
            "review_app": "gpt_codex_review_app",
            "bridge_score": item.get("highlight_score"),
            "bridge_recommended_action": item.get("recommended_action"),
            "bridge_event_types": list(item.get("event_types", [])),
            "gpt_meta_path": str(gpt_meta_path),
            "gpt_processed_path": item.get("gpt_processed_path"),
            "gpt_final_path": final_path,
        }
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


def cleanup_runtime_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    del gpt_repo
    manifest_path = _resolve_path(session_manifest)
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != RUNTIME_REVIEW_SESSION_SCHEMA_VERSION:
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
) -> tuple[list[dict[str, Any]], str, str]:
    root = _default_sidecar_root(game, sidecar_root)
    sidecar_paths = sorted(root.rglob("*.runtime_analysis.json"))
    selection_source = str(root)
    effective_action = str(action or "review_default")

    highlight_candidates: list[dict[str, Any]] = []
    inspect_candidates: list[dict[str, Any]] = []
    for sidecar_path in sidecar_paths:
        candidate = _candidate_from_sidecar(sidecar_path, game=game)
        if candidate is None:
            continue
        runtime_action = candidate["recommended_action"]
        if effective_action == "highlight_candidate":
            if runtime_action == "highlight_candidate":
                highlight_candidates.append(candidate)
            continue
        if effective_action == "inspect":
            if runtime_action == "inspect":
                inspect_candidates.append(candidate)
            continue
        if effective_action == "all_non_skip":
            if runtime_action == "highlight_candidate":
                highlight_candidates.append(candidate)
            elif runtime_action == "inspect":
                inspect_candidates.append(candidate)
            continue
        if runtime_action == "highlight_candidate":
            highlight_candidates.append(candidate)
        elif runtime_action == "inspect":
            inspect_candidates.append(candidate)

    highlight_candidates.sort(key=lambda item: (-float(item["highlight_score"]), str(item["source"])))
    inspect_candidates.sort(key=lambda item: (-float(item["highlight_score"]), str(item["source"])))

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


def _candidate_from_sidecar(sidecar_path: Path, *, game: str) -> dict[str, Any] | None:
    sidecar = _load_json(sidecar_path)
    if sidecar.get("schema_version") != "runtime_analysis_v1":
        return None
    if sidecar.get("game") != game:
        return None
    if not sidecar.get("ok", False):
        return None

    source_path = Path(str(sidecar.get("source", ""))).expanduser()
    if not source_path.is_absolute():
        source_path = source_path.resolve()
    if not source_path.exists() or not source_path.is_file():
        return None

    events_payload = sidecar.get("events", {})
    matcher_payload = sidecar.get("matcher", {})
    runtime_action = str(sidecar.get("runtime_review", {}).get("recommended_action") or "").strip()
    if not runtime_action:
        runtime_action = _recommended_action_from_sidecar(sidecar)

    event_rows = list(events_payload.get("rows", []))
    event_types = sorted({str(row.get("event_type", "")) for row in event_rows if row.get("event_type")})
    return {
        "sidecar_path": str(sidecar_path.resolve()),
        "source": str(source_path.resolve()),
        "highlight_score": float(sidecar.get("runtime_review", {}).get("highlight_score") or _highlight_score_from_sidecar(sidecar)),
        "recommended_action": runtime_action,
        "event_count": int(events_payload.get("event_count", len(event_rows)) or 0),
        "confirmed_detection_count": len(list(matcher_payload.get("confirmed_detections", []))),
        "event_types": event_types,
    }


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

    bridge_stem = f"runtime-review-{session_id.split('-')[-1]}-{index:03d}-{_source_slug(candidate['source'])}"
    gpt_processed_path = processing_dir / f"{bridge_stem}.mp4"
    gpt_meta_path = inbox_dir / f"{bridge_stem}.meta.json"

    source_path = Path(str(candidate["source"])).resolve()
    shutil.copy2(source_path, gpt_processed_path)

    gpt_meta = {
        "clip_id": bridge_stem,
        "game": game,
        "clip_path": str(gpt_processed_path),
        "processed_path": str(gpt_processed_path),
        "meta_path": str(gpt_meta_path),
        "status": "queue",
        "created_from": "runtime_review_bridge",
        "selected_template_id": BRIDGE_TEMPLATE_ID,
        "scoring": {
            "highlight_score": _highlight_score(candidate["highlight_score"]),
            "clip_type": BRIDGE_CLIP_TYPE,
            "suggested_title": Path(str(candidate["source"])).stem,
            "suggested_caption": f"runtime score {candidate['highlight_score']:.4f}",
            "score_reasoning": _score_reasoning(candidate),
        },
        "runtime_review_bridge": {
            "bridge_owned": True,
            "session_id": session_id,
            "source_sidecar_path": candidate["sidecar_path"],
            "source_clip_path": candidate["source"],
            "highlight_score": candidate["highlight_score"],
            "recommended_action": candidate["recommended_action"],
            "event_types": list(candidate["event_types"]),
        },
    }
    gpt_meta_path.write_text(json.dumps(gpt_meta, indent=2), encoding="utf-8")

    return {
        "clip_id": bridge_stem,
        "sidecar_path": candidate["sidecar_path"],
        "source": candidate["source"],
        "gpt_processed_path": str(gpt_processed_path),
        "gpt_meta_path": str(gpt_meta_path),
        "highlight_score": candidate["highlight_score"],
        "recommended_action": candidate["recommended_action"],
        "event_count": candidate["event_count"],
        "confirmed_detection_count": candidate["confirmed_detection_count"],
        "event_types": list(candidate["event_types"]),
        "materialization_mode": "copy",
        "bridge_owned": True,
        "apply_status": "pending",
        "review_status": "unreviewed",
    }


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
        if path.exists():
            if path.is_dir():
                continue
            path.unlink()
            removed_any = True
    processed_path = Path(str(item["gpt_processed_path"]))
    thumb_path = processed_path.with_suffix(".thumb.jpg")
    if thumb_path.exists():
        thumb_path.unlink()
        removed_any = True
    item["cleanup_status"] = "removed" if removed_any else "already_clean"
    return removed_any


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
        return (REPO_ROOT / "outputs" / "runtime_analysis" / game).resolve()
    return _resolve_path(sidecar_root)


def _session_manifest_path(game: str, session_id: str) -> Path:
    return REPO_ROOT / "outputs" / "runtime_review_sessions" / game / f"{session_id}.runtime_review_session.json"


def _session_id(
    game: str,
    candidates: list[dict[str, Any]],
    action: str,
    session_name: str | None,
    created_at: str,
) -> str:
    slug = _slug(session_name or "session")
    payload = "\n".join(
        [game, action, created_at, *[f"{item['sidecar_path']}|{item['highlight_score']}" for item in candidates]]
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"{game}-runtime-review-{slug}-{digest}"


def _top_level_materialization_mode(modes: set[str]) -> str:
    if not modes:
        return "none"
    if len(modes) == 1:
        return next(iter(modes))
    return "mixed"


def _highlight_score(score: float) -> int:
    return max(0, min(100, int(round(float(score) * 100))))


def _score_reasoning(candidate: dict[str, Any]) -> str:
    event_text = ", ".join(candidate.get("event_types", [])) or "none"
    return (
        f"Runtime review bridge candidate. Score={candidate['highlight_score']:.4f}. "
        f"Action={candidate['recommended_action']}. Events={event_text}."
    )


def _highlight_score_from_sidecar(sidecar: dict[str, Any]) -> float:
    runtime_review = sidecar.get("runtime_review", {})
    if isinstance(runtime_review, dict) and runtime_review.get("highlight_score") is not None:
        return float(runtime_review["highlight_score"])
    events_payload = sidecar.get("events", {})
    event_count = int(events_payload.get("event_count", 0) or 0)
    if event_count > 0:
        return min(1.0, event_count * 0.25)
    matcher_payload = sidecar.get("matcher", {})
    detection_count = len(list(matcher_payload.get("confirmed_detections", [])))
    return min(1.0, detection_count * 0.05)


def _recommended_action_from_sidecar(sidecar: dict[str, Any]) -> str:
    runtime_review = sidecar.get("runtime_review", {})
    if isinstance(runtime_review, dict) and runtime_review.get("recommended_action"):
        return str(runtime_review["recommended_action"])
    score = _highlight_score_from_sidecar(sidecar)
    if score >= 0.60:
        return "highlight_candidate"
    if score >= 0.25:
        return "inspect"
    return "skip"


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
