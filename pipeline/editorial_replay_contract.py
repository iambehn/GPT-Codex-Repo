from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
EDITORIAL_IDENTITY_SCHEMA_VERSION = "editorial_identity_v1"
EDITORIAL_DECISION_SCHEMA_VERSION = "editorial_decision_v1"
EXPORT_READY_SNAPSHOT_SCHEMA_VERSION = "export_ready_snapshot_v1"


def runtime_editorial_object_id(*, game: str, source: str) -> str:
    return _editorial_object_id("runtime", game, source, None)


def fused_editorial_object_id(*, game: str, source: str, event_id: str) -> str:
    return _editorial_object_id("fused", game, source, event_id)


def persist_runtime_review_decision(
    *,
    repo_root: str | Path | None,
    manifest: dict[str, Any],
    item: dict[str, Any],
    review_status: str,
    reviewed_at: str | None,
    sidecar: dict[str, Any],
) -> dict[str, Any]:
    root = _resolve_repo_root(repo_root)
    game = str(manifest.get("game") or "").strip() or str(sidecar.get("game") or "").strip() or "unknown_game"
    source = _canonical_source(item.get("source") or sidecar.get("source") or "")
    if not source:
        raise ValueError("runtime replay contract requires source")

    expected_editorial_object_id = runtime_editorial_object_id(
        game=game,
        source=source,
    )
    editorial_object_id = expected_editorial_object_id
    identity_payload = {
        "schema_version": EDITORIAL_IDENTITY_SCHEMA_VERSION,
        "editorial_object_id": editorial_object_id,
        "review_surface": "runtime",
        "game": game,
        "source": source,
        "event_id": None,
        "event_type": None,
        "identity_basis": "runtime_source_v1",
        "identity_version": "v1",
        "source_sidecar_path": str(item.get("sidecar_path") or "").strip() or None,
        "updated_at": _utc_now(),
    }
    identity_path = _identity_path(root=root, game=game, editorial_object_id=editorial_object_id)
    identity_payload["created_at"] = _preserve_created_at(identity_path, "created_at")
    _write_json(identity_path, identity_payload)

    decision_record_id = _decision_record_id(
        review_surface="runtime",
        editorial_object_id=editorial_object_id,
        review_session_id=str(manifest.get("session_id") or "").strip(),
    )
    decision_payload = {
        "schema_version": EDITORIAL_DECISION_SCHEMA_VERSION,
        "decision_record_id": decision_record_id,
        "editorial_object_id": editorial_object_id,
        "review_surface": "runtime",
        "review_status": review_status,
        "reviewed_at": reviewed_at,
        "review_session_id": str(manifest.get("session_id") or "").strip() or None,
        "decision_reason": "persisted_runtime_review_bridge_decision",
        "source_sidecar_schema_version": str(sidecar.get("schema_version") or "").strip() or None,
        "source_sidecar_game": str(sidecar.get("game") or "").strip() or None,
        "source_sidecar_source": _canonical_source(sidecar.get("source") or "") or None,
        "runtime_event_types": list(item.get("event_types", [])),
        "bridge_score": item.get("highlight_score"),
        "bridge_recommended_action": item.get("recommended_action"),
        "gpt_meta_path": str(item.get("gpt_meta_path") or "").strip() or None,
        "gpt_processed_path": str(item.get("gpt_processed_path") or "").strip() or None,
        "gpt_final_path": str(item.get("gpt_final_path") or "").strip() or None,
        "sidecar_path_at_review": str(item.get("sidecar_path") or "").strip() or None,
        "created_at": _utc_now(),
    }
    decision_path = _decision_path(root=root, game=game, decision_record_id=decision_record_id)
    _write_json(decision_path, decision_payload)
    return {
        "editorial_object_id": editorial_object_id,
        "identity_path": str(identity_path),
        "decision_record_id": decision_record_id,
        "decision_path": str(decision_path),
    }


def persist_fused_review_decision(
    *,
    repo_root: str | Path | None,
    manifest: dict[str, Any],
    item: dict[str, Any],
    review_status: str,
    reviewed_at: str | None,
    sidecar: dict[str, Any],
) -> dict[str, Any]:
    root = _resolve_repo_root(repo_root)
    game = str(manifest.get("game") or "").strip() or str(sidecar.get("game") or "").strip() or "unknown_game"
    source = _canonical_source(item.get("source") or sidecar.get("source") or "")
    event_id = str(item.get("event_id") or "").strip()
    if not source or not event_id:
        raise ValueError("fused replay contract requires source and event_id")

    expected_editorial_object_id = fused_editorial_object_id(
        game=game,
        source=source,
        event_id=event_id,
    )
    editorial_object_id = expected_editorial_object_id
    identity_payload = {
        "schema_version": EDITORIAL_IDENTITY_SCHEMA_VERSION,
        "editorial_object_id": editorial_object_id,
        "review_surface": "fused",
        "game": game,
        "source": source,
        "event_id": event_id,
        "event_type": str(item.get("event_type") or "").strip() or None,
        "identity_basis": "fused_event_id_v1",
        "identity_version": "v1",
        "source_sidecar_path": str(item.get("sidecar_path") or "").strip() or None,
        "updated_at": _utc_now(),
    }
    identity_path = _identity_path(root=root, game=game, editorial_object_id=editorial_object_id)
    identity_payload["created_at"] = _preserve_created_at(identity_path, "created_at")
    _write_json(identity_path, identity_payload)

    decision_record_id = _decision_record_id(
        review_surface="fused",
        editorial_object_id=editorial_object_id,
        review_session_id=str(manifest.get("session_id") or "").strip(),
    )
    decision_payload = {
        "schema_version": EDITORIAL_DECISION_SCHEMA_VERSION,
        "decision_record_id": decision_record_id,
        "editorial_object_id": editorial_object_id,
        "review_surface": "fused",
        "review_status": review_status,
        "reviewed_at": reviewed_at,
        "review_session_id": str(manifest.get("session_id") or "").strip() or None,
        "decision_reason": "persisted_fused_review_bridge_decision",
        "source_sidecar_schema_version": str(sidecar.get("schema_version") or "").strip() or None,
        "source_sidecar_game": str(sidecar.get("game") or "").strip() or None,
        "source_sidecar_source": _canonical_source(sidecar.get("source") or "") or None,
        "event_id": event_id,
        "event_type": str(item.get("event_type") or "").strip() or None,
        "final_score": item.get("final_score"),
        "recommended_action": item.get("recommended_action"),
        "gate_status": item.get("gate_status"),
        "suggested_start_timestamp": item.get("suggested_start_timestamp"),
        "suggested_end_timestamp": item.get("suggested_end_timestamp"),
        "gpt_meta_path": str(item.get("gpt_meta_path") or "").strip() or None,
        "gpt_processed_path": str(item.get("gpt_processed_path") or "").strip() or None,
        "gpt_final_path": str(item.get("gpt_final_path") or "").strip() or None,
        "sidecar_path_at_review": str(item.get("sidecar_path") or "").strip() or None,
        "created_at": _utc_now(),
    }
    decision_path = _decision_path(root=root, game=game, decision_record_id=decision_record_id)
    _write_json(decision_path, decision_payload)
    return {
        "editorial_object_id": editorial_object_id,
        "identity_path": str(identity_path),
        "decision_record_id": decision_record_id,
        "decision_path": str(decision_path),
    }


def persist_export_ready_snapshots(
    *,
    repo_root: str | Path | None,
    workflow_run_id: str,
    workflow_created_at: str | None,
    lifecycle_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    root = _resolve_repo_root(repo_root)
    created: list[dict[str, Any]] = []
    for row in lifecycle_rows:
        game = str(row.get("game") or "").strip() or "unknown_game"
        source = _canonical_source(row.get("source") or "")
        event_id = str(row.get("event_id") or "").strip()
        if not source or not event_id:
            continue
        editorial_object_id = fused_editorial_object_id(game=game, source=source, event_id=event_id)
        selected_details = _load_json_dict(row.get("selected_highlight_details_json"))
        snapshot_id = _snapshot_id(workflow_run_id=workflow_run_id, editorial_object_id=editorial_object_id)
        payload = {
            "schema_version": EXPORT_READY_SNAPSHOT_SCHEMA_VERSION,
            "export_ready_snapshot_id": snapshot_id,
            "editorial_object_id": editorial_object_id,
            "candidate_id_at_selection": str(row.get("candidate_id") or "").strip() or None,
            "game": game,
            "source": source,
            "fixture_id": str(row.get("fixture_id") or "").strip() or None,
            "event_id": event_id,
            "event_type": selected_details.get("event_type") or None,
            "selection_manifest_path": str(row.get("highlight_selection_manifest_path") or "").strip() or None,
            "workflow_run_id": workflow_run_id,
            "selection_basis": str(row.get("selection_basis") or "").strip() or None,
            "approved_review_status": str(row.get("latest_review_status") or "").strip() or None,
            "selected_at": workflow_created_at or _utc_now(),
            "start_seconds": selected_details.get("start_seconds"),
            "end_seconds": selected_details.get("end_seconds"),
            "final_score": row.get("final_score"),
            "fused_sidecar_path_at_selection": str(row.get("fused_sidecar_path") or "").strip() or None,
            "hook_id": None,
            "hook_mode": None,
            "hook_archetype": None,
            "packaging_strategy": None,
            "created_at": _utc_now(),
        }
        target = _snapshot_path(root=root, game=game, snapshot_id=snapshot_id)
        _write_json(target, payload)
        created.append(payload | {"manifest_path": str(target)})
    return created


def load_export_ready_snapshots(
    *,
    repo_root: str | Path | None,
    workflow_run_id: str | None = None,
    selection_manifest_path: str | Path | None = None,
    game: str | None = None,
) -> list[dict[str, Any]]:
    root = _resolve_repo_root(repo_root)
    snapshot_root = root / "outputs" / "editorial_replay" / "snapshots"
    if not snapshot_root.exists():
        return []
    normalized_selection = str(Path(selection_manifest_path).resolve()) if selection_manifest_path is not None else None
    normalized_game = str(game or "").strip() or None
    rows: list[dict[str, Any]] = []
    for path in sorted(snapshot_root.rglob("*.export_ready_snapshot.json")):
        payload = _read_json(path)
        if payload.get("schema_version") != EXPORT_READY_SNAPSHOT_SCHEMA_VERSION:
            continue
        if workflow_run_id is not None and str(payload.get("workflow_run_id") or "").strip() != str(workflow_run_id).strip():
            continue
        if normalized_selection is not None and str(payload.get("selection_manifest_path") or "").strip() != normalized_selection:
            continue
        if normalized_game is not None and str(payload.get("game") or "").strip() != normalized_game:
            continue
        payload["manifest_path"] = str(path.resolve())
        rows.append(payload)
    rows.sort(
        key=lambda row: (
            str(row.get("game") or ""),
            str(row.get("fixture_id") or ""),
            str(row.get("candidate_id_at_selection") or ""),
        )
    )
    return rows


def replay_runtime_editorial_decision(
    runtime_sidecar: str | Path,
    *,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    sidecar_path = _resolve_path(runtime_sidecar)
    sidecar = _read_json(sidecar_path)
    if str(sidecar.get("schema_version") or "").strip() != "runtime_analysis_v1":
        return {"ok": False, "status": "invalid_runtime_sidecar"}
    game = str(sidecar.get("game") or "").strip()
    source = _canonical_source(sidecar.get("source") or "")
    if not game or not source:
        return {"ok": False, "status": "missing_runtime_identity_fields"}
    editorial_object_id = runtime_editorial_object_id(game=game, source=source)
    decision = _latest_decision(
        repo_root=repo_root,
        game=game,
        editorial_object_id=editorial_object_id,
        review_surface="runtime",
    )
    if decision is None:
        return {"ok": False, "status": "no_runtime_decision_record", "editorial_object_id": editorial_object_id}
    sidecar["runtime_review"] = {
        "session_id": decision.get("review_session_id"),
        "review_status": decision.get("review_status"),
        "reviewed_at": decision.get("reviewed_at"),
        "review_app": "repo_local_editorial_replay",
        "bridge_score": decision.get("bridge_score"),
        "bridge_recommended_action": decision.get("bridge_recommended_action"),
        "bridge_event_types": list(decision.get("runtime_event_types", [])),
        "gpt_meta_path": decision.get("gpt_meta_path"),
        "gpt_processed_path": decision.get("gpt_processed_path"),
        "gpt_final_path": decision.get("gpt_final_path"),
    }
    sidecar_path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "editorial_object_id": editorial_object_id,
        "decision_record_id": decision.get("decision_record_id"),
        "runtime_sidecar_path": str(sidecar_path),
    }


def replay_fused_editorial_decisions(
    fused_sidecar: str | Path,
    *,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    sidecar_path = _resolve_path(fused_sidecar)
    sidecar = _read_json(sidecar_path)
    if str(sidecar.get("schema_version") or "").strip() != "fused_analysis_v1":
        return {"ok": False, "status": "invalid_fused_sidecar"}
    game = str(sidecar.get("game") or "").strip()
    source = _canonical_source(sidecar.get("source") or "")
    if not game or not source:
        return {"ok": False, "status": "missing_fused_identity_fields"}
    fused_review = sidecar.setdefault("fused_review", {})
    event_reviews = fused_review.setdefault("events", {})
    applied_count = 0
    for event in list(sidecar.get("fused_events", [])):
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            continue
        editorial_object_id = fused_editorial_object_id(game=game, source=source, event_id=event_id)
        decision = _latest_decision(
            repo_root=repo_root,
            game=game,
            editorial_object_id=editorial_object_id,
            review_surface="fused",
        )
        if decision is None:
            continue
        event_reviews[event_id] = {
            "session_id": decision.get("review_session_id"),
            "review_status": decision.get("review_status"),
            "reviewed_at": decision.get("reviewed_at"),
            "review_app": "repo_local_editorial_replay",
            "bridge_final_score": decision.get("final_score"),
            "bridge_recommended_action": decision.get("recommended_action"),
            "bridge_event_type": decision.get("event_type"),
            "bridge_gate_status": decision.get("gate_status"),
            "bridge_segment": {
                "start_timestamp": decision.get("suggested_start_timestamp"),
                "end_timestamp": decision.get("suggested_end_timestamp"),
            },
            "gpt_meta_path": decision.get("gpt_meta_path"),
            "gpt_processed_path": decision.get("gpt_processed_path"),
            "gpt_final_path": decision.get("gpt_final_path"),
        }
        applied_count += 1
    if applied_count == 0:
        return {"ok": False, "status": "no_fused_decision_records"}
    fused_review["session_id"] = fused_review.get("session_id") or "repo_local_editorial_replay"
    fused_review["reviewed_event_count"] = len(event_reviews)
    sidecar_path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "applied_count": applied_count,
        "fused_sidecar_path": str(sidecar_path),
    }


def _latest_decision(
    *,
    repo_root: str | Path | None,
    game: str,
    editorial_object_id: str,
    review_surface: str,
) -> dict[str, Any] | None:
    root = _resolve_repo_root(repo_root)
    decision_root = root / "outputs" / "editorial_replay" / "decisions" / game
    if not decision_root.exists():
        return None
    matches: list[dict[str, Any]] = []
    for path in sorted(decision_root.rglob("*.editorial_decision.json")):
        payload = _read_json(path)
        if payload.get("schema_version") != EDITORIAL_DECISION_SCHEMA_VERSION:
            continue
        if str(payload.get("editorial_object_id") or "").strip() != editorial_object_id:
            continue
        if str(payload.get("review_surface") or "").strip() != review_surface:
            continue
        payload["manifest_path"] = str(path.resolve())
        matches.append(payload)
    if not matches:
        return None
    matches.sort(
        key=lambda row: (
            str(row.get("reviewed_at") or ""),
            str(row.get("created_at") or ""),
            str(row.get("decision_record_id") or ""),
        )
    )
    return matches[-1]


def _editorial_object_id(review_surface: str, game: str, source: str, event_id: str | None) -> str:
    payload = [review_surface.strip(), game.strip(), _canonical_source(source)]
    if event_id is not None:
        payload.append(event_id.strip())
    digest = hashlib.sha1("::".join(payload).encode("utf-8")).hexdigest()[:16]
    return f"editorial-{digest}"


def _decision_record_id(*, review_surface: str, editorial_object_id: str, review_session_id: str) -> str:
    digest = hashlib.sha1(
        "::".join([review_surface.strip(), editorial_object_id.strip(), review_session_id.strip()]).encode("utf-8")
    ).hexdigest()[:16]
    return f"decision-{digest}"


def _snapshot_id(*, workflow_run_id: str, editorial_object_id: str) -> str:
    digest = hashlib.sha1("::".join([workflow_run_id.strip(), editorial_object_id.strip()]).encode("utf-8")).hexdigest()[:16]
    return f"snapshot-{digest}"


def _identity_path(*, root: Path, game: str, editorial_object_id: str) -> Path:
    return root / "outputs" / "editorial_replay" / "identities" / game / f"{editorial_object_id}.editorial_identity.json"


def _decision_path(*, root: Path, game: str, decision_record_id: str) -> Path:
    return root / "outputs" / "editorial_replay" / "decisions" / game / f"{decision_record_id}.editorial_decision.json"


def _snapshot_path(*, root: Path, game: str, snapshot_id: str) -> Path:
    return root / "outputs" / "editorial_replay" / "snapshots" / game / f"{snapshot_id}.export_ready_snapshot.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_repo_root(repo_root: str | Path | None) -> Path:
    if repo_root is None:
        return REPO_ROOT
    return _resolve_path(repo_root)


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (Path.cwd() / candidate).resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _preserve_created_at(path: Path, field_name: str) -> str:
    existing = _read_json(path)
    preserved = str(existing.get(field_name) or "").strip()
    if preserved:
        return preserved
    return _utc_now()


def _load_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _canonical_source(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    candidate = Path(text).expanduser()
    try:
        if candidate.is_absolute():
            resolved = candidate.resolve()
            repo_relative = _repo_relative_path(resolved)
            return repo_relative or resolved.as_posix()

        repo_candidate = (REPO_ROOT / candidate).resolve()
        repo_relative = _repo_relative_path(repo_candidate)
        if repo_relative:
            return repo_relative
        return candidate.as_posix()
    except OSError:
        return candidate.as_posix()


def _repo_relative_path(path: Path) -> str | None:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return None
