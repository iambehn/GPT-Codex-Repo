from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import load_yaml_file

_IDENTITY_BLOCKING_QA_TYPES = {
    "ambiguous_identity_match",
    "conflicting_identity_match",
    "identity_match_rejected",
}
_IDENTITY_INFO_QA_TYPES = {"canonical_identity_preference_applied"}


def summarize_onboarding_batch(
    root: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root).expanduser().resolve()
    if not root_path.exists():
        raise FileNotFoundError(f"onboarding batch root does not exist: {root_path}")

    draft_roots = _discover_draft_roots(root_path)
    if game:
        requested_game = str(game).strip()
        draft_roots = [path for path in draft_roots if _draft_game_id(path) == requested_game]
    else:
        requested_game = ""

    drafts = [_summarize_draft(path) for path in draft_roots]
    _attach_draft_comparisons(drafts)
    payload = {
        "ok": True,
        "root": str(root_path),
        "game_filter": requested_game or None,
        "draft_count": len(drafts),
        "summary": _build_batch_summary(drafts),
        "drafts": drafts,
    }
    if output_path is not None:
        report_path = Path(output_path).expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        payload["output_path"] = str(report_path)
    return payload


def _discover_draft_roots(root: Path) -> list[Path]:
    if (root / "manifests" / "onboarding_state.json").exists():
        return [root]
    discovered = {
        path.parent.parent.resolve()
        for path in root.rglob("manifests/onboarding_state.json")
        if path.is_file()
    }
    return sorted(discovered, key=lambda item: (str(item.parent), str(item.name)))


def _draft_game_id(draft_root: Path) -> str:
    state_path = draft_root / "manifests" / "onboarding_state.json"
    if state_path.exists():
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return ""
        return str(payload.get("game_id", "")).strip()
    return ""


def _summarize_draft(draft_root: Path) -> dict[str, Any]:
    state = _load_json_required(draft_root / "manifests" / "onboarding_state.json", label="onboarding state")
    assets_manifest = _load_json_required(draft_root / "manifests" / "assets_manifest.json", label="assets manifest")
    detection_manifest = _load_yaml_mapping(draft_root / "manifests" / "detection_manifest.yaml", label="detection manifest")
    ontology = _load_yaml_mapping(draft_root / "entities.yaml", label="entities")

    heroes = ontology.get("heroes", [])
    abilities = ontology.get("abilities", [])
    events = ontology.get("events", [])
    if not isinstance(heroes, list) or not isinstance(abilities, list) or not isinstance(events, list):
        raise ValueError(f"draft ontology must define heroes, abilities, and events lists: {draft_root}")

    candidates = assets_manifest.get("candidates", [])
    bindings = _read_csv_rows(draft_root / "catalog" / "bindings.csv")
    qa_rows = _read_csv_rows(draft_root / "catalog" / "qa_queue.csv")
    source_fetch_log = assets_manifest.get("source_fetch_log", [])
    if not isinstance(source_fetch_log, list):
        raise ValueError(f"draft assets manifest source_fetch_log must be a list: {draft_root}")

    detection_rows = detection_manifest.get("rows", [])
    if not isinstance(detection_rows, list):
        raise ValueError(f"draft detection manifest rows must be a list: {draft_root}")

    game = str(state.get("game_id", assets_manifest.get("game_id", ""))).strip()
    phase_status = str(state.get("phase_status", assets_manifest.get("phase_status", "unknown"))).strip() or "unknown"
    qa_by_type = _count_by_field(qa_rows, "item_type")
    binding_status_counts = _count_by_field(bindings, "status")
    source_status_counts = _count_by_field(source_fetch_log, "status")
    published_root = draft_root.parent.parent.parent if game else None
    readiness_report = validate_onboarding_publish(draft_root)
    identity_summary = _build_identity_summary(qa_by_type, readiness_report)

    summary = {
        "game": game,
        "draft_root": str(draft_root),
        "phase_status": phase_status,
        "publish_readiness": "published"
        if bool(published_root and (published_root / "manifests" / "detection_manifest.yaml").exists())
        else str(readiness_report.get("readiness", "unknown")),
        "can_publish": bool(readiness_report.get("can_publish", False)),
        "published_pack_present": bool(published_root and (published_root / "manifests" / "detection_manifest.yaml").exists()),
        "updated_at": str(state.get("updated_at", "")),
        "source_count": int(state.get("source_count", assets_manifest.get("source_count", 0)) or 0),
        "source_status_counts": source_status_counts,
        "ontology_counts": {
            "heroes": len(heroes),
            "abilities": len(abilities),
            "events": len(events),
            "total": len(heroes) + len(abilities) + len(events),
        },
        "detection_counts": {
            "rows": int(detection_manifest.get("row_count", len(detection_rows)) or 0),
            "required_rows": int(detection_manifest.get("required_row_count", 0) or 0),
            "ready_rows": int(detection_manifest.get("ready_row_count", 0) or 0),
            "rows_needing_assets": int(detection_manifest.get("rows_needing_assets", 0) or 0),
        },
        "candidate_counts": {
            "assets": len(candidates) if isinstance(candidates, list) else 0,
            "by_quality": _count_by_field(candidates if isinstance(candidates, list) else [], "candidate_quality"),
            "by_family": _count_by_field(candidates if isinstance(candidates, list) else [], "asset_family"),
        },
        "binding_counts": {
            "total": len(bindings),
            "by_status": binding_status_counts,
            "accepted": int(binding_status_counts.get("accepted", 0)),
        },
        "qa_counts": {
            "total": len(qa_rows),
            "by_type": qa_by_type,
        },
        "identity_summary": identity_summary,
        "artifact_paths": {
            "onboarding_state": str(draft_root / "manifests" / "onboarding_state.json"),
            "assets_manifest": str(draft_root / "manifests" / "assets_manifest.json"),
            "detection_manifest": str(draft_root / "manifests" / "detection_manifest.yaml"),
            "bindings_csv": str(draft_root / "catalog" / "bindings.csv"),
            "qa_queue_csv": str(draft_root / "catalog" / "qa_queue.csv"),
            "source_fetch_log_csv": str(draft_root / "catalog" / "source_fetch_log.csv"),
        },
        "readiness_counts": dict(readiness_report.get("counts", {})),
    }
    return summary


def _build_batch_summary(drafts: list[dict[str, Any]]) -> dict[str, Any]:
    by_game: dict[str, dict[str, Any]] = {}
    readiness_counts: dict[str, int] = {}
    aggregate_identity_counts: dict[str, int] = {}
    aggregate_identity_blocking = 0
    aggregate_identity_info = 0
    for draft in drafts:
        readiness = str(draft.get("publish_readiness", "unknown"))
        readiness_counts[readiness] = readiness_counts.get(readiness, 0) + 1
        game = str(draft.get("game", ""))
        draft_identity_summary = draft.get("identity_summary", {})
        if isinstance(draft_identity_summary, dict):
            aggregate_identity_blocking += _coerce_int(draft_identity_summary.get("blocking_total"))
            aggregate_identity_info += _coerce_int(draft_identity_summary.get("informational_total"))
            identity_by_type = draft_identity_summary.get("by_type", {})
            if isinstance(identity_by_type, dict):
                for key, value in identity_by_type.items():
                    aggregate_identity_counts[str(key)] = aggregate_identity_counts.get(str(key), 0) + _coerce_int(value)
        game_summary = by_game.setdefault(
            game,
            {
                "draft_count": 0,
                "latest_updated_at": "",
                "latest_draft_root": "",
                "previous_updated_at": "",
                "previous_draft_root": "",
                "readiness_counts": {},
                "qa_total": 0,
                "identity_summary": {
                    "identity_blocked": False,
                    "blocking_total": 0,
                    "informational_total": 0,
                    "by_type": {},
                },
                "comparison": None,
            },
        )
        game_summary["draft_count"] += 1
        game_summary["qa_total"] += int(draft.get("qa_counts", {}).get("total", 0) or 0)
        game_summary["readiness_counts"][readiness] = game_summary["readiness_counts"].get(readiness, 0) + 1
        updated_at = str(draft.get("updated_at", ""))
        if updated_at >= str(game_summary["latest_updated_at"]):
            game_summary["previous_updated_at"] = str(game_summary["latest_updated_at"])
            game_summary["previous_draft_root"] = str(game_summary["latest_draft_root"])
            game_summary["latest_updated_at"] = updated_at
            game_summary["latest_draft_root"] = str(draft.get("draft_root", ""))
            game_summary["identity_summary"] = draft_identity_summary
            game_summary["comparison"] = draft.get("comparison_to_previous")
        elif updated_at >= str(game_summary["previous_updated_at"]):
            game_summary["previous_updated_at"] = updated_at
            game_summary["previous_draft_root"] = str(draft.get("draft_root", ""))
    return {
        "games": by_game,
        "readiness_counts": readiness_counts,
        "identity_summary": {
            "blocking_total": aggregate_identity_blocking,
            "informational_total": aggregate_identity_info,
            "by_type": aggregate_identity_counts,
        },
        "published_game_count": sum(1 for draft in drafts if draft.get("published_pack_present")),
        "qa_total": sum(int(draft.get("qa_counts", {}).get("total", 0) or 0) for draft in drafts),
    }


def _attach_draft_comparisons(drafts: list[dict[str, Any]]) -> None:
    drafts_by_game: dict[str, list[dict[str, Any]]] = {}
    for draft in drafts:
        drafts_by_game.setdefault(str(draft.get("game", "")), []).append(draft)
    for grouped in drafts_by_game.values():
        ordered = sorted(grouped, key=lambda row: (str(row.get("updated_at", "")), str(row.get("draft_root", ""))))
        previous: dict[str, Any] | None = None
        for draft in ordered:
            draft["comparison_to_previous"] = _build_draft_comparison(previous, draft) if previous else None
            previous = draft


def _build_draft_comparison(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    previous_readiness = str(previous.get("publish_readiness", "unknown"))
    current_readiness = str(current.get("publish_readiness", "unknown"))
    return {
        "previous_draft_root": str(previous.get("draft_root", "")),
        "previous_updated_at": str(previous.get("updated_at", "")),
        "readiness_changed": current_readiness != previous_readiness,
        "previous_publish_readiness": previous_readiness,
        "current_publish_readiness": current_readiness,
        "delta": {
            "source_failures": _nested_count_delta(current, previous, "source_status_counts", "fetch_failed"),
            "empty_sources": _nested_count_delta(current, previous, "source_status_counts", "empty_source"),
            "weak_source_extraction": _nested_count_delta(current, previous, "qa_counts", "by_type", "weak_source_extraction"),
            "weak_image_anchor": _nested_count_delta(current, previous, "qa_counts", "by_type", "weak_image_anchor"),
            "conflicting_image_anchor": _nested_count_delta(current, previous, "qa_counts", "by_type", "conflicting_image_anchor"),
            "filename_only_anchor": _nested_count_delta(current, previous, "qa_counts", "by_type", "filename_only_anchor"),
            "ambiguous_identity_match": _nested_count_delta(current, previous, "identity_summary", "by_type", "ambiguous_identity_match"),
            "conflicting_identity_match": _nested_count_delta(current, previous, "identity_summary", "by_type", "conflicting_identity_match"),
            "identity_match_rejected": _nested_count_delta(current, previous, "identity_summary", "by_type", "identity_match_rejected"),
            "canonical_identity_preference_applied": _nested_count_delta(current, previous, "identity_summary", "by_type", "canonical_identity_preference_applied"),
            "identity_blocking_total": _nested_count_delta(current, previous, "identity_summary", "blocking_total"),
            "identity_informational_total": _nested_count_delta(current, previous, "identity_summary", "informational_total"),
            "total_qa": _nested_count_delta(current, previous, "qa_counts", "total"),
            "heroes": _nested_count_delta(current, previous, "ontology_counts", "heroes"),
            "abilities": _nested_count_delta(current, previous, "ontology_counts", "abilities"),
            "events": _nested_count_delta(current, previous, "ontology_counts", "events"),
            "candidate_assets": _nested_count_delta(current, previous, "candidate_counts", "assets"),
            "high_quality_candidates": _nested_count_delta(current, previous, "candidate_counts", "by_quality", "high"),
            "medium_quality_candidates": _nested_count_delta(current, previous, "candidate_counts", "by_quality", "medium"),
            "low_quality_candidates": _nested_count_delta(current, previous, "candidate_counts", "by_quality", "low"),
            "accepted_bindings": _nested_count_delta(current, previous, "binding_counts", "accepted"),
        },
    }


def _nested_count_delta(current: dict[str, Any], previous: dict[str, Any], *path: str) -> int:
    current_value = _coerce_int(_nested_get(current, *path))
    previous_value = _coerce_int(_nested_get(previous, *path))
    return current_value - previous_value


def _nested_get(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return 0
        current = current.get(key, 0)
    return current


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _build_identity_summary(qa_by_type: dict[str, int], readiness_report: dict[str, Any]) -> dict[str, Any]:
    by_type = {
        qa_type: _coerce_int(qa_by_type.get(qa_type, 0))
        for qa_type in sorted(_IDENTITY_BLOCKING_QA_TYPES | _IDENTITY_INFO_QA_TYPES)
    }
    identity_blocker_counts = _identity_blocker_counts(readiness_report)
    return {
        "identity_blocked": bool(identity_blocker_counts),
        "blocking_total": sum(identity_blocker_counts.values()),
        "informational_total": sum(by_type.get(qa_type, 0) for qa_type in _IDENTITY_INFO_QA_TYPES),
        "by_type": by_type,
    }


def _identity_blocker_counts(readiness_report: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in readiness_report.get("findings", []):
        if not isinstance(finding, dict):
            continue
        finding_type = str(finding.get("type", "")).strip()
        if finding_type in _IDENTITY_BLOCKING_QA_TYPES:
            counts[finding_type] = counts.get(finding_type, 0) + 1
    return counts
def _load_json_required(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"draft is missing {label}: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"draft {label} must be a mapping: {path}")
    return payload


def _load_yaml_mapping(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"draft is missing {label}: {path}")
    payload = load_yaml_file(path)
    if not isinstance(payload, dict):
        raise ValueError(f"draft {label} must be a mapping: {path}")
    return payload


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    if len(rows) == 1 and set(rows[0].keys()) == {"empty"}:
        return []
    return rows


def _count_by_field(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field, "")).strip() or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return counts
