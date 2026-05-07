from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.clip_registry import query_clip_registry


REPO_ROOT = Path(__file__).resolve().parent.parent
APPROVAL_TARGET_DATASET_SCHEMA_VERSION = "approval_target_dataset_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "approval_target_datasets"


def build_approval_target_dataset(
    *,
    registry_path: str | Path | None,
    game: str,
    platform: str | None = None,
    evidence_mode: str | None = None,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_evidence_mode = _normalize_evidence_mode(evidence_mode)
    if normalized_evidence_mode is None:
        return {
            "ok": False,
            "status": "unsupported_evidence_mode",
            "error": f"unsupported evidence mode: {evidence_mode}",
        }

    filters = {
        "game": game,
        "platform": platform,
        "evidence_mode": normalized_evidence_mode,
    }
    resolved = _resolve_registry_rows(
        registry_path=registry_path,
        game=game,
        platform=platform,
        evidence_mode=normalized_evidence_mode,
    )
    if not resolved["ok"]:
        return resolved

    rows = [
        dataset_row
        for row in resolved["candidate_rows"]
        if (dataset_row := _approval_dataset_row(
            row,
            fused_by_key=resolved["fused_by_key"],
            hooks_by_candidate=resolved["hooks_by_candidate"],
            candidate_evidence_modes=resolved["candidate_evidence_modes"],
            platform_filtered_candidate_ids=resolved["platform_filtered_candidate_ids"],
            requested_evidence_mode=normalized_evidence_mode,
            platform=platform,
        )) is not None
    ]
    rows.sort(key=lambda row: (str(row.get("fixture_id") or ""), str(row.get("candidate_id") or "")))

    dataset_id = _approval_dataset_id(resolved["registry_path"], filters)
    summary = _approval_dataset_summary(rows)
    artifact = {
        "ok": summary["status"] != "invalid_registry",
        "status": summary["status"],
        "schema_version": APPROVAL_TARGET_DATASET_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "created_at": datetime.now(UTC).isoformat(),
        "registry_path": resolved["registry_path"],
        "filters": {key: value for key, value in filters.items() if value is not None},
        "row_count": summary["row_count"],
        "positive_count": summary["positive_count"],
        "negative_count": summary["negative_count"],
        "training_ready": summary["training_ready"],
        "readiness_reason": summary["readiness_reason"],
        "rows": rows,
        "warnings": [],
    }

    manifest_path = _default_output_path(
        output_root=output_root,
        output_path=output_path,
        game=game,
        dataset_id=dataset_id,
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    csv_path = manifest_path.with_suffix(".csv")
    _write_csv(csv_path, rows)
    artifact["manifest_path"] = str(manifest_path)
    artifact["csv_path"] = str(csv_path)
    return artifact


def _resolve_registry_rows(
    *,
    registry_path: str | Path | None,
    game: str,
    platform: str | None,
    evidence_mode: str,
) -> dict[str, Any]:
    candidate_result = query_clip_registry(
        mode="candidate-lifecycles",
        registry_path=registry_path,
        game=game,
    )
    if not candidate_result.get("ok"):
        return {
            "ok": False,
            "status": "invalid_registry",
            "registry_path": candidate_result.get("registry_path"),
            "error": candidate_result.get("error"),
        }

    hook_result = query_clip_registry(
        mode="hook-candidates",
        registry_path=registry_path,
        game=game,
    )
    if not hook_result.get("ok"):
        return {
            "ok": False,
            "status": "invalid_registry",
            "registry_path": hook_result.get("registry_path"),
            "error": hook_result.get("error"),
        }

    fused_result = query_clip_registry(
        mode="fused-events",
        registry_path=registry_path,
        game=game,
    )
    if not fused_result.get("ok"):
        return {
            "ok": False,
            "status": "invalid_registry",
            "registry_path": fused_result.get("registry_path"),
            "error": fused_result.get("error"),
        }

    post_result = query_clip_registry(
        mode="post-ledger-records",
        registry_path=registry_path,
        game=game,
        platform=platform,
    )
    if not post_result.get("ok"):
        return {
            "ok": False,
            "status": "invalid_registry",
            "registry_path": post_result.get("registry_path"),
            "error": post_result.get("error"),
        }

    metrics_result = query_clip_registry(
        mode="posted-metrics",
        registry_path=registry_path,
        game=game,
        platform=platform,
    )
    if not metrics_result.get("ok"):
        return {
            "ok": False,
            "status": "invalid_registry",
            "registry_path": metrics_result.get("registry_path"),
            "error": metrics_result.get("error"),
        }

    hooks_by_candidate: dict[str, list[dict[str, Any]]] = {}
    for row in hook_result["rows"]:
        candidate_id = str(row.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        hooks_by_candidate.setdefault(candidate_id, []).append(row)

    fused_by_key = {
        (str(row.get("event_id") or ""), str(row.get("sidecar_path") or "")): row
        for row in fused_result["rows"]
        if row.get("event_id") and row.get("sidecar_path")
    }

    platform_filtered_candidate_ids = {
        str(row.get("candidate_id") or "").strip()
        for row in [*post_result["rows"], *metrics_result["rows"]]
        if str(row.get("candidate_id") or "").strip()
    }
    candidate_evidence_modes = _candidate_evidence_modes(
        candidate_rows=candidate_result["rows"],
        post_rows=post_result["rows"],
        metrics_rows=metrics_result["rows"],
    )
    return {
        "ok": True,
        "status": "ok",
        "registry_path": candidate_result["registry_path"],
        "candidate_rows": candidate_result["rows"],
        "hooks_by_candidate": hooks_by_candidate,
        "fused_by_key": fused_by_key,
        "candidate_evidence_modes": candidate_evidence_modes,
        "platform_filtered_candidate_ids": platform_filtered_candidate_ids,
    }


def _approval_dataset_row(
    row: dict[str, Any],
    *,
    fused_by_key: dict[tuple[str, str], dict[str, Any]],
    hooks_by_candidate: dict[str, list[dict[str, Any]]],
    candidate_evidence_modes: dict[str, str],
    platform_filtered_candidate_ids: set[str],
    requested_evidence_mode: str,
    platform: str | None,
) -> dict[str, Any] | None:
    candidate_id = str(row.get("candidate_id") or "").strip()
    if not candidate_id:
        return None
    if platform is not None and candidate_id not in platform_filtered_candidate_ids:
        return None

    label = _approval_label_from_registry_row(row)
    if label is None:
        return None

    evidence = candidate_evidence_modes.get(candidate_id, "real_only")
    if requested_evidence_mode != "mixed" and evidence != requested_evidence_mode:
        return None

    selected_highlight = _selected_highlight_details(row)
    preferred_hook = _preferred_hook_row(hooks_by_candidate.get(candidate_id, []))
    fused = fused_by_key.get((str(row.get("event_id") or ""), str(row.get("fused_sidecar_path") or "")), {})
    return {
        "candidate_id": candidate_id,
        "game": row.get("game"),
        "source": row.get("source"),
        "fixture_id": row.get("fixture_id"),
        "event_id": row.get("event_id"),
        "review_outcome": row.get("latest_review_status"),
        "lifecycle_state": row.get("lifecycle_state"),
        "approval_label": label["approval_label"],
        "label_source": label["label_source"],
        "final_score": row.get("final_score"),
        "fused_confidence": fused.get("confidence"),
        "hook_strength": preferred_hook.get("hook_strength") if preferred_hook else None,
        "hook_mode": preferred_hook.get("hook_mode") if preferred_hook else None,
        "hook_archetype": preferred_hook.get("hook_archetype") if preferred_hook else None,
        "selected_highlight_event_type": selected_highlight.get("event_type"),
        "selected_highlight_fusion_id": selected_highlight.get("fusion_id"),
        "evidence_mode": evidence,
        "fused_sidecar_path": row.get("fused_sidecar_path"),
        "highlight_selection_manifest_path": row.get("highlight_selection_manifest_path"),
        "transitions": _parse_json_list(row.get("transitions_json")),
    }


def _approval_label_from_registry_row(row: dict[str, Any]) -> dict[str, Any] | None:
    review_outcome = str(row.get("latest_review_status") or "").strip().lower()
    lifecycle_state = str(row.get("lifecycle_state") or "").strip().lower()
    if review_outcome == "approved":
        return {"approval_label": 1.0, "label_source": "review_outcome"}
    if review_outcome == "rejected":
        return {"approval_label": 0.0, "label_source": "review_outcome"}
    if lifecycle_state in {"approved", "selected_for_export"}:
        return {"approval_label": 1.0, "label_source": "lifecycle_state"}
    if lifecycle_state in {"rejected", "invalidated", "superseded"}:
        return {"approval_label": 0.0, "label_source": "lifecycle_state"}
    return None


def _approval_dataset_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive_count = sum(1 for row in rows if float(row.get("approval_label") or 0.0) >= 0.5)
    negative_count = sum(1 for row in rows if float(row.get("approval_label") or 0.0) < 0.5)
    row_count = len(rows)
    if row_count == 0:
        return {
            "status": "no_rows",
            "row_count": 0,
            "positive_count": 0,
            "negative_count": 0,
            "training_ready": False,
            "readiness_reason": "no_rows",
        }
    if positive_count == 0:
        readiness_reason = "no_positive_labels"
    elif negative_count == 0:
        readiness_reason = "no_negative_labels"
    else:
        readiness_reason = "ready"
    return {
        "status": "ok",
        "row_count": row_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "training_ready": readiness_reason == "ready",
        "readiness_reason": readiness_reason,
    }


def _approval_dataset_id(registry_path: str, filters: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "registry_path": registry_path,
            "filters": {key: value for key, value in filters.items() if value is not None},
        },
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def _default_output_path(
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    game: str,
    dataset_id: str,
) -> Path:
    if output_path is not None:
        return Path(output_path).expanduser().resolve()
    root = DEFAULT_OUTPUT_ROOT if output_root is None else Path(output_root).expanduser().resolve()
    return root / game / f"approval-target-{dataset_id}.manifest.json"


def _preferred_hook_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return max(
        rows,
        key=lambda row: (
            float(row.get("hook_strength") or 0.0),
            str(row.get("hook_id") or ""),
        ),
    )


def _selected_highlight_details(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("selected_highlight_details_json")
    if not payload:
        return {}
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return {}
    elif isinstance(payload, dict):
        parsed = payload
    else:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _candidate_evidence_modes(
    *,
    candidate_rows: list[dict[str, Any]],
    post_rows: list[dict[str, Any]],
    metrics_rows: list[dict[str, Any]],
) -> dict[str, str]:
    post_by_candidate: dict[str, list[dict[str, Any]]] = {}
    metrics_by_candidate: dict[str, list[dict[str, Any]]] = {}
    for row in post_rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        if candidate_id:
            post_by_candidate.setdefault(candidate_id, []).append(row)
    for row in metrics_rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        if candidate_id:
            metrics_by_candidate.setdefault(candidate_id, []).append(row)

    result: dict[str, str] = {}
    for row in candidate_rows:
        candidate_id = str(row.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        modes = {
            *[_post_row_evidence_mode(post) for post in post_by_candidate.get(candidate_id, [])],
            *[_metrics_row_evidence_mode(metric) for metric in metrics_by_candidate.get(candidate_id, [])],
        }
        if "synthetic_augmented" in modes:
            result[candidate_id] = "synthetic_augmented"
        else:
            result[candidate_id] = "real_only"
    return result


def _normalize_evidence_mode(value: str | None) -> str | None:
    normalized = str(value or "mixed").strip().lower()
    return normalized if normalized in {"mixed", "real_only", "synthetic_augmented"} else None


def _post_row_evidence_mode(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return "real_only"
    caption_ref = str(row.get("caption_ref") or "").strip().lower()
    caption_text = str(row.get("caption_text") or "").strip().lower()
    media_asset_path = str(row.get("media_asset_path") or "").strip().lower()
    synthetic_markers = ("synthetic", "augmented", "generated")
    if any(marker in value for marker in synthetic_markers for value in (caption_ref, caption_text, media_asset_path)):
        return "synthetic_augmented"
    return "real_only"


def _metrics_row_evidence_mode(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return "real_only"
    normalized = str(row.get("evidence_mode") or "").strip().lower()
    if normalized in {"real_only", "synthetic_augmented"}:
        return normalized
    return "real_only"


def _parse_json_list(value: Any) -> list[dict[str, Any]]:
    if not value:
        return []
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return [row for row in parsed if isinstance(row, dict)] if isinstance(parsed, list) else []


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "candidate_id",
        "game",
        "source",
        "fixture_id",
        "event_id",
        "review_outcome",
        "lifecycle_state",
        "approval_label",
        "label_source",
        "final_score",
        "fused_confidence",
        "hook_strength",
        "hook_mode",
        "hook_archetype",
        "selected_highlight_event_type",
        "selected_highlight_fusion_id",
        "evidence_mode",
        "fused_sidecar_path",
        "highlight_selection_manifest_path",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
