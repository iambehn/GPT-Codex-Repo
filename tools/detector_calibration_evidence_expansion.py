from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCHEMA_VERSION = "detector_calibration_evidence_expansion_v1"
PUBLISH_DECISION_SCHEMA_VERSION = "detector_calibration_publish_decision_manifest_v1"
REVIEW_RECORD_SCHEMA_VERSION = "detector_calibration_review_v1"
PROMOTION_RECORD_SCHEMA_VERSION = "detector_calibration_revised_crop_promotion_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
ALLOWED_SOURCE_DECISIONS = {"needs_broader_evidence"}


def create_evidence_expansion(
    *,
    publish_decision_manifest: str | Path,
    asset_id: str,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(publish_decision_manifest)
    payload = _load_json(manifest_path)
    _validate_publish_decision_manifest(payload)
    target_asset_id = str(asset_id or "").strip()
    row = _find_asset_row(payload, target_asset_id)
    if row is None:
        return {"ok": False, "status": "unknown_asset_id", "error": f"asset_id '{target_asset_id}' was not found"}
    decision_status = str(row.get("decision_status") or "").strip()
    if decision_status not in ALLOWED_SOURCE_DECISIONS:
        return {
            "ok": False,
            "status": "ineligible_source_decision_status",
            "error": f"asset_id '{target_asset_id}' must come from {sorted(ALLOWED_SOURCE_DECISIONS)}",
        }

    game = str(payload.get("game") or "").strip()
    target_thresholds = dict(payload.get("decision_thresholds") or {}) if isinstance(payload.get("decision_thresholds"), dict) else {}
    evidence_snapshot = _build_evidence_snapshot(row)
    requested_evidence_items = _build_requested_evidence_items(
        row=row,
        target_thresholds=target_thresholds,
    )
    expansion_payload = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "asset_id": target_asset_id,
        "created_at": _utc_now(),
        "source_publish_decision_manifest_path": str(manifest_path),
        "source_publish_decision_row": row,
        "target_thresholds": target_thresholds,
        "current_evidence_snapshot": evidence_snapshot,
        "status": "planned",
        "requested_evidence_items": requested_evidence_items,
        "linked_session_roots": [],
        "linked_promotion_record_paths": [],
        "operator_notes": [],
    }
    target_output_path = _resolve_expansion_output_path(
        game=game,
        asset_id=target_asset_id,
        output_path=output_path,
    )
    _write_json(target_output_path, expansion_payload)
    return {
        "ok": True,
        "status": "ok",
        "expansion_manifest_path": str(target_output_path),
        "asset_id": target_asset_id,
        "requested_evidence_item_count": len(requested_evidence_items),
        "expansion_manifest": expansion_payload,
    }


def link_evidence_expansion_session(
    *,
    expansion_manifest_path: str | Path,
    session_root: str | Path,
) -> dict[str, Any]:
    manifest_path = _resolve_path(expansion_manifest_path)
    payload = _load_json(manifest_path)
    _validate_expansion_manifest(payload)
    resolved_session_root = _resolve_path(session_root)
    review_record_path = resolved_session_root / "review_record.json"
    if not review_record_path.is_file():
        return {"ok": False, "status": "missing_review_record", "error": f"review_record.json does not exist under {resolved_session_root}"}
    review_record = _load_json(review_record_path)
    if str(review_record.get("schema_version") or "").strip() != REVIEW_RECORD_SCHEMA_VERSION:
        return {"ok": False, "status": "invalid_review_record_schema", "error": "review record schema_version is invalid"}
    session_asset_id = str(_nested_value(review_record, "target", "asset_id") or "").strip()
    manifest_asset_id = str(payload.get("asset_id") or "").strip()
    if session_asset_id != manifest_asset_id:
        return {
            "ok": False,
            "status": "asset_id_mismatch",
            "error": f"session asset_id '{session_asset_id}' does not match expansion asset_id '{manifest_asset_id}'",
        }
    linked = payload.get("linked_session_roots") if isinstance(payload.get("linked_session_roots"), list) else []
    session_root_str = str(resolved_session_root)
    if session_root_str not in linked:
        linked.append(session_root_str)
    payload["linked_session_roots"] = linked
    if str(payload.get("status") or "").strip() == "planned":
        payload["status"] = "in_progress"
    _write_json(manifest_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "expansion_manifest_path": str(manifest_path),
        "linked_session_roots": payload["linked_session_roots"],
        "expansion_status": payload["status"],
    }


def link_evidence_expansion_promotion(
    *,
    expansion_manifest_path: str | Path,
    promotion_record_path: str | Path,
) -> dict[str, Any]:
    manifest_path = _resolve_path(expansion_manifest_path)
    payload = _load_json(manifest_path)
    _validate_expansion_manifest(payload)
    resolved_promotion_path = _resolve_path(promotion_record_path)
    if not resolved_promotion_path.is_file():
        return {"ok": False, "status": "missing_promotion_record", "error": f"promotion record does not exist: {resolved_promotion_path}"}
    promotion_record = _load_json(resolved_promotion_path)
    if str(promotion_record.get("schema_version") or "").strip() != PROMOTION_RECORD_SCHEMA_VERSION:
        return {"ok": False, "status": "invalid_promotion_record_schema", "error": "promotion record schema_version is invalid"}
    promotion_asset_id = str(_nested_value(promotion_record, "published_asset", "asset_id") or "").strip()
    manifest_asset_id = str(payload.get("asset_id") or "").strip()
    if promotion_asset_id != manifest_asset_id:
        return {
            "ok": False,
            "status": "asset_id_mismatch",
            "error": f"promotion asset_id '{promotion_asset_id}' does not match expansion asset_id '{manifest_asset_id}'",
        }
    linked = payload.get("linked_promotion_record_paths") if isinstance(payload.get("linked_promotion_record_paths"), list) else []
    promotion_path_str = str(resolved_promotion_path)
    if promotion_path_str not in linked:
        linked.append(promotion_path_str)
    payload["linked_promotion_record_paths"] = linked
    if str(payload.get("status") or "").strip() == "planned":
        payload["status"] = "in_progress"
    _write_json(manifest_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "expansion_manifest_path": str(manifest_path),
        "linked_promotion_record_paths": payload["linked_promotion_record_paths"],
        "expansion_status": payload["status"],
    }


def check_evidence_expansion(
    *,
    expansion_manifest_path: str | Path,
    publish_decision_manifest: str | Path,
    apply: bool = False,
) -> dict[str, Any]:
    manifest_path = _resolve_path(expansion_manifest_path)
    payload = _load_json(manifest_path)
    _validate_expansion_manifest(payload)
    decision_manifest_path = _resolve_path(publish_decision_manifest)
    decision_payload = _load_json(decision_manifest_path)
    _validate_publish_decision_manifest(decision_payload)
    asset_id = str(payload.get("asset_id") or "").strip()
    matched_row = _find_asset_row(decision_payload, asset_id)
    suggested_status = _suggest_expansion_status(
        current_status=str(payload.get("status") or "").strip(),
        matched_row=matched_row,
    )
    updated_snapshot = _build_evidence_snapshot(matched_row) if isinstance(matched_row, dict) else None
    result = {
        "ok": True,
        "status": "ok",
        "expansion_manifest_path": str(manifest_path),
        "publish_decision_manifest_path": str(decision_manifest_path),
        "asset_id": asset_id,
        "current_status": str(payload.get("status") or "").strip(),
        "suggested_status": suggested_status,
        "matched_publish_decision_row": matched_row,
        "updated_evidence_snapshot": updated_snapshot,
        "applied": False,
    }
    if apply:
        if updated_snapshot is not None:
            payload["current_evidence_snapshot"] = updated_snapshot
        payload["status"] = suggested_status
        _write_json(manifest_path, payload)
        result["applied"] = True
        result["persisted_status"] = payload["status"]
    return result


def abandon_evidence_expansion(
    *,
    expansion_manifest_path: str | Path,
    note: str | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(expansion_manifest_path)
    payload = _load_json(manifest_path)
    _validate_expansion_manifest(payload)
    notes = payload.get("operator_notes") if isinstance(payload.get("operator_notes"), list) else []
    normalized_note = str(note or "").strip()
    if normalized_note:
        notes.append(normalized_note)
    payload["operator_notes"] = notes
    payload["status"] = "abandoned"
    _write_json(manifest_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "expansion_manifest_path": str(manifest_path),
        "expansion_status": payload["status"],
        "operator_notes": payload["operator_notes"],
    }


def _build_requested_evidence_items(*, row: dict[str, Any], target_thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    replay_count = _safe_int(row.get("replay_count_for_asset")) or 0
    distinct_source_count = _safe_int(row.get("distinct_source_count_for_asset")) or 0
    min_replays = _safe_int(target_thresholds.get("minimum_publish_ready_replays_per_asset")) or 0
    min_sources = _safe_int(target_thresholds.get("minimum_distinct_sources_per_asset")) or 0
    if replay_count < min_replays:
        items.append(
            {
                "source_hint": None,
                "reason": "need_second_replay_backed_improvement",
                "status": "planned",
                "notes": None,
            }
        )
    if distinct_source_count < min_sources:
        items.append(
            {
                "source_hint": None,
                "reason": "need_second_distinct_source",
                "status": "planned",
                "notes": None,
            }
        )
    return items


def _build_evidence_snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    return {
        "decision_status": row.get("decision_status"),
        "decision_reason": row.get("decision_reason"),
        "replay_count_for_asset": row.get("replay_count_for_asset"),
        "distinct_source_count_for_asset": row.get("distinct_source_count_for_asset"),
        "source_keys_for_asset": list(row.get("source_keys_for_asset") or []) if isinstance(row.get("source_keys_for_asset"), list) else [],
    }


def _suggest_expansion_status(*, current_status: str, matched_row: dict[str, Any] | None) -> str:
    if current_status == "abandoned":
        return "abandoned"
    if isinstance(matched_row, dict) and str(matched_row.get("decision_status") or "").strip() == "ready_to_publish":
        return "satisfied"
    if current_status in {"in_progress", "satisfied"}:
        return "in_progress"
    if current_status == "planned":
        return "planned"
    return "planned"


def _find_asset_row(payload: dict[str, Any], asset_id: str) -> dict[str, Any] | None:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    target = str(asset_id or "").strip()
    for row in rows:
        if isinstance(row, dict) and str(row.get("asset_id") or "").strip() == target:
            return row
    return None


def _validate_publish_decision_manifest(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("publish decision manifest payload must be a mapping")
    if str(payload.get("schema_version") or "").strip() != PUBLISH_DECISION_SCHEMA_VERSION:
        raise ValueError("publish decision manifest schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("publish decision manifest rows must be a list")
    if not isinstance(payload.get("decision_thresholds"), dict):
        raise ValueError("publish decision manifest decision_thresholds must be a mapping")


def _validate_expansion_manifest(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("expansion manifest payload must be a mapping")
    if str(payload.get("schema_version") or "").strip() != SCHEMA_VERSION:
        raise ValueError("expansion manifest schema_version is invalid")


def _resolve_expansion_output_path(*, game: str, asset_id: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_OUTPUT_ROOT / game / "evidence_expansion" / _asset_slug(asset_id) / f"{timestamp}.detector_calibration_evidence_expansion.json"


def _asset_slug(asset_id: str) -> str:
    return asset_id.split(".")[-1] if asset_id else "asset"


def _nested_value(payload: dict[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = _resolve_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage detector calibration evidence expansion manifests")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("--publish-decision-manifest", required=True)
    create_parser.add_argument("--asset-id", required=True)
    create_parser.add_argument("--output-path")

    link_session_parser = subparsers.add_parser("link-session")
    link_session_parser.add_argument("--expansion-manifest-path", required=True)
    link_session_parser.add_argument("--session-root", required=True)

    link_promotion_parser = subparsers.add_parser("link-promotion")
    link_promotion_parser.add_argument("--expansion-manifest-path", required=True)
    link_promotion_parser.add_argument("--promotion-record-path", required=True)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--expansion-manifest-path", required=True)
    check_parser.add_argument("--publish-decision-manifest", required=True)
    check_parser.add_argument("--apply", action="store_true")

    abandon_parser = subparsers.add_parser("abandon")
    abandon_parser.add_argument("--expansion-manifest-path", required=True)
    abandon_parser.add_argument("--note")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "create":
        result = create_evidence_expansion(
            publish_decision_manifest=args.publish_decision_manifest,
            asset_id=args.asset_id,
            output_path=args.output_path,
        )
    elif args.command == "link-session":
        result = link_evidence_expansion_session(
            expansion_manifest_path=args.expansion_manifest_path,
            session_root=args.session_root,
        )
    elif args.command == "link-promotion":
        result = link_evidence_expansion_promotion(
            expansion_manifest_path=args.expansion_manifest_path,
            promotion_record_path=args.promotion_record_path,
        )
    elif args.command == "check":
        result = check_evidence_expansion(
            expansion_manifest_path=args.expansion_manifest_path,
            publish_decision_manifest=args.publish_decision_manifest,
            apply=bool(args.apply),
        )
    else:
        result = abandon_evidence_expansion(
            expansion_manifest_path=args.expansion_manifest_path,
            note=args.note,
        )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
