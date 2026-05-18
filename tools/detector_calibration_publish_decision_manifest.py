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


SCHEMA_VERSION = "detector_calibration_publish_decision_manifest_v1"
TRIAGE_SCHEMA_VERSION = "detector_calibration_promotion_triage_manifest_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
DEFAULT_MIN_REPLAYS = 2
DEFAULT_MIN_SOURCES = 2
STATUS_ORDER = {
    "ready_to_publish": 0,
    "needs_broader_evidence": 1,
    "defer": 2,
}


def generate_detector_calibration_publish_decision_manifest(
    *,
    triage_manifest: str | Path,
    output_path: str | Path | None = None,
    minimum_publish_ready_replays_per_asset: int = DEFAULT_MIN_REPLAYS,
    minimum_distinct_sources_per_asset: int = DEFAULT_MIN_SOURCES,
) -> dict[str, Any]:
    triage_manifest_path = _resolve_path(triage_manifest)
    triage_payload = _load_json(triage_manifest_path)
    _validate_triage_manifest_payload(triage_payload)
    game = str(triage_payload.get("game") or "").strip()
    source_rows = triage_payload.get("rows") if isinstance(triage_payload.get("rows"), list) else []
    breadth_by_asset, promotion_context_by_path = _build_breadth_by_asset(source_rows)
    rows = [
        _derive_decision_row(
            row,
            breadth_by_asset=breadth_by_asset,
            promotion_context_by_path=promotion_context_by_path,
            minimum_publish_ready_replays_per_asset=minimum_publish_ready_replays_per_asset,
            minimum_distinct_sources_per_asset=minimum_distinct_sources_per_asset,
        )
        for row in source_rows
        if isinstance(row, dict)
    ]
    ordered_rows = sorted(rows, key=_decision_sort_key)
    status_counts = {
        "ready_to_publish": sum(1 for row in ordered_rows if row.get("decision_status") == "ready_to_publish"),
        "needs_broader_evidence": sum(1 for row in ordered_rows if row.get("decision_status") == "needs_broader_evidence"),
        "defer": sum(1 for row in ordered_rows if row.get("decision_status") == "defer"),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_triage_manifest_path": str(triage_manifest_path),
        "source_row_count": len(source_rows),
        "decision_thresholds": {
            "minimum_publish_ready_replays_per_asset": minimum_publish_ready_replays_per_asset,
            "minimum_distinct_sources_per_asset": minimum_distinct_sources_per_asset,
        },
        "row_count": len(ordered_rows),
        "status_counts": status_counts,
        "rows": ordered_rows,
    }
    target_output_path = _resolve_output_path(game=game, output_path=output_path)
    _write_json(target_output_path, manifest)
    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "source_triage_manifest_path": str(triage_manifest_path),
        "output_path": str(target_output_path),
        "source_row_count": len(source_rows),
        "row_count": len(ordered_rows),
        "status_counts": status_counts,
        "rows": ordered_rows,
        "emitted_manifest": {
            "path": str(target_output_path),
            "row_count": len(ordered_rows),
            "source_row_count": len(source_rows),
            "status_counts": status_counts,
            "top_row": ordered_rows[0] if ordered_rows else None,
        },
    }


def _build_breadth_by_asset(rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    breadth: dict[str, dict[str, Any]] = {}
    contexts: dict[str, dict[str, Any]] = {}
    for row in rows:
        promotion_record_path_raw = str(row.get("promotion_record_path") or "").strip()
        if not promotion_record_path_raw:
            continue
        contexts[promotion_record_path_raw] = {
            "promotion_record_ok": False,
            "source_key": None,
        }
        if str(row.get("triage_status") or "").strip() != "publish_ready":
            continue
        if str(row.get("draft_validation_status") or "").strip() != "ok":
            continue
        asset_id = str(row.get("asset_id") or "").strip()
        if not asset_id:
            continue
        promotion_payload = _load_json_safely(promotion_record_path_raw)
        if promotion_payload is None:
            continue
        source_key = _source_key_for_promotion(promotion_payload)
        contexts[promotion_record_path_raw] = {
            "promotion_record_ok": True,
            "source_key": source_key,
        }
        bucket = breadth.setdefault(asset_id, {"replay_count": 0, "source_keys": []})
        bucket["replay_count"] += 1
        if source_key and source_key not in bucket["source_keys"]:
            bucket["source_keys"].append(source_key)
    return breadth, contexts


def _derive_decision_row(
    row: dict[str, Any],
    *,
    breadth_by_asset: dict[str, dict[str, Any]],
    promotion_context_by_path: dict[str, dict[str, Any]],
    minimum_publish_ready_replays_per_asset: int,
    minimum_distinct_sources_per_asset: int,
) -> dict[str, Any]:
    asset_id = str(row.get("asset_id") or "").strip()
    breadth = breadth_by_asset.get(asset_id, {"replay_count": 0, "source_keys": []})
    replay_count = int(breadth.get("replay_count") or 0)
    source_keys = [str(item) for item in breadth.get("source_keys", []) if str(item or "").strip()]
    distinct_source_count = len(source_keys)
    promotion_record_path = str(row.get("promotion_record_path") or "").strip()
    promotion_context = promotion_context_by_path.get(
        promotion_record_path,
        {"promotion_record_ok": False, "source_key": None},
    )
    decision_status, decision_reason = _classify_decision_row(
        row,
        promotion_record_ok=bool(promotion_context.get("promotion_record_ok")),
        source_key=str(promotion_context.get("source_key") or "").strip() or None,
        replay_count_for_asset=replay_count,
        distinct_source_count_for_asset=distinct_source_count,
        minimum_publish_ready_replays_per_asset=minimum_publish_ready_replays_per_asset,
        minimum_distinct_sources_per_asset=minimum_distinct_sources_per_asset,
    )
    return {
        "decision_status": decision_status,
        "decision_reason": decision_reason,
        "triage_status": row.get("triage_status"),
        "triage_reason": row.get("triage_reason"),
        "promotion_record_path": row.get("promotion_record_path"),
        "promotion_root": row.get("promotion_root"),
        "promotion_id": row.get("promotion_id"),
        "created_at": row.get("created_at"),
        "asset_id": row.get("asset_id"),
        "candidate_id": row.get("candidate_id"),
        "run_id": row.get("run_id"),
        "delta_iou": row.get("delta_iou"),
        "absolute_delta_iou": row.get("absolute_delta_iou"),
        "difference_summary": row.get("difference_summary"),
        "draft_validation_status": row.get("draft_validation_status"),
        "replay_count_for_asset": replay_count,
        "distinct_source_count_for_asset": distinct_source_count,
        "source_keys_for_asset": source_keys,
    }


def _classify_decision_row(
    row: dict[str, Any],
    *,
    promotion_record_ok: bool,
    source_key: str | None,
    replay_count_for_asset: int,
    distinct_source_count_for_asset: int,
    minimum_publish_ready_replays_per_asset: int,
    minimum_distinct_sources_per_asset: int,
) -> tuple[str, str]:
    if str(row.get("triage_status") or "").strip() != "publish_ready":
        return "defer", "source_triage_not_publish_ready"
    if str(row.get("draft_validation_status") or "").strip() != "ok":
        return "defer", "draft_validation_not_ok"
    if not promotion_record_ok:
        return "defer", "missing_promotion_record_for_breadth_count"
    if not source_key:
        return "defer", "missing_source_key_for_breadth_count"
    if replay_count_for_asset < minimum_publish_ready_replays_per_asset:
        return "needs_broader_evidence", "publish_ready_but_single_evidence_base"
    if distinct_source_count_for_asset < minimum_distinct_sources_per_asset:
        return "needs_broader_evidence", "publish_ready_but_insufficient_distinct_sources"
    return "ready_to_publish", "publish_ready_with_sufficient_breadth"


def _source_key_for_promotion(payload: dict[str, Any]) -> str | None:
    source_evidence = payload.get("source_evidence")
    if not isinstance(source_evidence, dict):
        return None
    for key in ("source", "runtime_sidecar_path", "session_root"):
        value = str(source_evidence.get(key) or "").strip()
        if value:
            return value
    return None


def _validate_triage_manifest_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("triage manifest payload must be a mapping")
    if str(payload.get("schema_version") or "").strip() != TRIAGE_SCHEMA_VERSION:
        raise ValueError("triage manifest schema_version is invalid")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError("triage manifest rows must be a list")
    required = ("game", "row_count")
    missing = [field for field in required if field not in payload]
    if missing:
        raise ValueError(f"triage manifest missing required fields: {', '.join(missing)}")


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_OUTPUT_ROOT / game / "publish_decisions" / f"{timestamp}.detector_calibration_publish_decision_manifest.json"


def _decision_sort_key(row: dict[str, Any]) -> tuple[int, float, float, str]:
    status_rank = STATUS_ORDER.get(str(row.get("decision_status") or ""), len(STATUS_ORDER))
    absolute_delta = _safe_float(row.get("absolute_delta_iou"))
    created_at = _parse_iso_datetime(str(row.get("created_at") or "").strip())
    created_epoch = created_at.timestamp() if created_at is not None else 0.0
    return (
        status_rank,
        -(absolute_delta if absolute_delta is not None else -1.0),
        -created_epoch,
        f"{row.get('promotion_record_path') or ''}",
    )


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _load_json_safely(path: str | Path) -> dict[str, Any] | None:
    try:
        return _load_json(path)
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = _resolve_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration publish decision manifest")
    parser.add_argument("--triage-manifest", required=True)
    parser.add_argument("--output-path")
    parser.add_argument("--minimum-publish-ready-replays-per-asset", type=int, default=DEFAULT_MIN_REPLAYS)
    parser.add_argument("--minimum-distinct-sources-per-asset", type=int, default=DEFAULT_MIN_SOURCES)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_publish_decision_manifest(
        triage_manifest=args.triage_manifest,
        output_path=args.output_path,
        minimum_publish_ready_replays_per_asset=args.minimum_publish_ready_replays_per_asset,
        minimum_distinct_sources_per_asset=args.minimum_distinct_sources_per_asset,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
