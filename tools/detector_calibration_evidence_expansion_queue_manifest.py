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


SCHEMA_VERSION = "detector_calibration_evidence_expansion_queue_manifest_v1"
EXPANSION_SCHEMA_VERSION = "detector_calibration_evidence_expansion_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
DEFAULT_EXPANSION_ROOT_NAME = "evidence_expansion"
STATUS_ORDER = {
    "in_progress": 0,
    "planned": 1,
    "satisfied": 2,
    "abandoned": 3,
}


def generate_detector_calibration_evidence_expansion_queue_manifest(
    *,
    game: str,
    evidence_expansion_root: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_root = (
        _resolve_path(evidence_expansion_root)
        if evidence_expansion_root is not None
        else DEFAULT_OUTPUT_ROOT / game / DEFAULT_EXPANSION_ROOT_NAME
    )
    expansion_manifest_paths = _discover_expansion_manifests(resolved_root)
    rows = [_derive_queue_row(path) for path in expansion_manifest_paths]
    ordered_rows = sorted(rows, key=_queue_sort_key)
    status_counts = {
        "in_progress": sum(1 for row in ordered_rows if row.get("status") == "in_progress"),
        "planned": sum(1 for row in ordered_rows if row.get("status") == "planned"),
        "satisfied": sum(1 for row in ordered_rows if row.get("status") == "satisfied"),
        "abandoned": sum(1 for row in ordered_rows if row.get("status") == "abandoned"),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "game": game,
        "generated_at": _utc_now(),
        "source_manifest_count": len(expansion_manifest_paths),
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
        "evidence_expansion_root": str(resolved_root),
        "output_path": str(target_output_path),
        "source_manifest_count": len(expansion_manifest_paths),
        "row_count": len(ordered_rows),
        "status_counts": status_counts,
        "rows": ordered_rows,
        "emitted_manifest": {
            "path": str(target_output_path),
            "row_count": len(ordered_rows),
            "source_manifest_count": len(expansion_manifest_paths),
            "status_counts": status_counts,
            "top_row": ordered_rows[0] if ordered_rows else None,
        },
    }


def _derive_queue_row(expansion_manifest_path: Path) -> dict[str, Any]:
    payload = _load_json(expansion_manifest_path)
    if str(payload.get("schema_version") or "").strip() != EXPANSION_SCHEMA_VERSION:
        raise ValueError(
            f"evidence expansion manifest at {expansion_manifest_path} has invalid schema_version"
        )
    source_row = payload.get("source_publish_decision_row") if isinstance(payload.get("source_publish_decision_row"), dict) else {}
    return {
        "status": str(payload.get("status") or "").strip() or None,
        "asset_id": str(payload.get("asset_id") or "").strip() or None,
        "expansion_manifest_path": str(expansion_manifest_path.resolve()),
        "created_at": str(payload.get("created_at") or "").strip() or None,
        "source_publish_decision_manifest_path": str(payload.get("source_publish_decision_manifest_path") or "").strip() or None,
        "decision_status": str(source_row.get("decision_status") or "").strip() or None,
        "decision_reason": str(source_row.get("decision_reason") or "").strip() or None,
        "replay_count_for_asset": _safe_int(source_row.get("replay_count_for_asset")),
        "distinct_source_count_for_asset": _safe_int(source_row.get("distinct_source_count_for_asset")),
        "requested_evidence_item_count": len(payload.get("requested_evidence_items")) if isinstance(payload.get("requested_evidence_items"), list) else 0,
        "linked_session_count": len(payload.get("linked_session_roots")) if isinstance(payload.get("linked_session_roots"), list) else 0,
        "linked_promotion_count": len(payload.get("linked_promotion_record_paths")) if isinstance(payload.get("linked_promotion_record_paths"), list) else 0,
    }


def _discover_expansion_manifests(root: Path) -> list[Path]:
    if not root.exists():
        return []
    discovered = [
        path.resolve()
        for path in root.rglob("*.detector_calibration_evidence_expansion.json")
    ]
    return sorted(discovered, key=lambda item: str(item))


def _queue_sort_key(row: dict[str, Any]) -> tuple[int, int, float, str]:
    status_rank = STATUS_ORDER.get(str(row.get("status") or ""), len(STATUS_ORDER))
    requested_count = _safe_int(row.get("requested_evidence_item_count")) or 0
    created_at = _parse_iso_datetime(str(row.get("created_at") or "").strip())
    created_epoch = created_at.timestamp() if created_at is not None else 0.0
    return (
        status_rank,
        -requested_count,
        -created_epoch,
        f"{row.get('expansion_manifest_path') or ''}",
    )


def _resolve_output_path(*, game: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_OUTPUT_ROOT / game / "evidence_expansion_queue" / f"{timestamp}.detector_calibration_evidence_expansion_queue_manifest.json"


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = _resolve_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate detector calibration evidence expansion queue manifest")
    parser.add_argument("--game", required=True)
    parser.add_argument("--evidence-expansion-root")
    parser.add_argument("--output-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = generate_detector_calibration_evidence_expansion_queue_manifest(
        game=args.game,
        evidence_expansion_root=args.evidence_expansion_root,
        output_path=args.output_path,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
