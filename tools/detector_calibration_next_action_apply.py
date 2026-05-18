from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.artifact_paths import (
    resolve_path as _artifact_resolve_path,
    resolve_timestamped_output_path as _artifact_resolve_timestamped_output_path,
    utc_timestamp_slug as _artifact_utc_timestamp_slug,
)
from tools.detector_calibration_evidence_expansion import (
    SCHEMA_VERSION as EXPANSION_SCHEMA_VERSION,
    create_evidence_expansion,
)
from tools.detector_calibration_next_actions_manifest import (
    SCHEMA_VERSION as NEXT_ACTIONS_SCHEMA_VERSION,
)


PROGRESS_SCHEMA_VERSION = "detector_calibration_evidence_expansion_progress_manifest_v1"
PUBLISH_DECISION_SCHEMA_VERSION = "detector_calibration_publish_decision_manifest_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"
REUSABLE_STATUSES = {"in_progress", "planned"}
REUSE_STATUS_ORDER = {"in_progress": 0, "planned": 1}


def apply_detector_calibration_next_action(
    *,
    next_actions_manifest: str | Path,
    asset_id: str,
    publish_decision_manifest: str | Path | None = None,
    evidence_expansion_root: str | Path | None = None,
) -> dict[str, Any]:
    next_actions_path = _resolve_path(next_actions_manifest)
    next_actions_payload = _load_json(next_actions_path)
    _validate_next_actions_manifest(next_actions_payload)
    target_asset_id = str(asset_id or "").strip()
    next_action_row = _find_asset_row(next_actions_payload.get("rows"), target_asset_id)
    if next_action_row is None:
        return {
            "ok": False,
            "status": "unknown_asset_id",
            "error": f"asset_id '{target_asset_id}' was not found",
        }
    action_status = str(next_action_row.get("action_status") or "").strip()
    if action_status != "collect_more_evidence":
        return {
            "ok": False,
            "status": "unsupported_action_status",
            "asset_id": target_asset_id,
            "action_status": action_status or None,
            "error": "next-action row is not actionable by this tool",
        }

    resolved_publish_decision_manifest = _resolve_publish_decision_manifest_path(
        explicit_publish_decision_manifest=publish_decision_manifest,
        next_actions_manifest_path=next_actions_path,
        next_actions_payload=next_actions_payload,
    )
    if resolved_publish_decision_manifest is None:
        return {
            "ok": False,
            "status": "unresolved_publish_decision_manifest",
            "asset_id": target_asset_id,
            "action_status": action_status,
            "error": "publish-decision manifest could not be resolved from the next-actions source chain",
        }

    publish_decision_payload = _load_json(resolved_publish_decision_manifest)
    _validate_publish_decision_manifest(publish_decision_payload)
    publish_decision_row = _find_asset_row(publish_decision_payload.get("rows"), target_asset_id)
    if publish_decision_row is None:
        return {
            "ok": False,
            "status": "unknown_publish_decision_asset_id",
            "asset_id": target_asset_id,
            "action_status": action_status,
            "error": f"asset_id '{target_asset_id}' was not found in the publish-decision manifest",
        }
    decision_status = str(publish_decision_row.get("decision_status") or "").strip()
    if decision_status != "needs_broader_evidence":
        return {
            "ok": False,
            "status": "stale_publish_decision_status",
            "asset_id": target_asset_id,
            "action_status": action_status,
            "publish_decision_status": decision_status or None,
            "error": "publish-decision row is no longer actionable for evidence expansion",
        }

    game = str(next_actions_payload.get("game") or publish_decision_payload.get("game") or "").strip()
    expansion_root = (
        _resolve_path(evidence_expansion_root)
        if evidence_expansion_root is not None
        else DEFAULT_OUTPUT_ROOT / game / "evidence_expansion"
    )
    reusable_manifests = _discover_reusable_expansion_manifests(expansion_root, target_asset_id)
    if reusable_manifests:
        chosen = reusable_manifests[0]
        result = {
            "ok": True,
            "status": "ok",
            "asset_id": target_asset_id,
            "action_status": action_status,
            "applied_action": "reuse_existing_expansion",
            "expansion_manifest_path": str(chosen["path"]),
            "reuse_mode": "reused_existing",
            "source_next_actions_manifest_path": str(next_actions_path),
            "publish_decision_manifest_path": str(resolved_publish_decision_manifest),
        }
        if len(reusable_manifests) > 1:
            result["warning"] = "multiple_active_expansion_manifests_found"
        return result

    target_output_path = _resolve_new_expansion_output_path(
        game=game,
        asset_id=target_asset_id,
        evidence_expansion_root=expansion_root,
    )
    created = create_evidence_expansion(
        publish_decision_manifest=resolved_publish_decision_manifest,
        asset_id=target_asset_id,
        output_path=target_output_path,
    )
    if not created.get("ok"):
        return {
            "ok": False,
            "status": str(created.get("status") or "create_evidence_expansion_failed"),
            "asset_id": target_asset_id,
            "action_status": action_status,
            "error": str(created.get("error") or "evidence expansion creation failed"),
        }
    return {
        "ok": True,
        "status": "ok",
        "asset_id": target_asset_id,
        "action_status": action_status,
        "applied_action": "create_new_expansion",
        "expansion_manifest_path": str(created.get("expansion_manifest_path")),
        "reuse_mode": "created_new",
        "source_next_actions_manifest_path": str(next_actions_path),
        "publish_decision_manifest_path": str(resolved_publish_decision_manifest),
    }


def _resolve_publish_decision_manifest_path(
    *,
    explicit_publish_decision_manifest: str | Path | None,
    next_actions_manifest_path: Path,
    next_actions_payload: dict[str, Any],
) -> Path | None:
    if explicit_publish_decision_manifest is not None:
        return _resolve_path(explicit_publish_decision_manifest)
    progress_manifest_path = next_actions_payload.get("source_progress_manifest_path")
    if not progress_manifest_path:
        return None
    resolved_progress_manifest_path = _resolve_relative_to(
        next_actions_manifest_path.parent,
        str(progress_manifest_path),
    )
    if not resolved_progress_manifest_path.is_file():
        return None
    progress_payload = _load_json(resolved_progress_manifest_path)
    if str(progress_payload.get("schema_version") or "").strip() != PROGRESS_SCHEMA_VERSION:
        return None
    chained_publish_decision_manifest_path = progress_payload.get("source_publish_decision_manifest_path")
    if not chained_publish_decision_manifest_path:
        return None
    resolved_publish_decision_path = _resolve_relative_to(
        resolved_progress_manifest_path.parent,
        str(chained_publish_decision_manifest_path),
    )
    if not resolved_publish_decision_path.is_file():
        return None
    return resolved_publish_decision_path


def _discover_reusable_expansion_manifests(root: Path, asset_id: str) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    discovered: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.detector_calibration_evidence_expansion.json"), key=lambda item: str(item)):
        payload = _load_json_safely(path)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("schema_version") or "").strip() != EXPANSION_SCHEMA_VERSION:
            continue
        if str(payload.get("asset_id") or "").strip() != asset_id:
            continue
        status = str(payload.get("status") or "").strip()
        if status not in REUSABLE_STATUSES:
            continue
        created_at = str(payload.get("created_at") or "").strip()
        discovered.append(
            {
                "path": path.resolve(),
                "status": status,
                "created_at": created_at,
            }
        )
    return sorted(discovered, key=_reusable_manifest_sort_key)


def _reusable_manifest_sort_key(item: dict[str, Any]) -> tuple[int, float, str]:
    status_rank = REUSE_STATUS_ORDER.get(str(item.get("status") or ""), len(REUSE_STATUS_ORDER))
    created_at = _parse_iso_datetime(str(item.get("created_at") or "").strip())
    created_epoch = created_at.timestamp() if created_at is not None else 0.0
    return (status_rank, -created_epoch, str(item.get("path") or ""))


def _resolve_new_expansion_output_path(
    *,
    game: str,
    asset_id: str,
    evidence_expansion_root: Path,
) -> Path:
    return _artifact_resolve_timestamped_output_path(
        output_path=None,
        default_dir=evidence_expansion_root / _asset_slug(asset_id),
        filename_suffix="detector_calibration_evidence_expansion.json",
        timestamp_slug=_utc_timestamp_slug(),
    )


def _asset_slug(asset_id: str) -> str:
    return asset_id.split(".")[-1] if asset_id else "asset"


def _find_asset_row(rows: Any, asset_id: str) -> dict[str, Any] | None:
    if not isinstance(rows, list):
        return None
    for row in rows:
        if isinstance(row, dict) and str(row.get("asset_id") or "").strip() == asset_id:
            return row
    return None


def _validate_next_actions_manifest(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != NEXT_ACTIONS_SCHEMA_VERSION:
        raise ValueError("next-actions manifest schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("next-actions manifest rows must be a list")


def _validate_publish_decision_manifest(payload: dict[str, Any]) -> None:
    if str(payload.get("schema_version") or "").strip() != PUBLISH_DECISION_SCHEMA_VERSION:
        raise ValueError("publish-decision manifest schema_version is invalid")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("publish-decision manifest rows must be a list")


def _resolve_relative_to(base_dir: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (base_dir / candidate).resolve()


def _resolve_path(path: str | Path) -> Path:
    return _artifact_resolve_path(path)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _load_json_safely(path: str | Path) -> dict[str, Any] | None:
    try:
        return _load_json(path)
    except (OSError, json.JSONDecodeError):
        return None


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _utc_timestamp_slug() -> str:
    return _artifact_utc_timestamp_slug()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply one detector calibration next-action row into evidence-expansion work")
    parser.add_argument("--next-actions-manifest", required=True)
    parser.add_argument("--asset-id", required=True)
    parser.add_argument("--publish-decision-manifest")
    parser.add_argument("--evidence-expansion-root")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = apply_detector_calibration_next_action(
        next_actions_manifest=args.next_actions_manifest,
        asset_id=args.asset_id,
        publish_decision_manifest=args.publish_decision_manifest,
        evidence_expansion_root=args.evidence_expansion_root,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
