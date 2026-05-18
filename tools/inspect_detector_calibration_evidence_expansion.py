from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

REQUIRED_TOP_LEVEL_FIELDS = (
    "schema_version",
    "game",
    "asset_id",
    "created_at",
    "source_publish_decision_manifest_path",
    "current_evidence_snapshot",
    "status",
    "requested_evidence_items",
    "linked_session_roots",
    "linked_promotion_record_paths",
    "operator_notes",
)


def inspect_detector_calibration_evidence_expansion(
    *,
    manifest: str | Path,
    emit_json: bool = False,
) -> dict[str, Any]:
    manifest_path = _resolve_path(manifest)
    payload = _load_json(manifest_path)
    _validate_manifest_payload(payload)
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(manifest_path, payload)
    return {
        "ok": True,
        "status": "ok",
        "manifest_path": str(manifest_path),
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "manifest": payload,
    }


def _render_compact_text(manifest_path: Path, payload: dict[str, Any]) -> str:
    evidence_snapshot = payload.get("current_evidence_snapshot") if isinstance(payload.get("current_evidence_snapshot"), dict) else {}
    requested_items = payload.get("requested_evidence_items") if isinstance(payload.get("requested_evidence_items"), list) else []
    linked_sessions = payload.get("linked_session_roots") if isinstance(payload.get("linked_session_roots"), list) else []
    linked_promotions = payload.get("linked_promotion_record_paths") if isinstance(payload.get("linked_promotion_record_paths"), list) else []
    operator_notes = payload.get("operator_notes") if isinstance(payload.get("operator_notes"), list) else []
    lines = [
        f"Manifest path: {manifest_path}",
        f"Game: {payload.get('game')}",
        f"Asset id: {payload.get('asset_id')}",
        f"Status: {payload.get('status')}",
        f"Source publish-decision manifest path: {payload.get('source_publish_decision_manifest_path')}",
        "Current evidence snapshot: "
        f"decision_status={_display(evidence_snapshot.get('decision_status'))}, "
        f"decision_reason={_display(evidence_snapshot.get('decision_reason'))}, "
        f"replay_count_for_asset={_display(evidence_snapshot.get('replay_count_for_asset'))}, "
        f"distinct_source_count_for_asset={_display(evidence_snapshot.get('distinct_source_count_for_asset'))}",
        f"Requested evidence item count: {len(requested_items)}",
        f"Linked session count: {len(linked_sessions)}",
        f"Linked promotion count: {len(linked_promotions)}",
    ]

    lines.append("")
    lines.append("Requested evidence")
    if not requested_items:
        lines.append("None")
    else:
        for item in requested_items:
            if not isinstance(item, dict):
                continue
            source_hint = str(item.get("source_hint") or "").strip()
            suffix = f" | source_hint={source_hint}" if source_hint else ""
            lines.append(
                f"- reason={_display(item.get('reason'))} | status={_display(item.get('status'))}{suffix}"
            )

    lines.append("")
    lines.append("Linked sessions")
    if not linked_sessions:
        lines.append("None")
    else:
        for session_root in linked_sessions:
            lines.append(f"- {_display(session_root)}")

    lines.append("")
    lines.append("Linked promotions")
    if not linked_promotions:
        lines.append("None")
    else:
        for promotion_path in linked_promotions:
            lines.append(f"- {_display(promotion_path)}")

    lines.append("")
    lines.append("Operator notes")
    if not operator_notes:
        lines.append("None")
    else:
        for note in operator_notes:
            lines.append(f"- {_display(note)}")

    return "\n".join(lines)


def _validate_manifest_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("manifest payload must be a mapping")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"manifest missing required fields: {', '.join(missing)}")
    if not isinstance(payload.get("requested_evidence_items"), list):
        raise ValueError("manifest requested_evidence_items must be a list")
    if not isinstance(payload.get("linked_session_roots"), list):
        raise ValueError("manifest linked_session_roots must be a list")
    if not isinstance(payload.get("linked_promotion_record_paths"), list):
        raise ValueError("manifest linked_promotion_record_paths must be a list")
    if not isinstance(payload.get("operator_notes"), list):
        raise ValueError("manifest operator_notes must be a list")


def _display(value: Any) -> str:
    if value is None or value == "":
        return "None"
    return str(value)


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(_resolve_path(path).read_text(encoding="utf-8"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect detector calibration evidence expansion manifest")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_evidence_expansion(
            manifest=args.manifest,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
