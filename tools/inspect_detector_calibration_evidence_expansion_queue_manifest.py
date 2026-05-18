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
    "source_manifest_count",
    "row_count",
    "status_counts",
    "rows",
)
STATUS_ORDER = ("in_progress", "planned", "satisfied", "abandoned")


def inspect_detector_calibration_evidence_expansion_queue_manifest(
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
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    status_counts = payload.get("status_counts") if isinstance(payload.get("status_counts"), dict) else {}
    lines = [
        f"Manifest path: {manifest_path}",
        f"Game: {payload.get('game')}",
        f"Source manifest count: {payload.get('source_manifest_count')}",
        f"Row count: {payload.get('row_count')}",
        "Status counts: "
        f"in_progress={_display(status_counts.get('in_progress'))}, "
        f"planned={_display(status_counts.get('planned'))}, "
        f"satisfied={_display(status_counts.get('satisfied'))}, "
        f"abandoned={_display(status_counts.get('abandoned'))}",
    ]

    if not rows:
        lines.append("")
        lines.append("Top in-progress")
        lines.append("No detector calibration evidence expansion queue rows.")
        return "\n".join(lines)

    top_in_progress = next(
        (
            row
            for row in rows
            if isinstance(row, dict) and str(row.get("status") or "").strip() == "in_progress"
        ),
        None,
    )
    lines.append("")
    lines.append("Top in-progress")
    if isinstance(top_in_progress, dict):
        lines.append(f"Asset id: {_display(top_in_progress.get('asset_id'))}")
        lines.append(
            f"Requested evidence item count: {_display(top_in_progress.get('requested_evidence_item_count'))}"
        )
        lines.append(f"Linked session count: {_display(top_in_progress.get('linked_session_count'))}")
        lines.append(f"Linked promotion count: {_display(top_in_progress.get('linked_promotion_count'))}")
        lines.append(f"Decision status: {_display(top_in_progress.get('decision_status'))}")
        lines.append(f"Decision reason: {_display(top_in_progress.get('decision_reason'))}")
    else:
        lines.append("None")

    for status in STATUS_ORDER:
        lines.append("")
        lines.append(status)
        matching_rows = [
            row
            for row in rows
            if isinstance(row, dict) and str(row.get("status") or "").strip() == status
        ]
        if not matching_rows:
            lines.append("None")
            continue
        for row in matching_rows:
            lines.append(
                f"- {_display(row.get('asset_id'))} | requested_evidence_item_count={_display(row.get('requested_evidence_item_count'))} | linked_session_count={_display(row.get('linked_session_count'))} | linked_promotion_count={_display(row.get('linked_promotion_count'))}"
            )
    return "\n".join(lines)


def _validate_manifest_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("manifest payload must be a mapping")
    missing = [field for field in REQUIRED_TOP_LEVEL_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"manifest missing required fields: {', '.join(missing)}")
    if not isinstance(payload.get("status_counts"), dict):
        raise ValueError("manifest status_counts must be a mapping")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("manifest rows must be a list")
    for row in payload["rows"]:
        if not isinstance(row, dict):
            raise ValueError("manifest rows must contain mappings")
        for field in (
            "status",
            "asset_id",
            "requested_evidence_item_count",
            "linked_session_count",
            "linked_promotion_count",
            "decision_status",
            "decision_reason",
        ):
            if field not in row:
                raise ValueError(f"manifest row missing required field: {field}")


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
    parser = argparse.ArgumentParser(description="Inspect detector calibration evidence expansion queue manifest")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_evidence_expansion_queue_manifest(
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
