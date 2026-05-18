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
    "source_evidence_expansion_queue_manifest_path",
    "source_publish_decision_manifest_path",
    "source_promotion_triage_manifest_path",
    "row_count",
    "status_counts",
    "rows",
)
STATUS_ORDER = ("actively_collecting", "waiting_for_work", "ready_to_publish", "not_triaged")


def inspect_detector_calibration_evidence_expansion_progress_manifest(
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
        f"Source queue manifest path: {payload.get('source_evidence_expansion_queue_manifest_path')}",
        f"Source publish-decision manifest path: {payload.get('source_publish_decision_manifest_path')}",
        f"Source promotion-triage manifest path: {payload.get('source_promotion_triage_manifest_path')}",
        f"Row count: {payload.get('row_count')}",
        "Status counts: "
        f"actively_collecting={_display(status_counts.get('actively_collecting'))}, "
        f"waiting_for_work={_display(status_counts.get('waiting_for_work'))}, "
        f"ready_to_publish={_display(status_counts.get('ready_to_publish'))}, "
        f"not_triaged={_display(status_counts.get('not_triaged'))}",
    ]

    if not rows:
        lines.append("")
        lines.append("Top actively-collecting")
        lines.append("No detector calibration evidence expansion progress rows.")
        return "\n".join(lines)

    top_actively_collecting = next(
        (
            row
            for row in rows
            if isinstance(row, dict) and str(row.get("progress_status") or "").strip() == "actively_collecting"
        ),
        None,
    )
    lines.append("")
    lines.append("Top actively-collecting")
    if isinstance(top_actively_collecting, dict):
        lines.append(f"Asset id: {_display(top_actively_collecting.get('asset_id'))}")
        lines.append(f"Progress status: {_display(top_actively_collecting.get('progress_status'))}")
        lines.append(f"Progress reason: {_display(top_actively_collecting.get('progress_reason'))}")
        lines.append(f"Queue status: {_display(top_actively_collecting.get('queue_status'))}")
        lines.append(f"Decision status: {_display(top_actively_collecting.get('decision_status'))}")
        lines.append(f"Triage status: {_display(top_actively_collecting.get('triage_status'))}")
        lines.append(f"Remaining replay gap: {_display(top_actively_collecting.get('remaining_replay_gap'))}")
        lines.append(
            f"Remaining distinct source gap: {_display(top_actively_collecting.get('remaining_distinct_source_gap'))}"
        )
    else:
        lines.append("None")

    for status in STATUS_ORDER:
        lines.append("")
        lines.append(status)
        matching_rows = [
            row
            for row in rows
            if isinstance(row, dict) and str(row.get("progress_status") or "").strip() == status
        ]
        if not matching_rows:
            lines.append("None")
            continue
        for row in matching_rows:
            lines.append(
                f"- {_display(row.get('asset_id'))} | queue_status={_display(row.get('queue_status'))} | remaining_replay_gap={_display(row.get('remaining_replay_gap'))} | remaining_distinct_source_gap={_display(row.get('remaining_distinct_source_gap'))}"
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
            "asset_id",
            "progress_status",
            "progress_reason",
            "queue_status",
            "decision_status",
            "triage_status",
            "remaining_replay_gap",
            "remaining_distinct_source_gap",
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
    parser = argparse.ArgumentParser(description="Inspect detector calibration evidence expansion progress manifest")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_detector_calibration_evidence_expansion_progress_manifest(
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
