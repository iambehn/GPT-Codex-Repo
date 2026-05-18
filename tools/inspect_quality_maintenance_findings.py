from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.contract_audit import audit_pipeline_contracts


_SEVERITY_ORDER = {
    "blocking": 0,
    "warning": 1,
    "informational": 2,
}


def inspect_quality_maintenance_findings(
    *,
    repo_root: str | Path | None = None,
    game: str | None = None,
    emit_json: bool = False,
) -> dict[str, Any]:
    resolved_repo_root = _resolve_repo_root(repo_root)
    audit_result = audit_pipeline_contracts(repo_root=resolved_repo_root)
    findings = audit_result.get("quality_maintenance_findings", [])
    if not isinstance(findings, list):
        raise ValueError("quality_maintenance_findings must be a list")
    filtered_findings = _filter_findings(findings, game=game)
    severity_counts = _severity_counts(filtered_findings)
    payload = {
        "repo_root": str(resolved_repo_root),
        "game_filter": game,
        "total_findings": len(filtered_findings),
        "severity_counts": severity_counts,
        "quality_maintenance_findings": filtered_findings,
    }
    rendered_output = json.dumps(payload, indent=2) if emit_json else _render_compact_text(payload)
    return {
        "ok": True,
        "status": "ok",
        "repo_root": str(resolved_repo_root),
        "game_filter": game,
        "emit_json": bool(emit_json),
        "rendered_output": rendered_output,
        "inspection_payload": payload,
    }


def _render_compact_text(payload: dict[str, Any]) -> str:
    severity_counts = payload.get("severity_counts", {}) if isinstance(payload.get("severity_counts"), dict) else {}
    findings = payload.get("quality_maintenance_findings", []) if isinstance(payload.get("quality_maintenance_findings"), list) else []
    lines = [
        f"Repo root: {payload.get('repo_root')}",
        f"Game filter: {_display(payload.get('game_filter'))}",
        f"Total findings: {_display(payload.get('total_findings'))}",
        f"Blocking count: {severity_counts.get('blocking', 0)}",
        f"Warning count: {severity_counts.get('warning', 0)}",
        f"Informational count: {severity_counts.get('informational', 0)}",
        "",
        "Findings",
    ]
    if not findings:
        lines.append("None")
        return "\n".join(lines)

    for row in findings:
        lines.append(_render_finding_line(row))
    return "\n".join(lines)


def _render_finding_line(row: dict[str, Any]) -> str:
    severity = _display(row.get("severity")).lower()
    surface = _display(row.get("surface"))
    status = _display(row.get("status"))
    context_parts: list[str] = []
    game = str(row.get("game") or "").strip()
    if game:
        context_parts.append(f"game={game}")
    draft_label = _draft_label(row)
    if draft_label:
        context_parts.append(f"draft={draft_label}")
    detail_parts: list[str] = []
    for key in (
        "stale_by_days",
        "fixture_count",
        "pending_review_count",
        "total_review_files",
        "pending_review_ratio",
        "stale_review_count",
    ):
        if key in row:
            detail_parts.append(f"{key}={row.get(key)}")
    parts = [f"[{severity}] {surface}", f"status={status}"]
    parts.extend(context_parts)
    parts.extend(detail_parts)
    return " | ".join(parts)


def _draft_label(row: dict[str, Any]) -> str:
    review_dir = str(row.get("review_dir") or "").strip()
    if review_dir:
        review_path = Path(review_dir)
        if len(review_path.parts) >= 3:
            return review_path.parent.parent.name
    draft_root = str(row.get("draft_root") or "").strip()
    if draft_root:
        return Path(draft_root).name
    return ""


def _filter_findings(findings: list[dict[str, Any]], *, game: str | None) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in findings:
        if not isinstance(row, dict):
            continue
        row_game = str(row.get("game") or "").strip()
        if game is not None and row_game != game:
            continue
        filtered.append(row)
    return sorted(
        filtered,
        key=lambda item: (
            _SEVERITY_ORDER.get(str(item.get("severity") or "").strip(), 99),
            str(item.get("surface") or ""),
            str(item.get("game") or ""),
            str(item.get("draft_root") or ""),
            str(item.get("status") or ""),
        ),
    )


def _severity_counts(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"blocking": 0, "warning": 0, "informational": 0}
    for row in findings:
        severity = str(row.get("severity") or "").strip()
        if severity in counts:
            counts[severity] += 1
    return counts


def _display(value: Any) -> str:
    if value is None or value == "":
        return "None"
    return str(value)


def _resolve_repo_root(repo_root: str | Path | None) -> Path:
    if repo_root is None:
        return REPO_ROOT
    resolved = Path(repo_root).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect repo quality-maintenance findings from contract audit")
    parser.add_argument("--repo-root")
    parser.add_argument("--game")
    parser.add_argument("--json", action="store_true", dest="emit_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_quality_maintenance_findings(
            repo_root=args.repo_root,
            game=args.game,
            emit_json=bool(args.emit_json),
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print(result["rendered_output"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
