from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.game_onboarding import publish_onboarding_draft
from pipeline.onboarding_publish_readiness import validate_onboarding_publish

_IDENTITY_BLOCKING_QA_TYPES = {
    "ambiguous_identity_match",
    "conflicting_identity_match",
    "identity_match_rejected",
}


def publish_onboarding_batch(
    root: str | Path,
    *,
    game: str | None = None,
    apply: bool = False,
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

    selected, skipped = _select_latest_per_game(draft_roots)
    published: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    ready: list[dict[str, Any]] = []

    for draft_root in selected:
        readiness = validate_onboarding_publish(draft_root)
        row = {
            "game": readiness.get("game", ""),
            "draft_root": str(draft_root),
            "readiness": readiness.get("readiness", "unknown"),
            "can_publish": bool(readiness.get("can_publish", False)),
            "phase_status": readiness.get("phase_status", ""),
            "finding_count": len(list(readiness.get("findings", []))),
        }
        if not readiness.get("can_publish"):
            row["findings"] = list(readiness.get("findings", []))
            identity_blocker_counts = _identity_blocker_counts(readiness)
            row["identity_blocked"] = bool(identity_blocker_counts)
            row["identity_blocker_counts"] = identity_blocker_counts
            row["identity_blocker_examples"] = _identity_blocker_examples(readiness)
            blocked.append(row)
            continue
        if not apply:
            ready.append(row)
            continue
        try:
            result = publish_onboarding_draft(draft_root)
        except Exception as exc:
            row["error"] = str(exc)
            failed.append(row)
            continue
        row["published_root"] = result.get("published_root", "")
        row["template_count"] = int(result.get("template_count", 0) or 0)
        published.append(row)

    payload = {
        "ok": True,
        "root": str(root_path),
        "game_filter": requested_game or None,
        "apply": apply,
        "draft_count": len(draft_roots),
        "selected_count": len(selected),
        "summary": {
            "published": len(published),
            "ready": len(ready),
            "blocked": len(blocked),
            "failed": len(failed),
            "skipped": len(skipped),
        },
        "published": published,
        "ready": ready,
        "blocked": blocked,
        "failed": failed,
        "skipped": skipped,
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
    payload = _load_state(draft_root)
    return str(payload.get("game_id", "")).strip()


def _load_state(draft_root: Path) -> dict[str, Any]:
    state_path = draft_root / "manifests" / "onboarding_state.json"
    if not state_path.exists():
        raise FileNotFoundError(f"draft is missing onboarding state: {state_path}")
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"draft onboarding state must be a mapping: {state_path}")
    return payload


def _select_latest_per_game(draft_roots: list[Path]) -> tuple[list[Path], list[dict[str, Any]]]:
    selected_by_game: dict[str, tuple[Path, str]] = {}
    skipped: list[dict[str, Any]] = []
    for draft_root in draft_roots:
        state = _load_state(draft_root)
        game = str(state.get("game_id", "")).strip()
        updated_at = str(state.get("updated_at", ""))
        existing = selected_by_game.get(game)
        if existing is None:
            selected_by_game[game] = (draft_root, updated_at)
            continue
        current_root, current_updated_at = existing
        if updated_at >= current_updated_at:
            skipped.append(
                {
                    "game": game,
                    "draft_root": str(current_root),
                    "reason": "superseded_by_newer_draft",
                    "selected_draft_root": str(draft_root),
                }
            )
            selected_by_game[game] = (draft_root, updated_at)
        else:
            skipped.append(
                {
                    "game": game,
                    "draft_root": str(draft_root),
                    "reason": "superseded_by_newer_draft",
                    "selected_draft_root": str(current_root),
                }
            )
    selected = [row[0] for _, row in sorted(selected_by_game.items(), key=lambda item: item[0])]
    return selected, skipped


def _identity_blocker_counts(readiness: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in readiness.get("findings", []):
        if not isinstance(finding, dict):
            continue
        finding_type = str(finding.get("type", "")).strip()
        if finding_type in _IDENTITY_BLOCKING_QA_TYPES:
            counts[finding_type] = counts.get(finding_type, 0) + 1
    return counts


def _identity_blocker_examples(readiness: dict[str, Any], *, limit: int = 3) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for finding in readiness.get("findings", []):
        if not isinstance(finding, dict):
            continue
        finding_type = str(finding.get("type", "")).strip()
        if finding_type not in _IDENTITY_BLOCKING_QA_TYPES:
            continue
        examples.append(
            {
                "type": finding_type,
                "message": str(finding.get("message", "") or finding.get("reason", "")).strip(),
                "detection_id": str(finding.get("detection_id", "")).strip(),
                "target_id": str(finding.get("target_id", "")).strip(),
            }
        )
        if len(examples) >= limit:
            break
    return examples
