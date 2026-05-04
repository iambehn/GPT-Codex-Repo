from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.game_onboarding import (
    _coerce_string_list,
    _load_starter_seed_data,
    _match_starter_seed_rows,
    _read_csv_rows,
    _write_csv,
)
from pipeline.onboarding_adapters import get_onboarding_adapter
from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from pipeline.structured_source_fields import aliases_equivalent, binding_key, clean_text, identity_keys, merge_aliases


REPO_ROOT = Path(__file__).resolve().parent.parent
ONBOARDING_IDENTITY_REVIEW_SESSION_SCHEMA_VERSION = "onboarding_identity_review_session_v1"
DEFAULT_GPT_REPO = Path.home() / "GPT-Codex-Repo"
BRIDGE_TEMPLATE_ID = "onboarding_identity_review_bridge"
BRIDGE_CLIP_TYPE = "onboarding_identity_candidate"
_BLOCKING_IDENTITY_QA_TYPES = {
    "ambiguous_identity_match",
    "conflicting_identity_match",
    "identity_match_rejected",
}
_AUDIT_QA_TYPES = {
    "identity_review_applied",
    "identity_review_deferred",
}
_TARGET_KIND_MAP = {
    "hero": ("heroes", "hero_id"),
    "ability": ("abilities", "ability_id"),
    "event": ("events", "event_id"),
}
_ALLOWED_DECISIONS = (
    "keep_source_identity",
    "adopt_seed_identity",
    "defer_identity_resolution",
)


def prepare_onboarding_identity_review(
    draft_root: str | Path,
    *,
    gpt_repo: str | Path | None = None,
    session_name: str | None = None,
) -> dict[str, Any]:
    resolved_draft_root = _resolve_path(draft_root)
    readiness = validate_onboarding_publish(resolved_draft_root)
    game = str(readiness.get("game", "")).strip()
    if not game:
        raise ValueError("onboarding draft does not define a valid game")

    gpt_repo_path = _resolve_gpt_repo(gpt_repo)
    ontology = _load_ontology(resolved_draft_root)
    qa_rows = _read_csv_rows(resolved_draft_root / "catalog" / "qa_queue.csv")
    adapter = get_onboarding_adapter(game)
    seed_data = _load_starter_seed_data(game, repo_root=REPO_ROOT, adapter=adapter)
    blocked_rows = _collect_blocked_identity_rows(qa_rows, readiness)

    created_at = _utc_now()
    session_id = _session_id(game, resolved_draft_root, blocked_rows, session_name, created_at)
    manifest_path = _session_manifest_path(game, session_id)

    items: list[dict[str, Any]] = []
    for index, blocked in enumerate(blocked_rows):
        row = _find_ontology_row(ontology, blocked["target_kind"], blocked["target_id"])
        if row is None:
            continue
        seed_candidates = _seed_candidates_for_row(row, blocked["target_kind"], seed_data)
        item = _materialize_review_item(
            game=game,
            draft_root=resolved_draft_root,
            blocked=blocked,
            row=row,
            seed_candidates=seed_candidates,
            gpt_repo=gpt_repo_path,
            session_id=session_id,
            index=index,
        )
        items.append(item)

    manifest = {
        "schema_version": ONBOARDING_IDENTITY_REVIEW_SESSION_SCHEMA_VERSION,
        "session_id": session_id,
        "game": game,
        "draft_root": str(resolved_draft_root),
        "gpt_repo": str(gpt_repo_path),
        "created_at": created_at,
        "item_count": len(items),
        "recommendation_counts": _count_by_field(items, "recommended_decision"),
        "items": items,
        "manifest_path": str(manifest_path),
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def apply_onboarding_identity_review(
    session_manifest: str | Path,
    *,
    gpt_repo: str | Path | None = None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(session_manifest)
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != ONBOARDING_IDENTITY_REVIEW_SESSION_SCHEMA_VERSION:
        return {
            "ok": False,
            "session_manifest": str(manifest_path),
            "error": "unsupported session manifest schema",
        }

    if gpt_repo is not None:
        manifest_gpt_repo = Path(str(manifest.get("gpt_repo", ""))).resolve()
        requested_gpt_repo = _resolve_path(gpt_repo)
        if manifest_gpt_repo != requested_gpt_repo:
            return {
                "ok": False,
                "session_manifest": str(manifest_path),
                "error": "session manifest gpt_repo does not match requested gpt_repo",
            }

    draft_root = Path(str(manifest.get("draft_root", ""))).resolve()
    ontology = _load_ontology(draft_root)
    qa_rows = _read_csv_rows(draft_root / "catalog" / "qa_queue.csv")

    resolved_count = 0
    deferred_count = 0
    unreviewed_count = 0
    rejected_count = 0
    failed_items: list[dict[str, Any]] = []
    applied_decision_counts: dict[str, int] = {}

    for item in manifest.get("items", []):
        gpt_meta_path = Path(str(item.get("gpt_meta_path", ""))).resolve()
        meta = _load_json(gpt_meta_path)
        review_status = _normalized_review_status(meta.get("review_status"))
        reviewed_at = str(meta.get("reviewed_at", "")).strip() or _utc_now()
        decision = str(meta.get("review_decision", "")).strip()
        selected_seed_id = str(meta.get("selected_seed_id", "")).strip()
        final_path = str(meta.get("final_path", "")).strip() or None

        item["review_status"] = review_status
        item["reviewed_at"] = reviewed_at
        item["review_decision"] = decision
        item["selected_seed_id"] = selected_seed_id
        item["gpt_final_path"] = final_path

        if review_status == "rejected":
            item["apply_status"] = "rejected"
            rejected_count += 1
            continue
        if review_status != "approved":
            item["apply_status"] = "unreviewed"
            unreviewed_count += 1
            continue

        target_kind = str(item.get("target_kind", "")).strip()
        original_target_id = str(item.get("target_id", "")).strip()
        row = _find_ontology_row(ontology, target_kind, original_target_id)
        if row is None:
            item["apply_status"] = "missing_target_row"
            failed_items.append(_failed_item(item, "missing_target_row"))
            continue

        validation_error = _validate_approved_review_item(item, decision=decision, selected_seed_id=selected_seed_id)
        if validation_error:
            item["apply_status"] = validation_error
            failed_items.append(_failed_item(item, validation_error))
            continue

        previous_id = str(row.get(_id_field_for_target_kind(target_kind), "")).strip()
        previous_display_name = str(row.get("display_name", "")).strip()

        if decision == "keep_source_identity":
            _stamp_identity_review_metadata(
                row,
                session_id=str(manifest.get("session_id", "")),
                review_status="resolved",
                review_decision=decision,
                reviewed_at=reviewed_at,
                chosen_identity_source="source",
                previous_id=previous_id,
                previous_display_name=previous_display_name,
                applied_id=previous_id,
                applied_display_name=previous_display_name,
            )
            qa_rows = _clear_identity_blockers(qa_rows, target_kind=target_kind, target_id=original_target_id)
            qa_rows = _append_audit_row(
                qa_rows,
                item_type="identity_review_applied",
                target_kind=target_kind,
                target_id=str(row.get(_id_field_for_target_kind(target_kind), "")),
                display_name=str(row.get("display_name", "")),
                reason=f"operator kept source identity '{previous_display_name}' ({previous_id})",
            )
            item["apply_status"] = "resolved"
            resolved_count += 1
            applied_decision_counts[decision] = applied_decision_counts.get(decision, 0) + 1
            continue

        if decision == "adopt_seed_identity":
            seed_candidates = item.get("seed_candidates", [])
            selected_seed = _resolve_selected_seed(seed_candidates, selected_seed_id)
            if selected_seed is None:
                item["apply_status"] = "invalid_seed_selection"
                failed_items.append(_failed_item(item, "invalid_seed_selection"))
                continue
            qa_rows = _clear_identity_blockers(qa_rows, target_kind=target_kind, target_id=original_target_id)
            _apply_seed_identity(
                row,
                target_kind=target_kind,
                seed_row=selected_seed,
                session_id=str(manifest.get("session_id", "")),
                reviewed_at=reviewed_at,
                qa_rows=qa_rows,
                previous_id=previous_id,
                previous_display_name=previous_display_name,
            )
            qa_rows = _append_alias_rejection_rows(
                qa_rows,
                target_kind=target_kind,
                row=row,
                alias_rejections=row.pop("_identity_alias_rejections", []),
            )
            qa_rows = _append_audit_row(
                qa_rows,
                item_type="identity_review_applied",
                target_kind=target_kind,
                target_id=str(row.get(_id_field_for_target_kind(target_kind), "")),
                display_name=str(row.get("display_name", "")),
                reason=(
                    f"operator adopted starter seed identity "
                    f"'{selected_seed.get('display_name', '')}' ({selected_seed.get('canonical_id', '')})"
                ),
            )
            item["apply_status"] = "resolved"
            resolved_count += 1
            applied_decision_counts[decision] = applied_decision_counts.get(decision, 0) + 1
            continue

        item["apply_status"] = "deferred"
        qa_rows = _append_audit_row(
            qa_rows,
            item_type="identity_review_deferred",
            target_kind=target_kind,
            target_id=original_target_id,
            display_name=str(row.get("display_name", "")),
            reason=f"operator deferred identity resolution for '{previous_display_name}' ({previous_id})",
        )
        _stamp_identity_review_metadata(
            row,
            session_id=str(manifest.get("session_id", "")),
            review_status="deferred",
            review_decision="defer_identity_resolution",
            reviewed_at=reviewed_at,
            chosen_identity_source="deferred",
            previous_id=previous_id,
            previous_display_name=previous_display_name,
            applied_id=previous_id,
            applied_display_name=previous_display_name,
        )
        deferred_count += 1
        applied_decision_counts["defer_identity_resolution"] = applied_decision_counts.get("defer_identity_resolution", 0) + 1

    _write_ontology(draft_root, ontology)
    _write_ontology_catalogs(draft_root, ontology)
    _write_csv(draft_root / "catalog" / "qa_queue.csv", qa_rows)
    _touch_onboarding_state(draft_root)

    manifest["applied_at"] = _utc_now()
    manifest["resolved_count"] = resolved_count
    manifest["deferred_count"] = deferred_count
    manifest["rejected_count"] = rejected_count
    manifest["unreviewed_count"] = unreviewed_count
    manifest["failed_item_count"] = len(failed_items)
    manifest["failed_items"] = failed_items
    manifest["failed_reason_counts"] = _count_by_field(failed_items, "apply_status")
    manifest["applied_decision_counts"] = applied_decision_counts
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "session_manifest": str(manifest_path),
        "session_id": manifest.get("session_id", ""),
        "resolved_count": resolved_count,
        "deferred_count": deferred_count,
        "rejected_count": rejected_count,
        "unreviewed_count": unreviewed_count,
        "failed_item_count": len(failed_items),
        "failed_items": failed_items,
        "item_count": len(manifest.get("items", [])),
    }


def cleanup_onboarding_identity_review(
    session_manifest: str | Path,
    *,
    gpt_repo: str | Path | None = None,
) -> dict[str, Any]:
    del gpt_repo
    manifest_path = _resolve_path(session_manifest)
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != ONBOARDING_IDENTITY_REVIEW_SESSION_SCHEMA_VERSION:
        return {
            "ok": False,
            "session_manifest": str(manifest_path),
            "error": "unsupported session manifest schema",
        }

    cleanup_count = 0
    for item in manifest.get("items", []):
        cleanup_count += int(_cleanup_session_item(item))

    manifest["cleanup_at"] = _utc_now()
    manifest["cleanup_count"] = cleanup_count
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "session_manifest": str(manifest_path),
        "session_id": manifest.get("session_id", ""),
        "cleanup_count": cleanup_count,
        "item_count": len(manifest.get("items", [])),
    }


def _collect_blocked_identity_rows(
    qa_rows: list[dict[str, str]],
    readiness: dict[str, Any],
) -> list[dict[str, Any]]:
    blocked_keys = {
        (
            str(finding.get("type", "")).strip(),
            str(finding.get("target_id", "")).strip(),
            str(finding.get("reason", "")).strip(),
        )
        for finding in readiness.get("findings", [])
        if isinstance(finding, dict) and str(finding.get("type", "")).strip() in _BLOCKING_IDENTITY_QA_TYPES
    }
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in qa_rows:
        item_type = str(row.get("item_type", "")).strip()
        target_kind = str(row.get("target_kind", "")).strip()
        target_id = str(row.get("target_id", "")).strip()
        reason = str(row.get("reason", "")).strip()
        if (item_type, target_id, reason) not in blocked_keys:
            continue
        key = (target_kind, target_id)
        entry = grouped.setdefault(
            key,
            {
                "target_kind": target_kind,
                "target_id": target_id,
                "display_name": str(row.get("display_name", "")).strip(),
                "findings": [],
            },
        )
        entry["findings"].append(
            {
                "item_type": item_type,
                "status": str(row.get("status", "")).strip(),
                "reason": reason,
            }
        )
    return sorted(grouped.values(), key=lambda item: (item["target_kind"], item["target_id"]))


def _seed_candidates_for_row(
    row: dict[str, Any],
    target_kind: str,
    seed_data: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    section, id_field = _TARGET_KIND_MAP[target_kind]
    matches = _match_starter_seed_rows(row, seed_data.get(section, []), id_field=id_field)
    candidates: list[dict[str, Any]] = []
    for seed_row in matches:
        canonical_id = str(seed_row.get(id_field, "")).strip()
        display_name = str(seed_row.get("display_name", "")).strip()
        aliases = _coerce_string_list(seed_row.get("aliases", []))
        candidates.append(
            {
                "canonical_id": canonical_id,
                "display_name": display_name,
                "aliases": aliases,
                "source_kind": str(seed_row.get("source_kind", "")).strip(),
                "match_evidence": _build_seed_match_evidence(
                    row,
                    canonical_id=canonical_id,
                    display_name=display_name,
                    aliases=aliases,
                ),
            }
        )
    return candidates


def _materialize_review_item(
    *,
    game: str,
    draft_root: Path,
    blocked: dict[str, Any],
    row: dict[str, Any],
    seed_candidates: list[dict[str, Any]],
    gpt_repo: Path,
    session_id: str,
    index: int,
) -> dict[str, Any]:
    gpt_paths = _gpt_paths(gpt_repo)
    processing_dir = gpt_paths["processing"] / game
    inbox_dir = gpt_paths["inbox"] / game
    processing_dir.mkdir(parents=True, exist_ok=True)
    inbox_dir.mkdir(parents=True, exist_ok=True)

    bridge_stem = f"onboarding-identity-review-{session_id.split('-')[-1]}-{index:03d}-{_slug(blocked['target_id'])}"
    gpt_processed_path = processing_dir / f"{bridge_stem}.json"
    gpt_meta_path = inbox_dir / f"{bridge_stem}.meta.json"

    processed_payload = {
        "game": game,
        "draft_root": str(draft_root),
        "target_kind": blocked["target_kind"],
        "target_id": blocked["target_id"],
        "display_name": blocked["display_name"],
        "row_snapshot": row,
        "blocking_identity_findings": blocked["findings"],
        "seed_candidates": seed_candidates,
        "recommended_decision": _recommended_decision(row, blocked, seed_candidates),
        "recommendation_reason": _recommendation_reason(row, blocked, seed_candidates),
        "decision_previews": _decision_previews(row, blocked, seed_candidates),
    }
    gpt_processed_path.write_text(json.dumps(processed_payload, indent=2), encoding="utf-8")

    allowed_decisions = ["keep_source_identity", "defer_identity_resolution"]
    if seed_candidates:
        allowed_decisions.insert(1, "adopt_seed_identity")
    recommended_decision = _recommended_decision(row, blocked, seed_candidates)
    recommendation_reason = _recommendation_reason(row, blocked, seed_candidates)
    decision_previews = _decision_previews(row, blocked, seed_candidates)
    gpt_meta = {
        "clip_id": bridge_stem,
        "game": game,
        "clip_path": str(gpt_processed_path),
        "processed_path": str(gpt_processed_path),
        "meta_path": str(gpt_meta_path),
        "status": "queue",
        "created_from": "onboarding_identity_review_bridge",
        "selected_template_id": BRIDGE_TEMPLATE_ID,
        "scoring": {
            "highlight_score": 100,
            "clip_type": BRIDGE_CLIP_TYPE,
            "suggested_title": f"{blocked['target_kind']} {blocked['display_name'] or blocked['target_id']}",
            "suggested_caption": f"{len(blocked['findings'])} blocking identity findings",
            "score_reasoning": _score_reasoning(blocked, seed_candidates),
        },
        "review_decision": "",
        "selected_seed_id": "",
        "recommended_decision": recommended_decision,
        "recommendation_reason": recommendation_reason,
        "decision_previews": decision_previews,
        "onboarding_identity_review_bridge": {
            "bridge_owned": True,
            "session_id": session_id,
            "draft_root": str(draft_root),
            "target_kind": blocked["target_kind"],
            "target_id": blocked["target_id"],
            "row_snapshot": row,
            "blocking_identity_findings": blocked["findings"],
            "seed_candidates": seed_candidates,
            "allowed_decisions": allowed_decisions,
            "recommended_decision": recommended_decision,
            "recommendation_reason": recommendation_reason,
            "decision_previews": decision_previews,
        },
    }
    gpt_meta_path.write_text(json.dumps(gpt_meta, indent=2), encoding="utf-8")

    return {
        "clip_id": bridge_stem,
        "draft_root": str(draft_root),
        "target_kind": blocked["target_kind"],
        "target_id": blocked["target_id"],
        "display_name": blocked["display_name"],
        "gpt_processed_path": str(gpt_processed_path),
        "gpt_meta_path": str(gpt_meta_path),
        "seed_candidates": seed_candidates,
        "allowed_decisions": allowed_decisions,
        "recommended_decision": recommended_decision,
        "recommendation_reason": recommendation_reason,
        "decision_previews": decision_previews,
        "materialization_mode": "metadata_only",
        "bridge_owned": True,
        "apply_status": "pending",
        "review_status": "unreviewed",
    }


def _resolve_selected_seed(seed_candidates: Any, selected_seed_id: str) -> dict[str, Any] | None:
    if not isinstance(seed_candidates, list):
        return None
    if selected_seed_id:
        for row in seed_candidates:
            if str(row.get("canonical_id", "")).strip() == selected_seed_id:
                return row
        return None
    if len(seed_candidates) == 1 and isinstance(seed_candidates[0], dict):
        return seed_candidates[0]
    return None


def _apply_seed_identity(
    row: dict[str, Any],
    *,
    target_kind: str,
    seed_row: dict[str, Any],
    session_id: str,
    reviewed_at: str,
    qa_rows: list[dict[str, str]],
    previous_id: str,
    previous_display_name: str,
) -> None:
    id_field = _id_field_for_target_kind(target_kind)
    row[id_field] = str(seed_row.get("canonical_id", "")).strip() or str(row.get(id_field, ""))
    row["display_name"] = str(seed_row.get("display_name", "")).strip() or str(row.get("display_name", ""))
    row["canonical_display_name_source"] = "starter_seed"
    row["canonical_id_source"] = "starter_seed"
    row["canonical_identity_basis"] = "operator_adopt_seed_identity"
    merged_aliases, alias_rejections = merge_aliases(
        [str(item) for item in row.get("aliases", []) if str(item).strip()],
        _coerce_string_list(seed_row.get("aliases", [])),
        canonical_name=str(row.get("display_name", "")),
    )
    row["aliases"] = merged_aliases
    if merged_aliases:
        row["aliases_source"] = "starter_seed"
    row["starter_seed_applied"] = True
    row["starter_seed_source"] = str(seed_row.get("source_kind", "")).strip()
    _stamp_identity_review_metadata(
        row,
        session_id=session_id,
        review_status="resolved",
        review_decision="adopt_seed_identity",
        reviewed_at=reviewed_at,
        chosen_identity_source="starter_seed",
        previous_id=previous_id,
        previous_display_name=previous_display_name,
        applied_id=str(row.get(id_field, "")).strip(),
        applied_display_name=str(row.get("display_name", "")).strip(),
    )
    row["_identity_alias_rejections"] = alias_rejections
    del qa_rows


def _append_alias_rejection_rows(
    qa_rows: list[dict[str, str]],
    *,
    target_kind: str,
    row: dict[str, Any],
    alias_rejections: list[dict[str, str]],
) -> list[dict[str, str]]:
    target_id = str(row.get(_id_field_for_target_kind(target_kind), "")).strip()
    display_name = str(row.get("display_name", "")).strip()
    for rejection in alias_rejections:
        alias = str(rejection.get("alias", "")).strip()
        status = str(rejection.get("status", "")).strip()
        if not alias or not status:
            continue
        if status == "alias_equivalent_to_canonical_name":
            reason = f"starter-seed alias '{alias}' normalized to the canonical display name"
        else:
            reason = f"starter-seed alias '{alias}' normalized to an existing alias"
        qa_rows.append(
            {
                "item_type": status,
                "target_kind": target_kind,
                "target_id": target_id,
                "display_name": display_name,
                "status": "info",
                "reason": reason,
            }
        )
    return qa_rows


def _clear_identity_blockers(
    qa_rows: list[dict[str, str]],
    *,
    target_kind: str,
    target_id: str,
) -> list[dict[str, str]]:
    return [
        row
        for row in qa_rows
        if not (
            str(row.get("target_kind", "")).strip() == target_kind
            and str(row.get("target_id", "")).strip() == target_id
            and str(row.get("item_type", "")).strip() in _BLOCKING_IDENTITY_QA_TYPES
        )
    ]


def _append_audit_row(
    qa_rows: list[dict[str, str]],
    *,
    item_type: str,
    target_kind: str,
    target_id: str,
    display_name: str,
    reason: str,
) -> list[dict[str, str]]:
    qa_rows.append(
        {
            "item_type": item_type,
            "target_kind": target_kind,
            "target_id": target_id,
            "display_name": display_name,
            "status": "info",
            "reason": reason,
        }
    )
    return qa_rows


def _recommended_decision(
    row: dict[str, Any],
    blocked: dict[str, Any],
    seed_candidates: list[dict[str, Any]],
) -> str:
    del blocked
    if len(seed_candidates) == 1 and _is_clear_seed_upgrade(row, seed_candidates[0]):
        return "adopt_seed_identity"
    if len(seed_candidates) > 1:
        return "defer_identity_resolution"
    if _row_is_stable(row):
        return "keep_source_identity"
    return "defer_identity_resolution"


def _recommendation_reason(
    row: dict[str, Any],
    blocked: dict[str, Any],
    seed_candidates: list[dict[str, Any]],
) -> str:
    finding_types = ", ".join(finding["item_type"] for finding in blocked["findings"]) or "unknown"
    if len(seed_candidates) == 1 and _is_clear_seed_upgrade(row, seed_candidates[0]):
        candidate = seed_candidates[0]
        return (
            f"one starter-seed candidate matches by shared identity keys and upgrades the canonical name to "
            f"'{candidate.get('display_name', '')}' without introducing extra ambiguity"
        )
    if len(seed_candidates) > 1:
        return f"multiple starter-seed candidates remain plausible for blocker types {finding_types}"
    if _row_is_stable(row):
        return f"the source-derived row is internally stable and no clear starter-seed upgrade was found for blocker types {finding_types}"
    return f"the row remains unclear and no safe deterministic identity resolution is available for blocker types {finding_types}"


def _decision_previews(
    row: dict[str, Any],
    blocked: dict[str, Any],
    seed_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    id_field = _id_field_for_target_kind(str(blocked.get("target_kind", "")))
    current_id = str(row.get(id_field, "")).strip()
    current_name = str(row.get("display_name", "")).strip()
    current_aliases = [str(item).strip() for item in row.get("aliases", []) if str(item).strip()]
    blocked_types = [str(finding.get("item_type", "")).strip() for finding in blocked.get("findings", []) if str(finding.get("item_type", "")).strip()]

    previews: dict[str, Any] = {
        "keep_source_identity": {
            "canonical_id": current_id,
            "display_name": current_name,
            "aliases_added": [],
            "aliases_suppressed": [],
            "provenance_changes": {
                "identity_review_identity_source": "source",
            },
            "clears_blocker_types": blocked_types,
        },
        "defer_identity_resolution": {
            "canonical_id": current_id,
            "display_name": current_name,
            "aliases_added": [],
            "aliases_suppressed": [],
            "provenance_changes": {},
            "clears_blocker_types": [],
            "blocker_remains": True,
        },
    }
    if seed_candidates:
        candidate_previews: list[dict[str, Any]] = []
        for candidate in seed_candidates:
            merged_aliases, alias_rejections = merge_aliases(
                current_aliases,
                _coerce_string_list(candidate.get("aliases", [])),
                canonical_name=str(candidate.get("display_name", current_name)),
            )
            candidate_previews.append(
                {
                    "selected_seed_id": str(candidate.get("canonical_id", "")).strip(),
                    "canonical_id": str(candidate.get("canonical_id", "")).strip() or current_id,
                    "display_name": str(candidate.get("display_name", "")).strip() or current_name,
                    "aliases_added": [alias for alias in merged_aliases if alias not in current_aliases],
                    "aliases_suppressed": [str(rejection.get("alias", "")).strip() for rejection in alias_rejections if str(rejection.get("alias", "")).strip()],
                    "provenance_changes": {
                        "canonical_display_name_source": "starter_seed",
                        "canonical_id_source": "starter_seed",
                        "canonical_identity_basis": "operator_adopt_seed_identity",
                    },
                    "clears_blocker_types": blocked_types,
                }
            )
        previews["adopt_seed_identity"] = {
            "candidate_previews": candidate_previews,
        }
    return previews


def _stamp_identity_review_metadata(
    row: dict[str, Any],
    *,
    session_id: str,
    review_status: str,
    review_decision: str,
    reviewed_at: str,
    chosen_identity_source: str,
    previous_id: str,
    previous_display_name: str,
    applied_id: str,
    applied_display_name: str,
) -> None:
    row["identity_review_status"] = review_status
    row["identity_review_decision"] = review_decision
    row["identity_review_session_id"] = session_id
    row["identity_reviewed_at"] = reviewed_at
    row["identity_review_identity_source"] = chosen_identity_source
    row["identity_review_previous_id"] = previous_id
    row["identity_review_previous_display_name"] = previous_display_name
    row["identity_review_applied_id"] = applied_id
    row["identity_review_applied_display_name"] = applied_display_name


def _load_ontology(draft_root: Path) -> dict[str, Any]:
    payload = load_yaml_file(draft_root / "entities.yaml")
    if not isinstance(payload, dict):
        raise ValueError(f"draft ontology must be a mapping: {draft_root / 'entities.yaml'}")
    for key in ("heroes", "abilities", "events"):
        if not isinstance(payload.get(key), list):
            raise ValueError(f"draft ontology must define a '{key}' list")
    return payload


def _write_ontology(draft_root: Path, ontology: dict[str, Any]) -> None:
    for section, id_field in (("heroes", "hero_id"), ("abilities", "ability_id"), ("events", "event_id")):
        ontology[section] = sorted(ontology.get(section, []), key=lambda row: str(row.get(id_field, "")))
    dump_yaml_file(draft_root / "entities.yaml", ontology)


def _write_ontology_catalogs(draft_root: Path, ontology: dict[str, Any]) -> None:
    catalog_root = draft_root / "catalog"
    _write_csv(catalog_root / "heroes.csv", ontology.get("heroes", []))
    _write_csv(catalog_root / "abilities.csv", ontology.get("abilities", []))
    _write_csv(catalog_root / "events.csv", ontology.get("events", []))


def _find_ontology_row(ontology: dict[str, Any], target_kind: str, target_id: str) -> dict[str, Any] | None:
    section, id_field = _TARGET_KIND_MAP[target_kind]
    for row in ontology.get(section, []):
        if str(row.get(id_field, "")).strip() == target_id:
            return row
    return None


def _id_field_for_target_kind(target_kind: str) -> str:
    return _TARGET_KIND_MAP[target_kind][1]


def _touch_onboarding_state(draft_root: Path) -> None:
    state_path = draft_root / "manifests" / "onboarding_state.json"
    state = _load_json(state_path)
    if not state:
        return
    state["updated_at"] = _utc_now()
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _cleanup_session_item(item: dict[str, Any]) -> bool:
    removed_any = False
    meta_path = Path(str(item.get("gpt_meta_path", "")))
    meta = _load_json(meta_path) if meta_path.exists() else {}
    final_path_value = item.get("gpt_final_path") or meta.get("final_path")
    for key in ("gpt_processed_path", "gpt_meta_path", "gpt_final_path"):
        path_value = final_path_value if key == "gpt_final_path" else item.get(key)
        if not path_value:
            continue
        path = Path(str(path_value))
        if path.exists() and path.is_file():
            path.unlink()
            removed_any = True
    item["cleanup_status"] = "removed" if removed_any else "already_clean"
    return removed_any


def _validate_approved_review_item(
    item: dict[str, Any],
    *,
    decision: str,
    selected_seed_id: str,
) -> str | None:
    allowed_decisions = item.get("allowed_decisions", [])
    if not decision:
        return "invalid_review_decision"
    if decision not in _ALLOWED_DECISIONS:
        return "invalid_review_decision"
    if not isinstance(allowed_decisions, list) or decision not in allowed_decisions:
        if decision == "adopt_seed_identity":
            return "disallowed_seed_adoption"
        return "invalid_review_decision"
    if decision == "keep_source_identity":
        if selected_seed_id:
            return "invalid_seed_selection"
        return None
    if decision == "defer_identity_resolution":
        if selected_seed_id:
            return "invalid_seed_selection"
        return None
    if decision == "adopt_seed_identity":
        seed_candidates = item.get("seed_candidates", [])
        if not isinstance(seed_candidates, list) or not seed_candidates:
            return "disallowed_seed_adoption"
        if not selected_seed_id and len(seed_candidates) != 1:
            return "invalid_seed_selection"
        selected_seed = _resolve_selected_seed(seed_candidates, selected_seed_id)
        if selected_seed is None:
            return "invalid_seed_selection"
        if not str(selected_seed.get("canonical_id", "")).strip() or not str(selected_seed.get("display_name", "")).strip():
            return "invalid_seed_selection"
    return None


def _failed_item(item: dict[str, Any], apply_status: str) -> dict[str, Any]:
    return {
        "target_kind": str(item.get("target_kind", "")).strip(),
        "target_id": str(item.get("target_id", "")).strip(),
        "display_name": str(item.get("display_name", "")).strip(),
        "apply_status": apply_status,
        "review_decision": str(item.get("review_decision", "")).strip(),
    }


def _build_seed_match_evidence(
    row: dict[str, Any],
    *,
    canonical_id: str,
    display_name: str,
    aliases: list[str],
) -> dict[str, Any]:
    id_field = _ontology_id_field_for_row(row)
    current_id = str(row.get(id_field, "")).strip()
    current_name = str(row.get("display_name", "")).strip()
    current_aliases = [str(item).strip() for item in row.get("aliases", []) if str(item).strip()]
    row_keys = identity_keys(current_name, canonical_id=current_id, aliases=current_aliases)
    candidate_keys = identity_keys(display_name, canonical_id=canonical_id, aliases=aliases)
    shared_keys = sorted(row_keys & candidate_keys)
    shared_aliases = [
        alias
        for alias in aliases
        if any(aliases_equivalent(alias, value) for value in [current_name, *current_aliases] if value)
    ]
    return {
        "shared_id_key": bool(current_id and canonical_id and binding_key(current_id) == binding_key(canonical_id)),
        "shared_display_name_key": bool(current_name and display_name and aliases_equivalent(current_name, display_name)),
        "shared_aliases": shared_aliases,
        "shared_alias_keys": [
            key
            for key in shared_keys
            if key
            and key not in {binding_key(clean_text(current_name)), binding_key(clean_text(current_id))}
            and key not in {binding_key(clean_text(display_name)), binding_key(clean_text(canonical_id))}
        ],
        "current_row_id": current_id,
        "current_row_display_name": current_name,
        "current_row_aliases": current_aliases,
    }


def _row_is_stable(row: dict[str, Any]) -> bool:
    return bool(str(row.get("display_name", "")).strip() and str(row.get(_ontology_id_field_for_row(row), "")).strip())


def _is_clear_seed_upgrade(row: dict[str, Any], candidate: dict[str, Any]) -> bool:
    current_name = str(row.get("display_name", "")).strip()
    candidate_name = str(candidate.get("display_name", "")).strip()
    if not current_name or not candidate_name or aliases_equivalent(current_name, candidate_name):
        return False
    aliases = _coerce_string_list(candidate.get("aliases", []))
    return any(aliases_equivalent(current_name, alias) for alias in aliases) or len(candidate_name) > len(current_name)


def _ontology_id_field_for_row(row: dict[str, Any]) -> str:
    if "hero_id" in row:
        return "hero_id"
    if "ability_id" in row:
        return "ability_id"
    return "event_id"


def _count_by_field(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field, "")).strip() or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return counts


def _resolve_gpt_repo(gpt_repo: str | Path | None) -> Path:
    candidate = _resolve_path(gpt_repo) if gpt_repo is not None else DEFAULT_GPT_REPO.resolve()
    if not candidate.exists() or not candidate.is_dir():
        raise ValueError(f"gpt review repo does not exist or is not a directory: {candidate}")
    return candidate


def _gpt_paths(gpt_repo: Path) -> dict[str, Path]:
    config_path = gpt_repo / "config.yaml"
    paths_config = {}
    if config_path.exists():
        loaded = load_yaml_file(config_path)
        if isinstance(loaded.get("paths"), dict):
            paths_config = loaded["paths"]
    return {
        "inbox": _gpt_repo_path(gpt_repo, paths_config.get("inbox", "inbox")),
        "processing": _gpt_repo_path(gpt_repo, paths_config.get("processing", "processing")),
        "accepted": _gpt_repo_path(gpt_repo, paths_config.get("accepted", "accepted")),
        "rejected": _gpt_repo_path(gpt_repo, paths_config.get("rejected", "rejected")),
    }


def _gpt_repo_path(gpt_repo: Path, path_value: str) -> Path:
    path = Path(str(path_value))
    if path.is_absolute():
        return path.resolve()
    return (gpt_repo / path).resolve()


def _session_manifest_path(game: str, session_id: str) -> Path:
    return REPO_ROOT / "outputs" / "onboarding_identity_review_sessions" / game / f"{session_id}.onboarding_identity_review_session.json"


def _session_id(
    game: str,
    draft_root: Path,
    blocked_rows: list[dict[str, Any]],
    session_name: str | None,
    created_at: str,
) -> str:
    slug = _slug(session_name or "session")
    payload = "\n".join(
        [
            game,
            str(draft_root),
            created_at,
            *[
                f"{row['target_kind']}|{row['target_id']}|{','.join(finding['item_type'] for finding in row['findings'])}"
                for row in blocked_rows
            ],
        ]
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"{game}-onboarding-identity-review-{slug}-{digest}"


def _normalized_review_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "accepted":
        return "approved"
    if normalized == "rejected":
        return "rejected"
    return "unreviewed"


def _resolve_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", str(value).lower()).strip("-") or "item"


def _score_reasoning(blocked: dict[str, Any], seed_candidates: list[dict[str, Any]]) -> str:
    finding_types = ", ".join(finding["item_type"] for finding in blocked["findings"]) or "none"
    return (
        f"Onboarding identity review candidate. Blocking findings={finding_types}. "
        f"Seed candidates={len(seed_candidates)}."
    )
