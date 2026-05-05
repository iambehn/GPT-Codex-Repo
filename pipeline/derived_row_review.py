from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.derived_detection_manifest import derive_game_detection_manifest
from pipeline.game_onboarding import (
    _build_qa_queue,
    _build_population_qa_queue,
    _build_onboarding_state,
    _derive_detection_manifest,
    _load_candidates_by_id_from_assets_manifest,
    _load_game_detection_schema,
    _merge_source_fetch_log_rows,
    _read_csv_rows,
    _refresh_phase_status_from_publish_readiness,
    _write_binding_review_artifacts,
)
from pipeline.simple_yaml import load_yaml_file


DERIVED_ROW_REVIEW_SCHEMA_VERSION = "derived_row_review_v1"
_ALLOWED_REVIEW_STATUSES = {"unreviewed", "approved", "rejected"}
_ALLOWED_DECISIONS = {"accept_candidate", "reject_all_candidates", "defer_row"}
_TERMINAL_BINDING_STATUSES = {"rejected", "superseded"}


def prepare_derived_row_review(
    draft_root: str | Path,
    detection_ids: list[str],
) -> dict[str, Any]:
    resolved_root = Path(draft_root).expanduser().resolve()
    derive_result = derive_game_detection_manifest(resolved_root)
    derived_manifest_path = Path(str(derive_result["manifest_path"]))
    derived_manifest = _load_json_or_yaml_mapping(derived_manifest_path, label="derived detection manifest")
    rows = derived_manifest.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError(f"derived detection manifest rows must be a list: {derived_manifest_path}")

    requested_detection_ids = _ordered_unique_nonempty(detection_ids)
    if not requested_detection_ids:
        raise ValueError("prepare derived row review requires at least one detection_id")

    rows_by_id = {
        detection_id: row
        for row in rows
        if isinstance(row, dict)
        and (detection_id := str(row.get("detection_id", "")).strip())
    }
    missing = [item for item in requested_detection_ids if item not in rows_by_id]
    if missing:
        raise ValueError(f"unknown detection_id values for derived row review: {', '.join(missing)}")

    game = str(derived_manifest.get("game_id", "")).strip()
    if not game:
        raise ValueError(f"derived detection manifest is missing game_id: {derived_manifest_path}")

    bindings = _read_csv_rows(resolved_root / "catalog" / "bindings.csv")
    bindings_by_detection: dict[str, list[dict[str, str]]] = {}
    for row in bindings:
        detection_id = str(row.get("detection_id", "")).strip()
        if detection_id:
            bindings_by_detection.setdefault(detection_id, []).append(row)
    candidates_by_id = _load_candidates_by_id_from_assets_manifest(resolved_root / "manifests" / "assets_manifest.json")

    review_root = resolved_root / "review" / "derived_row_reviews"
    review_root.mkdir(parents=True, exist_ok=True)

    items: list[dict[str, Any]] = []
    for detection_id in requested_detection_ids:
        row = rows_by_id[detection_id]
        candidate_rows = [
            binding
            for binding in sorted(
                bindings_by_detection.get(detection_id, []),
                key=lambda item: (-float(item.get("confidence", 0.0) or 0.0), str(item.get("candidate_id", ""))),
            )
            if str(binding.get("status", "")).strip() not in _TERMINAL_BINDING_STATUSES
        ]
        candidate_options = [
            _candidate_option(binding, candidates_by_id.get(str(binding.get("candidate_id", "")).strip(), {}))
            for binding in candidate_rows
        ]
        recommended_candidate_id = _recommended_candidate_id_from_options(candidate_options)
        recommended_decision = "accept_candidate" if recommended_candidate_id else "defer_row"
        review_payload = {
            "schema_version": DERIVED_ROW_REVIEW_SCHEMA_VERSION,
            "review_id": _review_id(detection_id),
            "created_at": _utc_now(),
            "draft_root": str(resolved_root),
            "game": game,
            "detection_id": detection_id,
            "row_snapshot": row,
            "allowed_review_statuses": sorted(_ALLOWED_REVIEW_STATUSES),
            "allowed_decisions": sorted(_ALLOWED_DECISIONS),
            "candidate_option_count": len(candidate_options),
            "candidate_options": candidate_options,
            "review_status": "unreviewed",
            "review_decision": "",
            "selected_candidate_id": "",
            "review_notes": "",
            "apply_status": "pending_review",
            "recommended_decision": recommended_decision,
            "recommended_candidate_id": recommended_candidate_id,
            "review_file_path": "",
        }
        review_path = review_root / f"{_slugify_detection_id(detection_id)}.review.json"
        review_payload["review_file_path"] = str(review_path)
        review_path.write_text(json.dumps(review_payload, indent=2), encoding="utf-8")
        items.append(
            {
                "detection_id": detection_id,
                "review_file_path": str(review_path),
                "candidate_option_count": len(candidate_options),
                "recommended_decision": review_payload["recommended_decision"],
                "recommended_candidate_id": recommended_candidate_id,
            }
        )

    return {
        "ok": True,
        "status": "derived_row_review_prepared",
        "game": game,
        "draft_root": str(resolved_root),
        "review_root": str(review_root),
        "item_count": len(items),
        "items": items,
    }


def summarize_derived_row_review(review_target: str | Path) -> dict[str, Any]:
    review_path = _resolve_review_target_path(review_target)
    review_files = _discover_review_files(review_path)
    if not review_files:
        raise FileNotFoundError(f"no derived row review files found at: {review_path}")

    rows: list[dict[str, Any]] = []
    pending_count = 0
    auto_accept_eligible_count = 0
    applied_count = 0
    decision_ready_count = 0
    for path in review_files:
        payload = _load_json_or_yaml_mapping(path, label="derived row review")
        if payload.get("schema_version") != DERIVED_ROW_REVIEW_SCHEMA_VERSION:
            raise ValueError(f"unsupported derived row review schema: {path}")
        recommended_candidate_id = _recommended_candidate_id(payload)
        review_status = str(payload.get("review_status", "")).strip()
        review_decision = str(payload.get("review_decision", "")).strip()
        apply_status = str(payload.get("apply_status", "")).strip() or "pending_review"
        candidate_option_count = int(payload.get("candidate_option_count", 0) or 0)
        if apply_status != "applied":
            pending_count += 1
        if recommended_candidate_id:
            auto_accept_eligible_count += 1
        if review_status == "approved" and review_decision:
            decision_ready_count += 1
        if apply_status == "applied":
            applied_count += 1
        row_snapshot = payload.get("row_snapshot", {}) if isinstance(payload.get("row_snapshot"), dict) else {}
        rows.append(
            {
                "detection_id": str(payload.get("detection_id", "")).strip(),
                "review_file_path": str(path),
                "review_status": review_status or "unreviewed",
                "review_decision": review_decision,
                "selected_candidate_id": str(payload.get("selected_candidate_id", "")).strip(),
                "apply_status": apply_status,
                "candidate_option_count": candidate_option_count,
                "recommended_decision": str(payload.get("recommended_decision", "")).strip(),
                "recommended_candidate_id": recommended_candidate_id,
                "auto_accept_eligible": bool(recommended_candidate_id and apply_status != "applied"),
                "target_display_name": str(row_snapshot.get("target_display_name", "")).strip(),
                "asset_family": str(row_snapshot.get("asset_family", "")).strip(),
                "blocking_publish": bool(row_snapshot.get("blocking_publish", False)),
                "row_status": str(row_snapshot.get("status", "")).strip(),
            }
        )

    rows.sort(key=lambda item: (item["apply_status"] == "applied", item["review_status"], item["asset_family"], item["detection_id"]))
    return {
        "ok": True,
        "status": "derived_row_review_summarized",
        "review_target": str(review_path),
        "review_file_count": len(review_files),
        "pending_count": pending_count,
        "decision_ready_count": decision_ready_count,
        "auto_accept_eligible_count": auto_accept_eligible_count,
        "applied_count": applied_count,
        "rows": rows,
    }


def apply_derived_row_review(
    review_target: str | Path,
    *,
    accept_recommended: bool = False,
    only_auto_populated: bool = False,
    reject_zero_candidate: bool = False,
    defer_zero_candidate: bool = False,
) -> dict[str, Any]:
    if reject_zero_candidate and defer_zero_candidate:
        raise ValueError("reject_zero_candidate and defer_zero_candidate cannot both be enabled")

    review_path = _resolve_review_target_path(review_target)
    review_files = _discover_review_files(review_path)
    if not review_files:
        raise FileNotFoundError(f"no derived row review files found at: {review_path}")

    grouped_by_draft: dict[Path, list[dict[str, Any]]] = {}
    for path in review_files:
        payload = _load_json_or_yaml_mapping(path, label="derived row review")
        if payload.get("schema_version") != DERIVED_ROW_REVIEW_SCHEMA_VERSION:
            raise ValueError(f"unsupported derived row review schema: {path}")
        payload["auto_populated_in_run"] = False
        if accept_recommended:
            _apply_recommended_defaults(payload)
        if reject_zero_candidate:
            _apply_zero_candidate_defaults(
                payload,
                decision="reject_all_candidates",
                note="auto-rejected zero-candidate row",
            )
        if defer_zero_candidate:
            _apply_zero_candidate_defaults(
                payload,
                decision="defer_row",
                note="auto-deferred zero-candidate row",
            )
        draft_root = Path(str(payload.get("draft_root", ""))).expanduser().resolve()
        grouped_by_draft.setdefault(draft_root, []).append(payload)

    applied_reviews: list[dict[str, Any]] = []
    skipped_reviews: list[dict[str, Any]] = []
    failed_reviews: list[dict[str, Any]] = []

    for draft_root, payloads in grouped_by_draft.items():
        manifest_path = draft_root / "manifests" / "assets_manifest.json"
        manifest_payload = _load_json_or_yaml_mapping(manifest_path, label="assets manifest")
        game = str(manifest_payload.get("game_id", "")).strip()
        if not game:
            raise ValueError(f"draft assets manifest must include game_id: {manifest_path}")

        ontology_payload = _load_json_or_yaml_mapping(draft_root / "entities.yaml", label="entities")
        ontology = {
            "heroes": ontology_payload.get("heroes", []),
            "abilities": ontology_payload.get("abilities", []),
            "events": ontology_payload.get("events", []),
        }
        detection_schema = _load_game_detection_schema(draft_root)
        detection_manifest = _derive_detection_manifest(game, ontology, detection_schema, adapter=_adapter_for_game(game))
        detection_rows = detection_manifest.get("rows", [])
        if not isinstance(detection_rows, list):
            raise ValueError("refreshed draft detection manifest must define a rows list")
        detection_rows_by_id = {
            detection_id: row
            for row in detection_rows
            if isinstance(row, dict)
            and (detection_id := str(row.get("detection_id", "")).strip())
        }

        bindings = _read_csv_rows(draft_root / "catalog" / "bindings.csv")
        candidates = manifest_payload.get("candidates", [])
        if not isinstance(candidates, list):
            raise ValueError("draft assets manifest candidates must be a list")
        source_fetch_log = list(manifest_payload.get("source_fetch_log", [])) if isinstance(manifest_payload.get("source_fetch_log", []), list) else []
        population_findings = list(manifest_payload.get("population_findings", [])) if isinstance(manifest_payload.get("population_findings", []), list) else []

        for payload in payloads:
            if only_auto_populated and not bool(payload.get("auto_populated_in_run", False)):
                payload["apply_status"] = "skipped_not_auto_populated"
                review_file_path = Path(str(payload.get("review_file_path", ""))).expanduser().resolve()
                review_file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                skipped_reviews.append(
                    {
                        "detection_id": str(payload.get("detection_id", "")).strip(),
                        "review_file_path": str(payload.get("review_file_path", "")).strip(),
                        "apply_status": "skipped_not_auto_populated",
                    }
                )
                continue
            result_bucket = _apply_single_review(payload, detection_rows_by_id, bindings)
            review_file_path = Path(str(payload.get("review_file_path", ""))).expanduser().resolve()
            review_file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            if result_bucket["bucket"] == "applied":
                applied_reviews.append(result_bucket["row"])
            elif result_bucket["bucket"] == "skipped":
                skipped_reviews.append(result_bucket["row"])
            else:
                failed_reviews.append(result_bucket["row"])

        merged_source_fetch_log = _merge_source_fetch_log_rows(source_fetch_log, [], [])
        qa_queue = _build_qa_queue(
            detection_rows,
            candidates,
            bindings,
            existing_qa=_build_population_qa_queue(
                ontology,
                candidates,
                population_findings,
                source_failures=[
                    row
                    for row in merged_source_fetch_log
                    if str(row.get("status", "")).strip() == "fetch_failed"
                ],
            ),
        )
        phase_status = "ready_to_publish" if not qa_queue else "bindings_pending"
        manifest_payload["bindings"] = bindings
        manifest_payload["phase_status"] = phase_status
        state_payload = _build_onboarding_state(
            game,
            phase_status=phase_status,
            source_count=int(manifest_payload.get("source_count", 0) or 0),
            schema_path="manifests/game_detection_schema.yaml",
        )
        _write_binding_review_artifacts(
            draft_root,
            ontology=ontology,
            detection_manifest=detection_manifest,
            candidates=candidates,
            bindings=bindings,
            qa_queue=qa_queue,
            manifest_payload=manifest_payload,
            state_payload=state_payload,
        )
        _refresh_phase_status_from_publish_readiness(
            draft_root,
            ontology=ontology,
            detection_manifest=detection_manifest,
            candidates=candidates,
            bindings=bindings,
            qa_queue=qa_queue,
            manifest_payload=manifest_payload,
            state_payload=state_payload,
        )
        derive_game_detection_manifest(draft_root)

    return {
        "ok": True,
        "status": "derived_row_review_applied",
        "review_target": str(review_path),
        "accept_recommended": accept_recommended,
        "only_auto_populated": only_auto_populated,
        "reject_zero_candidate": reject_zero_candidate,
        "defer_zero_candidate": defer_zero_candidate,
        "applied_count": len(applied_reviews),
        "skipped_count": len(skipped_reviews),
        "failed_count": len(failed_reviews),
        "applied_reviews": applied_reviews,
        "skipped_reviews": skipped_reviews,
        "failed_reviews": failed_reviews,
    }


def _apply_single_review(
    payload: dict[str, Any],
    detection_rows_by_id: dict[str, dict[str, Any]],
    bindings: list[dict[str, str]],
) -> dict[str, Any]:
    detection_id = str(payload.get("detection_id", "")).strip()
    row = {
        "detection_id": detection_id,
        "review_file_path": str(payload.get("review_file_path", "")).strip(),
    }
    review_status = str(payload.get("review_status", "")).strip()
    review_decision = str(payload.get("review_decision", "")).strip()
    selected_candidate_id = str(payload.get("selected_candidate_id", "")).strip()
    review_notes = str(payload.get("review_notes", "")).strip()

    if detection_id not in detection_rows_by_id:
        payload["apply_status"] = "missing_detection_row"
        row["apply_status"] = "missing_detection_row"
        return {"bucket": "failed", "row": row}
    if review_status not in _ALLOWED_REVIEW_STATUSES:
        payload["apply_status"] = "invalid_review_status"
        row["apply_status"] = "invalid_review_status"
        return {"bucket": "failed", "row": row}
    if review_status != "approved":
        payload["apply_status"] = "unreviewed"
        row["apply_status"] = "unreviewed"
        return {"bucket": "skipped", "row": row}
    if review_decision not in _ALLOWED_DECISIONS:
        payload["apply_status"] = "invalid_review_decision"
        row["apply_status"] = "invalid_review_decision"
        return {"bucket": "failed", "row": row}

    candidate_ids = {
        str(item.get("candidate_id", "")).strip()
        for item in payload.get("candidate_options", [])
        if isinstance(item, dict) and str(item.get("candidate_id", "")).strip()
    }

    if review_decision == "accept_candidate":
        if not selected_candidate_id:
            payload["apply_status"] = "missing_selected_candidate"
            row["apply_status"] = "missing_selected_candidate"
            return {"bucket": "failed", "row": row}
        if selected_candidate_id not in candidate_ids:
            payload["apply_status"] = "invalid_selected_candidate"
            row["apply_status"] = "invalid_selected_candidate"
            return {"bucket": "failed", "row": row}
        found = False
        for binding in bindings:
            if str(binding.get("detection_id", "")).strip() != detection_id:
                continue
            candidate_id = str(binding.get("candidate_id", "")).strip()
            if candidate_id == selected_candidate_id:
                binding["status"] = "accepted"
                binding["review_notes"] = review_notes
                binding["derived_row_review_status"] = "approved"
                binding["derived_row_review_decision"] = review_decision
                binding["derived_row_reviewed_at"] = _utc_now()
                found = True
            elif str(binding.get("status", "")).strip() not in _TERMINAL_BINDING_STATUSES:
                binding["status"] = "superseded"
                binding["review_notes"] = review_notes
                binding["derived_row_review_status"] = "approved"
                binding["derived_row_review_decision"] = review_decision
                binding["derived_row_reviewed_at"] = _utc_now()
        if not found:
            payload["apply_status"] = "selected_binding_not_found"
            row["apply_status"] = "selected_binding_not_found"
            return {"bucket": "failed", "row": row}
    elif review_decision == "reject_all_candidates":
        for binding in bindings:
            if str(binding.get("detection_id", "")).strip() != detection_id:
                continue
            binding["status"] = "rejected"
            binding["review_notes"] = review_notes
            binding["derived_row_review_status"] = "approved"
            binding["derived_row_review_decision"] = review_decision
            binding["derived_row_reviewed_at"] = _utc_now()
    else:
        for binding in bindings:
            if str(binding.get("detection_id", "")).strip() != detection_id:
                continue
            binding["review_notes"] = review_notes
            binding["derived_row_review_status"] = "approved"
            binding["derived_row_review_decision"] = review_decision
            binding["derived_row_reviewed_at"] = _utc_now()

    payload["apply_status"] = "applied"
    payload["applied_at"] = _utc_now()
    row["apply_status"] = "applied"
    row["review_decision"] = review_decision
    row["selected_candidate_id"] = selected_candidate_id or None
    return {"bucket": "applied", "row": row}


def _candidate_option(binding: dict[str, str], candidate: dict[str, Any]) -> dict[str, Any]:
    candidate_id = str(binding.get("candidate_id", "")).strip()
    return {
        "binding_id": str(binding.get("binding_id", "")).strip(),
        "candidate_id": candidate_id,
        "candidate_display_name": str(binding.get("candidate_display_name", "")).strip() or str(candidate.get("display_name", "")).strip(),
        "confidence": float(binding.get("confidence", 0.0) or 0.0),
        "binding_score": float(binding.get("binding_score", 0.0) or 0.0),
        "status": str(binding.get("status", "")).strip(),
        "reason": str(binding.get("reason", "")).strip(),
        "source_url": str(candidate.get("source_url", "")).strip(),
        "source_kind": str(candidate.get("source_kind", "")).strip(),
        "candidate_quality": str(candidate.get("candidate_quality", "")).strip(),
        "master_path": str(candidate.get("master_path", "")).strip(),
    }


def _adapter_for_game(game: str) -> Any:
    from pipeline.onboarding_adapters import get_onboarding_adapter

    return get_onboarding_adapter(game)


def _load_json_or_yaml_mapping(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing {label}: {path}")
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        payload = load_yaml_file(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a mapping: {path}")
    return payload


def _discover_review_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(path.rglob("*.review.json"))
    return []


def _resolve_review_target_path(review_target: str | Path) -> Path:
    path = Path(review_target).expanduser().resolve()
    if path.is_dir() and not path.name.endswith("derived_row_reviews") and not list(path.rglob("*.review.json")):
        candidate = path / "review" / "derived_row_reviews"
        if candidate.exists():
            return candidate.resolve()
    return path


def _recommended_candidate_id_from_options(candidate_options: list[dict[str, Any]]) -> str:
    if len(candidate_options) != 1:
        return ""
    return str(candidate_options[0].get("candidate_id", "")).strip()


def _recommended_candidate_id(payload: dict[str, Any]) -> str:
    explicit = str(payload.get("recommended_candidate_id", "")).strip()
    if explicit:
        return explicit
    if str(payload.get("recommended_decision", "")).strip() != "accept_candidate":
        return ""
    candidate_options = payload.get("candidate_options", [])
    if not isinstance(candidate_options, list):
        return ""
    return _recommended_candidate_id_from_options([item for item in candidate_options if isinstance(item, dict)])


def _apply_recommended_defaults(payload: dict[str, Any]) -> None:
    if str(payload.get("apply_status", "")).strip() == "applied":
        return
    if str(payload.get("review_decision", "")).strip():
        return
    recommended_candidate_id = _recommended_candidate_id(payload)
    recommended_decision = str(payload.get("recommended_decision", "")).strip()
    if recommended_decision != "accept_candidate" or not recommended_candidate_id:
        return
    payload["review_status"] = "approved"
    payload["review_decision"] = "accept_candidate"
    payload["selected_candidate_id"] = recommended_candidate_id
    if not str(payload.get("review_notes", "")).strip():
        payload["review_notes"] = "auto-applied recommended candidate"
    payload["auto_populated_in_run"] = True


def _apply_zero_candidate_defaults(
    payload: dict[str, Any],
    *,
    decision: str,
    note: str,
) -> None:
    if str(payload.get("apply_status", "")).strip() == "applied":
        return
    if str(payload.get("review_decision", "")).strip():
        return
    if int(payload.get("candidate_option_count", 0) or 0) != 0:
        return
    payload["review_status"] = "approved"
    payload["review_decision"] = decision
    payload["selected_candidate_id"] = ""
    if not str(payload.get("review_notes", "")).strip():
        payload["review_notes"] = note
    payload["auto_populated_in_run"] = True


def _ordered_unique_nonempty(values: list[str]) -> list[str]:
    ordered: list[str] = []
    for value in values:
        cleaned = str(value).strip()
        if cleaned and cleaned not in ordered:
            ordered.append(cleaned)
    return ordered


def _slugify_detection_id(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "row"


def _review_id(detection_id: str) -> str:
    digest = hashlib.sha1(detection_id.encode("utf-8")).hexdigest()[:12]
    return f"derived-row-review-{digest}"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()
