from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.game_onboarding import (
    _asset_family_for_ability_row,
    _build_detection_row,
    _load_game_detection_schema,
    _read_csv_rows,
)
from pipeline.onboarding_adapters import get_onboarding_adapter
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file


def derive_game_detection_manifest(
    draft_root: str | Path,
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_root = Path(draft_root).expanduser().resolve()
    schema = _load_game_detection_schema(resolved_root)
    entities_payload = _load_yaml_mapping(resolved_root / "entities.yaml", label="entities")
    game_payload = _load_yaml_mapping(resolved_root / "game.yaml", label="game")

    game = str(
        game_payload.get("game_id")
        or schema.get("game_id")
        or ""
    ).strip()
    if not game:
        raise ValueError("derived detection manifest requires game_id in game.yaml or game detection schema")

    ontology = {
        "heroes": _coerce_row_list(entities_payload.get("heroes", []), label="entities heroes"),
        "abilities": _coerce_row_list(entities_payload.get("abilities", []), label="entities abilities"),
        "events": _coerce_row_list(entities_payload.get("events", []), label="entities events"),
    }
    adapter = get_onboarding_adapter(game)
    families = schema.get("families", {})
    if not isinstance(families, dict):
        raise ValueError("game detection schema must define a families mapping")

    bindings = _read_csv_rows(resolved_root / "catalog" / "bindings.csv")
    accepted_bindings_by_detection = _best_accepted_bindings_by_detection(bindings)
    binding_counts_by_detection = _binding_counts_by_detection(bindings)
    candidates_by_id = _load_candidates_by_id(resolved_root / "manifests" / "assets_manifest.json")

    rows: list[dict[str, Any]] = []

    hero_family = families.get("hero_portrait")
    if isinstance(hero_family, dict):
        for row in ontology["heroes"]:
            rows.append(
                _derive_manifest_row(
                    game=game,
                    ontology_row=row,
                    family_spec=hero_family,
                    asset_family="hero_portrait",
                    family_enabled=hero_family.get("enabled", True) is not False,
                    accepted_bindings_by_detection=accepted_bindings_by_detection,
                    binding_counts_by_detection=binding_counts_by_detection,
                    candidates_by_id=candidates_by_id,
                )
            )

    for row in ontology["abilities"]:
        asset_family = _asset_family_for_ability_row(row, adapter=adapter)
        family_spec = families.get(asset_family)
        if not isinstance(family_spec, dict):
            continue
        rows.append(
            _derive_manifest_row(
                game=game,
                ontology_row=row,
                family_spec=family_spec,
                asset_family=asset_family,
                family_enabled=family_spec.get("enabled", True) is not False,
                accepted_bindings_by_detection=accepted_bindings_by_detection,
                binding_counts_by_detection=binding_counts_by_detection,
                candidates_by_id=candidates_by_id,
            )
        )

    medal_family = families.get("medal_icon")
    if isinstance(medal_family, dict):
        for row in ontology["events"]:
            rows.append(
                _derive_manifest_row(
                    game=game,
                    ontology_row=row,
                    family_spec=medal_family,
                    asset_family="medal_icon",
                    family_enabled=medal_family.get("enabled", True) is not False,
                    accepted_bindings_by_detection=accepted_bindings_by_detection,
                    binding_counts_by_detection=binding_counts_by_detection,
                    candidates_by_id=candidates_by_id,
                )
            )

    rows.sort(key=lambda item: (str(item.get("asset_family", "")), str(item.get("target_id", ""))))
    family_summaries = _build_family_summaries(families=families, ontology=ontology, adapter=adapter)
    counts = _build_counts(rows, family_summaries)
    manifest = {
        "schema_version": "derived_game_detection_manifest_v1",
        "baseline_schema_version": str(
            schema.get("baseline_schema_version", schema.get("schema_version", "runtime_detection_schema_v1"))
        ),
        "game_id": game,
        "draft_root": str(resolved_root),
        "source_artifacts": {
            "game": str(resolved_root / "game.yaml"),
            "entities": str(resolved_root / "entities.yaml"),
            "game_detection_schema": str(resolved_root / "manifests" / "game_detection_schema.yaml"),
            "bindings_csv": str(resolved_root / "catalog" / "bindings.csv"),
            "assets_manifest": str(resolved_root / "manifests" / "assets_manifest.json"),
        },
        "counts": counts,
        "family_summaries": family_summaries,
        "rows": rows,
    }

    manifest_path = Path(output_path).expanduser().resolve() if output_path else resolved_root / "manifests" / "derived_detection_manifest.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    dump_yaml_file(manifest_path, manifest)

    return {
        "ok": True,
        "status": "derived_detection_manifest_written",
        "game": game,
        "draft_root": str(resolved_root),
        "manifest_path": str(manifest_path),
        "counts": counts,
        "artifacts": {
            "derived_detection_manifest": str(manifest_path),
        },
    }


def _load_yaml_mapping(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"draft is missing {label}: {path}")
    payload = load_yaml_file(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a mapping: {path}")
    return payload


def _coerce_row_list(value: Any, *, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    rows: list[dict[str, Any]] = []
    for row in value:
        if not isinstance(row, dict):
            raise ValueError(f"{label} entries must be mappings")
        rows.append(row)
    return rows


def _load_candidates_by_id(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"assets manifest must be a mapping: {path}")
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list):
        raise ValueError(f"assets manifest candidates must be a list: {path}")
    return {
        candidate_id: row
        for row in candidates
        if isinstance(row, dict)
        and (candidate_id := str(row.get("candidate_id", "")).strip())
    }


def _best_accepted_bindings_by_detection(bindings: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    accepted: dict[str, dict[str, Any]] = {}
    for row in bindings:
        if str(row.get("status", "")).strip() != "accepted":
            continue
        detection_id = str(row.get("detection_id", "")).strip()
        if not detection_id:
            continue
        existing = accepted.get(detection_id)
        if existing is None or float(row.get("confidence", 0.0) or 0.0) >= float(existing.get("confidence", 0.0) or 0.0):
            accepted[detection_id] = row
    return accepted


def _binding_counts_by_detection(bindings: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in bindings:
        detection_id = str(row.get("detection_id", "")).strip()
        if not detection_id:
            continue
        counts[detection_id] = counts.get(detection_id, 0) + 1
    return counts


def _derive_manifest_row(
    *,
    game: str,
    ontology_row: dict[str, Any],
    family_spec: dict[str, Any],
    asset_family: str,
    family_enabled: bool,
    accepted_bindings_by_detection: dict[str, dict[str, Any]],
    binding_counts_by_detection: dict[str, int],
    candidates_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    base_row = _build_detection_row(game, ontology_row, family_spec, asset_family=asset_family)
    detection_id = str(base_row.get("detection_id", "")).strip()
    accepted_binding = accepted_bindings_by_detection.get(detection_id)
    binding_count = int(binding_counts_by_detection.get(detection_id, 0))

    if not family_enabled:
        status = "optional_family_disabled"
        required = False
        reason = "family is disabled in the game detection schema adaptation"
        binding_status = "not_applicable"
    elif str(base_row.get("status", "")).strip() == "missing_semantic_values":
        status = "unresolved_missing_semantics"
        required = bool(base_row.get("requires_asset", True))
        reason = "required semantic fields were not derived for this detection row"
        binding_status = "unbound"
    elif accepted_binding is not None:
        status = "resolved"
        required = bool(base_row.get("requires_asset", True))
        reason = "accepted binding exists for this detection row"
        binding_status = "accepted"
    elif binding_count > 0:
        status = "unresolved_pending_review"
        required = bool(base_row.get("requires_asset", True))
        reason = "candidate bindings exist but no accepted binding has been selected yet"
        binding_status = "pending_review"
    else:
        status = "unresolved"
        required = bool(base_row.get("requires_asset", True))
        reason = "no accepted binding exists for this required detection row yet"
        binding_status = "unbound"

    accepted_candidate_id = str(accepted_binding.get("candidate_id", "")).strip() if accepted_binding else ""
    accepted_candidate = candidates_by_id.get(accepted_candidate_id, {}) if accepted_candidate_id else {}
    return {
        **base_row,
        "status": status,
        "required": required,
        "blocking_publish": bool(required and status != "resolved"),
        "binding_status": binding_status,
        "reason": reason,
        "semantic_ids": list(base_row.get("required_semantic_fields", [])),
        "accepted_candidate_id": accepted_candidate_id or None,
        "accepted_candidate_master_path": str(accepted_candidate.get("master_path", "")).strip() or None,
        "accepted_candidate_source_url": str(accepted_candidate.get("source_url", "")).strip() or None,
        "binding_candidate_count": binding_count,
        "provenance_basis": {
            "ontology_collection": str(base_row.get("ontology_collection", "")).strip() or None,
            "source_page_url": str(ontology_row.get("source_page_url", "")).strip() or None,
            "source_role": str(ontology_row.get("source_role", "")).strip() or None,
            "canonical_identity_basis": str(ontology_row.get("canonical_identity_basis", "")).strip() or None,
        },
    }


def _build_family_summaries(
    *,
    families: dict[str, Any],
    ontology: dict[str, list[dict[str, Any]]],
    adapter: Any,
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for asset_family, family_spec in sorted(families.items()):
        if not isinstance(family_spec, dict):
            continue
        ontology_collection = str(family_spec.get("ontology_collection", "")).strip()
        enabled = family_spec.get("enabled", True) is not False
        target_count = _family_target_count(
            asset_family=asset_family,
            ontology_collection=ontology_collection,
            ontology=ontology,
            adapter=adapter,
        )
        if not enabled:
            status = "optional_family_disabled"
            reason = "family disabled in game detection schema adaptation"
        elif target_count == 0:
            status = "optional_unsupported"
            reason = "no ontology targets were derived for this family"
        else:
            status = "supported"
            reason = "family has ontology targets and remains active"
        summaries.append(
            {
                "asset_family": asset_family,
                "ontology_collection": ontology_collection or None,
                "enabled": enabled,
                "requires_asset": bool(family_spec.get("requires_asset", True)),
                "target_count": target_count,
                "status": status,
                "reason": reason,
                "roi_ref": str(family_spec.get("roi_ref", "")).strip() or None,
            }
        )
    return summaries


def _family_target_count(
    *,
    asset_family: str,
    ontology_collection: str,
    ontology: dict[str, list[dict[str, Any]]],
    adapter: Any,
) -> int:
    if ontology_collection == "abilities" and asset_family in {"ability_icon", "equipment_icon"}:
        return sum(
            1
            for row in ontology.get("abilities", [])
            if _asset_family_for_ability_row(row, adapter=adapter) == asset_family
        )
    return len(ontology.get(ontology_collection, []))


def _build_counts(rows: list[dict[str, Any]], family_summaries: list[dict[str, Any]]) -> dict[str, int]:
    required_rows = sum(1 for row in rows if bool(row.get("required", False)))
    resolved_rows = sum(1 for row in rows if str(row.get("status", "")).strip() == "resolved")
    unresolved_required_rows = sum(
        1
        for row in rows
        if bool(row.get("required", False)) and str(row.get("status", "")).strip() != "resolved"
    )
    optional_rows = len(rows) - required_rows
    unresolved_optional_rows = sum(
        1
        for row in rows
        if not bool(row.get("required", False)) and str(row.get("status", "")).strip() != "resolved"
    )
    return {
        "row_count": len(rows),
        "required_row_count": required_rows,
        "optional_row_count": optional_rows,
        "resolved_row_count": resolved_rows,
        "unresolved_required_row_count": unresolved_required_rows,
        "unresolved_optional_row_count": unresolved_optional_rows,
        "family_count": len(family_summaries),
        "disabled_family_count": sum(1 for row in family_summaries if str(row.get("status", "")) == "optional_family_disabled"),
        "unsupported_family_count": sum(1 for row in family_summaries if str(row.get("status", "")) == "optional_unsupported"),
    }
