from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from pipeline.asset_candidate_quality import analyze_asset_candidate, score_binding_candidate
from pipeline.onboarding_adapters import GameOnboardingAdapter, StarterSeedSpec, get_onboarding_adapter
from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from pipeline.source_normalization import (
    SourceFetchError as WikiFetchError,
    build_fetch_target,
    fetch_source_record,
    image_anchor_text,
    normalize_source_url,
)
from pipeline.structured_source_fields import (
    aliases_equivalent,
    binding_key,
    clean_text,
    extract_structured_fields,
    find_explicit_listing_matches,
    find_explicit_listing_match,
    identity_keys,
    merge_aliases,
    reconcile_identity,
    slugify,
)
from pipeline.runtime_ontology import load_runtime_signal_event_ontology, validate_group_by_fields, validate_runtime_rule_terms


REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_TIMEOUT_SECONDS = 20
_RUNTIME_DETECTION_SCHEMA_PATH = REPO_ROOT / "starter_assets" / "runtime_detection_schema.yaml"
_GAME_DETECTION_SCHEMA_DRAFT = "game_detection_schema_v1"
_ONBOARDING_STATE_VERSION = "game_onboarding_state_v1"
_DIRECT_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
_ASSET_FAMILY_DEFAULTS = {
    "hero_portrait": {
        "roi_ref": "hero_portrait",
        "match_method": "TM_CCOEFF_NORMED",
        "threshold": 0.90,
        "scale_set": [0.9, 1.0, 1.1],
        "temporal_window": 3,
    },
    "ability_icon": {
        "roi_ref": "ability_hud",
        "match_method": "TM_CCOEFF_NORMED",
        "threshold": 0.93,
        "scale_set": [0.9, 1.0, 1.1],
        "temporal_window": 3,
    },
    "equipment_icon": {
        "roi_ref": "ability_hud",
        "match_method": "TM_CCOEFF_NORMED",
        "threshold": 0.93,
        "scale_set": [0.9, 1.0, 1.1],
        "temporal_window": 3,
    },
    "medal_icon": {
        "roi_ref": "medal_area",
        "match_method": "TM_CCORR_NORMED",
        "threshold": 0.95,
        "scale_set": [0.9, 1.0],
        "temporal_window": 2,
    },
    "hud_icon": {
        "roi_ref": "kill_feed",
        "match_method": "TM_CCOEFF_NORMED",
        "threshold": 0.90,
        "scale_set": [1.0],
        "temporal_window": 2,
    },
}
_GENERIC_IMAGE_WORDS = {
    "icon",
    "portrait",
    "logo",
    "artwork",
    "render",
    "splash",
    "image",
    "png",
    "jpg",
    "jpeg",
}


@dataclass(frozen=True)
class OnboardingSource:
    role: str
    url: str
    notes: str = ""


def adapt_game_schema(
    game: str,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    repo_root = (repo_root or REPO_ROOT).resolve()
    timestamp = _timestamp_slug()
    drafts_parent = repo_root / "assets" / "games" / game / "drafts" / "onboarding"
    drafts_parent.mkdir(parents=True, exist_ok=True)
    draft_root = drafts_parent / timestamp
    stage_root = Path(tempfile.mkdtemp(prefix=f".{timestamp}.", dir=drafts_parent))
    catalog_root = stage_root / "catalog"
    masters_root = stage_root / "masters"
    review_root = stage_root / "review"
    manifests_root = stage_root / "manifests"
    for path in (catalog_root, masters_root, review_root, manifests_root):
        path.mkdir(parents=True, exist_ok=True)

    try:
        baseline_schema = _load_runtime_detection_schema(repo_root=repo_root)
        game_schema = _adapt_detection_schema(game, baseline_schema, repo_root=repo_root)
        ontology = _empty_ontology()
        detection_manifest = _empty_detection_manifest(game, game_schema)
        hud = _default_hud_for_game(game, repo_root=repo_root)
        weights = _default_weights_for_game(game, repo_root=repo_root)
        game_payload = _game_payload_for_game(game, repo_root=repo_root)
        state_payload = _build_onboarding_state(
            game,
            phase_status="schema_adapted",
            schema_path="manifests/game_detection_schema.yaml",
        )
        manifest_payload = _build_assets_manifest_payload(
            game=game,
            source_records=[],
            detection_manifest=detection_manifest,
            candidates=[],
            bindings=[],
            phase_status="schema_adapted",
            schema_path="manifests/game_detection_schema.yaml",
        )
        _write_schema_adaptation_artifacts(
            stage_root,
            catalog_root,
            game_payload=game_payload,
            ontology=ontology,
            detection_manifest=detection_manifest,
            game_schema=game_schema,
            hud=hud,
            weights=weights,
            manifest_payload=manifest_payload,
            state_payload=state_payload,
        )
        if draft_root.exists():
            shutil.rmtree(draft_root)
        stage_root.rename(draft_root)
    except Exception:
        shutil.rmtree(stage_root, ignore_errors=True)
        raise

    return {
        "ok": True,
        "status": "schema_adapted",
        "game": game,
        "draft_root": str(draft_root),
        "catalog_root": str(draft_root / "catalog"),
        "masters_root": str(draft_root / "masters"),
        "review_root": str(draft_root / "review"),
        "counts": {
            "active_asset_families": len(game_schema.get("families", {})),
            "detection_rows": 0,
            "candidate_assets": 0,
            "binding_candidates": 0,
            "qa_queue": 0,
        },
        "artifacts": {
            "game": str(draft_root / "game.yaml"),
            "entities": str(draft_root / "entities.yaml"),
            "hud": str(draft_root / "hud.yaml"),
            "weights": str(draft_root / "weights.yaml"),
            "game_detection_schema": str(draft_root / "manifests" / "game_detection_schema.yaml"),
            "assets_manifest": str(draft_root / "manifests" / "assets_manifest.json"),
            "onboarding_state": str(draft_root / "manifests" / "onboarding_state.json"),
            "detection_manifest": str(draft_root / "manifests" / "detection_manifest.yaml"),
        },
    }


def onboard_game_from_manifest(
    game: str,
    source_manifest: str | Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    adapter = get_onboarding_adapter(game)
    manifest_data = load_yaml_file(source_manifest)
    manifest_game = str(manifest_data.get("game", game))
    if manifest_game != game:
        raise ValueError(f"source manifest game '{manifest_game}' does not match requested game '{game}'")
    raw_sources = manifest_data.get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ValueError("source manifest must define a non-empty 'sources' list")
    sources: list[OnboardingSource] = []
    for row in raw_sources:
        if not isinstance(row, dict):
            raise ValueError("source rows must be objects with 'role' and 'url'")
        role = str(row.get("role", "")).strip()
        url = str(row.get("url", "")).strip()
        notes = str(row.get("notes", "")).strip()
        if not role or not url:
            raise ValueError("source rows must include 'role' and 'url'")
        if role not in adapter.supported_source_roles:
            raise ValueError(f"unsupported onboarding source role: {role}")
        sources.append(OnboardingSource(role=role, url=url, notes=notes))
    return onboard_game_from_sources(game, sources, repo_root=repo_root)


def onboard_game_from_sources(
    game: str,
    sources: list[OnboardingSource],
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    repo_root = (repo_root or REPO_ROOT).resolve()
    schema_result = adapt_game_schema(game, repo_root=repo_root)
    draft_root = Path(schema_result["draft_root"])
    ingest_onboarding_sources(draft_root, sources, repo_root=repo_root)
    return build_onboarding_draft(draft_root, repo_root=repo_root)


def ingest_onboarding_sources(
    schema_draft_or_game: str | Path,
    sources: list[OnboardingSource],
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    repo_root = (repo_root or REPO_ROOT).resolve()
    draft_root = _resolve_or_create_onboarding_draft(schema_draft_or_game, repo_root=repo_root)
    game_schema = _load_game_detection_schema(draft_root)
    game = str(game_schema.get("game_id", "")).strip()
    adapter = get_onboarding_adapter(game)
    if not game:
        raise ValueError("game detection schema draft must include game_id")

    normalized_sources = [_normalize_source(source) for source in sources]
    for source in normalized_sources:
        if source.role not in adapter.supported_source_roles:
            raise ValueError(f"unsupported onboarding source role for '{game}': {source.role}")
    source_records: list[dict[str, Any]] = []
    source_failures: list[dict[str, Any]] = []
    first_failure: WikiFetchError | None = None
    for source in normalized_sources:
        try:
            source_records.append(_fetch_source_record(source))
        except WikiFetchError as exc:
            if first_failure is None:
                first_failure = exc
            source_failures.append(_source_failure_row(source, exc))
    if not source_records and first_failure is not None:
        raise first_failure
    ontology, ontology_findings = _build_ontology(game, source_records, repo_root=repo_root, adapter=adapter)
    detection_manifest = _derive_detection_manifest(game, ontology, game_schema, adapter=adapter)
    candidates = _collect_candidate_assets(source_records, draft_root / "masters", adapter=adapter)
    qa_queue = _build_population_qa_queue(
        ontology,
        candidates,
        ontology_findings,
        source_failures=source_failures,
    )
    manifest_payload = _build_assets_manifest_payload(
        game=game,
        source_records=source_records,
        detection_manifest=detection_manifest,
        candidates=candidates,
        bindings=[],
        population_findings=ontology_findings,
        source_failures=source_failures,
        phase_status="sources_ingested",
        schema_path="manifests/game_detection_schema.yaml",
    )
    state_payload = _build_onboarding_state(
        game,
        phase_status="sources_ingested",
        source_count=len(source_records),
        schema_path="manifests/game_detection_schema.yaml",
    )
    _write_source_ingestion_artifacts(
        draft_root,
        ontology=ontology,
        detection_manifest=detection_manifest,
        candidates=candidates,
        qa_queue=qa_queue,
        manifest_payload=manifest_payload,
        state_payload=state_payload,
    )

    return {
        "ok": True,
        "status": "sources_ingested",
        "game": game,
        "draft_root": str(draft_root),
        "catalog_root": str(draft_root / "catalog"),
        "masters_root": str(draft_root / "masters"),
        "review_root": str(draft_root / "review"),
        "source_count": len(source_records),
        "counts": {
            "heroes": len(ontology["heroes"]),
            "abilities": len(ontology["abilities"]),
            "events": len(ontology["events"]),
            "detection_rows": int(detection_manifest["row_count"]),
            "candidate_assets": len(candidates),
            "binding_candidates": 0,
            "qa_queue": len(qa_queue),
            "source_failures": len(source_failures),
        },
        "artifacts": {
            "game_detection_schema": str(draft_root / "manifests" / "game_detection_schema.yaml"),
            "assets_manifest": str(draft_root / "manifests" / "assets_manifest.json"),
            "onboarding_state": str(draft_root / "manifests" / "onboarding_state.json"),
            "entities": str(draft_root / "entities.yaml"),
            "detection_manifest": str(draft_root / "manifests" / "detection_manifest.yaml"),
            "detection_rows_csv": str(draft_root / "catalog" / "detection_rows.csv"),
            "asset_candidates_csv": str(draft_root / "catalog" / "asset_candidates.csv"),
            "source_fetch_log_csv": str(draft_root / "catalog" / "source_fetch_log.csv"),
        },
    }


def build_onboarding_draft(
    populated_draft_root: str | Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    repo_root = (repo_root or REPO_ROOT).resolve()
    draft_root = Path(populated_draft_root).expanduser().resolve()
    manifest_path = draft_root / "manifests" / "assets_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"draft does not contain assets manifest: {manifest_path}")

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    game = str(manifest_payload.get("game_id", "")).strip()
    if not game:
        raise ValueError("draft assets manifest must include game_id")
    candidates = manifest_payload.get("candidates", [])
    if not isinstance(candidates, list):
        raise ValueError("draft assets manifest candidates must be a list")

    ontology_payload = load_yaml_file(draft_root / "entities.yaml")
    if not isinstance(ontology_payload, dict):
        raise ValueError("draft entities.yaml must be a mapping")
    ontology = {
        "heroes": ontology_payload.get("heroes", []),
        "abilities": ontology_payload.get("abilities", []),
        "events": ontology_payload.get("events", []),
    }
    adapter = get_onboarding_adapter(game)
    detection_manifest = load_yaml_file(draft_root / "manifests" / "detection_manifest.yaml")
    if not isinstance(detection_manifest, dict):
        raise ValueError("draft detection manifest must be a mapping")
    detection_rows = detection_manifest.get("rows", [])
    if not isinstance(detection_rows, list):
        raise ValueError("draft detection manifest must define a rows list")
    existing_qa = _read_csv_rows(draft_root / "catalog" / "qa_queue.csv") if (draft_root / "catalog" / "qa_queue.csv").exists() else []

    bindings = _build_binding_candidates(game, detection_rows, candidates, adapter=adapter)
    qa_queue = _build_qa_queue(detection_rows, candidates, bindings, existing_qa=existing_qa)
    phase_status = "ready_to_publish" if not qa_queue else "bindings_pending"
    updated_manifest = dict(manifest_payload)
    updated_manifest["bindings"] = bindings
    updated_manifest["population_findings"] = updated_manifest.get("population_findings", [])
    updated_manifest["phase_status"] = phase_status
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
        manifest_payload=updated_manifest,
        state_payload=state_payload,
    )

    return {
        "ok": True,
        "status": phase_status,
        "game": game,
        "draft_root": str(draft_root),
        "catalog_root": str(draft_root / "catalog"),
        "masters_root": str(draft_root / "masters"),
        "review_root": str(draft_root / "review"),
        "source_count": int(manifest_payload.get("source_count", 0) or 0),
        "counts": {
            "heroes": len(ontology["heroes"]) if isinstance(ontology["heroes"], list) else 0,
            "abilities": len(ontology["abilities"]) if isinstance(ontology["abilities"], list) else 0,
            "events": len(ontology["events"]) if isinstance(ontology["events"], list) else 0,
            "detection_rows": int(detection_manifest.get("row_count", len(detection_rows)) or 0),
            "candidate_assets": len(candidates),
            "binding_candidates": len(bindings),
            "qa_queue": len(qa_queue),
        },
        "artifacts": {
            "game": str(draft_root / "game.yaml"),
            "entities": str(draft_root / "entities.yaml"),
            "hud": str(draft_root / "hud.yaml"),
            "weights": str(draft_root / "weights.yaml"),
            "game_detection_schema": str(draft_root / "manifests" / "game_detection_schema.yaml"),
            "assets_manifest": str(draft_root / "manifests" / "assets_manifest.json"),
            "onboarding_state": str(draft_root / "manifests" / "onboarding_state.json"),
            "detection_manifest": str(draft_root / "manifests" / "detection_manifest.yaml"),
            "bindings_csv": str(draft_root / "catalog" / "bindings.csv"),
            "detection_rows_csv": str(draft_root / "catalog" / "detection_rows.csv"),
            "qa_queue_csv": str(draft_root / "catalog" / "qa_queue.csv"),
        },
    }


def publish_onboarding_draft(
    draft_root: str | Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    repo_root = (repo_root or REPO_ROOT).resolve()
    draft_root = Path(draft_root).expanduser().resolve()
    manifest_path = draft_root / "manifests" / "assets_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"draft does not contain assets manifest: {manifest_path}")

    readiness = validate_onboarding_publish(draft_root, repo_root=repo_root)
    if not readiness.get("can_publish"):
        readiness_name = str(readiness.get("readiness", "unknown"))
        finding_messages = [
            str(row.get("message", row.get("reason", ""))).strip()
            for row in list(readiness.get("findings", []))[:3]
            if str(row.get("message", row.get("reason", ""))).strip()
        ]
        suffix = f": {'; '.join(finding_messages)}" if finding_messages else ""
        raise ValueError(f"draft is not publish-ready ({readiness_name}){suffix}")

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    game = str(manifest_payload["game_id"])
    bindings = _read_csv_rows(draft_root / "catalog" / "bindings.csv")
    candidates = {row["candidate_id"]: row for row in manifest_payload.get("candidates", [])}
    detection_manifest_payload = load_yaml_file(draft_root / "manifests" / "detection_manifest.yaml")
    if not isinstance(detection_manifest_payload, dict):
        raise ValueError("draft detection manifest must be a mapping")
    detection_rows = detection_manifest_payload.get("rows", [])
    if not isinstance(detection_rows, list):
        raise ValueError("draft detection manifest must define a rows list")
    schema_draft_path = draft_root / "manifests" / "game_detection_schema.yaml"
    detection_schema = _load_game_detection_schema(draft_root) if schema_draft_path.exists() else _load_runtime_detection_schema(repo_root=repo_root)

    accepted_bindings = [row for row in bindings if row.get("status") == "accepted"]
    accepted_bindings_by_detection = _best_accepted_bindings_by_detection(accepted_bindings)
    published_root = repo_root / "assets" / "games" / game
    stage_root = Path(tempfile.mkdtemp(prefix=f".publish-{game}.", dir=published_root.parent))
    manifests_root = stage_root / "manifests"
    templates_root = stage_root / "templates"
    masters_root = stage_root / "masters"
    for path in (manifests_root, templates_root, masters_root):
        path.mkdir(parents=True, exist_ok=True)

    try:
        game_payload = load_yaml_file(draft_root / "game.yaml")
        entities_payload = load_yaml_file(draft_root / "entities.yaml")
        hud_payload = load_yaml_file(draft_root / "hud.yaml")
        weights_payload = load_yaml_file(draft_root / "weights.yaml")
        cv_templates: list[dict[str, Any]] = []
        published_assets: list[dict[str, Any]] = []
        unresolved_rows = _unresolved_detection_rows(detection_rows, accepted_bindings_by_detection, candidates)
        if unresolved_rows:
            unresolved_ids = ", ".join(row["detection_id"] for row in unresolved_rows[:5])
            raise ValueError(f"detection manifest is incomplete; unresolved rows: {unresolved_ids}")

        published_detection_rows: list[dict[str, Any]] = []
        for detection_row in detection_rows:
            binding = accepted_bindings_by_detection.get(str(detection_row["detection_id"]))
            if binding is None:
                continue
            candidate = candidates.get(binding["candidate_id"])
            if candidate is None or not candidate.get("master_path"):
                continue
            source_path = Path(candidate["master_path"])
            if not source_path.exists():
                continue
            asset_family = str(detection_row["asset_family"])
            family_dir = _template_family_dir(asset_family)
            target_slug = slugify(str(detection_row["target_display_name"]))
            extension = source_path.suffix or ".png"
            master_relative = Path("masters") / family_dir / f"{target_slug}{extension}"
            template_relative = Path("templates") / family_dir / f"{target_slug}{extension}"
            published_master_path = stage_root / master_relative
            published_template_path = stage_root / template_relative
            published_master_path.parent.mkdir(parents=True, exist_ok=True)
            published_template_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, published_master_path)
            shutil.copyfile(source_path, published_template_path)

            asset_id = f"{game}.{detection_row['target_id']}.{asset_family}"
            template_row = {
                "asset_id": asset_id,
                "game_id": game,
                "asset_family": asset_family,
                "display_name": detection_row["target_display_name"],
                "template_path": str(template_relative).replace("\\", "/"),
                "mask_path": "",
                "roi_ref": detection_row["template_defaults"]["roi_ref"],
                "match_method": detection_row["template_defaults"]["match_method"],
                "threshold": detection_row["template_defaults"]["threshold"],
                "scale_set": detection_row["template_defaults"]["scale_set"],
                "temporal_window": detection_row["template_defaults"]["temporal_window"],
                "source_url": candidate["source_url"],
                "source_page_url": candidate["source_page_url"],
                "source_kind": candidate["source_kind"],
                "license_note": candidate.get("license_note", "unknown"),
                "binding_status": "accepted",
            }
            for field, value in dict(detection_row.get("template_semantics", {})).items():
                template_row[field] = value
            cv_templates.append(template_row)
            published_assets.append(
                {
                    "asset_id": asset_id,
                    "candidate_id": candidate["candidate_id"],
                    "target_id": detection_row["target_id"],
                    "display_name": detection_row["target_display_name"],
                    "asset_family": asset_family,
                    "master_path": str(master_relative).replace("\\", "/"),
                    "template_path": str(template_relative).replace("\\", "/"),
                    "source_url": candidate["source_url"],
                    "source_page_url": candidate["source_page_url"],
                    "source_kind": candidate["source_kind"],
                    "license_note": candidate.get("license_note", "unknown"),
                    "detection_id": detection_row["detection_id"],
                }
            )
            published_row = dict(detection_row)
            published_row["binding_status"] = "accepted"
            published_row["asset_status"] = "published"
            published_row["published_asset_id"] = asset_id
            published_row["candidate_id"] = candidate["candidate_id"]
            published_row["master_path"] = str(master_relative).replace("\\", "/")
            published_row["template_path"] = str(template_relative).replace("\\", "/")
            published_detection_rows.append(published_row)

        runtime_cv_rules = _build_runtime_cv_rules_manifest(published_detection_rows)
        fusion_rules = _build_fusion_rules_manifest(published_detection_rows, detection_schema)

        dump_yaml_file(stage_root / "game.yaml", game_payload)
        dump_yaml_file(stage_root / "entities.yaml", entities_payload)
        dump_yaml_file(stage_root / "hud.yaml", hud_payload)
        dump_yaml_file(stage_root / "weights.yaml", weights_payload)
        dump_yaml_file(manifests_root / "cv_templates.yaml", {"templates": cv_templates})
        dump_yaml_file(manifests_root / "runtime_cv_rules.yaml", runtime_cv_rules)
        dump_yaml_file(manifests_root / "fusion_rules.yaml", fusion_rules)
        dump_yaml_file(
            manifests_root / "detection_manifest.yaml",
            {
                "schema_version": detection_manifest_payload.get("schema_version", "game_detection_manifest_v1"),
                "baseline_schema_version": detection_manifest_payload.get("baseline_schema_version", "runtime_detection_schema_v1"),
                "game_id": game,
                "row_count": len(published_detection_rows),
                "required_row_count": sum(1 for row in published_detection_rows if row.get("requires_asset")),
                "ready_row_count": len(published_detection_rows),
                "rows_needing_assets": 0,
                "rows": published_detection_rows,
            },
        )
        from pipeline.contract_audit import audit_published_manifest_consistency

        consistency = audit_published_manifest_consistency(stage_root, repo_root=repo_root)
        if consistency["failures"]:
            drift_statuses = ", ".join(str(row.get("status", "unknown")) for row in consistency["failures"][:5])
            raise ValueError(f"published runtime contract consistency failed: {drift_statuses}")
        (manifests_root / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": game,
                    "published_at": datetime.now(UTC).isoformat(),
                    "published_assets": published_assets,
                    "published_detection_rows": [row["detection_id"] for row in published_detection_rows],
                    "candidates": manifest_payload.get("candidates", []),
                    "bindings": bindings,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        if published_root.exists():
            preserved: dict[Path, bytes] = {}
            for relative in (Path("characters.yaml"), Path("abilities.yaml"), Path("action_moments.yaml"), Path("roi_profiles.yaml"), Path("labels.yaml"), Path("score_weights.yaml")):
                existing = published_root / relative
                if existing.exists() and existing.is_file():
                    preserved[relative] = existing.read_bytes()
            drafts_dir = published_root / "drafts"
            if drafts_dir.exists() and drafts_dir.is_dir():
                shutil.copytree(drafts_dir, stage_root / "drafts")
            shutil.rmtree(published_root)
            for relative, data in preserved.items():
                path = stage_root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)

        stage_root.rename(published_root)
    except Exception:
        shutil.rmtree(stage_root, ignore_errors=True)
        raise

    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "published_root": str(published_root),
        "template_count": len(accepted_bindings),
        "contract_consistency": consistency,
        "artifacts": {
            "game": str(published_root / "game.yaml"),
            "entities": str(published_root / "entities.yaml"),
            "hud": str(published_root / "hud.yaml"),
            "weights": str(published_root / "weights.yaml"),
            "cv_templates": str(published_root / "manifests" / "cv_templates.yaml"),
            "runtime_cv_rules": str(published_root / "manifests" / "runtime_cv_rules.yaml"),
            "fusion_rules": str(published_root / "manifests" / "fusion_rules.yaml"),
            "detection_manifest": str(published_root / "manifests" / "detection_manifest.yaml"),
            "assets_manifest": str(published_root / "manifests" / "assets_manifest.json"),
        },
    }


def _normalize_source(source: OnboardingSource) -> OnboardingSource:
    return OnboardingSource(role=source.role, url=normalize_source_url(source.url), notes=source.notes)


def _fetch_source_record(source: OnboardingSource) -> dict[str, Any]:
    record = fetch_source_record(source.url, source.role)
    record["notes"] = source.notes
    return record


def _build_ontology(
    game: str,
    source_records: list[dict[str, Any]],
    *,
    repo_root: Path,
    adapter: GameOnboardingAdapter,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    heroes: dict[str, dict[str, Any]] = {}
    abilities: dict[str, dict[str, Any]] = {}
    events: dict[str, dict[str, Any]] = {}
    findings: list[dict[str, Any]] = []

    for source in source_records:
        entity_kind = adapter.role_to_kind.get(str(source["role"]))
        if entity_kind is None:
            continue
        if str(source.get("page_type", "")) == "category":
            for item in source.get("category_items", []):
                structured = extract_structured_fields(
                    item.name,
                    source_role=source["role"],
                    section_heading="Category",
                    record_type=entity_kind,
                )
                normalized_name = _normalize_schema_name(structured["display_name"], adapter=adapter)
                if not normalized_name:
                    continue
                _append_ontology_row(
                    entity_kind,
                    normalized_name,
                    structured,
                    source,
                    heroes=heroes,
                    abilities=abilities,
                    events=events,
                    findings=findings,
                )
            _enrich_category_ontology_from_detail_sections(
                source,
                entity_kind=entity_kind,
                heroes=heroes,
                abilities=abilities,
                events=events,
                findings=findings,
            )
            continue
        article_container_detected = bool(source.get("article_container_detected", False))
        for section in source.get("sections", []):
            section_heading = str(getattr(section, "heading", ""))
            has_item_text = bool(getattr(section, "items", []))
            for raw_name in section.items:
                structured = extract_structured_fields(
                    raw_name,
                    source_role=source["role"],
                    section_heading=section_heading,
                    record_type=entity_kind,
                )
                normalized_name = _normalize_schema_name(structured["display_name"], adapter=adapter)
                if not normalized_name:
                    continue
                _append_ontology_row(
                    entity_kind,
                    normalized_name,
                    structured,
                    source,
                    heroes=heroes,
                    abilities=abilities,
                    events=events,
                    findings=findings,
                )
            for image in getattr(section, "images", []):
                if bool(getattr(image, "infobox_like", False)):
                    continue
                raw_name = image_anchor_text(image)
                if not raw_name:
                    continue
                anchor_strength = str(getattr(image, "anchor_strength", "weak"))
                allow_image_row = anchor_strength in {"strong", "medium"}
                if bool(getattr(image, "gallery_like", False)):
                    allow_image_row = anchor_strength == "strong"
                    if not article_container_detected:
                        allow_image_row = bool(getattr(image, "paragraph_backed", False)) and anchor_strength == "medium"
                elif not article_container_detected:
                    allow_image_row = (
                        (bool(getattr(image, "captioned", False)) and anchor_strength == "strong")
                        or (bool(getattr(image, "paragraph_backed", False)) and anchor_strength == "medium")
                    )
                if allow_image_row and not has_item_text:
                    structured = extract_structured_fields(
                        raw_name,
                        source_role=source["role"],
                        section_heading=section_heading,
                        record_type=entity_kind,
                    )
                    normalized_name = _normalize_schema_name(structured["display_name"], adapter=adapter)
                    if normalized_name:
                        _append_ontology_row(
                            entity_kind,
                            normalized_name,
                            structured,
                            source,
                            heroes=heroes,
                            abilities=abilities,
                            events=events,
                            findings=findings,
                        )
                if anchor_strength == "weak":
                    findings.append(
                        {
                            "status": "weak_image_anchor",
                            "target_kind": entity_kind,
                            "display_name": raw_name,
                            "source_page_url": source["url"],
                            "source_role": source["role"],
                            "field": "image_anchor",
                            "anchor_source": str(getattr(image, "anchor_source", "")),
                        }
                    )
                if bool(getattr(image, "anchor_conflict", False)):
                    findings.append(
                        {
                            "status": "conflicting_image_anchor",
                            "target_kind": entity_kind,
                            "display_name": raw_name,
                            "source_page_url": source["url"],
                            "source_role": source["role"],
                            "field": "image_anchor",
                            "candidate_values": list(getattr(image, "anchor_candidates", [])),
                        }
                    )
                if bool(getattr(image, "paragraph_ambiguous", False)):
                    findings.append(
                        {
                            "status": (
                                "surrounding_paragraph_ambiguous_anchor"
                                if str(getattr(image, "anchor_ambiguity_type", "")) == "surrounding_paragraph"
                                else "cross_paragraph_ambiguous_anchor"
                                if str(getattr(image, "anchor_ambiguity_type", "")) == "cross_paragraph"
                                else "ambiguous_paragraph_anchor"
                            ),
                            "target_kind": entity_kind,
                            "display_name": raw_name or str(getattr(image, "filename_hint", "")),
                            "source_page_url": source["url"],
                            "source_role": source["role"],
                            "field": "image_anchor",
                            "candidate_values": list(getattr(image, "anchor_candidates", [])),
                        }
                    )
                if bool(getattr(image, "paragraph_referential", False)):
                    findings.append(
                        {
                            "status": "referential_paragraph_anchor",
                            "target_kind": entity_kind,
                            "display_name": raw_name or str(getattr(image, "filename_hint", "")),
                            "source_page_url": source["url"],
                            "source_role": source["role"],
                            "field": "image_anchor",
                        }
                    )
    ontology = {
        "heroes": sorted(heroes.values(), key=lambda row: row["hero_id"]),
        "abilities": sorted(abilities.values(), key=lambda row: row["ability_id"]),
        "events": sorted(events.values(), key=lambda row: row["event_id"]),
    }
    seed_data = _load_starter_seed_data(game, repo_root=repo_root, adapter=adapter)
    _enrich_ontology_with_starter_seed(game, ontology, seed_data, findings)
    return ontology, findings


def _append_ontology_row(
    entity_kind: str,
    normalized_name: str,
    structured: dict[str, Any],
    source: dict[str, Any],
    *,
    heroes: dict[str, dict[str, Any]],
    abilities: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    findings: list[dict[str, Any]],
) -> None:
    for finding in structured.get("findings", []):
        findings.append(
            {
                **finding,
                "target_kind": entity_kind,
                "display_name": normalized_name,
                "source_page_url": source["url"],
                "source_role": source["role"],
            }
        )

    rows_by_id, id_field, field_name, field_source_name, default_unknown = _ontology_target_config(
        entity_kind,
        heroes=heroes,
        abilities=abilities,
        events=events,
    )
    candidate_aliases = [str(item) for item in structured.get("aliases", [])]
    candidate_row = {
        "display_name": normalized_name,
        "canonical_id": _canonical_id(normalized_name),
        "aliases": candidate_aliases,
    }
    existing_key, overlapping_keys = _find_existing_identity_row(
        rows_by_id,
        candidate_row=candidate_row,
        id_field=id_field,
    )
    if len(overlapping_keys) > 1:
        findings.append(
            {
                "status": "ambiguous_identity_match",
                "target_kind": entity_kind,
                "target_id": "",
                "display_name": normalized_name,
                "source_page_url": source["url"],
                "source_role": source["role"],
                "candidate_ids": overlapping_keys,
            }
        )
        existing_key = None

    if existing_key is None:
        row_id = candidate_row["canonical_id"]
        rows_by_id[row_id] = {
            id_field: row_id,
            "display_name": normalized_name,
            "entity_type": entity_kind,
            field_name: structured.get(field_name, "") or default_unknown,
            field_source_name: structured.get(field_source_name, ""),
            "aliases": [],
            "aliases_source": "",
            "source_page_url": source["url"],
            "source_role": source["role"],
            "starter_seed_applied": False,
            "starter_seed_source": "",
            "canonical_display_name_source": "source",
            "canonical_id_source": "source",
            "canonical_identity_basis": "source_initial",
        }
        existing = rows_by_id[row_id]
    else:
        existing = rows_by_id[existing_key]
        identity = reconcile_identity(
            {
                "display_name": str(existing.get("display_name", "")),
                "canonical_id": str(existing.get(id_field, "")),
                "aliases": [str(item) for item in existing.get("aliases", []) if str(item).strip()],
            },
            candidate_row,
            preferred_source="source",
        )
        if identity["match_status"] in {"conflicting_identity_match", "identity_match_rejected"}:
            findings.append(
                {
                    "status": identity["match_status"],
                    "target_kind": entity_kind,
                    "target_id": str(existing.get(id_field, "")),
                    "display_name": str(existing.get("display_name", normalized_name)),
                    "source_page_url": source["url"],
                    "source_role": source["role"],
                    "candidate_ids": identity.get("candidate_ids", []),
                    "candidate_values": identity.get("candidate_names", []),
                }
            )
        elif identity["match_status"] == "canonical_identity_preference_applied":
            old_id = str(existing.get(id_field, ""))
            new_id = str(identity["chosen_canonical_id"])
            existing["display_name"] = str(identity["chosen_display_name"])
            existing[id_field] = new_id
            existing["canonical_display_name_source"] = "source"
            existing["canonical_id_source"] = "source"
            existing["canonical_identity_basis"] = str(identity.get("basis", ""))
            if new_id != old_id:
                rows_by_id.pop(old_id, None)
                rows_by_id[new_id] = existing
            findings.append(
                {
                    "status": "canonical_identity_preference_applied",
                    "target_kind": entity_kind,
                    "target_id": new_id,
                    "display_name": str(existing.get("display_name", "")),
                    "source_page_url": source["url"],
                    "source_role": source["role"],
                    "candidate_ids": [old_id, new_id],
                    "candidate_values": [normalized_name],
                    "identity_source": "source",
                    "basis": identity.get("basis", ""),
                }
            )

    merged_aliases, alias_rejections = merge_aliases(
        [str(item) for item in existing.get("aliases", []) if str(item).strip()],
        candidate_aliases,
        canonical_name=str(existing.get("display_name", normalized_name)),
    )
    existing["aliases"] = merged_aliases
    if merged_aliases and not existing.get("aliases_source"):
        existing["aliases_source"] = structured.get("aliases_source", "")
    for rejection in alias_rejections:
        findings.append(
            {
                "status": rejection["status"],
                "alias": rejection["alias"],
                "target_kind": entity_kind,
                "target_id": str(existing.get(id_field, "")),
                "display_name": str(existing.get("display_name", normalized_name)),
                "source_page_url": source["url"],
                "source_role": source["role"],
            }
        )
    if not existing.get(field_name) and structured.get(field_name):
        existing[field_name] = structured.get(field_name, "")
        existing[field_source_name] = structured.get(field_source_name, "")
    if not existing.get("source_page_url"):
        existing["source_page_url"] = source["url"]
    if not existing.get("source_role"):
        existing["source_role"] = source["role"]


def _ontology_target_config(
    entity_kind: str,
    *,
    heroes: dict[str, dict[str, Any]],
    abilities: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], str, str, str, str]:
    if entity_kind == "character_or_operator":
        return heroes, "hero_id", "role", "role_source", ""
    if entity_kind == "ability_or_equipment":
        return abilities, "ability_id", "class", "class_source", "unknown"
    return events, "event_id", "category", "category_source", "unknown"


def _find_existing_identity_row(
    rows_by_id: dict[str, dict[str, Any]],
    *,
    candidate_row: dict[str, Any],
    id_field: str,
) -> tuple[str | None, list[str]]:
    candidate_keys = identity_keys(
        str(candidate_row.get("display_name", "")),
        canonical_id=str(candidate_row.get("canonical_id", "")),
        aliases=[str(item) for item in candidate_row.get("aliases", []) if str(item).strip()],
    )
    overlapping_keys: list[str] = []
    for row_id, row in rows_by_id.items():
        row_keys = identity_keys(
            str(row.get("display_name", "")),
            canonical_id=str(row.get(id_field, row_id)),
            aliases=[str(item) for item in row.get("aliases", []) if str(item).strip()],
        )
        if candidate_keys & row_keys:
            overlapping_keys.append(row_id)
    if len(overlapping_keys) == 1:
        return overlapping_keys[0], overlapping_keys
    return None, overlapping_keys


def _enrich_category_ontology_from_detail_sections(
    source: dict[str, Any],
    *,
    entity_kind: str,
    heroes: dict[str, dict[str, Any]],
    abilities: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    findings: list[dict[str, Any]],
) -> None:
    if entity_kind == "character_or_operator":
        target_rows = heroes
    elif entity_kind == "ability_or_equipment":
        target_rows = abilities
    else:
        target_rows = events
    candidate_names = sorted(row["display_name"] for row in target_rows.values())
    if not candidate_names:
        return
    detail_contributions: dict[str, list[dict[str, Any]]] = {}
    for section in source.get("sections", []):
        for paragraph_text in getattr(section, "paragraphs", []):
            matched_names = find_explicit_listing_matches(paragraph_text, candidate_names)
            if len(matched_names) > 1:
                findings.append(
                    {
                        "status": "ambiguous_listing_detail_enrichment",
                        "target_kind": entity_kind,
                        "display_name": "",
                        "source_page_url": source["url"],
                        "source_role": source["role"],
                        "field": "detail_paragraph",
                        "candidate_values": matched_names,
                    }
                )
                continue
            matched_name = find_explicit_listing_match(paragraph_text, candidate_names)
            if not matched_name:
                continue
            structured = extract_structured_fields(
                paragraph_text,
                source_role=source["role"],
                section_heading=str(getattr(section, "heading", "")),
                record_type=entity_kind,
            )
            detail_contributions.setdefault(matched_name, []).append(
                {
                    "aliases": [str(item) for item in structured.get("aliases", []) if clean_text(str(item))],
                    "aliases_source": structured.get("aliases_source", ""),
                    "role": structured.get("role", ""),
                    "role_source": structured.get("role_source", ""),
                    "class": structured.get("class", ""),
                    "class_source": structured.get("class_source", ""),
                    "category": structured.get("category", ""),
                    "category_source": structured.get("category_source", ""),
                }
            )
    for matched_name, contributions in detail_contributions.items():
        target_row = target_rows.get(_canonical_id(matched_name))
        if target_row is None:
            continue
        conflict_fields = _append_conflicting_listing_detail_findings(
            findings,
            contributions,
            target_kind=entity_kind,
            target_row=target_row,
            source=source,
        )
        conflict_fields = _append_existing_row_listing_detail_conflicts(
            findings,
            contributions,
            conflict_fields=conflict_fields,
            target_kind=entity_kind,
            target_row=target_row,
            source=source,
        )
        aliases = _merge_nonconflicting_listing_detail_aliases(
            contributions,
            conflict_fields,
            target_row=target_row,
            findings=findings,
            target_kind=entity_kind,
            source=source,
        )
        if aliases:
            existing_aliases = [str(item) for item in target_row.get("aliases", []) if clean_text(str(item))]
            merged_aliases: list[str] = []
            for alias in existing_aliases + aliases:
                cleaned = clean_text(alias)
                if cleaned and cleaned not in merged_aliases:
                    merged_aliases.append(cleaned)
            target_row["aliases"] = merged_aliases
            if not target_row.get("aliases_source"):
                target_row["aliases_source"] = "source"
        if entity_kind == "character_or_operator":
            role_value = _resolve_consistent_listing_detail_field(contributions, "role", target_row=target_row)
            if role_value and not target_row.get("role"):
                target_row["role"] = role_value
                target_row["role_source"] = "source"
        elif entity_kind == "ability_or_equipment":
            class_value = _resolve_consistent_listing_detail_field(contributions, "class", target_row=target_row)
            if class_value and target_row.get("class") in {"", "unknown"}:
                target_row["class"] = class_value
                target_row["class_source"] = "source"
        elif entity_kind == "event_badge_or_medal":
            category_value = _resolve_consistent_listing_detail_field(contributions, "category", target_row=target_row)
            if category_value and target_row.get("category") in {"", "unknown"}:
                target_row["category"] = category_value
                target_row["category_source"] = "source"


def _append_conflicting_listing_detail_findings(
    findings: list[dict[str, Any]],
    contributions: list[dict[str, Any]],
    *,
    target_kind: str,
    target_row: dict[str, Any],
    source: dict[str, Any],
) -> set[str]:
    conflict_fields: set[str] = set()
    for field_name in ("role", "class", "category"):
        values = sorted({clean_text(str(row.get(field_name, ""))) for row in contributions if clean_text(str(row.get(field_name, "")))})
        if len(values) <= 1:
            continue
        conflict_fields.add(field_name)
        findings.append(
            {
                "status": "conflicting_listing_detail_enrichment",
                "target_kind": target_kind,
                "target_id": target_row.get("id", ""),
                "display_name": target_row.get("display_name", ""),
                "source_page_url": source["url"],
                "source_role": source["role"],
                "field": field_name,
                "candidate_values": values,
            }
        )
    return conflict_fields

def _append_existing_row_listing_detail_conflicts(
    findings: list[dict[str, Any]],
    contributions: list[dict[str, Any]],
    *,
    conflict_fields: set[str],
    target_kind: str,
    target_row: dict[str, Any],
    source: dict[str, Any],
) -> set[str]:
    merged_conflict_fields = set(conflict_fields)
    for field_name in ("role", "class", "category"):
        detail_value = _resolve_consistent_listing_detail_field(contributions, field_name)
        existing_value = clean_text(str(target_row.get(field_name, "")))
        if not detail_value or not existing_value or detail_value == existing_value:
            continue
        merged_conflict_fields.add(field_name)
        findings.append(
            {
                "status": "existing_listing_detail_enrichment_conflict",
                "target_kind": target_kind,
                "target_id": target_row.get("id", ""),
                "display_name": target_row.get("display_name", ""),
                "source_page_url": source["url"],
                "source_role": source["role"],
                "field": field_name,
                "candidate_values": [detail_value],
                "existing_value": existing_value,
            }
        )
    return merged_conflict_fields


def _merge_nonconflicting_listing_detail_aliases(
    contributions: list[dict[str, Any]],
    conflict_fields: set[str],
    *,
    target_row: dict[str, Any],
    findings: list[dict[str, Any]],
    target_kind: str,
    source: dict[str, Any],
) -> list[str]:
    merged_aliases: list[str] = []
    existing_aliases = [clean_text(str(item)) for item in target_row.get("aliases", []) if clean_text(str(item))]
    for row in contributions:
        row_conflicted = _listing_detail_contribution_conflicts_row(row, conflict_fields, target_row=target_row)
        conflict_field_names = _listing_detail_contribution_conflict_fields(row, conflict_fields, target_row=target_row)
        if row_conflicted:
            for alias in row.get("aliases", []):
                cleaned = clean_text(str(alias))
                if not cleaned:
                    continue
                findings.append(
                    {
                        "status": "alias_suppressed_by_detail_conflict",
                        "target_kind": target_kind,
                        "target_id": target_row.get("id", ""),
                        "display_name": target_row.get("display_name", ""),
                        "source_page_url": source["url"],
                        "source_role": source["role"],
                        "alias": cleaned,
                        "candidate_values": sorted(conflict_field_names),
                    }
                )
            continue
        for alias in row.get("aliases", []):
            cleaned = clean_text(str(alias))
            if not cleaned:
                continue
            if aliases_equivalent(cleaned, str(target_row.get("display_name", ""))):
                findings.append(
                    {
                        "status": "alias_equivalent_to_canonical_name",
                        "target_kind": target_kind,
                        "target_id": target_row.get("id", ""),
                        "display_name": target_row.get("display_name", ""),
                        "source_page_url": source["url"],
                        "source_role": source["role"],
                        "alias": cleaned,
                    }
                )
                continue
            if any(aliases_equivalent(cleaned, existing_alias) for existing_alias in existing_aliases + merged_aliases):
                findings.append(
                    {
                        "status": "alias_equivalent_to_existing_alias",
                        "target_kind": target_kind,
                        "target_id": target_row.get("id", ""),
                        "display_name": target_row.get("display_name", ""),
                        "source_page_url": source["url"],
                        "source_role": source["role"],
                        "alias": cleaned,
                    }
                )
                continue
            merged_aliases.append(cleaned)
    return merged_aliases


def _listing_detail_contribution_conflicts_row(
    row: dict[str, Any],
    conflict_fields: set[str],
    *,
    target_row: dict[str, Any] | None = None,
) -> bool:
    for field_name in conflict_fields:
        field_value = clean_text(str(row.get(field_name, "")))
        if not field_value:
            continue
        if target_row is None:
            return True
        existing_value = clean_text(str(target_row.get(field_name, "")))
        if existing_value and field_value != existing_value:
            return True
        if not existing_value:
            return True
    return False


def _listing_detail_contribution_conflict_fields(
    row: dict[str, Any],
    conflict_fields: set[str],
    *,
    target_row: dict[str, Any],
) -> set[str]:
    row_conflicts: set[str] = set()
    for field_name in conflict_fields:
        field_value = clean_text(str(row.get(field_name, "")))
        if not field_value:
            continue
        existing_value = clean_text(str(target_row.get(field_name, "")))
        if existing_value and field_value != existing_value:
            row_conflicts.add(field_name)
        elif not existing_value:
            row_conflicts.add(field_name)
    return row_conflicts


def _resolve_consistent_listing_detail_field(
    contributions: list[dict[str, Any]],
    field_name: str,
    *,
    target_row: dict[str, Any] | None = None,
) -> str:
    values = [clean_text(str(row.get(field_name, ""))) for row in contributions if clean_text(str(row.get(field_name, "")))]
    unique_values: list[str] = []
    for value in values:
        if value not in unique_values:
            unique_values.append(value)
    if len(unique_values) != 1:
        return ""
    resolved_value = unique_values[0]
    if target_row is not None:
        existing_value = clean_text(str(target_row.get(field_name, "")))
        if existing_value and existing_value != resolved_value:
            return ""
    return resolved_value


def _collect_candidate_assets(
    source_records: list[dict[str, Any]],
    masters_root: Path,
    *,
    adapter: GameOnboardingAdapter,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for source in source_records:
        if source["page_type"] == "direct_image" and source.get("direct_image_url"):
            row = _build_candidate_row(
                source=source,
                image_url=source["direct_image_url"],
                display_name=Path(urlparse(source["direct_image_url"]).path).stem,
                source_kind="direct_image",
                adapter=adapter,
            )
            if row.get("hard_reject"):
                continue
            candidates.append(_download_candidate(row, masters_root))
            continue

        if source.get("page_type") != "category":
            for section in source.get("sections", []):
                for image in section.images:
                    if image.src in seen_urls:
                        continue
                    seen_urls.add(image.src)
                    image_section_heading = section.heading
                    if bool(getattr(image, "infobox_like", False)):
                        image_section_heading = str(source.get("role", "")) or section.heading
                    elif bool(getattr(image, "gallery_like", False)):
                        image_section_heading = image_anchor_text(image) or section.heading
                    display_name = _normalize_candidate_name(
                        image_anchor_text(image),
                        fallback=image_section_heading,
                        adapter=adapter,
                    )
                    if not display_name:
                        continue
                    row = _build_candidate_row(
                        source=source,
                        image_url=image.src,
                        display_name=display_name,
                        source_kind=(
                            "infobox_image"
                            if bool(getattr(image, "infobox_like", False))
                            else "gallery_image"
                            if bool(getattr(image, "gallery_like", False))
                            else "page_image"
                        ),
                        section_heading=image_section_heading,
                        raw_label=image_anchor_text(image) or image_section_heading,
                        anchor_source=str(getattr(image, "anchor_source", "")),
                        anchor_ambiguous=bool(getattr(image, "paragraph_ambiguous", False)),
                        anchor_ambiguity_type=str(getattr(image, "anchor_ambiguity_type", "")),
                        paragraph_referential=bool(getattr(image, "paragraph_referential", False)),
                        adapter=adapter,
                    )
                    if row.get("hard_reject"):
                        continue
                    candidates.append(_download_candidate(row, masters_root))
        for item in source.get("category_items", []):
            if not item.image_src or item.image_src in seen_urls:
                continue
            seen_urls.add(item.image_src)
            display_name = _normalize_candidate_name(item.name, fallback=item.image_alt, adapter=adapter)
            if not display_name:
                continue
            row = _build_candidate_row(
                source=source,
                image_url=item.image_src,
                display_name=display_name,
                source_kind="category_member_image",
                section_heading="Category",
                raw_label=item.image_alt or item.name,
                adapter=adapter,
            )
            if row.get("hard_reject"):
                continue
            candidates.append(_download_candidate(row, masters_root))
    return candidates


def _load_runtime_detection_schema(*, repo_root: Path) -> dict[str, Any]:
    preferred_path = repo_root / "starter_assets" / "runtime_detection_schema.yaml"
    schema_path = preferred_path if preferred_path.exists() else _RUNTIME_DETECTION_SCHEMA_PATH
    payload = load_yaml_file(schema_path)
    _validate_detection_schema_payload(payload, repo_root=repo_root, schema_label="runtime detection schema")
    payload["_schema_path"] = str(schema_path)
    payload["_ontology_version"] = load_runtime_signal_event_ontology(repo_root=repo_root).schema_version
    return payload


def _validate_detection_schema_payload(payload: Any, *, repo_root: Path, schema_label: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError(f"{schema_label} must be a mapping")
    families = payload.get("families", {})
    fusion_rules = payload.get("fusion_rules", {})
    if not isinstance(families, dict) or not families:
        raise ValueError(f"{schema_label} must define a non-empty families mapping")
    if not isinstance(fusion_rules, dict):
        raise ValueError(f"{schema_label} fusion_rules must be a mapping")
    ontology = load_runtime_signal_event_ontology(repo_root=repo_root)
    for asset_family, family_spec in families.items():
        if not isinstance(family_spec, dict):
            raise ValueError(f"{schema_label} family '{asset_family}' must be a mapping")
        if family_spec.get("enabled", True) is False:
            continue
        runtime_rule = family_spec.get("runtime_rule", {})
        if not isinstance(runtime_rule, dict):
            raise ValueError(f"{schema_label} family '{asset_family}' runtime_rule must be a mapping")
        findings = validate_runtime_rule_terms(
            ontology,
            signal_type=str(runtime_rule.get("signal_type", "")).strip(),
            event_type=str(runtime_rule.get("event_type", "")).strip(),
            target_field=str(runtime_rule.get("target_field", "")).strip() or None,
            target_value_field=str(runtime_rule.get("target_value_field", "")).strip() or None,
        )
        if findings:
            raise ValueError(f"{schema_label} family '{asset_family}' uses invalid ontology terms: {findings}")
        fusion_rule_ids = [str(item).strip() for item in family_spec.get("fusion_rule_ids", []) if str(item).strip()]
        missing_fusion_rule_ids = [rule_id for rule_id in fusion_rule_ids if rule_id not in fusion_rules]
        if missing_fusion_rule_ids:
            raise ValueError(
                f"{schema_label} family '{asset_family}' references missing fusion_rule_ids: {missing_fusion_rule_ids}"
            )
    for rule_id, rule_spec in fusion_rules.items():
        if not isinstance(rule_spec, dict):
            raise ValueError(f"{schema_label} fusion rule '{rule_id}' must be a mapping")
        signal_types = [str(item).strip() for item in rule_spec.get("signal_types", []) if str(item).strip()]
        required_signal_types = [str(item).strip() for item in rule_spec.get("required_signal_types", []) if str(item).strip()]
        unknown_signal_types = [signal_type for signal_type in signal_types if signal_type not in ontology.signal_types]
        unknown_required_signal_types = [signal_type for signal_type in required_signal_types if signal_type not in ontology.signal_types]
        event_type = str(rule_spec.get("event_type", "")).strip()
        invalid_group_by_fields = validate_group_by_fields(
            ontology,
            [str(item).strip() for item in rule_spec.get("group_by", []) if str(item).strip()],
        )
        if unknown_signal_types or unknown_required_signal_types or event_type not in ontology.event_types or invalid_group_by_fields:
            raise ValueError(
                f"{schema_label} fusion rule '{rule_id}' uses invalid ontology terms: "
                f"signal_types={unknown_signal_types}, required_signal_types={unknown_required_signal_types}, "
                f"event_type={event_type}, invalid_group_by_fields={invalid_group_by_fields}"
            )


def _derive_detection_manifest(
    game: str,
    ontology: dict[str, list[dict[str, Any]]],
    schema: dict[str, Any],
    *,
    adapter: GameOnboardingAdapter,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    families = _active_families(schema)
    if "hero_portrait" in families:
        for row in ontology.get("heroes", []):
            rows.append(_build_detection_row(game, row, families["hero_portrait"], asset_family="hero_portrait"))
    for row in ontology.get("abilities", []):
        asset_family = _asset_family_for_ability_row(row, adapter=adapter)
        if asset_family in families:
            rows.append(_build_detection_row(game, row, families[asset_family], asset_family=asset_family))
    if "medal_icon" in families:
        for row in ontology.get("events", []):
            rows.append(_build_detection_row(game, row, families["medal_icon"], asset_family="medal_icon"))

    rows.sort(key=lambda item: (str(item["asset_family"]), str(item["target_id"])))
    required_row_count = sum(1 for row in rows if row["requires_asset"])
    ready_row_count = sum(1 for row in rows if row["status"] == "ready_for_binding")
    return {
        "schema_version": "game_detection_manifest_v1",
        "baseline_schema_version": str(
            schema.get("baseline_schema_version", schema.get("schema_version", "runtime_detection_schema_v1"))
        ),
        "game_id": game,
        "row_count": len(rows),
        "required_row_count": required_row_count,
        "ready_row_count": ready_row_count,
        "rows_needing_assets": sum(1 for row in rows if row["requires_asset"] and row["status"] == "ready_for_binding"),
        "rows": rows,
    }


def _active_families(schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_families = schema.get("families", {})
    if not isinstance(raw_families, dict):
        return {}
    return {
        asset_family: family_spec
        for asset_family, family_spec in raw_families.items()
        if isinstance(family_spec, dict) and family_spec.get("enabled", True) is not False
    }


def _build_detection_row(game: str, ontology_row: dict[str, Any], family_spec: dict[str, Any], *, asset_family: str) -> dict[str, Any]:
    target_id_field = str(family_spec["target_id_field"])
    target_id = str(ontology_row[target_id_field])
    target_display_name = str(ontology_row.get("display_name", target_id))
    semantic_field = str(family_spec["template_semantic_field"])
    semantic_value = target_id
    runtime_rule = dict(family_spec.get("runtime_rule", {}))
    template_defaults = {
        "roi_ref": family_spec["roi_ref"],
        "match_method": family_spec["match_method"],
        "threshold": float(family_spec["threshold"]),
        "scale_set": [float(item) for item in family_spec.get("scale_set", [])],
        "temporal_window": int(family_spec["temporal_window"]),
    }
    status = "ready_for_binding" if semantic_value else "missing_semantic_values"
    detection_id = f"{game}.{target_id}.{asset_family}"
    return {
        "detection_id": detection_id,
        "game_id": game,
        "target_kind": str(family_spec.get("target_kind", "unknown")),
        "target_id": target_id,
        "target_display_name": target_display_name,
        "target_aliases": [str(item).strip() for item in ontology_row.get("aliases", []) if str(item).strip()],
        "ontology_collection": str(family_spec.get("ontology_collection", "unknown")),
        "asset_family": asset_family,
        "requires_asset": bool(family_spec.get("requires_asset", True)),
        "required_semantic_fields": [semantic_field],
        "template_semantics": {semantic_field: semantic_value},
        "template_defaults": template_defaults,
        "runtime_rule": runtime_rule,
        "fusion_rule_ids": list(family_spec.get("fusion_rule_ids", [])),
        "status": status,
        "binding_status": "unbound",
        "source_page_url": ontology_row.get("source_page_url", ""),
    }


def _asset_family_for_ability_row(row: dict[str, Any], *, adapter: GameOnboardingAdapter) -> str:
    return (
        "equipment_icon"
        if _source_role_asset_family(str(row.get("source_role", "")), str(row.get("display_name", "")), adapter=adapter) == "equipment_icon"
        else "ability_icon"
    )


def _build_candidate_row(
    *,
    source: dict[str, Any],
    image_url: str,
    display_name: str,
    source_kind: str,
    adapter: GameOnboardingAdapter,
    section_heading: str = "",
    raw_label: str = "",
    anchor_source: str = "",
    anchor_ambiguous: bool = False,
    anchor_ambiguity_type: str = "",
    paragraph_referential: bool = False,
) -> dict[str, Any]:
    normalized_display_name = _normalize_schema_name(display_name, adapter=adapter)
    candidate_id = f"candidate_{hashlib.sha1(f'{source['url']}|{image_url}|{normalized_display_name}'.encode('utf-8')).hexdigest()[:12]}"
    asset_family = _source_role_asset_family(source["role"], normalized_display_name, adapter=adapter)
    quality = analyze_asset_candidate(
        display_name=normalized_display_name,
        asset_family=asset_family,
        source_role=source["role"],
        source_kind=source_kind,
        source_url=image_url,
        section_heading=section_heading,
        raw_label=raw_label or display_name,
    )
    return {
        "candidate_id": candidate_id,
        "display_name": normalized_display_name,
        "normalized_name": _canonical_id(normalized_display_name),
        "binding_key": quality["binding_key"],
        "asset_family": asset_family,
        "source_url": image_url,
        "source_page_url": source["url"],
        "source_role": source["role"],
        "source_title": source["title"],
        "source_kind": source_kind,
        "section_heading": section_heading,
        "raw_label": raw_label or display_name,
        "anchor_source": anchor_source,
        "anchor_ambiguous": bool(anchor_ambiguous),
        "anchor_ambiguity_type": anchor_ambiguity_type,
        "paragraph_referential": bool(paragraph_referential),
        "notes": source.get("notes", ""),
        "candidate_quality": quality["candidate_quality"],
        "quality_score": quality["quality_score"],
        "quality_reasons": quality["quality_reasons"],
        "portrait_like": quality["portrait_like"],
        "icon_like": quality["icon_like"],
        "badge_like": quality["badge_like"],
        "artwork_like": quality["artwork_like"],
        "generic_page_art": quality["generic_page_art"],
        "logo_like": quality["logo_like"],
        "map_like": quality["map_like"],
        "prose_like": quality["prose_like"],
        "hard_reject": quality["hard_reject"],
        "reject_reasons": quality["reject_reasons"],
        "fetch_status": "pending",
        "master_path": "",
        "license_note": "unknown",
    }


def _download_candidate(candidate: dict[str, Any], masters_root: Path) -> dict[str, Any]:
    source_url = candidate["source_url"]
    extension = Path(urlparse(source_url).path).suffix.lower() or ".png"
    family_dir = _template_family_dir(candidate["asset_family"])
    filename = f"{candidate['normalized_name']}-{candidate['candidate_id'][-6:]}{extension}"
    target_path = masters_root / family_dir / filename
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urlopen(build_fetch_target(source_url), timeout=_DEFAULT_TIMEOUT_SECONDS) as response:
            target_path.write_bytes(response.read())
    except (HTTPError, URLError, OSError):
        candidate["fetch_status"] = "failed"
        candidate["master_path"] = ""
        return candidate
    candidate["fetch_status"] = "downloaded"
    candidate["master_path"] = str(target_path)
    return candidate


def _build_binding_candidates(
    game: str,
    detection_rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    *,
    adapter: GameOnboardingAdapter,
) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = []
    for detection_row in detection_rows:
        target_kind = str(detection_row["target_kind"])
        target_id = str(detection_row["target_id"])
        target_display_name = str(detection_row["target_display_name"])
        asset_family = str(detection_row["asset_family"])
        target_aliases = [str(item).strip() for item in detection_row.get("target_aliases", []) if str(item).strip()]
        for candidate in candidates:
            if candidate["fetch_status"] != "downloaded":
                continue
            if candidate["asset_family"] != asset_family and candidate["source_role"] != "assets_reference":
                continue
            binding = score_binding_candidate(
                target_display_name=target_display_name,
                target_aliases=target_aliases,
                asset_family=asset_family,
                candidate=candidate,
            )
            confidence = float(binding["score"])
            if confidence <= 0:
                continue
            bindings.append(
                {
                    "binding_id": f"binding_{hashlib.sha1(f'{target_id}|{candidate['candidate_id']}'.encode('utf-8')).hexdigest()[:12]}",
                    "game_id": game,
                    "detection_id": detection_row["detection_id"],
                    "target_kind": target_kind,
                    "target_id": target_id,
                    "target_display_name": target_display_name,
                    "candidate_id": candidate["candidate_id"],
                    "candidate_display_name": candidate["display_name"],
                    "source_url": candidate["source_url"],
                    "asset_family": asset_family,
                    "confidence": round(confidence, 2),
                    "binding_score": round(confidence, 2),
                    "reason": "; ".join(binding["reasons"]),
                    "binding_reasons": binding["reasons"],
                    "name_match_quality": binding["name_match_quality"],
                    "candidate_quality": candidate.get("candidate_quality", ""),
                    "quality_score": candidate.get("quality_score", 0.0),
                    "source_kind": candidate.get("source_kind", ""),
                    "image_kind_mismatch": bool(binding["flags"].get("image_kind_mismatch")),
                    "lower_trust_source_kind": bool(binding["flags"].get("lower_trust_source_kind")),
                    "weak_name_match": bool(binding["flags"].get("weak_name_match")),
                    "status": "pending_review",
                }
            )
    bindings.sort(key=lambda row: (-float(row["confidence"]), row["target_id"], row["candidate_id"]))
    return bindings


def _build_qa_queue(
    detection_rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    bindings: list[dict[str, Any]],
    *,
    existing_qa: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    qa_rows: list[dict[str, Any]] = list(existing_qa or [])
    binding_targets = {row["detection_id"] for row in bindings}
    bound_candidates = {row["candidate_id"] for row in bindings}

    for detection_row in detection_rows:
        if detection_row["status"] == "missing_semantic_values":
            qa_rows.append(
                {
                    "item_type": "missing_semantic_values",
                    "detection_id": detection_row["detection_id"],
                    "target_id": detection_row["target_id"],
                    "display_name": detection_row["target_display_name"],
                    "status": "needs_schema_completion",
                    "reason": "required semantic fields were not derived for this detection row",
                }
            )
            continue
        if detection_row["requires_asset"] and detection_row["detection_id"] not in binding_targets:
            qa_rows.append(
                {
                    "item_type": "missing_binding",
                    "detection_id": detection_row["detection_id"],
                    "target_id": detection_row["target_id"],
                    "display_name": detection_row["target_display_name"],
                    "status": "needs_better_reference",
                    "reason": "no candidate source image matched this detection row",
                }
            )
    for candidate in candidates:
        if candidate["candidate_id"] in bound_candidates:
            continue
        qa_rows.append(
            {
                "item_type": "unbound_candidate",
                "target_id": "",
                "display_name": candidate["display_name"],
                "status": "pending_review",
                "reason": "candidate image was collected but not bound to any schema row",
            }
        )
        if str(candidate.get("anchor_source", "")).strip() == "filename":
            qa_rows.append(
                {
                    "item_type": "filename_only_anchor",
                    "target_id": "",
                    "display_name": candidate["display_name"],
                    "status": "needs_candidate_review",
                    "reason": "candidate naming was inferred from the image filename only",
                }
            )
        if bool(candidate.get("anchor_ambiguous", False)):
            qa_rows.append(
                {
                    "item_type": (
                        "surrounding_paragraph_ambiguous_anchor"
                        if str(candidate.get("anchor_ambiguity_type", "")) == "surrounding_paragraph"
                        else "cross_paragraph_ambiguous_anchor"
                        if str(candidate.get("anchor_ambiguity_type", "")) == "cross_paragraph"
                        else "ambiguous_paragraph_anchor"
                    ),
                    "target_id": "",
                    "display_name": candidate["display_name"],
                    "status": "needs_candidate_review",
                    "reason": (
                        "preceding and following adjacent paragraphs mentioned conflicting plausible entities for one image"
                        if str(candidate.get("anchor_ambiguity_type", "")) == "surrounding_paragraph"
                        else "adjacent paragraphs mentioned conflicting plausible entities for one image"
                        if str(candidate.get("anchor_ambiguity_type", "")) == "cross_paragraph"
                        else "adjacent paragraph mentioned multiple plausible entities for one image"
                    ),
                }
            )
        if bool(candidate.get("paragraph_referential", False)):
            qa_rows.append(
                {
                    "item_type": "referential_paragraph_anchor",
                    "target_id": "",
                    "display_name": candidate["display_name"],
                    "status": "needs_candidate_review",
                    "reason": "adjacent paragraph referenced an entity indirectly without an explicit name anchor",
                }
            )
        if not str(candidate.get("raw_label", "")).strip():
            qa_rows.append(
                {
                    "item_type": "image_only_candidate",
                    "target_id": "",
                    "display_name": candidate["display_name"],
                    "status": "needs_candidate_review",
                    "reason": "candidate came from an image-heavy source without a strong nearby text label",
                }
            )
        if candidate.get("candidate_quality") == "low":
            qa_rows.append(
                {
                    "item_type": "low_quality_candidate",
                    "target_id": "",
                    "display_name": candidate["display_name"],
                    "status": "needs_candidate_review",
                    "reason": f"candidate is still reviewable but scored low quality: {', '.join(candidate.get('quality_reasons', []))}",
                }
            )
        if candidate.get("artwork_like") or candidate.get("generic_page_art"):
            qa_rows.append(
                {
                    "item_type": "image_kind_mismatch",
                    "target_id": "",
                    "display_name": candidate["display_name"],
                    "status": "needs_candidate_review",
                    "reason": f"candidate looks like artwork or generic page art for asset family {candidate.get('asset_family', '')}",
                }
            )

    candidate_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for candidate in candidates:
        group_key = (
            str(candidate.get("source_page_url", "")),
            str(candidate.get("binding_key", "")),
            str(candidate.get("asset_family", "")),
        )
        candidate_groups.setdefault(group_key, []).append(candidate)
    for grouped in candidate_groups.values():
        if len(grouped) > 1:
            qa_rows.append(
                {
                    "item_type": "duplicate_candidate_cluster",
                    "target_id": "",
                    "display_name": grouped[0]["display_name"],
                    "status": "needs_candidate_review",
                    "reason": f"multiple near-equivalent candidates were collected from one source page: {len(grouped)}",
                }
            )

    candidates_by_binding_target: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for candidate in candidates:
        group_key = (str(candidate.get("binding_key", "")), str(candidate.get("asset_family", "")))
        candidates_by_binding_target.setdefault(group_key, []).append(candidate)
    for grouped in candidates_by_binding_target.values():
        source_kinds = {str(row.get("source_kind", "")) for row in grouped}
        if source_kinds == {"infobox_image"}:
            qa_rows.append(
                {
                    "item_type": "infobox_only_candidate",
                    "target_id": "",
                    "display_name": grouped[0]["display_name"],
                    "status": "needs_candidate_review",
                    "reason": "only infobox-derived image candidates were collected for this target",
                }
            )
        elif "infobox_image" in source_kinds and any(kind in {"page_image", "direct_image", "category_member_image"} for kind in source_kinds):
            qa_rows.append(
                {
                    "item_type": "infobox_competes_with_body_candidate",
                    "target_id": "",
                    "display_name": grouped[0]["display_name"],
                    "status": "needs_candidate_review",
                    "reason": "both infobox and stronger non-infobox candidates exist for this target",
                }
            )
        if source_kinds == {"gallery_image"}:
            qa_rows.append(
                {
                    "item_type": "gallery_only_candidate",
                    "target_id": "",
                    "display_name": grouped[0]["display_name"],
                    "status": "needs_candidate_review",
                    "reason": "only gallery-derived image candidates were collected for this target",
                }
            )
        elif "gallery_image" in source_kinds and any(kind in {"page_image", "direct_image", "category_member_image"} for kind in source_kinds):
            qa_rows.append(
                {
                    "item_type": "gallery_competes_with_body_candidate",
                    "target_id": "",
                    "display_name": grouped[0]["display_name"],
                    "status": "needs_candidate_review",
                    "reason": "both gallery and stronger non-gallery candidates exist for this target",
                }
            )

    bindings_by_detection: dict[str, list[dict[str, Any]]] = {}
    for binding in bindings:
        bindings_by_detection.setdefault(str(binding["detection_id"]), []).append(binding)
        qa_rows.append(
            {
                "item_type": "binding_candidate",
                "detection_id": binding["detection_id"],
                "target_id": binding["target_id"],
                "display_name": binding["target_display_name"],
                "status": binding["status"],
                "reason": binding["reason"],
            }
        )
        if binding.get("weak_name_match"):
            qa_rows.append(
                {
                    "item_type": "weak_name_match",
                    "detection_id": binding["detection_id"],
                    "target_id": binding["target_id"],
                    "display_name": binding["target_display_name"],
                    "status": "needs_binding_review",
                    "reason": "binding is plausible but relies on a weak name match",
                }
            )
        if binding.get("lower_trust_source_kind"):
            qa_rows.append(
                {
                    "item_type": "lower_trust_source_kind",
                    "detection_id": binding["detection_id"],
                    "target_id": binding["target_id"],
                    "display_name": binding["target_display_name"],
                    "status": "needs_binding_review",
                    "reason": "binding candidate is strong but comes from a lower-trust source kind",
                }
            )
        if binding.get("image_kind_mismatch"):
            qa_rows.append(
                {
                    "item_type": "binding_image_kind_mismatch",
                    "detection_id": binding["detection_id"],
                    "target_id": binding["target_id"],
                    "display_name": binding["target_display_name"],
                    "status": "needs_binding_review",
                    "reason": "candidate name matches, but image kind does not closely fit the target family",
                }
            )
    for detection_id, grouped in bindings_by_detection.items():
        ordered = sorted(grouped, key=lambda row: (-float(row.get("confidence", 0.0)), str(row.get("candidate_id", ""))))
        if len(ordered) >= 2 and float(ordered[0].get("confidence", 0.0)) >= 0.75 and abs(float(ordered[0].get("confidence", 0.0)) - float(ordered[1].get("confidence", 0.0))) <= 0.05:
            qa_rows.append(
                {
                    "item_type": "conflicting_binding_candidates",
                    "detection_id": detection_id,
                    "target_id": ordered[0]["target_id"],
                    "display_name": ordered[0]["target_display_name"],
                    "status": "needs_binding_review",
                    "reason": f"multiple plausible candidates are close in score: {ordered[0]['candidate_id']}, {ordered[1]['candidate_id']}",
                }
            )
    return qa_rows


def _build_population_qa_queue(
    ontology: dict[str, list[dict[str, Any]]],
    candidates: list[dict[str, Any]],
    findings: list[dict[str, Any]],
    *,
    source_failures: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    qa_rows: list[dict[str, Any]] = []
    candidate_names = {_binding_key(str(candidate.get("display_name", ""))) for candidate in candidates}

    for failure in source_failures or []:
        qa_rows.append(
            {
                "item_type": "source_fetch_failed",
                "target_kind": "source",
                "target_id": "",
                "display_name": failure.get("source_role", ""),
                "status": "needs_source_review",
                "reason": failure.get("error", ""),
            }
        )

    for candidate in candidates:
        if str(candidate.get("anchor_source", "")).strip() == "filename":
            qa_rows.append(
                {
                    "item_type": "filename_only_anchor",
                    "target_kind": "candidate",
                    "target_id": "",
                    "display_name": candidate.get("display_name", ""),
                    "status": "needs_candidate_review",
                    "reason": "candidate naming was inferred from the image filename only",
                }
            )

    for row in ontology.get("abilities", []):
        if str(row.get("class", "unknown")).strip() == "unknown":
            qa_rows.append(
                {
                    "item_type": "missing_classification",
                    "target_kind": "ability",
                    "target_id": row.get("ability_id", ""),
                    "display_name": row.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": "ability class remains unknown after source ingestion and starter-seed fallback",
                }
            )
        if _binding_key(str(row.get("display_name", ""))) in candidate_names and str(row.get("class", "unknown")).strip() == "unknown":
            qa_rows.append(
                {
                    "item_type": "weak_metadata_with_candidate",
                    "target_kind": "ability",
                    "target_id": row.get("ability_id", ""),
                    "display_name": row.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": "candidate asset exists but ontology metadata is still weak",
                }
            )

    for row in ontology.get("events", []):
        if str(row.get("category", "unknown")).strip() == "unknown":
            qa_rows.append(
                {
                    "item_type": "missing_classification",
                    "target_kind": "event",
                    "target_id": row.get("event_id", ""),
                    "display_name": row.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": "event category remains unknown after source ingestion and starter-seed fallback",
                }
            )
        if _binding_key(str(row.get("display_name", ""))) in candidate_names and str(row.get("category", "unknown")).strip() == "unknown":
            qa_rows.append(
                {
                    "item_type": "weak_metadata_with_candidate",
                    "target_kind": "event",
                    "target_id": row.get("event_id", ""),
                    "display_name": row.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": "candidate asset exists but ontology metadata is still weak",
                }
            )

    for finding in findings:
        status = str(finding.get("status", ""))
        if status == "ambiguous_starter_seed_match":
            qa_rows.append(
                {
                    "item_type": "ambiguous_seed_match",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"starter seed produced multiple plausible matches: {', '.join(finding.get('candidate_ids', []))}",
                }
            )
        elif status == "ambiguous_identity_match":
            qa_rows.append(
                {
                    "item_type": "ambiguous_identity_match",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"multiple canonical identity candidates remain unresolved: {', '.join(finding.get('candidate_ids', []))}",
                }
            )
        elif status == "conflicting_identity_match":
            qa_rows.append(
                {
                    "item_type": "conflicting_identity_match",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"candidate identities conflict with the current canonical row: {', '.join(finding.get('candidate_values', []))}",
                }
            )
        elif status == "identity_match_rejected":
            qa_rows.append(
                {
                    "item_type": "identity_match_rejected",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": "identity match was rejected because canonical identity could not be reconciled safely",
                }
            )
        elif status == "canonical_identity_preference_applied":
            qa_rows.append(
                {
                    "item_type": "canonical_identity_preference_applied",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "info",
                    "reason": f"canonical identity preference was applied from {finding.get('identity_source', 'source')} via {finding.get('basis', 'identity reconciliation')}",
                }
            )
        elif status == "weak_source_extraction":
            qa_rows.append(
                {
                    "item_type": "weak_source_extraction",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"source text did not yield a confident {finding.get('field', 'structured')} value",
                }
            )
        elif status == "ambiguous_structured_extraction":
            qa_rows.append(
                {
                    "item_type": "ambiguous_structured_extraction",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"source text implied multiple {finding.get('field', 'structured')} values: {', '.join(finding.get('candidate_values', []))}",
                }
            )
        elif status == "source_seed_disagreement":
            qa_rows.append(
                {
                    "item_type": "source_seed_disagreement",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": (
                        f"source-derived {finding.get('field', 'field')} '{finding.get('source_value', '')}' "
                        f"disagrees with starter-seed value '{finding.get('starter_seed_value', '')}'"
                    ),
                }
            )
        elif status == "alias_ambiguity":
            qa_rows.append(
                {
                    "item_type": "alias_ambiguity",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"multiple alias candidates were inferred: {', '.join(finding.get('candidate_aliases', []))}",
                }
            )
        elif status == "ambiguous_listing_detail_enrichment":
            qa_rows.append(
                {
                    "item_type": "ambiguous_listing_detail_enrichment",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"detail paragraph matched multiple listed rows: {', '.join(finding.get('candidate_values', []))}",
                }
            )
        elif status == "conflicting_listing_detail_enrichment":
            qa_rows.append(
                {
                    "item_type": "conflicting_listing_detail_enrichment",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": (
                        f"detail enrichment disagreed on {finding.get('field', 'field')}: "
                        f"{', '.join(finding.get('candidate_values', []))}"
                    ),
                }
            )
        elif status == "existing_listing_detail_enrichment_conflict":
            qa_rows.append(
                {
                    "item_type": "existing_listing_detail_enrichment_conflict",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": (
                        f"detail enrichment disagreed with existing {finding.get('field', 'field')} "
                        f"'{finding.get('existing_value', '')}': "
                        f"{', '.join(finding.get('candidate_values', []))}"
                    ),
                }
            )
        elif status == "alias_equivalent_to_canonical_name":
            qa_rows.append(
                {
                    "item_type": "alias_equivalent_to_canonical_name",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"detail alias '{finding.get('alias', '')}' matches canonical display name",
                }
            )
        elif status == "alias_equivalent_to_existing_alias":
            qa_rows.append(
                {
                    "item_type": "alias_equivalent_to_existing_alias",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"detail alias '{finding.get('alias', '')}' matches an existing alias",
                }
            )
        elif status == "alias_suppressed_by_detail_conflict":
            qa_rows.append(
                {
                    "item_type": "alias_suppressed_by_detail_conflict",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": (
                        f"detail alias '{finding.get('alias', '')}' was suppressed because the detail contribution "
                        f"conflicted on fields: {', '.join(finding.get('candidate_values', []))}"
                    ),
                }
            )
        elif status == "weak_image_anchor":
            qa_rows.append(
                {
                    "item_type": "weak_image_anchor",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"image anchor relied on weak {finding.get('anchor_source', 'image')} context",
                }
            )
        elif status == "conflicting_image_anchor":
            qa_rows.append(
                {
                    "item_type": "conflicting_image_anchor",
                    "target_kind": finding.get("target_kind", ""),
                    "target_id": finding.get("target_id", ""),
                    "display_name": finding.get("display_name", ""),
                    "status": "needs_population_review",
                    "reason": f"image anchor conflicted across nearby labels: {', '.join(finding.get('candidate_values', []))}",
                }
            )
    return qa_rows


def _write_schema_adaptation_artifacts(
    stage_root: Path,
    catalog_root: Path,
    *,
    game_payload: dict[str, Any],
    ontology: dict[str, list[dict[str, Any]]],
    detection_manifest: dict[str, Any],
    game_schema: dict[str, Any],
    hud: dict[str, Any],
    weights: dict[str, Any],
    manifest_payload: dict[str, Any],
    state_payload: dict[str, Any],
) -> None:
    manifests_root = stage_root / "manifests"
    manifests_root.mkdir(parents=True, exist_ok=True)
    dump_yaml_file(stage_root / "game.yaml", game_payload)
    dump_yaml_file(stage_root / "entities.yaml", ontology)
    dump_yaml_file(stage_root / "hud.yaml", hud)
    dump_yaml_file(stage_root / "weights.yaml", weights)
    dump_yaml_file(manifests_root / "cv_templates.yaml", {"templates": []})
    dump_yaml_file(manifests_root / "game_detection_schema.yaml", game_schema)
    dump_yaml_file(manifests_root / "detection_manifest.yaml", detection_manifest)
    (manifests_root / "assets_manifest.json").write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    (manifests_root / "onboarding_state.json").write_text(json.dumps(state_payload, indent=2), encoding="utf-8")

    _write_csv(catalog_root / "heroes.csv", ontology["heroes"])
    _write_csv(catalog_root / "abilities.csv", ontology["abilities"])
    _write_csv(catalog_root / "events.csv", ontology["events"])
    _write_csv(catalog_root / "detection_rows.csv", detection_manifest["rows"])
    _write_csv(catalog_root / "asset_candidates.csv", [])
    _write_csv(catalog_root / "bindings.csv", [])
    _write_csv(catalog_root / "qa_queue.csv", [])
    _write_csv(catalog_root / "source_fetch_log.csv", [])


def _write_source_ingestion_artifacts(
    draft_root: Path,
    *,
    ontology: dict[str, list[dict[str, Any]]],
    detection_manifest: dict[str, Any],
    candidates: list[dict[str, Any]],
    qa_queue: list[dict[str, Any]],
    manifest_payload: dict[str, Any],
    state_payload: dict[str, Any],
) -> None:
    manifests_root = draft_root / "manifests"
    catalog_root = draft_root / "catalog"
    dump_yaml_file(draft_root / "entities.yaml", ontology)
    dump_yaml_file(manifests_root / "detection_manifest.yaml", detection_manifest)
    (manifests_root / "assets_manifest.json").write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    (manifests_root / "onboarding_state.json").write_text(json.dumps(state_payload, indent=2), encoding="utf-8")

    _write_csv(catalog_root / "heroes.csv", ontology["heroes"])
    _write_csv(catalog_root / "abilities.csv", ontology["abilities"])
    _write_csv(catalog_root / "events.csv", ontology["events"])
    _write_csv(catalog_root / "detection_rows.csv", detection_manifest["rows"])
    _write_csv(catalog_root / "asset_candidates.csv", candidates)
    _write_csv(catalog_root / "bindings.csv", [])
    _write_csv(catalog_root / "qa_queue.csv", qa_queue)
    _write_csv(catalog_root / "source_fetch_log.csv", manifest_payload["source_fetch_log"])


def _write_binding_review_artifacts(
    draft_root: Path,
    *,
    ontology: dict[str, list[dict[str, Any]]],
    detection_manifest: dict[str, Any],
    candidates: list[dict[str, Any]],
    bindings: list[dict[str, Any]],
    qa_queue: list[dict[str, Any]],
    manifest_payload: dict[str, Any],
    state_payload: dict[str, Any],
) -> None:
    manifests_root = draft_root / "manifests"
    catalog_root = draft_root / "catalog"
    dump_yaml_file(draft_root / "entities.yaml", ontology)
    dump_yaml_file(manifests_root / "detection_manifest.yaml", detection_manifest)
    (manifests_root / "assets_manifest.json").write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    (manifests_root / "onboarding_state.json").write_text(json.dumps(state_payload, indent=2), encoding="utf-8")

    _write_csv(catalog_root / "heroes.csv", ontology["heroes"])
    _write_csv(catalog_root / "abilities.csv", ontology["abilities"])
    _write_csv(catalog_root / "events.csv", ontology["events"])
    _write_csv(catalog_root / "detection_rows.csv", detection_manifest["rows"])
    _write_csv(catalog_root / "asset_candidates.csv", candidates)
    _write_csv(catalog_root / "bindings.csv", bindings)
    _write_csv(catalog_root / "qa_queue.csv", qa_queue)
    _write_csv(catalog_root / "source_fetch_log.csv", manifest_payload["source_fetch_log"])


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        if not rows:
            writer.writerow({"empty": ""})
            return
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key, "")) for key in headers})


def _csv_value(value: Any) -> Any:
    if isinstance(value, list):
        return json.dumps(value)
    return value


def _best_accepted_bindings_by_detection(bindings: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in bindings:
        detection_id = str(row.get("detection_id", "")).strip()
        if not detection_id:
            continue
        existing = best.get(detection_id)
        if existing is None or float(row.get("confidence", 0.0) or 0.0) > float(existing.get("confidence", 0.0) or 0.0):
            best[detection_id] = row
    return best


def _unresolved_detection_rows(
    detection_rows: list[dict[str, Any]],
    accepted_bindings_by_detection: dict[str, dict[str, Any]],
    candidates: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    unresolved: list[dict[str, Any]] = []
    for row in detection_rows:
        if str(row.get("status", "")) == "missing_semantic_values":
            unresolved.append(row)
            continue
        if not bool(row.get("requires_asset", True)):
            continue
        binding = accepted_bindings_by_detection.get(str(row.get("detection_id", "")))
        if binding is None:
            unresolved.append(row)
            continue
        candidate = candidates.get(str(binding.get("candidate_id", "")))
        if candidate is None or not candidate.get("master_path"):
            unresolved.append(row)
            continue
        if not Path(str(candidate["master_path"])).exists():
            unresolved.append(row)
    return unresolved


def _build_runtime_cv_rules_manifest(detection_rows: list[dict[str, Any]]) -> dict[str, Any]:
    event_mappings: dict[str, dict[str, Any]] = {}
    for row in detection_rows:
        asset_family = str(row["asset_family"])
        event_mappings.setdefault(asset_family, dict(row.get("runtime_rule", {})))
    return {"event_mappings": event_mappings}


def _build_fusion_rules_manifest(detection_rows: list[dict[str, Any]], detection_schema: dict[str, Any]) -> dict[str, Any]:
    fusion_rule_specs = detection_schema.get("fusion_rules", {})
    required_rule_ids = {
        str(rule_id)
        for row in detection_rows
        for rule_id in row.get("fusion_rule_ids", [])
        if str(rule_id).strip()
    }
    rules = [dict(fusion_rule_specs[rule_id]) for rule_id in sorted(required_rule_ids) if rule_id in fusion_rule_specs]
    return {
        "schema_version": "fusion_rules_v1",
        "rules": rules,
    }


def _source_log_row(record: dict[str, Any]) -> dict[str, Any]:
    status = "fetched"
    if str(record.get("page_type", "")) == "category" and not record.get("category_items"):
        status = "empty_source"
    elif str(record.get("page_type", "")) == "article":
        sections = record.get("sections", [])
        has_content = any(getattr(section, "items", []) or getattr(section, "images", []) for section in sections)
        if not has_content:
            status = "empty_source"
    return {
        "source_page_url": record["url"],
        "source_role": record["role"],
        "source_title": record["title"],
        "status": status,
        "content_type": record["content_type"],
        "page_type": record["page_type"],
    }


def _source_failure_row(source: OnboardingSource, error: WikiFetchError) -> dict[str, Any]:
    return {
        "source_page_url": source.url,
        "source_role": source.role,
        "source_title": "",
        "status": "fetch_failed",
        "content_type": "",
        "page_type": "",
        "failure_category": error.category,
        "error": error.message,
        "hint": error.hint,
    }


def _empty_ontology() -> dict[str, list[dict[str, Any]]]:
    return {"heroes": [], "abilities": [], "events": []}


def _empty_detection_manifest(game: str, schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "game_detection_manifest_v1",
        "baseline_schema_version": str(
            schema.get("baseline_schema_version", schema.get("schema_version", "runtime_detection_schema_v1"))
        ),
        "game_id": game,
        "row_count": 0,
        "required_row_count": 0,
        "ready_row_count": 0,
        "rows_needing_assets": 0,
        "rows": [],
    }


def _build_assets_manifest_payload(
    *,
    game: str,
    source_records: list[dict[str, Any]],
    detection_manifest: dict[str, Any],
    candidates: list[dict[str, Any]],
    bindings: list[dict[str, Any]],
    population_findings: list[dict[str, Any]] | None = None,
    source_failures: list[dict[str, Any]] | None = None,
    phase_status: str,
    schema_path: str,
) -> dict[str, Any]:
    return {
        "game_id": game,
        "generated_at": datetime.now(UTC).isoformat(),
        "phase_status": phase_status,
        "schema_path": schema_path,
        "source_count": len(source_records),
        "source_fetch_log": [_source_log_row(record) for record in source_records] + list(source_failures or []),
        "detection_manifest": {
            "schema_version": detection_manifest["schema_version"],
            "row_count": detection_manifest["row_count"],
            "required_row_count": detection_manifest["required_row_count"],
            "ready_row_count": detection_manifest["ready_row_count"],
            "rows_needing_assets": detection_manifest["rows_needing_assets"],
        },
        "candidates": candidates,
        "bindings": bindings,
        "population_findings": list(population_findings or []),
        "source_failures": list(source_failures or []),
    }


def _build_onboarding_state(
    game: str,
    *,
    phase_status: str,
    source_count: int = 0,
    schema_path: str,
) -> dict[str, Any]:
    return {
        "schema_version": _ONBOARDING_STATE_VERSION,
        "game_id": game,
        "phase_status": phase_status,
        "schema_path": schema_path,
        "source_count": source_count,
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _resolve_or_create_onboarding_draft(schema_draft_or_game: str | Path, *, repo_root: Path) -> Path:
    candidate = Path(schema_draft_or_game).expanduser()
    if candidate.exists():
        draft_root = candidate.resolve()
        schema_path = draft_root / "manifests" / "game_detection_schema.yaml"
        if not schema_path.exists():
            raise FileNotFoundError(f"draft does not contain game detection schema: {schema_path}")
        return draft_root
    result = adapt_game_schema(str(schema_draft_or_game), repo_root=repo_root)
    return Path(result["draft_root"])


def _load_game_detection_schema(draft_root: Path) -> dict[str, Any]:
    schema_path = draft_root / "manifests" / "game_detection_schema.yaml"
    if not schema_path.exists():
        raise FileNotFoundError(f"draft does not contain game detection schema: {schema_path}")
    payload = load_yaml_file(schema_path)
    if not isinstance(payload, dict):
        raise ValueError("game detection schema draft must be a mapping")
    return payload


def _adapt_detection_schema(game: str, baseline_schema: dict[str, Any], *, repo_root: Path) -> dict[str, Any]:
    schema = json.loads(json.dumps(baseline_schema))
    adapter = get_onboarding_adapter(game)
    overrides = _load_game_detection_schema_overrides(game, repo_root=repo_root)
    disabled_families = {
        str(item).strip()
        for item in list(adapter.disabled_families) + list(overrides.get("disabled_families", []))
        if str(item).strip()
    }
    families = schema.get("families", {})
    if not isinstance(families, dict):
        raise ValueError("runtime detection schema must define a families mapping")
    family_overrides = overrides.get("families", {})
    if not isinstance(family_overrides, dict):
        raise ValueError("game detection schema overrides families must be a mapping")
    for asset_family, family_spec in families.items():
        if not isinstance(family_spec, dict):
            continue
        if asset_family in disabled_families:
            family_spec["enabled"] = False
        merged_override = {}
        adapter_override = adapter.family_overrides.get(asset_family, {})
        if isinstance(adapter_override, dict):
            merged_override.update(adapter_override)
        explicit_override = family_overrides.get(asset_family, {})
        if isinstance(explicit_override, dict):
            merged_override.update(explicit_override)
        for key, value in merged_override.items():
            family_spec[key] = value

    schema["schema_version"] = _GAME_DETECTION_SCHEMA_DRAFT
    schema["baseline_schema_version"] = str(baseline_schema.get("schema_version", "runtime_detection_schema_v1"))
    schema["game_id"] = game
    schema["active_asset_families"] = sorted(_active_families(schema))
    _validate_detection_schema_payload(schema, repo_root=repo_root, schema_label="game detection schema draft")
    return schema


def _load_game_detection_schema_overrides(game: str, *, repo_root: Path) -> dict[str, Any]:
    overrides_path = repo_root / "starter_assets" / game / "game_detection_schema_overrides.yaml"
    if not overrides_path.exists():
        return {}
    payload = load_yaml_file(overrides_path)
    if not isinstance(payload, dict):
        raise ValueError(f"game detection schema overrides for '{game}' must be a mapping")
    disabled_families = payload.get("disabled_families", [])
    if disabled_families and not isinstance(disabled_families, list):
        raise ValueError(f"game detection schema overrides for '{game}' disabled_families must be a list")
    families = payload.get("families", {})
    if families and not isinstance(families, dict):
        raise ValueError(f"game detection schema overrides for '{game}' families must be a mapping")
    if isinstance(families, dict):
        for asset_family, family_override in families.items():
            if not isinstance(family_override, dict):
                raise ValueError(
                    f"game detection schema overrides for '{game}' family '{asset_family}' must be a mapping"
                )
    return payload


def _load_starter_seed_data(
    game: str,
    *,
    repo_root: Path,
    adapter: GameOnboardingAdapter,
) -> dict[str, list[dict[str, Any]]]:
    starter_root = repo_root / "starter_assets" / game
    seeds: dict[str, list[dict[str, Any]]] = {"heroes": [], "abilities": [], "events": []}
    for spec in adapter.starter_seed_specs:
        _append_seed_rows_from_spec(seeds, starter_root, spec)
    return seeds


def _append_seed_rows_from_spec(
    seeds: dict[str, list[dict[str, Any]]],
    starter_root: Path,
    spec: StarterSeedSpec,
) -> None:
    seed_path = starter_root / spec.file_name
    if not seed_path.exists():
        return
    payload = load_yaml_file(seed_path)
    rows = payload.get(spec.root_key, []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_id = str(row.get("id", "")).strip()
        if not raw_id:
            continue
        seed_row = {
            spec.id_field: raw_id,
            "display_name": str(row.get("display_name", row.get("description", raw_id))).strip() or raw_id,
            "aliases": _coerce_string_list(row.get("aliases", [])),
            "source_kind": spec.source_kind,
        }
        for field in spec.extra_fields:
            seed_row[field] = str(row.get(field, "")).strip()
        seeds[spec.ontology_section].append(seed_row)


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _enrich_ontology_with_starter_seed(
    game: str,
    ontology: dict[str, list[dict[str, Any]]],
    seed_data: dict[str, list[dict[str, Any]]],
    findings: list[dict[str, Any]],
) -> None:
    for hero in ontology.get("heroes", []):
        matches = _match_starter_seed_rows(hero, seed_data.get("heroes", []), id_field="hero_id")
        if len(matches) > 1:
            findings.append(_seed_finding("ambiguous_identity_match", "hero", hero.get("hero_id", ""), hero.get("display_name", ""), [str(row.get("hero_id", "")).strip() for row in matches if str(row.get("hero_id", "")).strip()]))
            continue
        if not matches:
            continue
        match = matches[0]
        identity = reconcile_identity(
            {
                "display_name": str(hero.get("display_name", "")),
                "canonical_id": str(hero.get("hero_id", "")),
                "aliases": [str(item) for item in hero.get("aliases", []) if str(item).strip()],
            },
            {
                "display_name": str(match.get("display_name", "")),
                "canonical_id": str(match.get("hero_id", "")),
                "aliases": _coerce_string_list(match.get("aliases", [])),
            },
            preferred_source="starter_seed",
        )
        if identity["match_status"] in {"conflicting_identity_match", "identity_match_rejected"}:
            findings.append(
                {
                    "status": identity["match_status"],
                    "target_kind": "hero",
                    "target_id": str(hero.get("hero_id", "")),
                    "display_name": str(hero.get("display_name", "")),
                    "candidate_ids": identity.get("candidate_ids", []),
                    "candidate_values": identity.get("candidate_names", []),
                    "source_page_url": hero.get("source_page_url", ""),
                    "source_role": hero.get("source_role", ""),
                }
            )
            continue
        if hero.get("role") and match.get("role") and str(hero["role"]) != str(match["role"]):
            findings.append(_seed_disagreement_finding("hero", hero.get("hero_id", ""), hero.get("display_name", ""), "role", str(hero["role"]), str(match["role"])))
        old_id = str(hero.get("hero_id", ""))
        hero["hero_id"] = str(identity["chosen_canonical_id"])
        hero["display_name"] = str(identity["chosen_display_name"])
        hero["canonical_display_name_source"] = str(identity.get("identity_source", "starter_seed"))
        hero["canonical_id_source"] = str(identity.get("identity_source", "starter_seed"))
        hero["canonical_identity_basis"] = str(identity.get("basis", ""))
        if identity["match_status"] == "canonical_identity_preference_applied":
            findings.append(
                {
                    "status": "canonical_identity_preference_applied",
                    "target_kind": "hero",
                    "target_id": hero.get("hero_id", ""),
                    "display_name": hero.get("display_name", ""),
                    "candidate_ids": [old_id, str(match.get("hero_id", ""))],
                    "candidate_values": [str(match.get("display_name", ""))],
                    "identity_source": "starter_seed",
                    "source_page_url": hero.get("source_page_url", ""),
                    "source_role": hero.get("source_role", ""),
                    "basis": identity.get("basis", ""),
                }
            )
        merged_aliases, alias_rejections = merge_aliases(
            [str(item) for item in hero.get("aliases", []) if str(item).strip()],
            _coerce_string_list(match.get("aliases", [])),
            canonical_name=hero["display_name"],
        )
        hero["aliases"] = merged_aliases
        if merged_aliases and not hero.get("aliases_source"):
            hero["aliases_source"] = "starter_seed"
        for rejection in alias_rejections:
            findings.append(
                {
                    "status": rejection["status"],
                    "alias": rejection["alias"],
                    "target_kind": "hero",
                    "target_id": hero.get("hero_id", ""),
                    "display_name": hero.get("display_name", ""),
                    "source_page_url": hero.get("source_page_url", ""),
                    "source_role": hero.get("source_role", ""),
                }
            )
        if not hero.get("role") and match.get("role"):
            hero["role"] = match["role"]
            hero["role_source"] = "starter_seed"
        hero["starter_seed_applied"] = True
        hero["starter_seed_source"] = str(match.get("source_kind", ""))

    for ability in ontology.get("abilities", []):
        matches = _match_starter_seed_rows(ability, seed_data.get("abilities", []), id_field="ability_id")
        if len(matches) > 1:
            findings.append(_seed_finding("ambiguous_identity_match", "ability", ability.get("ability_id", ""), ability.get("display_name", ""), [str(row.get("ability_id", "")).strip() for row in matches if str(row.get("ability_id", "")).strip()]))
            continue
        if not matches:
            continue
        match = matches[0]
        identity = reconcile_identity(
            {
                "display_name": str(ability.get("display_name", "")),
                "canonical_id": str(ability.get("ability_id", "")),
                "aliases": [str(item) for item in ability.get("aliases", []) if str(item).strip()],
            },
            {
                "display_name": str(match.get("display_name", "")),
                "canonical_id": str(match.get("ability_id", "")),
                "aliases": _coerce_string_list(match.get("aliases", [])),
            },
            preferred_source="starter_seed",
        )
        if identity["match_status"] in {"conflicting_identity_match", "identity_match_rejected"}:
            findings.append(
                {
                    "status": identity["match_status"],
                    "target_kind": "ability",
                    "target_id": str(ability.get("ability_id", "")),
                    "display_name": str(ability.get("display_name", "")),
                    "candidate_ids": identity.get("candidate_ids", []),
                    "candidate_values": identity.get("candidate_names", []),
                    "source_page_url": ability.get("source_page_url", ""),
                    "source_role": ability.get("source_role", ""),
                }
            )
            continue
        if ability.get("class") and ability.get("class") != "unknown" and match.get("class") and str(ability["class"]) != str(match["class"]):
            findings.append(_seed_disagreement_finding("ability", ability.get("ability_id", ""), ability.get("display_name", ""), "class", str(ability["class"]), str(match["class"])))
        if ability.get("class", "unknown") == "unknown" and match.get("class"):
            ability["class"] = match["class"]
            ability["class_source"] = "starter_seed"
        if match.get("character_id"):
            ability["character_id"] = match["character_id"]
        old_id = str(ability.get("ability_id", ""))
        ability["ability_id"] = str(identity["chosen_canonical_id"])
        ability["display_name"] = str(identity["chosen_display_name"])
        ability["canonical_display_name_source"] = str(identity.get("identity_source", "starter_seed"))
        ability["canonical_id_source"] = str(identity.get("identity_source", "starter_seed"))
        ability["canonical_identity_basis"] = str(identity.get("basis", ""))
        if identity["match_status"] == "canonical_identity_preference_applied":
            findings.append(
                {
                    "status": "canonical_identity_preference_applied",
                    "target_kind": "ability",
                    "target_id": ability.get("ability_id", ""),
                    "display_name": ability.get("display_name", ""),
                    "candidate_ids": [old_id, str(match.get("ability_id", ""))],
                    "candidate_values": [str(match.get("display_name", ""))],
                    "identity_source": "starter_seed",
                    "source_page_url": ability.get("source_page_url", ""),
                    "source_role": ability.get("source_role", ""),
                    "basis": identity.get("basis", ""),
                }
            )
        merged_aliases, alias_rejections = merge_aliases(
            [str(item) for item in ability.get("aliases", []) if str(item).strip()],
            _coerce_string_list(match.get("aliases", [])),
            canonical_name=ability["display_name"],
        )
        ability["aliases"] = merged_aliases
        if merged_aliases and not ability.get("aliases_source"):
            ability["aliases_source"] = "starter_seed"
        for rejection in alias_rejections:
            findings.append(
                {
                    "status": rejection["status"],
                    "alias": rejection["alias"],
                    "target_kind": "ability",
                    "target_id": ability.get("ability_id", ""),
                    "display_name": ability.get("display_name", ""),
                    "source_page_url": ability.get("source_page_url", ""),
                    "source_role": ability.get("source_role", ""),
                }
            )
        ability["starter_seed_applied"] = True
        ability["starter_seed_source"] = str(match.get("source_kind", ""))

    for event in ontology.get("events", []):
        matches = _match_starter_seed_rows(event, seed_data.get("events", []), id_field="event_id")
        if len(matches) > 1:
            findings.append(_seed_finding("ambiguous_identity_match", "event", event.get("event_id", ""), event.get("display_name", ""), [str(row.get("event_id", "")).strip() for row in matches if str(row.get("event_id", "")).strip()]))
            continue
        if not matches:
            continue
        match = matches[0]
        identity = reconcile_identity(
            {
                "display_name": str(event.get("display_name", "")),
                "canonical_id": str(event.get("event_id", "")),
                "aliases": [str(item) for item in event.get("aliases", []) if str(item).strip()],
            },
            {
                "display_name": str(match.get("display_name", "")),
                "canonical_id": str(match.get("event_id", "")),
                "aliases": _coerce_string_list(match.get("aliases", [])),
            },
            preferred_source="starter_seed",
        )
        if identity["match_status"] in {"conflicting_identity_match", "identity_match_rejected"}:
            findings.append(
                {
                    "status": identity["match_status"],
                    "target_kind": "event",
                    "target_id": str(event.get("event_id", "")),
                    "display_name": str(event.get("display_name", "")),
                    "candidate_ids": identity.get("candidate_ids", []),
                    "candidate_values": identity.get("candidate_names", []),
                    "source_page_url": event.get("source_page_url", ""),
                    "source_role": event.get("source_role", ""),
                }
            )
            continue
        if event.get("category") and event.get("category") != "unknown" and match.get("category") and str(event["category"]) != str(match["category"]):
            findings.append(_seed_disagreement_finding("event", event.get("event_id", ""), event.get("display_name", ""), "category", str(event["category"]), str(match["category"])))
        if event.get("category", "unknown") == "unknown" and match.get("category"):
            event["category"] = match["category"]
            event["category_source"] = "starter_seed"
        old_id = str(event.get("event_id", ""))
        event["event_id"] = str(identity["chosen_canonical_id"])
        event["display_name"] = str(identity["chosen_display_name"])
        event["canonical_display_name_source"] = str(identity.get("identity_source", "starter_seed"))
        event["canonical_id_source"] = str(identity.get("identity_source", "starter_seed"))
        event["canonical_identity_basis"] = str(identity.get("basis", ""))
        if identity["match_status"] == "canonical_identity_preference_applied":
            findings.append(
                {
                    "status": "canonical_identity_preference_applied",
                    "target_kind": "event",
                    "target_id": event.get("event_id", ""),
                    "display_name": event.get("display_name", ""),
                    "candidate_ids": [old_id, str(match.get("event_id", ""))],
                    "candidate_values": [str(match.get("display_name", ""))],
                    "identity_source": "starter_seed",
                    "source_page_url": event.get("source_page_url", ""),
                    "source_role": event.get("source_role", ""),
                    "basis": identity.get("basis", ""),
                }
            )
        merged_aliases, alias_rejections = merge_aliases(
            [str(item) for item in event.get("aliases", []) if str(item).strip()],
            _coerce_string_list(match.get("aliases", [])),
            canonical_name=event["display_name"],
        )
        event["aliases"] = merged_aliases
        if merged_aliases and not event.get("aliases_source"):
            event["aliases_source"] = "starter_seed"
        for rejection in alias_rejections:
            findings.append(
                {
                    "status": rejection["status"],
                    "alias": rejection["alias"],
                    "target_kind": "event",
                    "target_id": event.get("event_id", ""),
                    "display_name": event.get("display_name", ""),
                    "source_page_url": event.get("source_page_url", ""),
                    "source_role": event.get("source_role", ""),
                }
            )
        event["starter_seed_applied"] = True
        event["starter_seed_source"] = str(match.get("source_kind", ""))

    ontology["heroes"] = sorted(ontology.get("heroes", []), key=lambda row: row["hero_id"])
    ontology["abilities"] = sorted(ontology.get("abilities", []), key=lambda row: row["ability_id"])
    ontology["events"] = sorted(ontology.get("events", []), key=lambda row: row["event_id"])


def _match_starter_seed_rows(
    row: dict[str, Any],
    seed_rows: list[dict[str, Any]],
    *,
    id_field: str,
) -> list[dict[str, Any]]:
    row_keys = identity_keys(
        str(row.get("display_name", "")),
        canonical_id=str(row.get(id_field, "")),
        aliases=[str(item) for item in row.get("aliases", []) if str(item).strip()],
    )
    if not row_keys:
        return []
    matches: list[dict[str, Any]] = []
    for row in seed_rows:
        seed_keys = identity_keys(
            str(row.get("display_name", "")),
            canonical_id=str(row.get(id_field, "")),
            aliases=_coerce_string_list(row.get("aliases", [])),
        )
        if row_keys & seed_keys:
            matches.append(row)
    return matches


def _seed_finding(status: str, target_kind: str, target_id: Any, display_name: Any, candidate_ids: list[str]) -> dict[str, Any]:
    return {
        "status": status,
        "target_kind": target_kind,
        "target_id": str(target_id),
        "display_name": str(display_name),
        "candidate_ids": candidate_ids,
    }


def _seed_disagreement_finding(
    target_kind: str,
    target_id: Any,
    display_name: Any,
    field: str,
    source_value: str,
    starter_seed_value: str,
) -> dict[str, Any]:
    return {
        "status": "source_seed_disagreement",
        "target_kind": target_kind,
        "target_id": str(target_id),
        "display_name": str(display_name),
        "field": field,
        "source_value": source_value,
        "starter_seed_value": starter_seed_value,
    }


def _game_payload_for_game(game: str, *, repo_root: Path) -> dict[str, Any]:
    starter_root = repo_root / "starter_assets" / game
    if (starter_root / "game.yaml").exists():
        starter = load_yaml_file(starter_root / "game.yaml")
        starter["game_id"] = game
        return starter
    return {
        "game_id": game,
        "display_name": game.replace("_", " ").title(),
        "genre": "unknown",
        "camera_mode": "unknown",
        "ui_version": "draft",
    }


def _default_hud_for_game(game: str, *, repo_root: Path) -> dict[str, Any]:
    starter_root = repo_root / "starter_assets" / game
    rois: dict[str, Any] = {
        "hero_portrait": {"anchor": "top_left", "x_pct": 0.02, "y_pct": 0.02, "w_pct": 0.12, "h_pct": 0.12},
        "ability_hud": {"anchor": "bottom_center", "x_pct": 0.32, "y_pct": 0.80, "w_pct": 0.36, "h_pct": 0.16},
        "medal_area": {"anchor": "center", "x_pct": 0.40, "y_pct": 0.18, "w_pct": 0.20, "h_pct": 0.18},
        "kill_feed": {"anchor": "top_right", "x_pct": 0.74, "y_pct": 0.05, "w_pct": 0.22, "h_pct": 0.22},
    }
    roi_profiles_path = starter_root / "roi_profiles.yaml"
    if roi_profiles_path.exists():
        roi_payload = load_yaml_file(roi_profiles_path)
        rois.update(roi_payload.get("rois", {}))
    return {"rois": rois}


def _default_weights_for_game(game: str, *, repo_root: Path) -> dict[str, Any]:
    starter_root = repo_root / "starter_assets" / game
    weights_path = starter_root / "score_weights.yaml"
    if weights_path.exists():
        return load_yaml_file(weights_path)
    return {"weights": {}, "thresholds": {}, "gates": {}}


def _template_family_dir(asset_family: str) -> str:
    return {
        "hero_portrait": "heroes",
        "ability_icon": "abilities",
        "equipment_icon": "equipment",
        "medal_icon": "medals",
        "hud_icon": "hud",
    }.get(asset_family, "misc")


def _source_role_asset_family(role: str, display_name: str, *, adapter: GameOnboardingAdapter) -> str:
    default_family = adapter.role_asset_families.get(role, "")
    if default_family == "ability_icon":
        if any(word in display_name.casefold() for word in ("grenade", "mine", "trap", "turret")):
            return "equipment_icon"
        return "ability_icon"
    if default_family:
        return default_family
    return "hud_icon"


def _normalize_schema_name(value: str, *, adapter: GameOnboardingAdapter) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    if cleaned.casefold() in {"contents", "overview", "trivia", "references"}:
        return ""
    overridden = adapter.display_name_overrides.get(binding_key(cleaned))
    if overridden:
        return overridden
    return cleaned


def _normalize_candidate_name(value: str, *, fallback: str, adapter: GameOnboardingAdapter) -> str:
    normalized = _normalize_schema_name(value or fallback, adapter=adapter)
    normalized = re.sub(r"\b(icon|logo|badge|medal|emblem|portrait|thumbnail)\b", "", normalized, flags=re.IGNORECASE)
    return _normalize_schema_name(re.sub(r"\s{2,}", " ", normalized), adapter=adapter)


def _binding_key(value: str) -> str:
    tokens = [token for token in _canonical_id(value).split("_") if token and token not in _GENERIC_IMAGE_WORDS]
    return "_".join(tokens)


def _binding_confidence(
    *,
    target_key: str,
    candidate_key: str,
    target_display_name: str,
    candidate_display_name: str,
    source_role: str,
) -> tuple[float, str]:
    if not target_key or not candidate_key:
        return 0.0, ""
    if target_key == candidate_key:
        return (0.98 if source_role != "assets_reference" else 0.93), "exact normalized name match"
    target_tokens = set(target_key.split("_"))
    candidate_tokens = set(candidate_key.split("_"))
    overlap = target_tokens & candidate_tokens
    if overlap and overlap == target_tokens:
        return 0.84, "target tokens fully contained in candidate name"
    if overlap and len(overlap) / max(len(target_tokens), 1) >= 0.6:
        return 0.72, "strong token overlap between target and candidate"
    if target_display_name.casefold() in candidate_display_name.casefold():
        return 0.68, "target display name appears in candidate name"
    return 0.0, ""


def _canonical_id(value: str) -> str:
    return slugify(value).replace("-", "_")


def _timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _looks_like_direct_image_url(url: str) -> bool:
    return Path(urlparse(url).path).suffix.lower() in _DIRECT_IMAGE_EXTENSIONS


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if rows and list(rows[0].keys()) == ["empty"]:
        return []
    return rows
