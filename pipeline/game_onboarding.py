from __future__ import annotations

import csv
import hashlib
import json
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from pipeline.wiki_enrichment import (
    WikiFetchError,
    _WikiHtmlParser,
    _build_fetch_target,
    _clean_text,
    _normalize_source_url,
    _slugify,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_TIMEOUT_SECONDS = 20
_SUPPORTED_SOURCE_ROLES = {
    "overview",
    "roster",
    "abilities",
    "events",
    "medals",
    "assets_reference",
}
_DIRECT_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
_ENTITY_ROLE_TO_KIND = {
    "roster": "character_or_operator",
    "abilities": "ability_or_equipment",
    "events": "event_badge_or_medal",
    "medals": "event_badge_or_medal",
}
_ENTITY_KIND_TO_ASSET_FAMILY = {
    "character_or_operator": "hero_portrait",
    "ability_or_equipment": "ability_icon",
    "event_badge_or_medal": "medal_icon",
}
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


def onboard_game_from_manifest(
    game: str,
    source_manifest: str | Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
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
        if role not in _SUPPORTED_SOURCE_ROLES:
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
    timestamp = _timestamp_slug()
    drafts_parent = repo_root / "assets" / "games" / game / "drafts" / "onboarding"
    drafts_parent.mkdir(parents=True, exist_ok=True)
    draft_root = drafts_parent / timestamp
    stage_root = Path(tempfile.mkdtemp(prefix=f".{timestamp}.", dir=drafts_parent))
    catalog_root = stage_root / "catalog"
    masters_root = stage_root / "masters"
    review_root = stage_root / "review"
    for path in (catalog_root, masters_root, review_root):
        path.mkdir(parents=True, exist_ok=True)

    try:
        normalized_sources = [_normalize_source(source) for source in sources]
        source_records = [_fetch_source_record(source) for source in normalized_sources]
        ontology = _build_ontology(game, source_records)
        candidates = _collect_candidate_assets(source_records, masters_root)
        for candidate in candidates:
            if candidate.get("master_path"):
                relative_master = Path(candidate["master_path"]).resolve().relative_to(stage_root.resolve())
                candidate["master_path"] = str(draft_root / relative_master)
        bindings = _build_binding_candidates(game, ontology, candidates)
        qa_queue = _build_qa_queue(ontology, candidates, bindings)
        hud = _default_hud_for_game(game, repo_root=repo_root)
        weights = _default_weights_for_game(game, repo_root=repo_root)
        game_payload = _game_payload_for_game(game, repo_root=repo_root)
        manifest_payload = {
            "game_id": game,
            "generated_at": datetime.now(UTC).isoformat(),
            "source_count": len(source_records),
            "source_fetch_log": [_source_log_row(record) for record in source_records],
            "candidates": candidates,
            "bindings": bindings,
        }
        _write_onboarding_artifacts(
            stage_root,
            catalog_root,
            game_payload=game_payload,
            ontology=ontology,
            hud=hud,
            weights=weights,
            candidates=candidates,
            bindings=bindings,
            qa_queue=qa_queue,
            manifest_payload=manifest_payload,
        )
        if draft_root.exists():
            shutil.rmtree(draft_root)
        stage_root.rename(draft_root)
    except Exception:
        shutil.rmtree(stage_root, ignore_errors=True)
        raise

    return {
        "ok": True,
        "status": "ok",
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
            "candidate_assets": len(candidates),
            "binding_candidates": len(bindings),
            "qa_queue": len(qa_queue),
        },
        "artifacts": {
            "game": str(draft_root / "game.yaml"),
            "entities": str(draft_root / "entities.yaml"),
            "hud": str(draft_root / "hud.yaml"),
            "weights": str(draft_root / "weights.yaml"),
            "assets_manifest": str(draft_root / "manifests" / "assets_manifest.json"),
            "bindings_csv": str(draft_root / "catalog" / "bindings.csv"),
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

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    game = str(manifest_payload["game_id"])
    bindings = _read_csv_rows(draft_root / "catalog" / "bindings.csv")
    candidates = {row["candidate_id"]: row for row in manifest_payload.get("candidates", [])}

    accepted_bindings = [row for row in bindings if row.get("status") == "accepted"]
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

        for binding in accepted_bindings:
            candidate = candidates.get(binding["candidate_id"])
            if candidate is None:
                continue
            if not candidate.get("master_path"):
                continue
            source_path = Path(candidate["master_path"])
            if not source_path.exists():
                continue
            asset_family = binding["asset_family"]
            family_dir = _template_family_dir(asset_family)
            target_slug = _slugify(binding["target_display_name"])
            extension = source_path.suffix or ".png"
            master_relative = Path("masters") / family_dir / f"{target_slug}{extension}"
            template_relative = Path("templates") / family_dir / f"{target_slug}{extension}"
            published_master_path = stage_root / master_relative
            published_template_path = stage_root / template_relative
            published_master_path.parent.mkdir(parents=True, exist_ok=True)
            published_template_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, published_master_path)
            shutil.copyfile(source_path, published_template_path)

            defaults = _ASSET_FAMILY_DEFAULTS[asset_family]
            asset_id = f"{game}.{binding['target_id']}.{asset_family}"
            cv_templates.append(
                {
                    "asset_id": asset_id,
                    "game_id": game,
                    "asset_family": asset_family,
                    "display_name": binding["target_display_name"],
                    "template_path": str(template_relative).replace("\\", "/"),
                    "mask_path": "",
                    "roi_ref": defaults["roi_ref"],
                    "match_method": defaults["match_method"],
                    "threshold": defaults["threshold"],
                    "scale_set": defaults["scale_set"],
                    "temporal_window": defaults["temporal_window"],
                    "source_url": candidate["source_url"],
                    "source_page_url": candidate["source_page_url"],
                    "source_kind": candidate["source_kind"],
                    "license_note": candidate.get("license_note", "unknown"),
                    "binding_status": "accepted",
                }
            )
            published_assets.append(
                {
                    "asset_id": asset_id,
                    "candidate_id": candidate["candidate_id"],
                    "target_id": binding["target_id"],
                    "display_name": binding["target_display_name"],
                    "asset_family": asset_family,
                    "master_path": str(master_relative).replace("\\", "/"),
                    "template_path": str(template_relative).replace("\\", "/"),
                    "source_url": candidate["source_url"],
                    "source_page_url": candidate["source_page_url"],
                    "source_kind": candidate["source_kind"],
                    "license_note": candidate.get("license_note", "unknown"),
                }
            )

        dump_yaml_file(stage_root / "game.yaml", game_payload)
        dump_yaml_file(stage_root / "entities.yaml", entities_payload)
        dump_yaml_file(stage_root / "hud.yaml", hud_payload)
        dump_yaml_file(stage_root / "weights.yaml", weights_payload)
        dump_yaml_file(manifests_root / "cv_templates.yaml", {"templates": cv_templates})
        (manifests_root / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": game,
                    "published_at": datetime.now(UTC).isoformat(),
                    "published_assets": published_assets,
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
        "artifacts": {
            "game": str(published_root / "game.yaml"),
            "entities": str(published_root / "entities.yaml"),
            "hud": str(published_root / "hud.yaml"),
            "weights": str(published_root / "weights.yaml"),
            "cv_templates": str(published_root / "manifests" / "cv_templates.yaml"),
            "assets_manifest": str(published_root / "manifests" / "assets_manifest.json"),
        },
    }


def _normalize_source(source: OnboardingSource) -> OnboardingSource:
    return OnboardingSource(role=source.role, url=_normalize_source_url(source.url), notes=source.notes)


def _fetch_source_record(source: OnboardingSource) -> dict[str, Any]:
    normalized_url = _normalize_source_url(source.url)
    try:
        response_target = _build_fetch_target(normalized_url)
        with urlopen(response_target, timeout=_DEFAULT_TIMEOUT_SECONDS) as response:
            content_type = response.headers.get_content_type() if hasattr(response, "headers") else "application/octet-stream"
            body = response.read()
    except HTTPError as exc:
        raise WikiFetchError(
            source_url=normalized_url,
            category="http_error",
            message=f"failed to fetch onboarding source: HTTP {exc.code}",
            hint="Verify the source exists or save the page locally and use a file path in the source manifest.",
            http_status=exc.code,
        ) from exc
    except URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise WikiFetchError(
            source_url=normalized_url,
            category="network_error",
            message=f"failed to fetch onboarding source: {reason}",
            hint="The source may be unavailable or the current environment may not have network access.",
        ) from exc

    if content_type.startswith("image/") or _looks_like_direct_image_url(normalized_url):
        return {
            "url": normalized_url,
            "role": source.role,
            "notes": source.notes,
            "title": Path(urlparse(normalized_url).path).name,
            "content_type": content_type,
            "page_type": "direct_image",
            "sections": [],
            "category_items": [],
            "direct_image_url": normalized_url,
        }

    text = body.decode("utf-8", errors="replace")
    parser = _WikiHtmlParser(normalized_url)
    parser.feed(text)
    return {
        "url": normalized_url,
        "role": source.role,
        "notes": source.notes,
        "title": _clean_text(parser.title) or Path(urlparse(normalized_url).path).stem or source.role,
        "content_type": content_type,
        "page_type": "html",
        "sections": parser.sections,
        "category_items": parser.category_items,
        "direct_image_url": "",
    }


def _build_ontology(game: str, source_records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    heroes: dict[str, dict[str, Any]] = {}
    abilities: dict[str, dict[str, Any]] = {}
    events: dict[str, dict[str, Any]] = {}

    for source in source_records:
        entity_kind = _ENTITY_ROLE_TO_KIND.get(source["role"])
        if entity_kind is None:
            continue
        names: list[str] = []
        for section in source.get("sections", []):
            names.extend(section.items)
        for item in source.get("category_items", []):
            names.append(item.name)
        for raw_name in names:
            normalized_name = _normalize_schema_name(raw_name)
            if not normalized_name:
                continue
            if entity_kind == "character_or_operator":
                hero_id = _canonical_id(normalized_name)
                heroes.setdefault(
                    hero_id,
                    {
                        "hero_id": hero_id,
                        "display_name": normalized_name,
                        "entity_type": entity_kind,
                        "source_page_url": source["url"],
                        "source_role": source["role"],
                    },
                )
            elif entity_kind == "ability_or_equipment":
                ability_id = _canonical_id(normalized_name)
                abilities.setdefault(
                    ability_id,
                    {
                        "ability_id": ability_id,
                        "display_name": normalized_name,
                        "entity_type": entity_kind,
                        "class": "unknown",
                        "source_page_url": source["url"],
                        "source_role": source["role"],
                    },
                )
            else:
                event_id = _canonical_id(normalized_name)
                events.setdefault(
                    event_id,
                    {
                        "event_id": event_id,
                        "display_name": normalized_name,
                        "entity_type": entity_kind,
                        "category": "unknown",
                        "source_page_url": source["url"],
                        "source_role": source["role"],
                    },
                )

    return {
        "heroes": sorted(heroes.values(), key=lambda row: row["hero_id"]),
        "abilities": sorted(abilities.values(), key=lambda row: row["ability_id"]),
        "events": sorted(events.values(), key=lambda row: row["event_id"]),
    }


def _collect_candidate_assets(source_records: list[dict[str, Any]], masters_root: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for source in source_records:
        if source["page_type"] == "direct_image" and source.get("direct_image_url"):
            row = _build_candidate_row(
                source=source,
                image_url=source["direct_image_url"],
                display_name=Path(urlparse(source["direct_image_url"]).path).stem,
                source_kind="direct_image",
            )
            candidates.append(_download_candidate(row, masters_root))
            continue

        for section in source.get("sections", []):
            for image in section.images:
                if image.src in seen_urls:
                    continue
                seen_urls.add(image.src)
                display_name = _normalize_candidate_name(image.alt, fallback=section.heading)
                if not display_name:
                    continue
                row = _build_candidate_row(
                    source=source,
                    image_url=image.src,
                    display_name=display_name,
                    source_kind="page_image",
                )
                candidates.append(_download_candidate(row, masters_root))
        for item in source.get("category_items", []):
            if not item.image_src or item.image_src in seen_urls:
                continue
            seen_urls.add(item.image_src)
            display_name = _normalize_candidate_name(item.image_alt, fallback=item.name)
            if not display_name:
                continue
            row = _build_candidate_row(
                source=source,
                image_url=item.image_src,
                display_name=display_name,
                source_kind="category_member_image",
            )
            candidates.append(_download_candidate(row, masters_root))
    return candidates


def _build_candidate_row(*, source: dict[str, Any], image_url: str, display_name: str, source_kind: str) -> dict[str, Any]:
    normalized_display_name = _normalize_schema_name(display_name)
    candidate_id = f"candidate_{hashlib.sha1(f'{source['url']}|{image_url}|{normalized_display_name}'.encode('utf-8')).hexdigest()[:12]}"
    asset_family = _source_role_asset_family(source["role"], normalized_display_name)
    return {
        "candidate_id": candidate_id,
        "display_name": normalized_display_name,
        "normalized_name": _canonical_id(normalized_display_name),
        "asset_family": asset_family,
        "source_url": image_url,
        "source_page_url": source["url"],
        "source_role": source["role"],
        "source_title": source["title"],
        "source_kind": source_kind,
        "notes": source.get("notes", ""),
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
        with urlopen(_build_fetch_target(source_url), timeout=_DEFAULT_TIMEOUT_SECONDS) as response:
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
    ontology: dict[str, list[dict[str, Any]]],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    bindings: list[dict[str, Any]] = []
    targets = (
        [("hero", row["hero_id"], row["display_name"], "hero_portrait") for row in ontology["heroes"]]
        + [("ability", row["ability_id"], row["display_name"], "ability_icon") for row in ontology["abilities"]]
        + [("event", row["event_id"], row["display_name"], "medal_icon") for row in ontology["events"]]
    )
    for target_kind, target_id, target_display_name, asset_family in targets:
        target_key = _binding_key(target_display_name)
        for candidate in candidates:
            if candidate["fetch_status"] != "downloaded":
                continue
            if candidate["asset_family"] != asset_family and candidate["source_role"] != "assets_reference":
                continue
            confidence, reason = _binding_confidence(
                target_key=target_key,
                candidate_key=_binding_key(candidate["display_name"]),
                target_display_name=target_display_name,
                candidate_display_name=candidate["display_name"],
                source_role=candidate["source_role"],
            )
            if confidence <= 0:
                continue
            bindings.append(
                {
                    "binding_id": f"binding_{hashlib.sha1(f'{target_id}|{candidate['candidate_id']}'.encode('utf-8')).hexdigest()[:12]}",
                    "game_id": game,
                    "target_kind": target_kind,
                    "target_id": target_id,
                    "target_display_name": target_display_name,
                    "candidate_id": candidate["candidate_id"],
                    "candidate_display_name": candidate["display_name"],
                    "source_url": candidate["source_url"],
                    "asset_family": asset_family,
                    "confidence": round(confidence, 2),
                    "reason": reason,
                    "status": "pending_review",
                }
            )
    bindings.sort(key=lambda row: (-float(row["confidence"]), row["target_id"], row["candidate_id"]))
    return bindings


def _build_qa_queue(
    ontology: dict[str, list[dict[str, Any]]],
    candidates: list[dict[str, Any]],
    bindings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    qa_rows: list[dict[str, Any]] = []
    binding_targets = {row["target_id"] for row in bindings}
    bound_candidates = {row["candidate_id"] for row in bindings}

    for hero in ontology["heroes"]:
        if hero["hero_id"] not in binding_targets:
            qa_rows.append(
                {
                    "item_type": "missing_binding",
                    "target_id": hero["hero_id"],
                    "display_name": hero["display_name"],
                    "status": "needs_better_reference",
                    "reason": "no candidate source image matched this hero",
                }
            )
    for ability in ontology["abilities"]:
        if ability["ability_id"] not in binding_targets:
            qa_rows.append(
                {
                    "item_type": "missing_binding",
                    "target_id": ability["ability_id"],
                    "display_name": ability["display_name"],
                    "status": "needs_better_reference",
                    "reason": "no candidate source image matched this ability",
                }
            )
    for event in ontology["events"]:
        if event["event_id"] not in binding_targets:
            qa_rows.append(
                {
                    "item_type": "missing_binding",
                    "target_id": event["event_id"],
                    "display_name": event["display_name"],
                    "status": "needs_better_reference",
                    "reason": "no candidate source image matched this event",
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
    for binding in bindings:
        qa_rows.append(
            {
                "item_type": "binding_candidate",
                "target_id": binding["target_id"],
                "display_name": binding["target_display_name"],
                "status": binding["status"],
                "reason": binding["reason"],
            }
        )
    return qa_rows


def _write_onboarding_artifacts(
    stage_root: Path,
    catalog_root: Path,
    *,
    game_payload: dict[str, Any],
    ontology: dict[str, list[dict[str, Any]]],
    hud: dict[str, Any],
    weights: dict[str, Any],
    candidates: list[dict[str, Any]],
    bindings: list[dict[str, Any]],
    qa_queue: list[dict[str, Any]],
    manifest_payload: dict[str, Any],
) -> None:
    manifests_root = stage_root / "manifests"
    manifests_root.mkdir(parents=True, exist_ok=True)
    dump_yaml_file(stage_root / "game.yaml", game_payload)
    dump_yaml_file(stage_root / "entities.yaml", ontology)
    dump_yaml_file(stage_root / "hud.yaml", hud)
    dump_yaml_file(stage_root / "weights.yaml", weights)
    dump_yaml_file(manifests_root / "cv_templates.yaml", {"templates": []})
    (manifests_root / "assets_manifest.json").write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")

    _write_csv(catalog_root / "heroes.csv", ontology["heroes"])
    _write_csv(catalog_root / "abilities.csv", ontology["abilities"])
    _write_csv(catalog_root / "events.csv", ontology["events"])
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


def _source_log_row(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_page_url": record["url"],
        "source_role": record["role"],
        "source_title": record["title"],
        "status": "fetched",
        "content_type": record["content_type"],
        "page_type": record["page_type"],
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


def _source_role_asset_family(role: str, display_name: str) -> str:
    if role == "roster":
        return "hero_portrait"
    if role == "abilities":
        if any(word in display_name.casefold() for word in ("grenade", "mine", "trap", "turret")):
            return "equipment_icon"
        return "ability_icon"
    if role in {"events", "medals"}:
        return "medal_icon"
    return "hud_icon"


def _normalize_schema_name(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""
    if cleaned.casefold() in {"contents", "overview", "trivia", "references"}:
        return ""
    return cleaned


def _normalize_candidate_name(value: str, *, fallback: str) -> str:
    return _normalize_schema_name(value or fallback)


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
    return _slugify(value).replace("-", "_")


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
