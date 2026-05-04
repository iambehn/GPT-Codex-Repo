from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from pipeline.asset_candidate_quality import analyze_asset_candidate
from pipeline.simple_yaml import dump_yaml_file
from pipeline.source_normalization import (
    SourceFetchError,
    build_fetch_target as _shared_build_fetch_target,
    fetch_source_record as _shared_fetch_source_record,
    image_anchor_text,
    normalize_source_url as _shared_normalize_source_url,
)
from pipeline.structured_source_fields import (
    aliases_equivalent,
    clean_text,
    extract_event_name,
    extract_structured_fields,
    find_explicit_listing_matches,
    find_explicit_listing_match,
    identity_keys,
    merge_aliases,
    reconcile_identity,
    slugify,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
_SECTION_GAME = "games"
_SECTION_ENTITIES = "entities"
_SECTION_ABILITIES = "abilities_or_equipment"
_SECTION_EVENTS = "events_or_medals"
_SECTION_ASSETS = "assets"
_SECTION_FETCH_LOG = "source_fetch_log"
_SECTION_QA = "qa_queue"
_CSV_TABLES = (
    _SECTION_GAME,
    _SECTION_ENTITIES,
    _SECTION_ABILITIES,
    _SECTION_EVENTS,
    _SECTION_ASSETS,
    _SECTION_FETCH_LOG,
    _SECTION_QA,
)
_ASSET_FAMILY_DEFAULTS = {
    "character_or_operator": "hero_portrait",
    "ability_or_equipment": "equipment_icon",
    "event_badge_or_medal": "medal_icon",
}
_EVENT_SECTION_HINTS = ("medal", "event", "badge", "killstreak")
_ABILITY_SECTION_HINTS = ("ability", "equipment", "perk", "loadout", "item")
_ENTITY_SECTION_HINTS = ("operator", "character", "hero")
_NEGATIVE_SECTION_HINTS = (
    "overview",
    "contents",
    "trivia",
    "references",
    "external links",
    "see also",
    "gallery",
    "navigation",
)
_DEFAULT_TIMEOUT_SECONDS = 20


@dataclass
class ParsedImage:
    src: str
    alt: str
    section: str
    section_text: str


@dataclass
class ParsedSection:
    section_key: str
    heading: str
    items: list[str] = field(default_factory=list)
    images: list[ParsedImage] = field(default_factory=list)


@dataclass
class ParsedCategoryItem:
    name: str
    href: str
    image_src: str = ""
    image_alt: str = ""


@dataclass(frozen=True)
class WikiSource:
    url: str
    role: str


WikiFetchError = SourceFetchError


def _normalize_source(source: WikiSource) -> WikiSource:
    return WikiSource(url=_normalize_source_url(source.url), role=source.role)


class _WikiHtmlParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.title = ""
        self._current_heading_tag = ""
        self._current_heading_text = ""
        self._current_text_parts: list[str] = []
        self._current_section = ParsedSection(section_key="overview", heading="Overview")
        self.sections: list[ParsedSection] = [self._current_section]
        self._capture_li = False
        self._capture_title = False
        self._article_detected = False
        self._article_depth = 0
        self._ignored_depth = 0
        self._tag_flags: list[tuple[str, bool, bool, bool, bool]] = []
        self.category_items: list[ParsedCategoryItem] = []
        self._category_listing_depth = 0
        self._category_member_depth = 0
        self._category_anchor_href = ""
        self._capture_category_anchor = False
        self._current_category_anchor_text = ""
        self._current_category_member_name = ""
        self._current_category_member_href = ""
        self._current_category_member_image_src = ""
        self._current_category_member_image_alt = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        enters_article = _is_article_container(tag, attr_map)
        enters_ignored = _is_ignored_container(tag, attr_map)
        enters_category_listing = _is_category_listing_container(tag, attr_map)
        enters_category_member = _is_category_member_container(tag, attr_map)

        if enters_article:
            if not self._article_detected:
                self._article_detected = True
                self._reset_sections()
            self._article_depth += 1

        if enters_ignored and self._capture_content_enabled():
            self._ignored_depth += 1

        self._tag_flags.append((tag, enters_article, enters_ignored, enters_category_listing, enters_category_member))

        if enters_category_listing:
            self._category_listing_depth += 1
        if enters_category_member:
            if self._category_member_depth == 0:
                self._reset_current_category_member()
            self._category_member_depth += 1

        if tag in {"h1", "h2", "h3"} and self._capture_content_enabled():
            self._current_heading_tag = tag
            self._current_heading_text = ""
        elif tag == "li" and self._capture_content_enabled():
            self._capture_li = True
            self._current_text_parts = []
        elif tag == "title":
            self._capture_title = True
        elif tag == "img" and self._capture_content_enabled():
            src = _image_source_from_attrs(attr_map)
            alt = (attr_map.get("alt") or "").strip()
            if self._should_capture_category_member_image() and src:
                self._current_category_member_image_src = urljoin(self.base_url, src)
                self._current_category_member_image_alt = alt
            if src:
                absolute_src = urljoin(self.base_url, src)
                self._current_section.images.append(
                    ParsedImage(
                        src=absolute_src,
                        alt=alt,
                        section=self._current_section.section_key,
                        section_text=self._current_section.heading,
                    )
                )
        elif tag == "a" and self._should_capture_category_anchor(attr_map):
            self._capture_category_anchor = True
            self._category_anchor_href = urljoin(self.base_url, attr_map.get("href") or "")
            self._current_category_anchor_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == self._current_heading_tag and self._current_heading_tag:
            heading = clean_text(self._current_heading_text)
            if heading:
                section_key = slugify(heading)
                self._current_section = ParsedSection(section_key=section_key, heading=heading)
                self.sections.append(self._current_section)
            self._current_heading_tag = ""
            self._current_heading_text = ""
        elif tag == "li" and self._capture_li:
            text = clean_text(" ".join(self._current_text_parts))
            if text:
                self._current_section.items.append(text)
            self._capture_li = False
            self._current_text_parts = []
        elif tag == "title":
            self._capture_title = False
        elif tag == "a" and self._capture_category_anchor:
            item_name = clean_text(self._current_category_anchor_text)
            if item_name and self._category_anchor_href and not _should_ignore_category_row(item_name):
                self._current_category_member_name = item_name
                self._current_category_member_href = self._category_anchor_href
                if self._category_member_depth <= 0:
                    self._finalize_category_member()
            self._capture_category_anchor = False
            self._category_anchor_href = ""
            self._current_category_anchor_text = ""

        while self._tag_flags:
            open_tag, enters_article, enters_ignored, enters_category_listing, enters_category_member = self._tag_flags.pop()
            if enters_ignored and self._ignored_depth > 0:
                self._ignored_depth -= 1
            if enters_article and self._article_depth > 0:
                self._article_depth -= 1
            if enters_category_member and self._category_member_depth > 0:
                self._category_member_depth -= 1
                if self._category_member_depth == 0:
                    self._finalize_category_member()
            if enters_category_listing and self._category_listing_depth > 0:
                self._category_listing_depth -= 1
            if open_tag == tag:
                break

    def handle_data(self, data: str) -> None:
        if self._current_heading_tag and self._capture_content_enabled():
            self._current_heading_text += data
        if self._capture_li and self._capture_content_enabled():
            self._current_text_parts.append(data)
        if self._capture_title:
            self.title += data
        if self._capture_category_anchor:
            self._current_category_anchor_text += data

    def _capture_content_enabled(self) -> bool:
        if self._article_detected:
            return self._article_depth > 0 and self._ignored_depth == 0
        return self._ignored_depth == 0

    def _reset_sections(self) -> None:
        self._current_section = ParsedSection(section_key="overview", heading="Overview")
        self.sections = [self._current_section]
        self._current_heading_tag = ""
        self._current_heading_text = ""
        self._current_text_parts = []
        self._capture_li = False

    def _should_capture_category_anchor(self, attrs: dict[str, str | None]) -> bool:
        if self._category_listing_depth <= 0 and self._category_member_depth <= 0:
            return False
        href = (attrs.get("href") or "").strip()
        if not href:
            return False
        classes = _attr_tokens(attrs.get("class"))
        if "category-page__member-link" in classes:
            return True
        return self._category_member_depth > 0

    def _should_capture_category_member_image(self) -> bool:
        return self._category_member_depth > 0

    def _finalize_category_member(self) -> None:
        if self._current_category_member_name and self._current_category_member_href:
            self.category_items.append(
                ParsedCategoryItem(
                    name=self._current_category_member_name,
                    href=self._current_category_member_href,
                    image_src=self._current_category_member_image_src,
                    image_alt=self._current_category_member_image_alt,
                )
            )
        self._reset_current_category_member()

    def _reset_current_category_member(self) -> None:
        self._current_category_member_name = ""
        self._current_category_member_href = ""
        self._current_category_member_image_src = ""
        self._current_category_member_image_alt = ""


def enrich_game_from_wiki(game: str, wiki_url: str, *, repo_root: Path | None = None) -> dict[str, Any]:
    return enrich_game_from_sources(game, [WikiSource(url=wiki_url, role="overview")], repo_root=repo_root)


def enrich_game_from_sources(game: str, sources: list[WikiSource], *, repo_root: Path | None = None) -> dict[str, Any]:
    repo_root = (repo_root or REPO_ROOT).resolve()
    normalized_sources = [_normalize_source(source) for source in sources]
    timestamp = _timestamp_slug()
    drafts_parent = repo_root / "assets" / "games" / game / "drafts" / "wiki"
    drafts_parent.mkdir(parents=True, exist_ok=True)
    draft_root = drafts_parent / timestamp
    stage_root = Path(tempfile.mkdtemp(prefix=f".{timestamp}.", dir=drafts_parent))
    catalog_root = stage_root / "catalog"
    downloads_root = stage_root / "downloads"
    templates_root = stage_root / "templates"
    masks_root = stage_root / "masks"
    for path in (catalog_root, downloads_root, templates_root, masks_root):
        path.mkdir(parents=True, exist_ok=True)

    try:
        source_records: list[dict[str, Any]] = []
        source_failures: list[dict[str, Any]] = []
        first_failure: WikiFetchError | None = None
        for source in normalized_sources:
            try:
                source_records.append(_fetch_source_record(source.url, source.role))
            except WikiFetchError as exc:
                if first_failure is None:
                    first_failure = exc
                source_failures.append(_source_failure_row(source, exc))
        if not source_records and first_failure is not None:
            raise first_failure
        normalized = _normalize_records(game, source_records)
        normalized[_SECTION_FETCH_LOG].extend(source_failures)
        normalized[_SECTION_QA].extend(
            {
                "source_page_url": row["source_page_url"],
                "source_role": row["source_role"],
                "qa_status": row["status"],
                "display_name": row["source_role"],
                "candidate_quality": "",
                "source_url": row["source_page_url"],
            }
            for row in source_failures
        )
        _download_assets(normalized["assets"], downloads_root, templates_root)
        _write_catalog_csvs(catalog_root, normalized)
        _write_draft_artifacts(stage_root, game, normalized)
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
        "wiki_url": normalized_sources[0].url if len(normalized_sources) == 1 else None,
        "draft_root": str(draft_root),
        "catalog_root": str(draft_root / "catalog"),
        "downloads_root": str(draft_root / "downloads"),
        "templates_root": str(draft_root / "templates"),
        "masks_root": str(draft_root / "masks"),
        "source_count": len(source_records),
        "sources": [{"url": source.url, "role": source.role} for source in normalized_sources],
        "counts": {
            "games": len(normalized[_SECTION_GAME]),
            "entities": len(normalized[_SECTION_ENTITIES]),
            "abilities_or_equipment": len(normalized[_SECTION_ABILITIES]),
            "events_or_medals": len(normalized[_SECTION_EVENTS]),
            "assets": len(normalized[_SECTION_ASSETS]),
            "qa_queue": len(normalized[_SECTION_QA]),
        },
        "artifacts": {
            "game_draft": str(draft_root / "game.draft.yaml"),
            "entities_draft": str(draft_root / "entities.draft.yaml"),
            "abilities_draft": str(draft_root / "abilities.draft.yaml"),
            "events_draft": str(draft_root / "events.draft.yaml"),
            "assets_manifest": str(draft_root / "assets_manifest.json"),
        },
    }


def _fetch_source_record(url: str, role: str) -> dict[str, Any]:
    return _shared_fetch_source_record(url, role)


def _build_fetch_target(url: str) -> str | Request:
    return _shared_build_fetch_target(url)


def _fetch_hint_for_status(status_code: int) -> str:
    from pipeline.source_normalization import fetch_hint_for_status

    return fetch_hint_for_status(status_code)


def _normalize_records(game: str, source_records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    games = [{
        "game_id": game,
        "display_name": _display_name_for_game(game),
        "seed_adapter": "call_of_duty",
        "primary_source_kind": "explicit_urls",
    }]
    entities_by_id: dict[str, dict[str, Any]] = {}
    abilities_by_id: dict[str, dict[str, Any]] = {}
    events_by_id: dict[str, dict[str, Any]] = {}
    assets: list[dict[str, Any]] = []
    fetch_log: list[dict[str, Any]] = []
    seen_assets: set[tuple[str, str]] = set()
    merge_qa: list[dict[str, Any]] = []

    for source in source_records:
        sections: list[ParsedSection] = source["sections"]
        article_container_detected = bool(source.get("article_container_detected", False))
        source_known_names: dict[str, set[str]] = {
            "character_or_operator": set(),
            "ability_or_equipment": set(),
            "event_badge_or_medal": set(),
        }
        fetch_log.append(
            {
                "source_page_url": source["url"],
                "source_role": source["role"],
                "source_title": source["title"],
                "source_scheme": source["source_scheme"],
                "page_type": source["page_type"],
                "status": _source_record_status(source),
                "content_type": source["content_type"],
                "section_count": len(sections),
            }
        )

        if source["page_type"] == "category":
            _normalize_category_source(
                game,
                source,
                entities_by_id,
                abilities_by_id,
                events_by_id,
                assets,
                seen_assets,
                source_known_names,
                merge_qa,
            )
            merge_qa.extend(_enrich_category_rows_from_detail_sections(
                game,
                source,
                source_known_names,
                entities_by_id,
                abilities_by_id,
                events_by_id,
            ))
            continue

        for section in sections:
            record_type = _classify_section(section.heading, source["role"])
            for item_text in section.items:
                item_name = _normalize_item_name(item_text, record_type=record_type, source_role=source["role"], section_heading=section.heading)
                if not item_name or _should_ignore_row_text(item_name):
                    continue
                if record_type is not None:
                    source_known_names[record_type].add(item_name)
                _upsert_schema_row(
                    game,
                    source,
                    section.heading,
                    record_type,
                    item_name,
                    entities_by_id,
                    abilities_by_id,
                    events_by_id,
                    merge_qa,
                    source_text=item_text,
                )

            for image in section.images:
                record_type = _classify_section(image.section_text, source["role"])
                image_section_heading = image.section_text
                if bool(getattr(image, "infobox_like", False)):
                    record_type = _record_type_for_role(source["role"])
                    image_section_heading = str(source.get("role", "")) or image.section_text
                elif bool(getattr(image, "gallery_like", False)):
                    if record_type is None:
                        record_type = _record_type_for_role(source["role"])
                    image_section_heading = image_anchor_text(image) or image.section_text
                if record_type is None:
                    continue
                anchor_strength = str(getattr(image, "anchor_strength", "weak"))
                candidate_name = _normalize_item_name(
                    image_anchor_text(image),
                    record_type=record_type,
                    source_role=source["role"],
                    section_heading=image_section_heading,
                )
                if not candidate_name or _should_ignore_row_text(candidate_name):
                    continue
                display_name = _bind_image_to_known_name(
                    candidate_name,
                    source_known_names.get(record_type, set()),
                    record_type=record_type,
                    source_role=source["role"],
                )
                if not display_name:
                    continue
                if not _should_keep_image(
                    image.src,
                    candidate_name,
                    source_role=source["role"],
                    section_heading=image_section_heading,
                ):
                    continue
                asset_key = (display_name.casefold(), image.src)
                if asset_key in seen_assets:
                    continue
                seen_assets.add(asset_key)
                allow_image_row = anchor_strength in {"strong", "medium"} and not bool(getattr(image, "infobox_like", False))
                if bool(getattr(image, "gallery_like", False)):
                    allow_image_row = anchor_strength == "strong"
                    if not article_container_detected:
                        allow_image_row = bool(getattr(image, "paragraph_backed", False)) and anchor_strength == "medium"
                elif not article_container_detected:
                    allow_image_row = (
                        (bool(getattr(image, "captioned", False)) and anchor_strength == "strong")
                        or (bool(getattr(image, "paragraph_backed", False)) and anchor_strength == "medium")
                    )
                if allow_image_row:
                    _upsert_schema_row(
                        game,
                        source,
                        image_section_heading,
                        record_type,
                        display_name,
                        entities_by_id,
                        abilities_by_id,
                        events_by_id,
                        merge_qa,
                        source_text=image_anchor_text(image),
                    )
                _append_asset_row(
                    assets,
                    game=game,
                    source=source,
                    record_type=record_type,
                    display_name=display_name,
                    source_url=image.src,
                    source_kind=(
                        "infobox_image"
                        if bool(getattr(image, "infobox_like", False))
                        else "gallery_image"
                        if bool(getattr(image, "gallery_like", False))
                        else "page_image"
                    ),
                    section_heading=image_section_heading,
                    raw_label=image_anchor_text(image) or candidate_name,
                    anchor_source=str(getattr(image, "anchor_source", "")),
                    anchor_ambiguous=bool(getattr(image, "paragraph_ambiguous", False)),
                    anchor_ambiguity_type=str(getattr(image, "anchor_ambiguity_type", "")),
                    paragraph_referential=bool(getattr(image, "paragraph_referential", False)),
                )

    qa_queue = [
        {
            "asset_id": asset["asset_id"],
            "display_name": asset["display_name"],
            "qa_status": asset["qa_status"],
            "candidate_quality": asset.get("candidate_quality", ""),
            "source_url": asset["source_url"],
            "source_role": asset["source_role"],
            "source_page_url": asset["source_page_url"],
        }
        for asset in assets
        if asset["qa_status"] != "verified_source" or asset.get("candidate_quality") == "low"
    ]
    qa_queue.extend(
        {
            "asset_id": asset["asset_id"],
            "display_name": asset["display_name"],
            "qa_status": "filename_only_anchor",
            "candidate_quality": asset.get("candidate_quality", ""),
            "source_url": asset["source_url"],
            "source_role": asset["source_role"],
            "source_page_url": asset["source_page_url"],
        }
        for asset in assets
        if str(asset.get("anchor_source", "")).strip() == "filename"
    )
    qa_queue.extend(
        {
            "asset_id": asset["asset_id"],
            "display_name": asset["display_name"],
            "qa_status": (
                "surrounding_paragraph_ambiguous_anchor"
                if str(asset.get("anchor_ambiguity_type", "")) == "surrounding_paragraph"
                else "cross_paragraph_ambiguous_anchor"
                if str(asset.get("anchor_ambiguity_type", "")) == "cross_paragraph"
                else "ambiguous_paragraph_anchor"
            ),
            "candidate_quality": asset.get("candidate_quality", ""),
            "source_url": asset["source_url"],
            "source_role": asset["source_role"],
            "source_page_url": asset["source_page_url"],
        }
        for asset in assets
        if bool(asset.get("anchor_ambiguous", False))
    )
    qa_queue.extend(
        {
            "asset_id": asset["asset_id"],
            "display_name": asset["display_name"],
            "qa_status": "referential_paragraph_anchor",
            "candidate_quality": asset.get("candidate_quality", ""),
            "source_url": asset["source_url"],
            "source_role": asset["source_role"],
            "source_page_url": asset["source_page_url"],
        }
        for asset in assets
        if bool(asset.get("paragraph_referential", False))
    )
    asset_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for asset in assets:
        group_key = (str(asset.get("binding_key", "")), str(asset.get("asset_family", "")))
        asset_groups.setdefault(group_key, []).append(asset)
    qa_queue.extend(
        {
            "asset_id": grouped[0]["asset_id"],
            "display_name": grouped[0]["display_name"],
            "qa_status": "infobox_only_candidate",
            "candidate_quality": grouped[0].get("candidate_quality", ""),
            "source_url": grouped[0]["source_url"],
            "source_role": grouped[0]["source_role"],
            "source_page_url": grouped[0]["source_page_url"],
        }
        for grouped in asset_groups.values()
        if {str(row.get("source_kind", "")) for row in grouped} == {"infobox_image"}
    )
    qa_queue.extend(
        {
            "asset_id": grouped[0]["asset_id"],
            "display_name": grouped[0]["display_name"],
            "qa_status": "gallery_only_candidate",
            "candidate_quality": grouped[0].get("candidate_quality", ""),
            "source_url": grouped[0]["source_url"],
            "source_role": grouped[0]["source_role"],
            "source_page_url": grouped[0]["source_page_url"],
        }
        for grouped in asset_groups.values()
        if {str(row.get("source_kind", "")) for row in grouped} == {"gallery_image"}
    )
    qa_queue.extend(merge_qa)

    return {
        _SECTION_GAME: games,
        _SECTION_ENTITIES: sorted(entities_by_id.values(), key=lambda row: row["entity_id"]),
        _SECTION_ABILITIES: sorted(abilities_by_id.values(), key=lambda row: row["ability_id"]),
        _SECTION_EVENTS: sorted(events_by_id.values(), key=lambda row: row["event_id"]),
        _SECTION_ASSETS: assets,
        _SECTION_FETCH_LOG: fetch_log,
        _SECTION_QA: qa_queue,
    }


def _download_assets(assets: list[dict[str, Any]], downloads_root: Path, templates_root: Path) -> None:
    for asset in assets:
        source_url = asset["source_url"]
        extension = _extension_for_url(source_url)
        asset_dir = _relative_asset_dir(asset)
        download_path = downloads_root / asset_dir / f"{_slugify(asset['display_name'])}{extension}"
        template_path = templates_root / asset_dir / f"{_slugify(asset['display_name'])}{extension}"
        download_path.parent.mkdir(parents=True, exist_ok=True)
        template_path.parent.mkdir(parents=True, exist_ok=True)

        if _looks_like_direct_asset_url(source_url):
            with urlopen(source_url) as response:
                data = response.read()
            download_path.write_bytes(data)
            shutil.copyfile(download_path, template_path)
            asset["draft_local_path"] = str(download_path)
            asset["template_path"] = str(template_path)
            asset["qa_status"] = "verified_source"
        else:
            asset["draft_local_path"] = ""
            asset["template_path"] = ""


def _write_catalog_csvs(catalog_root: Path, normalized: dict[str, list[dict[str, Any]]]) -> None:
    for table_name in _CSV_TABLES:
        rows = normalized.get(table_name, [])
        headers = _headers_for_rows(rows)
        path = catalog_root / f"{table_name}.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow(_stringify_row(row, headers))


def _write_draft_artifacts(draft_root: Path, game: str, normalized: dict[str, list[dict[str, Any]]]) -> None:
    dump_yaml_file(
        draft_root / "game.draft.yaml",
        normalized[_SECTION_GAME][0],
    )
    dump_yaml_file(
        draft_root / "entities.draft.yaml",
        {"characters": [
            {
                "id": row["entity_id"],
                "display_name": row["display_name"],
                "role": row.get("role", ""),
                "aliases": row.get("aliases", []),
                "source_page_url": row["source_page_url"],
            }
            for row in normalized[_SECTION_ENTITIES]
        ]},
    )
    dump_yaml_file(
        draft_root / "abilities.draft.yaml",
        {"abilities": [
            {
                "id": row["ability_id"],
                "display_name": row["display_name"],
                "class": row.get("class", ""),
                "aliases": row.get("aliases", []),
                "source_page_url": row["source_page_url"],
            }
            for row in normalized[_SECTION_ABILITIES]
        ]},
    )
    dump_yaml_file(
        draft_root / "events.draft.yaml",
        {"events": [
            {
                "id": row["event_id"],
                "display_name": row["display_name"],
                "category": row.get("category", ""),
                "aliases": row.get("aliases", []),
                "source_page_url": row["source_page_url"],
            }
            for row in normalized[_SECTION_EVENTS]
        ]},
    )
    manifest_payload = {
        "game_id": game,
        "source_count": len(normalized[_SECTION_FETCH_LOG]),
        "assets": normalized[_SECTION_ASSETS],
        "qa_queue": normalized[_SECTION_QA],
    }
    (draft_root / "assets_manifest.json").write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")


def _normalize_category_source(
    game: str,
    source: dict[str, Any],
    entities_by_id: dict[str, dict[str, Any]],
    abilities_by_id: dict[str, dict[str, Any]],
    events_by_id: dict[str, dict[str, Any]],
    assets: list[dict[str, Any]],
    seen_assets: set[tuple[str, str]],
    source_known_names: dict[str, set[str]],
    qa_rows: list[dict[str, Any]],
) -> None:
    record_type = _record_type_for_role(source["role"])
    if record_type not in {"character_or_operator", "ability_or_equipment", "event_badge_or_medal"}:
        return
    for item in source.get("category_items", []):
        item_name = _normalize_item_name(
            item.name,
            record_type=record_type,
            source_role=source["role"],
            section_heading="Category",
        )
        if not item_name or _should_ignore_row_text(item_name) or _should_ignore_category_row(item_name):
            continue
        source_known_names[record_type].add(item_name)
        _upsert_schema_row(
            game,
            source,
            "Category",
            record_type,
            item_name,
            entities_by_id,
            abilities_by_id,
            events_by_id,
            qa_rows,
            source_text=item.name,
        )
        if not item.image_src:
            continue
        if not _should_keep_image(
            item.image_src,
            item.image_alt or item_name,
            source_role=source["role"],
            section_heading="Category",
        ):
            continue
        asset_key = (item_name.casefold(), item.image_src)
        if asset_key in seen_assets:
            continue
        seen_assets.add(asset_key)
        _append_asset_row(
            assets,
            game=game,
            source=source,
            record_type=record_type,
            display_name=item_name,
            source_url=item.image_src,
            source_kind="category_member_image",
            section_heading="Category",
            raw_label=item.image_alt or item_name,
        )


def _upsert_schema_row(
    game: str,
    source: dict[str, Any],
    section_heading: str,
    record_type: str | None,
    display_name: str,
    entities_by_id: dict[str, dict[str, Any]],
    abilities_by_id: dict[str, dict[str, Any]],
    events_by_id: dict[str, dict[str, Any]],
    qa_rows: list[dict[str, Any]],
    *,
    source_text: str | None = None,
) -> None:
    if record_type is None:
        return
    structured = extract_structured_fields(
        source_text or display_name,
        source_role=source["role"],
        section_heading=section_heading,
        record_type=record_type,
    )
    row = {
        "game_id": game,
        "display_name": structured["display_name"] or display_name,
        "source_page_url": source["url"],
        "source_role": source["role"],
        "source_title": source["title"],
        "section_heading": section_heading,
        "aliases": list(structured.get("aliases", [])),
        "aliases_source": structured.get("aliases_source", ""),
    }
    merged_aliases, alias_rejections = merge_aliases([], row["aliases"], canonical_name=row["display_name"])
    row["aliases"] = merged_aliases
    for rejection in alias_rejections:
        qa_rows.append(
            {
                "asset_id": "",
                "display_name": row["display_name"],
                "qa_status": rejection["status"],
                "candidate_quality": "",
                "source_url": source["url"],
                "source_role": source["role"],
                "source_page_url": source["url"],
                "alias": rejection["alias"],
            }
        )
    row["canonical_display_name_source"] = "source"
    row["canonical_identity_basis"] = "source_initial"
    candidate_row = {
        "display_name": row["display_name"],
        "canonical_id": _asset_entity_id(game, row["display_name"]),
        "aliases": [str(item) for item in row.get("aliases", []) if str(item).strip()],
    }
    if record_type == "character_or_operator":
        rows_by_id = entities_by_id
        id_field = "entity_id"
        defaults = {
            "entity_type": "character_or_operator",
            "role": structured.get("role", ""),
            "role_source": structured.get("role_source", ""),
        }
        field_kwargs = {"role": structured.get("role", ""), "role_source": structured.get("role_source", "")}
    elif record_type == "ability_or_equipment":
        rows_by_id = abilities_by_id
        id_field = "ability_id"
        defaults = {
            "class": structured.get("class", ""),
            "class_source": structured.get("class_source", ""),
        }
        field_kwargs = {"class_value": structured.get("class", ""), "class_source": structured.get("class_source", "")}
    else:
        rows_by_id = events_by_id
        id_field = "event_id"
        defaults = {
            "category": structured.get("category", ""),
            "category_source": structured.get("category_source", ""),
        }
        field_kwargs = {"category": structured.get("category", ""), "category_source": structured.get("category_source", "")}

    existing_key, overlapping_keys = _find_existing_schema_row(rows_by_id, candidate_row=candidate_row, id_field=id_field)
    if len(overlapping_keys) > 1:
        qa_rows.append(
            {
                "asset_id": "",
                "display_name": row["display_name"],
                "qa_status": "ambiguous_identity_match",
                "candidate_quality": "",
                "source_url": source["url"],
                "source_role": source["role"],
                "source_page_url": source["url"],
                "candidate_ids": ",".join(overlapping_keys),
            }
        )
        existing_key = None

    if existing_key is None:
        canonical_id = candidate_row["canonical_id"]
        rows_by_id[canonical_id] = {
            id_field: canonical_id,
            **defaults,
            **row,
        }
        existing = rows_by_id[canonical_id]
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
            qa_rows.append(
                {
                    "asset_id": "",
                    "display_name": str(existing.get("display_name", row["display_name"])),
                    "qa_status": identity["match_status"],
                    "candidate_quality": "",
                    "source_url": source["url"],
                    "source_role": source["role"],
                    "source_page_url": source["url"],
                }
            )
        elif identity["match_status"] == "canonical_identity_preference_applied":
            old_id = str(existing.get(id_field, ""))
            new_id = str(identity["chosen_canonical_id"])
            existing["display_name"] = str(identity["chosen_display_name"])
            existing[id_field] = new_id
            existing["canonical_display_name_source"] = "source"
            existing["canonical_identity_basis"] = str(identity.get("basis", ""))
            if new_id != old_id:
                rows_by_id.pop(old_id, None)
                rows_by_id[new_id] = existing
            qa_rows.append(
                {
                    "asset_id": "",
                    "display_name": str(existing.get("display_name", "")),
                    "qa_status": "canonical_identity_preference_applied",
                    "candidate_quality": "",
                    "source_url": source["url"],
                    "source_role": source["role"],
                    "source_page_url": source["url"],
                }
            )

    _merge_additive_row_fields(
        existing,
        row,
        qa_rows=qa_rows,
        source=source,
        **field_kwargs,
    )


def _merge_additive_row_fields(
    existing: dict[str, Any],
    row: dict[str, Any],
    *,
    qa_rows: list[dict[str, Any]],
    source: dict[str, Any],
    role: str = "",
    role_source: str = "",
    class_value: str = "",
    class_source: str = "",
    category: str = "",
    category_source: str = "",
) -> None:
    if not existing.get("source_page_url"):
        existing["source_page_url"] = row.get("source_page_url", "")
    if not existing.get("source_role"):
        existing["source_role"] = row.get("source_role", "")
    if not existing.get("source_title"):
        existing["source_title"] = row.get("source_title", "")
    if not existing.get("section_heading"):
        existing["section_heading"] = row.get("section_heading", "")
    if row.get("aliases"):
        merged_aliases, alias_rejections = merge_aliases(
            [str(item) for item in existing.get("aliases", []) if str(item).strip()],
            [str(item) for item in row["aliases"]],
            canonical_name=str(existing.get("display_name", row.get("display_name", ""))),
        )
        if merged_aliases:
            existing["aliases"] = merged_aliases
            if not existing.get("aliases_source"):
                existing["aliases_source"] = row.get("aliases_source", "")
        for rejection in alias_rejections:
            qa_rows.append(
                {
                    "asset_id": "",
                    "display_name": str(existing.get("display_name", row.get("display_name", ""))),
                    "qa_status": rejection["status"],
                    "candidate_quality": "",
                    "source_url": source["url"],
                    "source_role": source["role"],
                    "source_page_url": source["url"],
                    "alias": rejection["alias"],
                    "target_id": existing.get("entity_id", existing.get("ability_id", existing.get("event_id", ""))),
                }
            )
    if role and not existing.get("role"):
        existing["role"] = role
        existing["role_source"] = role_source
    if class_value and not existing.get("class"):
        existing["class"] = class_value
        existing["class_source"] = class_source
    if category and not existing.get("category"):
        existing["category"] = category
        existing["category_source"] = category_source


def _find_existing_schema_row(
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


def _enrich_category_rows_from_detail_sections(
    game: str,
    source: dict[str, Any],
    source_known_names: dict[str, set[str]],
    entities_by_id: dict[str, dict[str, Any]],
    abilities_by_id: dict[str, dict[str, Any]],
    events_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    role = str(source.get("role", ""))
    record_type = _record_type_for_role(role)
    if record_type not in {"character_or_operator", "ability_or_equipment", "event_badge_or_medal"}:
        return []
    candidate_names = sorted(source_known_names.get(record_type, set()))
    if not candidate_names:
        return []
    qa_rows: list[dict[str, Any]] = []
    detail_contributions: dict[str, list[dict[str, Any]]] = {}
    for section in source.get("sections", []):
        for paragraph_text in getattr(section, "paragraphs", []):
            matched_names = find_explicit_listing_matches(paragraph_text, candidate_names)
            if len(matched_names) > 1:
                qa_rows.append(
                    {
                        "asset_id": "",
                        "display_name": "",
                        "qa_status": "ambiguous_listing_detail_enrichment",
                        "candidate_quality": "",
                        "source_url": source["url"],
                        "source_role": role,
                        "source_page_url": source["url"],
                    }
                )
                continue
            matched_name = find_explicit_listing_match(paragraph_text, candidate_names)
            if not matched_name:
                continue
            structured = extract_structured_fields(
                paragraph_text,
                source_role=role,
                section_heading=str(getattr(section, "heading", "")),
                record_type=record_type,
            )
            detail_contributions.setdefault(matched_name, []).append(
                {
                    "aliases": list(structured.get("aliases", [])),
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
        canonical_id = _asset_entity_id(game, matched_name)
        if record_type == "character_or_operator":
            target_row = entities_by_id.get(canonical_id)
        elif record_type == "ability_or_equipment":
            target_row = abilities_by_id.get(canonical_id)
        else:
            target_row = events_by_id.get(canonical_id)
        if target_row is None:
            continue
        conflict_fields = _resolve_detail_conflict_fields(
            contributions,
            target_row=target_row,
            qa_rows=qa_rows,
            source=source,
            display_name=matched_name,
        )
        conflict_fields = _append_existing_row_detail_conflicts(
            contributions,
            conflict_fields=conflict_fields,
            target_row=target_row,
            qa_rows=qa_rows,
            source=source,
            display_name=matched_name,
        )
        merged_aliases = _merge_nonconflicting_detail_aliases(
            contributions,
            conflict_fields,
            target_row=target_row,
            qa_rows=qa_rows,
            source=source,
            display_name=matched_name,
        )
        _merge_additive_row_fields(
            target_row,
            {
                "aliases": merged_aliases,
                "aliases_source": "source" if merged_aliases else "",
            },
            qa_rows=qa_rows,
            source=source,
            role=_resolve_consistent_detail_field(contributions, "role", target_row=target_row),
            role_source="source",
            class_value=_resolve_consistent_detail_field(contributions, "class", target_row=target_row),
            class_source="source",
            category=_resolve_consistent_detail_field(contributions, "category", target_row=target_row),
            category_source="source",
        )
    return qa_rows


def _resolve_detail_conflict_fields(
    contributions: list[dict[str, Any]],
    *,
    target_row: dict[str, Any],
    qa_rows: list[dict[str, Any]],
    source: dict[str, Any],
    display_name: str,
) -> set[str]:
    conflict_fields: set[str] = set()
    for field_name in ("role", "class", "category"):
        values = sorted({clean_text(str(row.get(field_name, ""))) for row in contributions if clean_text(str(row.get(field_name, "")))})
        if len(values) <= 1:
            continue
        conflict_fields.add(field_name)
        qa_rows.append(
            {
                "asset_id": "",
                "display_name": display_name,
                "qa_status": "conflicting_listing_detail_enrichment",
                "candidate_quality": "",
                "source_url": source["url"],
                "source_role": source["role"],
                "source_page_url": source["url"],
                "field": field_name,
                "candidate_values": values,
                "target_id": target_row.get("id", ""),
            }
        )
    return conflict_fields


def _append_existing_row_detail_conflicts(
    contributions: list[dict[str, Any]],
    *,
    conflict_fields: set[str],
    target_row: dict[str, Any],
    qa_rows: list[dict[str, Any]],
    source: dict[str, Any],
    display_name: str,
) -> set[str]:
    merged_conflict_fields = set(conflict_fields)
    for field_name in ("role", "class", "category"):
        detail_value = _resolve_consistent_detail_field(contributions, field_name)
        existing_value = clean_text(str(target_row.get(field_name, "")))
        if not detail_value or not existing_value or detail_value == existing_value:
            continue
        merged_conflict_fields.add(field_name)
        qa_rows.append(
            {
                "asset_id": "",
                "display_name": display_name,
                "qa_status": "existing_listing_detail_enrichment_conflict",
                "candidate_quality": "",
                "source_url": source["url"],
                "source_role": source["role"],
                "source_page_url": source["url"],
                "field": field_name,
                "candidate_values": [detail_value],
                "existing_value": existing_value,
                "target_id": target_row.get("id", ""),
            }
        )
    return merged_conflict_fields


def _merge_nonconflicting_detail_aliases(
    contributions: list[dict[str, Any]],
    conflict_fields: set[str],
    *,
    target_row: dict[str, Any],
    qa_rows: list[dict[str, Any]],
    source: dict[str, Any],
    display_name: str,
) -> list[str]:
    merged_aliases: list[str] = []
    existing_aliases = [clean_text(str(item)) for item in target_row.get("aliases", []) if clean_text(str(item))]
    for row in contributions:
        row_conflicted = _detail_contribution_conflicts_row(row, conflict_fields, target_row=target_row)
        conflict_field_names = _detail_contribution_conflict_fields(row, conflict_fields, target_row=target_row)
        if row_conflicted:
            for alias in row.get("aliases", []):
                cleaned = clean_text(str(alias))
                if not cleaned:
                    continue
                qa_rows.append(
                    {
                        "asset_id": "",
                        "display_name": display_name,
                        "qa_status": "alias_suppressed_by_detail_conflict",
                        "candidate_quality": "",
                        "source_url": source["url"],
                        "source_role": source["role"],
                        "source_page_url": source["url"],
                        "alias": cleaned,
                        "conflict_fields": sorted(conflict_field_names),
                        "target_id": target_row.get("id", ""),
                    }
                )
            continue
        for alias in row.get("aliases", []):
            cleaned = clean_text(str(alias))
            if not cleaned:
                continue
            if aliases_equivalent(cleaned, str(target_row.get("display_name", ""))):
                qa_rows.append(
                    {
                        "asset_id": "",
                        "display_name": display_name,
                        "qa_status": "alias_equivalent_to_canonical_name",
                        "candidate_quality": "",
                        "source_url": source["url"],
                        "source_role": source["role"],
                        "source_page_url": source["url"],
                        "alias": cleaned,
                        "target_id": target_row.get("id", ""),
                    }
                )
                continue
            if any(aliases_equivalent(cleaned, existing_alias) for existing_alias in existing_aliases + merged_aliases):
                qa_rows.append(
                    {
                        "asset_id": "",
                        "display_name": display_name,
                        "qa_status": "alias_equivalent_to_existing_alias",
                        "candidate_quality": "",
                        "source_url": source["url"],
                        "source_role": source["role"],
                        "source_page_url": source["url"],
                        "alias": cleaned,
                        "target_id": target_row.get("id", ""),
                    }
                )
                continue
            merged_aliases.append(cleaned)
    return merged_aliases


def _detail_contribution_conflicts_row(
    row: dict[str, Any],
    conflict_fields: set[str],
    *,
    target_row: dict[str, Any],
) -> bool:
    for field_name in conflict_fields:
        field_value = clean_text(str(row.get(field_name, "")))
        if not field_value:
            continue
        existing_value = clean_text(str(target_row.get(field_name, "")))
        if existing_value and field_value != existing_value:
            return True
        if not existing_value:
            return True
    return False


def _detail_contribution_conflict_fields(
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


def _resolve_consistent_detail_field(
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


def _append_asset_row(
    assets: list[dict[str, Any]],
    *,
    game: str,
    source: dict[str, Any],
    record_type: str,
    display_name: str,
    source_url: str,
    source_kind: str = "page_image",
    section_heading: str = "",
    raw_label: str = "",
    anchor_source: str = "",
    anchor_ambiguous: bool = False,
    anchor_ambiguity_type: str = "",
    paragraph_referential: bool = False,
) -> None:
    entity_id = _asset_entity_id(game, display_name)
    asset_id = f"{game}.{record_type}.{_slugify(display_name)}.{hashlib.sha1(source_url.encode('utf-8')).hexdigest()[:8]}"
    qa_status = "verified_source" if _looks_like_direct_asset_url(source_url) else "needs_manual_crop"
    quality = analyze_asset_candidate(
        display_name=display_name,
        asset_family=_ASSET_FAMILY_DEFAULTS[record_type],
        source_role=source["role"],
        source_kind=source_kind,
        source_url=source_url,
        section_heading=section_heading,
        raw_label=raw_label or display_name,
    )
    assets.append(
        {
            "asset_id": asset_id,
            "game_id": game,
            "entity_id": entity_id,
            "asset_family": _ASSET_FAMILY_DEFAULTS[record_type],
            "display_name": display_name,
            "binding_key": quality["binding_key"],
            "candidate_quality": quality["candidate_quality"],
            "quality_score": quality["quality_score"],
            "quality_reasons": quality["quality_reasons"],
            "portrait_like": quality["portrait_like"],
            "icon_like": quality["icon_like"],
            "badge_like": quality["badge_like"],
            "artwork_like": quality["artwork_like"],
            "generic_page_art": quality["generic_page_art"],
            "source_url": source_url,
            "source_page_url": source["url"],
            "source_role": source["role"],
            "source_title": source["title"],
            "source_kind": source_kind,
            "anchor_source": anchor_source,
            "anchor_ambiguous": bool(anchor_ambiguous),
            "anchor_ambiguity_type": anchor_ambiguity_type,
            "paragraph_referential": bool(paragraph_referential),
            "draft_local_path": "",
            "template_path": "",
            "mask_path": "",
            "roi_ref": _default_roi_ref(record_type),
            "match_method": _default_match_method(record_type),
            "threshold": _default_threshold(record_type),
            "scale_set": _default_scale_set(record_type),
            "temporal_window": _default_temporal_window(record_type),
            "license_note": "internal_review_required",
            "qa_status": qa_status,
        }
    )


def _headers_for_rows(rows: list[dict[str, Any]]) -> list[str]:
    headers: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    return headers or ["empty"]


def _source_record_status(source: dict[str, Any]) -> str:
    if str(source.get("page_type", "")) == "category" and not source.get("category_items"):
        return "empty_source"
    if str(source.get("page_type", "")) == "article":
        sections = source.get("sections", [])
        has_content = any(getattr(section, "items", []) or getattr(section, "images", []) for section in sections)
        if not has_content:
            return "empty_source"
    return "fetched"


def _source_failure_row(source: WikiSource, error: WikiFetchError) -> dict[str, Any]:
    return {
        "source_page_url": source.url,
        "source_role": source.role,
        "source_title": "",
        "source_scheme": urlparse(source.url).scheme or "file",
        "page_type": "",
        "status": "fetch_failed",
        "content_type": "",
        "section_count": 0,
        "failure_category": error.category,
        "error": error.message,
        "hint": error.hint,
    }


def _stringify_row(row: dict[str, Any], headers: list[str]) -> dict[str, str]:
    rendered: dict[str, str] = {}
    for header in headers:
        value = row.get(header, "")
        if isinstance(value, list):
            rendered[header] = json.dumps(value)
        elif isinstance(value, dict):
            rendered[header] = json.dumps(value, sort_keys=True)
        else:
            rendered[header] = "" if value is None else str(value)
    return rendered


def _classify_section(heading: str, source_role: str) -> str | None:
    role_default = _record_type_for_role(source_role)
    if role_default == "overview":
        return None
    lowered = heading.lower()
    if not lowered or any(token in lowered for token in _NEGATIVE_SECTION_HINTS):
        return None
    if _looks_like_toc_entry(heading):
        return None
    if source_role.strip().lower() == "events":
        if any(token in lowered for token in ("contract", "intel", "map", "video", "progression", "crossplay", "vehicle", "weapon")):
            return None
        if any(token in lowered for token in _EVENT_SECTION_HINTS):
            return "event_badge_or_medal"
        return None
    if any(token in lowered for token in _EVENT_SECTION_HINTS):
        return "event_badge_or_medal"
    if any(token in lowered for token in _ABILITY_SECTION_HINTS):
        return "ability_or_equipment"
    if any(token in lowered for token in _ENTITY_SECTION_HINTS):
        return "character_or_operator"
    return role_default


def _default_roi_ref(record_type: str) -> str:
    if record_type == "character_or_operator":
        return "hud.hero_portrait"
    if record_type == "ability_or_equipment":
        return "hud.ability_bar"
    return "hud.event_badge"


def _default_match_method(record_type: str) -> str:
    if record_type == "character_or_operator":
        return "TM_CCOEFF_NORMED"
    return "TM_CCORR_NORMED"


def _default_threshold(record_type: str) -> float:
    if record_type == "character_or_operator":
        return 0.88
    if record_type == "ability_or_equipment":
        return 0.90
    return 0.93


def _default_scale_set(record_type: str) -> list[float]:
    if record_type == "character_or_operator":
        return [0.75, 1.0, 1.25]
    return [0.9, 1.0, 1.1]


def _default_temporal_window(record_type: str) -> int:
    if record_type == "event_badge_or_medal":
        return 3
    return 2


def _relative_asset_dir(asset: dict[str, Any]) -> Path:
    family = asset["asset_family"]
    entity_slug = _slugify(asset["entity_id"])
    if family == "medal_icon":
        return Path("events") / entity_slug
    if family == "equipment_icon":
        return Path("equipment") / entity_slug
    return Path("entities") / entity_slug


def _asset_entity_id(game: str, name: str) -> str:
    return f"{game}.{_slugify(name)}"


def _display_name_for_game(game: str) -> str:
    return " ".join(part.capitalize() for part in game.split("_"))


def _normalize_item_name(
    text: str,
    *,
    record_type: str | None = None,
    source_role: str | None = None,
    section_heading: str | None = None,
) -> str:
    structured = extract_structured_fields(
        text,
        source_role=(source_role or ""),
        section_heading=(section_heading or ""),
        record_type=record_type,
    )
    return structured["display_name"]


def _clean_text(text: str) -> str:
    return clean_text(text)


def _slugify(value: str) -> str:
    return slugify(value)


def _timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _normalize_source_url(url: str) -> str:
    return _shared_normalize_source_url(url)


def _looks_like_direct_asset_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https", "file"}:
        return False
    extension = os.path.splitext(parsed.path)[1].lower()
    return extension in {".png", ".jpg", ".jpeg", ".webp"}


def _extension_for_url(url: str) -> str:
    extension = os.path.splitext(urlparse(url).path)[1].lower()
    return extension or ".bin"


def _image_source_from_attrs(attrs: dict[str, str | None]) -> str:
    for key in ("src", "data-src"):
        value = (attrs.get(key) or "").strip()
        if value:
            return value
    for key in ("srcset", "data-srcset"):
        value = (attrs.get(key) or "").strip()
        if value:
            first_candidate = value.split(",")[0].strip().split(" ")[0].strip()
            if first_candidate:
                return first_candidate
    return ""


def _is_article_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = _attr_tokens(attrs.get("class"))
    element_id = (attrs.get("id") or "").strip().lower()
    if tag == "div" and "mw-parser-output" in classes:
        return True
    if element_id == "mw-content-text":
        return True
    return False


def _is_ignored_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = _attr_tokens(attrs.get("class"))
    element_id = (attrs.get("id") or "").strip().lower()
    ignored_tokens = {
        "toc",
        "table-of-contents",
        "portable-infobox",
        "infobox",
        "wds-global-navigation",
        "page-header",
        "comments",
        "gallery",
        "category-page__trending-pages",
        "category-page__alphabet-shortcuts",
        "category-page__pagination",
    }
    if element_id in ignored_tokens:
        return True
    return any(token in classes for token in ignored_tokens)


def _is_category_listing_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = _attr_tokens(attrs.get("class"))
    return any(
        token in classes
        for token in {
            "category-page__members",
            "category-page__members-for-char",
        }
    )


def _is_category_member_container(tag: str, attrs: dict[str, str | None]) -> bool:
    return "category-page__member" in _attr_tokens(attrs.get("class"))


def _attr_tokens(value: str | None) -> set[str]:
    if not value:
        return set()
    return {token.strip().lower() for token in value.split() if token.strip()}


def _looks_like_toc_entry(text: str) -> bool:
    return bool(re.match(r"^\d+(?:\.\d+)*\s+", text.strip()))


def _should_ignore_row_text(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return True
    if _looks_like_toc_entry(text):
        return True
    if lowered in {"contents", "trivia", "references", "external links", "see also"}:
        return True
    if "call of duty wiki" in lowered:
        return True
    if "subject of this article" in lowered:
        return True
    return False


def _should_keep_image(source_url: str, display_name: str, *, source_role: str, section_heading: str) -> bool:
    asset_family = _ASSET_FAMILY_DEFAULTS.get(_record_type_for_role(source_role) or "", "hud_icon")
    quality = analyze_asset_candidate(
        display_name=display_name,
        asset_family=asset_family,
        source_role=source_role,
        source_kind="page_image",
        source_url=source_url,
        section_heading=section_heading,
        raw_label=display_name,
    )
    if quality["hard_reject"]:
        return False
    parsed = urlparse(source_url)
    lowered_name = display_name.casefold()
    lowered_url = source_url.casefold()
    role = source_role.strip().lower()
    section_lowered = section_heading.casefold()
    if parsed.scheme == "data":
        return False
    if "site-logo" in lowered_url or "wiki-logo" in lowered_url:
        return False
    if "call of duty wiki" in lowered_name or "site logo" in lowered_name:
        return False
    if "subject of this article" in lowered_name:
        return False
    if "artwork" in lowered_name or "banner" in lowered_name:
        return False
    if role in {"overview", "maps"}:
        return False
    if role == "operators" and "logo" in lowered_name:
        return False
    if role == "equipment" and any(token in lowered_name for token in ("banner", "artwork", "screenshot")):
        return False
    if role == "events":
        if any(token in lowered_name for token in ("map", "verdansk", "rebirth", "launch", "season", "pre")):
            return False
        has_icon_like_name = any(token in lowered_name for token in ("icon", "logo", "badge", "medal", "event", "emblem"))
        has_event_like_section = any(token in section_lowered for token in ("event", "medal", "badge"))
        if not has_icon_like_name and not has_event_like_section:
            return False
        if not has_icon_like_name and _looks_like_prose(lowered_name):
            return False
    if role == "operators" and _looks_like_prose(lowered_name):
        return False
    if role == "equipment" and _looks_like_prose(lowered_name):
        return False
    return True


def _record_type_for_role(source_role: str) -> str | None:
    role = source_role.strip().lower()
    if role in {"operators", "factions"}:
        return "character_or_operator"
    if role == "equipment":
        return "ability_or_equipment"
    if role == "events":
        return "event_badge_or_medal"
    if role in {"overview", "maps"}:
        return "overview"
    return None


def _detect_page_type(url: str, title: str, category_items: list[ParsedCategoryItem]) -> str:
    parsed = urlparse(url)
    path = parsed.path.casefold()
    title_lowered = title.casefold()
    if "/wiki/category:" in path or title_lowered.startswith("category:"):
        return "category"
    if category_items:
        return "category"
    return "article"


def _extract_event_name(text: str, *, section_heading: str) -> str:
    return extract_event_name(text, section_heading=section_heading)


def _looks_like_prose(text: str) -> bool:
    return len(text.split()) > 6 or "." in text


def _bind_image_to_known_name(
    candidate_name: str,
    known_names: set[str],
    *,
    record_type: str,
    source_role: str,
) -> str:
    normalized_candidate = _clean_text(candidate_name)
    if not normalized_candidate:
        return ""
    if not known_names:
        return normalized_candidate
    if normalized_candidate in known_names:
        return normalized_candidate

    structured = extract_structured_fields(
        normalized_candidate,
        source_role=source_role,
        section_heading="Events" if record_type == "event_badge_or_medal" else "",
        record_type=record_type,
    )
    aliases = [_strip_image_label_suffixes(normalized_candidate), structured.get("display_name", "")]
    aliases.extend(structured.get("aliases", []))
    if record_type == "event_badge_or_medal" or source_role.strip().lower() == "events":
        aliases.append(_extract_event_name(normalized_candidate, section_heading="Events"))

    for alias in aliases:
        if alias and alias in known_names:
            return alias

    stripped_slug = _slugify(_strip_image_label_suffixes(normalized_candidate))
    candidate_slug = _slugify(normalized_candidate)
    for known_name in known_names:
        known_slug = _slugify(known_name)
        if known_slug == candidate_slug or known_slug == stripped_slug:
            return known_name
    return ""


def _strip_image_label_suffixes(text: str) -> str:
    stripped = re.sub(r"\b(icon|logo|badge|medal|emblem|portrait|thumbnail)\b", "", text, flags=re.IGNORECASE)
    return _clean_text(re.sub(r"\s{2,}", " ", stripped))


def _should_ignore_category_row(text: str) -> bool:
    lowered = text.strip().casefold()
    if not lowered:
        return True
    if lowered.startswith("category:"):
        return True
    if lowered in {"trending pages", "all items", "popular pages", "recent changes"}:
        return True
    if len(lowered) == 1 and lowered.isalpha():
        return True
    return False
