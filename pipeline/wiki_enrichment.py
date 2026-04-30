from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen

from pipeline.simple_yaml import dump_yaml_file


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

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag in {"h1", "h2", "h3"}:
            self._current_heading_tag = tag
            self._current_heading_text = ""
        elif tag == "li":
            self._capture_li = True
            self._current_text_parts = []
        elif tag == "title":
            self._capture_title = True
        elif tag == "img":
            src = attr_map.get("src") or ""
            alt = (attr_map.get("alt") or "").strip()
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

    def handle_endtag(self, tag: str) -> None:
        if tag == self._current_heading_tag and self._current_heading_tag:
            heading = _clean_text(self._current_heading_text)
            if heading:
                section_key = _slugify(heading)
                self._current_section = ParsedSection(section_key=section_key, heading=heading)
                self.sections.append(self._current_section)
            self._current_heading_tag = ""
            self._current_heading_text = ""
        elif tag == "li" and self._capture_li:
            text = _clean_text(" ".join(self._current_text_parts))
            if text:
                self._current_section.items.append(text)
            self._capture_li = False
            self._current_text_parts = []
        elif tag == "title":
            self._capture_title = False

    def handle_data(self, data: str) -> None:
        if self._current_heading_tag:
            self._current_heading_text += data
        if self._capture_li:
            self._current_text_parts.append(data)
        if self._capture_title:
            self.title += data


def enrich_game_from_wiki(game: str, wiki_url: str, *, repo_root: Path | None = None) -> dict[str, Any]:
    repo_root = (repo_root or REPO_ROOT).resolve()
    timestamp = _timestamp_slug()
    draft_root = repo_root / "assets" / "games" / game / "drafts" / "wiki" / timestamp
    catalog_root = draft_root / "catalog"
    downloads_root = draft_root / "downloads"
    templates_root = draft_root / "templates"
    masks_root = draft_root / "masks"
    for path in (catalog_root, downloads_root, templates_root, masks_root):
        path.mkdir(parents=True, exist_ok=True)

    source_records = [_fetch_source_record(wiki_url)]
    normalized = _normalize_records(game, source_records)
    _download_assets(normalized["assets"], downloads_root, templates_root)
    _write_catalog_csvs(catalog_root, normalized)
    _write_draft_artifacts(draft_root, game, normalized)

    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "wiki_url": wiki_url,
        "draft_root": str(draft_root),
        "catalog_root": str(catalog_root),
        "downloads_root": str(downloads_root),
        "templates_root": str(templates_root),
        "masks_root": str(masks_root),
        "source_count": len(source_records),
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


def _fetch_source_record(url: str) -> dict[str, Any]:
    with urlopen(url) as response:
        content_type = response.headers.get_content_type() if hasattr(response, "headers") else "text/html"
        body = response.read()
    text = body.decode("utf-8", errors="replace")
    parser = _WikiHtmlParser(url)
    parser.feed(text)
    return {
        "url": url,
        "content_type": content_type,
        "title": _clean_text(parser.title) or _slugify(urlparse(url).path) or "source",
        "sections": parser.sections,
    }


def _normalize_records(game: str, source_records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    games = [{
        "game_id": game,
        "display_name": _display_name_for_game(game),
        "seed_adapter": "call_of_duty",
        "primary_source_kind": "explicit_url",
    }]
    entities_by_id: dict[str, dict[str, Any]] = {}
    abilities_by_id: dict[str, dict[str, Any]] = {}
    events_by_id: dict[str, dict[str, Any]] = {}
    assets: list[dict[str, Any]] = []
    fetch_log: list[dict[str, Any]] = []

    for source in source_records:
        sections: list[ParsedSection] = source["sections"]
        fetch_log.append(
            {
                "source_page_url": source["url"],
                "source_title": source["title"],
                "status": "fetched",
                "content_type": source["content_type"],
                "section_count": len(sections),
            }
        )

        for section in sections:
            record_type = _classify_section(section.heading)
            for item_text in section.items:
                item_name = _normalize_item_name(item_text)
                if not item_name:
                    continue
                if record_type == "character_or_operator":
                    entity_id = _asset_entity_id(game, item_name)
                    entities_by_id.setdefault(
                        entity_id,
                        {
                            "entity_id": entity_id,
                            "game_id": game,
                            "entity_type": "character_or_operator",
                            "display_name": item_name,
                            "source_page_url": source["url"],
                            "section_heading": section.heading,
                        },
                    )
                elif record_type == "ability_or_equipment":
                    ability_id = _asset_entity_id(game, item_name)
                    abilities_by_id.setdefault(
                        ability_id,
                        {
                            "ability_id": ability_id,
                            "game_id": game,
                            "display_name": item_name,
                            "source_page_url": source["url"],
                            "section_heading": section.heading,
                        },
                    )
                else:
                    event_id = _asset_entity_id(game, item_name)
                    events_by_id.setdefault(
                        event_id,
                        {
                            "event_id": event_id,
                            "game_id": game,
                            "display_name": item_name,
                            "source_page_url": source["url"],
                            "section_heading": section.heading,
                        },
                    )

            for image in section.images:
                record_type = _classify_section(image.section_text)
                display_name = _normalize_item_name(image.alt or section.heading)
                if not display_name:
                    display_name = "unnamed_asset"
                entity_id = _asset_entity_id(game, display_name)
                if record_type == "character_or_operator":
                    entities_by_id.setdefault(
                        entity_id,
                        {
                            "entity_id": entity_id,
                            "game_id": game,
                            "entity_type": "character_or_operator",
                            "display_name": display_name,
                            "source_page_url": source["url"],
                            "section_heading": section.heading,
                        },
                    )
                elif record_type == "ability_or_equipment":
                    abilities_by_id.setdefault(
                        entity_id,
                        {
                            "ability_id": entity_id,
                            "game_id": game,
                            "display_name": display_name,
                            "source_page_url": source["url"],
                            "section_heading": section.heading,
                        },
                    )
                else:
                    events_by_id.setdefault(
                        entity_id,
                        {
                            "event_id": entity_id,
                            "game_id": game,
                            "display_name": display_name,
                            "source_page_url": source["url"],
                            "section_heading": section.heading,
                        },
                    )

                asset_id = f"{game}.{record_type}.{_slugify(display_name)}.{hashlib.sha1(image.src.encode('utf-8')).hexdigest()[:8]}"
                qa_status = "verified_source" if _looks_like_direct_asset_url(image.src) else "needs_manual_crop"
                assets.append(
                    {
                        "asset_id": asset_id,
                        "game_id": game,
                        "entity_id": entity_id,
                        "asset_family": _ASSET_FAMILY_DEFAULTS[record_type],
                        "display_name": display_name,
                        "source_url": image.src,
                        "source_page_url": source["url"],
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

    qa_queue = [
        {
            "asset_id": asset["asset_id"],
            "display_name": asset["display_name"],
            "qa_status": asset["qa_status"],
            "source_url": asset["source_url"],
        }
        for asset in assets
        if asset["qa_status"] != "verified_source"
    ]

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


def _headers_for_rows(rows: list[dict[str, Any]]) -> list[str]:
    headers: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    return headers or ["empty"]


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


def _classify_section(heading: str) -> str:
    lowered = heading.lower()
    if any(token in lowered for token in _EVENT_SECTION_HINTS):
        return "event_badge_or_medal"
    if any(token in lowered for token in _ABILITY_SECTION_HINTS):
        return "ability_or_equipment"
    if any(token in lowered for token in _ENTITY_SECTION_HINTS):
        return "character_or_operator"
    return "event_badge_or_medal"


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


def _normalize_item_name(text: str) -> str:
    cleaned = _clean_text(re.sub(r"\s*[:\-–]\s*.*$", "", text))
    return cleaned


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "item"


def _timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _looks_like_direct_asset_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https", "file"}:
        return False
    extension = os.path.splitext(parsed.path)[1].lower()
    return extension in {".png", ".jpg", ".jpeg", ".webp"}


def _extension_for_url(url: str) -> str:
    extension = os.path.splitext(urlparse(url).path)[1].lower()
    return extension or ".bin"
