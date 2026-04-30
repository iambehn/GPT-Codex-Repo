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
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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
_HTML_ACCEPT_HEADER = "text/html,application/xhtml+xml"
_DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9"
_DEFAULT_TIMEOUT_SECONDS = 20
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


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


class WikiFetchError(RuntimeError):
    def __init__(
        self,
        *,
        source_url: str,
        category: str,
        message: str,
        hint: str,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.source_url = source_url
        self.category = category
        self.message = message
        self.hint = hint
        self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "ok": False,
            "status": "fetch_failed",
            "wiki_url": self.source_url,
            "error": self.message,
            "hint": self.hint,
            "failure_category": self.category,
        }
        if self.http_status is not None:
            payload["http_status"] = self.http_status
        return payload


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
        elif tag == "a" and self._capture_category_anchor:
            item_name = _clean_text(self._current_category_anchor_text)
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
        source_records = [_fetch_source_record(source.url, source.role) for source in normalized_sources]
        normalized = _normalize_records(game, source_records)
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
    normalized_url = _normalize_source_url(url)
    try:
        response_target = _build_fetch_target(normalized_url)
        with urlopen(response_target, timeout=_DEFAULT_TIMEOUT_SECONDS) as response:
            content_type = response.headers.get_content_type() if hasattr(response, "headers") else "text/html"
            body = response.read()
    except HTTPError as exc:
        raise WikiFetchError(
            source_url=normalized_url,
            category="http_error",
            message=f"failed to fetch source page: HTTP {exc.code}",
            hint=_fetch_hint_for_status(exc.code),
            http_status=exc.code,
        ) from exc
    except URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise WikiFetchError(
            source_url=normalized_url,
            category="network_error",
            message=f"failed to fetch source page: {reason}",
            hint="The source may be unavailable or the current environment may not have network access.",
        ) from exc

    try:
        text = body.decode("utf-8", errors="replace")
    except UnicodeDecodeError as exc:
        raise WikiFetchError(
            source_url=normalized_url,
            category="decode_error",
            message="failed to decode source page as text",
            hint="The source responded with content that could not be decoded as HTML text.",
        ) from exc

    parser = _WikiHtmlParser(normalized_url)
    parser.feed(text)
    page_type = _detect_page_type(normalized_url, _clean_text(parser.title), parser.category_items)
    source_scheme = urlparse(normalized_url).scheme or "file"
    return {
        "url": normalized_url,
        "role": role,
        "source_scheme": source_scheme,
        "content_type": content_type,
        "title": _clean_text(parser.title) or _slugify(urlparse(normalized_url).path) or "source",
        "page_type": page_type,
        "sections": parser.sections,
        "category_items": parser.category_items,
    }


def _build_fetch_target(url: str) -> str | Request:
    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"}:
        referer = f"{parsed.scheme}://{parsed.netloc}/" if parsed.netloc else url
        return Request(
            url,
            headers={
                "User-Agent": _BROWSER_USER_AGENT,
                "Accept": _HTML_ACCEPT_HEADER,
                "Accept-Language": _DEFAULT_ACCEPT_LANGUAGE,
                "Referer": referer,
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
            },
        )
    return url


def _fetch_hint_for_status(status_code: int) -> str:
    if status_code == 403:
        return "The source blocked the request even after browser-like headers were applied. Save the page locally and point the manifest/source url at that HTML file."
    if status_code == 404:
        return "The source URL was not found. Verify the page still exists."
    if 500 <= status_code < 600:
        return "The source site returned a server error. Retry later or choose another source."
    return "The source returned an unexpected HTTP error."


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

    for source in source_records:
        sections: list[ParsedSection] = source["sections"]
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
                "status": "fetched",
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
            )
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
                )

            for image in section.images:
                record_type = _classify_section(image.section_text, source["role"])
                if record_type is None:
                    continue
                candidate_name = _normalize_item_name(
                    image.alt or section.heading,
                    record_type=record_type,
                    source_role=source["role"],
                    section_heading=image.section_text,
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
                    section_heading=image.section_text,
                ):
                    continue
                asset_key = (display_name.casefold(), image.src)
                if asset_key in seen_assets:
                    continue
                seen_assets.add(asset_key)
                _append_asset_row(
                    assets,
                    game=game,
                    source=source,
                    record_type=record_type,
                    display_name=display_name,
                    source_url=image.src,
                )

    qa_queue = [
        {
            "asset_id": asset["asset_id"],
            "display_name": asset["display_name"],
            "qa_status": asset["qa_status"],
            "source_url": asset["source_url"],
            "source_role": asset["source_role"],
            "source_page_url": asset["source_page_url"],
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


def _normalize_category_source(
    game: str,
    source: dict[str, Any],
    entities_by_id: dict[str, dict[str, Any]],
    abilities_by_id: dict[str, dict[str, Any]],
    events_by_id: dict[str, dict[str, Any]],
    assets: list[dict[str, Any]],
    seen_assets: set[tuple[str, str]],
    source_known_names: dict[str, set[str]],
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
) -> None:
    if record_type is None:
        return
    row = {
        "game_id": game,
        "display_name": display_name,
        "source_page_url": source["url"],
        "source_role": source["role"],
        "source_title": source["title"],
        "section_heading": section_heading,
    }
    canonical_id = _asset_entity_id(game, display_name)
    if record_type == "character_or_operator":
        entities_by_id.setdefault(
            canonical_id,
            {
                "entity_id": canonical_id,
                "entity_type": "character_or_operator",
                **row,
            },
        )
    elif record_type == "ability_or_equipment":
        abilities_by_id.setdefault(
            canonical_id,
            {
                "ability_id": canonical_id,
                **row,
            },
        )
    elif record_type == "event_badge_or_medal":
        events_by_id.setdefault(
            canonical_id,
            {
                "event_id": canonical_id,
                **row,
            },
        )


def _append_asset_row(
    assets: list[dict[str, Any]],
    *,
    game: str,
    source: dict[str, Any],
    record_type: str,
    display_name: str,
    source_url: str,
) -> None:
    entity_id = _asset_entity_id(game, display_name)
    asset_id = f"{game}.{record_type}.{_slugify(display_name)}.{hashlib.sha1(source_url.encode('utf-8')).hexdigest()[:8]}"
    qa_status = "verified_source" if _looks_like_direct_asset_url(source_url) else "needs_manual_crop"
    assets.append(
        {
            "asset_id": asset_id,
            "game_id": game,
            "entity_id": entity_id,
            "asset_family": _ASSET_FAMILY_DEFAULTS[record_type],
            "display_name": display_name,
            "source_url": source_url,
            "source_page_url": source["url"],
            "source_role": source["role"],
            "source_title": source["title"],
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
    if record_type == "event_badge_or_medal" or (source_role or "").strip().lower() == "events":
        event_name = _extract_event_name(text, section_heading=section_heading or "")
        if event_name:
            return event_name
    cleaned = _clean_text(re.sub(r"\s*[:\-–]\s*.*$", "", text))
    return cleaned


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "item"


def _timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _normalize_source_url(url: str) -> str:
    stripped = (url or "").strip()
    parsed = urlparse(stripped)
    if parsed.scheme in {"http", "https", "file"}:
        return stripped
    path = Path(stripped).expanduser()
    if path.exists():
        return path.resolve().as_uri()
    return stripped


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
    cleaned = _clean_text(text)
    if not cleaned:
        return ""
    match = re.match(r"^(?:The\s+)?([A-Z][A-Za-z0-9'&().:/ -]{1,80}?)\s+event\b", cleaned)
    if match:
        return _clean_text(match.group(1))
    if cleaned.count(" ") <= 5 and "." not in cleaned:
        return _clean_text(re.sub(r"\s*[:\-–]\s*.*$", "", cleaned))
    if "events" in section_heading.casefold():
        return ""
    return _clean_text(re.sub(r"\s*[:\-–]\s*.*$", "", cleaned))


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
    if not normalized_candidate or not known_names:
        return ""
    if normalized_candidate in known_names:
        return normalized_candidate

    aliases = [_strip_image_label_suffixes(normalized_candidate)]
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
