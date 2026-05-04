from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from pipeline.structured_source_fields import clean_text, slugify


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
    infobox_like: bool = False
    gallery_like: bool = False
    captioned: bool = False
    anchor_text: str = ""
    anchor_source: str = ""
    anchor_strength: str = "weak"
    filename_hint: str = ""
    anchor_candidates: list[str] = field(default_factory=list)
    anchor_conflict: bool = False
    paragraph_backed: bool = False
    paragraph_ambiguous: bool = False
    paragraph_referential: bool = False
    anchor_ambiguity_type: str = ""
    paragraph_anchor_candidates: list[str] = field(default_factory=list)
    paragraph_blocks_seen: int = 0
    preceding_paragraph_candidate: str = ""


@dataclass
class ParsedSection:
    section_key: str
    heading: str
    items: list[str] = field(default_factory=list)
    paragraphs: list[str] = field(default_factory=list)
    images: list[ParsedImage] = field(default_factory=list)


@dataclass
class ParsedCategoryItem:
    name: str
    href: str
    image_src: str = ""
    image_alt: str = ""


class SourceFetchError(RuntimeError):
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


def normalize_source_url(url: str) -> str:
    stripped = (url or "").strip()
    parsed = urlparse(stripped)
    if parsed.scheme in {"http", "https", "file"}:
        return stripped
    path = Path(stripped).expanduser()
    if path.exists():
        return path.resolve().as_uri()
    return stripped


def build_fetch_target(url: str) -> str | Request:
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


def fetch_hint_for_status(status_code: int) -> str:
    if status_code == 403:
        return "The source blocked the request even after browser-like headers were applied. Save the page locally and point the manifest/source url at that HTML file."
    if status_code == 404:
        return "The source URL was not found. Verify the page still exists."
    if 500 <= status_code < 600:
        return "The source site returned a server error. Retry later or choose another source."
    return "The source returned an unexpected HTTP error."


def fetch_source_record(url: str, role: str) -> dict[str, Any]:
    normalized_url = normalize_source_url(url)
    try:
        response_target = build_fetch_target(normalized_url)
        with urlopen(response_target, timeout=_DEFAULT_TIMEOUT_SECONDS) as response:
            content_type = response.headers.get_content_type() if hasattr(response, "headers") else "text/html"
            body = response.read()
    except HTTPError as exc:
        raise SourceFetchError(
            source_url=normalized_url,
            category="http_error",
            message=f"failed to fetch source page: HTTP {exc.code}",
            hint=fetch_hint_for_status(exc.code),
            http_status=exc.code,
        ) from exc
    except URLError as exc:
        reason = getattr(exc, "reason", exc)
        raise SourceFetchError(
            source_url=normalized_url,
            category="network_error",
            message=f"failed to fetch source page: {reason}",
            hint="The source may be unavailable or the current environment may not have network access.",
        ) from exc

    source_scheme = urlparse(normalized_url).scheme or "file"
    if content_type.startswith("image/") or looks_like_direct_image_url(normalized_url):
        return {
            "url": normalized_url,
            "role": role,
            "source_scheme": source_scheme,
            "content_type": content_type,
            "title": Path(urlparse(normalized_url).path).name,
            "page_type": "direct_image",
            "sections": [],
            "category_items": [],
            "direct_image_url": normalized_url,
        }

    try:
        text = body.decode("utf-8", errors="replace")
    except UnicodeDecodeError as exc:
        raise SourceFetchError(
            source_url=normalized_url,
            category="decode_error",
            message="failed to decode source page as text",
            hint="The source responded with content that could not be decoded as HTML text.",
        ) from exc

    parser = WikiHtmlParser(normalized_url)
    parser.feed(text)
    title = clean_text(parser.title) or slugify(urlparse(normalized_url).path) or "source"
    return {
        "url": normalized_url,
        "role": role,
        "source_scheme": source_scheme,
        "content_type": content_type,
        "title": title,
        "page_type": detect_page_type(normalized_url, title, parser.category_items),
        "sections": parser.sections,
        "category_items": parser.category_items,
        "direct_image_url": "",
        "article_container_detected": parser.article_detected,
    }


class WikiHtmlParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.title = ""
        self._current_heading_tag = ""
        self._current_heading_text = ""
        self._current_text_parts: list[str] = []
        self._current_text_tag = ""
        self._current_section = ParsedSection(section_key="overview", heading="Overview")
        self.sections: list[ParsedSection] = [self._current_section]
        self._capture_li = False
        self._capture_title = False
        self._article_detected = False
        self._article_depth = 0
        self._ignored_depth = 0
        self._infobox_depth = 0
        self._gallery_depth = 0
        self._tag_flags: list[tuple[str, bool, bool, bool, bool, bool, bool]] = []
        self.category_items: list[ParsedCategoryItem] = []
        self._category_listing_depth = 0
        self._category_member_depth = 0
        self._category_anchor_href = ""
        self._capture_category_anchor = False
        self._current_category_anchor_text = ""
        self._capture_category_label = False
        self._current_category_label_text = ""
        self._current_category_member_name = ""
        self._current_category_member_href = ""
        self._current_category_member_image_src = ""
        self._current_category_member_image_alt = ""
        self._recent_text_block = ""
        self._recent_text_tag = ""
        self._recent_paragraph_candidate = ""
        self._recent_paragraph_ambiguous = False
        self._pending_anchor_images: list[ParsedImage] = []

    @property
    def article_detected(self) -> bool:
        return self._article_detected

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        enters_article = _is_article_container(tag, attr_map)
        enters_ignored = _is_ignored_container(tag, attr_map)
        enters_infobox = _is_infobox_container(tag, attr_map)
        enters_gallery = _is_gallery_container(tag, attr_map)
        enters_category_listing = _is_category_listing_container(tag, attr_map)
        enters_category_member = _is_category_member_container(tag, attr_map) or (
            tag == "tr" and self._category_listing_depth > 0
        )

        if enters_article:
            if not self._article_detected:
                preserved_images = list(self._current_section.images)
                self._article_detected = True
                self._reset_sections()
                self._current_section.images.extend(preserved_images)
            self._article_depth += 1

        if enters_infobox and self._capture_image_enabled():
            self._infobox_depth += 1

        if enters_ignored and self._capture_image_enabled():
            self._ignored_depth += 1

        if enters_gallery and self._capture_image_enabled():
            self._gallery_depth += 1

        self._tag_flags.append(
            (tag, enters_article, enters_ignored, enters_infobox, enters_gallery, enters_category_listing, enters_category_member)
        )

        if enters_category_listing:
            self._category_listing_depth += 1
        if enters_category_member:
            if self._category_member_depth == 0:
                self._reset_current_category_member()
            self._category_member_depth += 1

        if tag in {"h1", "h2", "h3"} and self._capture_heading_enabled():
            self._current_heading_tag = tag
            self._current_heading_text = ""
        elif tag in {"li", "p", "figcaption"} and self._should_capture_text_tag(tag):
            self._start_text_capture(tag)
        elif tag == "title":
            self._capture_title = True
        elif tag == "img" and self._capture_image_enabled():
            src = image_source_from_attrs(attr_map)
            alt = (attr_map.get("alt") or "").strip()
            if self._should_capture_category_member_image() and src:
                self._current_category_member_image_src = urljoin(self.base_url, src)
                self._current_category_member_image_alt = alt
            if src:
                absolute_src = urljoin(self.base_url, src)
                image = ParsedImage(
                    src=absolute_src,
                    alt=alt,
                    section=self._current_section.section_key,
                    section_text=self._current_section.heading,
                    infobox_like=self._infobox_depth > 0,
                    gallery_like=self._gallery_depth > 0,
                )
                _apply_image_anchor_context(image, recent_text=self._recent_text_block)
                if self._recent_text_tag == "p" and self._recent_paragraph_candidate and not self._recent_paragraph_ambiguous:
                    image.preceding_paragraph_candidate = self._recent_paragraph_candidate
                    image.paragraph_anchor_candidates.append(self._recent_paragraph_candidate)
                self._current_section.images.append(image)
                self._pending_anchor_images.append(image)
        elif tag == "a" and self._should_capture_category_anchor(attr_map):
            self._capture_category_anchor = True
            self._category_anchor_href = urljoin(self.base_url, attr_map.get("href") or "")
            self._current_category_anchor_text = ""
        if self._should_capture_category_label(tag, attr_map):
            self._capture_category_label = True
            self._current_category_label_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == self._current_heading_tag and self._current_heading_tag:
            heading = clean_text(self._current_heading_text)
            if heading:
                self._pending_anchor_images = []
                section_key = slugify(heading)
                self._current_section = ParsedSection(section_key=section_key, heading=heading)
                self.sections.append(self._current_section)
            self._current_heading_tag = ""
            self._current_heading_text = ""
        elif tag == self._current_text_tag and self._current_text_tag:
            self._finalize_text_capture(tag)
        elif tag == "title":
            self._capture_title = False
        elif tag == "a" and self._capture_category_anchor:
            item_name = clean_text(self._current_category_anchor_text)
            if item_name and self._category_anchor_href and not should_ignore_category_row(item_name):
                self._current_category_member_name = item_name
                self._current_category_member_href = self._category_anchor_href
                if self._category_member_depth <= 0:
                    self._finalize_category_member()
            self._capture_category_anchor = False
            self._category_anchor_href = ""
            self._current_category_anchor_text = ""
        if self._capture_category_label and tag in {"span", "p", "div", "figcaption"}:
            item_name = clean_text(self._current_category_label_text)
            if item_name and not should_ignore_category_row(item_name) and not self._current_category_member_name:
                self._current_category_member_name = item_name
                if not self._current_category_member_href:
                    self._current_category_member_href = self.base_url
            self._capture_category_label = False
            self._current_category_label_text = ""

        while self._tag_flags:
            open_tag, enters_article, enters_ignored, enters_infobox, enters_gallery, enters_category_listing, enters_category_member = self._tag_flags.pop()
            if enters_ignored and self._ignored_depth > 0:
                self._ignored_depth -= 1
            if enters_infobox and self._infobox_depth > 0:
                self._infobox_depth -= 1
            if enters_gallery and self._gallery_depth > 0:
                self._gallery_depth -= 1
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
        if self._current_heading_tag and self._capture_text_enabled():
            self._current_heading_text += data
        if self._current_text_tag and self._capture_text_enabled():
            self._current_text_parts.append(data)
        if self._capture_title:
            self.title += data
        if self._capture_category_anchor:
            self._current_category_anchor_text += data
        if self._capture_category_label:
            self._current_category_label_text += data

    def _capture_image_enabled(self) -> bool:
        if self._article_detected:
            return self._article_depth > 0 and self._ignored_depth == 0
        return self._ignored_depth == 0

    def _capture_text_enabled(self) -> bool:
        return self._capture_image_enabled() and self._infobox_depth == 0

    def _capture_heading_enabled(self) -> bool:
        return self._capture_text_enabled() and self._gallery_depth == 0

    def _should_capture_text_tag(self, tag: str) -> bool:
        if not self._capture_text_enabled():
            return False
        if self._gallery_depth == 0:
            return True
        if not self._article_detected and tag == "p":
            return True
        return tag == "figcaption"

    def _reset_sections(self) -> None:
        self._current_section = ParsedSection(section_key="overview", heading="Overview")
        self.sections = [self._current_section]
        self._current_heading_tag = ""
        self._current_heading_text = ""
        self._current_text_parts = []
        self._capture_li = False
        self._current_text_tag = ""
        self._recent_text_block = ""
        self._recent_text_tag = ""
        self._recent_paragraph_candidate = ""
        self._recent_paragraph_ambiguous = False
        self._pending_anchor_images = []
        self._infobox_depth = 0
        self._gallery_depth = 0

    def _start_text_capture(self, tag: str) -> None:
        self._capture_li = tag == "li"
        self._current_text_tag = tag
        self._current_text_parts = []

    def _finalize_text_capture(self, tag: str) -> None:
        text = clean_text(" ".join(self._current_text_parts))
        if text:
            if tag == "li":
                self._current_section.items.append(text)
            elif tag == "p":
                self._current_section.paragraphs.append(text)
            self._recent_text_block = text
            self._recent_text_tag = tag
            if tag == "p":
                paragraph_candidate, paragraph_ambiguous = _paragraph_anchor_candidate(text)
                self._recent_paragraph_candidate = paragraph_candidate
                self._recent_paragraph_ambiguous = paragraph_ambiguous
            else:
                self._recent_paragraph_candidate = ""
                self._recent_paragraph_ambiguous = False
            if self._pending_anchor_images:
                anchor_source = "caption" if tag == "figcaption" else "paragraph" if tag == "p" else "caption"
                for image in self._pending_anchor_images:
                    _merge_text_anchor(image, text, source=anchor_source)
                    if tag == "p":
                        image.paragraph_blocks_seen += 1
                if tag != "p":
                    self._pending_anchor_images = []
                else:
                    self._pending_anchor_images = [
                        image for image in self._pending_anchor_images if image.paragraph_blocks_seen < 2
                    ]
        self._capture_li = False
        self._current_text_tag = ""
        self._current_text_parts = []

    def _should_capture_category_anchor(self, attrs: dict[str, str | None]) -> bool:
        if self._category_listing_depth <= 0 and self._category_member_depth <= 0:
            return False
        href = (attrs.get("href") or "").strip()
        if not href:
            return False
        classes = attr_tokens(attrs.get("class"))
        if "category-page__member-link" in classes:
            return True
        return self._category_member_depth > 0

    def _should_capture_category_member_image(self) -> bool:
        return self._category_member_depth > 0

    def _should_capture_category_label(self, tag: str, attrs: dict[str, str | None]) -> bool:
        if self._category_member_depth <= 0 or self._capture_category_anchor:
            return False
        if tag not in {"span", "p", "div", "figcaption"}:
            return False
        classes = attr_tokens(attrs.get("class"))
        label_tokens = {
            "card-label",
            "card-title",
            "member-name",
            "item-name",
            "tile-label",
            "gallerytext",
            "caption",
            "label",
            "name",
            "title",
        }
        return any(token in classes for token in label_tokens)

    def _finalize_category_member(self) -> None:
        if self._current_category_member_name:
            self.category_items.append(
                ParsedCategoryItem(
                    name=self._current_category_member_name,
                    href=self._current_category_member_href or self.base_url,
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
        self._capture_category_label = False
        self._current_category_label_text = ""


def detect_page_type(url: str, title: str, category_items: list[ParsedCategoryItem]) -> str:
    parsed = urlparse(url)
    path = parsed.path.casefold()
    title_lowered = clean_text(title).casefold()
    if "/wiki/category:" in path or title_lowered.startswith("category:"):
        return "category"
    if category_items:
        return "category"
    return "article"


def looks_like_direct_image_url(url: str) -> bool:
    return Path(urlparse(url).path).suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".gif"}


def image_anchor_text(image: ParsedImage) -> str:
    return clean_text(image.anchor_text) or clean_text(image.alt) or clean_text(image.filename_hint)


def image_source_from_attrs(attrs: dict[str, str | None]) -> str:
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


def attr_tokens(value: str | None) -> set[str]:
    if not value:
        return set()
    return {token.strip().lower() for token in value.split() if token.strip()}


def should_ignore_category_row(text: str) -> bool:
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


def image_anchor_strength(image: ParsedImage) -> str:
    anchor = clean_text(image.anchor_text)
    alt = clean_text(image.alt)
    filename_hint = clean_text(image.filename_hint)
    if anchor and (image.anchor_source == "caption" or image.captioned):
        return "strong"
    if anchor and (image.anchor_source == "paragraph" or image.paragraph_backed):
        return "medium"
    if anchor and image.anchor_source in {"alt", "recent_text"}:
        return "medium"
    if not anchor and alt:
        return "medium"
    if filename_hint:
        return "weak"
    return "weak"


def _apply_image_anchor_context(image: ParsedImage, *, recent_text: str = "") -> None:
    filename_hint = _filename_hint_for_image(image.src)
    image.filename_hint = filename_hint
    cleaned_alt = _clean_image_label(image.alt)
    image.anchor_candidates = [candidate for candidate in [cleaned_alt, filename_hint] if candidate]
    if cleaned_alt:
        image.anchor_text = cleaned_alt
        image.anchor_source = "alt"
    elif filename_hint:
        image.anchor_text = filename_hint
        image.anchor_source = "filename"
    image.anchor_conflict = _has_anchor_conflict(image.anchor_candidates)
    image.anchor_strength = image_anchor_strength(image)


def _merge_text_anchor(image: ParsedImage, text: str, *, source: str) -> None:
    if source == "paragraph":
        candidate, ambiguous = _paragraph_anchor_candidate(text)
    else:
        candidate = _clean_image_label(text)
        ambiguous = False
    if not candidate:
        if source == "paragraph":
            if ambiguous:
                image.paragraph_ambiguous = True
                image.anchor_ambiguity_type = "same_paragraph"
            elif _is_referential_paragraph(text):
                image.paragraph_referential = True
        return
    if source == "caption":
        image.captioned = True
    if source == "paragraph":
        if candidate not in image.paragraph_anchor_candidates:
            image.paragraph_anchor_candidates.append(candidate)
        if len(image.paragraph_anchor_candidates) > 1:
            image.paragraph_ambiguous = True
            image.anchor_ambiguity_type = "surrounding_paragraph" if image.preceding_paragraph_candidate else "cross_paragraph"
            image.anchor_conflict = True
            image.paragraph_backed = False
            if image.anchor_source == "paragraph":
                image.anchor_text = ""
                image.anchor_source = "filename" if image.filename_hint else ""
        else:
            image.paragraph_backed = True
        if ambiguous:
            image.paragraph_ambiguous = True
            image.anchor_ambiguity_type = "same_paragraph"
    image.anchor_candidates.append(candidate)
    if not image.anchor_text or image.anchor_source in {"filename", "recent_text"}:
        image.anchor_text = candidate
        image.anchor_source = source
    elif image.anchor_source == "alt" and not _anchor_candidates_match(image.anchor_text, candidate):
        image.anchor_text = candidate
        image.anchor_source = source
    image.anchor_conflict = _has_anchor_conflict(image.anchor_candidates)
    image.anchor_strength = image_anchor_strength(image)


def _filename_hint_for_image(url: str) -> str:
    stem = Path(urlparse(url).path).stem
    return _clean_image_label(stem.replace("_", " ").replace("-", " "))


def _clean_image_label(value: str) -> str:
    lowered = clean_text(value)
    lowered = re.sub(
        r"\b(?:icon|portrait|render|artwork|splash|thumb|thumbnail|latest|revision|badge|medal|agent)\b",
        " ",
        lowered,
        flags=re.IGNORECASE,
    )
    return clean_text(lowered)


def _paragraph_anchor_candidate(value: str) -> tuple[str, bool]:
    text = clean_text(value)
    if not text:
        return "", False
    if _is_referential_paragraph(text):
        return "", False
    explicit_pattern = re.compile(
        r"([A-Z][a-z0-9'_-]+(?:\s+[A-Z][a-z0-9'_-]+){0,2})\s+(?:is|was|are|uses|wields|appears|serves|remains)\b"
    )
    explicit_candidates = [_clean_image_label(match.group(1)) for match in explicit_pattern.finditer(text)]
    explicit_candidates = [candidate for candidate in explicit_candidates if candidate]
    unique_candidates = []
    for candidate in explicit_candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    if len(unique_candidates) > 1:
        return "", True
    if len(unique_candidates) == 1:
        return unique_candidates[0], False
    leading_phrase = re.match(r"^([A-Z][a-z0-9'_-]+(?:\s+[A-Z][a-z0-9'_-]+){0,2})\b", text)
    if leading_phrase and len(text.split()) <= 10:
        return _clean_image_label(leading_phrase.group(1)), False
    return "", False


def _is_referential_paragraph(value: str) -> bool:
    text = clean_text(value)
    if not text:
        return False
    return bool(
        re.match(
            r"^(?:He|She|They|This\s+(?:operator|agent|hero|character|veteran|soldier)|The\s+(?:operator|agent|hero|character|veteran|soldier))\b",
            text,
            flags=re.IGNORECASE,
        )
    )


def _anchor_candidates_match(left: str, right: str) -> bool:
    return slugify(left) == slugify(right)


def _has_anchor_conflict(candidates: list[str]) -> bool:
    normalized = {slugify(candidate) for candidate in candidates if clean_text(candidate)}
    return len(normalized) > 1


def _is_article_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = attr_tokens(attrs.get("class"))
    element_id = (attrs.get("id") or "").strip().lower()
    if tag == "div" and "mw-parser-output" in classes:
        return True
    if element_id == "mw-content-text":
        return True
    return False


def _is_ignored_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = attr_tokens(attrs.get("class"))
    element_id = (attrs.get("id") or "").strip().lower()
    ignored_tokens = {
        "toc",
        "table-of-contents",
        "wds-global-navigation",
        "page-header",
        "comments",
        "category-page__trending-pages",
        "category-page__alphabet-shortcuts",
        "category-page__pagination",
    }
    if element_id in ignored_tokens:
        return True
    return any(token in classes for token in ignored_tokens)


def _is_infobox_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = attr_tokens(attrs.get("class"))
    element_id = (attrs.get("id") or "").strip().lower()
    infobox_tokens = {
        "portable-infobox",
        "infobox",
        "infoboxtable",
        "pi-item",
        "pi-image",
        "pi-image-thumbnail",
    }
    if element_id in infobox_tokens:
        return True
    return any(token in classes for token in infobox_tokens)


def _is_gallery_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = attr_tokens(attrs.get("class"))
    element_id = (attrs.get("id") or "").strip().lower()
    gallery_tokens = {
        "gallery",
        "gallerybox",
        "gallerycarousel",
        "wikia-gallery",
        "gallery-item",
        "gallerytext",
        "thumb",
    }
    if element_id in gallery_tokens:
        return True
    return any(token in classes for token in gallery_tokens)


def _is_category_listing_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = attr_tokens(attrs.get("class"))
    if any(
        token in classes
        for token in {
            "category-page__members",
            "category-page__members-for-char",
            "gallery-grid",
            "gallery-cards",
            "cards-gallery",
            "media-gallery",
            "roster-gallery",
            "directory-grid",
            "directory-list",
            "media-directory",
            "asset-directory",
            "listing-grid",
            "listing-cards",
        }
    ):
        return True
    if tag == "table" and any(token in classes for token in {"wikitable", "item-table", "roster-table", "items-table"}):
        return True
    if tag not in {"div", "section", "ul", "ol"}:
        return False
    return any(
        token in classes
        for token in {
            "card-grid",
            "item-grid",
            "cards-grid",
            "roster-grid",
            "tiles-grid",
            "gallery-grid",
            "gallery-cards",
            "cards-gallery",
            "media-gallery",
            "roster-gallery",
            "directory-grid",
            "directory-list",
            "media-directory",
            "asset-directory",
            "listing-grid",
            "listing-cards",
        }
    )


def _is_category_member_container(tag: str, attrs: dict[str, str | None]) -> bool:
    classes = attr_tokens(attrs.get("class"))
    if "category-page__member" in classes:
        return True
    if tag not in {"article", "div", "li"}:
        return False
    return any(
        token in classes
        for token in {
            "card-item",
            "item-card",
            "grid-card",
            "member-card",
            "tile-card",
            "gallery-card",
            "gallery-member",
            "media-card",
            "thumb-card",
            "gallery-tile",
            "directory-card",
            "directory-item",
            "listing-card",
            "listing-item",
            "media-tile",
            "directory-tile",
        }
    )
