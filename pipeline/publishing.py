"""Small publishing text helpers used by active packaging code paths."""

from __future__ import annotations


def publishing_title(metadata: dict, limit: int | None = None, default: str = "Gaming Clip") -> str:
    """Return the canonical platform title with deterministic fallbacks."""
    title_engine = metadata.get("title_engine") or {}
    scoring = metadata.get("scoring") or {}
    title = (
        title_engine.get("title")
        or scoring.get("suggested_title")
        or metadata.get("clip_id")
        or default
    )
    title = str(title).strip() or default
    return title[:limit].strip() if limit else title


def publishing_caption(metadata: dict, limit: int | None = None) -> str:
    """Return canonical caption/body text with title and hashtag fallbacks."""
    title_engine = metadata.get("title_engine") or {}
    scoring = metadata.get("scoring") or {}
    caption = title_engine.get("caption") or scoring.get("suggested_caption")

    if not caption:
        title = publishing_title(metadata)
        hashtags = " ".join(str(tag) for tag in (title_engine.get("hashtags") or []) if tag)
        caption = " ".join(part for part in (title, hashtags) if part)

    caption = str(caption).strip()
    return caption[:limit].strip() if limit else caption
