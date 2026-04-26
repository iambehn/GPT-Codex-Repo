"""
Analytics logging and dashboard data helpers.

Google Sheets remains the V1 source of truth. The normalized model uses three
worksheets:
  - Posts: one row per published platform post.
  - MetricSnapshots: one row per post per metric collection time.
  - Decisions: deterministic analytics decisions computed from latest metrics.

The legacy "Analytics" worksheet is still written by log_clip for backwards
compatibility with earlier spreadsheets.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)

SUPPORTED_PLATFORMS = {
    "youtube_shorts",
    "tiktok",
    "instagram_reels",
    "twitter_x",
    "reddit",
}

LEGACY_HEADERS = [
    "clip_id",
    "game",
    "pipeline_date",
    "downloaded_at",
    "duration_seconds",
    "quality_tag",
    "resolution_height",
    "fps",
    "motion_level",
    "audio_energy",
    "keywords",
    "highlight_score",
    "clip_type",
    "suggested_title",
    "suggested_caption",
    "score_reasoning",
    "review_status",
    "reviewed_at",
    "template_id",
    "youtube_url",
    "tiktok_publish_id",
    "instagram_url",
    "twitter_url",
    "reddit_url",
    "distributed_at",
]

POSTS_HEADERS = [
    "post_id",
    "clip_id",
    "game",
    "platform",
    "channel",
    "url",
    "posted_at",
    "title",
    "caption",
    "template_id",
    "hook_type",
    "title_category",
    "hook_gate_passed",
    "composite_score",
    "player_entity",
    "detected_event",
    "source_meta_path",
    "created_at",
]

METRIC_HEADERS = [
    "snapshot_id",
    "post_id",
    "platform",
    "channel",
    "source_platform",
    "snapshot_at",
    "views",
    "likes",
    "comments",
    "shares",
    "saves",
    "retention",
    "rewatch_rate",
    "follows",
    "profile_visits",
    "paid_spend",
    "organic_views",
    "paid_views",
    "import_batch_id",
    "imported_at",
    "raw_field_map",
]

DECISION_HEADERS = [
    "decision_id",
    "post_id",
    "computed_at",
    "boost_candidate",
    "recycle_candidate",
    "underperforming",
    "winner_tier",
    "decision_reason",
    "score",
    "post_age_hours",
    "engagement_rate",
    "follower_conversion",
    "paid_status",
]

_NUMERIC_FIELDS = {
    "views",
    "likes",
    "comments",
    "shares",
    "saves",
    "follows",
    "profile_visits",
    "paid_spend",
    "organic_views",
    "paid_views",
}
_RATE_FIELDS = {"retention", "rewatch_rate"}

_FIELD_ALIASES = {
    "post_id": ("post_id", "post id", "video_id", "video id", "publish_id", "id"),
    "url": ("url", "post_url", "post url", "video_url", "video url", "permalink", "link"),
    "platform": ("platform", "source_platform", "source platform"),
    "channel": ("channel", "account", "account_name", "username", "subreddit"),
    "snapshot_at": ("snapshot_at", "snapshot at", "collected_at", "date", "timestamp", "time"),
    "views": ("views", "view_count", "video_views", "plays", "play_count", "impressions"),
    "likes": ("likes", "like_count", "favorites"),
    "comments": ("comments", "comment_count", "replies"),
    "shares": ("shares", "share_count", "retweets", "reposts"),
    "saves": ("saves", "save_count", "bookmarks", "favorites"),
    "retention": ("retention", "avg_retention", "average_retention", "watch_percentage"),
    "rewatch_rate": ("rewatch_rate", "rewatches", "replay_rate", "loops"),
    "follows": ("follows", "followers", "followers_gained", "new_followers"),
    "profile_visits": ("profile_visits", "profile views", "profile_clicks", "profile_visits"),
    "paid_spend": ("paid_spend", "spend", "cost", "amount_spent"),
    "organic_views": ("organic_views", "organic views"),
    "paid_views": ("paid_views", "paid views", "promoted_views"),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_sheets_client():
    """Return an authenticated gspread client via service account."""
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_path:
        raise EnvironmentError(
            "GOOGLE_SERVICE_ACCOUNT_JSON not set in .env; point it at your service account key file."
        )
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        raise ImportError("Run: pip install gspread google-auth") from e

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)


def _open_spreadsheet():
    sheets_id = os.getenv("GOOGLE_SHEETS_ID")
    if not sheets_id:
        raise EnvironmentError("GOOGLE_SHEETS_ID not set; analytics sheets unavailable.")
    return _get_sheets_client().open_by_key(sheets_id)


def _worksheet(spreadsheet, title: str, headers: list[str]):
    try:
        ws = spreadsheet.worksheet(title)
    except Exception:
        ws = spreadsheet.add_worksheet(title=title, rows=10000, cols=len(headers))
    _ensure_headers(ws, headers)
    return ws


def _ensure_headers(worksheet, headers: list[str]) -> None:
    existing = worksheet.row_values(1)
    if not existing:
        worksheet.append_row(headers, value_input_option="RAW")


def _append_dict_rows(worksheet, headers: list[str], rows: list[dict[str, Any]]) -> None:
    for row in rows:
        worksheet.append_row([_sheet_value(row.get(header, "")) for header in headers], value_input_option="USER_ENTERED")


def _sheet_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return "" if value is None else value


def _records(worksheet) -> list[dict[str, Any]]:
    try:
        return worksheet.get_all_records()
    except Exception:
        return []


def log_clip(metadata: dict, distribution_results: dict, config: dict | None = None) -> bool:
    """Log distribution results to legacy and normalized analytics worksheets."""
    if config is not None and not config.get("analytics", {}).get("enabled", True):
        logger.debug("Analytics disabled in config; skipping.")
        return True

    if not os.getenv("GOOGLE_SHEETS_ID"):
        logger.debug("GOOGLE_SHEETS_ID not set; analytics logging skipped.")
        return True

    try:
        spreadsheet = _open_spreadsheet()
        _log_legacy_clip(spreadsheet, metadata, distribution_results)
        post_rows = build_post_rows(metadata, distribution_results, config or {})
        if post_rows:
            ws_posts = _worksheet(spreadsheet, "Posts", POSTS_HEADERS)
            existing_ids = {str(row.get("post_id", "")) for row in _records(ws_posts)}
            new_rows = [row for row in post_rows if row["post_id"] not in existing_ids]
            _append_dict_rows(ws_posts, POSTS_HEADERS, new_rows)
        logger.info(f"Analytics logged: {metadata.get('clip_id')} -> Google Sheets")
        return True
    except Exception as e:
        logger.error(f"Analytics logging failed (non-fatal): {e}")
        return False


def _log_legacy_clip(spreadsheet, metadata: dict, distribution_results: dict) -> None:
    ws = _worksheet(spreadsheet, "Analytics", LEGACY_HEADERS)
    scoring = metadata.get("scoring", {})
    dist = distribution_results or {}

    def _url(platform: str) -> str:
        return dist.get(platform, {}).get("url") or ""

    row = [
        metadata.get("clip_id", ""),
        metadata.get("game", ""),
        _now_iso(),
        metadata.get("downloaded_at", ""),
        metadata.get("duration_seconds", ""),
        metadata.get("quality_tag", ""),
        metadata.get("resolution_height", ""),
        metadata.get("fps", ""),
        metadata.get("motion_level", ""),
        metadata.get("audio_energy", ""),
        ", ".join(metadata.get("keywords", [])),
        scoring.get("highlight_score", ""),
        scoring.get("clip_type", ""),
        scoring.get("suggested_title", ""),
        scoring.get("suggested_caption", ""),
        scoring.get("score_reasoning", ""),
        metadata.get("review_status", ""),
        metadata.get("reviewed_at", ""),
        metadata.get("selected_template_id", ""),
        _url("youtube_shorts"),
        dist.get("tiktok", {}).get("publish_id") or "",
        _url("instagram_reels"),
        _url("twitter_x"),
        _url("reddit"),
        _now_iso(),
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")


def build_post_rows(
    metadata: dict,
    distribution_results: dict,
    config: dict | None = None,
    now: str | None = None,
) -> list[dict[str, Any]]:
    """Build normalized Posts rows from a clip's distribution results."""
    created_at = now or _now_iso()
    clip_id = str(metadata.get("clip_id", "") or "")
    game = str(metadata.get("game", "") or "")
    title_engine = metadata.get("title_engine") or {}
    scoring = metadata.get("scoring") or {}
    decision = metadata.get("decision") or {}
    context = metadata.get("context") or {}

    title = title_engine.get("title") or scoring.get("suggested_title") or clip_id
    caption = title_engine.get("caption") or scoring.get("suggested_caption") or ""
    hook_alignment = decision.get("hook_alignment") or {}

    rows: list[dict[str, Any]] = []
    for platform, result in (distribution_results or {}).items():
        if platform not in SUPPORTED_PLATFORMS:
            continue
        if not _is_published_result(result):
            continue
        identifier = result.get("url") or result.get("publish_id") or result.get("id") or clip_id
        rows.append({
            "post_id": make_post_id(clip_id, platform, identifier),
            "clip_id": clip_id,
            "game": game,
            "platform": platform,
            "channel": _platform_channel(platform, metadata, config or {}),
            "url": result.get("url") or "",
            "posted_at": result.get("posted_at") or metadata.get("distributed_at") or created_at,
            "title": title,
            "caption": caption,
            "template_id": metadata.get("selected_template_id") or title_engine.get("template_used") or "",
            "hook_type": hook_alignment.get("mode") or context.get("detected_event") or "",
            "title_category": title_engine.get("category") or "",
            "hook_gate_passed": decision.get("hook_gate_passed", ""),
            "composite_score": decision.get("composite_score", ""),
            "player_entity": context.get("player_entity") or "",
            "detected_event": context.get("detected_event") or "",
            "source_meta_path": metadata.get("meta_path") or "",
            "created_at": created_at,
        })
    return rows


def _is_published_result(result: Any) -> bool:
    if not isinstance(result, dict):
        return False
    return bool(result.get("success") or result.get("url") or result.get("publish_id") or result.get("id"))


def _platform_channel(platform: str, metadata: dict, config: dict) -> str:
    if platform == "reddit":
        game = metadata.get("game")
        subreddits = (((config.get("distribution") or {}).get("platforms") or {}).get("reddit") or {}).get("subreddits") or {}
        return subreddits.get(game, "")
    return str(metadata.get("channel") or metadata.get("account") or "")


def make_post_id(clip_id: str, platform: str, identifier: str) -> str:
    stable_identifier = identifier or clip_id
    raw = f"{platform}|{stable_identifier}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{platform}_{digest}"


def make_snapshot_id(row: dict[str, Any]) -> str:
    raw = "|".join(str(row.get(key, "")) for key in (
        "post_id",
        "platform",
        "snapshot_at",
        "views",
        "likes",
        "comments",
        "shares",
        "saves",
        "paid_spend",
    ))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def normalize_metric_import(
    payload: str,
    source_platform: str,
    *,
    existing_snapshot_ids: set[str] | None = None,
    imported_at: str | None = None,
    import_batch_id: str | None = None,
) -> dict[str, Any]:
    """Parse CSV/JSON metric payload into normalized MetricSnapshots rows."""
    platform = _normalize_platform(source_platform)
    if platform not in SUPPORTED_PLATFORMS:
        return {"ok": False, "rows": [], "errors": [f"unsupported platform: {source_platform}"], "warnings": []}

    imported_at = imported_at or _now_iso()
    import_batch_id = import_batch_id or f"manual_{hashlib.sha1((platform + imported_at).encode()).hexdigest()[:10]}"
    existing_snapshot_ids = existing_snapshot_ids or set()

    try:
        raw_rows = _parse_metric_payload(payload)
    except ValueError as e:
        return {"ok": False, "rows": [], "errors": [str(e)], "warnings": []}

    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []
    for index, raw in enumerate(raw_rows, start=1):
        normalized, row_errors = _normalize_metric_row(raw, platform, imported_at, import_batch_id)
        if row_errors:
            errors.extend(f"row {index}: {err}" for err in row_errors)
            continue
        if normalized["snapshot_id"] in existing_snapshot_ids:
            warnings.append(f"row {index}: duplicate snapshot skipped")
            continue
        existing_snapshot_ids.add(normalized["snapshot_id"])
        rows.append(normalized)

    return {"ok": not errors, "rows": rows, "errors": errors, "warnings": warnings}


def _parse_metric_payload(payload: str) -> list[dict[str, Any]]:
    text = (payload or "").strip()
    if not text:
        raise ValueError("metric import payload is empty")

    if text.startswith("{") or text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON import is malformed: {e}") from e
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            for key in ("metrics", "rows", "data"):
                if isinstance(parsed.get(key), list):
                    return [item for item in parsed[key] if isinstance(item, dict)]
            return [parsed]
        raise ValueError("JSON import must be an object or list of objects")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV import is missing a header row")
    return [dict(row) for row in reader]


def _normalize_metric_row(
    raw: dict[str, Any],
    platform: str,
    imported_at: str,
    import_batch_id: str,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    lowered = {_normalize_key(key): value for key, value in raw.items()}

    def pick(field: str) -> Any:
        for alias in _FIELD_ALIASES.get(field, (field,)):
            key = _normalize_key(alias)
            if key in lowered and lowered[key] not in ("", None):
                return lowered[key]
        return ""

    raw_post_id = str(pick("post_id") or "").strip()
    url = str(pick("url") or "").strip()
    post_id = raw_post_id or (make_post_id("", platform, url) if url else "")
    if not post_id:
        errors.append("post_id or url is required")

    row = {
        "snapshot_id": "",
        "post_id": post_id,
        "platform": platform,
        "channel": str(pick("channel") or "").strip(),
        "source_platform": platform,
        "snapshot_at": str(pick("snapshot_at") or imported_at),
        "views": _to_number(pick("views"), integer=True),
        "likes": _to_number(pick("likes"), integer=True),
        "comments": _to_number(pick("comments"), integer=True),
        "shares": _to_number(pick("shares"), integer=True),
        "saves": _to_number(pick("saves"), integer=True),
        "retention": _to_rate(pick("retention")),
        "rewatch_rate": _to_rate(pick("rewatch_rate")),
        "follows": _to_number(pick("follows"), integer=True),
        "profile_visits": _to_number(pick("profile_visits"), integer=True),
        "paid_spend": _to_number(pick("paid_spend"), integer=False),
        "organic_views": _to_number(pick("organic_views"), integer=True),
        "paid_views": _to_number(pick("paid_views"), integer=True),
        "import_batch_id": import_batch_id,
        "imported_at": imported_at,
        "raw_field_map": json.dumps(sorted(raw.keys())),
    }
    row["snapshot_id"] = make_snapshot_id(row)
    return row, errors


def import_metric_payload(config: dict, source_platform: str, payload: str) -> dict[str, Any]:
    """Normalize a manual import and append MetricSnapshots/Decisions rows."""
    if not config.get("analytics", {}).get("enabled", True):
        return {"ok": False, "imported": 0, "errors": ["analytics disabled"], "warnings": []}

    try:
        spreadsheet = _open_spreadsheet()
        ws_posts = _worksheet(spreadsheet, "Posts", POSTS_HEADERS)
        ws_metrics = _worksheet(spreadsheet, "MetricSnapshots", METRIC_HEADERS)
        ws_decisions = _worksheet(spreadsheet, "Decisions", DECISION_HEADERS)
        existing_ids = {str(row.get("snapshot_id", "")) for row in _records(ws_metrics)}
    except Exception as e:
        return {"ok": False, "imported": 0, "errors": [str(e)], "warnings": []}

    normalized = normalize_metric_import(payload, source_platform, existing_snapshot_ids=existing_ids)
    if normalized["rows"]:
        _append_dict_rows(ws_metrics, METRIC_HEADERS, normalized["rows"])

    posts = _records(ws_posts)
    metrics = _records(ws_metrics) + normalized["rows"]
    affected = {row["post_id"] for row in normalized["rows"]}
    decisions = [
        row for row in compute_decision_rows(posts, metrics, config)
        if row["post_id"] in affected
    ]
    if decisions:
        _append_dict_rows(ws_decisions, DECISION_HEADERS, decisions)

    return {
        "ok": normalized["ok"],
        "imported": len(normalized["rows"]),
        "decisions": len(decisions),
        "errors": normalized["errors"],
        "warnings": normalized["warnings"],
    }


def read_analytics_tables(config: dict) -> dict[str, Any]:
    """Read normalized analytics worksheets, returning an empty state if unconfigured."""
    if not config.get("analytics", {}).get("enabled", True):
        return {"configured": False, "error": "analytics disabled", "posts": [], "metrics": [], "decisions": []}
    if not os.getenv("GOOGLE_SHEETS_ID") or not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"):
        return {
            "configured": False,
            "error": "GOOGLE_SHEETS_ID or GOOGLE_SERVICE_ACCOUNT_JSON not configured",
            "posts": [],
            "metrics": [],
            "decisions": [],
        }

    try:
        spreadsheet = _open_spreadsheet()
        return {
            "configured": True,
            "error": None,
            "posts": _records(_worksheet(spreadsheet, "Posts", POSTS_HEADERS)),
            "metrics": _records(_worksheet(spreadsheet, "MetricSnapshots", METRIC_HEADERS)),
            "decisions": _records(_worksheet(spreadsheet, "Decisions", DECISION_HEADERS)),
        }
    except Exception as e:
        return {"configured": False, "error": str(e), "posts": [], "metrics": [], "decisions": []}


def build_dashboard_state(config: dict, filters: dict[str, str] | None = None) -> dict[str, Any]:
    """Build the Flask dashboard view model from normalized analytics tables."""
    tables = read_analytics_tables(config)
    filters = filters or {}
    posts = [_coerce_record(row) for row in tables["posts"]]
    metrics = [_coerce_record(row) for row in tables["metrics"]]
    decisions = [_coerce_record(row) for row in tables["decisions"]]

    latest_metric = _latest_by(metrics, "post_id", "snapshot_at")
    latest_decision = _latest_by(decisions, "post_id", "computed_at")
    if not latest_decision and posts and metrics:
        latest_decision = {
            row["post_id"]: row
            for row in compute_decision_rows(posts, metrics, config)
        }

    joined = []
    for post in posts:
        post_id = str(post.get("post_id", ""))
        item = {
            **post,
            "metrics": latest_metric.get(post_id, {}),
            "decision": latest_decision.get(post_id, {}),
        }
        item["score"] = _float(item["decision"].get("score"), 0.0)
        item["views"] = _int(item["metrics"].get("views"), 0)
        item["retention"] = _float(item["metrics"].get("retention"), 0.0)
        joined.append(item)

    filtered = _filter_posts(joined, filters)
    return {
        "configured": tables["configured"],
        "error": tables["error"],
        "filters": filters,
        "overview": _overview(filtered),
        "posts": sorted(filtered, key=lambda item: item.get("score", 0.0), reverse=True),
        "top_performers": sorted(filtered, key=lambda item: item.get("score", 0.0), reverse=True)[:10],
        "boost_candidates": [item for item in filtered if _truthy(item["decision"].get("boost_candidate"))],
        "recycle_candidates": [item for item in filtered if _truthy(item["decision"].get("recycle_candidate"))],
        "underperformers": [item for item in filtered if _truthy(item["decision"].get("underperforming"))],
        "platforms": sorted({str(item.get("platform", "")) for item in joined if item.get("platform")}),
        "games": sorted({str(item.get("game", "")) for item in joined if item.get("game")}),
        "templates": sorted({str(item.get("template_id", "")) for item in joined if item.get("template_id")}),
        "hook_types": sorted({str(item.get("hook_type", "")) for item in joined if item.get("hook_type")}),
    }


def build_post_detail(config: dict, post_id: str) -> dict[str, Any] | None:
    tables = read_analytics_tables(config)
    posts = [_coerce_record(row) for row in tables["posts"]]
    metrics = [_coerce_record(row) for row in tables["metrics"]]
    decisions = [_coerce_record(row) for row in tables["decisions"]]
    post = next((row for row in posts if str(row.get("post_id")) == post_id), None)
    if not post:
        return None
    post_metrics = [row for row in metrics if str(row.get("post_id")) == post_id]
    post_decisions = [row for row in decisions if str(row.get("post_id")) == post_id]
    if not post_decisions and post_metrics:
        post_decisions = [row for row in compute_decision_rows([post], metrics, config) if row["post_id"] == post_id]
    return {
        "configured": tables["configured"],
        "error": tables["error"],
        "post": post,
        "metrics": sorted(post_metrics, key=lambda row: str(row.get("snapshot_at", "")), reverse=True),
        "decisions": sorted(post_decisions, key=lambda row: str(row.get("computed_at", "")), reverse=True),
    }


def compute_decision_rows(posts: list[dict[str, Any]], metrics: list[dict[str, Any]], config: dict) -> list[dict[str, Any]]:
    """Compute deterministic analytics decisions from latest post metrics."""
    rules = _decision_rules(config)
    latest_metric = _latest_by([_coerce_record(row) for row in metrics], "post_id", "snapshot_at")
    scored: list[tuple[dict[str, Any], float]] = []

    for post in [_coerce_record(row) for row in posts]:
        metric = latest_metric.get(str(post.get("post_id", "")))
        if not metric:
            continue
        score = _post_score(metric)
        scored.append((post, score))

    winner_cutoff = _percentile([score for _, score in scored], float(rules["winner_percentile"]))
    rows: list[dict[str, Any]] = []
    computed_at = _now_iso()
    for post, score in scored:
        metric = latest_metric.get(str(post.get("post_id", "")), {})
        views = _int(metric.get("views"), 0)
        retention = _float(metric.get("retention"), 0.0)
        shares = _int(metric.get("shares"), 0)
        likes = _int(metric.get("likes"), 0)
        comments = _int(metric.get("comments"), 0)
        saves = _int(metric.get("saves"), 0)
        follows = _int(metric.get("follows"), 0)
        organic_views = _int(metric.get("organic_views"), 0)
        paid_views = _int(metric.get("paid_views"), 0)
        paid_spend = _float(metric.get("paid_spend"), 0.0)

        engagement_rate = (likes + comments + shares + saves) / views if views else 0.0
        share_rate = shares / views if views else 0.0
        follower_conversion = follows / views if views else 0.0
        post_age_hours = _age_hours(post.get("posted_at"), computed_at)
        paid_status = "paid" if paid_spend > 0 or paid_views > 0 else "organic"

        boost = (
            paid_status == "organic"
            and views >= int(rules["min_views_for_decision"])
            and retention >= float(rules["boost_retention_min"])
            and (
                engagement_rate >= float(rules["boost_engagement_rate_min"])
                or share_rate >= float(rules["boost_share_rate_min"])
            )
        )
        recycle = (
            retention >= float(rules["recycle_retention_min"])
            and views <= int(rules["recycle_views_max"])
            and paid_status == "organic"
        )
        underperforming = (
            views >= int(rules["underperform_views_min"])
            and retention <= float(rules["underperform_retention_max"])
        )
        winner_tier = score >= winner_cutoff and views >= int(rules["min_views_for_decision"])

        reason = _decision_reason(boost, recycle, underperforming, winner_tier, retention, engagement_rate, views, paid_status)
        post_id = str(post.get("post_id", ""))
        rows.append({
            "decision_id": hashlib.sha1(f"{post_id}|{computed_at}|{score}".encode()).hexdigest()[:16],
            "post_id": post_id,
            "computed_at": computed_at,
            "boost_candidate": boost,
            "recycle_candidate": recycle,
            "underperforming": underperforming,
            "winner_tier": winner_tier,
            "decision_reason": reason,
            "score": round(score, 4),
            "post_age_hours": round(post_age_hours, 2) if post_age_hours is not None else "",
            "engagement_rate": round(engagement_rate, 4),
            "follower_conversion": round(follower_conversion, 4),
            "paid_status": paid_status,
        })
    return rows


def _decision_rules(config: dict) -> dict[str, Any]:
    rules = {
        "min_views_for_decision": 500,
        "boost_retention_min": 0.60,
        "boost_engagement_rate_min": 0.04,
        "boost_share_rate_min": 0.01,
        "recycle_retention_min": 0.55,
        "recycle_views_max": 1500,
        "underperform_retention_max": 0.35,
        "underperform_views_min": 500,
        "winner_percentile": 0.90,
    }
    rules.update(((config.get("analytics") or {}).get("decision_rules") or {}))
    return rules


def _decision_reason(
    boost: bool,
    recycle: bool,
    underperforming: bool,
    winner_tier: bool,
    retention: float,
    engagement_rate: float,
    views: int,
    paid_status: str,
) -> str:
    reasons = []
    if winner_tier:
        reasons.append("winner-tier score")
    if boost:
        reasons.append("organic retention and engagement clear boost thresholds")
    if recycle:
        reasons.append("strong retention with limited distribution")
    if underperforming:
        reasons.append("low retention after enough impressions")
    if not reasons:
        reasons.append("monitor until stronger signal accumulates")
    return f"{'; '.join(reasons)} (retention={retention:.2f}, engagement={engagement_rate:.2f}, views={views}, {paid_status})"


def _post_score(metric: dict[str, Any]) -> float:
    views = _int(metric.get("views"), 0)
    retention = _float(metric.get("retention"), 0.0)
    shares = _int(metric.get("shares"), 0)
    saves = _int(metric.get("saves"), 0)
    follows = _int(metric.get("follows"), 0)
    profile_visits = _int(metric.get("profile_visits"), 0)
    share_rate = shares / views if views else 0.0
    save_rate = saves / views if views else 0.0
    follow_rate = follows / views if views else 0.0
    profile_rate = profile_visits / views if views else 0.0
    volume = min(1.0, views / 10000.0)
    return (
        retention * 0.45
        + share_rate * 8.0 * 0.20
        + save_rate * 8.0 * 0.15
        + follow_rate * 20.0 * 0.15
        + profile_rate * 10.0 * 0.03
        + volume * 0.02
    )


def _overview(posts: list[dict[str, Any]]) -> dict[str, Any]:
    views = sum(_int(item.get("metrics", {}).get("views"), 0) for item in posts)
    follows = sum(_int(item.get("metrics", {}).get("follows"), 0) for item in posts)
    retention_values = [_float(item.get("metrics", {}).get("retention"), 0.0) for item in posts if item.get("metrics")]
    return {
        "posts_tracked": len(posts),
        "total_views": views,
        "avg_retention": round(sum(retention_values) / len(retention_values), 3) if retention_values else 0.0,
        "follower_conversion": round(follows / views, 4) if views else 0.0,
        "top_game": _top_group(posts, "game"),
        "top_template": _top_group(posts, "template_id"),
    }


def _filter_posts(posts: list[dict[str, Any]], filters: dict[str, str]) -> list[dict[str, Any]]:
    result = posts
    for key in ("platform", "game", "template_id", "hook_type"):
        value = (filters.get(key) or "").strip()
        if value:
            result = [item for item in result if str(item.get(key, "")) == value]

    paid_filter = (filters.get("paid") or "").strip()
    if paid_filter == "paid":
        result = [item for item in result if _float(item.get("metrics", {}).get("paid_spend"), 0.0) > 0 or _int(item.get("metrics", {}).get("paid_views"), 0) > 0]
    elif paid_filter == "organic":
        result = [item for item in result if _float(item.get("metrics", {}).get("paid_spend"), 0.0) <= 0 and _int(item.get("metrics", {}).get("paid_views"), 0) <= 0]
    return result


def _latest_by(rows: list[dict[str, Any]], id_field: str, time_field: str) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get(id_field, ""))
        if not key:
            continue
        existing = latest.get(key)
        if existing is None or str(row.get(time_field, "")) >= str(existing.get(time_field, "")):
            latest[key] = row
    return latest


def _top_group(posts: list[dict[str, Any]], key: str) -> str:
    totals: dict[str, float] = {}
    for item in posts:
        group = str(item.get(key) or "")
        if not group:
            continue
        totals[group] = totals.get(group, 0.0) + float(item.get("score", 0.0))
    if not totals:
        return ""
    return max(totals.items(), key=lambda pair: pair[1])[0]


def _normalize_platform(raw: str) -> str:
    platform = (raw or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "youtube": "youtube_shorts",
        "shorts": "youtube_shorts",
        "instagram": "instagram_reels",
        "ig": "instagram_reels",
        "x": "twitter_x",
        "twitter": "twitter_x",
        "tiktok": "tiktok",
        "reddit": "reddit",
    }
    return aliases.get(platform, platform)


def _normalize_key(raw: str) -> str:
    return str(raw or "").strip().lower().replace("-", "_").replace(" ", "_")


def _coerce_record(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field in _NUMERIC_FIELDS:
        if field in result:
            result[field] = _to_number(result[field], integer=field != "paid_spend")
    for field in _RATE_FIELDS:
        if field in result:
            result[field] = _to_rate(result[field])
    return result


def _to_number(value: Any, *, integer: bool) -> int | float:
    if value in ("", None):
        return 0 if integer else 0.0
    if isinstance(value, (int, float)):
        return int(value) if integer else round(float(value), 4)
    text = str(value).strip().replace(",", "").replace("$", "")
    if text.endswith("%"):
        text = text[:-1]
    try:
        number = float(text)
    except ValueError:
        return 0 if integer else 0.0
    return int(round(number)) if integer else round(number, 4)


def _to_rate(value: Any) -> float:
    if value in ("", None):
        return 0.0
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip().replace(",", "")
        is_percent = text.endswith("%")
        if is_percent:
            text = text[:-1]
        try:
            number = float(text)
        except ValueError:
            return 0.0
        if is_percent:
            number /= 100.0
    if number > 1.0 and number <= 100.0:
        number /= 100.0
    return round(max(0.0, min(1.0, number)), 4)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _age_hours(start: Any, end: Any) -> float | None:
    try:
        start_dt = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        return max(0.0, (end_dt - start_dt).total_seconds() / 3600.0)
    except (TypeError, ValueError):
        return None


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    percentile = max(0.0, min(1.0, percentile))
    index = int(round((len(values) - 1) * percentile))
    return values[index]
