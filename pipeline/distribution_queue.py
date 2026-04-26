"""SQLite-backed distribution queue and compliance scheduler.

This module keeps operational posting state out of clip folders while leaving
Google Sheets as the analytics source of truth. Direct publishing only happens
for accounts configured with policy_mode=official_api; other accounts receive a
manual publish pack instead of browser automation.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pipeline.distribution import _publishing_caption, _publishing_title, upload_to_platform
from pipeline.game_pack import list_supported_games
from utils.analytics import log_clip
from utils.logger import get_logger

logger = get_logger(__name__)

TASK_STATES = {
    "ready",
    "scheduled",
    "needs_human_publish",
    "posting",
    "posted",
    "failed_retryable",
    "failed_terminal",
    "paused_compliance",
}

RETRYABLE_ERRORS = {"rate_limit", "transient_network", "platform_unavailable"}
TERMINAL_ERRORS = {"auth_error", "content_rejected", "invalid_media"}
SCHEMA_VERSION = "distribution_queue.v1"

_PLATFORM_ENV_VARS = {
    "youtube_shorts": ("YOUTUBE_CLIENT_ID", "YOUTUBE_CLIENT_SECRET"),
    "tiktok": ("TIKTOK_ACCESS_TOKEN",),
    "instagram_reels": ("INSTAGRAM_ACCESS_TOKEN", "INSTAGRAM_ACCOUNT_ID"),
    "twitter_x": (
        "TWITTER_API_KEY",
        "TWITTER_API_SECRET",
        "TWITTER_ACCESS_TOKEN",
        "TWITTER_ACCESS_SECRET",
    ),
    "reddit": ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USERNAME", "REDDIT_PASSWORD"),
}

_MANUAL_CHECKLIST = [
    "Confirm the video is the intended final edit.",
    "Confirm the title/caption matches platform policy and account voice.",
    "Confirm copyrighted audio risk is acceptable or audio was replaced/muted.",
    "Publish using the platform's official app or website, then mark this task posted.",
]


def schedule_distribution_tasks(config: dict, now: datetime | None = None) -> dict[str, Any]:
    """Create queue tasks for accepted clips and configured accounts."""
    now_dt = _as_utc(now)
    conn = _connect(config)
    try:
        _init_db(conn)
        accounts = _enabled_accounts(config)
        accepted_items = _accepted_clips(config)
        created = 0
        skipped = 0
        paused = 0
        manual = 0

        for item in accepted_items:
            metadata = item["metadata"]
            template_targets = _template_targets(metadata, config)
            for account in accounts:
                platform = account["platform"]
                if template_targets and platform not in template_targets:
                    skipped += 1
                    continue

                task_id = _task_id(metadata.get("clip_id", item["clip_path"].stem), platform, account["account_id"])
                if _task_exists(conn, task_id):
                    skipped += 1
                    continue

                scheduled_at = _next_scheduled_time(conn, account, config, now_dt)
                compliance = _compliance_check(conn, item, account, config, now_dt)
                status = _initial_status(account, compliance, scheduled_at, now_dt)
                manual_pack_path = ""
                if status == "needs_human_publish":
                    manual_pack_path = _write_manual_pack(item, account, scheduled_at, config, task_id)
                    manual += 1
                if status == "paused_compliance":
                    paused += 1

                task = {
                    "task_id": task_id,
                    "clip_id": str(metadata.get("clip_id") or item["clip_path"].stem),
                    "game": str(metadata.get("game") or item["game"]),
                    "platform": platform,
                    "account_id": account["account_id"],
                    "channel": account.get("channel", ""),
                    "clip_path": str(item["clip_path"]),
                    "meta_path": str(item["meta_path"]),
                    "title": _publishing_title(metadata),
                    "caption": _publishing_caption(metadata),
                    "hashtags_json": json.dumps((metadata.get("title_engine") or {}).get("hashtags") or []),
                    "scheduled_at": _iso(scheduled_at),
                    "status": status,
                    "policy_mode": account.get("policy_mode", "human_assisted"),
                    "attempt_count": 0,
                    "last_error_class": "",
                    "last_error": "",
                    "next_attempt_at": "",
                    "published_url": "",
                    "publish_id": "",
                    "manual_pack_path": manual_pack_path,
                    "compliance_reason": compliance.get("reason", ""),
                    "created_at": _iso(now_dt),
                    "updated_at": _iso(now_dt),
                }
                _insert_task(conn, task)
                _record_compliance(conn, task_id, compliance, now_dt)
                created += 1

        conn.commit()
        return {"ok": True, "created": created, "skipped": skipped, "paused": paused, "manual": manual}
    finally:
        conn.close()


def run_distribution_queue(
    config: dict,
    limit: int | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Run due official-API distribution tasks."""
    now_dt = _as_utc(now)
    conn = _connect(config)
    try:
        _init_db(conn)
        tasks = _due_tasks(conn, now_dt, limit)
        posted = 0
        retryable = 0
        terminal = 0
        skipped = 0

        for task in tasks:
            if task["policy_mode"] != "official_api":
                skipped += 1
                continue
            if dry_run:
                skipped += 1
                logger.info(f"[distribution_queue] DRY RUN would post {task['task_id']} to {task['platform']}")
                continue

            _update_task(conn, task["task_id"], status="posting", updated_at=_iso(now_dt))
            conn.commit()
            start = time.monotonic()
            result: dict[str, Any]
            try:
                metadata = _read_json(Path(task["meta_path"]))
                account = _account_by_id(config, task["account_id"]) or {
                    "account_id": task["account_id"],
                    "platform": task["platform"],
                    "policy_mode": task["policy_mode"],
                }
                result = upload_to_platform(task["platform"], task["clip_path"], metadata, config, account=account)
            except Exception as exc:  # defensive: platform adapters should return structured errors.
                result = {"success": False, "url": None, "error": str(exc)}

            duration = time.monotonic() - start
            error_class = classify_distribution_error(result)
            attempt_count = int(task["attempt_count"] or 0) + 1
            _record_attempt(conn, task, result, error_class, duration, now_dt)

            if result.get("success"):
                _mark_task_posted(conn, task, result, now_dt)
                _write_distribution_result(task, result, config)
                posted += 1
            elif error_class in RETRYABLE_ERRORS and attempt_count < _retry_max_attempts(config):
                retry_at = now_dt + _retry_delay(config, attempt_count)
                _update_task(
                    conn,
                    task["task_id"],
                    status="failed_retryable",
                    attempt_count=attempt_count,
                    last_error_class=error_class,
                    last_error=str(result.get("error") or ""),
                    next_attempt_at=_iso(retry_at),
                    updated_at=_iso(now_dt),
                )
                retryable += 1
            else:
                _update_task(
                    conn,
                    task["task_id"],
                    status="failed_terminal",
                    attempt_count=attempt_count,
                    last_error_class=error_class,
                    last_error=str(result.get("error") or ""),
                    updated_at=_iso(now_dt),
                )
                terminal += 1

            _apply_circuit_breaker(conn, task, config, now_dt)
            conn.commit()

        return {
            "ok": True,
            "due": len(tasks),
            "posted": posted,
            "retryable": retryable,
            "terminal": terminal,
            "skipped": skipped,
        }
    finally:
        conn.close()


def mark_manual_posted(task_id: str, url: str, config: dict, now: datetime | None = None) -> dict[str, Any]:
    """Mark a human-assisted task as published and persist the URL."""
    now_dt = _as_utc(now)
    conn = _connect(config)
    try:
        _init_db(conn)
        task = _get_task(conn, task_id)
        if task is None:
            return {"ok": False, "error": f"task not found: {task_id}"}
        if task["status"] == "posted":
            return {"ok": True, "already_posted": True, "task_id": task_id, "url": task["published_url"]}
        if not url:
            return {"ok": False, "error": "url is required"}

        result = {
            "success": True,
            "url": url,
            "posted_at": _iso(now_dt),
            "manual": True,
            "task_id": task_id,
            "account_id": task["account_id"],
        }
        _mark_task_posted(conn, task, result, now_dt)
        _record_attempt(conn, task, result, "none", 0.0, now_dt)
        conn.commit()
        _write_distribution_result(task, result, config)
        return {"ok": True, "task_id": task_id, "url": url}
    finally:
        conn.close()


def distribution_status(config: dict) -> dict[str, Any]:
    """Return queue counts and recent tasks for CLI/dashboard views."""
    conn = _connect(config)
    try:
        _init_db(conn)
        counts = {
            row["status"]: row["count"]
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM distribution_tasks GROUP BY status")
        }
        recent = [dict(row) for row in conn.execute(
            """
            SELECT * FROM distribution_tasks
            ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
            LIMIT 50
            """
        )]
        return {"ok": True, "counts": counts, "recent": recent, "db_path": str(_db_path(config))}
    finally:
        conn.close()


def get_distribution_dashboard(config: dict, filters: dict[str, str] | None = None) -> dict[str, Any]:
    """Build a Flask view model for the Distribution tab."""
    filters = filters or {}
    conn = _connect(config)
    try:
        _init_db(conn)
        where = []
        values: list[Any] = []
        for key in ("status", "platform", "account_id", "game"):
            if filters.get(key):
                where.append(f"{key} = ?")
                values.append(filters[key])
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        tasks = [dict(row) for row in conn.execute(
            f"""
            SELECT * FROM distribution_tasks
            {where_sql}
            ORDER BY datetime(scheduled_at) ASC, datetime(created_at) DESC
            LIMIT 250
            """,
            values,
        )]
        counts = {
            row["status"]: row["count"]
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM distribution_tasks GROUP BY status")
        }
        attempts = [dict(row) for row in conn.execute(
            """
            SELECT * FROM distribution_attempts
            ORDER BY datetime(attempted_at) DESC
            LIMIT 25
            """
        )]
        return {
            "configured": True,
            "db_path": str(_db_path(config)),
            "filters": filters,
            "tasks": tasks,
            "counts": counts,
            "attempts": attempts,
            "states": sorted(TASK_STATES),
            "platforms": sorted({task["platform"] for task in tasks} | {a["platform"] for a in _enabled_accounts(config)}),
            "accounts": sorted({task["account_id"] for task in tasks} | {a["account_id"] for a in _enabled_accounts(config)}),
        }
    except Exception as exc:
        return {"configured": False, "error": str(exc), "tasks": [], "counts": {}, "attempts": []}
    finally:
        conn.close()


def classify_distribution_error(result: dict[str, Any]) -> str:
    """Map adapter responses into retry/terminal classes."""
    if result.get("success"):
        return "none"
    error = str(result.get("error") or result.get("message") or "").lower()
    status_code = str(result.get("status_code") or result.get("code") or "")
    if any(term in error for term in ("token", "credential", "auth", "unauthorized", "forbidden", "not set")):
        return "auth_error"
    if "rate" in error or "quota" in error or status_code == "429":
        return "rate_limit"
    if any(term in error for term in ("timeout", "connection", "temporar", "network", "5xx")) or status_code.startswith("5"):
        return "transient_network"
    if any(term in error for term in ("policy", "copyright", "rejected", "violation")):
        return "content_rejected"
    if any(term in error for term in ("invalid media", "file", "mimetype", "codec", "too large")):
        return "invalid_media"
    if "unavailable" in error or "maintenance" in error:
        return "platform_unavailable"
    return "unknown_error"


def _init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS distribution_tasks (
            task_id TEXT PRIMARY KEY,
            clip_id TEXT NOT NULL,
            game TEXT NOT NULL,
            platform TEXT NOT NULL,
            account_id TEXT NOT NULL,
            channel TEXT,
            clip_path TEXT NOT NULL,
            meta_path TEXT NOT NULL,
            title TEXT,
            caption TEXT,
            hashtags_json TEXT,
            scheduled_at TEXT,
            status TEXT NOT NULL,
            policy_mode TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error_class TEXT,
            last_error TEXT,
            next_attempt_at TEXT,
            published_url TEXT,
            publish_id TEXT,
            manual_pack_path TEXT,
            compliance_reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            schema_version TEXT NOT NULL DEFAULT 'distribution_queue.v1',
            UNIQUE(clip_id, platform, account_id)
        );
        CREATE INDEX IF NOT EXISTS idx_distribution_tasks_status ON distribution_tasks(status);
        CREATE INDEX IF NOT EXISTS idx_distribution_tasks_due ON distribution_tasks(status, scheduled_at, next_attempt_at);

        CREATE TABLE IF NOT EXISTS distribution_attempts (
            attempt_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            attempted_at TEXT NOT NULL,
            duration_seconds REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            error_class TEXT,
            error_message TEXT,
            response_json TEXT,
            schema_version TEXT NOT NULL DEFAULT 'distribution_queue.v1'
        );
        CREATE INDEX IF NOT EXISTS idx_distribution_attempts_task ON distribution_attempts(task_id);

        CREATE TABLE IF NOT EXISTS distribution_compliance (
            record_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            checked_at TEXT NOT NULL,
            status TEXT NOT NULL,
            reason TEXT,
            details_json TEXT,
            schema_version TEXT NOT NULL DEFAULT 'distribution_queue.v1'
        );
        CREATE INDEX IF NOT EXISTS idx_distribution_compliance_task ON distribution_compliance(task_id);
        """
    )


def _connect(config: dict) -> sqlite3.Connection:
    path = _db_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _db_path(config: dict) -> Path:
    queue_cfg = (config.get("distribution") or {})
    raw = queue_cfg.get("queue_db_path") or "data/distribution_queue.sqlite3"
    return _resolve_path(raw)


def _manual_pack_dir(config: dict) -> Path:
    raw = ((config.get("distribution") or {}).get("manual_pack_dir") or "distribution/manual_packs")
    return _resolve_path(raw)


def _resolve_path(raw: str | Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else (Path.cwd() / path).resolve()


def _enabled_accounts(config: dict) -> list[dict[str, Any]]:
    dist = config.get("distribution") or {}
    accounts = [dict(item) for item in (dist.get("accounts") or []) if item.get("enabled", True)]
    if accounts:
        return [_normalize_account(account, config) for account in accounts]

    derived = []
    for platform, platform_cfg in (dist.get("platforms") or {}).items():
        if platform_cfg.get("enabled", False):
            derived.append({
                "account_id": platform,
                "platform": platform,
                "channel": platform,
                "enabled": True,
                "policy_mode": "official_api",
            })
    return [_normalize_account(account, config) for account in derived]


def _normalize_account(account: dict[str, Any], config: dict) -> dict[str, Any]:
    schedule = ((config.get("distribution") or {}).get("schedule") or {})
    platform = str(account.get("platform") or "").strip()
    account_id = str(account.get("account_id") or f"{platform}_default").strip()
    policy_mode = str(account.get("policy_mode") or "human_assisted").strip()
    return {
        **account,
        "account_id": account_id,
        "platform": platform,
        "channel": str(account.get("channel") or account_id),
        "policy_mode": policy_mode,
        "daily_cap": int(account.get("daily_cap") or schedule.get("default_daily_cap", 3)),
        "min_spacing_minutes": int(account.get("min_spacing_minutes") or schedule.get("default_min_spacing_minutes", 180)),
        "timezone": str(account.get("timezone") or schedule.get("default_timezone", "UTC")),
    }


def _account_by_id(config: dict, account_id: str) -> dict[str, Any] | None:
    for account in _enabled_accounts(config):
        if account["account_id"] == account_id:
            return account
    return None


def _accepted_clips(config: dict) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    paths = config.get("paths") or {}
    accepted_root = _resolve_path(paths.get("accepted", "accepted"))
    inbox_root = _resolve_path(paths.get("inbox", "inbox"))
    for game in list_supported_games(config):
        game_dir = accepted_root / game
        if not game_dir.exists():
            continue
        for clip_path in sorted(game_dir.glob("*.mp4")):
            meta_path = _find_meta_for_clip(clip_path, inbox_root / game)
            if meta_path is None:
                continue
            metadata = _read_json(meta_path)
            if metadata.get("review_status") != "accepted":
                continue
            metadata.setdefault("meta_path", str(meta_path))
            items.append({"game": game, "clip_path": clip_path, "meta_path": meta_path, "metadata": metadata})
    return items


def _find_meta_for_clip(clip_file: Path, inbox_game_dir: Path) -> Path | None:
    parts = clip_file.stem.split("_", 2)
    guesses = [parts[2] if len(parts) == 3 else clip_file.stem, clip_file.stem]
    for guess in guesses:
        candidate = inbox_game_dir / f"{guess}.meta.json"
        if candidate.exists():
            return candidate
    for meta_file in inbox_game_dir.glob("*.meta.json"):
        try:
            meta = _read_json(meta_file)
        except Exception:
            continue
        if meta.get("final_path") == str(clip_file) or Path(str(meta.get("final_path", ""))).name == clip_file.name:
            return meta_file
    return None


def _template_targets(metadata: dict, config: dict) -> set[str]:
    template_id = metadata.get("selected_template_id")
    if not template_id:
        return set()
    templates_root = _resolve_path((config.get("paths") or {}).get("templates", "templates"))
    for template_file in templates_root.rglob(f"{template_id}.*.json"):
        try:
            template = _read_json(template_file)
        except Exception:
            continue
        return set(template.get("output", {}).get("platform_targets", []))
    return set()


def _next_scheduled_time(conn: sqlite3.Connection, account: dict, config: dict, now: datetime) -> datetime:
    schedule = ((config.get("distribution") or {}).get("schedule") or {})
    jitter = int(schedule.get("jitter_minutes", 0) or 0)
    spacing = int(account.get("min_spacing_minutes", 180))
    daily_cap = int(account.get("daily_cap", 3))
    candidate = now + timedelta(minutes=int(schedule.get("ready_delay_minutes", 0) or 0))

    latest = conn.execute(
        """
        SELECT scheduled_at FROM distribution_tasks
        WHERE account_id = ? AND scheduled_at != ''
        ORDER BY datetime(scheduled_at) DESC
        LIMIT 1
        """,
        (account["account_id"],),
    ).fetchone()
    if latest and latest["scheduled_at"]:
        candidate = max(candidate, _parse_dt(latest["scheduled_at"]) + timedelta(minutes=spacing))

    while _scheduled_count_for_day(conn, account["account_id"], candidate) >= daily_cap:
        candidate = (candidate + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)

    if jitter > 0:
        candidate += timedelta(minutes=random.randint(0, jitter))
    return candidate


def _scheduled_count_for_day(conn: sqlite3.Connection, account_id: str, dt: datetime) -> int:
    day = _iso(dt)[:10]
    row = conn.execute(
        """
        SELECT COUNT(*) AS count FROM distribution_tasks
        WHERE account_id = ? AND substr(scheduled_at, 1, 10) = ?
        """,
        (account_id, day),
    ).fetchone()
    return int(row["count"] or 0)


def _compliance_check(
    conn: sqlite3.Connection,
    item: dict[str, Any],
    account: dict[str, Any],
    config: dict,
    now: datetime,
) -> dict[str, Any]:
    metadata = item["metadata"]
    platform = account["platform"]
    policy_mode = account.get("policy_mode", "human_assisted")
    if policy_mode == "disabled":
        return _compliance("block", "account_disabled", {"account_id": account["account_id"]})
    if platform == "instagram_reels" and policy_mode == "official_api" and not account.get("instagram_official_publish_confirmed"):
        return _compliance("block", "instagram_requires_manual_review", {"platform": platform})
    if policy_mode == "official_api" and account.get("require_credentials", True) and not _credentials_present(platform):
        return _compliance("block", "missing_credentials", {"platform": platform, "required_env": _PLATFORM_ENV_VARS.get(platform, ())})
    if _copyright_risk(metadata):
        return _compliance("block", "copyright_risk", {"audio_mode": metadata.get("_audio_mode")})
    duplicate = _recent_duplicate(conn, account, metadata, now)
    if duplicate:
        return _compliance("block", "duplicate_recent_content", duplicate)
    if _circuit_open(conn, account, config, now):
        return _compliance("block", "circuit_breaker_open", {"account_id": account["account_id"]})
    if policy_mode == "human_assisted":
        return _compliance("manual", "human_assisted_policy_mode", {"account_id": account["account_id"]})
    if policy_mode != "official_api":
        return _compliance("block", "unsupported_policy_mode", {"policy_mode": policy_mode})
    return _compliance("pass", "ok", {})


def _compliance(status: str, reason: str, details: dict[str, Any]) -> dict[str, Any]:
    return {"status": status, "reason": reason, "details": details}


def _credentials_present(platform: str) -> bool:
    required = _PLATFORM_ENV_VARS.get(platform, ())
    return bool(required) and all(os.getenv(name) for name in required)


def _copyright_risk(metadata: dict) -> bool:
    if metadata.get("copyright_risk") or metadata.get("content_flags", {}).get("copyright_risk"):
        return True
    if metadata.get("_copyright_match") and metadata.get("_audio_mode") == "original":
        return True
    return False


def _recent_duplicate(conn: sqlite3.Connection, account: dict, metadata: dict, now: datetime) -> dict[str, Any] | None:
    title = _publishing_title(metadata).strip().lower()
    if not title:
        return None
    cutoff = now - timedelta(hours=24)
    row = conn.execute(
        """
        SELECT task_id, title FROM distribution_tasks
        WHERE account_id = ? AND lower(title) = ? AND datetime(created_at) >= datetime(?)
        LIMIT 1
        """,
        (account["account_id"], title, _iso(cutoff)),
    ).fetchone()
    if row:
        return {"matching_task_id": row["task_id"], "title": row["title"]}
    return None


def _circuit_open(conn: sqlite3.Connection, account: dict, config: dict, now: datetime) -> bool:
    schedule = ((config.get("distribution") or {}).get("schedule") or {})
    threshold = int(schedule.get("circuit_breaker_failure_threshold", 3))
    window = int(schedule.get("circuit_breaker_window_minutes", 60))
    cutoff = now - timedelta(minutes=window)
    row = conn.execute(
        """
        SELECT COUNT(*) AS count FROM distribution_attempts a
        JOIN distribution_tasks t ON t.task_id = a.task_id
        WHERE t.account_id = ?
          AND datetime(a.attempted_at) >= datetime(?)
          AND a.error_class IN ('auth_error', 'content_rejected')
        """,
        (account["account_id"], _iso(cutoff)),
    ).fetchone()
    return int(row["count"] or 0) >= threshold


def _initial_status(account: dict, compliance: dict, scheduled_at: datetime, now: datetime) -> str:
    if compliance["status"] == "block":
        return "paused_compliance"
    if compliance["status"] == "manual":
        return "needs_human_publish"
    return "ready" if scheduled_at <= now else "scheduled"


def _write_manual_pack(
    item: dict[str, Any],
    account: dict[str, Any],
    scheduled_at: datetime,
    config: dict,
    task_id: str,
) -> str:
    pack_dir = _manual_pack_dir(config)
    pack_dir.mkdir(parents=True, exist_ok=True)
    metadata = item["metadata"]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "task_id": task_id,
        "clip_id": metadata.get("clip_id"),
        "game": metadata.get("game"),
        "platform": account["platform"],
        "account_id": account["account_id"],
        "channel": account.get("channel", ""),
        "scheduled_at": _iso(scheduled_at),
        "video_path": str(item["clip_path"]),
        "title": _publishing_title(metadata),
        "caption": _publishing_caption(metadata),
        "hashtags": (metadata.get("title_engine") or {}).get("hashtags") or [],
        "checklist": _MANUAL_CHECKLIST,
    }
    path = pack_dir / f"{task_id}.json"
    path.write_text(json.dumps(payload, indent=2))
    return str(path)


def _insert_task(conn: sqlite3.Connection, task: dict[str, Any]) -> None:
    task = {**task, "schema_version": SCHEMA_VERSION}
    columns = ", ".join(task.keys())
    placeholders = ", ".join("?" for _ in task)
    conn.execute(f"INSERT OR IGNORE INTO distribution_tasks ({columns}) VALUES ({placeholders})", tuple(task.values()))


def _task_exists(conn: sqlite3.Connection, task_id: str) -> bool:
    return conn.execute("SELECT 1 FROM distribution_tasks WHERE task_id = ?", (task_id,)).fetchone() is not None


def _get_task(conn: sqlite3.Connection, task_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM distribution_tasks WHERE task_id = ?", (task_id,)).fetchone()
    return dict(row) if row else None


def _due_tasks(conn: sqlite3.Connection, now: datetime, limit: int | None) -> list[dict[str, Any]]:
    sql = """
        SELECT * FROM distribution_tasks
        WHERE (
            status = 'ready'
            OR (status = 'scheduled' AND datetime(scheduled_at) <= datetime(?))
            OR (status = 'failed_retryable' AND datetime(next_attempt_at) <= datetime(?))
        )
        ORDER BY datetime(scheduled_at) ASC, datetime(updated_at) ASC
    """
    params: list[Any] = [_iso(now), _iso(now)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [dict(row) for row in conn.execute(sql, params)]


def _record_compliance(conn: sqlite3.Connection, task_id: str, compliance: dict, now: datetime) -> None:
    conn.execute(
        """
        INSERT INTO distribution_compliance
        (record_id, task_id, checked_at, status, reason, details_json, schema_version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            task_id,
            _iso(now),
            compliance["status"],
            compliance.get("reason", ""),
            json.dumps(compliance.get("details") or {}, sort_keys=True),
            SCHEMA_VERSION,
        ),
    )


def _record_attempt(
    conn: sqlite3.Connection,
    task: dict[str, Any],
    result: dict[str, Any],
    error_class: str,
    duration: float,
    now: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO distribution_attempts
        (attempt_id, task_id, attempted_at, duration_seconds, status, error_class, error_message, response_json, schema_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            task["task_id"],
            _iso(now),
            round(duration, 3),
            "posted" if result.get("success") else "failed",
            error_class,
            str(result.get("error") or ""),
            json.dumps(result, sort_keys=True, default=str),
            SCHEMA_VERSION,
        ),
    )


def _mark_task_posted(conn: sqlite3.Connection, task: dict[str, Any], result: dict[str, Any], now: datetime) -> None:
    _update_task(
        conn,
        task["task_id"],
        status="posted",
        attempt_count=int(task.get("attempt_count") or 0) + 1,
        published_url=str(result.get("url") or ""),
        publish_id=str(result.get("publish_id") or result.get("id") or ""),
        last_error_class="",
        last_error="",
        next_attempt_at="",
        updated_at=_iso(now),
    )


def _update_task(conn: sqlite3.Connection, task_id: str, **fields: Any) -> None:
    if not fields:
        return
    assignments = ", ".join(f"{key} = ?" for key in fields)
    conn.execute(f"UPDATE distribution_tasks SET {assignments} WHERE task_id = ?", (*fields.values(), task_id))


def _write_distribution_result(task: dict[str, Any], result: dict[str, Any], config: dict) -> None:
    meta_path = Path(task["meta_path"])
    if not meta_path.exists():
        return
    meta = _read_json(meta_path)
    platform_result = {
        **result,
        "success": bool(result.get("success")),
        "task_id": task["task_id"],
        "account_id": task["account_id"],
        "policy_mode": task["policy_mode"],
        "posted_at": result.get("posted_at") or _iso(datetime.now(timezone.utc)),
    }
    meta.setdefault("distribution", {})[task["platform"]] = platform_result
    meta["distributed_at"] = platform_result["posted_at"]
    meta_path.write_text(json.dumps(meta, indent=2))
    log_clip(meta, {task["platform"]: platform_result}, config)


def _apply_circuit_breaker(conn: sqlite3.Connection, task: dict[str, Any], config: dict, now: datetime) -> None:
    account = {"account_id": task["account_id"]}
    if not _circuit_open(conn, account, config, now):
        return
    _update_task(
        conn,
        task["task_id"],
        status="paused_compliance",
        compliance_reason="circuit_breaker_open",
        updated_at=_iso(now),
    )


def _retry_max_attempts(config: dict) -> int:
    return int((((config.get("distribution") or {}).get("schedule") or {}).get("retry_max_attempts", 3)))


def _retry_delay(config: dict, attempt_count: int) -> timedelta:
    schedule = ((config.get("distribution") or {}).get("schedule") or {})
    base = int(schedule.get("retry_base_minutes", 30))
    jitter = int(schedule.get("retry_jitter_minutes", 10))
    minutes = base * (2 ** max(0, attempt_count - 1))
    if jitter > 0:
        minutes += random.randint(0, jitter)
    return timedelta(minutes=minutes)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _task_id(clip_id: Any, platform: str, account_id: str) -> str:
    raw = f"{clip_id}|{platform}|{account_id}"
    return "dist_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _as_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc, microsecond=0)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _parse_dt(value: str) -> datetime:
    if not value:
        return datetime.now(timezone.utc).replace(microsecond=0)
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    return _as_utc(dt)


def _iso(value: datetime) -> str:
    return _as_utc(value).isoformat(timespec="seconds")
