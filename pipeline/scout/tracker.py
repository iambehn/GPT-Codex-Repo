"""
Game Scouting Dashboard — Background Tracker

Polls Twitch, YouTube Data API, and Google Trends for each tracked game
on a configurable schedule. Computes a Trend Score (0–10) and logs each
snapshot to the "Scout" tab in the existing Google Sheets spreadsheet.

Signals:
  Twitch:         concurrent viewers + active streamer count (Helix API)
                  Requires: TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET
  YouTube:        new uploads mentioning the game in the last 24h
                  Requires: YOUTUBE_API_KEY (separate from upload OAuth)
                  Free quota: 10,000 units/day; each search = 100 units.
                  Polling 10 games every 6h = 40 searches/day — well within limit.
  Google Trends:  relative search interest 0–100 over the last 7 days
                  Uses pytrends (unofficial). Rate-limited if polled too fast;
                  6h interval is safe for a personal pipeline.

Trend Score (0–10):
  Twitch viewers vs previous poll:
    +3  growth > 20%  (or ≥5k viewers on first poll)
    +2  growth > 5%   (or ≥1k viewers on first poll)
  Twitch active streamers:
    +1  ≥ 10 streamers
    +1  ≥ 30 streamers  (extra point, stacks with above)
  YouTube uploads in last 24h:
    +2  ≥ 10 uploads
    +1  ≥ 3 uploads
  Google Trends score:
    +2  ≥ 70
    +1  ≥ 40
  Capped at 10.

Longevity Score (0–10): manual, set via the dashboard. Default: 5.
Flag condition: Trend ≥ trend_score_min AND Longevity ≥ longevity_score_min
               (defaults: 6 / 5, configured in config.yaml scout.thresholds)

Local state is stored in scout_cache.json (project root). Google Sheets
is the historical record — one row per poll per game in the "Scout" tab.
"""

import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests as _requests

from utils.logger import get_logger

logger = get_logger(__name__)

_CACHE_PATH = Path("scout_cache.json")
_CACHE_LOCK = threading.Lock()

_TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
_TWITCH_GAMES_URL = "https://api.twitch.tv/helix/games"
_TWITCH_STREAMS_URL = "https://api.twitch.tv/helix/streams"

_SCOUT_HEADERS = [
    "timestamp", "game",
    "twitch_viewers", "twitch_streamers",
    "youtube_clips_24h", "google_trends_score",
    "trend_score", "longevity_score", "flagged",
]


# ---------------------------------------------------------------------------
# Cache — local JSON as UI source of truth
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    """Load scout_cache.json; return empty skeleton if missing or corrupt."""
    with _CACHE_LOCK:
        if _CACHE_PATH.exists():
            try:
                return json.loads(_CACHE_PATH.read_text())
            except (json.JSONDecodeError, OSError):
                pass
    return {"last_poll": None, "games": {}}


def save_cache(data: dict) -> None:
    with _CACHE_LOCK:
        _CACHE_PATH.write_text(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Twitch helpers
# ---------------------------------------------------------------------------

def _get_twitch_token(client_id: str, client_secret: str) -> str:
    resp = _requests.post(
        _TWITCH_TOKEN_URL,
        params={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _get_game_id(game_name: str, client_id: str, token: str) -> str | None:
    resp = _requests.get(
        _TWITCH_GAMES_URL,
        params={"name": game_name},
        headers={"Client-Id": client_id, "Authorization": f"Bearer {token}"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    return data[0]["id"] if data else None


def _fetch_twitch_metrics(game_name: str) -> dict:
    """Return current Twitch viewers and active streamer count.

    Fetches the top 100 live streams for the game. For emerging games this
    captures the full picture; for established games (>100 streams) it covers
    the majority of viewership. Returns None values on failure so the caller
    can degrade gracefully.
    """
    client_id = os.getenv("TWITCH_CLIENT_ID")
    client_secret = os.getenv("TWITCH_CLIENT_SECRET")
    if not client_id or not client_secret:
        return {
            "twitch_viewers": None, "twitch_streamers": None,
            "twitch_error": "TWITCH_CLIENT_ID/SECRET not set",
        }
    try:
        token = _get_twitch_token(client_id, client_secret)
        game_id = _get_game_id(game_name, client_id, token)
        if not game_id:
            return {"twitch_viewers": 0, "twitch_streamers": 0}

        resp = _requests.get(
            _TWITCH_STREAMS_URL,
            params={"game_id": game_id, "first": 100},
            headers={"Client-Id": client_id, "Authorization": f"Bearer {token}"},
            timeout=15,
        )
        resp.raise_for_status()
        streams = resp.json().get("data", [])
        return {
            "twitch_viewers": sum(s.get("viewer_count", 0) for s in streams),
            "twitch_streamers": len(streams),
        }
    except Exception as e:
        logger.warning(f"[Scout] Twitch fetch failed for '{game_name}': {e}")
        return {"twitch_viewers": None, "twitch_streamers": None, "twitch_error": str(e)}


# ---------------------------------------------------------------------------
# YouTube helpers
# ---------------------------------------------------------------------------

def _fetch_youtube_metrics(game_name: str) -> dict:
    """Count YouTube videos mentioning the game uploaded in the last 24 hours.

    Requires YOUTUBE_API_KEY in .env (read-only API key, not OAuth).
    Free tier: 10,000 units/day. Each search request costs 100 units.
    Polling 10 games every 6h = 40 requests/day — well within the free limit.
    """
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        return {"youtube_clips_24h": None, "youtube_error": "YOUTUBE_API_KEY not set"}

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        resp = _requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet",
                "q": f"{game_name} clips highlights",
                "type": "video",
                "publishedAfter": cutoff,
                "maxResults": 50,
                "key": api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return {"youtube_clips_24h": len(resp.json().get("items", []))}
    except Exception as e:
        logger.warning(f"[Scout] YouTube fetch failed for '{game_name}': {e}")
        return {"youtube_clips_24h": None, "youtube_error": str(e)}


# ---------------------------------------------------------------------------
# Google Trends helpers
# ---------------------------------------------------------------------------

def _fetch_google_trends(game_name: str) -> dict:
    """Return relative search interest (0–100) over the last 7 days via pytrends.

    pytrends is an unofficial library. It can return HTTP 429 if called too
    frequently. With a 6-hour poll interval on a small game list this is safe.
    """
    try:
        from pytrends.request import TrendReq
        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
        pytrends.build_payload([game_name], timeframe="now 7-d")
        df = pytrends.interest_over_time()
        if df.empty or game_name not in df.columns:
            return {"google_trends_score": 0}
        return {"google_trends_score": int(df[game_name].iloc[-1])}
    except Exception as e:
        logger.warning(f"[Scout] Google Trends fetch failed for '{game_name}': {e}")
        return {"google_trends_score": None, "trends_error": str(e)}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _calculate_trend_score(metrics: dict, previous_viewers: int | None) -> int:
    score = 0
    viewers = metrics.get("twitch_viewers")
    streamers = metrics.get("twitch_streamers")
    yt = metrics.get("youtube_clips_24h")
    trends = metrics.get("google_trends_score")

    # Twitch viewer growth (or absolute count on first poll)
    if viewers is not None:
        if previous_viewers is not None and previous_viewers > 0:
            growth = (viewers - previous_viewers) / previous_viewers
            if growth > 0.20:
                score += 3
            elif growth > 0.05:
                score += 2
        else:
            if viewers >= 5000:
                score += 3
            elif viewers >= 1000:
                score += 2
            elif viewers >= 200:
                score += 1

    # Twitch streamer count
    if streamers is not None:
        if streamers >= 30:
            score += 2
        elif streamers >= 10:
            score += 1

    # YouTube recent uploads
    if yt is not None:
        if yt >= 10:
            score += 2
        elif yt >= 3:
            score += 1

    # Google Trends
    if trends is not None:
        if trends >= 70:
            score += 2
        elif trends >= 40:
            score += 1

    return min(score, 10)


# ---------------------------------------------------------------------------
# Poll a single game
# ---------------------------------------------------------------------------

def poll_game(game_name: str, config: dict) -> dict:
    """Fetch all signals for one game, compute scores, update cache, log to Sheets."""
    logger.info(f"[Scout] Polling: {game_name}")

    cache = load_cache()
    game_entry = cache.get("games", {}).get(game_name, {})
    prev_latest = game_entry.get("latest", {})
    previous_viewers = prev_latest.get("twitch_viewers")
    longevity_score = game_entry.get("longevity_score", 5)

    twitch = _fetch_twitch_metrics(game_name)
    youtube = _fetch_youtube_metrics(game_name)
    trends = _fetch_google_trends(game_name)
    metrics = {**twitch, **youtube, **trends}

    trend_score = _calculate_trend_score(metrics, previous_viewers)

    thresholds = config.get("scout", {}).get("thresholds", {})
    trend_min = thresholds.get("trend_score_min", 6)
    longevity_min = thresholds.get("longevity_score_min", 5)
    flagged = (trend_score >= trend_min) and (longevity_score >= longevity_min)

    # Track which signals were available this poll
    signals_ok = []
    if twitch.get("twitch_viewers") is not None:
        signals_ok.append("twitch")
    if youtube.get("youtube_clips_24h") is not None:
        signals_ok.append("youtube")
    if trends.get("google_trends_score") is not None:
        signals_ok.append("trends")

    snapshot = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "twitch_viewers": twitch.get("twitch_viewers"),
        "twitch_streamers": twitch.get("twitch_streamers"),
        "youtube_clips_24h": youtube.get("youtube_clips_24h"),
        "google_trends_score": trends.get("google_trends_score"),
        "trend_score": trend_score,
        "longevity_score": longevity_score,
        "flagged": flagged,
        "signals_ok": signals_ok,
    }

    # Log a flag transition
    was_flagged = prev_latest.get("flagged", False)
    if flagged and not was_flagged:
        logger.info(
            f"[Scout] ★ FLAG: '{game_name}' crossed threshold "
            f"(trend={trend_score}, longevity={longevity_score})"
        )

    # Update cache
    cache.setdefault("games", {}).setdefault(game_name, {})
    cache["games"][game_name]["longevity_score"] = longevity_score
    cache["games"][game_name]["previous_trend_score"] = prev_latest.get("trend_score")
    cache["games"][game_name]["latest"] = snapshot
    cache["last_poll"] = datetime.now().isoformat(timespec="seconds")
    save_cache(cache)

    _log_to_sheets(game_name, snapshot, config)
    return snapshot


def poll_all_games(config: dict) -> None:
    """Poll every tracked game. Seeds the cache from config on first run."""
    cache = load_cache()

    # First run: seed game list from config.yaml scout.games
    if not cache.get("games"):
        for entry in config.get("scout", {}).get("games", []):
            name = entry if isinstance(entry, str) else entry.get("name", "")
            ls = entry.get("longevity_score", 5) if isinstance(entry, dict) else 5
            if name:
                cache.setdefault("games", {})[name] = {"longevity_score": ls}
        save_cache(cache)

    for game_name in list(cache.get("games", {}).keys()):
        try:
            poll_game(game_name, config)
        except Exception as e:
            logger.error(f"[Scout] Unexpected error polling '{game_name}': {e}")

    logger.info(f"[Scout] Poll complete — {len(cache.get('games', {}))} game(s).")


# ---------------------------------------------------------------------------
# Google Sheets logging
# ---------------------------------------------------------------------------

def _log_to_sheets(game_name: str, snapshot: dict, config: dict) -> None:
    """Append one row to the 'Scout' worksheet in the existing spreadsheet."""
    sheets_id = os.getenv("GOOGLE_SHEETS_ID")
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sheets_id or not sa_path:
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        sh = gspread.authorize(creds).open_by_key(sheets_id)
        try:
            ws = sh.worksheet("Scout")
        except Exception:
            ws = sh.add_worksheet(title="Scout", rows=10000, cols=len(_SCOUT_HEADERS))

        if not ws.row_values(1):
            ws.append_row(_SCOUT_HEADERS, value_input_option="RAW")

        ws.append_row([
            snapshot.get("timestamp", ""),
            game_name,
            snapshot.get("twitch_viewers", ""),
            snapshot.get("twitch_streamers", ""),
            snapshot.get("youtube_clips_24h", ""),
            snapshot.get("google_trends_score", ""),
            snapshot.get("trend_score", ""),
            snapshot.get("longevity_score", ""),
            "YES" if snapshot.get("flagged") else "no",
        ], value_input_option="USER_ENTERED")
    except Exception as e:
        logger.warning(f"[Scout] Sheets logging failed (non-fatal): {e}")


# ---------------------------------------------------------------------------
# Game management (mutate scout_cache.json)
# ---------------------------------------------------------------------------

def add_game(game_name: str, longevity_score: int = 5) -> None:
    cache = load_cache()
    if game_name not in cache.setdefault("games", {}):
        cache["games"][game_name] = {"longevity_score": longevity_score}
        save_cache(cache)
        logger.info(f"[Scout] Added: {game_name}")


def remove_game(game_name: str) -> None:
    cache = load_cache()
    if game_name in cache.get("games", {}):
        del cache["games"][game_name]
        save_cache(cache)
        logger.info(f"[Scout] Removed: {game_name}")


def set_longevity(game_name: str, score: int, config: dict) -> None:
    """Update longevity score and immediately recalculate the flag on the latest snapshot."""
    score = max(0, min(10, int(score)))
    cache = load_cache()
    game = cache.setdefault("games", {}).setdefault(game_name, {})
    game["longevity_score"] = score
    latest = game.get("latest", {})
    if latest:
        thresholds = config.get("scout", {}).get("thresholds", {})
        trend_min = thresholds.get("trend_score_min", 6)
        longevity_min = thresholds.get("longevity_score_min", 5)
        latest["longevity_score"] = score
        latest["flagged"] = (latest.get("trend_score", 0) >= trend_min) and (score >= longevity_min)
    save_cache(cache)


# ---------------------------------------------------------------------------
# Background polling thread
# ---------------------------------------------------------------------------

def start_background_polling(config: dict) -> None:
    """Spin up a daemon thread that polls all tracked games on a schedule.

    Runs an initial poll immediately so the dashboard has data on first load.
    Subsequent polls run every scout.poll_interval_hours (default: 6).
    """
    if not config.get("scout", {}).get("enabled", True):
        logger.debug("[Scout] Disabled in config — background polling not started.")
        return

    interval_hours = config.get("scout", {}).get("poll_interval_hours", 6)
    interval_secs = interval_hours * 3600

    def _loop() -> None:
        logger.info(f"[Scout] Background polling started — interval: {interval_hours}h.")
        try:
            poll_all_games(config)
        except Exception as e:
            logger.error(f"[Scout] Initial poll error: {e}")
        while True:
            time.sleep(interval_secs)
            try:
                poll_all_games(config)
            except Exception as e:
                logger.error(f"[Scout] Scheduled poll error: {e}")

    threading.Thread(target=_loop, daemon=True, name="scout-poller").start()
