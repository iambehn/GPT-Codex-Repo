"""
Analytics logging — appends one row per distributed clip to a Google Sheet.

Setup (one-time):
  1. In Google Cloud Console, create a Service Account under the same project
     as your YouTube OAuth app (or any GCP project).
  2. Grant the service account Editor access to the spreadsheet
     (Share → paste the service account email → Editor).
  3. Download the JSON key file and store it somewhere safe on your machine.
  4. Add to .env:
       GOOGLE_SERVICE_ACCOUNT_JSON=/path/to/service-account-key.json
       GOOGLE_SHEETS_ID=<the long ID in the spreadsheet URL>

Required packages:
  pip install gspread google-auth

The sheet is auto-created ("Analytics" tab) with headers on first run.
"""

import os
from datetime import datetime
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

# Column order in the spreadsheet — changing this order requires clearing the
# header row in the sheet so it gets re-written correctly on the next run.
_HEADERS = [
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


def _get_sheets_client():
    """Return an authenticated gspread client via service account."""
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_path:
        raise EnvironmentError(
            "GOOGLE_SERVICE_ACCOUNT_JSON not set in .env — "
            "point it at your service account key file."
        )
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        raise ImportError("Run: pip install gspread google-auth")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)


def _ensure_headers(worksheet) -> None:
    """Write the header row if the sheet is brand new."""
    existing = worksheet.row_values(1)
    if not existing:
        worksheet.append_row(_HEADERS, value_input_option="RAW")


def log_clip(metadata: dict, distribution_results: dict) -> bool:
    """Append one analytics row for a clip to the configured Google Sheet.

    Skips silently if GOOGLE_SHEETS_ID is not set (analytics is optional).
    Never raises — logs the error and returns False on failure so the
    pipeline continues uninterrupted.

    Args:
        metadata:             Full clip metadata dict (all pipeline stages merged).
        distribution_results: Platform → {success, url, error} from run_distribution.

    Returns:
        True on success or if analytics is not configured; False on failure.
    """
    sheets_id = os.getenv("GOOGLE_SHEETS_ID")
    if not sheets_id:
        logger.debug("GOOGLE_SHEETS_ID not set — analytics logging skipped.")
        return True  # Not configured is not an error

    try:
        client = _get_sheets_client()
        sh = client.open_by_key(sheets_id)

        try:
            ws = sh.worksheet("Analytics")
        except Exception:
            ws = sh.add_worksheet(title="Analytics", rows=10000, cols=len(_HEADERS))

        _ensure_headers(ws)

        scoring = metadata.get("scoring", {})
        dist = distribution_results or {}

        def _url(platform: str) -> str:
            return dist.get(platform, {}).get("url") or ""

        row = [
            metadata.get("clip_id", ""),
            metadata.get("game", ""),
            datetime.now().isoformat(timespec="seconds"),   # when this row was written
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
            datetime.now().isoformat(timespec="seconds"),   # distributed_at
        ]

        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"Analytics logged: {metadata.get('clip_id')} → Google Sheets")
        return True

    except Exception as e:
        logger.error(f"Analytics logging failed (non-fatal): {e}")
        return False
