"""
Cloud backup — uploads approved clips and their .meta.json sidecars to
Google Drive using the same service account as analytics logging.

Setup (one-time):
  1. Use the same service account JSON as analytics (GOOGLE_SERVICE_ACCOUNT_JSON).
  2. Create a folder in Google Drive named e.g. "ClipBot Backups".
  3. Share that folder with the service account email (Editor access).
  4. Copy the folder ID from the URL:
       drive.google.com/drive/folders/<FOLDER_ID>
  5. Add to .env:
       GOOGLE_DRIVE_BACKUP_FOLDER_ID=<FOLDER_ID>

Folder structure created in Drive:
  ClipBot Backups/
    arc_raiders/
      arc_raiders_20260413_clip_id.mp4
      clip_id.meta.json
    marvel_rivals/
      ...
    deadlock/
      ...

Both the video file and the .meta.json sidecar are backed up.
Files are skipped if they already exist in Drive (idempotent).

Required packages: google-api-python-client google-auth
(already in requirements.txt for YouTube distribution)
"""

import os
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)


def _get_drive_service():
    """Return an authenticated Google Drive API v3 service."""
    sa_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_path:
        raise EnvironmentError(
            "GOOGLE_SERVICE_ACCOUNT_JSON not set in .env"
        )
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        raise ImportError(
            "Run: pip install google-api-python-client google-auth"
        )

    scopes = ["https://www.googleapis.com/auth/drive.file"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return build("drive", "v3", credentials=creds)


def _get_or_create_folder(service, name: str, parent_id: str) -> str:
    """Return the Drive folder ID for `name` under `parent_id`, creating if missing."""
    # Escape single quotes in the folder name for the query string
    safe_name = name.replace("'", "\\'")
    query = (
        f"name='{safe_name}' "
        f"and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents "
        f"and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id)").execute()
    existing = results.get("files", [])
    if existing:
        return existing[0]["id"]

    folder = service.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id",
    ).execute()
    return folder["id"]


def _file_exists_in_drive(service, name: str, parent_id: str) -> bool:
    """Return True if a non-trashed file with this name exists under parent_id."""
    safe_name = name.replace("'", "\\'")
    query = f"name='{safe_name}' and '{parent_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    return bool(results.get("files"))


def _upload_file(service, local_path: Path, parent_id: str, mimetype: str) -> str:
    """Upload a local file to Drive and return its file ID."""
    try:
        from googleapiclient.http import MediaFileUpload
    except ImportError:
        raise ImportError("Run: pip install google-api-python-client")

    media = MediaFileUpload(str(local_path), mimetype=mimetype, resumable=True)
    file_meta = {"name": local_path.name, "parents": [parent_id]}
    result = service.files().create(
        body=file_meta,
        media_body=media,
        fields="id",
    ).execute()
    return result["id"]


def backup_clip(clip_path: str, metadata: dict, config: dict) -> bool:
    """Upload an approved clip and its .meta.json to Google Drive.

    Skips silently if GOOGLE_DRIVE_BACKUP_FOLDER_ID is not set.
    Never raises — logs the error and returns False so the pipeline continues.

    Args:
        clip_path: Absolute path to the clip in accepted/{game}/.
        metadata:  Full clip metadata dict.
        config:    Full parsed config.yaml dict.

    Returns:
        True on success or if backup is not configured; False on failure.
    """
    root_folder_id = os.getenv("GOOGLE_DRIVE_BACKUP_FOLDER_ID")
    if not root_folder_id:
        logger.debug("GOOGLE_DRIVE_BACKUP_FOLDER_ID not set — Drive backup skipped.")
        return True  # Not configured is not an error

    clip = Path(clip_path)
    game = metadata.get("game", "unknown")

    try:
        service = _get_drive_service()
        game_folder_id = _get_or_create_folder(service, game, root_folder_id)

        # --- Back up the video clip ---
        if not _file_exists_in_drive(service, clip.name, game_folder_id):
            _upload_file(service, clip, game_folder_id, "video/mp4")
            logger.info(f"Drive backup: uploaded {clip.name}")
        else:
            logger.debug(f"Drive backup: {clip.name} already exists — skipping.")

        # --- Back up the .meta.json sidecar ---
        inbox_root = Path(config["paths"]["inbox"])
        meta_path = inbox_root / game / f"{metadata.get('clip_id', clip.stem)}.meta.json"
        if meta_path.exists():
            if not _file_exists_in_drive(service, meta_path.name, game_folder_id):
                _upload_file(service, meta_path, game_folder_id, "application/json")
                logger.debug(f"Drive backup: uploaded {meta_path.name}")

        return True

    except Exception as e:
        logger.error(f"Drive backup failed for {clip.name} (non-fatal): {e}")
        return False
