"""
Stage 8 — Distribution

Publishes approved clips to social media platforms via direct API calls.
Each platform is an independent upload function. Only platforms that are
both enabled in config['distribution']['platforms'] AND present in the
template's platform_targets are posted to.

Platform implementations:
  YouTube Shorts   — Google Data API v3 (google-api-python-client)
  TikTok           — TikTok Content Posting API v2 (requests)
  Instagram Reels  — Meta Graph API v1.0 (requests)
  Twitter/X        — Twitter API v2 (requests)
  Reddit           — PRAW (Python Reddit API Wrapper)

Each upload function returns:
  {"success": bool, "url": str | None, "error": str | None}

Required environment variables per platform (set in .env):
  YouTube:          YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET
                    (OAuth2 — browser flow runs once, token cached to youtube_token.json)
  TikTok:           TIKTOK_ACCESS_TOKEN
  Instagram:        INSTAGRAM_ACCESS_TOKEN, INSTAGRAM_ACCOUNT_ID
  Twitter/X:        TWITTER_API_KEY, TWITTER_API_SECRET,
                    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
  Reddit:           REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
                    REDDIT_USERNAME, REDDIT_PASSWORD

Distribution results are stored in .meta.json under "distribution":
  {
    "youtube_shorts": {"success": true, "url": "https://youtu.be/...", "error": null},
    "tiktok": {"success": false, "url": null, "error": "TIKTOK_ACCESS_TOKEN not set"},
    ...
  }
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

# Platform → template platform_target keys
_PLATFORM_KEYS = {
    "youtube_shorts": "youtube_shorts",
    "tiktok": "tiktok",
    "instagram_reels": "instagram_reels",
    "twitter_x": "twitter_x",
    "reddit": "reddit",
}

_NOT_CONFIGURED = lambda platform, var: {
    "success": False,
    "url": None,
    "error": f"{var} not set — configure in .env to enable {platform} uploads.",
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _extract_thumbnail(clip_path: Path) -> Path | None:
    """Extract the highest-motion keyframe from a clip as a JPEG.

    Uses FFmpeg's scene-change detection to find the most visually active
    frame (scene change score > 0.3). Falls back to the frame at 3 seconds
    if no high-motion frame is found.

    Returns the path to a temporary JPEG file, or None on failure.
    The caller is responsible for deleting the file after use.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
    tmp.close()
    thumb_path = Path(tmp.name)

    # Try highest-motion frame first
    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", str(clip_path),
                "-vf", "select=gt(scene\\,0.3),scale=1280:720",
                "-frames:v", "1",
                "-q:v", "2",
                str(thumb_path),
            ],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0 and thumb_path.stat().st_size > 0:
            return thumb_path
    except Exception:
        pass

    # Fallback: frame at 3 seconds
    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-ss", "3",
                "-i", str(clip_path),
                "-frames:v", "1",
                "-q:v", "2",
                "-vf", "scale=1280:720",
                str(thumb_path),
            ],
            capture_output=True,
            timeout=30,
        )
        if result.returncode == 0 and thumb_path.stat().st_size > 0:
            return thumb_path
    except Exception:
        pass

    thumb_path.unlink(missing_ok=True)
    return None


# ---------------------------------------------------------------------------
# YouTube Shorts
# ---------------------------------------------------------------------------

def _upload_youtube_shorts(clip_path: Path, metadata: dict, cfg: dict) -> dict:
    """Upload a clip to YouTube Shorts using the YouTube Data API v3.

    OAuth2 credentials are obtained via an installed-app flow on first run.
    The token is cached to youtube_token.json and refreshed automatically.

    Required env vars: YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET
    Required pip packages: google-api-python-client, google-auth-oauthlib
    """
    client_id = os.getenv("YOUTUBE_CLIENT_ID")
    client_secret = os.getenv("YOUTUBE_CLIENT_SECRET")
    if not client_id or not client_secret:
        return _NOT_CONFIGURED("YouTube", "YOUTUBE_CLIENT_ID / YOUTUBE_CLIENT_SECRET")

    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request as GoogleRequest
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError:
        return {
            "success": False, "url": None,
            "error": "Missing packages. Run: pip install google-api-python-client google-auth-oauthlib google-auth-httplib2",
        }

    scoring = metadata.get("scoring", {})
    title = (scoring.get("suggested_title") or metadata.get("clip_id", "Gaming Clip"))[:100]
    description = scoring.get("suggested_caption", "") + "\n\n#Shorts"
    privacy = cfg.get("privacy", "public")
    category_id = str(cfg.get("category_id", "20"))  # 20 = Gaming

    scopes = ["https://www.googleapis.com/auth/youtube.upload"]
    token_path = Path("youtube_token.json")
    creds = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
        else:
            client_config = {
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            }
            flow = InstalledAppFlow.from_client_config(client_config, scopes)
            creds = flow.run_local_server(port=0)

        token_path.write_text(creds.to_json())

    try:
        youtube = build("youtube", "v3", credentials=creds)

        body = {
            "snippet": {
                "title": title,
                "description": description,
                "categoryId": category_id,
                "tags": ["Shorts", "Gaming", "FPS"],
            },
            "status": {"privacyStatus": privacy},
        }

        media = MediaFileUpload(
            str(clip_path),
            mimetype="video/mp4",
            resumable=True,
            chunksize=5 * 1024 * 1024,  # 5 MB chunks
        )

        request = youtube.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media,
        )

        response = None
        while response is None:
            _, response = request.next_chunk()

        video_id = response.get("id", "")
        url = f"https://youtu.be/{video_id}"
        logger.info(f"YouTube Shorts uploaded: {url}")

        # Upload custom thumbnail (best-effort — non-fatal if it fails)
        thumb_path = _extract_thumbnail(clip_path)
        if thumb_path:
            try:
                thumb_media = MediaFileUpload(str(thumb_path), mimetype="image/jpeg")
                youtube.thumbnails().set(
                    videoId=video_id,
                    media_body=thumb_media,
                ).execute()
                logger.debug(f"YouTube thumbnail uploaded for {video_id}")
            except Exception as thumb_err:
                logger.warning(f"YouTube thumbnail upload failed (non-fatal): {thumb_err}")
            finally:
                thumb_path.unlink(missing_ok=True)

        return {"success": True, "url": url, "error": None}

    except Exception as e:
        logger.error(f"YouTube upload failed: {e}")
        return {"success": False, "url": None, "error": str(e)}


# ---------------------------------------------------------------------------
# TikTok
# ---------------------------------------------------------------------------

def _upload_tiktok(clip_path: Path, metadata: dict, cfg: dict) -> dict:
    """Upload a clip to TikTok using the Content Posting API v2.

    Flow: initialize upload → upload video to provided URL → check status.
    Requires a creator/business account with Content Posting API access.

    Required env var: TIKTOK_ACCESS_TOKEN
    API docs: https://developers.tiktok.com/doc/content-posting-api-reference-direct-post
    """
    access_token = os.getenv("TIKTOK_ACCESS_TOKEN")
    if not access_token:
        return _NOT_CONFIGURED("TikTok", "TIKTOK_ACCESS_TOKEN")

    try:
        import requests
    except ImportError:
        return {"success": False, "url": None, "error": "requests not installed"}

    scoring = metadata.get("scoring", {})
    title = (scoring.get("suggested_title") or metadata.get("clip_id", ""))[:150]
    privacy = cfg.get("privacy_level", "SELF_ONLY")  # Use SELF_ONLY for testing
    file_size = clip_path.stat().st_size

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
    }

    # Step 1: Initialize upload
    init_payload = {
        "post_info": {
            "title": title,
            "privacy_level": privacy,
            "disable_duet": False,
            "disable_comment": False,
            "disable_stitch": False,
        },
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": file_size,
            "chunk_size": file_size,
            "total_chunk_count": 1,
        },
    }

    try:
        init_resp = requests.post(
            "https://open.tiktokapis.com/v2/post/publish/video/init/",
            headers=headers,
            json=init_payload,
            timeout=30,
        )
        init_data = init_resp.json()

        if init_data.get("error", {}).get("code") != "ok":
            return {
                "success": False, "url": None,
                "error": f"TikTok init failed: {init_data.get('error')}",
            }

        upload_url = init_data["data"]["upload_url"]
        publish_id = init_data["data"]["publish_id"]

        # Step 2: Upload video
        with open(clip_path, "rb") as f:
            upload_resp = requests.put(
                upload_url,
                data=f,
                headers={
                    "Content-Type": "video/mp4",
                    "Content-Range": f"bytes 0-{file_size - 1}/{file_size}",
                },
                timeout=300,
            )

        if upload_resp.status_code not in (200, 201, 206):
            return {
                "success": False, "url": None,
                "error": f"TikTok upload chunk failed: HTTP {upload_resp.status_code}",
            }

        logger.info(f"TikTok video uploaded (publish_id={publish_id}) — processing asynchronously.")
        return {
            "success": True,
            "url": None,  # URL available after TikTok finishes processing (~minutes)
            "error": None,
            "publish_id": publish_id,
        }

    except Exception as e:
        logger.error(f"TikTok upload failed: {e}")
        return {"success": False, "url": None, "error": str(e)}


def _poll_tiktok_publish_status(publish_id: str, access_token: str) -> str | None:
    """Check the processing status of a TikTok upload and return the video URL if ready.

    Returns the video URL string if processing is complete, or None if still
    processing or on error.

    API docs: https://developers.tiktok.com/doc/content-posting-api-reference-direct-post
    """
    try:
        import requests as req
    except ImportError:
        return None

    try:
        resp = req.post(
            "https://open.tiktokapis.com/v2/post/publish/status/fetch/",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={"publish_id": publish_id},
            timeout=15,
        )
        data = resp.json()
        status_data = data.get("data", {})
        status = status_data.get("status", "")

        if status == "PUBLISH_COMPLETE":
            # TikTok returns the post URL in publicaly_available_post_id
            post_id = status_data.get("publicaly_available_post_id", [])
            if post_id:
                return f"https://www.tiktok.com/@me/video/{post_id[0]}"
        elif status in ("FAILED", "PUBLISH_FAILED"):
            logger.warning(f"TikTok publish failed for {publish_id}: {status_data.get('fail_reason')}")

    except Exception as e:
        logger.debug(f"TikTok status poll error for {publish_id}: {e}")

    return None


def poll_tiktok_pending(config: dict) -> None:
    """Scan all meta.json files for TikTok uploads that have a publish_id but no URL yet.

    For each such clip, queries the TikTok status API and updates meta.json
    with the final URL if processing is complete.
    """
    access_token = os.getenv("TIKTOK_ACCESS_TOKEN")
    if not access_token:
        logger.error("TIKTOK_ACCESS_TOKEN not set — cannot poll TikTok status.")
        return

    inbox_root = Path(config["paths"]["inbox"])
    updated = 0
    pending = 0

    for game in config.get("games", {}):
        inbox_dir = inbox_root / game
        if not inbox_dir.exists():
            continue
        for meta_file in inbox_dir.glob("*.meta.json"):
            try:
                meta = json.loads(meta_file.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            dist = meta.get("distribution", {})
            tiktok = dist.get("tiktok", {})
            if not tiktok.get("success"):
                continue
            if tiktok.get("url"):
                continue  # Already resolved
            publish_id = tiktok.get("publish_id")
            if not publish_id:
                continue

            pending += 1
            url = _poll_tiktok_publish_status(publish_id, access_token)
            if url:
                meta["distribution"]["tiktok"]["url"] = url
                meta_file.write_text(json.dumps(meta, indent=2))
                logger.info(f"TikTok URL resolved: {meta.get('clip_id')} → {url}")
                updated += 1
            else:
                logger.debug(f"TikTok still processing: {meta.get('clip_id')} (publish_id={publish_id})")

    logger.info(f"TikTok poll complete: {updated}/{pending} URL(s) resolved.")


# ---------------------------------------------------------------------------
# Instagram Reels
# ---------------------------------------------------------------------------

def _upload_instagram_reels(clip_path: Path, metadata: dict, cfg: dict) -> dict:
    """Upload a clip as an Instagram Reel using the Meta Graph API.

    Two-step process: create media container → publish container.
    Video must be accessible at a public URL for the container creation step —
    a hosting step (e.g. temporary S3 upload) is required before calling the
    Graph API. This stub returns not-implemented until a hosting layer is added.

    Required env vars: INSTAGRAM_ACCESS_TOKEN, INSTAGRAM_ACCOUNT_ID
    API docs: https://developers.facebook.com/docs/instagram-api/guides/reels
    """
    access_token = os.getenv("INSTAGRAM_ACCESS_TOKEN")
    account_id = os.getenv("INSTAGRAM_ACCOUNT_ID")

    if not access_token:
        return _NOT_CONFIGURED("Instagram", "INSTAGRAM_ACCESS_TOKEN")
    if not account_id:
        return _NOT_CONFIGURED("Instagram", "INSTAGRAM_ACCOUNT_ID")

    # Instagram Reels requires the video to be reachable at a public URL.
    # Until a hosting/CDN layer is wired in, this step cannot be completed.
    return {
        "success": False,
        "url": None,
        "error": (
            "Instagram Reels requires the video to be hosted at a public URL before "
            "the Graph API container can be created. Add a hosting step (e.g. temporary "
            "S3 pre-signed URL) and implement the container creation + publish calls here."
        ),
    }


# ---------------------------------------------------------------------------
# Twitter / X
# ---------------------------------------------------------------------------

def _upload_twitter_x(clip_path: Path, metadata: dict, cfg: dict) -> dict:
    """Post a clip to Twitter/X using the Twitter API v2 media upload + tweet.

    Two-step process: chunked media upload → create tweet with media_id.

    Required env vars: TWITTER_API_KEY, TWITTER_API_SECRET,
                       TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
    API docs: https://developer.x.com/en/docs/x-api/v2/tweets/manage-tweets
    """
    api_key = os.getenv("TWITTER_API_KEY")
    api_secret = os.getenv("TWITTER_API_SECRET")
    access_token = os.getenv("TWITTER_ACCESS_TOKEN")
    access_secret = os.getenv("TWITTER_ACCESS_SECRET")

    missing = [
        v for v, k in [
            ("TWITTER_API_KEY", api_key),
            ("TWITTER_API_SECRET", api_secret),
            ("TWITTER_ACCESS_TOKEN", access_token),
            ("TWITTER_ACCESS_SECRET", access_secret),
        ] if not k
    ]
    if missing:
        return _NOT_CONFIGURED("Twitter/X", " / ".join(missing))

    try:
        import requests
        from requests_oauthlib import OAuth1
    except ImportError:
        return {
            "success": False, "url": None,
            "error": "Missing package. Run: pip install requests requests-oauthlib",
        }

    auth = OAuth1(api_key, api_secret, access_token, access_secret)
    scoring = metadata.get("scoring", {})
    tweet_text = (scoring.get("suggested_caption") or
                  scoring.get("suggested_title") or "")[:280]
    file_size = clip_path.stat().st_size

    try:
        # Step 1: INIT
        init_resp = requests.post(
            "https://upload.twitter.com/1.1/media/upload.json",
            auth=auth,
            data={
                "command": "INIT",
                "total_bytes": file_size,
                "media_type": "video/mp4",
                "media_category": "tweet_video",
            },
            timeout=30,
        )
        media_id = init_resp.json()["media_id_string"]

        # Step 2: APPEND chunks (5 MB)
        chunk_size = 5 * 1024 * 1024
        segment = 0
        with open(clip_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                requests.post(
                    "https://upload.twitter.com/1.1/media/upload.json",
                    auth=auth,
                    data={"command": "APPEND", "media_id": media_id, "segment_index": segment},
                    files={"media": chunk},
                    timeout=120,
                )
                segment += 1

        # Step 3: FINALIZE
        requests.post(
            "https://upload.twitter.com/1.1/media/upload.json",
            auth=auth,
            data={"command": "FINALIZE", "media_id": media_id},
            timeout=30,
        )

        # Step 4: Post tweet
        tweet_resp = requests.post(
            "https://api.twitter.com/2/tweets",
            auth=auth,
            json={"text": tweet_text, "media": {"media_ids": [media_id]}},
            timeout=30,
        )
        tweet_data = tweet_resp.json()
        tweet_id = tweet_data.get("data", {}).get("id")
        url = f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None
        logger.info(f"Twitter/X posted: {url}")
        return {"success": True, "url": url, "error": None}

    except Exception as e:
        logger.error(f"Twitter/X upload failed: {e}")
        return {"success": False, "url": None, "error": str(e)}


# ---------------------------------------------------------------------------
# Reddit
# ---------------------------------------------------------------------------

def _upload_reddit(clip_path: Path, metadata: dict, cfg: dict, game: str) -> dict:
    """Post a clip to a game-specific subreddit using PRAW.

    Required env vars: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
                       REDDIT_USERNAME, REDDIT_PASSWORD
    Required pip package: praw
    """
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")

    missing = [v for v, k in [
        ("REDDIT_CLIENT_ID", client_id), ("REDDIT_CLIENT_SECRET", client_secret),
        ("REDDIT_USERNAME", username), ("REDDIT_PASSWORD", password),
    ] if not k]
    if missing:
        return _NOT_CONFIGURED("Reddit", " / ".join(missing))

    try:
        import praw
    except ImportError:
        return {"success": False, "url": None, "error": "Missing package. Run: pip install praw"}

    subreddits_cfg = cfg.get("subreddits", {})
    subreddit_name = subreddits_cfg.get(game, "")
    if not subreddit_name:
        return {
            "success": False, "url": None,
            "error": f"No subreddit configured for game '{game}'. Add to config.yaml distribution.platforms.reddit.subreddits.",
        }

    scoring = metadata.get("scoring", {})
    title = (scoring.get("suggested_title") or metadata.get("clip_id", "Gaming Clip"))[:300]

    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            user_agent="ClipBot/1.0 (by u/{username})",
        )

        subreddit = reddit.subreddit(subreddit_name.lstrip("r/"))

        # Flair support: per-subreddit flair_id in config (run --list-reddit-flairs to discover)
        subreddit_cfg = cfg.get("subreddit_config", {}).get(game, {})
        flair_id = subreddit_cfg.get("flair_id")
        flair_text = subreddit_cfg.get("flair_text")

        submit_kwargs = {
            "title": title,
            "video_path": str(clip_path),
            "videogif": False,
            "nsfw": False,
            "spoiler": False,
        }
        if flair_id:
            submit_kwargs["flair_id"] = flair_id
        if flair_text:
            submit_kwargs["flair_text"] = flair_text

        submission = subreddit.submit_video(**submit_kwargs)

        url = f"https://reddit.com{submission.permalink}"
        logger.info(f"Reddit posted: {url}")
        return {"success": True, "url": url, "error": None}

    except Exception as e:
        logger.error(f"Reddit upload failed: {e}")
        return {"success": False, "url": None, "error": str(e)}


def list_reddit_flairs(config: dict) -> None:
    """Print available link flairs for each configured subreddit.

    Run this once with: python run.py --list-reddit-flairs
    Then add the desired flair_id to config.yaml under:
      distribution.platforms.reddit.subreddit_config.<game>.flair_id

    Required env vars: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
                       REDDIT_USERNAME, REDDIT_PASSWORD
    """
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")

    missing = [v for v, k in [
        ("REDDIT_CLIENT_ID", client_id), ("REDDIT_CLIENT_SECRET", client_secret),
        ("REDDIT_USERNAME", username), ("REDDIT_PASSWORD", password),
    ] if not k]
    if missing:
        logger.error(f"Missing Reddit env vars: {', '.join(missing)}")
        return

    try:
        import praw
    except ImportError:
        logger.error("Missing package. Run: pip install praw")
        return

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        user_agent=f"ClipBot/1.0 (by u/{username})",
    )

    subreddits_cfg = config.get("distribution", {}).get("platforms", {}).get("reddit", {}).get("subreddits", {})
    for game, subreddit_name in subreddits_cfg.items():
        print(f"\n--- r/{subreddit_name} ({game}) ---")
        try:
            subreddit = reddit.subreddit(subreddit_name.lstrip("r/"))
            flairs = list(subreddit.flair.link_templates)
            if not flairs:
                print("  (no link flairs configured)")
            for f in flairs:
                print(f"  id={f['id']!r}  text={f['text']!r}  type={f.get('type','')}")
        except Exception as e:
            print(f"  Error fetching flairs: {e}")

    print("\nTo apply a flair, add to config.yaml:")
    print("  distribution:")
    print("    platforms:")
    print("      reddit:")
    print("        subreddit_config:")
    print("          <game>:")
    print("            flair_id: \"<id from above>\"")


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_HANDLERS = {
    "youtube_shorts": _upload_youtube_shorts,
    "tiktok": _upload_tiktok,
    "instagram_reels": _upload_instagram_reels,
    "twitter_x": _upload_twitter_x,
}


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_distribution(clip_path: str, metadata: dict, config: dict) -> dict:
    """Publish an approved clip to all configured and targeted platforms.

    Only posts to platforms that are:
      1. Enabled in config['distribution']['platforms']
      2. Listed in the template's platform_targets

    Idempotent: skips platforms already present in metadata['distribution'].

    Args:
        clip_path: Path to the approved clip in accepted/{game}/.
        metadata:  Full clip metadata dict (includes scoring and template info).
        config:    Full parsed config.yaml dict.

    Returns:
        Dict mapping platform key → result dict {success, url, error}.
    """
    clip = Path(clip_path)
    game = metadata.get("game", "")

    # Load which platforms the template targets
    template_targets: set[str] = set()
    template_id = metadata.get("selected_template_id")
    if template_id:
        template_file = list(
            Path(config["paths"]["templates"]).rglob(f"{template_id}.*.json")
        )
        if template_file:
            try:
                tmpl = json.loads(template_file[0].read_text())
                template_targets = set(tmpl.get("output", {}).get("platform_targets", []))
            except (json.JSONDecodeError, OSError):
                pass

    dist_config = config.get("distribution", {}).get("platforms", {})

    # Load existing distribution results (idempotency)
    inbox_root = Path(config["paths"]["inbox"])
    meta_path = inbox_root / game / f"{metadata.get('clip_id', clip.stem)}.meta.json"
    existing_results: dict = {}
    if meta_path.exists():
        stored = json.loads(meta_path.read_text())
        existing_results = stored.get("distribution", {})

    results: dict = dict(existing_results)

    for platform, platform_cfg in dist_config.items():
        if not platform_cfg.get("enabled", False):
            logger.debug(f"Skipping {platform} (disabled in config).")
            continue

        target_key = _PLATFORM_KEYS.get(platform, platform)
        if template_targets and target_key not in template_targets:
            logger.debug(f"Skipping {platform} (not in template platform_targets).")
            continue

        if platform in existing_results:
            logger.debug(f"Skipping {platform} (already distributed).")
            continue

        logger.info(f"Uploading to {platform}...")

        if platform == "reddit":
            result = _upload_reddit(clip, metadata, platform_cfg, game)
        elif platform in _HANDLERS:
            result = _HANDLERS[platform](clip, metadata, platform_cfg)
        else:
            result = {"success": False, "url": None, "error": f"Unknown platform: {platform}"}

        results[platform] = result
        status = "✓" if result["success"] else "✗"
        logger.info(f"{status} {platform}: {result.get('url') or result.get('error')}")

    # Persist results
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        meta["distribution"] = results
        meta_path.write_text(json.dumps(meta, indent=2))

    return results
