# Gaming Clip Farming Bot — Project Planning

> Living document. Update as the system evolves.

---

## Pipeline Overview

```
[ Ingestion ] → [ Transcription ] → [ Feature Extraction ] → [ Decision Engine ]
     → [ Processing ] → [ AI Scoring ] → [ Manual Review ] → [ Distribution ] → [ Optimize ]
```

---

## Stages

### 1. Ingestion
**Tools:** Twitch Helix API + yt-dlp + FFprobe

- Fetch top clips via Twitch Helix API (`TWITCH_CLIENT_ID` + `TWITCH_CLIENT_SECRET`)
- Download via yt-dlp into `inbox/{game}/`
- Probe each file with FFprobe: duration, resolution, fps, has_audio
- Assign quality tag: `high` (≥1080p), `medium` (≥720p), `low` (≥480p)
- Quarantine: probe failure, no video stream, duration out of configured range, resolution < 480p
- Write sidecar `.meta.json` per good clip; skip if sidecar already exists (idempotent)

**Folder structure:**
```
inbox/{game}/     quarantine/{game}/     processing/{game}/     accepted/{game}/     rejected/{game}/
```

---

### 2. Transcription
**Tool:** Whisper (OpenAI) — segment-level output

- Convert speech to text; stored alongside clip, used by Feature Extraction and AI Scoring
- **Planned:** Language filter — skip non-English clips (`transcription.language_filter: "en"`)
- **Future:** WhisperX for word-level timestamps (word-highlight captions)

---

### 3. Feature Extraction

Extracts and merges into `.meta.json`: duration, motion level, audio levels, keywords (from transcript), quality tag.

---

### 4. Decision Engine

Rule-based template selection from clip metadata (no AI at this stage):

**FPS Keyword Lists:**

| Scope | Keywords |
|---|---|
| All FPS (shared) | ace, kill, clutch, headshot, one tap, multi-kill, 1v5, streak, insane, insane play |
| Arc Raiders | wipe, squad wipe, extraction, clutch extract, solo, ambush, no scope, raid, down |
| Marvel Rivals | ult, team wipe, wombo combo, flank, dive, penta, dominate, shutdown, combo |
| Deadlock | gank, teamfight, carry, comeback, steal, outplay, lane, ambush |

**Template Library** (`templates/`):
- `fast_hype/` — high energy, fast cuts, kill streaks
- `cinematic_highlight/` — single impactful moment, cinematic pacing
- `recap_montage/` — multi-clip compilation
- `commentary_reaction/` — low-motion, mic-heavy
- `tutorial_tips/` — instructional, keyword-triggered

Templates are versioned (e.g. `template_hype_v2`) to allow refinement without losing prior versions.

---

### 5. Processing
**Tool:** FFmpeg

- Applies selected template; encodes output per platform format
- Audio modes: `original | mute | replace` (config.yaml `audio.mode`)
- Caption filter: `subtitles=filename=` (requires FFmpeg with libass; falls back gracefully if missing — fix: `brew upgrade ffmpeg`)
- **Future:** Background music slot in templates (`enabled: false`, `asset_path: null`) — add royalty-free track, set `enabled: true`

---

### 6. AI Scoring Engine
**Tool:** Claude API

- Runs on finished processed clip; assigns virality score (0–100)
- Score factors: energy, keyword relevance, pacing, audio impact, visual quality
- Score stored in `.meta.json`, displayed in Manual Review

---

### 7. Manual Review
**Tool:** Flask web app

- Video player with virality score display
- Approve → `accepted/` | Reject → `rejected/`
- Queue skips already-reviewed clips

---

### 8. Distribution
**Platforms:** YouTube Shorts, TikTok, Instagram Reels, Twitter/X, Reddit

| Platform       | Resolution  | Aspect | Max Duration |
|----------------|-------------|--------|--------------|
| YouTube Shorts | 1080×1920   | 9:16   | 60s          |
| TikTok         | 1080×1920   | 9:16   | 10 min       |
| Instagram Reels| 1080×1920   | 9:16   | 90s          |
| Twitter/X      | 1280×720    | 16:9   | 2m 20s       |
| Reddit         | 1920×1080   | 16:9   | varies       |

Idempotent: already-distributed platforms skipped. Run via `python run.py --distribute`.
Post-distribution: analytics logged to Google Sheets, clip backed up to Google Drive.

**File naming:** `{game}_{date}_{clip_id}.mp4`

---

### 9. Optimize

- Primary metric: **Viewership** (total views per published clip)
- One variable at a time; measure before and after each change
- Versioned templates enable rollback
- Loop: `Experience → Measurement → System Design → repeat`

---

## Backlog — Copyright Music Detection ✓ Implemented

Twitch clips often contain licensed music. YouTube Content ID and TikTok will mute, monetize against, or remove flagged videos.

**How it works:** `pipeline/processing.py` calls ACRCloud before applying any template. A 10-second fingerprint is taken at the 10-second mark of each clip and sent to the ACRCloud API. If confidence ≥ threshold (default 80), the configured `audio.mode` is applied to that clip. If no match, original audio is kept regardless of mode setting.

**Fallback behaviour:** if detection fails (quota exceeded, network error, missing library) the configured `audio.mode` is applied as a blanket policy.

**Setup:**
```
pip install pyacrcloud
# .env:
ACRCLOUD_ACCESS_KEY=...
ACRCLOUD_ACCESS_SECRET=...
ACRCLOUD_HOST=identify-eu-west-1.acrcloud.com
```
```yaml
# config.yaml:
audio:
  mode: "mute"           # applied when copyright is detected
  detection:
    enabled: true
    confidence_threshold: 80
```
Free tier: 100 recognitions/day. Sign up at https://www.acrcloud.com

**Alternative:** AudD.io — simpler REST API, 100 free/month then $20/month.

---

## Setup Notes

### YouTube API Key (Scout Dashboard + Thumbnails)
The scout dashboard needs a **read-only API key** (not OAuth) for the YouTube signal. This is separate from the OAuth credentials used to upload Shorts.

1. Go to Google Cloud Console → APIs & Services → Credentials
2. Create API key → restrict it to **YouTube Data API v3** only
3. Add to `.env`:
```
YOUTUBE_API_KEY=AIza...
```
Free quota: 10,000 units/day. Each scout search = 100 units. Polling 10 games every 6h = 40 requests/day — well within the free limit.

The existing OAuth credentials (`YOUTUBE_CLIENT_ID` / `YOUTUBE_CLIENT_SECRET`) are only needed for uploading videos and are unrelated to this key.

---

## Backlog — Quality of Life Features

### Implemented ✓
1. ~~**Language filter**~~ — Done. Whisper-detected language checked against `transcription.language_filter: "en"` in config.
2. ~~**`--watch` mode**~~ — Done. `python run.py --watch` loops all games on `pipeline.watch_interval_seconds`.
3. ~~**`--dry-run` flag**~~ — Done. `python run.py --distribute --dry-run` previews without uploading.
4. ~~**Clip freshness filter**~~ — Done. `ingestion.max_clip_age_hours: 72` in config; filters by Twitch `started_at`.
5. ~~**YouTube thumbnail generation**~~ — Done. Extracts highest-motion keyframe via FFmpeg scene detection; uploads via `thumbnails.set()`.
6. ~~**Video thumbnails in review UI**~~ — Done. `/thumb/<game>/<stem>` route; cached `.thumb.jpg` sidecar; shown in queue.
7. ~~**TikTok publish_id polling**~~ — Done. `python run.py --poll-tiktok` resolves pending TikTok URLs.
8. ~~**Reddit flair auto-detection**~~ — Done. `python run.py --list-reddit-flairs`; apply via `config.yaml` subreddit_config.

### Lower Priority
9. **Retry failed distributions** — `python run.py --retry-failed` for clips with `distribution.{platform}.success: false`.
10. **Clip deduplication** — Store seen Twitch clip IDs in `seen_clips.json`; skip re-downloads regardless of filename.
11. **Completion notification** — Desktop/email alert when batch run finishes (macOS: `osascript`).
12. **Aggregate stats in review UI** — `/stats` route: clips by game, acceptance rate, avg score, top keywords, clips per platform.

---

## Strategic Direction — FPS Multi-Channel, First-Mover Model

### Core Model

- **Niche:** FPS / competitive shooters only
- **Structure:** One dedicated channel per game
- **Edge:** First-mover advantage on emerging titles
- **Exit:** Drop dead games quickly, zero emotional attachment

**Example channels:** Arc Raiders Daily · Deadlock Moments · Top Marvel Rivals Clips

---

### Why This Works

Don't compete in saturated games (Fortnite, CoD, Valorant, R6S) — established channels have years of algorithm authority, experienced editors, and locked-in brand deals.

The opportunity is in games that are newly released or early access, growing but not yet saturated, or emerging from a major update or esports scene. The goal: be the default channel for a game before anyone else is.

**Operating principles:**
- **Speed > Polish** — first to upload = more impressions; volume = more algorithm chances
- **System > Creativity** — same format, template, and workflow across all channels; only the game changes
- **Timing > Skill** — enter early, exit early; "good enough + fast" beats "perfect + slow"

You are not building a gaming channel. You are building a **system that captures attention during the lifecycle of games**. Treat games like assets in a portfolio — no emotional attachment; if the data says exit, exit.

---

### Game Selection

**Add a game when:**
- Recently released or in early access (no established dedicated short-form channel yet)
- Active Twitch presence with clip-worthy (fast-paced, skill-moment) gameplay
- Growing interest; active subreddit

**Drop a game when:**
- Clips averaging fewer views than your baseline after 30 days of posting
- Twitch concurrent viewers declining for 2+ consecutive weeks
- Subreddit going inactive or a larger established channel has claimed the space

**Data sources:** Twitch category viewer counts (`GET /helix/streams`), YouTube search trends, TikTok clip frequency

---

### Game Lifecycle Model

| Phase | Signal | Action |
|---|---|---|
| **Launch / Hype** | Twitch viewers rising, clip volume increasing | Add immediately, post aggressively |
| **Growth** | Consistent engagement, growing subreddit | Maintain frequency, refine best formats |
| **Stable** | Plateau in views | Keep running, reduce manual attention |
| **Declining** | Clip views dropping, Twitch viewers falling | Reduce posting frequency |
| **Dead** | Near-zero Twitch presence, no clip traction | Remove from pipeline, archive channel |

Adding a game = one `config.yaml` entry. Exiting = flip `enabled: false`. No infrastructure changes.

---

### Content Sourcing

- Track top streamers per game (5–10 per game)
- Download VODs; target: high-skill plays, funny moments, clutch situations
- **1 stream = 20–50 usable clips**
- Pipeline handles download automatically (Twitch Helix API → yt-dlp → FFprobe)

---

### Editing Philosophy

- Clean, fast-paced, minimal effects
- Auto captions on every video; visual change every 2–3 seconds
- Subtle zooms, optional sound effects, clear crop on action
- Standardize everything: hook style, caption format, clip length, template — only variable is the game

---

### Risk Management

| Risk | Mitigation |
|---|---|
| Game dies | Multiple channels — no single point of failure; rotate effort to next game |
| Content removal | Transformative elements (captions, edits, cuts) as standard practice |
| Burnout | Batch work; limit manual effort per video; pipeline automation handles the rest |

---

### Multi-Channel Architecture Requirements

**Works today:**
- New game = one `config.yaml` entry; processes independently
- Twitch API resolves any game by display name
- Distribution routes per-game to correct subreddit

**Needs to be built:**
- Per-game YouTube/TikTok channel credentials (separate OAuth tokens per channel, routing by game at upload time)
- Per-game channel ID in `config.yaml` under each game entry
- Game health monitoring: scheduled Twitch concurrent viewer check → auto-flag games below threshold in analytics sheet

---

### Sponsorship Roadmap

| Stage | Subs | Target | Notes |
|---|---|---|---|
| Early | 10k–50k | Gaming peripherals | Headsets, chairs, keyboards, mice — actively sponsor game-specific channels |
| Mid | 50k–250k | Game publishers/devs | Keys, early access, direct deals — email community/marketing teams directly |
| Advanced | 50k–100k+ combined | Hardware manufacturers | NVIDIA, Intel, AMD, ASUS ROG, MSI — FPS is their core demographic |

Avoid early: energy drinks, broad consumer brands, exclusivity deals that block the peripheral/hardware tier.

---

## Monetization Reference

Revenue split by stage:
- Early: ~100% platform payouts (small absolute numbers)
- Growth: 60–90% from sponsorships, rest from platform + affiliates

**What advertisers actually evaluate:** average views per video (consistency > one viral hit), watch time %, engagement rate, audience demographics, posting frequency.

**Pricing formula:** `Avg Views ÷ 1,000 × $5–$20` (CPM-equivalent)

| Stage | Followers | Range per post |
|---|---|---|
| Small | <50k | $50–$300 |
| Growing | 50k–250k | $300–$2,000 |
| Established | 250k+ | $2,000+ |

**Build early:** media kit (channel description, avg views, audience demographics, example videos). Target: after first 30 days of consistent posting.

**Archetype CPM comparison:** Gaming (low–medium) · Lore/narration (medium–high) · Poker (high — finance-adjacent audience). Highest views ≠ highest money. Gaming = volume/growth engine; poker/lore = monetization engines.

---

## Future Archetypes (Not Current Scope)

Three additional channel types documented for later reference.

**Lore Narration Shorts:** 20–60s narrated history or fictional lore (League, WoW, Warhammer; historical: mafia/gangsters/Yakuza, ancient empires). Slideshow style, AI voiceover (ElevenLabs), cinematic music. Higher CPM than gaming; strong brand deal potential.

**Interactive Poker Clips:** Short poker hands with hidden hole cards — viewer guesses the hand via multiple choice overlay. Requires CapCut (not FFmpeg) for interactive UI layers (countdown timer, elimination animation, reveal). Very high CPM. Data tagging system (hand type, action, result, hook format, engagement) feeds into existing Google Sheets analytics.

**Poker CapCut Template:** 9-track layout: base video → mask layer → question prompt → multiple choice UI → timer → elimination animation → reveal → result highlight → audio. Assembly time 2–5 min per video once built. Card masking tiers: manual coordinates → FFmpeg overlay from config → full OpenCV/YOLO automation (overkill initially).

---

## Game Scouting Dashboard

The goal is to detect momentum early and act faster than everyone else — treating games as attention assets the same way a trader tracks stocks.

### Signals to Track

| Signal | What to monitor |
|---|---|
| Twitch (primary) | Current viewers, peak viewers, # of active streamers |
| YouTube | New uploads per day, views on top clips |
| TikTok | Clip frequency, viral videos emerging |
| Google Trends | Search volume spikes |

### Game Status States

`Upcoming → Beta/Playtest → Newly Released → Trending → Declining → Dead`

---

### Breakout Pattern

Most breakout games follow: small baseline → big streamer plays it → sudden viewer spike → algorithm amplifies clips → other streamers pile in.

**Entry signal:** game jumps from ~500 → 10k+ viewers overnight with a notable streamer playing it.

---

### Real Breakout vs. One-Time Spike

| Type | Characteristics |
|---|---|
| **Real breakout** | Multiple streamers join over days, viewer base stabilizes after spike, clips spread across platforms |
| **One-time spike** | Single streamer, drops 70–90% next day, no spread to other streamers |

**Rule:** If interest spreads → it's real. If it stays concentrated → it's temporary.

---

### Flavor of the Week (Important Caveat)

Some games attract massive multi-streamer attention very quickly but the viewership comes in huge waves and collapses just as fast. This is driven by novelty rather than gameplay depth — every big streamer plays it on day 1 and drops it by day 3.

This pattern is **less common in FPS games** because competitive shooters have a skill ceiling that keeps players engaged long-term. A new battle royale or survival novelty game burns fast; a mechanically deep FPS tends to retain a core audience.

**Warning signs that a spike is flavor-of-the-week:**
- All major streamers appear simultaneously on launch day (coordinated hype, not organic discovery)
- The game has no ranked mode, competitive scene, or meaningful skill progression
- Viewership collapses within 72 hours even with multiple streamers still playing
- Clips are funny/reaction content rather than skill-based highlights

**What this means for the pipeline:** It's still worth creating a channel early — you can capture the spike traffic — but don't invest heavily in long-term channel development until the game shows a stable post-hype baseline (at least 2 weeks of sustained Twitch presence after the initial wave).

---

### Scoring System

**Trend Score (0–10):** +3 Twitch viewer growth, +2 multiple streamers, +2 YouTube clip activity, +2 TikTok presence, +1 Google Trends spike

**Longevity Score (0–10):** +3 competitive/skill-based gameplay, +3 replayability, +2 active community, +2 developer support/roadmap

**Action thresholds:** Trend ≥ 6 AND Longevity ≥ 5 → pursue aggressively. High Trend + Low Longevity → capture the spike, don't build long-term.

---

### Daily Scouting Workflow (15–30 min)

1. Check Twitch categories for new games and sudden viewer spikes
2. Update game scores; flag immediate-action opportunities
3. Weekly: review which games are rising vs. dying; cut anything 2+ weeks below baseline

---

### First-Mover Response

When a confirmed real breakout is detected — act within 24–48 hours: create the channel, upload 5–15 clips/day. Early phase = low competition + algorithm curiosity boost for new topics.

---

### Implementation

- **Start:** Google Sheets — game list, scores, trend notes updated manually each day
- **Later:** Python + Twitch Helix API — scrape viewer counts on a schedule, store in a local database, plot trends, trigger alerts on spikes. Integrates naturally with the existing pipeline's Twitch API auth.

---

## Changelog

| Date       | Change |
|------------|--------|
| 2026-04-05 | Initial planning document |
| 2026-04-05 | Pipeline order corrected; AI Scoring moved after Processing; Decision Engine is metadata-only |
| 2026-04-05 | Template library created; music disabled; blur pillarbox default for vertical fill |
| 2026-04-12 | Scalable faceless content system notes added |
| 2026-04-13 | Analytics (Google Sheets), Drive backup, audio copyright mode added |
| 2026-04-13 | Copyright detection (ACRCloud) and QoL backlog added |
| 2026-04-14 | Strategic direction finalized: FPS multi-channel first-mover model |
| 2026-04-14 | Full document condensed; future archetypes collapsed; monetization and sponsorship roadmap added |
| 2026-04-14 | Game scouting dashboard added: signals, scoring system, breakout detection, flavor-of-the-week warning |
| 2026-04-14 | ACRCloud copyright detection implemented: _detect_copyright() in processing.py; pyacrcloud added to requirements |
