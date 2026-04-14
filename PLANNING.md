# Gaming Clip Farming Bot — Project Planning

> Living document. Update as the system evolves.

---

## Pipeline Overview

```
[ Ingestion ]
     ↓
[ Transcription ]
     ↓
[ Feature Extraction ]
     ↓
[ Decision Engine ]        ← picks template from metadata (no AI score yet)
     ↓
[ Processing ]
     ↓
[ AI Scoring Engine ]      ← virality score assigned to the finished clip
     ↓
[ Manual Review ]
     ↓
[ Output ]
     ↓
[ Optimize ]
```

---

## Stages

### 1. Ingestion
**Tool:** yt-dlp

**Sources:** Twitch game clips pages (one per game)
- Arc Raiders: `https://www.twitch.tv/directory/game/Arc%20Raiders/clips`
- Marvel Rivals: `https://www.twitch.tv/directory/game/Marvel%20Rivals/clips`
- Deadlock: `https://www.twitch.tv/directory/game/Deadlock/clips`

- Download clips from the game's Twitch clips page
- Classify inputs before processing: check duration, resolution, and audio levels
- Assign each clip a quality tag (e.g., `low`, `medium`, `high`) based on classification
- On error: log the error, move the file to a quarantine folder, and continue — do not crash
- Quarantine bad clips so they don't block the pipeline or clutter the workspace
- The `{game}` token in output filenames is derived from the subfolder the clip was downloaded into

**Folders:** All pipeline folders use game-specific subfolders.
```
inbox/
  arc_raiders/
  marvel_rivals/
  deadlock/
quarantine/
  arc_raiders/
  marvel_rivals/
  deadlock/
processing/
  arc_raiders/
  marvel_rivals/
  deadlock/
accepted/
  arc_raiders/
  marvel_rivals/
  deadlock/
rejected/
  arc_raiders/
  marvel_rivals/
  deadlock/
```

---

### 2. Transcription
**Tool:** Whisper (OpenAI) — segment-level output

- Convert speech to text for each clip
- Store transcript alongside the clip (e.g., `clip_001.txt`)
- Used downstream by Feature Extraction and AI Scoring

> **Future upgrade:** Word-level timestamps (WhisperX) are the planned upgrade path for word-highlight captions — not in scope for the initial build.

---

### 3. Feature Extraction

Extract structured metadata from each clip:
- Duration
- Motion level (scene change frequency / optical flow)
- Audio levels (peak, average, silence ratio)
- Keywords (from transcript)
- Quality tag (carried over from Ingestion)

Output: a JSON metadata file per clip (e.g., `clip_001_meta.json`)

---

### 4. Decision Engine

- Reads clip metadata from Feature Extraction
- Selects the most appropriate **template** for the clip
- No virality score exists yet — template selection is driven purely by metadata (duration, motion, audio energy, keywords, quality tag)
- A template defines:
  - Timeline structure
  - Text/caption placement and style
  - Effects (zoom, color grade, transitions)
  - Resolution and format
  - Audio settings
- Templates are versioned (e.g., `template_hype_v2`) so settings (zoom strength, clip length, pacing, caption style) can be refined without losing prior versions
- Decision logic: rule-based matching first, ML-based selection later

**FPS Keyword Lists** (used for template matching — game-specific keywords extend the shared set):

| Scope | Keywords |
|---|---|
| All FPS (shared) | ace, kill, clutch, headshot, one tap, multi-kill, 1v5, streak, insane, insane play |
| Arc Raiders | wipe, squad wipe, extraction, clutch extract, solo, ambush, no scope, headshot, raid, down |
| Marvel Rivals | ult, team wipe, clutch, wombo combo, flank, dive, penta, dominate, shutdown, combo |
| Deadlock | gank, teamfight, wipe, carry, clutch, comeback, steal, outplay, lane, ambush |

The existing `fast_hype` and `cinematic_highlight` templates already cover the shared FPS keyword set. Game-specific keywords increase matching precision without requiring new templates.

**Template Library** (`templates/`):
- `fast_hype/` — high energy, fast cuts, kill streaks
- `cinematic_highlight/` — single impactful moment, cinematic pacing
- `recap_montage/` — multi-clip session compilation
- `commentary_reaction/` — low-motion, mic-heavy content
- `tutorial_tips/` — instructional, keyword-triggered
- *(add more as needed)*

---

### 5. Processing
**Tools:** FFmpeg (permanent primary processing engine)

- Implement a **Watch Folder / Hot Folder** system:
  - Loop continuously over the `inbox/` folder
  - Pick up new files and process them one by one
  - Move each file through the pipeline stages
- Apply the template selected by the Decision Engine
- Encode output per platform format (see Output stage)
- HandBrake may be used as an optional final compression pass if file size is a concern (not a core dependency)

> **Future — Music Library:** Each template has a `background_music` slot with `enabled: false` and `asset_path: null`. When a music library is added, set `enabled: true` and point `asset_path` at an audio file (e.g., `assets/music/hype_track_01.mp3`). Plan to organize the library by energy level or genre and tag tracks to match template types. Audio ducking settings are already configured per template so music will automatically duck under speech once enabled.

---

### 6. AI Scoring Engine
**Tool:** Claude API

- Runs on the **finished processed clip** (post-production)
- Assigns a **virality score** to the clip (e.g., 0–100)
- Score factors may include: energy, keyword relevance, pacing, audio impact, visual quality
- Score is stored in the clip's metadata JSON and displayed during Manual Review
- Over time, scoring weights should be tunable via the Optimize stage

> **Note:** A secondary, lightweight pre-processing score (derived purely from raw metadata) could also feed the Decision Engine to help pick templates — but the primary virality score lives here, after the clip is fully produced.

---

### 7. Manual Review
**Tool:** Flask web app with built-in video player

Features:
- Video player for watching processed clips
- Virality score displayed alongside the clip
- Toolbar for manual edits:
  - Trim (in/out points)
  - Resize / crop
- Soundboard: overlay popular sound bites onto the clip audio
- **Approve** button → moves clip to `accepted/`, loads next clip
- **Reject** button → moves clip to `rejected/`, loads next clip
- Queue view: shows remaining clips to review

---

### 8. Output

**File Naming Convention:**
```
{game}_{date}_{clip_id}_{platform}.{ext}
# Example: valorant_20260405_clip042_tiktok.mp4
```

**Platform Formats:**

| Platform         | Format | Resolution  | Aspect Ratio | Max Duration | Notes                     |
|------------------|--------|-------------|--------------|--------------|---------------------------|
| YouTube          | MP4    | 1920×1080   | 16:9         | Unlimited    | H.264, AAC                |
| YouTube Shorts   | MP4    | 1080×1920   | 9:16         | 60s          | Vertical                  |
| TikTok           | MP4    | 1080×1920   | 9:16         | 10 min       | Vertical preferred        |
| Instagram Reels  | MP4    | 1080×1920   | 9:16         | 90s          | Vertical                  |
| Twitter/X        | MP4    | 1280×720    | 16:9 or 1:1  | 2 min 20s    | H.264                     |
| Reddit           | MP4    | 1920×1080   | 16:9         | Varies       | Check subreddit rules     |

---

### 9. Optimize

**Primary metric: Viewership** (total views per published clip). May be revised once enough data accumulates — follow the single-metric rule and do not change until the current metric has been properly evaluated.

Principles:
- Inputs: current system state, performance data, objectives, constraints
- Make small, measurable changes that improve the defined metric — one variable at a time
- Measure before and after each change
- Keep rollback mechanisms: versioned templates, config history, model checkpoints
- Avoid scope creep: no new variables until the current change is evaluated
- Iterative loop: `Experience → Measurement → System Design → repeat`

---

### 10. Research (Ongoing)

See [TOOLS.md](./TOOLS.md) for a full reference of all tools in the ecosystem — status, cost model, pipeline fit, and reason for exclusion where applicable.

---

## Open Questions

- [x] Primary metric for Optimize: **Viewership** (may change once data accumulates)
- [x] Clip sources: **Twitch game clips pages** (Arc Raiders, Marvel Rivals, Deadlock)
- [x] Initial game targets: **Arc Raiders, Marvel Rivals, Deadlock** (FPS focus)
- [x] Deployment: **Local desktop**

---

## Backlog — Copyright Music Detection

> Not yet implemented. Come back to this before enabling distribution on YouTube or TikTok.

### Why it matters
Twitch clips often contain licensed music (in-game soundtracks, streamer background music). YouTube Content ID and TikTok's matching system use audio fingerprinting and will automatically mute, monetize against, or remove flagged videos.

### Current interim solution
`config.yaml → audio.mode` can be set to `"mute"` (strip all audio) or `"replace"` (swap in a royalty-free track). This is a blanket policy — no detection, applies to all clips. Safe but removes all original audio.

### Recommended detection approach: ACRCloud
ACRCloud is the industry-standard audio fingerprinting service. Used by Twitch, SoundCloud, and Deezer. Free tier: 100 recognitions/day — sufficient for a personal pipeline.

**Sign up:** https://www.acrcloud.com

**How it works:**
1. Extract 10–15 seconds of audio from the clip as an audio fingerprint
2. Send to ACRCloud API — returns: matched song title, artist, confidence score (0–100)
3. If confidence ≥ threshold (e.g. 80) → apply configured `audio.mode` (mute or replace)
4. If no match → keep original audio

**Python integration:**
```bash
pip install pyacrcloud
```
The integration stub is already in `pipeline/processing.py` as detailed comments in `run_processing()`. Add these to `.env` to activate:
```
ACRCLOUD_ACCESS_KEY=...
ACRCLOUD_ACCESS_SECRET=...
ACRCLOUD_HOST=identify-eu-west-1.acrcloud.com
```
Then set `config.yaml → audio.detection.enabled: true`.

**Alternative:** AudD.io — simpler REST API, 100 free recognitions/month, then $20/month.

---

## Backlog — Quality of Life Features

> Prioritized list of improvements to implement after core pipeline is stable.

### High priority

1. **Language filter** — Most impactful near-term fix. Whisper detects the transcript language on every clip. Add `transcription.language_filter: "en"` to config. After transcription, check the detected language; if it doesn't match, skip the clip and log a warning. Eliminates all the Russian/Cyrillic/non-English clips currently cluttering the queue. One check in `pipeline/transcription.py`, ~5 lines of code.

2. **`--watch` mode** — Add a `python run.py --watch` flag that loops continuously: run pipeline for all games → sleep `pipeline.watch_interval_seconds` → repeat. Makes the pipeline truly hands-free. Currently requires manual re-triggering.

3. **`--dry-run` flag** — Process clips and show what WOULD be distributed (platforms, titles, scores) without actually uploading anywhere. Essential for testing new platform setups without burning API quota or posting test content publicly.

4. **Clip freshness filter** — Twitch's top clips by view count are largely the same week over week. Add `ingestion.max_clip_age_hours: 72` to config. Use the Twitch Helix clips API's `started_at` field (already returned) to skip clips older than the threshold. Prevents re-downloading stale viral clips on every run.

### Medium priority

5. **Thumbnail generation for YouTube** — YouTube rewards videos with custom thumbnails. Extract a keyframe from the highest-motion moment using FFmpeg (`-vf select='gt(scene,0.4)'`) and pass it to the YouTube API's `thumbnails.set()` endpoint. Big engagement lift for essentially no extra work.

6. **Video thumbnails in review UI** — The review queue shows filenames only. Use FFmpeg to extract a single frame at the 3-second mark for each clip in `processing/` and serve it as an `<img>` in the queue. Makes clip selection much faster.

7. **TikTok publish_id polling** — After uploading to TikTok, the URL is not returned immediately (TikTok processes the video async). Add a background polling function that checks `POST /v2/post/publish/status/fetch/` using the stored `publish_id` and updates the meta.json with the final URL once it's available. Completes the analytics picture.

8. **Reddit flair auto-detection** — Some subreddits require a flair tag or they auto-remove the post. Add a one-time config step: for each configured subreddit, call `subreddit.flair.link_templates` via PRAW and store the correct flair ID in `config.yaml`. Apply on `submit_video()`. Prevents silent post removals.

### Lower priority

9. **Retry failed distributions** — Add `python run.py --retry-failed` that re-runs distribution only for clips in `accepted/` that have `distribution.{platform}.success: false` in their meta.json. Avoids re-running the full pipeline just to retry an API timeout.

10. **Clip deduplication** — Twitch clips have stable IDs in their URLs. Store seen clip IDs in a local `seen_clips.json` file and skip re-downloading on subsequent runs. Currently the idempotency check relies on the filename already existing in `inbox/`, which is fragile if files are renamed or moved.

11. **Email/desktop notification** — Send a notification when a batch run completes (e.g. "12 clips processed, 3 failed"). On macOS: `osascript -e 'display notification "..." with title "ClipBot"'`. Useful for long overnight runs.

12. **Aggregate stats in review UI** — Add a `/stats` route to the Flask review app showing: total clips by game, acceptance rate, average highlight score, top keywords, clips distributed per platform. Basic analytics without needing to open the Google Sheet.

---

## Changelog

| Date       | Change                          |
|------------|---------------------------------|
| 2026-04-05 | Initial planning document       |
| 2026-04-05 | Corrected pipeline order: AI Scoring moved after Processing; Decision Engine uses metadata only |
| 2026-04-05 | Template library created: schema + 5 starter templates. Music disabled pending music library. Blur pillarbox set as default vertical fill. |
| 2026-04-12 | Brainstorming: Scalable Faceless Content System notes added (see bottom of document) |
| 2026-04-13 | Analytics logging (Google Sheets), Drive backup, audio copyright mode added |
| 2026-04-13 | Backlog notes added: copyright detection (ACRCloud) and QoL feature roadmap |

---

## Brainstorming: Scalable Faceless Short-Form Content System

> Additional notes for future reference — not current build scope.

### Core Philosophy

Treat content creation as a system, not a creative process. Optimize for speed, consistency, and low decision-making overhead. Each channel operates as a content production pipeline.

---

### Content Paradigm

Every video follows a fixed structure:

**Format:** Vertical (9:16), 20–40 seconds

**Structure:**
1. Hook (first 1–2 seconds)
2. Context (brief setup)
3. Main content / payoff
4. Loop or cliffhanger ending

**Visual Rules:**
- Subtitles required on every video
- Visual change every 2–3 seconds
- Background music always present

---

### Template System

Templates replace scripting and enable scale. Each template contains:
- Pre-built timeline (cuts, transitions)
- Caption styles (font, animation, placement)
- Audio presets (music + SFX)
- Visual effects (zoom, motion, overlays)

Workflow: duplicate template → replace assets (clips, text, audio) → export.

---

### Workflow Pipeline

1. **Idea Generation** — source trending topics or reusable formats
2. **Script / Clip Selection** — AI-generated scripts or extracted clips
3. **Asset Collection** — video clips, images, audio
4. **Editing (Template-Based)** — insert assets into predefined template
5. **Export & Upload**

---

### Content Unit Strategy

Maximize output per source:
- 1 long video / stream → 10–30 short clips
- 1 topic → multiple angle variations

Focus on batch processing, not one-off videos.

---

### CapCut Capabilities and Constraints

**Capabilities:**
- Template-based editing
- AI tools: auto captions, text-to-speech, AutoCut / highlight generation
- Presets for text, effects, and audio

**Constraints:**
- No official scripting or API
- No macro recording
- Limited UI customization

---

### Automation Approach

**Inside CapCut:** Templates are the primary automation method. Presets reduce repetitive setup. AI features reduce manual editing time.

**Outside CapCut (optional enhancements):**
- Clip sourcing: Twitch VOD downloaders, yt-dlp
- Transcription: Whisper
- Workflow automation: n8n, Zapier
- Video processing: FFmpeg, MoviePy

**Pseudo-automated workflow:**
1. Download source content (streams, podcasts)
2. Extract clips
3. Load template project in CapCut
4. Replace assets
5. Apply auto captions
6. Export and publish

---

### Quality Control Checklist

Each video must pass before publishing:
- Hook within first 2 seconds
- No dead air
- Subtitles present
- Visual change every 2–3 seconds
- Clear audio

---

### Channel Archetypes

Reusable channel types, each with its own template, pacing rules, and content sources:

| Archetype | Description |
|---|---|
| Clip-based | Gaming highlights, podcast moments |
| Story-based | Reddit stories, interesting facts |
| Educational micro-content | Quick tips, how-tos |
| AI narration | Fully generated voiceover content |

---

### Performance Targets

| Task | Target Time |
|---|---|
| Idea / Script | 2–5 min |
| Editing | 5–15 min |
| Upload | 2 min |

Goal: 10–20 videos per day at scaled output.

---

### Key Insight

CapCut is a template-driven system, not a programmable tool. Automation is achieved through predefined structures, asset replacement, and AI-assisted editing — not scripting or code.

---

### Future Optimization Path

- Build external automation pipeline (Python + FFmpeg) — *this is what we're building*
- Store clips and metadata in structured datasets
- Test and rank video performance
- Iterate templates based on analytics

---

## Brainstorming: Channel Archetype — Story-Based / Lore Narration Shorts

> Additional notes for future reference — not current build scope.

### Core Concept

**Content Type:** Short-form narrated stories (20–60 seconds)

**Topics:**
- History (events, figures, mysteries)
- Fictional lore (fantasy / sci-fi worlds)

**Examples:**
- League of Legends lore
- Warhammer 40,000 factions
- World of Warcraft characters

---

### Content Paradigm (Strict Ruleset)

**Structure (non-negotiable):**

1. **Hook** (0–2s) — "This character wiped out an entire army alone…" / "This empire collapsed in 3 days…"
2. **Context** (2–5s) — Who / where / when
3. **Story** (5–40s) — Key events only (compressed storytelling)
4. **Ending** — Cliffhanger or twist; encourages looping

**Visual System:**
- Slideshow style (image changes every 2–4 seconds)
- Subtle motion (zoom / pan)
- Dark / cinematic tone
- Consistent font and caption style

**Audio System:**
- AI narration (calm, dramatic tone)
- Background music (low volume, cinematic)
- Optional ambient sound effects

---

### Tool Stack (Optimized for Simplicity + Scale)

| Purpose | Primary Tool | Alternatives |
|---|---|---|
| Script generation | ChatGPT (structured prompt) | Notion for idea/script storage |
| Voice generation | ElevenLabs | CapCut TTS (simpler, faster) |
| Visual assets | Midjourney, DALL·E | Wiki pages, game lore archives |
| Editing / assembly | CapCut (template-based slideshow, auto captions) | — |
| Automation layer | n8n, Python + FFmpeg | — |

---

### Content Sourcing System

#### A. Structured Source Pools

**1. Fictional Lore**
- Official wikis: League of Legends, World of Warcraft, Warhammer
- Character pages, event timelines
- Each page generates content for multiple videos

**2. Historical Content**
- Wikipedia: "Obscure history" lists, battle pages, biography articles
- Topic categories:
  - Mafia / gangsters / Yakuza
  - Ancient empires and collapses
  - Forgotten wars and sieges
  - Mysterious historical figures

**3. Community Sources**
- Reddit:
  - r/AskHistorians
  - r/40kLore
  - r/leagueoflegends lore threads

#### B. Content Extraction Method

For each source:
- Identify the character / event
- Extract 3–5 key moments
- Convert into a short narrative script

---

### Script Generation Framework

**Fixed prompt template:**
> "Summarize [topic] into a 30-second story with: a strong hook, clear narrative progression, and a dramatic or surprising ending. Keep sentences short and engaging."

**Output format:**
- 5–8 sentences maximum
- Each sentence = one visual scene

---

### Visual Production System

**Per video:**
- 5–10 images total
- Types: characters, landscapes/cities, battle scenes

**Consistency rules:**
- Same art style per channel
- Same color tone (dark fantasy, etc.)
- Same motion style (slow zoom)

