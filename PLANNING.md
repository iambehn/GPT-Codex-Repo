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

## Changelog

| Date       | Change                          |
|------------|---------------------------------|
| 2026-04-05 | Initial planning document       |
| 2026-04-05 | Corrected pipeline order: AI Scoring moved after Processing; Decision Engine uses metadata only |
| 2026-04-05 | Template library created: schema + 5 starter templates. Music disabled pending music library. Blur pillarbox set as default vertical fill. |
| 2026-04-07 | Applied scope decisions: FPS games (Arc Raiders, Marvel Rivals, Deadlock), Twitch clips pages as source, segment-level Whisper captions (word-highlight deferred), Flask for Review UI, Claude API for scoring, direct API calls for distribution, Viewership as optimize metric. Per-game subfolder structure added to all pipeline folders. FPS keyword lists added to Decision Engine. |
