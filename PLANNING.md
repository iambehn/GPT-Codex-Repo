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

---

## Brainstorming: Channel Archetype — Interactive Poker Clips (Guess the Hand / Range)

> Additional notes for future reference — not current build scope.

### Core Concept

**Content Type:** Short-form interactive poker clips (20–45 seconds)

**Engagement mechanic:** viewer guesses hidden information — hand, card, or outcome

**Format variations:**
- Hide one player's full hole cards
- Hide one card (turn or river suspense build)
- Hide both hands → reveal winner at the end
- Range guessing (e.g., suited connectors vs. premium pairs)

---

### Content Paradigm (Strict Ruleset)

**Structure:**

1. **Hook (0–2s)** — "Can you guess this hand from the action?" / "Only pros get this right…"
2. **Setup (2–6s)** — Stakes, positions, preflop action
3. **Hand Progression (6–25s)** — Flop → turn → river with key bets highlighted
4. **Interactive Phase** — Prompt appears: "What is Player A holding?" with multiple choice answers
5. **Reveal (final 3–5s)** — Cards shown, optional quick explanation

---

### Engagement Mechanics (Core Differentiator)

**A. Hidden Information System**
- Mask hole cards, a single card, or the entire board (rare variant)
- Keyframed blur or overlay shape follows card movement

**B. Multiple Choice System**

Basic version:
- A: AK  B: QQ  C: 76s  D: Bluff

Advanced variant:
- Options eliminate over time (Progressive Elimination Effect)
- Countdown timer builds tension

**C. Range-Based Questions**
- "Which range does this belong to?" → Value / Bluff / Draw / Trap
- Aligns well with poker / statistics background — natural content angle

---

### Content Sourcing System

**Primary sources:**
- Twitch VODs (downloaded early — live VODs can be deleted, archive locally)
- YouTube tournament streams
- Online poker replay sites

**Clip extraction strategy (per stream):**
- Long streams (4–8 hours) → identify 20–50 hands
- Target: big pots, all-ins, unusual lines, hero calls
- Extract: 20–40 second segments per hand
- 1 stream = 20–50 clips

---

### Editing System (Template-Based)

Primary tool: **CapCut** (not FFmpeg — animation and UI effects are visual timeline work)

**Template layers:**
1. **Base video** — cropped poker table footage
2. **Masking / censoring** — blur or shape overlay on cards, keyframed to follow movement
3. **UI overlay** — text prompts, multiple choice answers, countdown timer
4. **Reveal layer** — mask removed OR hard cut to uncensored version

**CapCut implementation methods:**

| Method | How |
|---|---|
| Simple Overlay | Text layers A/B/C/D, fade in at start, fade out incorrect answers |
| Progressive Elimination | All answers appear → timed animations remove wrong options at 2s, 4s, etc. |
| Countdown Timer | Animated 3→2→1 countdown synced with music buildup |
| Correct Answer Highlight | Post-reveal glow effect or color change (green = correct, red = wrong) |

**FFmpeg role in this archetype:**
- Batch cropping and encoding (yes)
- Interactive visuals and UI effects (no — use CapCut for those)

---

### Repeatable Workflow

1. **Source** — Download VOD
2. **Select** — Identify 10–20 hands per stream
3. **Edit** — Drop into template, add mask, add question + choice overlay
4. **Export** — Standardized format for all platforms

---

### Consistency Rules

- Same question format every video
- Same answer layout and position
- Same animation timing
- Same reveal style

---

### Anti-Removal / Transformation Strategy

This archetype has strong copyright defense:
- Adds an interactive layer not present in the original
- Changes the original context entirely
- Introduces original editing, overlays, and optional narration
- Qualifies as transformative use under most platform content policies

---

### Scaling Potential

- Endless content from long streams (1 stream = 20–50 clips)
- Multiple format variants: Guess the Hand, Guess the Range, Guess the Outcome
- Natural expansion path: strategy explanation clips, hand history reviews

---

### Optional Next Steps (Future)

- Design a CapCut template layout (layer-by-layer blueprint) → **see section below**
- Define a data-driven hand tagging system (tag hands by type, stakes, outcome) and track which formats perform best in analytics → **see section below**

---

## Brainstorming: Poker Channel — CapCut Template Blueprint + Automation Layer

> Detailed implementation notes. Not current build scope.

### Reality Check: CapCut + Automation

CapCut has no scripting API. Automation happens **before** CapCut, not inside it.

```
[AI + Scripts + Clip Detection] → [Assets Ready] → [CapCut Template Assembly]
```

CapCut = execution layer (manual but fast). Templates reduce assembly to **2–5 minutes per video** once built. You are not building each video from scratch — you are replacing the base clip, adjusting mask position, and updating text.

---

### CapCut Template Layout — Layer-by-Layer Blueprint

Design once → reuse forever.

| Track | Purpose | Notes |
|---|---|---|
| 1 | Base video | Poker clip, cropped/zoomed on table |
| 2 | Mask layer | Rectangle/blur overlay on hole cards; keyframe if camera moves |
| 3 | Question prompt | "What does Player A have?" — appears at ~2s |
| 4 | Multiple choice UI | A/B/C/D text layers, consistent position every video |
| 5 | Timer / tension | Countdown 3→2→1 or animated progress bar |
| 6 | Elimination animation | Option D fades at 2s, Option B fades at 4s (pre-built) |
| 7 | Reveal layer | Remove mask OR hard cut to uncensored clip |
| 8 | Result highlight | Correct answer glows green, wrong answers dim |
| 9 | Audio | Background music + optional narration track |

---

### Why CapCut Over FFmpeg for This Archetype

| Task | Tool |
|---|---|
| Batch cropping and encoding | FFmpeg |
| Interactive UI overlays | CapCut |
| Animation timing | CapCut |
| Visual iteration | CapCut |

FFmpeg has no visual timeline. Animations and UI effects require it.

---

### Card Detection Automation (Advanced — Future Work)

Three tiers of complexity:

**Tier 1 — Manual (Baseline, Start Here)**
- Place mask once in the template
- Reuse coordinates across clips
- Works well when layout is consistent (fixed-camera streams)

**Tier 2 — Semi-Automated (Practical)**
- Standardize your crop/zoom so card positions are always the same
- Predefine mask coordinates in a config file
- Apply programmatically via FFmpeg or an image overlay script

**Tier 3 — Full Automation (Advanced, High Setup Cost)**
- OpenCV + YOLO object detection to detect and track card regions frame-by-frame
- Automatically generate keyframed blur boxes
- Requires training data and significant upfront ML work — overkill initially

---

### AI Tools for Clip Detection

This archetype benefits from automated highlight detection to surface the best hands.

**What to detect:**
- Big pots / all-ins / showdowns
- Emotional reactions from streamer
- Rapid action sequences

**Tool options:**

| Tool | Use |
|---|---|
| Whisper | Detect excitement spikes in commentary audio |
| PySceneDetect | Detect cuts and transitions (useful for showdown moments) |
| Custom classifier (long-term) | Train on pot size changes, action frequency, commentary intensity |

**Practical starting point:** manual scanning + timestamping. Evolve toward semi-automated detection once content volume justifies it.

---

### AI Narration Workflow

Narration adds context and tension without requiring screen presence.

**Use cases:**
- Explain action: "He 3-bets preflop… barrels the turn…"
- Build tension: "This decision changes everything…"

**Workflow:**
1. Generate script (ChatGPT using the fixed prompt template)
2. Convert to voice (ElevenLabs for quality; CapCut TTS for speed)
3. Drop audio into Track 9 of template

---

### Data Tagging System (Analytics Edge)

Structured database of clips — this is where a poker/statistics background turns into a content advantage.

**Schema (per clip):**

| Field | Examples |
|---|---|
| Hand type | Bluff, value bet, set, draw, trap, hero call |
| Situation | 3-bet pot, river shove, flip, cooler |
| Position | BTN, BB, CO, UTG |
| Action type | 3-bet, check-raise, overbet, fold equity |
| Result | Win / Lose |
| Hook type | Question, statement, statistic |
| Question format | Multiple choice, open-ended, range |
| Reveal style | Hard cut, mask remove, freeze frame |
| Views | From platform analytics |
| Watch time % | From platform analytics |
| Comments / engagement | From platform analytics |

**What the data answers over time:**
- Do bluff clips outperform value-hand clips?
- Do multiple-choice formats beat open-ended questions?
- Which hook types (question vs. statement) drive more views?
- Which hand types generate the most comments?

This maps directly into the existing analytics spreadsheet infrastructure already built for the gaming channel — the same Google Sheets setup can log poker clip data with a separate tab.

---

### Full Pipeline Overview

```
1. Source poker VOD (Twitch / YouTube / replay site)
2. Detect interesting clips (manual → semi-automated over time)
3. Tag clips in database (hand type, situation, action)
4. Generate script (ChatGPT prompt template)
5. Generate narration (ElevenLabs / CapCut TTS)
6. Load into CapCut template (replace clip + adjust mask + update text)
7. Export + distribute
```

---

### Housekeeping Note

When `PLANNING.md` becomes too long to navigate comfortably, split into:
- `PLANNING.md` — core pipeline (Stages 1–9)
- `BRAINSTORMING.md` — channel archetypes and system design notes
- `BACKLOG.md` — QoL feature list and copyright detection roadmap

---

## Brainstorming: Monetization Strategy — Archetype Comparison + Brand Deals

> Business strategy notes for future reference — not current build scope.

### Revenue Reality

Platform payouts (CPM/RPM) are typically not the primary income source:
- TikTok / Instagram Reels: very low or inconsistent
- YouTube Shorts: improved but still modest compared to long-form

**Where the money actually comes from:**
- **Brand deals / sponsorships** — the majority of income once you have traction
- **Affiliates / products** — meaningful in niches with high-converting audiences (poker tools, coaching)

**Rule of thumb by stage:**
- Early: ~100% platform payouts (small absolute numbers)
- Growth: 60–90% from sponsorships, rest from platform + affiliates

---

### Archetype Monetization Comparison

| Archetype | Views ceiling | CPM quality | Sponsor appeal | Best for |
|---|---|---|---|---|
| Gaming clips | Highest | Low–medium | Medium (peripherals, energy drinks, game launches) | Growth + volume |
| Lore / narration | Medium | Medium–high | High (books, subscriptions, games, broad brands) | Balanced growth + monetization |
| Poker | Lower baseline, spiky upside | High (finance-adjacent) | Very high (training sites, poker tools, fintech crossover) | High-value niche monetization |

**Key insight:** highest views ≠ highest money. Monetization depends on who watches, how long they watch, and how easy it is for brands to plug into your format.

- Gaming = attention engine
- Poker = monetization engine
- Lore = brand + longevity engine

If the goal is **money per view**, poker or lore outperform gaming. If the goal is **audience growth and top-of-funnel volume**, gaming wins.

---

### Recommended Sequencing (Given This Setup)

1. **Gaming clips** — build volume, prove the system, establish posting consistency
2. **Poker channel** — target the high-value niche, start monetization earlier with smaller audience
3. **Lore channel** — long-term brand deal asset once infrastructure is mature

---

### What Advertisers Actually Look At

They don't buy views. They buy:

- Average views per video (last 10–20 posts) — consistency matters more than one viral hit
- **Watch time / retention %** — the most important single metric for brand pitch
- Engagement rate (likes, comments, shares)
- Audience demographics (age, country) — this is why poker CPM is high; the audience is older, analytical, and higher spending
- Posting frequency — brands want to know you won't go dormant

---

### How Brand Deals Happen (by Stage)

| Stage | Followers | Dynamic |
|---|---|---|
| Early | 0–50k | You reach out; small affiliate deals |
| Mid | 50k–250k | Mix of inbound + outbound; first real sponsorships |
| Established | 250k+ | Brands approach consistently; agencies make contact |

---

### Making Yourself Visible to Advertisers (Do This Early)

**1. Media kit** — a simple one-page document:
- Channel description and niche
- Audience demographics
- Average views and engagement rate
- Example videos

**2. Contact info everywhere:**
- Bio: "Business inquiries: [email]"
- Linktree or landing page

**3. Be brand-friendly in content:**
- Clean editing, consistent format, clear niche identity

**4. Tag relevant brands subtly** — especially in gaming and poker niches

---

### Outreach Pitch Structure

When reaching out to brands yourself:
1. Who you are + your channel niche
2. Your audience (size, demographics, engagement)
3. Why you fit their brand specifically
4. One simple proposal (1 post, 1 integration)

Contact targets: marketing email, creator partnership managers, talent agencies

---

### Data to Track for Negotiation Leverage

Build a simple running table:

| Video | Views | Watch % | Shares | Format |
|---|---|---|---|---|
| example | 80k | 92% | 1.2k | Multiple choice |

When you can say "this format averages 80k views with 92% retention," you have pricing leverage. This feeds directly into the analytics Google Sheet already built — add watch time % as a column once platform analytics APIs are connected.

---

### Pricing Framework (Early Stage)

| Creator stage | Range per post |
|---|---|
| Small | $50–$300 |
| Growing | $300–$2,000 |
| Established | $2,000+ |

**Formula:** `Avg Views ÷ 1,000 × $5–$20` (CPM-equivalent)

---

### Negotiation Mindset

You are selling audience attention and conversion potential, not a video.

**Things to negotiate:**
- Usage rights — can they repurpose your clip in their own ads? (Charge more if yes)
- Number of posts in the deal
- Exclusivity — avoid category exclusivity early; it limits future deals
- Payment terms — net-30 is standard, push for 50% upfront on large deals

**Common early-stage traps to avoid:**
- Lowball deals framed as "exposure"
- Commission-only affiliate arrangements with no guaranteed minimum
- Long-term contracts with usage rights you don't fully understand

---

### When to Bring in Professionals

- **Early stage:** not necessary; use common sense and simple written agreements
- **Deals over $1,000–$5,000:** consider a one-time lawyer contract review
- **Long-term or exclusivity contracts:** always get legal review
- **Revenue growing:** accountant for tax structure (content creation income has specific considerations)

---

### Future Assets to Build

- **Media kit template** — a one-page PDF with channel stats, audience demographics, example placements, and contact info. Build this once stats are meaningful (target: after first 30 days of consistent posting).
- **First sponsor pitch template** — short outreach email tailored to the poker channel's natural sponsor set (training sites, HUD tools, online platforms).

---

## Strategic Direction: FPS Gaming — Multi-Channel, First-Mover Model

> Core strategic decisions. This section defines the operating model for the project.

### Chosen Direction

**One channel per game. FPS-focused. First-mover advantage over editing skill.**

Channel naming convention examples:
- Arc Raiders Daily
- Deadlock Moments
- Top Marvel Rivals Clips

The competitive edge is not editing quality — it's being **first** in the channel namespace for newer games before established creators claim the territory. This is a systems advantage: the pipeline allows launching a new game channel faster than a manual creator can, and exiting a dead game with zero sunk cost.

---

### Why This Works Against Established Niches

Games like Fortnite, Call of Duty, Rainbow Six Siege, and Valorant already have:
- Large established YouTube channels with loyal audiences
- Experienced editors producing higher-quality content
- Brand relationships and sponsorships locked in
- Algorithm history and search authority built over years

**Don't compete there.** The opportunity is in games that are:
- Newly released or in early access (no established channels yet)
- Growing but not yet saturated (Arc Raiders, Deadlock, Marvel Rivals)
- Emerging from a major update or esports scene development

The goal is to be the default channel for a game before anyone else is.

---

### Game Lifecycle Model

Each game goes through a lifecycle. The pipeline should track it and respond:

| Phase | Signal | Action |
|---|---|---|
| **Launch / Hype** | Twitch viewer count rising, clip volume increasing | Add to pipeline immediately, post daily |
| **Growth** | Consistent clip engagement, growing subreddit | Maintain posting frequency, refine content |
| **Stable** | Plateau in views, consistent but not growing | Keep running, reduce manual attention |
| **Declining** | Clip view counts dropping, Twitch concurrent viewers falling | Reduce posting frequency |
| **Dead** | Near-zero Twitch presence, clips getting no traction | Remove from pipeline, archive channel |

**Signals to watch (available from Twitch Helix API):**
- `view_count` on returned clips — if top clips are getting fewer views week over week, the game is cooling
- Twitch concurrent viewers (separate endpoint: `GET /helix/streams`) — the most direct health signal
- Reddit post activity on game-specific subreddits

**Adding a new game:** one config.yaml entry. The pipeline handles the rest automatically.

**Exiting a game:** flip `enabled: false` in config or remove the entry entirely. No content is deleted, no infrastructure changes needed.

---

### Sponsorship Roadmap

**Stage 1 — Gaming peripherals (natural first target):**
- Headsets, gaming chairs, keyboards, mice, monitors, mousepads
- These companies actively sponsor game-specific channels at modest follower counts (10k–50k)
- Game-specific channels are ideal because the audience has demonstrated they are active gamers
- Target brands: SteelSeries, HyperX, Logitech G, Corsair, Secretlab, DXRacer

**Stage 2 — Game publishers and developers:**
- Developers of the games you're covering may offer: game keys, early access, direct sponsorships
- Arc Raiders (Embark Studios), Marvel Rivals (NetEase), Deadlock (Valve — unlikely to sponsor but others will)
- This is a realistic early-stage partnership because developers want coverage of their games
- Approach: email the community/marketing team directly with channel stats

**Stage 3 — Hardware manufacturers (longer-term):**
- NVIDIA, Intel, AMD, ASUS ROG, MSI
- These companies sponsor at scale — realistic once channels have 50k–100k+ combined subscribers
- FPS gaming is their core demographic; the pitch writes itself

**What to avoid early:**
- Energy drink / food brands — these require large audiences and are often approached rather than outreached
- Broad consumer brands with no gaming connection — poor audience fit, poor conversion, poor rates
- Exclusivity deals that lock out the peripheral/hardware tier

---

### Pipeline Architecture Implications

The current system already supports this model well:

**What works today:**
- Adding a new game = one config.yaml entry (`games:` block + subreddit mapping)
- Twitch Helix API resolves any game by display name automatically
- Each game processes independently via `python run.py --game <name>`
- Distribution already routes by game (Reddit subreddit per game, platform tags)

**What needs to be built for multi-channel:**
- **Per-game YouTube / TikTok channel credentials** — the current distribution assumes one set of API tokens. Supporting separate channels per game requires storing one OAuth token per channel and routing by game at upload time. This is a moderate architecture change to `distribution.py`.
- **Per-game channel config** — extend `config.yaml` to allow each game entry to specify which platform channel ID to post to
- **Game health monitoring** — a lightweight scheduled check that pulls Twitch concurrent viewers for each configured game and logs it to the analytics sheet. Flags games below a threshold automatically.

**What doesn't need to change:**
- Ingestion, transcription, feature extraction, AI scoring, manual review — all game-agnostic already
- Analytics spreadsheet — add a per-channel tab or filter by game column

---

### Decision Framework for Adding / Dropping Games

**Add a game when:**
- It has Twitch presence (top game directory has clips being created)
- No established dedicated short-form channel exists yet (quick YouTube search)
- It's FPS or action-adjacent (fits the existing template system)
- The game has a subreddit with active engagement

**Drop a game when:**
- Clips are averaging fewer than X views after 30 days of posting (set X based on your baseline)
- Twitch concurrent viewers have dropped below a threshold for 2+ consecutive weeks
- The subreddit is inactive or posts are being removed
- A larger established channel has claimed the space and is outperforming significantly

The key principle: **zero emotional attachment to a game**. The channel is an asset, not a hobby. If the data says exit, exit.



