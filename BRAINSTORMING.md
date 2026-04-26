# Gaming Clip Farming Bot — Brainstorming

<<<<<<< HEAD
> Pipeline-shaped idea bank for future directions, open design questions, and workflow experiments.
>
> This file is not the implementation source of truth. If a note here conflicts with [PLANNING.md](/Users/tj/GPT-Codex-Repo/PLANNING.md) or the code, trust the code first.

## Cross-Cutting Workflow Layers — `Planned`

### Game-Pack / Onboarding

- New game onboarding should keep trending toward a reusable “game pack” contract instead of per-game code branches.
- Ideal end state: game metadata, entities, moments, ROIs, score weights, and detector labels can all be scaffolded and then verified with a short manual pass.
- The real open question is how much onboarding can be safely automated before low-quality scraped data becomes a liability.

### Wiki-Based Enrichment

- Explicit-wiki enrichment is a strong draft source for entities, aliases, and icon leads.
- A future promotion flow should let approved draft entities and assets move into the canonical game pack with less manual copy/paste.
- The challenge is keeping scraped data obviously “draft” until a human validates it.

### Gold-Set / Evaluation Workflow

- The project should keep moving toward game-specific gold sets that measure detector precision, judge outcomes, and title quality together.
- Long term, promotion of weights, templates, and models should be gated by repeatable evaluation rather than intuition alone.

### Scout / Market Selection Logic

- Game selection still feels like a cross-cutting system rather than a single-stage feature.
- There is room for a tighter loop between scouting, onboarding priority, channel expansion, and post-performance data.
- The open design question is how much the scout signal should influence strictness, scheduling, or even whether a game gets its own channel at all.

## 1. Future Channel Archetypes — `Speculative`

### Lore

- Game lore, fictional history, and narrated recap formats could extend beyond FPS while still using reusable packaging logic.
- This lane likely wants stronger voiceover, story structure, and visual pacing than the current gameplay-first pipeline.

### Poker

- Interactive poker shorts are still attractive because they create a natural “guess before reveal” loop.
- This probably belongs in its own pipeline with different templates, annotations, and review tools rather than as a bolt-on to the FPS workflow.

### Poker CapCut Template

- A dedicated layered template could support hidden cards, timer pressure, elimination animations, and reveal states.
- If this lane becomes real, the template system likely needs a richer composition model than the current FFmpeg-first gameplay edits.

### Dueling Warriors

- A stylized versus-format channel could compare named fighters, loadouts, or archetypes in short bracket-style edits.
- This would emphasize matchup framing, character identity, and audience debate over raw highlight detection.

### Zombie Outbreak

- A survival / outbreak channel could focus on escalating scenario edits, panic pacing, and narrative compression.
- This format would probably reward stronger atmosphere and story progression than pure mechanical skill clips.

### Other Channel Archetypes

- Patch/meta breakdowns
- Creator reaction or authority formats
- Character- or weapon-specific channels
- Any expansion should still behave like a structured channel archetype, not an ad hoc content branch

## 2. Ingestion — `Speculative`

### Proxy Signal Detection — Cheap Alternatives to Frame-by-Frame Analysis

- Chat velocity
- OCR text spikes
- clip metadata heuristics
- audio-first VOD mining

The core idea is still valid: reduce expensive full processing by finding cheap signals that correlate with strong moments early.

### Audio Event Detection — DSP-Based Kill and Highlight Detection

- Audio spikes can still act as a lightweight pre-filter before heavier visual analysis.
- The stronger version of this idea would separate ambient noise from real hype triggers such as reaction peaks, game announcer cues, or clustered impact sounds.

### Other Ingestion Ideas

- Smarter pre-download ranking for creators, games, or clip sources
- Scarcity-aware ingestion so the system changes how wide it casts the net depending on channel needs
- Better deduplication and freshness heuristics before the pipeline spends real cost

## 3. Transcription — `Speculative`

- Transcription could become more than subtitle generation and keyword extraction.
- Possible future use:
  - reaction intensity scoring
  - callout detection
  - moment labeling from speech
  - narrator-ready semantic summaries for non-gameplay formats
- The main question is whether transcript-derived context can improve early clip judgment without adding too much cost or latency.

## 4. Feature Extraction — `Speculative`

### Clip Detection — What It Actually Means and How to Build It

- Detection is not just “find kills.”
- The real target is:

```text
high-signal moment detection
+ correct timing
+ stable context resolution
+ low false positives
+ low per-clip cost
```

- The useful abstraction is still multi-signal extraction rather than betting everything on one model.

### ROI Standardization and Modular Filtergraphs

- HUD standardization remains important because so much reliable context still comes from fixed UI elements.
- ROI definitions should keep behaving like reusable assets that can feed:
  - OpenCV matching
  - ROI cropping tools
  - visual debug tooling
  - detector-specific preprocessing

### YOLO + NiceShot as Detection-Enrichment Tracks

- YOLO looks best as a factual context layer and flexible visual fallback, not the only authority.
- NiceShot looks best as a candidate-moment and excitement-signal layer, not the sole decider.
- The interesting future question is how to combine their strengths without turning the system into an opaque black box.

### Future Extraction Questions

- Should visual detectors stay modular per signal, or eventually feed one shared feature bundle?
- How much learned extraction is worth the added difficulty in debugging bad keeps and bad rejects?

## 5. Decision Engine — `Speculative`

### Systems Thinking — Filters and Templates

- Filters reduce what enters the expensive path.
- Templates reduce how accepted content gets executed and packaged.
- The long-term advantage is still constraint design rather than “infinite flexibility.”

### Hook / Early-Retention Enforcement

- The project should keep biasing toward first-second clarity instead of treating hooks as optional polish.
- Future work could explore:
  - stricter hook classes
  - different remediation strategies
  - per-channel hook standards
  - hook confidence tied to actual retention outcomes

### Clip Judge Evolution

- The clip judge should stay explainable even if it becomes more sophisticated.
- A good future path is richer evidence and better confidence decomposition, not an un-auditable “score machine.”
- The open design question is how far the judge should learn from post-performance signals without losing deterministic fallback behavior.

### Deterministic vs ML Decision Logic

- Deterministic rules are easier to debug, cheaper to operate, and safer early.
- Learned ranking becomes more interesting only when enough gold-set data and post-performance labels exist to support it.
- If ML expands, quarantine recovery and explanation should remain mandatory.

## 6. Processing — `Speculative`

### FFmpeg Beyond Cutting — Advanced Filter Techniques

- There is still plenty of room for a better internal library of reusable effects:
  - kill zooms
  - cleaner blur stacks
  - game-aware subtitle styling
  - loop-friendly transitions

### Managing FFmpeg at Scale — Library and Orchestration Patterns

- The project would benefit from treating edit logic as composable modules instead of long one-off filter strings.
- Strong long-term direction:
  - reusable filtergraph components
  - parameterized assembly
  - cleaner template orchestration

### Montage Assembly — Multi-Clip Concatenation

- Montage support exists, but the broader idea still has room to grow.
- Future directions:
  - theme-based montages
  - scoreboard or chapter overlays
  - per-game montage archetypes
  - montage ranking logic driven by feedback instead of simple ordering

## 7. AI Scoring — `Speculative`

- Post-processing scoring could evolve from a single highlight score into a more explicit packaging-quality evaluation.
- Possible future dimensions:
  - clarity
  - emotional payoff
  - replayability
  - narrative compression
  - channel fit
- The main tradeoff is whether richer AI scoring actually improves packaging decisions or just adds another expensive opinion layer after the clip judge.

## 8. Manual Review — `Planned`

### Quarantine Review Concepts

- Quarantine should continue behaving like a recovery path, not a dead-end dump.
- The strongest direction is guided repair: show exactly what is missing, let the reviewer add context, then rescan quickly.

### ROI Training / Asset Harvesting

- The manual ROI flow is still a strong pattern for harvesting training assets from real gameplay instead of pristine marketing material.
- Long term, this could expand beyond hero/weapon icons into medals, ability markers, or patch-changed HUD elements.

### Review Feedback Loop

- The new feedback loop opens a path to structured human corrections instead of vague “that felt wrong” judgments.
- The next design question is how much feedback should update thresholds automatically versus simply generating recommendations for review.

### Replay Viewer / Visual Debugging

- A replay viewer with overlays is still one of the highest-leverage missing tools.
- It would make it much easier to debug:
  - bad YOLO detections
  - missed ROI matches
  - hook alignment mistakes
  - incorrect keep/reject decisions

## 9. Distribution — `Planned`

### Algorithm-Aligned Content Strategy

- Distribution thinking should stay grounded in staged testing, not vanity metrics.
- Hooks, retention, replay, and clarity still matter more than raw editing effort.
- Multi-account strategy only works if each account has a clean enough identity that results can be interpreted.

### Modular Title Engine

- Title generation should keep exploiting factual metadata rather than generic hype phrasing.
- The long-term interesting question is how much variation should come from deterministic assembly versus controlled LLM rephrasing.

### Distribution Queue / Compliance Scheduler

- The queue/compliance architecture is the right shape, but there is still room to evolve:
  - better platform-specific scheduling logic
  - duplicate-content safeguards
  - account spacing rules
  - channel-specific posting cadences

### Multi-Account Scheduling and Platform Policy

- A future scheduling model should think in terms of account portfolios, not just posts.
- The hard problem is balancing scale with safety:
  - platform policy boundaries
  - human-assisted versus API-driven posting
  - spacing and duplication constraints
  - keeping channel identities distinct enough to learn from them

## 10. Optimize — `Planned`

### Operating at Scale — Content as Structured Data

- The project should keep moving toward treating each clip, post, snapshot, and decision as linked records.
- That is the basis for reliable experimentation across games, channels, templates, titles, and posting windows.

### Analytics Dashboard / Feedback-to-Weights

- The analytics layer should eventually do more than report winners and losers.
- The interesting long-term use is feeding outcomes back into:
  - title strategy
  - hook policy
  - template ranking
  - per-game weight tuning

### Model Evaluation / Tuning / Canary Promotion

- Gold sets, retrain recommendations, and promotion checks should eventually converge into one clean evaluation lifecycle.
- The big open question is how strict the project should be before allowing new weights, labels, or models into active use.

### Post-Performance Learning Loops

- The strongest future compounding loop is:

```text
post -> measure -> explain -> tune -> re-evaluate
```

- The system should learn from real platform outcomes without losing traceability about why it changed.

## Archived Overlap — `Archived`

- Older detection fragments are now partly represented by the real judge, hook, quarantine, YOLO, and NiceShot architecture.
- Older title-engine fragments are now partly represented by the modular title engine.
- Older analytics and distribution theory is now partly represented by the queue, dashboard, and feedback surfaces.
- Keep the principles; do not treat the older fragments as current implementation specs.
=======
Ideas, frameworks, and future directions that aren't current implementation priorities.
Nothing here is committed to — it's reference material for when the time comes.

---

## Future Archetypes (Not Current Scope)

**Lore Narration Shorts:** 20–60s narrated history or fictional lore (League, WoW, Warhammer; historical: mafia/gangsters/Yakuza, ancient empires). Slideshow style, AI voiceover (ElevenLabs), cinematic music. Higher CPM than gaming; strong brand deal potential.

**Interactive Poker Clips:** Short poker hands with hidden hole cards — viewer guesses the hand via multiple choice overlay. Requires CapCut (not FFmpeg) for interactive UI layers (countdown timer, elimination animation, reveal). Very high CPM. Data tagging system (hand type, action, result, hook format, engagement) feeds into existing Google Sheets analytics.

**Poker CapCut Template:** 9-track layout: base video → mask layer → question prompt → multiple choice UI → timer → elimination animation → reveal → result highlight → audio. Assembly time 2–5 min per video once built. Card masking tiers: manual coordinates → FFmpeg overlay from config → full OpenCV/YOLO automation (overkill initially).

---

## Systems Thinking — Filters and Templates

Filters and templates are two mechanisms for removing human decision points from a workflow so work becomes repeatable instead of constantly re-decided.

**Filters solve the selection problem.** They define what gets processed and what doesn't, based on rules — automatically. Without filters, someone evaluates every incoming item manually: should I act on this? That micro-decision, repeated at scale, is where most cognitive load accumulates.

Examples:
- Only process clips longer than 20 seconds with engagement above threshold
- Only route support tickets tagged "billing" to a specific queue
- Only ingest clips from streamers with ≥ 50k followers

**Templates solve the execution problem.** Once something qualifies, a template defines exactly how it gets handled — title format, editing steps, hashtag rules, posting schedule, required fields. Execution becomes filling in variables, not re-inventing the process each time.

**Together:**

> Filters reduce *what* you think about. Templates reduce *how* you think about it.

That combination creates scalability:
- New items flow in continuously — no manual sorting bottleneck
- Processing output is standardised — no variability from operator to operator
- Volume increases translate to throughput increases, not decision fatigue

The goal in any automated pipeline is to push as much work as possible into:

```
if condition_met → execute_predefined_action
```

### Layered Architecture

```
Input → Filter Layer 1 → Filter Layer 2 → Transform → Template → Delivery
```

**Filter layers narrow scope progressively:**

| Layer | Job | Example |
|---|---|---|
| Schema / validity | Only well-formed data enters | Format checks, duration minimums, resolution threshold |
| Business logic | Only high-value items proceed | Engagement score, profitability, rank threshold |
| Contextual | Only items matching current goal | Viral mode vs. retention mode vs. budget audience |

**Template layers handle output variation without multiplying complexity:**
- **Base template** — universal structure (hook → content → CTA)
- **Variant templates** — structural adjustments per context (short-form vs. long-form, TikTok vs. Shorts)
- **Dynamic fields** — placeholders filled from filtered attributes (tone, topic, score)

**The transformation layer** sits between filters and templates. Filters decide *what survives*. Transforms decide *what matters*. Templates decide *how it is expressed*.

**Separation of concerns is what makes this scalable.** Each layer is independently tunable. Each layer reduces complexity until the final step is almost trivial: fill structured fields and ship.

### Designing and Refining Combinations

Systems don't start with optimal filter-template pairings. They start with reasonable assumptions and improve through measurement.

**Stage 1 — Human-designed pairings.** Chosen by domain understanding: "high-engagement clips → viral short-form template." This already outperforms ad-hoc decision-making even before any optimisation.

**Stage 2 — Feedback loops.** Track outcomes: which filter thresholds produce better results? Which template gets higher retention? Adjustment becomes evidence-based. A/B testing and manual comparison work fine — no ML required.

**Stage 3 — ML (only when necessary).** Useful when there are too many combinations to test manually, or the system needs to adapt continuously. Conceptually it's the same loop — filter → transform → template → outcome → feedback → adjustment — ML just compresses the feedback and decision step.

**Filters and templates are coupled, not independent.** A filter is designed with a specific template in mind:
- "Fast viral short" template → filter for high-energy moments
- "Educational breakdown" template → filter for clarity and structure

**The scaling trick is constraint design.** Instead of 50 filters × 50 templates = 2,500 combinations, design 5 filters each mapped to 1–2 templates. The system becomes stable and predictable without needing intelligence overhead to manage it.

---

## Operating at Scale — Content as Structured Data

At the scale of **100+ short-form posts per day**, "clip farmers" usually stop thinking like creators and start thinking like **operators running a content distribution system**. The workflow becomes less about individual videos and more about **asset tracking, scheduling, analytics, and feedback loops**.

They typically keep everything organized by assigning each clip a unique record in a dashboard or database:

```text
clip_id | source_creator | game | template | platform | channel | post_time | url | views | retention | status
```

A common pipeline is:

1. ingest clip
2. process/edit
3. queue for upload
4. store returned post URL
5. collect performance data
6. compare performance by template/channel/platform

The minimum useful dashboard tracks:

- clip ID
- source file
- target platform
- account/channel
- scheduled time
- upload status
- post URL
- views
- engagement
- retention
- repost eligibility
- error logs

### Paid Tooling

Social media managers commonly use:

- **Hootsuite** / **Buffer** / **Later** / **Metricool** / **Sprout Social**

These provide multi-platform scheduling, post calendars, analytics, team workflows, link tracking, and approval systems.

### Open-Source / Low-Cost Stack

- **n8n** — automation and workflow orchestration
- **Airbyte** — pulling analytics from platform APIs
- **Metabase** — visual dashboards over a database
- **Grafana** — metrics and time-series monitoring
- **Supabase or PostgreSQL** — primary storage
- **Airtable** — lightweight content database (early stage)
- **Appsmith** — internal admin panels

A typical scalable open-source pipeline:

```text
yt-dlp → FFmpeg/Resolve → upload script → database → Metabase dashboard
```

or

```text
ingestion → queue → editor → uploader → analytics collector → dashboard
```

### The Dashboard as Control Panel

The dashboard often becomes the central interface, with clips organized by status:

- "ready to post"
- "posted"
- "underperforming"
- "viral"
- "recycle later"
- "copyright issue"
- "manual review"

At scale, operators usually rely on one of three dashboard styles:

| Style | Characteristics |
|---|---|
| **Spreadsheet** | Simple, works early; Airtable or Google Sheets |
| **Database dashboard** | Custom backend; better for large volume |
| **Kanban** | Visual queue management; clip lifecycle tracking |

Example lifecycle:

```text
Downloaded → Edited → Approved → Scheduled → Posted → Analyzed
```

### Posting Automation

Some teams build custom upload wrappers around platform APIs, browser automation, queue systems, and retry handling — because native social APIs often have limitations (rate limits, missing features, inconsistent behavior).

### Engagement at Scale

At high volume, most operators separate engagement by value:

**High-value:** top comments, repeat viewers, sponsors, potential leads — respond selectively.

**Low-value:** generic comments, spam, one-word replies — let pass.

Common approach:
- engage deeply on a few high-performing posts
- let most posts remain passive
- use analytics to decide where engagement actually moves the needle

Trying to reply to everything at scale feels robotic and doesn't compound. The better model:

```text
Automation handles posting
Human handles relationship
```

### Signals Worth Monitoring

- comment velocity
- save rate
- shares
- retention
- follows per post
- profile clicks
- conversion rate

### Recommended Stack for This Pipeline

For an automated pipeline like this one, the most practical open-source combination:

```text
PostgreSQL + n8n + Metabase
```

This gives automation, storage, visual reporting, low cost, and scalability — without proprietary lock-in.

**The key shift at scale:** you stop managing videos individually and start managing **content as structured data**. Every post becomes a data point for improving hooks, captions, templates, posting times, and platform selection.

---

## Algorithm-Aligned Content Strategy

### How Platform Distribution Works

Every post on TikTok and Instagram goes through a staged test:

```text
Small initial batch → measure behavior → expand if strong, die if weak
```

The only metrics that determine whether a post expands:

1. **Scroll-stop rate** (first 1–2 seconds) — did they stop instantly?
2. **Retention** (% watched) — did they finish?
3. **Rewatch / loop rate** — did it replay?
4. **Early engagement velocity** — likes, comments, shares in the first few minutes

Priority order: `Hook → Retention → Rewatch → Engagement`. Fail at step 1 or 2 and nothing downstream matters.

### FPS Clips: What Works

FPS content has a natural advantage — built-in excitement spikes.

**High-performing:** clutch moments (1v3, 1v5), fast kill streaks, "what just happened" moments, high-skill flicks/snipes.

**Low-performing:** slow buildups, context-heavy clips, long rotations, delayed payoff.

If the viewer doesn't understand the hype within 2 seconds, the clip fails.

### The Hook Problem

Front-load the moment:

- Bad: `setup → buildup → kill`
- Good: `kill → replay → continuation`

Techniques: start at the kill frame, add an instant subtitle ("1v4 clutch"), zoom on the first frame, audio spike immediately. The pipeline should enforce a hook within the first 0–1.5 seconds.

### Retention Engineering

Eliminate: dead air, static frames, slow pacing, silence, confusion.

Template rules:
- clip length: 6–20 seconds
- remove downtime between events
- slight zoom during action
- subtitles tightly synced

### Loop Optimization

Platforms reward loops. Design endings that connect back to the beginning — a replay that cuts to the clip's opening frame increases rewatch rate automatically without any extra effort.

### Scaling With Filters and Templates

**Filters** decide what gets posted: duration (5–30s), audio spike threshold, kill/event detection score. Only the top X% move forward.

**Template structure** (example FPS):

```text
0.0s   → action frame (hook)
0.2s   → zoom + subtitle
0–6s   → main action
6–10s  → replay or continuation
end    → loop transition
```

Together, filters and templates remove decision fatigue, enforce consistency, and align output with algorithm expectations.

### Posting as Experimentation

Each post is a data point, not a piece of content. Track: retention %, watch time, shares, follows per video.

Optimization loop: `Post → Measure → Adjust template → Repeat`

Multi-account strategy: separate game, style, and template per account so performance data is clean and comparable.

### Early-Stage Boost

Post when the target audience is active. Pin a comment; reply to 1–2 early comments. Hashtags and captions alone don't drive distribution — retention does.

### Automation Model

System handles: auto-select (filters), auto-apply template, auto-post, log metrics.

Human handles: template refinement, reviewing top performers, adjusting filter rules.

The long-term edge isn't posting more or fancier editing — it's **consistently passing the initial distribution test**.

---

## Clip Detection — What It Actually Means and How to Build It

Great clips are necessary but not sufficient. If detection is poor, nothing else matters. If detection is strong but hooks and editing are weak, clips still die in the first test batch. Detection needs to be paired with a template that guarantees a strong first 1–2 seconds.

### What "Optimized Detection" Actually Means

Not just "find kills." The real goal:

```text
High-signal moment detection
+ correct timing (start/end)
+ consistency across games
+ low false positives
+ low cost per clip
```

The objective: maximize the % of clips that pass the algorithm's first-stage distribution test.

### Paid / Subscription Tools

These are AI highlight generators, not raw detectors. They train models on highlight datasets and combine visual cues, audio spikes, and engagement patterns with continuous user-feedback refinement.

- **Eklipse.gg** — gaming-focused; detects stream highlights, auto-clips and exports
- **Sizzle.gg** — FPS/esports-oriented; AI + heuristics for exciting moments
- **Opus Clip** — general (not gaming-specific); targets "viral moments"
- **Medal.tv** — local detection via hotkeys and auto-clipping; less AI-driven

Downside: limited control, black-box behavior, harder to integrate into a custom pipeline.

### Open-Source Stack

No single tool handles everything — you build a stack:

| Tool | Role |
|---|---|
| OpenCV | Kill feed / HUD detection; template and feature matching |
| PySceneDetect | Scene change detection; useful for cuts/transitions |
| librosa / pydub | Audio analysis; volume and intensity spikes |
| OpenAI Whisper | Speech keyword detection ("clutch", "oh my god", reactions) |
| FFmpeg | Frame extraction, clip segmentation, preprocessing |

### How Multi-Signal Detection Works

Most systems are pipelines, not single models:

```text
Video
  ↓
Preprocess (normalize resolution + audio)
  ↓
Feature extraction (visual + audio + speech)
  ↓
Scoring engine
  ↓
Clip selection
```

Common scoring rules:

```text
score = (kills × 2) + (headshots × 3) + (audio_spike × 5) + (event_density × 2)
if score > threshold → keep clip
```

Context modifiers: late-game scenarios and clutch situations get score multipliers.

### Must-Have Features for a Custom Detector

1. **Preprocessing** — normalize resolution and audio levels before any analysis
2. **Multi-signal detection** — visual + audio + speech running in parallel
3. **Configurable scoring engine** — weighted rules with adjustable thresholds per game
4. **Clip boundary logic** — define start/end windows carefully; avoid cutting too early/late
5. **Deduplication** — skip already-processed clips
6. **Logging and metrics** — track scores, pass/fail rates, false positive rate

Example config shape:

```yaml
game: deadlock
kill_weight: 2
audio_threshold_db: -10
min_score: 8
```

### How Filters and Templates Help Detection

Filters simplify detection by removing low-quality input early (duration, resolution, audio threshold), reducing the search space, and stabilizing outputs. Templates help indirectly: if every clip starts near the action with consistent pacing, the scoring model is easier to tune and less likely to misclassify.

### Iterative Optimization

Detection improves through feedback:

```text
Detect → Post → Measure → Adjust weights → Repeat
```

High-retention clips → increase weight on their contributing signals. Low-retention clips → decrease. Over time the scoring model aligns with what the algorithm rewards, not just what looks exciting in gameplay.

### The Key Insight

Detection isn't about finding cool moments. It's about **finding moments that perform well in the distribution algorithm**. Most people optimize for gameplay excitement; the target should be viewer retention behavior.

---

## FFmpeg Beyond Cutting — Advanced Filter Techniques

The real power of FFmpeg is its filtergraph (`-filter_complex`), which treats video frames as raw data manipulable like a professional NLE. For FPS clips the goal is **information density**: removing dead space while highlighting mechanical skill and reaction time.

### High-Level Effects

- **Dynamic remapping (`remap`)** — pixel-map warping for lens correction or fish-eye POV; makes fast movement feel even faster
- **Motion interpolation (`minterpolate`)** — generates synthetic frames to smooth 30 FPS clips: `minterpolate=fps=60:mi_mode=mci:mc_mode=aobmc`
- **LUT color grading (`lut3d`)** — apply professional `.cube` LUTs instead of simple brightness/contrast adjustments
- **Audio ducking (`sidechaincompress`)** — automatically lower game audio when a kill SFX or streamer voice triggers

### Effects That Look Complex But Aren't

- **Dynamic zoom / Ken Burns (`zoompan`)** — programmatic zoom centered on a HUD coordinate at the moment a shot fires; use the `d` (duration) and `x`/`y` expressions to target the kill feed
- **Chroma key HUD isolation (`colorkey`)** — turn specific RGB values transparent to isolate and reposition UI elements like the kill feed or health bar
- **Progress bar overlay (`drawbox`)** — duration-tracking bar using the `t` variable to drive box width; no extra assets needed

### FPS Short-Form Power Combinations

The three filters most used by top FPS TikTok/Shorts channels:

1. **Vertical blur-stack (9:16 crop)** — duplicate the 16:9 clip, blur the background copy, center the original on top:
   ```
   [0:v]split[bg][fg];[bg]scale=384:684,boxblur=20:10[bgout];[fg]scale=384:-1[fgout];[bgout][fgout]overlay=(W-w)/2:(H-h)/2
   ```
2. **Kill-zoom** — 1.1× zoom triggered for ~0.5 seconds on a headshot; creates visual impact that syncs with game audio
3. **Crosshair / hitmarker overlay** — `overlay` filter with a custom high-contrast hitmarker PNG, making action legible on small mobile screens

### FPS Pipeline Cheat Sheet

| Function | Flag / Filter | Use Case |
|---|---|---|
| Fast seek / cut | `-ss [time] -to [time]` | Cut VODs without re-encoding |
| Silence detection | `silencedetect` | Find dead air; auto-trim rotations |
| Metadata injection | `-metadata title="Clutch"` | Platform SEO tags before upload |
| Timestamp burn-in | `drawtext=text='%{pts\:hms}'` | Burn timestamps for reviewing gold sets |
| Hardware acceleration | `-hwaccel nvdec` | GPU decoding; ~10× faster than CPU |

### Companion Tools

| Tool | Role |
|---|---|
| OpenCV | Locates kill-feed pixels and hitmarkers; tells FFmpeg where to cut |
| yt-dlp | Pulls high-quality VODs from Twitch/YouTube |
| SoX | Advanced audio spike detection; finds the exact frame a weapon fires |
| ImageMagick | Generates dynamic text overlay PNGs ("Clutch", "Top 5") before FFmpeg layers them |
| Whisper | Generates `.srt` subtitle files that FFmpeg burns in via the `subtitles` filter |

---

## Managing FFmpeg at Scale — Library and Orchestration Patterns

### Filter vs. Filtergraph vs. filter_complex

- **Filter** — a single atomic operation (`scale`, `boxblur`, `transpose`); one input, one output
- **Simple filtergraph** (`-vf` / `-af`) — a chain of filters; one input, one output. Example: `scale=1280:-1,format=yuv420p`
- **filter_complex** — handles multiple inputs and multiple outputs; required for overlays, kill-zooms that split a stream into branches and merge them back, side-by-side compositions

### Community and "Marketplace"

FFmpeg has no centralized template store. Logic is shared as snippets:

- **GitHub Gists** — search `#ffmpeg-snippets`, `#ffmpeg-filter-complex`
- **Stack Overflow / SuperUser** — where complex filter logic is effectively traded
- **Reddit r/ffmpeg** — primary community hub
- **Doom9 Forums** — high-level video encoding veterans
- **VideoHelp Forums** — "how do I achieve this look" threads

Paid options exist at the abstraction layer, not the command level. Services like Shotstack and Creatomate are cloud APIs that wrap FFmpeg filtergraphs — you pay for the engine, not the command string.

### How Pros Manage Their Filter Library

Rather than storing commands in a document, experienced editors build modular components:

- **YAML/JSON config files** — store filter *parameters*; Python injects them into command strings at runtime (this is what `config.yaml` already does for templates)
- **HUD library** — normalized ROI coordinates per game (Warzone, Deadlock, Valorant) that define kill-feed position; updated when a game patches its HUD
- **Python/Node wrappers** — small functions like `apply_vertical_blur(input_file)` that abstract the raw FFmpeg string
- **Snippet managers** — Raycast, Alfred, or VS Code snippets for quick recall of standard commands (9:16 crop, loudnorm, etc.)
- **Docker images** — ensure custom-compiled filter builds (frei0r, vapoursynth) work consistently across machines

### The Orchestrator Pattern

At scale you don't run FFmpeg directly — you dispatch jobs:

1. **Selection** — a job manager picks a clip and reads its `game` tag
2. **Assembly** — pulls the corresponding `filter_complex` snippet from the library
3. **Variable injection** — replaces placeholders (`{{STRICTNESS}}`, `{{CROP_X}}`) with live values from config or the Market Density Monitor
4. **Execution** — the assembled command is dispatched to a worker

This is the pattern that makes templates reusable and the pipeline independent of any specific clip or game.

---

## ROI Standardization and Modular Filtergraphs

Treating video as a grid of data rather than an image is what makes clip processing scalable across games. Two systems enable this: a HUD coordinate library and a modular filtergraph template set.

### HUD Coordinate Library

In FPS games, HUD elements occupy fixed screen positions. Rather than hardcoding pixel coordinates in scripts, store them in config keyed by game and resolution:

```yaml
games:
  deadlock:
    resolution: 1080p
    roi:
      kill_feed:   [1450, 50, 450, 300]   # [x, y, w, h]
      ammo_count:  [1700, 950, 150, 80]
      weapon_icon: [1600, 950, 100, 80]
  marvel_rivals:
    resolution: 1080p
    roi:
      kill_feed:   [1500, 20, 400, 250]
```

This lets OpenCV and FFmpeg scripts read coordinates from config rather than being re-measured per game. When a game patches its HUD, one config update fixes every downstream script.

### Five Core Filtergraph Modules

A modular template is a collection of chainable functions, not a monolithic command string:

1. **Vertical blur-stack** — fits 16:9 gameplay into a 9:16 frame by blurring a scaled background copy
2. **Kill-zoom punch** — brief 1.2× scale-up centered on the crosshair during an event
3. **Dynamic subtitle burner** — takes an `.srt` file and applies a game-specific font/color style
4. **HUD overlay** — crops the kill-feed ROI and repositions it to screen center for mobile legibility
5. **Loop transition** — crossfade or wipe that blends the last 0.5s of a clip with the first 0.5s

### Weapon Detection via HUD Template Matching

Don't detect the weapon in the 3D world (hard — it moves). Detect the **weapon icon in the HUD** (easy — it's static).

Flow:
1. FFmpeg extracts a single frame from the middle of the clip
2. OpenCV crops to the `weapon_icon` ROI from the coordinate library
3. `matchTemplate` compares the crop against a reference PNG library
4. If confidence > 80% → returns `WEAPON_TYPE: SNIPER`
5. Job manager uses that tag to pull from a title list and injects it into upload metadata

Tools by approach:

| Approach | Tool | Notes |
|---|---|---|
| Static HUD icon | OpenCV `matchTemplate` | Standard; works for fixed UI sprites |
| In-world weapon (3D) | YOLO object detection | Requires training on FPS images; significantly more complex |
| Text-based weapon name | Tesseract OCR | Reads text directly from HUD (CoD, Apex); no reference image needed |

---

## Modular Title Engine

The core idea: OpenCV reads the HUD to identify which gun or hero the player is using, then injects that name directly into the post title. A generic template becomes a specific, searchable, real claim about the clip.

**Example:**
1. Template library holds: `"The {weapon} is absolutely BROKEN right now"`
2. OpenCV crops the weapon-icon ROI and runs `matchTemplate` → identifies "Sniper Rifle"
3. Engine substitutes: `"The Sniper Rifle is absolutely BROKEN right now"`

Without the CV detection step, you're left posting generic titles ("This gun is broken") that are low-SEO and indistinguishable from every other gaming channel. With it, every clip auto-generates a title that names the exact weapon or hero — which is what viewers search for.

### How the Detection → Injection Pipeline Works

1. **OpenCV layer** — during clip processing, the Weapon Detector crops the HUD weapon-icon ROI and runs template matching against a library of 64×64 PNG reference images. The best match above a confidence threshold is written to `meta.json` as `weapon_detection.weapon_id` and `weapon_detection.display_name`.

2. **Context layer** — the Title Engine reads `meta.json` after all CV stages complete. It has access to: detected weapon/hero name, kill count, headshot count, sweat score.

3. **Selector layer** — maps CV signals to a title category:
   - headshots + multi-kill, or `sweat_score > 50` → `aggressive`
   - fallback → alternates `educational` ↔ `engagement_bait`

4. **Substitution layer** — picks a template from the selected category and fills `{weapon}` (and any other variables) using the CV-detected data:
   ```
   "Why every pro is switching to the {weapon}"
   → "Why every pro is switching to the Rail Gun"
   ```

### Template Categories

```yaml
categories:
  aggressive:
    - "The {weapon} is actually UNFAIR right now"
    - "Nobody talks about how strong {weapon} is"
    - "{kill_count} kills with {weapon} — this is insane"
  educational:
    - "Why every pro is switching to the {weapon}"
    - "The secret to mastering {weapon} this patch"
  engagement_bait:
    - "Is the {weapon} getting nerfed soon?"
    - "I can't believe this worked with {weapon}"
```

The same `{weapon}` placeholder also populates hashtags (`#SniperRifle`, `#Deadlock`) and the video description automatically — one CV read, multiple surfaces filled.

### LLM Hybrid (Optional Upgrade)

When template substitution isn't varied enough, pass the structured CV data to Claude instead of doing string interpolation:

- **Input:** `{"weapon": "Rail Gun", "kill_count": 4, "event": "headshot_streak"}`
- **Prompt:** "Write 3 viral short-form titles under 50 characters using these facts."
- **Why CV first:** The LLM is grounded in real detected data, so it can't hallucinate ("calling a sniper clip an SMG clip"). CV provides accuracy; LLM provides phrasing variety.

### What You Need to Build the Icon Library

- Save a 64×64 PNG crop of each weapon/hero icon from an actual game frame
- Place under `assets/weapon_icons/{game}/{weapon_id}.png`
- Add `weapon_id: "Display Name"` to `config.yaml → weapon_detector.games.{game}.weapons`
- The Weapon Detector handles the rest at runtime

### Preventing Repetition

- Title history is tracked per game in `assets/title_history.json` (last N templates used)
- The selector skips recently used templates before picking, so adjacent posts don't share the same hook
- 50+ weapon variables × 20+ hooks = 1,000+ combinations before any LLM involvement

---

## Proxy Signal Detection — Cheap Alternatives to Frame-by-Frame Analysis

Processing a full-resolution VOD frame-by-frame to find clips is computationally expensive. Professional systems avoid this by using proxy signals — data sources that are orders of magnitude cheaper than video but highly correlated with hype and action.

### 1. Chat Velocity (IRC Log Analysis)

Twitch chat messages are plain text. Processing 8 hours of chat takes milliseconds; processing 8 hours of video takes hours.

- Pull the IRC chat log for a VOD via the Twitch API
- Track messages per second; identify spikes, especially keywords: "LUL", "OMG", "???", "CLIP IT", "POG"
- Use spike timestamps as bookmarks — pass only those 30-second windows to OpenCV rather than the full VOD
- A sudden message-rate spike is a ~90% indicator that something clip-worthy occurred

### 2. Audio Peak Detection

Audio data is significantly lighter than video. Extract audio with FFmpeg `astats` or `showwaves`, measure dB levels, and flag windows where the peak exceeds a threshold. Silent or flat audio means no action. Loud spikes (gunshots, explosions, streamer shouting) correlate directly with highlight moments.

```bash
ffmpeg -i vod.mp4 -af "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" -f null -
```

### 3. API Markers and User-Generated Clips

- **Stream markers** — many streamers press a physical "marker" button when something notable happens; pull these via `GET /helix/streams/markers` with the VOD ID
- **Viewer clips** — if viewers manually created clips during the stream, those timestamps are public; a cluster of manual clips at the same time stamp is a strong signal without any video analysis at all

### 4. Low-Resolution Pre-Scanning

When video analysis is necessary (e.g., kill-feed or weapon icon detection), downscale the VOD to 360p or 144p for the detection pass. OpenCV matchTemplate works equally well on low-res frames for HUD icons and UI elements, at roughly 1/10th the processing cost. Only upscale to 1080p for the final clip extraction.

### 5. Frame Skipping (Temporal Sampling)

Sample 1 frame every 2 seconds instead of every frame. For a 30-second clip: 1,800 frames vs. 15 frames. Once the weapon icon or kill-feed event is confirmed in a sampled frame, run high-detail analysis only on that specific window.

### The Funnel Approach

```text
1. Chat log scan       → identify ~10 hype moments    (cost: near zero)
2. Audio spike check   → confirm energy at those moments (cost: very low)
3. Low-res OpenCV scan → weapon/kill detection at 360p   (cost: low)
4. 1080p clip extract  → FFmpeg cut only the winners     (cost: moderate)
```

This funnel reduces computational cost by ~95% compared to scanning the full VOD at native resolution.

---

## Montage Assembly — Multi-Clip Concatenation

Stringing approved clips into a short montage creates a second content format from the same asset pool with minimal extra work. A 4-clip montage at 10–15 seconds per clip fits comfortably in a 45–60 second Short or TikTok.

### Transition Choice: Hard Cut

Hard cut (no transition) is the industry standard for FPS short-form montages. Crossfades and wipes interrupt the dopamine loop — any visual transition longer than ~2 frames gives the viewer a chance to scroll.

**The one exception that works:** a 1–2 frame white or black flash between clips. Fast enough to maintain pace, explicit enough to signal a new clip is starting. Used as the default in this pipeline.

**The real transition is audio:** cutting on the beat of a background music track matters more than any visual effect. A hard cut that lands on a snare or bass hit feels completely smooth. If a replacement track is configured (`audio.mode: "replace"`), cuts should be beat-aligned where possible.

### Clip Selection

Pull from `accepted/{game}/` — clips that have already passed manual review. Order by `kill_feed.sweat_score` descending (highest action first), fall back to recency if kill_feed data is unavailable.

### Pipeline Integration

Montage assembly runs as a separate trigger (`python run.py --montage`), not as part of the per-clip pipeline. Output goes through the review UI before distribution so the assembled montage can be approved or rejected as a unit.

---

## Audio Event Detection — DSP-Based Kill and Highlight Detection

Whisper is a speech-to-text engine — it tells you what was *said*, not what sounds occurred. To detect kills, headshots, multi-kills, victory/defeat, and game objectives from audio you need Digital Signal Processing (DSP): analysis of the shape of the sound wave itself, not its words.

### Why Audio Before OpenCV

Audio data is orders of magnitude smaller than video. Running audio analysis first means you can discard boring segments before OpenCV ever touches a frame. The cost funnel:

1. **Audio spike detection** — is there any action in this window? (milliseconds, no GPU)
2. **OpenCV kill-feed** — confirm visually at the spike timestamps only
3. **Whisper** — transcribe only confirmed highlights

### The Z-Score Method (No Paid APIs Needed)

Don't just look for "loud" moments — some streamers are always loud. Look for *relative* spikes above the clip's own baseline:

1. Compute rolling RMS (average loudness) over a 60-second window
2. Flag any 1-second window that is ≥ 3 standard deviations above that baseline
3. Apply a bandpass filter to isolate the frequency range of game sounds (kills/headshots live in the 800–4000 Hz "treble" band; explosions live in the 50–500 Hz "bass" band)

### FFmpeg-Only vs Python Audio Libraries

| Approach | What it can do | New dependency |
|---|---|---|
| **FFmpeg `astats` filter** | Per-frame RMS/peak dB log, parseable with Python | None |
| **FFmpeg + numpy/scipy** | Z-score, bandpass filter, full spike detection | numpy (likely already installed) |
| **Librosa** | Onset detection, spectrogram template matching, beat alignment | `librosa` (~50 MB) |

The FFmpeg + numpy path covers kill/headshot/objective spike detection with no new ML dependencies. Librosa adds spectrogram "audio fingerprinting" — recording the exact headshot sound in isolation and cross-correlating against the clip, similar to weapon icon template matching but for audio.

### Audio Fingerprinting (Spectrogram Template Match)

For games with distinctive sounds (e.g. Deadlock soul orb pickup, Marvel Rivals ultimate activation), you can record that sound as a reference WAV, compute its spectrogram, and slide it across the clip's spectrogram looking for a match — the same concept as `cv2.matchTemplate` but in the frequency-time domain. Near 100% precision for known sounds.

### Detected Event Types

- **Kill / headshot** — sharp transient spike in the 800–4000 Hz band
- **Multi-kill / ace** — cluster of spikes within a short window (≤ 5 seconds apart)
- **Victory / defeat** — sustained loudness change with low-frequency content (music swell, crowd cheer)
- **Objective capture** — game-specific audio cue; requires fingerprint reference clip

### Pipeline Position

Runs after ingestion, before kill-feed OpenCV — feeding spike timestamps into kill-feed so OpenCV only samples frames at audio-confirmed action windows instead of the full clip.

---

## Audio-First VOD Mining — Industry Pattern

Audio as a primary filter for highlight discovery is a proven production pattern used by high-end clipping platforms (Insights.gg, Tencent Game Video Analyzer) to keep server costs viable at scale.

**Core principle:** Video decoding is GPU-heavy and slow. Audio scanning is CPU-only and fast. Scanning an hour of audio for volume/frequency spikes takes seconds; scanning the same hour of video takes minutes of intense work. By running audio first you can discard 90–95% of a VOD — the "walking simulator" segments — before a single video frame is decoded.

### The Funnel

| Stage | Signal | Cost | What it discards |
|---|---|---|---|
| 1 | Audio spike detection | Lowest | Boring stretches — no action sounds |
| 2 | OCR / kill-feed visual check | Medium | False audio positives (jump scares, BGM) |
| 3 | AI scoring | Highest | Clips that are visually interesting but not viral-quality |

### Acoustic Signatures Worth Detecting

- **Volume spike** — sudden loudness vs. rolling baseline (already implemented)
- **Pitch shift** — caster/streamer excitement; rising pitch = escalating action
- **MFCCs** (Mel-Frequency Cepstral Coefficients) — frequency fingerprint that distinguishes a menu click from a sniper shot at the waveform level; used by Librosa and most professional audio classifiers

### Known Limitations

- **Streamer scream problem** — audio spikes can be a jump scare or dropped coffee, not a kill; visual verification is still the final authority
- **BGM interference** — loud background music buries mechanical game sounds; bandpass filtering (800–4000 Hz) partially solves this
- **Silent clutches** — some of the best moments (1v5 ninja defuse, quiet sniper play) have no audio signal at all; audio-first would miss them without a visual fallback

### Future Application: Full VOD Ingestion

The current pipeline ingests pre-clipped Twitch clips (already 30–60s). The full payoff of audio-first comes when ingesting full VODs — extract an 8kHz mono audio proxy (~1/100th the file size), spike-detect the whole stream in seconds, download only the ±10s windows around hits. Allows mining 10× more streamers for the same bandwidth budget.

>>>>>>> origin/main
