# Niceshot AI + YOLOv8 Blueprint

This blueprint extends your existing gaming clip pipeline with a configurable "worthy clip" intelligence layer for FPS and hero shooter games.

The goal is to make clip judgment:

1. Repeatable
2. Per-game configurable
3. Cheap to run for easy cases
4. Escalatable when confidence is low
5. Safe when the detector cannot identify core context

---

## Design Intent

Your current plan already has:

- ingestion
- metadata extraction
- scoring
- quarantine / DLQ thinking
- OpenCV ROI mapping
- config-driven game support

This feature should fit into that style instead of becoming a separate mini-app.

Recommended insertion point:

```text
Ingestion
  -> Base Probe
  -> Context Detection
  -> Niceshot/YOLO Feature Extraction
  -> Composite Worthiness Score
  -> Decision
       -> promote to processing
       -> reject
       -> quarantine for enrichment
```

---

## Core Principle

Do not ask Niceshot AI to do everything alone.

Use a layered decision stack:

1. Static per-game knowledge files define what can exist
2. YOLOv8 detects known visual entities
3. Lightweight ROI/snipping rules detect HUD icons and medals
4. Audio and transcript cues add context
5. Niceshot contributes action-quality signals
6. A composite scorer decides `accept`, `reject`, or `quarantine`

That keeps the system explainable and tunable.

---

## Proposed Module Layout

```text
pipeline/
  intelligence/
    __init__.py
    registry.py
    detectors/
      niceshot_adapter.py
      yolo_adapter.py
      roi_matcher.py
      audio_features.py
      transcript_features.py
    scoring/
      composite.py
      thresholds.py
      explain.py
    quarantine/
      router.py
      enrich.py
      reasons.py
    schemas/
      game_schema.py
      manifest_schema.py
assets/
  games/
    marvel_rivals/
      game.yaml
      characters.yaml
      abilities.yaml
      action_moments.yaml
      roi_profiles.yaml
      labels.yaml
      score_weights.yaml
      examples/
        roi_reference_frames/
        positive_clips/
        negative_clips/
  roi_library/
    marvel_rivals/
      kill_medal_headshot.png
      ult_ready_icon.png
      ace_banner.png
models/
  yolo/
    marvel_rivals/
      dataset.yaml
      labels.txt
      weights/
quarantine/
  unresolved_context/
  missing_entities/
  low_confidence/
  roi_required/
```

---

## The Files Niceshot and Your Pipeline Should Look For

If Niceshot has hard requirements in its own package, adapt names as needed, but conceptually you want these files per game:

### 1. `game.yaml`
Defines the game and detector strategy.

```yaml
game_id: marvel_rivals
display_name: "Marvel Rivals"
genre: hero_shooter
camera_mode: first_person
ui_version: "2026-04-launch"
detectors:
  niceshot:
    enabled: true
    profile: cod_like_default
  yolo:
    enabled: true
    model_path: models/yolo/marvel_rivals/weights/best.pt
  roi_matcher:
    enabled: true
resolution_profiles:
  base_capture_height: 1080
  normalize_to: 1920x1080
```

### 2. `characters.yaml`
Canonical list of playable characters or operators.

```yaml
characters:
  - id: punisher
    display_name: "The Punisher"
    aliases: ["punisher"]
    role: duelist
  - id: groot
    display_name: "Groot"
    aliases: ["groot"]
    role: vanguard
```

### 3. `abilities.yaml`
Special skills, ultimates, gadgets, and signature effects.

```yaml
abilities:
  - id: punisher_ult
    character_id: punisher
    display_name: "Final Judgment"
    aliases: ["ult", "ultimate", "final judgment"]
    class: ultimate
    worthiness_boost: 0.18
  - id: groot_wall
    character_id: groot
    display_name: "Thornlash Wall"
    class: utility
    worthiness_boost: 0.05
```

### 4. `action_moments.yaml`
The high-value events worth looking for.

```yaml
moments:
  - id: team_wipe
    category: combat
    description: "Multiple eliminations in a short window"
    evidence:
      min_kills_in_window: 3
      window_seconds: 5
      audio_peak_required: false
    score_weight: 0.22
  - id: clutch_win
    category: outcome
    description: "Round-saving or low-odds conversion"
    evidence:
      transcript_keywords: ["clutch", "no way", "1v", "last alive"]
    score_weight: 0.20
  - id: ultimate_swing
    category: ability
    description: "Ultimate creates a decisive fight swing"
    evidence:
      required_ability_class: ultimate
    score_weight: 0.17
```

### 5. `roi_profiles.yaml`
Basic HUD regions and snipping targets.

```yaml
rois:
  kill_feed:
    anchor: top_right
    x_pct: 0.74
    y_pct: 0.05
    w_pct: 0.22
    h_pct: 0.22
  medal_area:
    anchor: center
    x_pct: 0.40
    y_pct: 0.18
    w_pct: 0.20
    h_pct: 0.18
  ability_hud:
    anchor: bottom_center
    x_pct: 0.32
    y_pct: 0.80
    w_pct: 0.36
    h_pct: 0.16
templates:
  - id: kill_medal_headshot
    roi: medal_area
    image: assets/roi_library/marvel_rivals/kill_medal_headshot.png
    match_threshold: 0.86
  - id: ult_ready_icon
    roi: ability_hud
    image: assets/roi_library/marvel_rivals/ult_ready_icon.png
    match_threshold: 0.83
```

### 6. `labels.yaml`
Maps raw detector outputs to canonical entities.

```yaml
labels:
  yolo:
    hero_punisher: punisher
    hero_groot: groot
    medal_headshot: headshot_medal
  niceshot:
    killstreak: streak_event
    flick: aim_skill
    explosive_entry: engagement_spike
```

### 7. `score_weights.yaml`
The composite score formula.

```yaml
weights:
  niceshot_action_score: 0.24
  yolo_event_score: 0.18
  roi_icon_score: 0.12
  audio_hype_score: 0.10
  transcript_hype_score: 0.08
  motion_energy_score: 0.10
  context_certainty_score: 0.08
  rarity_score: 0.10
thresholds:
  accept: 0.72
  quarantine: 0.48
  reject_below: 0.48
gates:
  require_context_fields:
    - match_type
    - player_character
  quarantine_if_missing:
    - player_character
    - detected_event
```

---

## How To Create These Files Repeatably

Build a "game onboarding" workflow. Every new game should go through the same five passes.

### Pass 1: Canonical Knowledge Pack

Create the structured source-of-truth files:

- `game.yaml`
- `characters.yaml`
- `abilities.yaml`
- `action_moments.yaml`

Populate these from:

- official game site
- community wiki
- patch notes
- hero/weapon guide pages
- your own manual review notes

Important rule:
Do not store freeform names only. Always create canonical IDs plus aliases.

Good:

```yaml
id: soldier_76
display_name: "Soldier: 76"
aliases: ["soldier", "76", "soldier 76"]
```

That gives you stable joins across transcript parsing, YOLO labels, and ROI detections.

### Pass 2: ROI Library Buildout

For each game, capture 10 to 30 clean frames from:

- kill feed visible
- medal visible
- hero/weapon HUD visible
- ability-ready state
- objective / round state

From those frames, create:

- cropped template PNGs
- ROI coordinates
- reference screenshots

This is where your snipping tool idea belongs.

Recommended helper flow:

1. Open a representative frame
2. Snip a target icon or medal
3. Save crop into `assets/roi_library/{game}/`
4. Record the ROI box in `roi_profiles.yaml`
5. Attach a threshold and expected semantic meaning

This should be a small tool, not a manual spreadsheet process.

### Pass 3: YOLO Dataset Assembly

YOLOv8 should not detect "worthiness" directly.
It should detect visual facts that are useful for worthiness.

Good YOLO classes:

- hero portrait
- weapon icon
- skull / elimination indicator
- medal badge
- ultimate ready icon
- objective won banner

Avoid abstract labels like:

- hype
- insane clip
- viral

For each game, produce:

- `models/yolo/{game}/dataset.yaml`
- `models/yolo/{game}/labels.txt`
- annotated images for training

Use a class naming convention:

```text
hero_punisher
hero_groot
medal_headshot
medal_multikill
icon_ult_ready
banner_victory
```

### Pass 4: Composite Score Tuning

Start from Niceshot's default FPS-friendly priors, but do not trust them as your final logic.

Tune by game family:

- tac FPS: reward clutch, precision, low-TTK kill confirmations
- arena shooter: reward streak density and rapid combat chaining
- hero shooter: reward ability combos, team wipes, ult value swings

Each game should have its own `score_weights.yaml`.

### Pass 5: Gold Set Evaluation

Create:

- `examples/positive_clips/`
- `examples/negative_clips/`

Then evaluate:

- precision
- recall
- quarantine rate
- missing-context rate

This lets you tune weights and thresholds with evidence instead of intuition.

---

## Composite Score Blueprint

Recommended score shape:

```text
worthiness_score =
  (niceshot_action_score * 0.24) +
  (yolo_event_score * 0.18) +
  (roi_icon_score * 0.12) +
  (audio_hype_score * 0.10) +
  (transcript_hype_score * 0.08) +
  (motion_energy_score * 0.10) +
  (context_certainty_score * 0.08) +
  (rarity_score * 0.10)
```

### Suggested signals

- `niceshot_action_score`
  - pacing
  - target switching
  - engagement intensity
  - likely highlight behavior

- `yolo_event_score`
  - identified medal classes
  - visible elimination bursts
  - hero/weapon context that matches known highlight patterns

- `roi_icon_score`
  - kill medals
  - ult-ready indicators
  - round win or objective banners

- `audio_hype_score`
  - loudness spike
  - caster/player reaction spike
  - crowd / announcer energy

- `transcript_hype_score`
  - "no way"
  - "clutch"
  - "team wipe"
  - "let's go"

- `context_certainty_score`
  - confidence that we know game, hero, event type, and basic HUD state

- `rarity_score`
  - an ace or 4k is rarer than a single elimination

---

## Quarantine Design

Your quarantine idea is exactly right, but it should be split by reason so enrichment can be targeted.

Recommended quarantine buckets:

```text
quarantine/
  unresolved_context/
  missing_entities/
  low_confidence/
  roi_required/
  ui_drift_suspected/
```

### When to quarantine

Move a clip to quarantine when any of these are true:

- Niceshot returns action confidence but cannot identify enough context
- YOLO sees activity but cannot map to canonical labels
- required core fields are missing
- ROI template matching is inconclusive
- HUD appears shifted after a patch

### Required core fields

At minimum, try to resolve:

- `game_id`
- `player_character` or `weapon`
- `detected_event`
- `combat_window_start`
- `combat_window_end`

If two or more are missing, quarantine.

### Quarantine manifest

Each quarantined clip should get a structured manifest like:

```json
{
  "clip_id": "abc123",
  "game_id": "marvel_rivals",
  "status": "quarantine",
  "quarantine_reason": "missing_entities",
  "missing_fields": ["player_character", "detected_event"],
  "detector_outputs": {
    "niceshot_action_score": 0.81,
    "yolo_labels": [],
    "roi_matches": []
  },
  "next_actions": [
    "run_roi_snip_search",
    "extract_reference_frames",
    "manual_label_if_still_unknown"
  ]
}
```

---

## ROI Snipping Tool Blueprint

This is one of the highest-value additions because it gives you a fast way to recover from detector misses and HUD drift.

### Purpose

Allow the operator to define:

- a region of interest
- a cropped template image
- the semantic meaning of that crop
- the game and UI version it belongs to

### Minimal flow

1. Load a frame from a quarantined clip
2. Drag a rectangle around an icon / medal / HUD element
3. Save the crop as a template PNG
4. Save the ROI coordinates
5. Label it
6. Re-run matching on the clip

### Files it should create

When a user snips a new icon, create:

- `assets/roi_library/{game}/{template_id}.png`
- append entry to `assets/games/{game}/roi_profiles.yaml`
- optional update to `assets/games/{game}/labels.yaml`

### Metadata to save with every snip

```yaml
id: medal_quad_kill
game_id: marvel_rivals
ui_version: 2026-04-launch
semantic_type: medal
maps_to_event: multi_kill
roi: medal_area
match_threshold: 0.84
source_clip_id: abc123
frame_time_seconds: 12.38
```

### Recommended implementation detail

Use the snipping tool as an annotation utility, not just a screenshot saver.

The output should be structured metadata first and image second.

---

## Suggested Per-Clip Manifest Fields

Add these to your `.meta.json` or future SQLite schema:

```json
{
  "context": {
    "game_id": "marvel_rivals",
    "ui_version": "2026-04-launch",
    "player_character": "punisher",
    "detected_event": "team_wipe"
  },
  "detectors": {
    "niceshot": {
      "action_score": 0.83,
      "profile": "cod_like_default"
    },
    "yolo": {
      "labels": [
        {"id": "hero_punisher", "confidence": 0.91},
        {"id": "medal_headshot", "confidence": 0.88}
      ]
    },
    "roi": {
      "matches": [
        {"id": "kill_medal_headshot", "confidence": 0.89}
      ]
    }
  },
  "worthiness": {
    "score": 0.79,
    "decision": "accept",
    "explanation": [
      "High action confidence",
      "Headshot medal matched",
      "Hero identified with strong certainty"
    ]
  }
}
```

This gives you explainability in the review UI.

---

## Recommended Decision Logic

Use a three-way outcome.

```text
if score >= accept_threshold and required_context_resolved:
    accept
elif score < reject_threshold and confidence is reliable:
    reject
else:
    quarantine
```

That prevents weak data from being thrown away just because it was incomplete.

---

## Best Way To Bootstrap New Games

For each new game, follow this checklist:

1. Create `game.yaml`
2. Create canonical roster or weapon list
3. Create ability list
4. Define 5 to 10 high-value action moments
5. Capture 10 to 30 reference frames
6. Build first ROI templates
7. Label first YOLO dataset
8. Tune `score_weights.yaml`
9. Run against a 25 to 50 clip gold set
10. Review quarantined clips and expand ROI library

This is the repeatable loop that keeps onboarding modular.

---

## Recommended Rollout Order

### Phase 1

Implement config-only scaffolding:

- per-game metadata files
- composite score config
- quarantine router
- enriched manifest schema

### Phase 2

Add ROI snipping + template matching:

- frame extractor
- ROI crop saver
- YAML updater
- re-run enrichment on quarantined clips

### Phase 3

Add YOLOv8 support:

- dataset format
- model registry per game
- label mapping to canonical entities

### Phase 4

Integrate Niceshot signals into the same scorer:

- adapter layer
- normalized score output
- game-specific profile overrides

### Phase 5

Build the review feedback loop:

- mark false positives
- mark false negatives
- update weights
- add new ROI templates
- retrain YOLO when needed

---

## Important Guardrails

- Keep all game knowledge outside code in YAML files
- Treat Niceshot as one signal, not the sole decider
- Keep YOLO labels factual and concrete
- Use quarantine as a recovery path, not a failure dump
- Version UI layouts with `ui_version`
- Store explanations for every score decision

---

## Straight Answer To Your Key Question

To create all the files the model will need, build a per-game asset pack containing:

- `game.yaml`
- `characters.yaml` or `weapons.yaml`
- `abilities.yaml`
- `action_moments.yaml`
- `roi_profiles.yaml`
- `labels.yaml`
- `score_weights.yaml`
- ROI template PNGs
- YOLO dataset config and labels
- positive/negative evaluation clips

Then generate those files with a repeatable onboarding workflow:

1. scrape or manually compile the canonical game entities
2. capture HUD reference frames
3. use a snipping tool to save ROI templates
4. annotate YOLO classes for concrete UI/gameplay objects
5. define score weights and thresholds
6. route uncertain clips to quarantine for enrichment

If you want, the next best step is for us to turn this blueprint into actual starter files for one game first, like `Marvel Rivals` or `Arc Raiders`, so the structure is real and reusable instead of just theoretical.
