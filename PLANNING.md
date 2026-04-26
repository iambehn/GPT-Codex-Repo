# Gaming Clip Farming Bot — Planning

> Working-tree snapshot. This document reflects the current active repo surface for solo use.
>
> Source of truth order: code in `run.py` and `pipeline/` first, then this document.

## Current System Overview

The active product is a game-pack-first clip pipeline focused on detector quality, deterministic judging, quarantine recovery, replay debugging, and YOLO asset/model iteration.

Current runtime order:

```text
ingestion
-> audio / kill_feed / weapon_detector / niceshot / yolo
-> hook_enforcer
-> clip_judge
-> transcription
-> feature_extraction
-> template selection
-> processing
-> scoring / title_engine
-> review
```

Current state and storage model:

- Clips move through filesystem stages: `inbox/`, `processing/`, `accepted/`, `rejected/`, `quarantine/`
- Per-clip metadata remains sidecar-first via `.meta.json`
- Game knowledge lives under `assets/games/<game>/`
- Detector icon assets live under `assets/weapon_icons/<game>/`
- YOLO registry and dataset artifacts live under `models/yolo/<game>/`
- Audit, review, and training artifacts stay on disk beside the active game assets

## Commands

Core pipeline:

- `python run.py --game <game>`
- `python run.py --game all`
- `python run.py --watch`

Game-pack lifecycle:

- `python run.py --init-game <game_slug>`
- `python run.py --validate-game-pack <game_slug>`
- `python run.py --evaluate-game-pack <game_slug>`
- `python run.py --build-yolo-dataset <game_slug>`
- `python run.py --train-yolo <game_slug> [--dry-run]`
- `python run.py --enrich-game-from-wiki <game_slug> --wiki-url <url>`
- `python run.py --enrich-quarantine <game_slug>`

Detector and asset iteration:

- `python run.py --refresh-weapon-detector <game_slug> [--weapon-frame-sample middle|kill_timestamps|all]`
- `python run.py --audit-weapon-detector <game_slug>`
- `python run.py --render-weapon-audit-review <game_slug> [--report <path>] [--top-k <n>]`
- `python run.py --promote-weapon-audit-crop <game_slug> [--report <path>] [--rank <n>] [--crop-source auto|candidate|roi] [--overwrite] [--dry-run]`

Other active commands:

- `python run.py --montage <game|all>`

Review app:

- `python -m pipeline.review.app`

Active review surfaces:

- `Queue`
- `Quarantine`
- `Replay` (clip-level debug view linked from Queue and Quarantine)

## Current Subsystems

### Game-Pack System — `Implemented`

- New game support is structured around `assets/games/<game>/`
- Canonical files: `game.yaml`, `entities.yaml`, `moments.yaml`, `hud.yaml`, `weights.yaml`
- Scaffolding, validation, wiki draft enrichment, and gold-set evaluation are wired

### Quarantine Review + ROI Icon Training — `Implemented`

- Quarantined clips are surfaced in the Flask app
- Reviewers can capture a frame, crop a hero/weapon icon, save it to detector assets, and rescan the current clip
- Audit metadata is written back to the clip sidecar
- Clips that recover can move back to `inbox/`

### YOLO Inference + Dataset / Training Path — `Partial`

- Runtime YOLO adapter is in place
- Per-game label mapping and weights paths are driven by game packs
- Dataset export, training harness, and promotion path exist
- Real trained weights and retraining automation are still pending

### NiceShot Adapter Integration — `Partial`

- NiceShot feeds the same judging path as the other detectors
- Current adapter supports `stub` and `fixture_json`
- Profile presets and game-specific overrides exist
- Real NiceShot CLI/API integration is not wired yet

### Hook Enforcer — `Implemented`

- Hook enforcement is an explicit stage before clip acceptance
- Current V1 remediation is hard trim only
- Original source clips are preserved; trim intent is stored in metadata and applied later during processing

### Clip Judge — `Implemented`

- The clip judge is the pre-processing keep/reject/quarantine gate
- It combines detector outputs, hook resolution, context resolution, and deterministic thresholds
- Current outputs include `candidate_moments`, `context`, `decision`, and `quarantine`

### Modular Title Engine — `Implemented`

- Deterministic fact-bundle-driven title assembly is in place
- Active packaging metadata prefers `title_engine.title` and `title_engine.caption`

### Replay Viewer / Visual Debugging — `Implemented`

- Queue and quarantine clips have a replay/debug page
- It shows detector timelines, HUD ROI overlays, YOLO boxes when present, hook alignment, trim intent, and judge explanations
- Raw detector and decision sections are exposed in one debug page

### Weapon Detector Audit + Asset Review — `Implemented`

- The project can rank OpenCV near-misses, export candidate crops from real footage, and render side-by-side review galleries
- Asset promotion is explicit and can be dry-run or backed up before overwrite

## Current Gaps / Not Yet Production-Ready

- NiceShot real API/CLI access is not integrated
- YOLO training and retraining are not automated
- Real per-game weights are still mostly missing
- Marvel Rivals kill-feed templates are still missing
- OpenCV icon assets still need curation from real gameplay for better pseudo-label growth
- The review app is local-tool safe, not hardened for internet exposure

## Near-Term Roadmap

1. Curate better Marvel Rivals icon assets from replay/audit output
2. Grow real clip-derived YOLO examples and train the first useful Marvel Rivals model
3. Use replay + gold-set evaluation to tighten detector manifests and thresholds before changing weights
4. Upgrade NiceShot from stub/fixture mode to a real adapter once the external contract is confirmed
5. Keep the active surface small; only add support features again when they solve a repeated bottleneck

## Archived / Frozen

These were removed from the active solo-use repo surface and are tracked in [ARCHIVE_FEATURES.md](/Users/tj/GPT-Codex-Repo/ARCHIVE_FEATURES.md):

- Analytics dashboard and import flow
- Distribution queue and compliance scheduler
- Review feedback and performance-feedback loops
- Scout dashboard and trend polling

## Recent Major Additions

- Game-pack-first architecture replaced hardcoded per-game config
- Clip judge became the real worthiness gate before transcription and processing
- Hook enforcement became a first-class stage
- Quarantine gained browser-based ROI/icon training and rescan
- YOLO gained inference scaffolding plus dataset/training/export tooling
- NiceShot gained normalized profile-driven scoring instead of a raw stub-only shape
- Replay viewer added a visual debug surface for queue and quarantine clips
- Weapon-detector audit and asset-review tooling added a cleaner path for real-footage asset correction
