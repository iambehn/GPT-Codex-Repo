# Gaming Clip Farming Bot — Planning

> Working-tree snapshot. This document reflects the current repo state, including implemented but not yet committed features where relevant.
>
> Source of truth order: code in `run.py` and `pipeline/` first, then this document.

## Current System Overview

The project is now a game-pack-first clip pipeline with a deterministic pre-processing judge, a local review app, a distribution queue, and a feedback loop.

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
-> distribution
-> analytics / feedback
```

Current state and storage model:

- Clips move through filesystem stages: `inbox/`, `processing/`, `accepted/`, `rejected/`, `quarantine/`
- Per-clip metadata remains sidecar-first via `.meta.json`
- Game knowledge lives under `assets/games/<game>/`
- Detector icon assets live under `assets/weapon_icons/<game>/`
- YOLO registry and dataset artifacts live under `models/yolo/<game>/`
- Distribution operational state is local SQLite
- Analytics source of truth is still Google Sheets

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
- `python run.py --enrich-game-from-wiki <game_slug> --wiki-url <url>`
- `python run.py --enrich-quarantine <game_slug>`

Distribution:

- `python run.py --schedule-distribution`
- `python run.py --run-distribution-queue`
- `python run.py --distribution-status`
- `python run.py --mark-manual-posted <task_id> --url <url>`
- `python run.py --distribute [--dry-run]`

Feedback and evaluation:

- `python run.py --review-feedback <game_slug>`
- `python run.py --apply-feedback <game_slug> [--dry-run]`

Other existing commands:

- `python run.py --montage <game|all>`
- `python run.py --poll-tiktok`
- `python run.py --list-reddit-flairs`

Review app:

- `python -m pipeline.review.app`

Review UI surfaces:

- `Queue`
- `Quarantine`
- `Feedback`
- `Scout`
- `Analytics`
- `Distribution`

## Current Subsystems

### Game-Pack System — `Implemented`

- New game support is structured around `assets/games/<game>/`
- Canonical files: `game.yaml`, `entities.yaml`, `moments.yaml`, `hud.yaml`, `weights.yaml`
- Scaffolding, validation, wiki draft enrichment, and gold-set evaluation are all wired
- Goal state: a new game should validate and run without core code edits

### Quarantine Review + ROI Icon Training — `Implemented`

- Quarantined clips are surfaced in the Flask app
- Reviewers can capture a frame, crop a hero/weapon icon, save it to detector assets, and rescan the current clip
- Audit metadata is written back to the clip sidecar
- Clips that recover can move back to `inbox/`

### YOLO Inference Scaffold + Dataset Builder — `Partial`

- Runtime YOLO adapter is in place
- Per-game label mapping and weights paths are driven by game packs
- Dataset registry builder exports `dataset.yaml`, `labels.txt`, `label_map.json`, and `seed_manifest.json`
- Current design uses existing icon, ROI, and reference-frame assets as seeds
- Real trained weights and full retraining automation are still pending

### NiceShot Adapter Integration — `Partial`

- NiceShot now feeds the same scoring path as the other detectors
- Current adapter supports `stub` and `fixture_json`
- Profile presets and game-specific overrides exist
- Output is normalized into structured scores and candidate moments
- Real NiceShot CLI/API integration is not wired yet

### Hook Enforcer — `Implemented`

- Hook enforcement is an explicit stage before clip acceptance
- Current V1 remediation is hard trim only
- Original source clips are preserved; trim intent is stored in metadata and applied later during processing

### Clip Judge — `Implemented`

- The clip judge is now the pre-processing decision engine
- It combines detector outputs, hook resolution, AI/deterministic scoring, context resolution, and quarantine routing
- Current outputs include `candidate_moments`, `context`, `detector_outputs`, `decision`, and `quarantine`

### Modular Title Engine — `Implemented`

- Deterministic fact-bundle-driven title assembly is in place
- Current publishing metadata prefers `title_engine.title` and `title_engine.caption`
- It uses detector and judge metadata instead of relying only on the older scoring title

### Analytics Dashboard — `Implemented`

- Analytics has its own Flask tab
- Current model separates posts, metric snapshots, and deterministic performance decisions
- Manual import is the current ingestion path; platform collectors are still future work

### Distribution Queue — `Implemented`

- Distribution is now queue-driven instead of direct-post-first
- Queue states, attempts, compliance records, and manual publish packs are wired
- Official API posting is gated by account policy mode and compliance defaults
- `human_assisted` is the safe default pattern where direct API behavior is not ready

### Review Feedback Loop — `Implemented`

- Reviewers can record false positives, false negatives, ROI-template needs, and retrain requests
- Feedback is stored per game, summarized, and surfaced in a dedicated dashboard
- Bounded weight updates and retrain recommendations can be generated from feedback
- This updates configuration and reporting only; it does not train models automatically

### Replay Viewer / Visual Debugging — `Implemented`

- The review app now includes a clip-level replay viewer for queue and quarantine sources
- V1 shows detector timelines, HUD ROI overlays, YOLO boxes when present, hook alignment, trim intent, and judge explanations
- Raw detector and decision sections are exposed in a single debug page so failures can be inspected without reading sidecar files manually

## Current Gaps / Not Yet Production-Ready

- NiceShot real API/CLI access is not integrated
- YOLO training and retraining are not automated
- Real per-game weights are still mostly missing
- Marvel Rivals kill-feed templates are still missing
- Analytics collection is still manual-import-first
- Distribution adapters are not complete for every platform/account path
- The review app is local-tool safe, not hardened for internet exposure

## Near-Term Roadmap

1. Train the first real Marvel Rivals YOLO model from seeded assets and labeled gold-set data
2. Use the replay viewer to tighten signal manifests, overlay quality, and gold-set debugging before changing weights
3. Upgrade NiceShot from stub/fixture mode to a real adapter once the external contract is confirmed
4. Add a cleaner review/promote flow for wiki drafts and game-pack onboarding artifacts
5. Tighten operator docs, health surfaces, and validation around the full review -> distribution -> analytics loop

## Backlog

Still real, still unbuilt, but no longer mixed with implemented systems:

- YOLO training orchestration and model promotion workflow
- NiceShot production adapter
- Analytics collectors for platform APIs
- Hardened auth/rate-limiting if the review app is ever exposed beyond localhost
- Platform-specific distribution adapters beyond the current queue/compliance scaffolding

## Recent Major Additions

- Game-pack-first architecture replaced hardcoded per-game config
- Clip judge became the real worthiness gate before transcription and processing
- Hook enforcement became a first-class stage
- Quarantine gained browser-based ROI/icon training and rescan
- YOLO gained inference scaffolding plus a dataset/registry builder
- NiceShot gained normalized profile-driven scoring instead of a raw stub-only shape
- Analytics gained its own dashboard and manual import flow
- Distribution moved to a SQLite-backed compliance queue
- Review feedback became a real loop with summaries, bounded weight updates, and retrain recommendations
- Replay viewer added a visual debug surface for queue and quarantine clips
