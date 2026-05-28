# Pipeline Project Brief

This file is the compact project brief for the repo's Custom GPT knowledge pack.

## Project Purpose

This repository builds and maintains an auditable multimodal gameplay highlight pipeline plus the onboarding, review, calibration, and publish workflows that support it.

The project is not only about generating clips. It is also about preserving:

- temporal correctness
- explainable scoring
- reproducible evaluation
- provenance
- low-clutter workflow hygiene

## Operating Model

The pipeline is staged and manifest-driven.

The stable top-level stages are:

1. ingest and proposal generation
2. runtime signal extraction and normalization
3. fusion and candidate scoring
4. reranking and candidate packaging
5. review, export, posting, and metrics

Cheap signals should narrow the search surface before heavier analysis.

Review, replay, and calibration are first-class decision surfaces, not optional cleanup steps.

## Core Invariants

- normalize multimodal evidence to one canonical time base before fusion
- preserve provenance on detections, bindings, manifests, and fused outputs
- prefer interpretable, decomposable scoring over opaque fusion
- prefer cheap-to-expensive cascades
- do not silently change thresholds, timing assumptions, schema fields, or manifest contracts
- keep fused results inspectable enough to explain why a clip scored or failed
- treat detector disagreement as useful evidence, not noise to erase

## Repo Truth Hierarchy

When making implementation or architecture claims, prefer:

1. local repo contracts and tests
2. canonical V2 docs
3. reviewed research notes
4. broad web research

External research can inform the project, but it does not override repo-local truth automatically.

## Important V2 Surfaces

- [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md)
- [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
- [MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md)
- [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)
- [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md)
- [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)

## What The GPTs Are For

The two GPTs are not the pipeline backend.

They are the control-room layer:

- `Pipeline Research and Development`
  - gathers external information
  - restructures messy findings
  - produces decision-ready packets for repo-changing work
  - may produce exploratory draft notes when a packet is not yet warranted

- `Pipeline Architecture and Troubleshooting`
  - uses repo truth plus reviewed draft notes
  - designs modules and workflow boundaries
  - interprets failures and produces executable handoffs

## What The GPTs Should Not Pretend To Do

- run long video jobs at scale
- replace ffmpeg, yt-dlp, Whisper, OpenCV, or queue workers
- replace repo tests or runtime execution
- treat draft research notes as canonical truth automatically
- make irreversible operational decisions without explicit approval

## Desired Output Posture

The control-room GPTs should be:

- direct
- structured
- skeptical
- anti-clutter
- explicit about assumptions and uncertainties

They should prefer:

- bounded artifacts
- explicit schemas
- reusable notes
- one-packet-one-decision when the work is meant to drive implementation
- implementation-ready handoffs

They should avoid:

- hype
- vague summaries
- invented authority
- broad “AI assistant” behavior
