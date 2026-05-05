# Detection / Runtime / Fusion

This document is the canonical V2 home for upstream highlight-detection logic.

## Canonical Scope

Use this doc for:

- proposal generation policy
- runtime signal extraction and normalization
- fusion and synergy rules
- shortlist and reranking boundaries
- detector expansion policy

Do not use this doc for:

- editorial hook strategy
- post-routing and account analytics
- identity-resolution or leaderboard sourcing

## Current V2 Position

The repo remains proposal-first and shortlist-first.

Stable policies:

- proxy or cheap proposal passes should reduce the candidate surface early
- runtime signals should be normalized before fusion
- fusion should preserve provenance and contributing evidence
- interaction and synergy are valid scoring constructs, but should remain explainable
- reranking stays top-N only
- heavy VLM work should never become a full-video default path

## Shared Signal Contract

V2 should treat normalized signals as one internal contract across proxy, runtime CV, audio, and fused layers.

Minimum expectations:

- one canonical time anchor per signal
- explicit modality and detector identity
- confidence preserved without hidden rescaling
- provenance back to the originating artifact or sidecar
- enough payload to explain why a downstream fused event exists

Fusion hardening rules:

- keep base evidence separate from interaction or confirmation logic
- preserve disagreement cases instead of collapsing them into opaque scores
- prefer cheap-to-expensive gating before broader inference
- do not introduce hidden timing or threshold changes without reviewable evidence

Detector expansion policy:

- add heavier detector families only when replay and reviewed evidence show unresolved recall gaps
- preserve the cheap-signal-first funnel even when heavier detectors are added
- treat model changes as trials with comparison and review evidence, not intuition

## What Belongs Elsewhere

- Hook packaging and opener logic belong in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
- Trial results and calibration outcomes belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) and the experiment ledger.
- Learned ranking adoption decisions belong in ADRs plus experiment records.

## Current Repo Anchors

- current roadmap summary of this layer: [FUTURE_FEATURES_ROADMAP.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md)
- research shortlist: [HIGHLIGHT_DETECTION_RESEARCH_SHORTLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/HIGHLIGHT_DETECTION_RESEARCH_SHORTLIST.md)
