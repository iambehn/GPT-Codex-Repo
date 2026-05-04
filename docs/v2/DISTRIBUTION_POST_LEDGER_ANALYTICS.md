# Distribution / Post Ledger / Analytics

This document is the canonical V2 home for downstream posting and performance state.

## Canonical Scope

Use this doc for:

- post ledger boundaries
- downstream routing and posting policy
- performance metrics and feedback surfaces
- relationship between approved clips and posts

Do not use this doc for:

- upstream runtime event truth
- source-enrichment matching logic
- raw hook taxonomy design

## Current V2 Position

Distribution is downstream of a stronger candidate pipeline.

Stable policies:

- approved clip and candidate artifacts remain upstream authority
- one clip may map to many posts
- post records need their own lifecycle, metrics, failures, and moderation context
- vendor tooling is acceptable early for publishing and inbox operations
- analytics should feed learning loops without redefining upstream evidence contracts

Minimum downstream concepts:

- post ledger
- post metrics
- comment or moderation state
- experiment linkage
- destination-aware routing once candidate trust is high enough

## What Belongs Elsewhere

- Candidate review state belongs in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Hook packaging logic belongs in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
- Calibration and replay outcomes belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) and the experiment ledger.
