# Hook / Editorial Packaging

This document is the canonical V2 home for hook logic and editorial packaging rules.

## Canonical Scope

Use this doc for:

- hook candidate artifact policy
- truthful opener and packaging logic
- natural vs synthetic hook distinctions
- measurable editorial fields and review expectations

Do not use this doc for:

- raw detector semantics
- upstream identity sourcing
- broad post-analytics strategy

## Current V2 Position

Hook logic is now a first-class V2 concern.

Stable policies:

- hook quality should become a measurable artifact, not an informal editing instinct
- event mapping tells us what happened
- hook packaging decides whether the opening makes the event legible, compelling, and truthful
- clips without a natural or defensible synthetic hook should be rejected rather than over-salvaged
- hook-related review and comparison data should stay connected to the same artifact lineage as upstream evidence
- hook artifacts remain advisory in V1; they do not change lifecycle gates by themselves

The intended artifact direction is:

- explicit hook candidate metadata
- hook archetype classification
- packaging strategy fields
- sound-off legibility and authenticity-risk style measurements

## Hook Evaluation V1

The repo now exposes a unified hook evaluation artifact:

- `hook_evaluation_report_v1`

It combines:

- fixture/trial hook comparisons
- approved or export-selected candidate rollups from the registry
- fused-vs-hook disagreement counts
- explicit advisory policy and future gate readiness status

Primary operator entrypoint:

```bash
python3 run.py \
  --report-hook-evaluation /path/to/fixtures.json \
  --baseline-sidecar-root /path/to/baseline \
  --trial-sidecar-root /path/to/trial \
  --registry-path /path/to/registry.sqlite \
  --game <game>
```

Registry-backed query surfaces:

- `hook-evaluation-reports`
- `hook-quality-rollups`

These exist to answer two separate questions:

- did the trial hook strategy improve the reviewed fixtures
- what editorial hook patterns are being approved and exported right now

## What Belongs Elsewhere

- Runtime/fusion scoring mechanics belong in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Calibration and comparison of hook strategies belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) and experiment records.
- Post-level performance interpretation belongs in [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md).
