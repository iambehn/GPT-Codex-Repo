# Review / Calibration / Replay

This document is the canonical V2 home for review-backed decision loops.

## Canonical Scope

Use this doc for:

- review surfaces and statuses
- replay and comparison policy
- fixture-driven evaluation workflow
- calibration posture and recommendation rules

Do not use this doc for:

- long-term storage schema design
- platform-posting operations
- source-enrichment ranking logic

## Current V2 Position

The repo already has strong replay and comparison infrastructure. V2 should treat it as a core operating surface.

Stable policies:

- review-backed comparisons outrank intuition
- sidecars are source artifacts for replay and comparison
- fixture-based baseline vs trial workflows should stay deterministic
- recommendation artifacts should remain human-decision aids, not auto-promotion logic
- viewer and review surfaces should make disagreement cases easy to inspect

Measured outcomes belong in experiment ledgers, not here.

This doc should stay focused on:

- what comparison and replay are for
- what artifacts they consume
- what recommendation states mean
- how review outcomes feed later decisions

## What Belongs Elsewhere

- Registry design belongs in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Learned-model adoption evidence belongs in experiment records and ADRs.
- Hook-specific packaging rules belong in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
