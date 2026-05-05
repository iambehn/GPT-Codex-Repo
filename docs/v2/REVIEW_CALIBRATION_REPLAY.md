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
- hook evaluation remains advisory in V1 even when its comparisons are queryable and registry-backed
- non-trivial detector, fusion, schema, and publish-workflow changes should pass through review-backed validation before promotion

Measured outcomes belong in experiment ledgers, not here.

This doc should stay focused on:

- what comparison and replay are for
- what artifacts they consume
- what recommendation states mean
- how review outcomes feed later decisions

## Release-Gate Role

Replay, calibration, and review-backed comparison are V2 release gates, not just debugging helpers.

That means:

- regressions should be surfaced before publish or promotion
- recommendation artifacts should stay stable enough to compare baseline vs trial behavior
- reviewer approvals and rejections should remain reusable evidence for later tuning
- compact operator reporting should still expose enough state to debug a blocked release

## Hook Comparison Integration

Hook evaluation now sits inside the same review-backed comparison loop as other fixture and trial work.

Relevant artifacts:

- `hook_candidate_comparison_v1`
- `hook_evaluation_report_v1`

The comparison flow is:

1. compare baseline and trial hook sidecars on the same fixture manifest
2. summarize recommendation state from matched rows
3. join registry-backed approved/exported hook rollups
4. preserve disagreement cases where fused quality and hook quality diverge

This is intentionally not promotion logic. The V1 report is for:

- measuring whether a hook trial improved editorial quality
- exposing which hook modes and archetypes are actually surviving selection and export
- showing whether disagreement patterns are strong enough to justify future gate review

## What Belongs Elsewhere

- Registry design belongs in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Learned-model adoption evidence belongs in experiment records and ADRs.
- Hook-specific packaging rules belong in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
