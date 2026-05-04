# Registry / Orchestration / State

This document is the canonical V2 home for persistent state and workflow sequencing.

## Canonical Scope

Use this doc for:

- queryable artifact-state goals
- candidate and review lifecycle states
- registry boundaries vs sidecar boundaries
- orchestration hardening policy

Do not use this doc for:

- detector scoring details
- editorial hook taxonomy
- downstream analytics interpretation

## Current V2 Position

The repo has moved beyond pure file coordination.

Stable policies:

- sidecars remain the detailed evidence artifacts
- a registry should index and query those artifacts rather than replace them
- candidate state should be explicit
- orchestration should automate clear state transitions, not infer them from directory scanning
- retries, failure recovery, and idempotency belong after state is explicit

Minimum lifecycle direction:

- pending review
- approved
- rejected
- exported
- posted

Registry-first expectations:

- query clips, runs, reviews, comparisons, and artifact lineage without bespoke file scans
- preserve provenance back to source sidecars
- avoid hidden or implicit workflow state

## What Belongs Elsewhere

- Review semantics belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md).
- Distribution-side post records belong in [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md).
- Source-enrichment candidate scoring belongs in [SOURCE_ENRICHMENT_IDENTITY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/SOURCE_ENRICHMENT_IDENTITY.md).
