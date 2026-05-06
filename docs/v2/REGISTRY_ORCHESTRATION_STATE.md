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

## Current Artifact Chain

The current operational chain should be treated as explicit and queryable:

- fused candidate lifecycle
- workflow run manifest
- highlight-selection manifest
- hook candidate manifest
- highlight export batch
- posted highlight ledger
- posted metrics snapshot

The registry is responsible for indexing and joining that chain. The artifacts remain the detailed evidence source of truth.

## Query Semantics

Current registry query behavior is intentionally split into two shapes:

- row-oriented modes return concrete artifact or lineage rows
- aggregate modes return summarized analytics payloads

The main examples are:

- `candidate-lifecycles`, `workflow-runs`, `highlight-exports`, `post-ledger-records`, and `posted-metrics` are row-oriented
- `posted-performance-rollups` is aggregate

Operators should treat workflow queues as lifecycle-first views:

- current lifecycle state decides what needs action next
- older workflow manifests provide batch provenance, not queue ownership
- repeated refresh should preserve idempotent state rather than re-derive action from historical runs

## What Belongs Elsewhere

- Review semantics belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md).
- Distribution-side post records belong in [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md).
- Source-enrichment candidate scoring belongs in [SOURCE_ENRICHMENT_IDENTITY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/SOURCE_ENRICHMENT_IDENTITY.md).
