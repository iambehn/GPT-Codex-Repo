# Architecture / Operating Model

This document is the canonical V2 home for the pipeline operating model.

## Canonical Scope

Use this doc for:

- staged pipeline boundaries
- cheap-signal-first compute policy
- artifact roles and handoff points
- separation between detection, scoring, editorial packaging, and downstream operations

Do not use this doc for:

- detailed experiment results
- one-off trial outcomes
- subsystem-specific implementation checklists

## Current V2 Position

The pipeline stays staged and manifest-driven.

Core operating rules:

- cheap global passes happen first
- expensive inference happens only on narrowed candidate sets
- sidecars remain the detailed evidence layer
- review, replay, and calibration are decision surfaces, not afterthoughts
- downstream posting and analytics do not redefine upstream event truth

The core boundary set is:

1. Ingest and proposal generation
2. Runtime signal extraction and normalization
3. Fusion and candidate scoring
4. Reranking and candidate packaging
5. Review, export, posting, and metrics

## What Belongs Elsewhere

- Game-pack and manifest field definitions belong in [MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md).
- Runtime, fusion, and reranking mechanics belong in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Hook packaging logic belongs in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
- Registry and orchestration state rules belong in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).

## Current Repo Anchors

- canonical roadmap: [FUTURE_FEATURES_ROADMAP.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md)
- V1 baseline summary: [V1_NOTES_CONDENSED_SUMMARY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/V1_NOTES_CONDENSED_SUMMARY.md)
