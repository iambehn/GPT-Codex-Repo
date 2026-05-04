# Manifest Contracts / Game Packs

This document is the canonical V2 home for manifest and game-pack rules.

## Canonical Scope

Use this doc for:

- game-pack structure
- canonical manifest categories
- required vs optional manifest rows
- publish validation and completeness policy
- asset provenance, versioning, and patch tracking

Do not use this doc for:

- runtime scoring heuristics
- hook packaging logic
- source-enrichment identity scoring

## Current V2 Position

The repo stays manifest-driven.

Stable expectations:

- game-specific knowledge should be expressed through packs and manifests, not global config sprawl
- required rows must be explicit
- unresolved required rows should block publish or promotion
- asset provenance, patch validity, and hashes are first-class fields
- ROI references and template metadata belong to manifests, not detector code

Expected game-pack categories:

- game metadata
- entities
- abilities / UI-affecting semantics
- medals / event badges
- HUD and ROI maps
- runtime templates and masks

## What Belongs Elsewhere

- Runtime detector and fusion behavior belongs in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Registry storage and lifecycle state belong in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Enrichment-source contracts belong in [SOURCE_ENRICHMENT_IDENTITY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/SOURCE_ENRICHMENT_IDENTITY.md).

## Current Repo Anchors

- starter assets: `/starter_assets`
- current game assets: `/assets/games`
