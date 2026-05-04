# Source Enrichment / Identity Resolution

This document is the canonical V2 home for leaderboards, external metadata, and identity-resolution logic.

## Canonical Scope

Use this doc for:

- leaderboard and wiki enrichment posture
- official-source vs tracker-source policy
- identity-edge and account-match confidence logic
- enrichment yield and review-burden expectations

Do not use this doc for:

- runtime event truth
- core highlight-score policy
- generic post-analytics decisions

## Current V2 Position

Source enrichment is a scored subsystem, not an assumed truth layer.

Stable policies:

- official-source-first where possible
- trackers and public profiles are enrichment layers, not authorities
- identity resolution is probabilistic and review-aware
- weak matches should be quarantined, not forced into runtime truth
- enrichment must prove downstream value through yield, compatibility, and review burden

This work stays downstream of stronger replay, state, and review foundations.

The default V2 question is not "can we scrape it?" but:

- does it improve sourcing, context, or downstream decisions enough to justify its complexity?

## What Belongs Elsewhere

- Core game-pack schema belongs in [MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md).
- Runtime highlight detection belongs in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Posting and analytics consequences belong in [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md).
