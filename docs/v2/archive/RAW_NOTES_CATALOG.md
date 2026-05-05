# Raw Notes Catalog

This file catalogs the current V2 note corpus and points each note to its canonical destination.

Raw notes remain preserved for depth and traceability. They are not equal-weight operating authorities.

## Catalog

| Note | Primary Category | Secondary Categories | Current Status | Canonical Destination |
| --- | --- | --- | --- | --- |
| `v2Rebuild` | Manifest Contracts / Game Packs | Detection / Runtime / Fusion, Architecture / Operating Model, Review / Calibration / Replay | sequencing absorbed | [FUTURE_FEATURES_ROADMAP.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md), [MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md), [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md) |
| `CodexPluginNotes` | Architecture / Operating Model | Review / Calibration / Replay, Distribution / Post Ledger / Analytics | partially absorbed | [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md), [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) |
| `HuggingFaceNotes` | Detection / Runtime / Fusion | Review / Calibration / Replay | partially absorbed | [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md) |
| `DeepResearch` | Manifest Contracts / Game Packs | Source Enrichment / Identity Resolution, Registry / Orchestration / State | partially absorbed | [MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md), [SOURCE_ENRICHMENT_IDENTITY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/SOURCE_ENRICHMENT_IDENTITY.md) |
| `v2NarrativeHook` | Hook / Editorial Packaging | Distribution / Post Ledger / Analytics, Source Enrichment / Identity Resolution | partially absorbed | [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md), [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md) |
| `FutureFeaturesRoadmap` | Architecture / Operating Model | Registry / Orchestration / State, Review / Calibration / Replay | canonicalized | [FUTURE_FEATURES_ROADMAP.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md) |
| `InterdependencyNotes` | Detection / Runtime / Fusion | Hook / Editorial Packaging, Experiments / Trials / Decisions | partially absorbed | [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md), [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md) |

## Use Rules

- If a note is marked `sequencing absorbed`, use the roadmap as the active implementation-order authority and move stable subsystem logic into canonical docs rather than back into the note.
- If a note is marked `canonicalized`, do not extend the note as an active planning surface.
- If a note is marked `partially absorbed`, move new stable logic into its canonical destination rather than appending more mixed planning prose to the note.
- If a future note does not fit one of the canonical categories, update the category model deliberately rather than creating an ad hoc new planning file.
