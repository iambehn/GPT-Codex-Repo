# V2 Source of Truth Index

This directory is the canonical V2 documentation spine.

Use this index to answer two questions:

1. Where does a topic belong?
2. Which document is authoritative for that topic?

The roadmap remains the sequencing authority:

- [Version 2 Roadmap](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md)

This index is the categorization authority.

## Canonical Categories

| Category | Canonical Doc | Use For | Do Not Use For |
| --- | --- | --- | --- |
| Architecture / Operating Model | [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md) | Pipeline boundaries, staged compute policy, operating model | Detailed experiment results or one-off implementation notes |
| Manifest Contracts / Game Packs | [MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md) | Game-pack structure, canonical manifest fields, publish/validation rules | Detector tuning or platform-posting behavior |
| Detection / Runtime / Fusion | [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md) | Proposal generation, runtime signals, fusion, reranking policy | Editorial packaging or sourcing logic |
| Hook / Editorial Packaging | [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md) | Hook artifacts, packaging logic, truthful editorial rules | Raw detector semantics or post metrics |
| Review / Calibration / Replay | [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) | Review loops, fixture comparisons, replay, calibration decisions | Broad architecture or registry schema ownership |
| Registry / Orchestration / State | [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md) | Queryable state, lifecycle, orchestration boundaries | Model-selection or hook taxonomy decisions |
| Source Enrichment / Identity Resolution | [SOURCE_ENRICHMENT_IDENTITY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/SOURCE_ENRICHMENT_IDENTITY.md) | Leaderboards, identity edges, enrichment confidence, yield | Runtime event truth or ranking core logic |
| Distribution / Post Ledger / Analytics | [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md) | Post records, routing, metrics, downstream learning surfaces | Upstream runtime or fusion contracts |
| Experiments / Trials / Decisions | [experiments/README.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/experiments/README.md) and [decisions/README.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/decisions/README.md) | Measured outcomes, trial evidence, ADRs | Stable subsystem theory |
| Archive / Raw Notes | [archive/RAW_NOTES_CATALOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/archive/RAW_NOTES_CATALOG.md) | Traceability back to Drive notes and older planning artifacts | Day-to-day operating truth |

## Overlap Rules

- Canonical logic is written once.
- Other docs point to the canonical explanation.
- Short contextual summaries are allowed where they reduce reader friction.
- Full repeated policy is discouraged.
- Decisions belong in ADRs, not mixed into subsystem docs.
- Measured outcomes belong in experiment ledgers, not mixed into architecture prose.
- Raw notes remain preserved, but they are references, not equal-weight authorities.

## Practical Placement Rules

- If the content answers "how is the pipeline supposed to work?", put it in a subsystem doc.
- If it answers "why did we choose this?", put it in an ADR.
- If it answers "what happened when we tried it?", put it in an experiment ledger.
- If it is exploratory or historical, catalog it in the archive.

## Current V2 Priorities

The first active V2 items remain:

1. Canonical documentation and source-of-truth setup
2. Unified replay/debug and report-aware inspection hardening
3. Persistent registry and queryable artifact state
4. Candidate and review lifecycle hardening
5. Hook candidate artifact family and measurable editorial logic

Everything else should sequence behind those unless the roadmap changes.
