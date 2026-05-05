# ADR: V2 Rebuild Sequencing After `v2Rebuild`

## Date

2026-05-06

## Status

Accepted

## Context

The repo already has substantial V2 baseline coverage in manifest-driven packs, runtime normalization, fusion, replay, review, registry, and export lineage.

The remaining problem is not net-new architecture. It is sequencing and contract hardening.

The Google Drive note `v2Rebuild` reinforced a specific implementation order:

1. manifest/onboarding production and publish safety
2. runtime/fusion contract hardening
3. review/calibration/replay release gates
4. candidate/export/downstream analytics integration

The existing roadmap treated manifest-driven game packs as completed baseline and started active sequencing later in the lifecycle. That was directionally correct for architecture, but too weak for current implementation priorities because manifest production and publish gating still need explicit hardening.

## Decision

The V2 sequencing authority remains `FUTURE_FEATURES_ROADMAP.md`.

`v2Rebuild` is adopted as a sequencing input, not as a competing planning surface.

The active V2 implementation order is now:

1. Manifest and publish hardening
2. Runtime and fusion contract hardening
3. Review, calibration, and replay release gating
4. Candidate, export, and downstream analytics integration

This does not revoke the completed baseline. It clarifies that baseline implementation exists, but contract-hardening and operational release gates are still active work.

## Consequences

- `FUTURE_FEATURES_ROADMAP.md` becomes the explicit owner of this sequencing change.
- `docs/v2/MANIFEST_CONTRACTS.md` owns required-row, publish-blocking, and draft-vs-published rules.
- `docs/v2/DETECTION_RUNTIME_FUSION.md` owns the shared signal and explainable fusion contract.
- `docs/v2/REVIEW_CALIBRATION_REPLAY.md` owns the release-gate role of replay, calibration, and review-backed comparisons.
- `docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md` remains downstream-only and should not redefine upstream evidence truth.

## Related Artifacts

- [FUTURE_FEATURES_ROADMAP.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md)
- [docs/v2/MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md)
- [docs/v2/DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
- [docs/v2/REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)
- [docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md)
