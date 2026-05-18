# Engineering Governance

This document expands the repo-wide governance spine in [AGENTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/AGENTS.md).

Use it for:

- decision-system placement rules
- anti-bloat engineering posture
- validation expectations for behavior changes
- Codex and human operator alignment

Do not use it for:

- subsystem-specific runtime mechanics
- detailed scoring thresholds
- calibration values
- publish-ready numeric policies

Those details belong in the canonical subsystem docs, manifests, code, and tests that already govern them.

## Decision Classes

### Repo / Build Heuristics

These govern how the repository is shaped and how automation is allowed to evolve.

Examples:

- whether a new workflow deserves a new file or should extend an existing contract
- whether a helper improves inspectability or only duplicates state
- whether a report belongs as a compact inspector or a broad manager command

These rules live primarily in:

- [AGENTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/AGENTS.md)
- this document
- focused contract-audit checks

### Contract / Schema Rules

These govern field ownership, schema versions, persisted artifact shape, and allowed workflow transitions.

Examples:

- manifest schema versions
- required ledger fields
- review state transitions
- artifact output contracts

These rules live primarily in:

- `pipeline/`
- `assets/games/.../manifests`
- `tests/`
- canonical subsystem docs under `docs/v2/`

### Data-Quality Rules

These govern provenance, completeness, drift visibility, replayability, and whether the system can still be trusted.

Examples:

- provenance fields must remain present
- reviewed fixtures should stay comparable over time
- unsupported schema variants should surface clearly
- sparse labels or imbalanced examples should be visible when they affect decision quality

These rules live primarily in:

- validation code
- audits
- replay and calibration tooling
- maintenance surfaces

### Maintenance Expectations

These govern recurring health checks and cleanup triggers.

Examples:

- when fixture refresh is required
- when schema drift is blocking
- when warnings can remain open temporarily
- when stale draft state should be archived or promoted

These rules live primarily in:

- [QUALITY_MAINTENANCE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/QUALITY_MAINTENANCE.md)
- compact audit/report tooling
- operator workflows and tests

## Heuristic Placement Model

Use this placement model consistently:

- Repo-wide policy belongs in `AGENTS.md`.
- Human-readable rationale belongs in canonical docs under `docs/v2/`.
- Machine-checkable truth belongs in code, manifests, configs, fixtures, or tests.

That means:

- a policy like "silent threshold changes are forbidden" belongs in `AGENTS.md`
- a rationale like "why this confidence bucket exists" belongs in canonical docs
- the actual threshold values belong in code, manifests, calibration policy files, or tests

## Acceptable Placement

Acceptable heuristic placement looks like this:

1. A doc explains why a rule exists.
2. The governing runtime/config surface expresses the actual rule.
3. A test, fixture, replay, or contract audit demonstrates the intended outcome.

Examples:

- A fusion confidence rule is explained in `docs/v2/DETECTION_RUNTIME_FUSION.md`, implemented in `pipeline/`, and covered by focused tests.
- A review-state contract is explained in `docs/v2/REVIEW_CALIBRATION_REPLAY.md`, enforced by workflow code, and checked by regression tests.
- A manifest field requirement is described in `docs/v2/MANIFEST_CONTRACTS.md`, stored in the manifest schema, and checked by validation/audit tests.

## Unacceptable Placement

Unacceptable heuristic placement includes:

- behavior-changing thresholds written only in prose
- chat-only decisions with no durable landing place
- duplicate docs that restate canonical policy with slight wording drift
- helper scripts that create a second source of truth instead of validating the first
- broad orchestration wrappers that hide contract ambiguity instead of resolving it

If a materially behavior-changing rule exists only in prose, treat it as not implemented.

## Validation Spine

Behavior-changing work should normally attach to one or more of these layers:

### Contract Tests

Use for:

- schema versions
- required fields
- artifact output shape
- persisted-state consistency
- canonical-source drift

`tests/test_contract_audit.py` is part of this layer and should remain a compact source-of-truth check.

### Heuristic Behavior Tests

Use for:

- decision boundaries
- validation helpers
- threshold-sensitive outputs
- explicit examples that prove why a branch should accept, reject, warn, or escalate

### Replay And Fixture Tests

Use for:

- baseline vs trial comparisons
- decision drift checks
- reviewed example preservation
- calibration and replay integrity

### Data-Quality And Health Tests

Use for:

- provenance presence
- manifest completeness
- supported vs unsupported schema detection
- label sparsity or imbalance warnings where they matter

## Anti-Bloat Review Questions

Before adding a new file, command, or artifact, ask:

1. Does an existing contract surface already own this behavior?
2. Is this new artifact authoritative, or is it only mirroring existing state?
3. Does this change improve validation, inspectability, or operator clarity enough to justify itself?
4. Are the non-goals explicit?
5. Is the smallest reliable version narrower than the first idea?

If these questions do not resolve cleanly, stop and tighten the design before implementing.
