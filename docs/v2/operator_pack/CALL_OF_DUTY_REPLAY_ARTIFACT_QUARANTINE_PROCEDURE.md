# Call of Duty Replay Artifact Quarantine Procedure

Status: active
Version: 0.1
Last updated: 2026-06-13

## Purpose

This is the bounded operator procedure for planning and verifying quarantine of
superseded `call_of_duty` replay artifacts before any cleanup implementation
slice.

It exists to answer:

- which superseded replay artifacts are safe to isolate from active replay
  surfaces?
- how should quarantine be recorded so cleanup is reversible and auditable?

It does not exist to:

- delete artifacts
- relocate artifacts yet
- modify canonical manifests
- widen beyond the bounded `call_of_duty` replay path

## Governing Scope

Use this procedure only after:

- [CALL_OF_DUTY_REPLAY_ARTIFACT_SUPERSESSION_PROCEDURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CALL_OF_DUTY_REPLAY_ARTIFACT_SUPERSESSION_PROCEDURE.md)
- [2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md)

Preserve these limits:

- `call_of_duty` only
- no runtime behavior changes
- no cleanup execution
- no artifact mutation in this procedure

## Preconditions

This procedure is valid only if the latest supersession audit reports:

- `unresolved identities = 0`
- `unresolved decisions = 0`
- `unresolved snapshots = 0`

If any unresolved artifacts remain:

- stop
- do not proceed to quarantine planning

## Artifact Classes Used Here

### Active

- directly referenced by the canonical runtime or fused review-session manifests

### Protected

- required for bounded historical export replay

### Superseded

- no longer referenced by canonical bounded replay authority
- replaced by normalized active artifacts
- still present on disk only for historical retention or cleanup planning

Only `superseded` artifacts are quarantine candidates.

## Quarantine Goal

Quarantine is not deletion.

Quarantine means defining a bounded, reversible isolation target for
superseded artifacts so that:

- active replay surfaces stay untouched
- superseded artifacts are explicitly separated from active replay roots
- any later cleanup implementation has a precise target set

## Quarantine Authority Surfaces

Use these as the source of truth:

- canonical runtime review session manifest
- canonical fused review session manifest
- protected historical snapshot anchor for:
  - `workflow-11aea2937311834b`
- latest supersession audit report

Do not improvise from raw directory contents alone.

## Procedure

### 1. Load the latest supersession audit result

Read:

- [2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md)

Extract:

- superseded identity artifact paths
- superseded decision artifact paths
- protected snapshot paths
- active identity and decision paths

### 2. Define the quarantine candidate set

The candidate set must contain only:

- superseded identities
- superseded decisions

It must not contain:

- active identities
- active decisions
- protected snapshots

If any active or protected artifact appears in the candidate set:

- fail the procedure

### 3. Define the intended quarantine destination contract

Quarantine planning should assume a future bounded destination like:

```text
outputs/editorial_replay_quarantine/call_of_duty/<timestamp>/
```

With subtrees:

- `identities/`
- `decisions/`
- `manifests/`

This procedure does not create that tree. It defines the contract for a later
cleanup implementation slice.

### 4. Define the quarantine manifest contract

Any future quarantine execution should produce a manifest containing at least:

- `schema_version`
- `quarantine_run_id`
- `game`
- `created_at`
- `source_audit_report_path`
- `superseded_identity_paths`
- `superseded_decision_paths`
- `protected_snapshot_paths`
- `active_identity_paths`
- `active_decision_paths`
- `non_goals`
- `reversal_rule`

Recommended reversal rule:

```text
Quarantine must be reversible by restoring superseded artifacts
to their original replay roots without recomputing payloads.
```

### 5. Define quarantine safety checks

Before any cleanup implementation is allowed, the future execution slice must
re-check:

1. every active identity path still exists
2. every active decision path still exists
3. the protected snapshot path still exists
4. every quarantine candidate path still exists
5. no unresolved artifacts have appeared since the last supersession audit

If any check fails:

- stop
- do not quarantine

### 6. Define post-quarantine verification requirements

Any later cleanup implementation must rerun:

- bounded editorial replay validation
- bounded historical export regeneration

Minimum pass checks after quarantine execution:

1. runtime replay still works
2. fused replay still works
3. historical export replay still works for:
   - `workflow-11aea2937311834b`
4. no canonical session manifest paths need rewriting

## Pass / Fail Conditions

### Pass

This quarantine-planning procedure passes if:

1. the latest supersession audit has zero unresolved artifacts
2. the quarantine candidate set contains only superseded artifacts
3. no active or protected artifact is included
4. the quarantine destination and manifest contracts are explicit
5. the required pre- and post-quarantine verification checks are explicit

### Fail

The procedure fails if:

- unresolved artifacts still exist
- any active or protected artifact is included in the quarantine candidate set
- quarantine cannot be defined without guessing

## Current Expected Candidate Set

Based on the current supersession audit baseline:

- superseded identities:
  - `6`
- superseded decisions:
  - `6`
- protected snapshots:
  - `1`

That means the current expected quarantine candidate set is:

- `12` replay artifacts total
  - `6` identity artifacts
  - `6` decision artifacts

## Allowed Next Actions

If this procedure passes, the next justified bounded goal is:

- define a cleanup implementation slice that:
  - materializes the quarantine manifest
  - moves only the superseded candidate set
  - reruns bounded replay validation afterward

If this procedure fails:

- stop
- do not implement cleanup
