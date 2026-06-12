# Call of Duty Replay Artifact Supersession Procedure

Status: active
Version: 0.1
Last updated: 2026-06-13

## Purpose

This is the bounded operator procedure for classifying persisted
`call_of_duty` replay artifacts as:

- active
- superseded
- protected

It exists to solve the replay-root hygiene problem identified in:

- [2026-06-13-call-of-duty-replay-contract-audit-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-contract-audit-report-v0.md)

It does not exist to:

- delete artifacts automatically
- change replay behavior
- change review or export policy
- widen beyond the bounded `call_of_duty` replay path

## Governing Scope

Use this procedure only for:

- replay artifacts under:
  - [outputs/editorial_replay/identities/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty)
  - [outputs/editorial_replay/decisions/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty)
  - [outputs/editorial_replay/snapshots/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/snapshots/call_of_duty)
- canonical review-session manifests for the bounded proof path
- the historical export workflow anchor:
  - `workflow-11aea2937311834b`

Preserve these limits:

- no multi-game cleanup
- no posting or publish surfaces
- no destructive mutation without an explicit cleanup slice

## Authority Surfaces

Use these surfaces as the source of truth:

### Canonical review-session manifests

- [call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json)
- [call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json)

### Replay procedure

- [CALL_OF_DUTY_EDITORIAL_REPLAY_PROCEDURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CALL_OF_DUTY_EDITORIAL_REPLAY_PROCEDURE.md)

### Audit baseline

- [2026-06-13-call-of-duty-replay-contract-audit-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-contract-audit-report-v0.md)

## Artifact Classes

### Active

A replay artifact is `active` if it is directly referenced by the current
canonical bounded proof-path manifests.

For identities and decisions, that means:

- the path appears in one of:
  - `editorial_identity_path`
  - `editorial_decision_path`
  on the current canonical runtime or fused review-session items

For snapshots, that means:

- the snapshot is required to replay a still-authoritative historical workflow
  run
- and its `editorial_object_id` matches a historical workflow row still treated
  as canonical replay evidence

### Superseded

A replay artifact is `superseded` if all of the following are true:

1. it is not referenced by the current canonical bounded proof-path manifests
2. it belongs to the same replay family and semantic object as a newer active
   artifact
3. the newer active artifact is replay-valid
4. the older artifact no longer participates in bounded replay or historical
   export regeneration

Typical example:

- a pre-normalization identity or decision artifact created before stable
  repo-relative source normalization, later replaced by a normalized active
  artifact referenced by the canonical review sessions

### Protected

A replay artifact is `protected` if deleting or relocating it would break:

- current canonical review replay
- current bounded export replay
- historical validation evidence still used by operator procedures or durable
  reports

By default:

- all active artifacts are protected
- all snapshots tied to authoritative historical export replay are protected

## Supersession Rules

### Rule 1: Canonical manifest linkage outranks raw presence

If an identity or decision artifact exists on disk but is not referenced by the
current canonical bounded review-session manifests, raw presence alone does not
make it active.

### Rule 2: Recomputed identity wins over older derivation

If the current bounded replay contract recomputes a stable
`editorial_object_id` that differs from an older artifact derivation, the
artifact linked by the current canonical review-session manifest is the active
one.

The older artifact becomes a supersession candidate.

### Rule 3: Historical export snapshots are protected by workflow replay

If a snapshot is required to replay:

- `workflow-11aea2937311834b`

then it remains protected even if the current lifecycle state has already moved
past `selected_for_export`.

### Rule 4: Audit warning is not cleanup authorization

An artifact classified as `superseded` is not automatically safe to delete in
the same slice.

This procedure authorizes only:

- classification
- reporting
- optional quarantine planning

It does not authorize:

- deletion
- relocation
- manifest rewrite

## Procedure

### 1. Load canonical authority surfaces

Read:

- the canonical runtime review session manifest
- the canonical fused review session manifest
- the historical export replay workflow anchor

Build the active path sets for:

- identity artifacts
- decision artifacts

### 2. Enumerate replay roots

Enumerate all persisted `call_of_duty` replay artifacts under:

- identities root
- decisions root
- snapshots root

### 3. Classify identities and decisions

For each identity or decision artifact:

1. if its path is referenced by the canonical session manifests:
   - classify as `active`
   - classify as `protected`
2. else if it corresponds to an older derivation replaced by an active
   normalized artifact:
   - classify as `superseded`
3. else:
   - classify as `unresolved`
   - stop and escalate before cleanup planning

### 4. Classify snapshots

For each snapshot artifact:

1. if it is required for replay of an authoritative historical workflow run:
   - classify as `active`
   - classify as `protected`
2. else if it is outside the bounded proof-path authority:
   - leave unclassified by this procedure
3. if uncertain:
   - classify as `unresolved`

### 5. Produce a supersession report

The report should include:

- active identity count
- active decision count
- active protected snapshot count
- superseded identity count
- superseded decision count
- any unresolved artifacts
- explicit example paths for each non-empty category

## Pass / Fail Conditions

### Pass

The supersession procedure passes if:

1. every canonical session-linked identity is present and classifiable as active
2. every canonical session-linked decision is present and classifiable as
   active
3. the historical export snapshot anchor remains protected
4. all non-linked replay artifacts can be cleanly classified as superseded or
   explicitly out of scope
5. no unresolved artifact remains

### Fail

The supersession procedure fails if:

- any canonical linked replay artifact is missing
- any historical export replay snapshot required by the bounded procedure is
  missing
- any orphaned artifact cannot be distinguished between superseded and active
- cleanup would require guessing instead of authority-surface evidence

## Allowed Next Actions

If the procedure passes, the next bounded goals may be:

1. write a supersession audit report
2. define a quarantine procedure for superseded replay artifacts
3. define a bounded cleanup implementation slice

If the procedure fails:

- stop
- report the unresolved artifacts
- do not delete or relocate anything

## Current Expected Classification Pattern

Based on the current audit baseline:

- active identities:
  - `6`
- active decisions:
  - `6`
- protected historical snapshot rows for `workflow-11aea2937311834b`:
  - `1`
- superseded pre-normalization identity artifacts:
  - `6`
- superseded pre-normalization decision artifacts:
  - `6`

Treat that as the expected bounded reference pattern until a later canonical
replay validation supersedes it.
