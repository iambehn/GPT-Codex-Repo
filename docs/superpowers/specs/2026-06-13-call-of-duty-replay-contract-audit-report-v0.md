# Call of Duty Replay Contract Audit Report v0

Date: 2026-06-13
Status: completed
Scope: bounded replay-contract audit only

## Objective

Audit the persisted bounded replay contract for the canonical `call_of_duty`
proof path and determine whether the three replay object families are coherent
enough to support repeatable bounded replay.

Contract objects under audit:

1. stable editorial identity records
2. editorial decision records
3. export-ready snapshot records

Non-goals:

- replay implementation changes
- artifact cleanup or deletion
- widening beyond `call_of_duty`
- publish-readiness claims

## Audit Basis

Operator authority:

- [CALL_OF_DUTY_EDITORIAL_REPLAY_PROCEDURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CALL_OF_DUTY_EDITORIAL_REPLAY_PROCEDURE.md)

Replay validation baseline:

- [2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md)

Canonical review-session artifacts:

- [call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json)
- [call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json)

Canonical replay roots:

- [outputs/editorial_replay/identities/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty)
- [outputs/editorial_replay/decisions/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty)
- [outputs/editorial_replay/snapshots/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/snapshots/call_of_duty)

Historical workflow reference:

- `workflow-11aea2937311834b`

## Audit Checks

### Identity and decision linkage

For every session item in the canonical runtime and fused review manifests:

- recompute `editorial_object_id`
- verify the linked identity artifact exists
- verify the linked decision artifact exists
- verify the identity payload references the same:
  - `editorial_object_id`
  - review surface
  - event id where applicable
- verify the decision payload references the same:
  - `editorial_object_id`
  - review surface
  - review status
  - event id where applicable

### Snapshot linkage

For the historical workflow run:

- load export-ready snapshots filtered to:
  - `workflow-11aea2937311834b`
  - `game = call_of_duty`
- verify that the snapshot editorial ids match the historical workflow rows

### Artifact-root hygiene

Compare:

- all replay artifacts present under the `call_of_duty` replay roots
- replay artifacts actively linked by the canonical runtime and fused review
  session manifests

Purpose:

- determine whether the replay roots are minimal or contain superseded artifact
  rows left behind by prior identity derivations

## Results

### 1. Active session-linked contract rows are coherent

Canonical review-session coverage:

- runtime session items:
  - `4`
- fused session items:
  - `2`

Linked replay artifacts:

- unique identity paths referenced by current canonical sessions:
  - `6`
- unique decision paths referenced by current canonical sessions:
  - `6`

Result:

- every active runtime session item resolves to an existing identity and
  decision artifact
- every active fused session item resolves to an existing identity and decision
  artifact
- recomputed editorial ids match the current session-linked replay artifacts
- decision payloads match the applied review surface and review status

Assessment:

- the active replay contract is coherent

### 2. Historical export snapshot linkage is coherent

Historical workflow coverage:

- workflow rows for `workflow-11aea2937311834b`:
  - `1`
- matching snapshot rows:
  - `1`

Observed snapshot:

- [snapshot-498838e69d868f43.export_ready_snapshot.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/snapshots/call_of_duty/snapshot-498838e69d868f43.export_ready_snapshot.json)

Observed editorial id:

- `editorial-dad698b25d4f9ddc`

Assessment:

- the preserved export-ready snapshot matches the historical workflow row
- historical export regeneration has a coherent snapshot anchor

### 3. Session-manifest source strings and identity payload source strings use different representations

Observed:

- canonical review-session item `source` values remain absolute paths
- active identity payload `source` values are stored in normalized repo-relative
  form

Example pattern:

- session item source:
  - absolute path under `outputs/public_gameplay_mining/...`
- identity payload source:
  - repo-relative `outputs/public_gameplay_mining/...`

Assessment:

- this is expected under the current normalized replay contract
- it is not a contract break
- audit checks should compare normalized source identity, not raw string
  equality, when evaluating coherence

### 4. Replay roots contain superseded pre-normalization artifacts

Observed counts:

- all `call_of_duty` identity artifacts present:
  - `12`
- active identity artifacts linked by canonical sessions:
  - `6`
- orphan identity artifacts:
  - `6`

- all `call_of_duty` decision artifacts present:
  - `12`
- active decision artifacts linked by canonical sessions:
  - `6`
- orphan decision artifacts:
  - `6`

Observed orphan pattern:

- orphan identities and decisions were created on:
  - `2026-06-12T21:36:33Z`
- they use the pre-normalization absolute-path identity basis
- they are no longer referenced by the canonical session manifests

Representative orphan identity rows:

- [editorial-704e50f8cf277339.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-704e50f8cf277339.editorial_identity.json)
- [editorial-835703f533661752.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-835703f533661752.editorial_identity.json)

Representative orphan decision rows:

- [decision-7de18a7394c04e55.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-7de18a7394c04e55.editorial_decision.json)
- [decision-dfb4cd6ad0181c5c.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-dfb4cd6ad0181c5c.editorial_decision.json)

Assessment:

- this is not a replay-correctness failure
- it is a replay-root hygiene issue
- the current contract is operationally coherent but not artifact-minimal

## Audit Classification

Classification:

- active replay contract coherence:
  - `pass`
- historical export snapshot linkage:
  - `pass`
- replay-root hygiene:
  - `warning`

Overall status:

- bounded replay contract = `operationally coherent, procedurally non-minimal`

## Conclusion

The bounded `call_of_duty` replay contract is strong enough to support
repeatable bounded replay and historical export regeneration from the active
canonical artifacts.

The remaining issue is not correctness. It is hygiene:

- canonical sessions now point at normalized replay artifacts
- older pre-normalization replay artifacts remain in the replay roots and are
  no longer referenced

That means the next useful goal on this track is not another contract redesign.
It is a bounded cleanup or audit-hardening slice, such as:

- define a replay-artifact supersession rule
- add a cleanup or inspector path for orphaned replay artifacts
- or add an explicit audit command that reports linked versus superseded replay
  objects

No such cleanup is justified in this report itself. This is an audit result
only.
