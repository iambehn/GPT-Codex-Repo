# Call of Duty Replay Artifact Cleanup Implementation Slice v0

Date: 2026-06-13
Status: proposed
Scope: bounded cleanup implementation design only

## Objective

Define the bounded cleanup implementation slice for superseded `call_of_duty`
replay artifacts.

This slice is limited to:

- materializing a quarantine manifest
- moving only the superseded replay artifacts already classified by the
  supersession audit
- rerunning bounded replay validation after the move

This slice does not include:

- deletion
- publish or posting changes
- multi-game cleanup
- replay contract redesign

## Preconditions

This cleanup implementation slice is valid only if all of the following remain
true at execution time:

1. the latest supersession audit still reports:
   - `unresolved identities = 0`
   - `unresolved decisions = 0`
   - `unresolved snapshots = 0`
2. the latest quarantine procedure still passes
3. every active replay artifact still exists
4. the protected historical snapshot for:
   - `workflow-11aea2937311834b`
   still exists

If any precondition fails:

- stop
- do not execute cleanup

## Authority Surfaces

Operator procedures:

- [CALL_OF_DUTY_REPLAY_ARTIFACT_SUPERSESSION_PROCEDURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CALL_OF_DUTY_REPLAY_ARTIFACT_SUPERSESSION_PROCEDURE.md)
- [CALL_OF_DUTY_REPLAY_ARTIFACT_QUARANTINE_PROCEDURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CALL_OF_DUTY_REPLAY_ARTIFACT_QUARANTINE_PROCEDURE.md)
- [CALL_OF_DUTY_EDITORIAL_REPLAY_PROCEDURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CALL_OF_DUTY_EDITORIAL_REPLAY_PROCEDURE.md)

Audit artifacts:

- [2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md)
- [2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md)

## Cleanup Target Set

The cleanup execution is authorized to move only the currently classified
superseded artifacts:

- superseded identities:
  - `6`
- superseded decisions:
  - `6`

Total move set:

- `12` replay artifacts

The cleanup execution is not authorized to move:

- any active identity artifact
- any active decision artifact
- any protected snapshot

## Intended Destination

The cleanup implementation should materialize:

```text
outputs/editorial_replay_quarantine/call_of_duty/<timestamp>/
```

with subtrees:

- `identities/`
- `decisions/`
- `manifests/`

The original relative filenames should be preserved inside the corresponding
subtrees.

## Quarantine Manifest Contract

The cleanup execution must write a quarantine manifest before any artifact move.

Minimum fields:

- `schema_version`
- `quarantine_run_id`
- `game`
- `created_at`
- `source_supersession_audit_report_path`
- `source_quarantine_procedure_path`
- `superseded_identity_paths`
- `superseded_decision_paths`
- `active_identity_paths`
- `active_decision_paths`
- `protected_snapshot_paths`
- `non_goals`
- `reversal_rule`
- `post_move_validation_required`

Recommended values:

- `schema_version = replay_artifact_quarantine_manifest_v1`
- `post_move_validation_required = true`

Reversal rule:

```text
Restore quarantined artifacts to their original replay roots without
recomputing payloads.
```

## Execution Sequence

### 1. Re-read the latest supersession audit

Purpose:

- ensure the move set is still authoritative

Stop if:

- any unresolved artifacts now exist

### 2. Re-check active and protected artifacts

Verify:

- all active identity paths exist
- all active decision paths exist
- the protected snapshot path exists

Stop if:

- any required active or protected artifact is missing

### 3. Materialize the quarantine manifest

Write the manifest into:

```text
outputs/editorial_replay_quarantine/call_of_duty/<timestamp>/manifests/
```

The manifest must be written before moving any artifact.

### 4. Move only the superseded candidate set

Move:

- the `6` superseded identity artifacts into:
  - `identities/`
- the `6` superseded decision artifacts into:
  - `decisions/`

Do not copy.

Reason:

- the goal is to reduce replay-root clutter while preserving a reversible
  quarantine set

### 5. Rerun bounded replay validation

Immediately rerun the bounded replay validation after the move.

Required checks:

1. runtime replay still works
2. fused replay still works
3. historical export replay still works for:
   - `workflow-11aea2937311834b`
4. runtime, fusion, and selection regression checks still pass

### 6. Record the cleanup result

The cleanup execution should produce a post-move report that records:

- quarantine manifest path
- moved identity count
- moved decision count
- active artifact counts after move
- replay validation pass/fail result
- whether rollback was required

## Pass / Fail Conditions

### Pass

The cleanup implementation slice passes only if:

1. exactly `12` superseded artifacts are moved
2. no active identity or decision artifact is moved
3. no protected snapshot is moved
4. the quarantine manifest exists and matches the moved set
5. bounded replay validation still passes after the move

### Fail

The cleanup implementation slice fails if:

- any active or protected artifact is included in the move set
- the move set differs from the latest supersession audit without an updated
  audit artifact
- bounded replay validation fails after the move
- rollback cannot be performed deterministically

## Rollback Rule

If post-move replay validation fails:

1. restore all moved identity artifacts to their original identity root
2. restore all moved decision artifacts to their original decision root
3. rerun bounded replay validation again
4. record the failed cleanup attempt and rollback outcome

## Recommended Next Execution Goal

When implementation is actually desired, the next bounded goal should be:

```text
/goal Execute the bounded call_of_duty replay-artifact cleanup implementation slice by materializing a quarantine manifest, moving only the 12 superseded replay artifacts into the quarantine destination, and rerunning bounded replay validation afterward. Do not move active replay artifacts or protected snapshots.
```

That would be the first destructive-or-reversible action in this replay-root
hygiene track, so it should be treated as a separate execution checkpoint.
