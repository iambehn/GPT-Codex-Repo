# Call of Duty Replay Artifact Cleanup Execution Report v0

Date: 2026-06-13
Status: completed
Scope: bounded cleanup execution only

## Objective

Execute the bounded `call_of_duty` replay-artifact cleanup implementation slice
by:

- materializing a quarantine manifest
- moving only the superseded replay artifacts into quarantine
- rerunning bounded replay validation after the move

Non-goals:

- multi-game cleanup
- deletion
- replay-contract redesign
- publish or posting changes

## Preconditions Rechecked

The cleanup execution rechecked the authority surfaces before moving any
artifact:

- [2026-06-13-call-of-duty-replay-artifact-cleanup-implementation-slice-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-artifact-cleanup-implementation-slice-v0.md)
- [2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-artifact-supersession-audit-report-v0.md)

Verified before execution:

- `unresolved identities = 0`
- `unresolved decisions = 0`
- `unresolved snapshots = 0`
- active runtime and fused replay artifacts still existed
- protected historical snapshot for `workflow-11aea2937311834b` still existed

## Authorized Move Set

Recomputed from current repo state before execution:

- active identities: `6`
- active decisions: `6`
- protected snapshots: `1`
- superseded identities: `6`
- superseded decisions: `6`

Authorized move count:

- `12`

No active identity artifact, active decision artifact, or protected snapshot
was included in the move set.

## Quarantine Output

Quarantine root:

- [20260612T221128Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay_quarantine/call_of_duty/20260612T221128Z)

Manifest:

- [call_of_duty.replay_artifact_quarantine_manifest.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay_quarantine/call_of_duty/20260612T221128Z/manifests/call_of_duty.replay_artifact_quarantine_manifest.json)

Moved:

- superseded identities:
  - `6`
- superseded decisions:
  - `6`

Not moved:

- active identities
- active decisions
- protected snapshots

## Post-Move Replay Validation

### Mechanical regression checks

Runtime replay still passed:

- sidecar:
  - [svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json)
- result:
  - `ok = true`
  - `event_count = 3`

Fusion replay still passed:

- sidecar:
  - [svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json)
- result:
  - `ok = true`
  - `fused_event_count = 3`

Selection export still passed from both reviewed and fresh fused sidecars:

- reviewed selection export:
  - [svbtc2azzyw-60s-70s.validation-20260613.highlight_selection.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.highlight_selection.json)
  - `selected_highlight_count = 3`
- fresh-fused selection export:
  - [svbtc2azzyw-60s-70s.validation-20260613-from-fresh-fused.highlight_selection.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613-from-fresh-fused.highlight_selection.json)
  - `selected_highlight_count = 3`

### Isolated repo-local replay validation

Isolated validation root:

- [20260612T221526Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/replay_validation/call_of_duty/20260612T221526Z)

Validation mechanism:

- copied only the cleaned active replay identities
- copied only the cleaned active replay decisions
- copied only the protected historical snapshot:
  - [snapshot-498838e69d868f43.export_ready_snapshot.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/snapshots/call_of_duty/snapshot-498838e69d868f43.export_ready_snapshot.json)
- rewrote copied GPT-path fields to nonexistent `/tmp/nonexistent/...`
- replayed runtime and fused review state against fresh sidecar copies using
  only the copied repo-local replay artifacts
- regenerated the historical export batch for `workflow-11aea2937311834b`

Repo-local replay result:

- copied identities: `6`
- copied decisions: `6`
- copied snapshots: `1`
- runtime replay:
  - `ok`
  - `review_status = approved`
  - `review_app = repo_local_editorial_replay`
- fused replay:
  - `ok`
  - `applied_count = 2`
  - `equipment_visibility_atomic-363ba2756b53 = rejected`
  - `equipment_visibility_atomic-e57f5f737fcb = approved`

Historical export regeneration result:

- manifest:
  - [bootstrap-real-cod.replayed.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/replay_validation/call_of_duty/20260612T221526Z/exports/bootstrap-real-cod.replayed.highlight_export_batch.json)
- result:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

## Final State

Replay roots after cleanup:

- remaining active identities: `6`
- remaining active decisions: `6`
- remaining protected snapshots: `1`
- remaining superseded identities: `0`
- remaining superseded decisions: `0`

Rollback:

- not required

## Conclusion

Classification:

- bounded cleanup execution = `passes`

What changed:

- the `12` superseded replay artifacts were quarantined out of the live replay
  roots

What did not change:

- active replay artifacts remained in place
- the protected historical snapshot remained in place
- runtime, fusion, and selection behavior still worked
- repo-local editorial replay still worked
- historical export regeneration still worked from the preserved snapshot

The `call_of_duty` replay roots are now cleaner without breaking bounded
editorial replay or historical export regeneration.
