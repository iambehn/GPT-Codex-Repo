# Marvel Rivals Second Bounded Clip Validation Report v0

Date: 2026-06-14
Status: completed
Scope: bounded second-clip `marvel_rivals` validation only

## Objective

Run a second bounded real-media `marvel_rivals` clip through the current:

- runtime-analysis path
- fused-review path
- local export path
- replay-validation path

and determine whether the replayable proof path is repeatable beyond the first
adjacent-game sample.

Non-goals:

- publish clearance
- multi-game rollout claims
- production-readiness claims
- policy changes

## Validation Target

Source clip:

- [kjeRAyM5ekA7XVsRm.mp4](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/public_gameplay_mining/marvel_rivals_quad_ace_sources/kjeRAyM5ekA7XVsRm.mp4)

Validation root:

- [20260612T233438Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z)

## Runtime Outputs

Runtime sidecar:

- [kjera.transfer.validation.runtime_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/runtime_analysis/marvel_rivals/kjera.transfer.validation.runtime_analysis.json)

Result:

- `ok = true`
- `event_count = 2`

Observed runtime event types:

- `round_state_seen`
- `team_wipe_seen`

## Fusion Outputs

Fused sidecar:

- [kjera.transfer.validation.fused_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/fused_analysis/marvel_rivals/kjera.transfer.validation.fused_analysis.json)

Result:

- `ok = true`
- proxy signal count: `3`
- fused event count: `2`

Bounded fused candidates:

1. `team_wipe_visibility_atomic-433cbe2551fe`
   - `event_type = team_wipe_seen`
   - `final_score = 0.9383`
   - bounded review decision = `approved`
2. `round_state_visibility_atomic-260564a7055e`
   - `event_type = round_state_seen`
   - `final_score = 0.84082`
   - bounded review decision = `rejected`

## Review Outputs

Local GPT workspace:

- [gpt_repo](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/gpt_repo)

Runtime review session:

- [marvel_rivals-runtime-review-transfer-kjera-local-5464a4359943.runtime_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_review_sessions/marvel_rivals/marvel_rivals-runtime-review-transfer-kjera-local-5464a4359943.runtime_review_session.json)
- result:
  - `item_count = 1`
  - `approved_count = 1`

Fused review session:

- [marvel_rivals-fused-review-transfer-kjera-local-fused-5b98c352594c.fused_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_review_sessions/marvel_rivals/marvel_rivals-fused-review-transfer-kjera-local-fused-5b98c352594c.fused_review_session.json)
- result:
  - `item_count = 2`
  - `approved_count = 1`
  - `rejected_count = 1`

Operator note:

- review-bridge apply still expects GPT metadata:
  - `review_status = accepted`

## Export Outputs

Highlight selection export:

- [kjera.transfer.validation.highlight_selection.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/highlight_selection_exports/marvel_rivals/kjera.transfer.validation.highlight_selection.json)
- result:
  - `selected_highlight_count = 2`

Hook candidates:

- [kjera.transfer.validation.hook_candidates.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/hooks/marvel_rivals/kjera.transfer.validation.hook_candidates.json)
- result:
  - `hook_candidate_count = 1`

Workflow run:

- [kjera.transfer.export_queue.workflow_run.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/workflow_runs/marvel_rivals/kjera.transfer.export_queue.workflow_run.json)
- result:
  - `workflow_run_id = workflow-715668f19522d994`
  - `item_count = 1`

Initial export batch:

- [kjera.transfer.initial.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/highlight_exports/marvel_rivals/kjera.transfer.initial.highlight_export_batch.json)
- result:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = false`

Replayed export batch:

- [kjera.transfer.replayed.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/highlight_exports/marvel_rivals/kjera.transfer.replayed.highlight_export_batch.json)
- result:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

## Replay Validation Outputs

Isolated replay validation root:

- [20260612T234120Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T233438Z/replay_validation/marvel_rivals/20260612T234120Z)

Replay contract copy set:

- identities copied: `3`
- decisions copied: `3`
- snapshots copied: `1`

Replay results:

- runtime replay:
  - `ok`
- fused replay:
  - `ok`
  - `applied_count = 2`
- export replay:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

Replay mechanism:

- copied only the clip-local runtime and fused editorial identity records
- copied only the clip-local runtime and fused decision records
- copied only the workflow-local export-ready snapshot
- rewrote copied GPT paths inside the copied decision records to
  `/tmp/nonexistent/...`
- replayed into fresh sidecar copies using only the copied repo-local replay
  artifacts

## Observed Failure Modes

### 1. Initial replayed export batch was not yet snapshot-based

Observed:

- the first replayed export-batch rerun still returned:
  - `replayed_from_export_ready_snapshot = false`

Resolution:

- refresh the bounded registry after the initial export batch so the exported
  lifecycle state is re-ingested
- rerun export-batch generation only after that refresh

Assessment:

- registry-refresh sequencing issue only
- not a replay-contract failure

## Assessment

Classification:

- bounded second-clip `marvel_rivals` validation = `passes`

What held on the second adjacent-game sample:

- runtime analysis
- fusion analysis
- local runtime review apply
- local fused review apply
- local export artifact creation
- snapshot-based export regeneration
- isolated repo-local replay validation

What remains unchanged:

- outputs remain local-only
- this is not publish-cleared
- this is bounded to one game family and one additional sample

## Conclusion

The replayable proof path is repeatable beyond the first `marvel_rivals`
sample.

Current stronger repo-backed claim:

- `marvel_rivals` replayability is no longer single-sample only
- bounded adjacent-game transfer now holds on at least two `marvel_rivals`
  clips
