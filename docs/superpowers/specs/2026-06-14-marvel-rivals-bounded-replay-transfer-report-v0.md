# Marvel Rivals Bounded Replay Transfer Report v0

Date: 2026-06-14
Status: completed
Scope: bounded adjacent-game transfer validation only

## Objective

Run one bounded real-media `marvel_rivals` clip through the current:

- runtime-analysis path
- fused-review path
- local export path
- replay-validation path

and determine whether the replayable proof path transfers beyond
`call_of_duty`.

Non-goals:

- multi-clip `marvel_rivals` validation
- publish clearance
- production-readiness claims
- policy changes

## Validation Target

Source clip:

- `/Users/tj/GPT-Codex-Repo/accepted/marvel_rivals/ABSOLUTE CINEMA_3522796292.mp4`

Validation root:

- [20260612T232308Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z)

## Runtime Outputs

Runtime sidecar:

- [absolute-cinema.transfer.validation.runtime_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/runtime_analysis/marvel_rivals/absolute-cinema.transfer.validation.runtime_analysis.json)

Result:

- `ok = true`
- `event_count = 9`
- `signal_count = 39`

Observed runtime families:

- `team_wipe_seen`
- `round_state_seen`
- `pov_character_identified`

## Fusion Outputs

Fused sidecar:

- [absolute-cinema.transfer.validation.fused_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/fused_analysis/marvel_rivals/absolute-cinema.transfer.validation.fused_analysis.json)

Result:

- `ok = true`
- proxy signals contributed: `6`
- runtime events contributed: `9`
- fused event count:
  - `39`

Bounded reviewed candidate:

- `team_wipe_visibility_atomic-2bafb5bf2577`
- `event_type = team_wipe_seen`
- `final_score = 0.97826`

## Review Outputs

Local GPT workspace:

- [gpt_repo](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/gpt_repo)

Runtime review session:

- [marvel_rivals-runtime-review-transfer-absolute-cinema-bdfe0e01b59b.runtime_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_review_sessions/marvel_rivals/marvel_rivals-runtime-review-transfer-absolute-cinema-bdfe0e01b59b.runtime_review_session.json)
- result:
  - `item_count = 1`
  - `approved_count = 1`

Fused review session:

- [marvel_rivals-fused-review-transfer-absolute-cinema-ed7b518fe939.fused_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_review_sessions/marvel_rivals/marvel_rivals-fused-review-transfer-absolute-cinema-ed7b518fe939.fused_review_session.json)
- result:
  - `item_count = 1`
  - `approved_count = 1`

Applied sidecar state:

- runtime sidecar:
  - `runtime_review.review_status = approved`
- fused sidecar:
  - `fused_review.events.team_wipe_visibility_atomic-2bafb5bf2577.review_status = approved`

Operator note:

- review-bridge apply expects GPT metadata:
  - `review_status = accepted`

## Export Outputs

Highlight selection export:

- [absolute-cinema.transfer.validation.highlight_selection.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/highlight_selection_exports/marvel_rivals/absolute-cinema.transfer.validation.highlight_selection.json)
- result:
  - `selected_highlight_count = 7`

Hook candidates:

- [absolute-cinema.transfer.validation.hook_candidates.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/hooks/marvel_rivals/absolute-cinema.transfer.validation.hook_candidates.json)
- result:
  - `hook_candidate_count = 1`

Workflow run:

- [transfer.export_queue.workflow_run.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/workflow_runs/marvel_rivals/transfer.export_queue.workflow_run.json)
- result:
  - `workflow_run_id = workflow-315441f610acf98b`
  - `item_count = 1`

Initial export batch:

- [transfer.initial.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/highlight_exports/marvel_rivals/transfer.initial.highlight_export_batch.json)
- result:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = false`

Replayed export batch:

- [transfer.replayed.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/highlight_exports/marvel_rivals/transfer.replayed.highlight_export_batch.json)
- result:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

## Replay Validation Outputs

Isolated replay validation root:

- [20260612T232641Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/marvel_rivals/20260612T232308Z/replay_validation/marvel_rivals/20260612T232641Z)

Copied replay contract set:

- identities copied: `2`
- decisions copied: `2`
- snapshots copied: `1`

Replay results:

- runtime replay:
  - `ok`
  - `review_status = approved`
- fused replay:
  - `ok`
  - `applied_count = 1`
  - replayed approved event:
    - `team_wipe_visibility_atomic-2bafb5bf2577`
- export replay:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

Replay mechanism:

- copied only clip-local `marvel_rivals` identity records
- copied only clip-local `marvel_rivals` decision records
- copied only the workflow-local export-ready snapshot
- rewrote copied GPT paths to nonexistent `/tmp/nonexistent/...`
- replayed into fresh sidecar copies using only the copied repo-local replay
  artifacts

## Observed Failure Modes

### 1. Fusion must wait for runtime materialization

Observed:

- the first fused command failed with:
  - `missing_runtime_sidecar`

Resolution:

- rerun fusion only after the runtime sidecar existed

Assessment:

- execution-order issue only

### 2. Runtime wrapper process remained live after sidecar write

Observed:

- the runtime shell session continued to run after the target sidecar had
  already been written and validated on disk

Resolution:

- continue using the completed sidecar as the local source of truth once
  `ok = true` and `event_count` were present

Assessment:

- operational wrapper issue, not a replay-contract failure

## Assessment

Classification:

- bounded adjacent-game transfer validation = `passes`

What transferred successfully from `call_of_duty` to `marvel_rivals`:

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
- this is one bounded `marvel_rivals` sample only

## Conclusion

The replayable proof path transfers beyond `call_of_duty` to at least one
bounded `marvel_rivals` clip.

Current stronger repo-backed claim:

- the replay contract is not only same-game reproducible
- it is also bounded adjacent-game transferable across at least one published
  but weaker game pack

What remains unproven:

- broader `marvel_rivals` clip coverage
- multi-game rollout readiness
- publish workflow readiness
