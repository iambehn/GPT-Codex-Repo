# Call of Duty Second Bounded Clip Validation Report v0

Date: 2026-06-13
Status: completed
Scope: bounded `call_of_duty` second-clip validation only

## Objective

Run a second bounded real-media `call_of_duty` clip through the current:

- runtime-analysis path
- fused-review path
- local export path
- replay-validation path

and determine whether the replayable proof path generalizes beyond the first
validated clip.

Non-goals:

- publish clearance
- multi-game validation
- production-readiness claims
- threshold or policy changes

## Validation Target

Source clip:

- [SVbTc2AZzYw.10s.mp4](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.10s.mp4)

Validation root:

- [20260612T231543Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z)

## Runtime Outputs

Runtime sidecar:

- [svbtc2azzyw-10s.second.validation.runtime_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/runtime_analysis/call_of_duty/svbtc2azzyw-10s.second.validation.runtime_analysis.json)

Result:

- `ok = true`
- `signal_count = 2`
- `event_count = 2`

Observed runtime assets:

- `call_of_duty.armor_satchel.equipment_icon`
- `call_of_duty.redeploy_extraction_token.equipment_icon`

## Fusion Outputs

Fused sidecar:

- [svbtc2azzyw-10s.second.validation.fused_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/fused_analysis/call_of_duty/svbtc2azzyw-10s.second.validation.fused_analysis.json)

Result:

- `ok = true`
- `normalized_signal_count = 2`
- `fused_event_count = 2`

Observed fused candidates:

- `equipment_visibility_atomic-0907ff67ffbf`
- `equipment_visibility_atomic-97767b8be508`

Proxy contribution:

- proxy sidecar auto-linked:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/proxy_scans/call_of_duty/svbtc2azzyw-10s-ef857f3a298d.proxy_scan.json`
- proxy signals:
  - `0`
- fusion remained runtime-only for this sample

## Review Outputs

Local GPT workspace:

- [gpt_repo](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/gpt_repo)

Runtime review session:

- [call_of_duty-runtime-review-second-clip-10s-9ce9e945165f.runtime_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-second-clip-10s-9ce9e945165f.runtime_review_session.json)
- result:
  - `item_count = 1`
  - `approved_count = 1`
  - `rejected_count = 0`

Fused review session:

- [call_of_duty-fused-review-second-clip-10s-e0d7657993dd.fused_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-second-clip-10s-e0d7657993dd.fused_review_session.json)
- result:
  - `item_count = 1`
  - `approved_count = 1`
  - `rejected_count = 0`

Applied review state:

- runtime sidecar now contains:
  - `runtime_review.review_status = approved`
- fused sidecar now contains:
  - `fused_review.events.equipment_visibility_atomic-0907ff67ffbf.review_status = approved`

Operator note:

- the review-bridge apply contract expects GPT metadata `review_status = accepted`
- `approved` is normalized to `unreviewed` at apply time and is not valid input

## Export Outputs

Highlight selection export:

- [svbtc2azzyw-10s.second.validation.highlight_selection.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/highlight_selection_exports/call_of_duty/svbtc2azzyw-10s.second.validation.highlight_selection.json)
- result:
  - `selected_highlight_count = 2`

Hook candidates:

- [svbtc2azzyw-10s.second.validation.hook_candidates.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/hooks/call_of_duty/svbtc2azzyw-10s.second.validation.hook_candidates.json)
- result:
  - `hook_candidate_count = 1`

Workflow run:

- [second-clip.export_queue.workflow_run.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/workflow_runs/call_of_duty/second-clip.export_queue.workflow_run.json)
- result:
  - `workflow_run_id = workflow-2d35370440c2b510`
  - `item_count = 1`

Initial export batch:

- [second-clip.initial.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/highlight_exports/call_of_duty/second-clip.initial.highlight_export_batch.json)
- result:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = false`

Replayed export batch after lifecycle advance:

- [second-clip.replayed.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/highlight_exports/call_of_duty/second-clip.replayed.highlight_export_batch.json)
- result:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

## Replay Validation Outputs

Isolated replay validation root:

- [20260612T231853Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/bounded_validation/call_of_duty/20260612T231543Z/replay_validation/call_of_duty/20260612T231853Z)

Copied replay contract set:

- active identities copied: `2`
- active decisions copied: `2`
- export-ready snapshots copied: `1`

Replay mechanism:

- copied only the clip-local runtime and fused decision records
- copied only the clip-local identities
- copied only the workflow-local export-ready snapshot
- rewrote copied GPT paths to nonexistent `/tmp/nonexistent/...`
- replayed into fresh sidecar copies using only the copied repo-local replay artifacts

Runtime replay:

- `ok`
- `review_status = approved`

Fused replay:

- `ok`
- `applied_count = 1`
- replayed approved event:
  - `equipment_visibility_atomic-0907ff67ffbf`

Export replay:

- `ok`
- `export_count = 1`
- `replayed_from_export_ready_snapshot = true`

## Observed Failure Modes

### 1. Fusion must wait for runtime sidecar materialization

Observed:

- a first fusion attempt failed with:
  - `missing_runtime_sidecar`

Resolution:

- rerun fusion only after runtime sidecar creation completed

Assessment:

- this was an execution-order issue, not a pipeline regression

### 2. Review metadata contract is strict

Observed:

- GPT metadata `review_status = approved` applied as:
  - `unreviewed`

Resolution:

- use `accepted` for approval when feeding the review bridge

Assessment:

- this is an operator-contract nuance worth preserving in procedure docs

## Assessment

Classification:

- second bounded clip validation = `passes`

What generalized successfully:

- runtime analysis
- fusion analysis
- review application
- local export artifact creation
- snapshot-based export regeneration
- isolated repo-local replay validation

What remains unchanged:

- outputs remain local-only
- nothing here is publish-cleared
- this still proves only bounded `call_of_duty` behavior, not multi-game readiness

## Conclusion

The replayable `call_of_duty` proof path generalizes beyond the original
`60s-70s` sample to a second bounded real-media clip.

Current bounded claim is now stronger:

- editorial replay is operationally reproducible on more than one `call_of_duty`
  sample
- historical export regeneration is operationally reproducible on more than one
  `call_of_duty` sample

The next justified decision is no longer about replay-contract sufficiency. It
is whether to:

- validate more `call_of_duty` clips first
- or test one adjacent game as the smallest widening step
