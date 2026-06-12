# Export Regeneration Gap Report v0

Date: 2026-06-13
Status: completed
Scope: bounded `call_of_duty` proof-path diagnosis only

## Objective

Audit the `call_of_duty` export regeneration path from reviewed fused sidecar to workflow run to export batch, and determine why the current lifecycle snapshot no longer reproduces a `selected_for_export` candidate set.

Non-goals:

- changing lifecycle policy
- changing review policy
- fixing export regeneration
- widening beyond the current bounded proof path

## Executive Result

The export regeneration path does **not** replay because it mixes two different time models:

1. preserved workflow history
2. current lifecycle state

The historical workflow record still preserves a `selected_for_export` moment.

The current lifecycle snapshot for the same candidate has already advanced to `exported`.

`create_workflow_run(export_queue)` recomputes from current lifecycle state only, so it now returns zero items. `create_highlight_export_batch` expects `selected_for_export` candidates, so it fails after the candidate has already moved past that state.

## Evidence Surfaces

Historical workflow record:

- [bootstrap-real-cod.export_queue.workflow_run.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/workflow_runs/call_of_duty/bootstrap-real-cod.export_queue.workflow_run.json)

Historical export batch:

- [bootstrap-real-cod.highlight_export_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/highlight_exports/call_of_duty/bootstrap-real-cod.highlight_export_batch.json)

Canonical selection manifest:

- [svbtc2azzyw-60s-70s.bootstrap-real-cod.highlight_selection.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.highlight_selection.json)

Fresh replay attempt:

- [bootstrap-real-cod.validation-20260613.export_queue.workflow_run.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/workflow_runs/call_of_duty/bootstrap-real-cod.validation-20260613.export_queue.workflow_run.json)

## Observed Historical State

The preserved workflow-run history records:

- `workflow_run_id = workflow-11aea2937311834b`
- `workflow_type = export_queue`
- `stage = selected_for_export`
- `item_counts.total = 1`
- exported candidate at that point:
  - `candidate-a0ca7ce9055af9a5`

The historical export batch records:

- `export_batch_id = export-batch-a2580333a37c391d`
- `workflow_run_id = workflow-11aea2937311834b`
- `export_count = 1`
- exported candidate:
  - `candidate-a0ca7ce9055af9a5`

This shows the proof path did successfully traverse:

- reviewed fused sidecar
- selected-for-export workflow run
- export batch creation

## Observed Current Lifecycle State

Current lifecycle rows for the bounded `SVbTc2AZzYw.60s-70s.mp4` slice are now:

- `candidate-8e8ba3f2dcbfe8a0`
  - `lifecycle_state = rejected`
- `candidate-72e5ebb1bc98d6fc`
  - `lifecycle_state = pending_review`
- `candidate-a0ca7ce9055af9a5`
  - `lifecycle_state = exported`

Important detail:

- there is **no** current `selected_for_export` candidate for this clip

## Replay Attempt Result

Fresh workflow-run regeneration produced:

- [bootstrap-real-cod.validation-20260613.export_queue.workflow_run.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/workflow_runs/call_of_duty/bootstrap-real-cod.validation-20260613.export_queue.workflow_run.json)
- `workflow_run_id = workflow-08713216ef4844e0`
- `item_counts.total = 0`

Fresh export-batch regeneration then failed with:

- `status = no_selected_candidates`

## Why Regeneration Breaks

### 1. `create_workflow_run(export_queue)` is current-state driven

The export-queue workflow run is created from lifecycle rows with:

- `lifecycle_state = selected_for_export`

It does **not** reconstruct from:

- historical workflow manifests
- historical export batches
- historical selection moments

Implication:

- once the approved candidate advances from `selected_for_export` to `exported`
- the current queue surface becomes empty

### 2. `create_highlight_export_batch` requires `selected_for_export`, not `exported`

The batch builder filters candidates by `_selected_lifecycle_rows`, which explicitly selects:

- `lifecycle_state == selected_for_export`

It does not accept:

- `exported`

Implication:

- the exact candidate that was historically exportable is now ineligible for regeneration because it already crossed the export boundary

### 3. Workflow history and lifecycle state are not replay-equivalent

The registry preserves:

- historical workflow-run items
- current candidate lifecycle rows

These are not interchangeable.

Observed mismatch:

- `query-clip-registry --mode workflow-runs` still shows the historical selected-for-export moment for `candidate-a0ca7ce9055af9a5`
- `query-workflow-queue --workflow-type export_queue` returns zero rows because it recomputes from current lifecycle state

Implication:

- historical workflow memory exists
- but the regeneration commands do not use it as a replay source

### 4. Export is treated as a one-way lifecycle transition

Current lifecycle semantics behave as:

- `approved -> selected_for_export -> exported`

Once `exported` is recorded:

- the candidate is preserved as exported
- but it no longer re-enters the export queue

This is coherent as operational policy, but it means:

- export generation is not idempotent
- historical export bundles are inspectable artifacts, not replayable queue states

## Narrowest Diagnosis

The export regeneration gap is **not** caused by missing fused analysis or missing selection manifests.

Those artifacts are present.

The gap is caused by:

- export regeneration being anchored to current lifecycle state
- current lifecycle state having already advanced beyond `selected_for_export`

In short:

- historical exportability exists
- current exportability no longer exists
- the regeneration command only knows how to use current exportability

## Classification

Current export regeneration status:

- historical export proof: preserved
- current export queue replay: not reproducible
- export batch replay from current lifecycle: not reproducible

Short classification:

- `End-to-End Export Regeneration = Incomplete`

## Missing Contract

The current proof path is missing a contract for **historical export replay**.

Needed property:

- a preserved export-ready moment should be replayable even after lifecycle has advanced to `exported`

Current gap:

- current queue generation only sees present lifecycle state
- export batch generation only accepts `selected_for_export`
- no command reconstructs an export batch from:
  - historical workflow-run item set
  - or historical selection manifest plus approved reviewed candidate identity

## Recommended Next Step

Do not widen scope.

The next useful step is to define the minimum artifact contract for bounded editorial replay and export replay together, because both gaps are currently caused by missing replay contracts rather than missing signal artifacts.
