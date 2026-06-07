# Short Clip Edit Order Station Transitions Design

Date: 2026-06-08
Status: draft
Owner: Codex

## Objective

Define the station-transition model for `short_clip_edit_order_v1`.

This spec is the next layer under the work-order model defined in:

- [2026-06-08-video-editing-work-order-model-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-08-video-editing-work-order-model-design.md)

The purpose of this spec is to make one editing work order behave like a routed production task rather than a loose bundle of artifacts.

## Core Principle

One work order moves through stations.

Each transition must answer four questions:

1. what station is being exited?
2. what artifact or condition proves exit readiness?
3. what station is entered next?
4. what blocks or diverts the order instead?

This should be explicit enough that:

- the pipeline architect can implement routing cleanly
- the video editor node knows what work is ready
- the manager can audit slow or blocked orders

## Scope

This spec applies only to:

- `short_clip_edit_order_v1`

This spec does not yet define:

- pricing
- client billing
- marketplace sourcing
- multi-order scheduling optimization
- staffing policies across many worker types

## Canonical Stations

The canonical station set for `short_clip_edit_order_v1` is:

1. `intake`
2. `source_readiness`
3. `signal_extraction`
4. `candidate_generation`
5. `editorial_selection`
6. `edit_packaging`
7. `approval`
8. `delivery`
9. `post_completion`

## Canonical Station Statuses

At the station level, use:

- `ready`
- `in_progress`
- `blocked`
- `review_required`
- `approval_required`
- `done`
- `failed`
- `deferred`

These are station-local states.

They should not replace the higher-order work-order status.

## Transition Rules

### Station 1: `intake`

Purpose:

- create the work order
- bind source assets
- bind requested deliverable intent

Entry criteria:

- a source reference exists
- an order has been requested

Exit criteria:

- work-order record exists
- `source_asset_refs` is populated
- `requested_deliverable` is populated
- `game` and `source_mode` are known or explicitly unknown

Success transition:

- `intake -> source_readiness`

Block transitions:

- `intake -> blocked`
  - if no source asset reference exists
  - if deliverable intent is too ambiguous to define the order

Defer transitions:

- `intake -> deferred`
  - if the order is real but intentionally not yet scheduled

Terminal transitions:

- none

### Station 2: `source_readiness`

Purpose:

- verify that the repo can process this order responsibly

Entry criteria:

- work order exists
- source refs exist

Exit criteria:

- source media resolves or a valid non-media fallback mode is explicitly allowed
- required game support exists
- required pack/support state exists
- the next executable analysis path is known

Success transition:

- `source_readiness -> signal_extraction`

Block transitions:

- `source_readiness -> blocked`
  - missing or unreadable source media
  - game pack is absent or invalid
  - no supported path exists for the requested order type

Defer transitions:

- `source_readiness -> deferred`
  - source exists but is not yet worth consuming

Failure transitions:

- `source_readiness -> failed`
  - source is known-bad and should not be retried as-is

### Station 3: `signal_extraction`

Purpose:

- produce machine-readable evidence from the source

Entry criteria:

- source readiness passed
- media input is available when required

Exit criteria:

- at least one supported sidecar/evidence artifact is produced
- produced artifacts are attributable to the current order

Primary artifact proofs:

- runtime analysis sidecar
- proxy scan sidecar
- visual/audio analysis artifacts where applicable

Success transition:

- `signal_extraction -> candidate_generation`

Block transitions:

- `signal_extraction -> blocked`
  - extraction path requires missing external evidence
  - game-specific signal support is insufficient

Failure transitions:

- `signal_extraction -> failed`
  - extraction attempted and no usable evidence artifact was produced

Defer transitions:

- `signal_extraction -> deferred`
  - extraction is intentionally skipped pending a higher-priority source

### Station 4: `candidate_generation`

Purpose:

- turn extracted evidence into candidate highlight claims

Entry criteria:

- at least one usable signal artifact exists

Exit criteria:

- fused candidate artifact exists
- or an explicit “no viable candidate produced” result exists

Primary artifact proofs:

- fused analysis sidecar
- candidate lifecycle rows

Success transition:

- `candidate_generation -> editorial_selection`

Block transitions:

- `candidate_generation -> blocked`
  - fusion rules are missing or invalid
  - upstream evidence is present but insufficiently structured for candidate generation

Failure transitions:

- `candidate_generation -> failed`
  - candidate generation runs but produces no defensible candidate for the order

### Station 5: `editorial_selection`

Purpose:

- decide whether any candidate satisfies the work order

Entry criteria:

- candidate-generation artifacts exist

Exit criteria:

- one candidate is selected
- or the order is explicitly rejected/revised due to no acceptable candidate

Primary artifact proofs:

- review session artifacts
- applied review outcomes
- selected highlight candidate state

Success transition:

- `editorial_selection -> edit_packaging`

Review transitions:

- `editorial_selection -> review_required`
  - human/editor judgment is required before selecting a candidate

Block transitions:

- `editorial_selection -> blocked`
  - the order cannot continue without editorial input that the current worker cannot supply

Failure transitions:

- `editorial_selection -> failed`
  - reviewed candidates are all rejected and the order does not justify revision

Retry transitions:

- `editorial_selection -> signal_extraction`
  - only if the order explicitly requires another evidence pass instead of candidate rejection

### Station 6: `edit_packaging`

Purpose:

- convert the selected candidate into deliverable-ready local artifacts

Entry criteria:

- one candidate is selected for the order

Exit criteria:

- highlight selection manifest exists
- export batch exists or equivalent local packaging artifact exists

Primary artifact proofs:

- highlight selection manifest
- highlight export batch

Success transition:

- `edit_packaging -> approval`

Block transitions:

- `edit_packaging -> blocked`
  - selected candidate exists but packaging cannot proceed due to missing export support

Failure transitions:

- `edit_packaging -> failed`
  - packaging attempted and no valid local delivery artifact was produced

### Station 7: `approval`

Purpose:

- enforce editorial and permission boundaries before release or posting

Entry criteria:

- deliverable-ready local packaging exists

Exit criteria:

- approved for local completion
- approved for posting
- rejected
- sent back for revision

Primary proof:

- explicit approval decision
- explicit rejection/revise decision

Success transitions:

- `approval -> delivery`

Revision transitions:

- `approval -> editorial_selection`
  - if the issue is candidate/editorial choice
- `approval -> edit_packaging`
  - if the issue is packaging only

Block transitions:

- `approval -> approval_required`
  - if a privileged human decision is required

Failure transitions:

- `approval -> failed`
  - if the order is formally rejected with no retry path

### Station 8: `delivery`

Purpose:

- materialize the final completion state for the order

Entry criteria:

- approval exists for the intended delivery mode

Exit criteria:

- local export artifact exists
- or downstream post ledger exists if posting is part of the order

Primary artifact proofs:

- local export artifact
- posted highlight ledger

Success transitions:

- `delivery -> post_completion`
  - if posting/metrics are in scope
- `delivery -> completed_local`
  - if local delivery is the terminal requirement

Block transitions:

- `delivery -> blocked`
  - external platform action is required but unavailable

Failure transitions:

- `delivery -> failed`
  - delivery action attempted and no accepted completion artifact exists

### Station 9: `post_completion`

Purpose:

- record downstream outcomes and close the operational record

Entry criteria:

- delivery completed

Exit criteria:

- downstream lineage is attached if available
- metrics snapshot is attached if available
- final work-order completion state is explicit

Primary artifact proofs:

- post ledger
- posted metrics snapshot

Success transitions:

- `post_completion -> completed_posted`

Defer transitions:

- `post_completion -> deferred`
  - if metrics follow-up is intentionally pending

## Terminal States

For `short_clip_edit_order_v1`, terminal work-order outcomes should be:

- `completed_local`
- `completed_posted`
- `failed`
- `deferred`

Meaning:

- `completed_local`: local deliverable completed, no posting obligation
- `completed_posted`: downstream posting state completed enough to close the order
- `failed`: the order should not continue without a new order or a major reset
- `deferred`: the order remains real but is intentionally parked

## Retryable States

Retryable states should be explicit.

Recommended retryable stations:

- `source_readiness`
- `signal_extraction`
- `candidate_generation`
- `editorial_selection`
- `edit_packaging`
- `approval`
- `delivery`

But retries should always preserve prior artifacts and reasons.

Retries should not overwrite history silently.

## Human Decision Gates

The first mandatory human gates should remain:

1. candidate/editorial acceptance when machine evidence is insufficient
2. final approval before posting or any permission-gated external action

The system may automate local deterministic stations.
It should not silently automate permissioned release decisions.

## Mapping To Existing Repo Surfaces

### `source_readiness`

Current repo surfaces:

- pack validation
- onboarding draft state
- source/media resolution

### `signal_extraction`

Current repo surfaces:

- runtime analysis
- proxy scan
- visual/audio sidecars

### `candidate_generation`

Current repo surfaces:

- fused analysis
- candidate lifecycle registry state

### `editorial_selection`

Current repo surfaces:

- runtime review bridge
- fused review bridge
- highlight review app

### `edit_packaging`

Current repo surfaces:

- highlight selection export
- highlight export batch

### `delivery`

Current repo surfaces:

- export artifacts
- post ledger

### `post_completion`

Current repo surfaces:

- posted metrics snapshot
- downstream analytics

## Relationship To Workflow Runs

`workflow_run` should remain an execution/batching artifact.

For now:

- work order = business or production unit of work
- workflow run = execution grouping within one or more stations

Recommended rule:

- a work order may create many workflow runs
- workflow runs should not become a substitute for the work-order record

## Minimal Implementation Guidance

The first implementation should avoid building a large new subsystem.

Recommended first implementation shape:

1. define a durable work-order manifest schema
2. define station and status enums
3. map existing artifact creation steps to station proofs
4. attach work-order references to workflow runs and key downstream artifacts only where necessary

Do not start by rewriting detector or registry internals.

## Audit Questions

When auditing one work order, the manager should be able to answer:

1. what station is this order in?
2. what artifact proves that?
3. what is it waiting on?
4. who or what owns the next action?
5. what closes this order?

If the system cannot answer those, the work-order model is not implemented well enough.

## Recommendation

Proceed with this transition model as the next design layer.

After this, the next useful design artifact should be:

- a concrete field-level manifest contract for `short_clip_edit_order_v1`

That should specify:

- schema version
- required fields
- station/status enums
- allowed transitions
- artifact link fields
- block/defer/failure reason fields
