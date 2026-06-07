# Video Editing Work Order Model Design

Date: 2026-06-08
Status: draft
Owner: Codex

## Objective

Define a first-pass operational model that reframes the gameplay video editing pipeline as a work-order system.

The goal is not to replace the current repo architecture.

The goal is to:

- preserve the existing evidence, review, export, and registry layers
- add one canonical operational object above them
- let the system reason about real editing work as routed, auditable tasks

## Why This Change

The current repo is strong at:

- generating and validating artifacts
- preserving provenance
- routing review
- tracking registry-backed workflow state
- producing export and post-delivery records

The current repo is weaker at expressing:

- what exact customer-facing or production-facing job is being processed
- what station currently owns that job
- what must happen before the job is done
- what is blocked versus merely incomplete

This design addresses that gap by introducing a work-order model.

## Design Intent

This design treats the pipeline like a production system processing work orders.

The analogy is:

- the business receives editing work
- a video editor node is the first specialized worker
- Codex and the surrounding pipeline supply the support system that keeps that worker productive
- each editing task becomes a work order that moves through stations

This means the pipeline should be optimized around completing work orders, not just around emitting detections.

## Core Principle

Detection, fusion, review, export, and posting are not the top-level system.

They are stations inside the top-level system.

The top-level system is:

- intake work
- validate readiness
- process tasks
- route exceptions
- produce deliverables
- record outcomes

## Smallest Useful Order Type

The first canonical order type should be:

- `short_clip_edit_order_v1`

Definition:

- one order represents one request to produce one publishable short-form FPS gaming clip from source gameplay media

This is the best first order because it overlaps most cleanly with the current repo:

- FPS-gaming focus already exists
- highlight detection is already present
- review and export surfaces already exist
- publish and post-ledger boundaries already exist

## Canonical Work Order

Each work order should be represented by one durable operational record.

Proposed fields:

```yaml
work_order_id: stable id
order_type: short_clip_edit_order_v1
customer_type: internal_inventory | external_client | speculative_content
source_mode: clip | vod | batch_vod
game: call_of_duty | marvel_rivals | other
source_asset_refs:
  - source media path or external reference
requested_deliverable:
  platform: tiktok | youtube_shorts | instagram_reels
  format: vertical_short
  target_count: 1
editorial_intent:
  style_profile: fps_highlight_v1
  hook_requirement: required | preferred | advisory
current_station: intake
station_status: draft | ready | in_progress | blocked | review_required | approval_required | completed | failed | deferred
priority: low | normal | high
blocking_reason: null
required_inputs:
  - source media
  - valid game pack
  - runnable signal extraction path
produced_artifacts: []
approvals:
  human_review_required: true
  publish_approval_required: true
completion_criteria:
  - review-approved selected clip exists
  - export artifact exists
  - if posting is enabled, post ledger exists
failure_reason: null
created_at: timestamp
updated_at: timestamp
```

## Stations

The first station model for `short_clip_edit_order_v1` should be:

1. `intake`
2. `source_readiness`
3. `signal_extraction`
4. `candidate_generation`
5. `editorial_selection`
6. `edit_packaging`
7. `approval`
8. `delivery`
9. `post_completion`

### 1. Intake

Purpose:

- register the job
- identify the requested output
- bind source assets and order metadata

Inputs:

- source reference
- game
- deliverable intent

Outputs:

- work-order record exists
- source refs are attached

### 2. Source Readiness

Purpose:

- verify the source is processable
- verify required pack/support exists
- fail early if the system cannot responsibly continue

Inputs:

- work order
- source media
- game pack

Outputs:

- readiness pass
- or explicit block reason

### 3. Signal Extraction

Purpose:

- produce raw machine evidence from the source

Inputs:

- media
- game pack

Outputs:

- runtime, proxy, visual, audio, or related sidecars

### 4. Candidate Generation

Purpose:

- turn extracted evidence into candidate highlight claims

Inputs:

- sidecars
- fusion rules

Outputs:

- candidate windows
- fused events

### 5. Editorial Selection

Purpose:

- decide which candidate satisfies the work order

Inputs:

- candidate outputs
- review signals
- human/editor decisions where needed

Outputs:

- selected candidate
- rejected candidates
- revise/blocked outcome if no candidate is acceptable

### 6. Edit Packaging

Purpose:

- generate deliverable-ready artifacts for the selected clip

Inputs:

- selected candidate
- export rules

Outputs:

- highlight selection manifest
- export batch
- later: subtitle/caption/crop metadata

### 7. Approval

Purpose:

- enforce editorial and permission gates

Inputs:

- packaged output
- review findings

Outputs:

- approved
- rejected
- revise

### 8. Delivery

Purpose:

- create the final local or external delivery result

Inputs:

- approved export

Outputs:

- local export artifact
- or post ledger if publication is performed

### 9. Post Completion

Purpose:

- record downstream outcomes and close the order cleanly

Inputs:

- post ledger
- posted metrics

Outputs:

- completed operational history

## Order Status Model

Order-level status should be separate from candidate-level status.

Proposed order statuses:

- `draft`
- `ready`
- `running`
- `blocked`
- `awaiting_review`
- `awaiting_approval`
- `completed_local`
- `completed_posted`
- `failed`
- `deferred`

Candidate lifecycle remains subordinate evidence.

A work order may contain:

- zero candidates
- many candidates
- one selected deliverable

## Mapping To Current Repo

The current repo already contains most of the evidence and execution surfaces required by this model.

### Current repo surfaces that map cleanly

`intake` / `source_readiness`

- onboarding drafts
- game pack validation
- source resolution
- onboarding report / QA queue

`signal_extraction`

- runtime analysis
- proxy scanning
- visual/audio scanning

`candidate_generation`

- fusion analysis
- candidate lifecycle rows

`editorial_selection`

- runtime review bridge
- fused review bridge
- highlight review app

`edit_packaging`

- highlight selection export
- highlight export batch

`delivery`

- local export artifacts
- post ledger boundaries

`post_completion`

- posted metrics snapshots
- distribution/post-ledger analytics

`operational history`

- clip registry
- workflow runs
- operator run log

## Relationship Between Operational Layers

Proposed hierarchy:

1. `work_order`
2. `workflow_run`
3. artifacts and sidecars

Interpretation:

- one work order is the business/job unit
- one or more workflow runs may execute stations for that order
- each workflow run produces artifacts

This preserves the current workflow-run and artifact model while giving it a real operational parent.

## Role Model

### Video Editor Node

Primary responsibility:

- complete editorial work orders

Likely responsibilities:

- candidate acceptance/rejection
- revision decisions
- packaging or finishing judgments where needed

### Codex / Pipeline Architect

Primary responsibility:

- build and operate the support system that keeps work flowing

Responsibilities:

- validate inputs
- run deterministic stations
- maintain contracts and ledgers
- route review and exception states
- surface blockers clearly

### Manager

Primary responsibility:

- audit slow, blocked, or permission-gated work

Responsibilities:

- approve restricted actions
- audit stalled orders
- manage special cases
- handle transaction and document boundaries

## What This Design Does Not Do Yet

Non-goals for this first version:

- pricing model
- marketplace integration
- buyer acquisition
- client contract management
- generalized multi-worker scheduling
- replacing the current detector/fusion architecture
- broad cross-game ERP generalization

Those are downstream concerns.

The first task is to make one work-order type operationally real.

## Recommended First Scope

The first implementation scope should remain:

- one FPS highlight clip
- one order type
- one game first
- one clean station chain

Recommended initial binding:

- `short_clip_edit_order_v1`
- `call_of_duty`
- local completion first
- posting optional and explicitly gated

## Migration Strategy

Do not rewrite the existing repo around a new framework.

Instead:

1. define the work-order schema
2. map current surfaces to stations
3. identify missing station/state fields
4. extend existing workflow and registry contracts only where necessary
5. keep sidecars, registries, review sessions, export batches, and post ledgers as evidence surfaces

This is an additive operational layer, not a replacement architecture.

## Immediate Next Design Task

The next design artifact should be:

- a station-transition specification for `short_clip_edit_order_v1`

That spec should answer:

- what transitions are allowed
- what artifact(s) prove a transition
- what states require human/editor approval
- what states are terminal
- what states are retryable

## Open Questions

1. Is `short_clip_edit_order_v1` the first monetizable unit we want to optimize for?
2. Should one work order always target exactly one final clip in v1?
3. Which stations are allowed to auto-run without human intervention?
4. Which station should own clip rejection when no acceptable candidate exists?
5. Should `workflow_run` become explicitly subordinate to `work_order`, or remain only execution metadata until phase two?

## Recommendation

Proceed with this work-order framing.

Do not start with buyer/marketplace mechanics or large architectural rewrites.

The correct next move is:

- formalize one canonical work order
- formalize its station transitions
- then map the current repo’s workflow runs, registries, review flows, and export surfaces onto that model
