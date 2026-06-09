# Transition Catalog v0

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Define the first canonical transition catalog for the pipeline redesign control plane.

This artifact maps named input states to named output states and identifies the work-order class, transition type, completion event, inspection event, and failure classes associated with each transition.

## Scope

This spec defines:

- canonical transition rows
- transition types
- explicit state-to-state mappings
- transition-level completion and inspection surfaces
- transition-level failure-class placeholders

This spec does not define:

- routing policy across the full graph
- qualification levels
- inventory policy
- planning logic

## Guardrails

- A transition is a state change, not a task list.
- A work order is an authorized request to perform one or more transitions.
- The transition catalog must reference canonical states from [2026-06-10-state-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-state-catalog-v0.md).
- Transition rows should stay compact enough that routing, qualification, and inspection can attach to them later without changing their identity.
- This is a v0 control-plane artifact, not a runtime schema change.

## Why This Artifact Exists

The redesign already has:

- a lane-oriented strategy layer
- a work-order reframing
- a canonical state foundation

The next missing layer is the explicit catalog of state changes the control plane is allowed to perform.

Without that layer:

- routing remains descriptive
- inspection targets remain implicit
- failure attribution remains loose
- qualification cannot attach to stable transition objects

This v0 catalog is built on the corrected terminal split in the state catalog:

- `completed_local`
- `completed_posted`

That split replaced the earlier single `published_asset` terminal state because local completion and externally posted completion must remain distinct before routing and qualification attach to transitions.

## Transition Schema

Each transition row in v0 should declare:

- `transition_id`
- `transition_name`
- `transition_type`
- `input_state`
- `output_state`
- `triggering_work_order_class`
- `completion_event`
- `inspection_event`
- `allowed_next_transitions`
- `known_failure_classes`

## Field Definitions

### `transition_id`

Stable identifier for the transition record.

### `transition_name`

Human-readable name for the transition.

### `transition_type`

One of:

- `production`
- `inspection`
- `control`
- `archive`

Meaning:

- `production`: normal artifact-progress transition
- `inspection`: review, approval, or acceptance transition
- `control`: non-happy-path handling transition such as blocking
- `archive`: retirement transition into archived state

### `input_state`

Canonical source state for the transition.

### `output_state`

Canonical target state for the transition.

### `triggering_work_order_class`

The work-order class most directly responsible for requesting the transition.

### `completion_event`

What observable event means the transition finished from an execution standpoint.

### `inspection_event`

What observable review, validation, or approval event confirms that the target state was actually reached.

### `allowed_next_transitions`

The transition IDs that may follow this transition in v0.

### `known_failure_classes`

Current high-level failure families most likely to prevent successful completion of this transition.

## Transition Rows

| transition_id | transition_name | transition_type | input_state | output_state | triggering_work_order_class | completion_event | inspection_event | allowed_next_transitions | known_failure_classes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TRANS-001 | raw_vod_to_review_pack_ready | production | `raw_vod` | `review_pack_ready` | `vod_to_review_pack` | review-pack artifact created and attributable to the order | review-pack readiness check passes | `TRANS-002`, `TRANS-003`, `TRANS-004`, `TRANS-005` | `source`, `support`, `execution` |
| TRANS-002 | review_pack_ready_to_review_pack_approved | inspection | `review_pack_ready` | `review_pack_approved` | `review_pack_to_approved_clips` | review decision recorded as accepted | review-pack approval recorded | `TRANS-006`, `TRANS-004`, `TRANS-005` | `inspection`, `requirement`, `control` |
| TRANS-003 | review_pack_ready_to_review_pack_rejected | inspection | `review_pack_ready` | `review_pack_rejected` | `review_pack_to_approved_clips` | review decision recorded as rejected | rejection decision recorded | `TRANS-015`, `TRANS-010`, `TRANS-004`, `TRANS-005` | `inspection`, `requirement`, `control` |
| TRANS-004 | review_pack_ready_to_blocked | control | `review_pack_ready` | `blocked` | `vod_to_review_pack` | blocking condition recorded against the active artifact | manager or operator confirms the block reason | state-specific unblock path defined later | `support`, `control`, `requirement` |
| TRANS-005 | review_pack_ready_to_archived | archive | `review_pack_ready` | `archived` | `vod_to_review_pack` | archive decision recorded | archive authorization recorded | none | `lifecycle`, `requirement` |
| TRANS-006 | review_pack_approved_to_approved_clips_ready | production | `review_pack_approved` | `approved_clips_ready` | `review_pack_to_approved_clips` | approved-clip set created and attributable | approved-clip set validation passes | `TRANS-007`, `TRANS-008`, `TRANS-009` | `execution`, `inspection`, `support` |
| TRANS-007 | approved_clips_ready_to_platform_package_ready | production | `approved_clips_ready` | `platform_package_ready` | `approved_clips_to_platform_package` | platform package artifact created | package readiness review passes | `TRANS-011`, `TRANS-012`, `TRANS-008`, `TRANS-009` | `execution`, `support`, `inspection` |
| TRANS-008 | approved_clips_ready_to_blocked | control | `approved_clips_ready` | `blocked` | `approved_clips_to_platform_package` | blocking condition recorded against the package path | manager or operator confirms the block reason | state-specific unblock path defined later | `support`, `control`, `requirement` |
| TRANS-009 | approved_clips_ready_to_archived | archive | `approved_clips_ready` | `archived` | `approved_clips_to_platform_package` | archive decision recorded | archive authorization recorded | none | `lifecycle`, `control` |
| TRANS-010 | review_pack_rejected_to_archived | archive | `review_pack_rejected` | `archived` | `review_pack_to_approved_clips` | rejected artifact retired from active flow | archive authorization recorded | none | `lifecycle`, `inspection` |
| TRANS-011 | platform_package_ready_to_completed_local | inspection | `platform_package_ready` | `completed_local` | `platform_package_to_local_delivery` | local delivery artifact accepted | final local-delivery inspection passes | `TRANS-013` | `inspection`, `control`, `support` |
| TRANS-012 | platform_package_ready_to_completed_posted | inspection | `platform_package_ready` | `completed_posted` | `platform_package_to_published_post` | posting action succeeds and is recorded | final publish inspection passes | `TRANS-014` | `inspection`, `control`, `support` |
| TRANS-013 | completed_local_to_archived | archive | `completed_local` | `archived` | `delivery_lifecycle` | local artifact retired from active flow | archive authorization recorded | none | `lifecycle` |
| TRANS-014 | completed_posted_to_archived | archive | `completed_posted` | `archived` | `completed_posted_lifecycle` | posted artifact retired from active flow | archive authorization recorded | none | `lifecycle` |
| TRANS-015 | review_pack_rejected_to_review_pack_ready | production | `review_pack_rejected` | `review_pack_ready` | `review_pack_rework` | revised review-pack artifact created and attributable | review-pack readiness check passes | `TRANS-002`, `TRANS-003`, `TRANS-004`, `TRANS-005` | `execution`, `requirement`, `support` |

## Transition-Type Notes

### Production transitions

These transitions represent normal artifact progression toward the intended work-product outcome.

### Inspection transitions

These transitions represent acceptance, approval, or review decisions that validate whether a target state was actually reached.

### Control transitions

These transitions represent non-happy-path posture changes, not productive advancement.

`blocked` is a non-terminal control state.

Its re-entry behavior is intentionally deferred to routing architecture rather than encoded as generic unblock transitions in v0.

### Archive transitions

These transitions represent retirement from active progression.

## Known Gaps Left For Later Layers

This v0 catalog intentionally does not yet define:

- how blocked artifacts return to a specific production state
- transition coverage for `invalid_source`
- whether review-pack rejection should support transition paths beyond the current rework and archive options
- transition-specific qualification thresholds
- transition-specific support assets
- routing priority or scheduling behavior

Those belong to later routing, qualification, and planning artifacts.

For `invalid_source`, transition coverage is deferred until source-intake states are modeled more explicitly.

## Immediate Follow-On Questions

The next review of this artifact should focus on:

1. whether any additional production states are needed before routing design
2. whether blocked and archive flows need more explicit re-entry transitions
3. whether the work-order class names are stable enough to anchor future qualification and inspection artifacts
