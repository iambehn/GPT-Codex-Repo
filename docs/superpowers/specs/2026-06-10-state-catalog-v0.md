# State Catalog v0

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Define the first canonical state catalog for the pipeline redesign control plane.

This artifact establishes the state foundation that later transition, routing, inspection, failure, and qualification designs will attach to.

## Guardrails

- States describe artifact condition, not process steps.
- A state must answer: what condition is the artifact in now?
- Process verbs such as `generate_review_pack` or `approve_clips` belong in the transition or work-order layer, not the state layer.
- Production states and cross-artifact control states should remain distinct even when they share the same schema.
- This is a v0 control-plane artifact, not a runtime schema change.

## Why This Artifact Exists

The redesign is shifting from:

- pipeline stages
- task sequences

toward:

- state
- transition
- work order
- inspection
- qualification

That shift requires one small, canonical state catalog before the transition layer can be made explicit.

## State Schema

Each state record in v0 should declare:

- `state_id`
- `state_name`
- `state_scope`
- `artifact_type`
- `definition`
- `entry_condition`
- `exit_allowed_when`
- `valid_next_states`
- `inspection_required`
- `terminal_state`

## Field Definitions

### `state_id`

Stable identifier for the state record.

### `state_name`

Human-readable state name.

### `state_scope`

One of:

- `production`
- `cross_artifact_control`

### `artifact_type`

The primary artifact family the state applies to.

Examples:

- `source_media`
- `review_pack`
- `approved_clip_set`
- `platform_package`
- `completed_local_delivery`
- `completed_posted_asset`
- `generic_control`

### `definition`

What condition the artifact is in while this state is true.

### `entry_condition`

What must be true before the artifact may enter the state.

### `exit_allowed_when`

What must be true before the artifact may leave the state.

### `valid_next_states`

The currently allowed successor states from this state in v0.

### `inspection_required`

Whether the state requires explicit inspection or approval before it can be treated as valid.

Use:

- `none`
- `required`

### `terminal_state`

Whether the state is terminal for the current artifact path.

Use:

- `yes`
- `no`

## Production States

| state_id | state_name | state_scope | artifact_type | definition | entry_condition | exit_allowed_when | valid_next_states | inspection_required | terminal_state |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| STATE-001 | raw_vod | production | source_media | Source gameplay media exists in raw ingest form and has not yet produced a review-ready downstream artifact. | Source media is present, referenced, and accepted as a candidate input. | The source is either found invalid, blocked, archived, or used to produce a review-pack-ready artifact. | `review_pack_ready`, `invalid_source`, `blocked`, `archived` | none | no |
| STATE-002 | review_pack_ready | production | review_pack | A review pack exists in a form that is ready for structured review or approval. | Candidate extraction or packaging output exists and is complete enough to review. | The review pack is explicitly approved, rejected, blocked, or archived. | `review_pack_approved`, `review_pack_rejected`, `blocked`, `archived` | required | no |
| STATE-003 | review_pack_approved | production | review_pack | The review pack has passed its expected review gate and is accepted as valid downstream input. | A review-ready pack exists and the required inspection has passed. | The approved review pack is used to generate approved clips, blocked, or archived. | `approved_clips_ready`, `blocked`, `archived` | required | no |
| STATE-004 | review_pack_rejected | production | review_pack | The review pack has been inspected and explicitly not accepted as a valid downstream basis. | A review-ready pack exists and the required inspection has failed or been denied. | The rejected pack is archived or replaced by a new successful review-pack-ready artifact. | `archived`, `review_pack_ready` | required | no |
| STATE-005 | approved_clips_ready | production | approved_clip_set | A bounded set of approved clips exists and is ready for packaging or downstream finishing. | Approved clips are explicitly selected and available as downstream inputs. | The approved clip set is used to produce a platform package, blocked, or archived. | `platform_package_ready`, `blocked`, `archived` | required | no |
| STATE-006 | platform_package_ready | production | platform_package | A platform-specific package exists in a state that is ready for final delivery or posting inspection. | The selected clip set has been transformed into a bounded package for a target platform. | The package is completed locally, completed as a posted asset, blocked, or archived. | `completed_local`, `completed_posted`, `blocked`, `archived` | required | no |
| STATE-007 | completed_local | production | completed_local_delivery | Terminal production state for an export-ready or locally delivered package with no posting obligation. | A platform package has passed the required final inspection for local completion. | No further production-state transition is expected in this v0 model. | `archived` | required | yes |
| STATE-008 | completed_posted | production | completed_posted_asset | Terminal production state for an externally posted or published asset. | A platform package has passed the required final inspection for posted completion and the posting action has succeeded. | No further production-state transition is expected in this v0 model. | `archived` | required | yes |

## Cross-Artifact Control States

These states do not represent normal production progress. They represent handling posture that may apply across artifact families.

| state_id | state_name | state_scope | artifact_type | definition | entry_condition | exit_allowed_when | valid_next_states | inspection_required | terminal_state |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| STATE-101 | invalid_source | cross_artifact_control | generic_control | The artifact or source input is not valid for the intended workflow and should not advance through normal production states. | Validation, intake, or review shows the source is unsupported, unusable, or malformed for the intended path. | The invalid condition is corrected through explicit re-entry or the artifact is archived. | `raw_vod`, `archived` | required | no |
| STATE-102 | blocked | cross_artifact_control | generic_control | The artifact cannot currently progress because a dependency, decision, input, or capability is missing. | A normal production transition cannot continue without an external unblock condition. | The blocking condition is resolved through a context-specific routing decision or the artifact is archived. | `context_specific`, `archived` | required | no |
| STATE-103 | archived | cross_artifact_control | generic_control | The artifact is intentionally removed from active progression and preserved only for record, reference, or later reactivation policy. | The artifact is complete, superseded, rejected, invalid, or intentionally retired from active control-plane flow. | Re-entry is only allowed by an explicit future policy, not by default in v0. | none | required | yes |

## First Rules For Future Transition Design

The transition layer should inherit these rules:

1. Every transition must reference one canonical input state and one canonical output state.
2. A transition may target either a production state or a cross-artifact control state.
3. A transition must not invent a new state ad hoc when an existing state record already describes the resulting condition.
4. Inspection should validate whether the claimed output state was actually reached.
5. Failure attribution should explain why the intended output state was not reached.

## Deferred Re-Entry Note

For v0, `blocked` is intentionally modeled as a non-terminal control posture without broad built-in re-entry paths.

Its return to a production state is deferred to routing architecture so unblock behavior remains context-specific rather than implicitly global.

## Non-Goals

- This v0 catalog does not define transition IDs.
- This v0 catalog does not define routing policy.
- This v0 catalog does not define qualification levels.
- This v0 catalog does not define inventory policy.
- This v0 catalog does not mutate runtime code, manifests, or schemas yet.

## State Patch Note

This v0 catalog originally used `published_asset` as a single terminal production state.

That state was split into:

- `completed_local`
- `completed_posted`

Reason:

- the control plane needs to distinguish local/export completion from externally posted completion before transition design, otherwise terminal transitions become ambiguous.

## Immediate Follow-On Layer

The next structurally dependent layer is transition design built on these revised state records as explicit input and output anchors.
