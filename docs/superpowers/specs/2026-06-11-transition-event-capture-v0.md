# Transition Event Capture v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Define the first minimal mechanism for generating subject-attributed transition outcome records at execution time so the system can populate the Transition Outcome Ledger without retrospective reconstruction.

This artifact states:

- when a transition event should be emitted
- what minimum execution-time information must be captured
- how capture records map into ledger rows
- how to preserve uncertainty without fabricating attribution or timestamps

## Scope

This spec defines:

- a minimal transition event capture model
- one execution-time event envelope
- emission rules for production, inspection, control, and source-intake transitions
- append-only write behavior into the ledger surface

This spec does not define:

- planning logic
- inventory logic
- staffing logic
- resource allocation
- qualification promotion policy

## Guardrails

- Event capture exists to record what happened when work happened.
- Event capture must not require later semantic archaeology to recover core event facts.
- Event capture should preserve uncertainty explicitly rather than inventing false precision.
- Event capture should emit ledger-compatible rows, not a second history schema.
- The capture layer must remain smaller than the ledger consumer layers that depend on it.

This capture layer must stay consistent with:

- [2026-06-11-transition-outcome-ledger-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-v0.md)
- [2026-06-11-transition-outcome-ledger-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-validation-report-v0.md)
- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)
- [2026-06-10-transition-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-transition-catalog-v0.md)

## Why This Artifact Exists

The transition outcome ledger and its validation showed:

- the ledger schema is viable
- the main missing information is observational, not structural

Specifically, the validation found weak or missing:

- subject attribution
- event timestamps for some upstream and inspection outcomes
- direct persistence of inspection outcomes as event rows

That means the next missing layer is not more model design.

It is the execution-time mechanism that turns:

- transition execution
- inspection result
- failure attribution

into durable history.

## Core Principle

For v0, every meaningful transition execution should have a chance to emit one append-only event record at the moment the system knows enough to say:

- which transition happened
- what state boundary it crossed
- what the result was
- who or what is being attributed
- what evidence proves it

## Position In The Stack

This artifact sits between execution-time workflow logic and the ledger:

- `Source`
- `State`
- `Transition`
- `Inspection`
- `Transition Event Capture`
- `Transition Outcome Ledger`
- `Failure`
- `Routing`
- `Qualification`

Interpretation:

- capture creates event records
- the ledger stores them
- later layers consume them

## Capture Model

Transition Event Capture v0 uses one minimal execution-time envelope that is ledger-compatible.

Preferred model:

- capture one event envelope
- normalize it directly into one ledger row
- append that row

Do not create:

- one raw event schema plus one unrelated ledger schema

That would recreate the same observational gap one layer earlier.

## Event Capture Envelope

The execution-time event envelope should contain the ledger-required row fields plus the minimum metadata needed to write them reliably.

Required capture fields:

- `event_id`
- `timestamp`
- `subject`
- `subject_attribution_basis`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`
- `notes`

Recommended capture fields:

- `artifact_ref`
- `workflow_ref`
- `outcome_status`
- `capture_source`

## Capture Field Meaning

### `event_id`

Generated at execution time and never reused.

This must identify one event occurrence, not one transition definition.

### `timestamp`

Execution-time observed timestamp.

Use:

- actual event timestamp when the workflow surface knows it
- capture-time timestamp when the event is being emitted by the system at the moment of execution

Do not use:

- reconstructed or backfilled pseudo-time when the event is only being inferred later

That is a ledger-validation finding and should remain explicit.

### `subject`

Execution-time attributed subject.

Allowed values in v0:

- `codex_structured_worker`
- `system_validator`
- `human_editor`
- `manager_approver`
- `unknown_subject`

### `subject_attribution_basis`

Execution-time explanation of why the recorded subject is believed to be correct.

Use short bounded values such as:

- `explicit_actor_argument`
- `deterministic_system_step`
- `explicit_review_record`
- `explicit_approval_record`
- `workflow_owned_default`
- `unknown_at_capture`

### `transition_id`

Canonical transition being emitted.

Must come from:

- committed transition catalog
- source-intake transition catalog

Do not emit ad hoc transition names in capture.

### `input_state`

State before the transition fired.

### `output_state`

State after the transition outcome was determined.

### `inspection_result`

Bounded execution-time or inspection-time result:

- `pass`
- `reject`
- `rework_required`
- `blocked`
- `archive`
- `not_applicable`

### `failure_family`

Failure family at execution time, if any:

- `requirement`
- `source`
- `execution`
- `inspection`
- `support`
- `control`
- `lifecycle`
- `none`

### `rescue_required`

Boolean flag that tells later consumers whether bounded recovery was needed.

### `evidence_reference`

Reference to the artifact or persisted surface proving the event occurred.

This must be available at execution time or be derivable immediately from the execution surface.

### `notes`

Compact context that explains caveats without replacing structured fields.

### `capture_source`

Optional field identifying which execution surface emitted the row, such as:

- `onboarding_publish_readiness`
- `derived_row_review`
- `highlight_review_app`
- `source_intake_adapter`

This is recommended for debugging and capture audits, but not required.

## Emission Rules

### Rule 1: emit on meaningful state boundary completion

Emit when the system has enough information to say a canonical transition outcome actually happened.

Examples:

- source fetch succeeded
- accepted binding was recorded
- publish-readiness concluded `needs_binding_review`
- review decision was saved

### Rule 2: do not emit speculative pre-events

Do not emit events for:

- planned transitions
- candidate next steps
- incomplete attempts where no outcome boundary is yet known

### Rule 3: append-only

Every attempt or outcome occurrence should append a new event row.

Never overwrite a previous event row.

Reason:

- qualification needs attempt history
- routing analysis needs repeated recoverable outcomes
- final outcome alone is not enough

### Rule 4: preserve uncertainty explicitly

If the subject is not recoverable at execution time:

- emit `unknown_subject`

If the failure family does not apply:

- emit `none`

If the transition does not require a substantive inspection event:

- emit `not_applicable`

### Rule 5: evidence reference is mandatory

No emitted event without an evidence reference.

If the execution surface cannot name the evidence it just produced or used, it should not emit a ledger-grade event yet.

## Minimal Capture Mechanism

Transition Event Capture v0 should stay implementation-light.

The minimal mechanism is:

1. execution surface produces or observes outcome
2. execution surface constructs one capture envelope
3. capture layer validates bounded fields
4. capture layer appends one ledger-compatible row

That is enough for v0.

Do not require:

- queue managers
- background planners
- scheduling engines
- inventory reconciliation

## Emission By Transition Class

### Production transitions

Emit when an attributable artifact or state advancement is completed.

Examples:

- `TRANS-001`
- `TRANS-006`
- `TRANS-007`
- `TRANS-015`
- `TRANS-018`
- `SITRANS-001`
- `SITRANS-004`
- `SITRANS-005`

### Inspection transitions

Emit when the inspection result is recorded, not merely anticipated.

Examples:

- `TRANS-002`
- `TRANS-003`
- `TRANS-016`
- `TRANS-017`
- `SITRANS-002`
- `SITRANS-003`

### Control transitions

Emit when the control decision is actually recorded.

Examples:

- `TRANS-004`
- `TRANS-008`
- `SITRANS-006`
- `SITRANS-007`
- `SITRANS-008`

### Archive transitions

Emit when retirement is explicitly recorded.

Examples:

- `TRANS-005`
- `TRANS-009`
- `TRANS-010`
- `TRANS-013`
- `TRANS-014`

## Execution-Time Attribution Strategy

The capture layer should prefer attribution sources in this order:

1. explicit actor argument from the workflow surface
2. explicit review or approval record
3. deterministic system-owned execution step
4. workflow-owned default subject
5. `unknown_subject`

This keeps attribution conservative without losing the event.

## Relationship To Existing Repo Surfaces

The repo already contains adjacent execution-time signals that should inform capture design:

- accepted binding rows store `derived_row_reviewed_at`
- accepted binding rows store review decisions and notes such as `auto-applied recommended candidate`
- review app surfaces already write `reviewed_at`
- append-only ledger patterns already exist elsewhere in the repo

Transition Event Capture v0 should reuse that posture:

- capture at write time
- append history
- preserve actor or reviewer context when the surface already has it

## Validation Rules For Capture

The capture layer should reject or downgrade rows when:

- `transition_id` is missing
- `timestamp` is missing
- `inspection_result` is outside the bounded vocabulary
- `failure_family` is outside the bounded vocabulary
- `evidence_reference` is missing

For `subject`:

- do not reject on unknown attribution
- instead emit `unknown_subject` with `subject_attribution_basis = unknown_at_capture`

## Non-Goals

Do not add in v0:

- automatic promotion of qualification levels
- duration or cost analytics
- queue delay measurement
- staffing assignment logic
- resource accounting
- inventory movement
- planning prioritization

## Explicit Deferrals

These are intentionally deferred beyond Transition Event Capture v0:

- capture aggregation and summarization
  - future ledger summary or analytics tools
- qualification update policy
  - future qualification evidence policy
- planning or capacity interpretation
  - future planning doctrine
- inventory-aware event semantics
  - future inventory model
- resource cost capture
  - future resource or operations analytics layer

## Minimum v0 Outcome

Transition Event Capture v0 is successful if it makes these points explicit:

- event capture happens at execution time, not through later reconstruction
- event capture produces ledger-compatible rows directly
- missing subject attribution does not block event capture
- timestamps should be recorded when the event happens or when the system records it, not fabricated later
- append-only history is the rule

## Recommended Next Pressure Test

Apply this capture model to one execution surface that already records partial event facts, such as:

- onboarding accepted binding writes
- publish-readiness result writes
- review-app approval writes

and ask:

- can that surface emit a ledger-grade event row at the moment the outcome is recorded?
- where is subject attribution still missing at execution time?
- where is timestamp capture still missing at execution time?
