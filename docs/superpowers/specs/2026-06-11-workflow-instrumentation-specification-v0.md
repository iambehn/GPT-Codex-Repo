# Workflow Instrumentation Specification v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Define the minimum workflow-emission contract required for real workflow surfaces to populate the Transition Outcome Ledger and Attribution Architecture without retrospective reconstruction.

This artifact states:

- what every instrumented workflow must emit when a transition outcome is recorded
- which identity and attribution fields are required
- which inspection and outcome fields are required
- what the current onboarding workflow family is missing

## Scope

This spec defines:

- a compact workflow instrumentation contract
- required identity fields
- required attribution fields
- required inspection fields
- required outcome-emission fields
- minimum per-surface deltas for the current onboarding workflow family

This spec does not define:

- control-plane redesign
- qualification policy
- planning or scheduling policy
- staffing policy
- inventory or resource allocation
- a second history schema parallel to the existing event-capture and ledger baseline

## Guardrails

- Workflow instrumentation exists to emit structured evidence at the moment work happens.
- Instrumentation must preserve truth even when identity is weak or unknown.
- Instrumentation must support direct event-capture and ledger population.
- Instrumentation must not require later semantic archaeology to recover core event facts.
- Instrumentation must extend existing workflow surfaces where possible rather than inventing parallel workflow files.

This artifact must stay consistent with:

- [2026-06-11-operational-observation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-operational-observation-report-v0.md)
- [2026-06-11-attribution-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-validation-report-v0.md)
- [2026-06-11-transition-event-capture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-event-capture-v0.md)
- [2026-06-11-attribution-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-architecture-v0.md)
- [2026-06-11-transition-outcome-ledger-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-v0.md)

## Why This Artifact Exists

The recent validation sequence converged on one repeated result:

- the architecture is surviving
- the workflows are not emitting enough structured evidence

Observed pattern:

- control-plane architecture survived
- transferability survived
- qualification architecture survived
- ledger schema survived
- attribution architecture survived

The repeated failure mode is not ontology failure.

It is workflow-emission failure.

The current onboarding workflow demonstrated:

- real transition outcomes are recoverable
- identity and attribution are mostly weak
- inspection and outcome semantics often require reconstruction

That means the next missing layer is workflow instrumentation.

## Position In The Stack

This artifact sits at the boundary between workflow execution and the evidence plane:

- `Workflow`
- `Transition Event Capture`
- `Attribution`
- `Transition Outcome Ledger`
- `Qualification`

Interpretation:

- workflows produce work
- instrumentation emits structured evidence
- capture normalizes that evidence
- the ledger stores it
- later consumers use it

## Core Principle

Any workflow surface that records a transition outcome must emit enough information to answer:

- what happened
- when it happened
- who or what is being attributed
- what the inspection result was
- what evidence proves the outcome

If a workflow cannot emit those facts directly, it is under-instrumented.

## Minimum Instrumentation Contract

The minimum workflow instrumentation contract is the smallest set of persisted facts required for a workflow surface to support direct event capture.

### Required identity fields

- `subject`
- `subject_kind`

Meaning:

- `subject`
  - the attributed subject value aligned to the current subject vocabulary
- `subject_kind`
  - whether the subject is:
    - `explicit`
    - `deterministic`
    - `workflow_default`
    - `unknown`

For v0:

- `subject` should use:
  - `codex_structured_worker`
  - `system_validator`
  - `human_editor`
  - `manager_approver`
  - `unknown_subject`

`subject_kind` is a workflow-emission convenience field.

It exists so the workflow surface can state whether the recorded subject is explicit or inferred without forcing later guesswork.

### Required attribution fields

- `subject_attribution_basis`
- `evidence_reference`

Meaning:

- `subject_attribution_basis`
  - bounded source for the attribution claim
- `evidence_reference`
  - persisted artifact or surface proving the event occurred

Bounded `subject_attribution_basis` values:

- `explicit_actor_argument`
- `explicit_review_record`
- `explicit_approval_record`
- `deterministic_system_step`
- `workflow_owned_default`
- `unknown_at_capture`

### Required inspection fields

- `inspection_result`
- `failure_family`
- `rescue_required`

Meaning:

- `inspection_result`
  - `pass`, `reject`, `rework_required`, `blocked`, `archive`, or `not_applicable`
- `failure_family`
  - `requirement`, `source`, `execution`, `inspection`, `support`, `control`, `lifecycle`, or `none`
- `rescue_required`
  - whether bounded correction or rescue was needed

### Required outcome-emission fields

- `event_id`
- `timestamp`
- `transition_id`
- `input_state`
- `output_state`

Meaning:

- `event_id`
  - unique transition-outcome occurrence identifier
- `timestamp`
  - actual event or write-time timestamp
- `transition_id`
  - canonical transition identifier
- `input_state`
  - canonical pre-transition state
- `output_state`
  - canonical post-transition state

### Recommended context fields

- `artifact_ref`
- `workflow_ref`
- `capture_source`
- `notes`

These are not the core bottleneck, but they make the emitted evidence inspectable.

## Instrumentation Rule Set

### Rule 1: instrument the write surface, not just the batch summary

Evidence should be emitted where the outcome is actually recorded:

- source-fetch write
- binding-accept write
- publish-readiness decision write
- review decision write

Do not rely only on later rollups such as:

- status summaries
- derived workflow snapshots
- post-hoc validators

### Rule 2: one transition attempt should produce one event-ready write

The workflow may store richer local state, but it must be possible to derive one transition-outcome event from one concrete outcome write without ambiguity.

### Rule 3: identity weakness must be persisted, not hidden

If the workflow knows the subject only weakly:

- emit `subject = unknown_subject` or the weakest defensible subject
- emit `subject_kind = workflow_default` or `unknown`
- emit bounded `subject_attribution_basis`

Do not promote weak workflow ownership into explicit reviewer identity.

### Rule 4: inspection outcome must be explicit at write time

The workflow surface should not require later interpretation of generic status labels to recover:

- `pass`
- `rework_required`
- `reject`

If a status implies one of those outcomes, the workflow should emit the normalized inspection result directly.

### Rule 5: append-only event history remains the target

Instrumentation should support event appends, not row mutation as the only history surface.

Correction should create:

- new outcome writes
- or explicit superseding writes

not silent mutation without durable event context.

## Current Workflow Family Diagnosis

Validated workflow family:

- onboarding draft family under `assets/games/<game>/drafts/onboarding/...`

Observed current surfaces:

- `catalog/source_fetch_log.csv`
- `catalog/bindings.csv`
- `catalog/qa_queue.csv`
- `manifests/onboarding_state.json`

Observed problem:

- these surfaces persist real operational state
- but they do not persist enough event-grade identity and outcome fields to avoid later reconstruction

## Per-Surface Delta Requirements

### 1. `catalog/source_fetch_log.csv`

Current fields:

- `content_type`
- `section_count`
- `source_page_url`
- `source_role`
- `source_title`
- `status`

Missing required instrumentation:

- `event_id`
- `timestamp`
- `subject`
- `subject_kind`
- `subject_attribution_basis`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`

Minimum acceptable v0 improvement:

- add:
  - `fetched_at`
  - `subject`
  - `subject_kind`
  - `subject_attribution_basis`
  - `transition_id`
  - `input_state`
  - `output_state`

Interpretation:

- source fetch is currently structurally attributable
- but it cannot become strong evidence until the workflow emits explicit event-grade fields

### 2. `catalog/bindings.csv`

Current useful fields:

- `status`
- `derived_row_review_decision`
- `derived_row_review_status`
- `derived_row_reviewed_at`
- `review_notes`

Missing required instrumentation:

- explicit reviewer identity
- `subject_kind`
- bounded `subject_attribution_basis`
- canonical `transition_id`
- canonical `input_state`
- canonical `output_state`
- normalized `inspection_result`
- normalized `failure_family`
- `rescue_required`
- explicit `event_id`
- explicit `evidence_reference`

Minimum acceptable v0 improvement:

- add:
  - `reviewed_by_subject`
  - `subject_kind`
  - `subject_attribution_basis`
  - `transition_id`
  - `input_state`
  - `output_state`
  - `inspection_result`
  - `failure_family`
  - `rescue_required`
  - `event_id`

Interpretation:

- this is the highest-leverage workflow surface
- the `112` accepted binding outcomes remain weakly attributable mainly because reviewer identity is not persisted

### 3. `manifests/onboarding_state.json`

Current useful fields:

- `phase_status`
- `updated_at`

Missing required instrumentation for readiness outcomes:

- explicit publish-readiness event row
- subject identity
- subject_kind
- subject_attribution_basis
- canonical `transition_id`
- canonical `input_state`
- canonical `output_state`
- normalized `inspection_result`
- normalized `failure_family`
- `rescue_required`
- `evidence_reference`

Minimum acceptable v0 improvement:

- do not overload `onboarding_state.json` with event history
- instead emit a small explicit readiness outcome record alongside the readiness write

Interpretation:

- workflow state alone is not sufficient instrumentation
- readiness outcomes need direct event emission

### 4. `catalog/qa_queue.csv`

Current useful fields:

- `item_type`
- `reason`
- `status`

Current limitation:

- this surface explains unresolved work pressure
- it does not represent canonical transition outcomes by itself

Minimum acceptable v0 role:

- keep as supporting evidence
- do not treat it as the primary transition event surface unless it is extended with canonical event fields

## Required Instrumentation Outcome

A workflow is instrumented enough for the evidence plane when:

- transition outcomes no longer need semantic reconstruction to get canonical `transition_id`
- inspection results no longer need reconstruction from status prose
- subject attribution no longer depends on workflow ownership alone
- event timestamps are emitted at the time the outcome is written
- the capture layer can populate a ledger row directly from the workflow emission surface

## Minimum v0 Emission Profiles

### Production outcome profile

Must emit:

- `event_id`
- `timestamp`
- `subject`
- `subject_kind`
- `subject_attribution_basis`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`

### Inspection outcome profile

Must emit the production profile plus:

- explicit normalized inspection result at decision time

### Deterministic system-step profile

May use:

- `subject = system_validator` or `codex_structured_worker`
- `subject_kind = deterministic`
- `subject_attribution_basis = deterministic_system_step`

But still must emit:

- `timestamp`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `evidence_reference`

## Non-Goals

Do not add in v0:

- planner metrics
- capacity metrics
- staffing assignment logic
- inventory movement
- cost accounting
- probabilistic identity resolution
- qualification promotion rules

## Explicit Deferrals

These remain deferred beyond Workflow Instrumentation Specification v0:

- planner-facing aggregation
  - future planning doctrine
- resource-cost capture
  - future resource model or operations analytics
- inventory-aware event semantics
  - future inventory model
- qualification policy updates based on captured evidence
  - future qualification evidence policy

## Minimum v0 Outcome

Workflow Instrumentation Specification v0 is successful if it makes these points explicit:

- workflows must emit identity and outcome data at write time
- explicit reviewer identity is the highest-leverage missing field in the current workflow family
- deterministic system-owned steps can be instrumented now
- workflow-owned default attribution must remain weak
- readiness outcomes need explicit event emission, not just state snapshots
- the current bottleneck is evidence generation, not evidence interpretation

## Recommended Next Pressure Test

Apply this instrumentation contract to one concrete workflow surface first:

- `catalog/bindings.csv` acceptance writes

and ask:

- can accepted binding writes persist explicit reviewer identity?
- can they emit canonical transition and inspection fields directly?
- can the resulting write populate one ledger-grade event row without reconstruction?
