# Transition Outcome Ledger v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Define the first compact transition-history artifact for the validated control-plane model.

This artifact states:

- what a transition outcome event is
- what fields must be captured for each event
- how subject attribution and evidence references should be recorded
- how the ledger can support qualification, failure analysis, routing analysis, and later planning without becoming those systems

## Scope

This spec defines:

- a transition outcome ledger schema
- one event-row contract
- subject-attributed evidence capture rules
- minimum aggregation expectations for later consumers

This spec does not define:

- qualification promotion rules
- failure-routing policy
- planning or scheduling policy
- staffing policy
- inventory or resource modeling

## Guardrails

- The ledger records what happened. It does not decide what should happen next.
- The ledger is a measurement surface, not a policy engine.
- The ledger must remain compact enough to append from real workflow execution without requiring future planning logic.
- Subject attribution should be explicit where known and conservative where unknown.
- The ledger should reference existing control-plane artifacts rather than duplicate them.

This ledger must stay consistent with:

- [2026-06-10-transition-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-transition-catalog-v0.md)
- [2026-06-10-inspection-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-inspection-architecture-v0.md)
- [2026-06-10-failure-taxonomy-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-failure-taxonomy-v0.md)
- [2026-06-10-routing-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-routing-architecture-v0.md)
- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)
- [2026-06-11-qualification-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-qualification-validation-report-v0.md)

## Why This Artifact Exists

The qualification validation report showed that the current architecture is ahead of the evidence.

The main missing infrastructure is not:

- more control logic
- more planning
- more inventory structure

It is:

- subject-attributed transition history
- compact transition outcome memory

Without that:

- qualification stays partly normative
- repeated failure patterns cannot be compared cleanly across transitions
- routing analysis remains anecdotal
- later planning would build on weak historical memory

## Core Principle

For v0, the transition outcome ledger should answer:

- which transition occurred
- who or what performed or authorized it
- what the inspection result was
- whether rescue or bounded correction was required
- what evidence proves that the recorded outcome actually happened

## Position In The Stack

This artifact sits after inspection and before later analytical consumers:

- `Source`
- `State`
- `Transition`
- `Inspection`
- `Transition Outcome Ledger`
- `Failure`
- `Routing`
- `Qualification`

Interpretation:

- `Failure`, `Routing`, and `Qualification` all consume ledger history
- the ledger does not replace those layers

## Ledger Shape

Use one compact JSON artifact shape aligned with existing repo ledger conventions.

Top-level fields:

- `schema_version`
- `generated_at`
- `ledger_id`
- `row_count`
- `source_artifact_family`
- `rows`

Suggested schema version:

- `transition_outcome_ledger_v1`

### Top-Level Field Meaning

#### `schema_version`

Versioned ledger contract identifier.

#### `generated_at`

UTC timestamp for ledger generation or emission.

#### `ledger_id`

Stable identifier for the ledger instance.

#### `row_count`

Number of event rows in the ledger.

#### `source_artifact_family`

Optional compact label describing the source workflow family that emitted the ledger, such as:

- `onboarding_draft`
- `accepted_clip_intake`
- `review_pack_work_order`

This is descriptive only. It must not replace explicit transition fields.

#### `rows`

Ordered event rows describing concrete transition outcomes.

## Event Row Contract

Each row should represent one observed transition outcome event.

Required fields:

- `event_id`
- `timestamp`
- `subject`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`
- `notes`

Recommended additional fields:

- `subject_attribution_basis`
- `artifact_ref`
- `workflow_ref`
- `outcome_status`

## Required Row Field Meaning

### `event_id`

Unique row identifier for this transition outcome event.

### `timestamp`

UTC timestamp for when the outcome was observed or recorded.

### `subject`

The attributed subject for the transition outcome.

Expected values should align with current qualification subjects when possible:

- `codex_structured_worker`
- `system_validator`
- `human_editor`
- `manager_approver`
- `unknown_subject`

`unknown_subject` is allowed in v0, but should be treated as evidence-thin.

### `transition_id`

Canonical transition identifier from the committed transition catalog or a documented upstream source-intake transition.

Examples:

- `TRANS-001`
- `TRANS-016`
- `SITRANS-005`

### `input_state`

Canonical source or control-plane state before the transition.

### `output_state`

Canonical source or control-plane state after the transition outcome.

### `inspection_result`

Compact recorded inspection outcome for the transition event.

Use this bounded set in v0:

- `pass`
- `reject`
- `rework_required`
- `blocked`
- `archive`
- `not_applicable`

`not_applicable` is allowed only where the transition does not require a substantive inspection event.

### `failure_family`

Top-level failure family if the outcome was negative or constrained.

Use the committed failure taxonomy families:

- `requirement`
- `source`
- `execution`
- `inspection`
- `support`
- `control`
- `lifecycle`
- `none`

Use `none` when no failure family applies.

### `rescue_required`

Boolean indicating whether bounded correction, manual rescue, or downstream recovery was required after this outcome.

This is not the same as `inspection_result = rework_required`.

Examples:

- `inspection_result = pass`, `rescue_required = true`
  - outcome passed only after bounded support or recovery
- `inspection_result = reject`, `rescue_required = false`
  - negative outcome recorded with no immediate rescue path yet executed

### `evidence_reference`

Reference to the artifact or persisted surface that proves the event occurred.

Expected forms may include:

- path to manifest or sidecar
- path plus row identifier
- artifact id plus local file path

The point is not a universal URI scheme in v0. The point is that every row points to inspectable evidence.

### `notes`

Compact human-readable context for the row.

Use notes for:

- ambiguity flags
- outcome qualification caveats
- brief rescue context

Do not use notes to hide required structured fields.

## Recommended Additional Fields

### `subject_attribution_basis`

Why the repo believes the recorded subject is the correct actor or approver for the event.

Examples:

- explicit approval record
- attributable artifact metadata
- deterministic system-owned validation
- inferred from workflow surface

This field is strongly recommended because the qualification validation report identified subject attribution as the main evidence gap.

### `artifact_ref`

Compact pointer to the artifact most directly associated with the transition outcome.

### `workflow_ref`

Pointer to the enclosing workflow, draft root, or work-order-like artifact.

### `outcome_status`

Short normalized status for fast summaries, such as:

- `succeeded`
- `succeeded_with_rescue`
- `failed_recoverable`
- `failed_terminal`

This field is optional in v0 because it can be derived from other fields, but it may help compact downstream reporting.

## Subject Attribution Model

The ledger must record subject attribution without overclaiming certainty.

Use this doctrine:

- if the acting or approving subject is explicit in the evidence, record it directly
- if the subject is only loosely inferred, record the best available subject and make the inference explicit in `subject_attribution_basis`
- if the subject cannot be reliably attributed, use `unknown_subject`

For v0:

- unattributed rows are valid operational history
- unattributed rows are weak qualification evidence

## Evidence Attribution Model

Every row must point to inspectable evidence.

Minimum rule:

- no transition outcome row without an `evidence_reference`

Evidence references should prefer existing repo truth surfaces such as:

- onboarding manifests
- `qa_queue.csv`
- `bindings.csv`
- derived detection manifests
- publish-readiness results
- adapted intake manifests
- future run artifacts that explicitly record transition outcomes

## Example Row Shape

```json
{
  "event_id": "transition-outcome-20260611T031500Z-001",
  "timestamp": "2026-06-11T03:15:00Z",
  "subject": "human_editor",
  "subject_attribution_basis": "explicit review decision record",
  "transition_id": "TRANS-016",
  "input_state": "review_pack_ready",
  "output_state": "review_pack_mixed_status",
  "inspection_result": "pass",
  "failure_family": "none",
  "rescue_required": false,
  "evidence_reference": "assets/games/call_of_duty/drafts/onboarding/20260524T225117Z/catalog/qa_queue.csv#row:review-pack-mixed-status-1",
  "artifact_ref": "assets/games/call_of_duty/drafts/onboarding/20260524T225117Z",
  "workflow_ref": "assets/games/call_of_duty/drafts/onboarding/20260524T225117Z",
  "notes": "Aggregate review artifact contains both resolved and unresolved required members."
}
```

## Minimum Consumer Expectations

The ledger is not itself the consumer logic, but it must support these minimum downstream uses.

### Qualification

Should be able to answer:

- how many attributable outcomes exist for a given `subject x transition`
- how many were successful
- how many needed rescue
- how many negative inspection results occurred

### Failure Analysis

Should be able to answer:

- which failure families recur most often by transition
- whether negative outcomes are mostly source, support, control, or execution problems

### Routing Analysis

Should be able to answer:

- which transitions commonly end in recoverable versus terminal outcomes
- how often rescue or rework follows a specific transition

### Future Planning

Should be able to answer later:

- where throughput is clean
- where repeated rescue is common

Planning logic itself remains out of scope for v0.

## v0 Non-Goals

Do not include in the ledger contract yet:

- duration
- cost
- queue delay
- staffing assignment policy
- scheduling priority
- capacity accounting
- inventory movement
- aggregate score rollups as required fields

These may become useful later, but they are not required to solve the current evidence gap.

## Explicit Deferrals

These are intentionally deferred beyond ledger v0:

- qualification thresholds for promotion and demotion
  - future qualification evidence policy or qualification v1
- routing consequences
  - existing routing architecture and later routing refinements
- inventory-aware history
  - future inventory model
- staffing and capacity metrics
  - future resource model or planning doctrine
- cost or time accounting
  - future planning or operational analytics layer

## Minimum v0 Outcome

Transition Outcome Ledger v0 is successful if it makes these points explicit:

- transition history is a first-class artifact separate from qualification
- subject attribution is captured directly where possible and conservatively where not
- every outcome row points to inspectable evidence
- the ledger can support future qualification, failure analysis, and routing analysis without becoming those systems

## Recommended Next Pressure Test

Validate the ledger contract against the same historical workflows already used for qualification validation and ask:

- can one `TRANS-016` style mixed-status outcome be recorded cleanly?
- can one `needs_population_review` style upstream source-intake outcome be recorded cleanly?
- can one publish-readiness pass outcome be recorded cleanly?
- where does subject attribution remain missing even when the row schema is available?
