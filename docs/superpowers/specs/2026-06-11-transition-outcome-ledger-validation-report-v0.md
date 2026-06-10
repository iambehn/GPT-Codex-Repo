# Transition Outcome Ledger Validation Report v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Validate Transition Outcome Ledger v0 against one real historical workflow and reconstruct transition outcome history from available evidence.

This report shows:

- which ledger fields were populated cleanly
- which fields required inference
- which fields could not be recovered from current artifacts
- the minimum schema changes required, if any

This report preserves the current ledger schema as the baseline.

It does not add:

- inventory
- planning
- staffing
- resource allocation

## Selected Workflow

Representative workflow:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

Why this workflow was selected:

- it is the same concrete workflow used in the first control-plane validation
- it contains source, candidate, binding, QA, and publish-readiness artifacts
- it contains both successful and unresolved outcomes
- it exposes the subject-attribution gap already identified in qualification validation

## Ledger Baseline Under Test

Ledger spec under validation:

- [2026-06-11-transition-outcome-ledger-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-v0.md)

Required row fields under test:

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

Recommended row fields considered in validation:

- `subject_attribution_basis`
- `artifact_ref`
- `workflow_ref`
- `outcome_status`

## Workflow Evidence Used

Artifacts inspected:

- `catalog/source_fetch_log.csv`
- `catalog/bindings.csv`
- `catalog/qa_queue.csv`
- `manifests/onboarding_state.json`
- `manifests/assets_manifest.json`
- publish-readiness results already captured in [2026-06-10-control-plane-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-control-plane-validation-report-v0.md)

Key observable facts:

- 5 sources were fetched successfully
- accepted binding rows contain:
  - `derived_row_review_decision`
  - `derived_row_review_status`
  - `derived_row_reviewed_at`
  - `review_notes`
- accepted binding rows also show:
  - `status = accepted`
  - `review_notes = auto-applied recommended candidate`
- QA rows contain unresolved findings such as:
  - `item_type = missing_binding`
  - `status = needs_better_reference`
- publish-readiness concluded:
  - `readiness = needs_binding_review`
  - `can_publish = false`

## Reconstruction Strategy

This validation uses three representative event types:

1. upstream source-intake success
2. accepted binding / partial transition success
3. inspected non-terminal failure at publish-readiness

The point is not to reconstruct every event in the workflow.

The point is to test whether the ledger schema can represent:

- one clean pass event
- one semantically successful but attribution-thin event
- one negative inspected outcome

## Reconstructed Example Events

### Event A: source fetch success

Closest ledger row:

```json
{
  "event_id": "workflow-20260524T225117Z-source-fetch-overview",
  "timestamp": "unknown_timestamp",
  "subject": "system_validator",
  "transition_id": "SITRANS-001",
  "input_state": "source_declared",
  "output_state": "source_fetched",
  "inspection_result": "pass",
  "failure_family": "none",
  "rescue_required": false,
  "evidence_reference": "assets/games/call_of_duty/drafts/onboarding/20260524T225117Z/catalog/source_fetch_log.csv#source_role=overview",
  "notes": "Source fetch log records fetched overview source."
}
```

Assessment:

- structurally representable
- timestamp not directly recoverable from the current source log
- subject is inferred rather than explicit

### Event B: accepted binding success

Closest ledger row:

```json
{
  "event_id": "workflow-20260524T225117Z-binding-alex-keller",
  "timestamp": "2026-05-05T22:31:08.438696+00:00",
  "subject": "unknown_subject",
  "subject_attribution_basis": "binding row shows reviewed_at and auto-applied review notes but does not identify a concrete actor",
  "transition_id": "TRANS-006",
  "input_state": "review_pack_approved",
  "output_state": "approved_clips_ready",
  "inspection_result": "pass",
  "failure_family": "none",
  "rescue_required": false,
  "evidence_reference": "assets/games/call_of_duty/drafts/onboarding/20260524T225117Z/catalog/bindings.csv#binding_id=binding_daa295e75d02",
  "notes": "Accepted binding exists for a resolved row, but the literal aggregate transition remains only a partial semantic fit."
}
```

Assessment:

- timestamp is recoverable
- evidence reference is strong
- subject attribution is not recoverable cleanly
- literal transition mapping remains semantically approximate because the workflow is row-level, not aggregate-state literal

### Event C: non-terminal publish-readiness failure

Closest ledger row:

```json
{
  "event_id": "workflow-20260524T225117Z-publish-readiness-needs-binding-review",
  "timestamp": "unknown_timestamp",
  "subject": "system_validator",
  "subject_attribution_basis": "publish-readiness result is produced by deterministic validation surface",
  "transition_id": "TRANS-017",
  "input_state": "review_pack_mixed_status",
  "output_state": "review_pack_needs_rework",
  "inspection_result": "rework_required",
  "failure_family": "inspection",
  "rescue_required": true,
  "evidence_reference": "docs/superpowers/specs/2026-06-10-control-plane-validation-report-v0.md#workflow-evidence",
  "notes": "Readiness result `needs_binding_review` is a strong non-terminal inspection outcome, but current artifacts do not persist a dedicated event row for it."
}
```

Assessment:

- outcome semantics are strong
- timestamp is not directly recoverable from the preserved validation output
- transition mapping is still partially interpretive because the workflow predates the aggregate mixed-status patch

## Field Recovery Assessment

### Fields populated cleanly

These fields can be populated reliably from the workflow evidence with little or no inference:

- `transition_id`
  - when a semantically closest committed transition exists
- `inspection_result`
  - especially for accepted rows and negative publish-readiness outcomes
- `failure_family`
  - especially for negative QA and readiness outcomes
- `rescue_required`
  - recoverable at a useful level from `needs_binding_review`, `needs_better_reference`, and similar statuses
- `evidence_reference`
  - current artifacts are inspectable and path-addressable
- `notes`
  - compact context is easy to provide from existing surfaces

### Fields requiring inference

These fields can usually be populated, but only with explicit caveats:

- `subject`
  - many rows require `unknown_subject` or best-effort inference
- `input_state`
  - often inferred from the closest control-plane or source-intake mapping
- `output_state`
  - often inferred from the same semantic mapping
- `subject_attribution_basis`
  - should explain the inference explicitly
- `outcome_status`
  - can be derived, but still reflects interpretation

### Fields not recoverable cleanly from current artifacts

These fields cannot yet be populated directly and consistently from the selected workflow:

- `timestamp`
  - source fetch success has no explicit per-row timestamp in `source_fetch_log.csv`
  - publish-readiness outcome is observable, but not persisted with a clean event timestamp in the selected evidence surface

## Main Validation Findings

### 1. The ledger schema survives contact with real artifacts

The current row contract is sufficient to represent real transition outcomes from the workflow.

That is the main success condition.

No required field appears fundamentally misplaced.

### 2. Subject attribution remains the dominant evidence gap

This was the strongest finding in qualification validation and it remains the strongest finding here.

The ledger can represent uncertainty correctly by using:

- `unknown_subject`
- `subject_attribution_basis`

That is a schema success.

But it also confirms that the repo still needs better subject-attribution capture if qualification is to become more empirical.

### 3. Timestamps are weaker than expected for upstream and inspection events

Accepted binding rows contain a clean reviewed timestamp.

But:

- source fetch rows do not
- publish-readiness outcomes are visible but not persisted as explicit timestamped event records in the workflow artifact family used here

This is the clearest operational weakness exposed by the validation.

### 4. Semantic transition mapping is usable, but not literal in all cases

The workflow predates some of the control-plane patch vocabulary and still behaves partly at row level.

So the ledger can record history now, but some rows will still need explicit notes acknowledging:

- semantic mapping
- aggregate-to-row mismatch
- pre-patch workflow interpretation

That is acceptable for v0 as long as the notes and attribution basis stay honest.

## Redundancy Check: `inspection_result` vs `failure_family`

The two fields are not redundant.

Current validation supports:

- `inspection_result`
  - what happened at the inspection boundary
- `failure_family`
  - why the outcome was negative or constrained

Recommended discipline from this validation:

- `inspection_result = pass`
  - should normally pair with `failure_family = none`
- negative inspection results
  - should map to one of the committed failure families

This is already consistent with the ledger spec and should remain an explicit review gate.

## Append-Only Check

A single transition should be able to produce multiple ledger rows across repeated attempts.

This validation supports:

- append, never overwrite

Reason:

- accepted binding rows and later readiness failure are different events
- future rework loops will matter to qualification and routing analysis
- final outcome alone is not enough to explain transition trust

No schema change is required for this behavior. It is already compatible with the event-row model.

## Qualification Consumer Check

Can the ledger support:

- “why is this Q2?”

Answer:

- partially, but not fully from this workflow yet

What it can support now:

- count of attributable versus unattributed outcomes
- count of recoverable negative outcomes
- count of pass outcomes with evidence references

What still remains weak:

- repeat attributable success for literal transitions
- consistent timestamped event history across all workflow stages

That is an evidence capture weakness, not a row-schema weakness.

## Minimum Schema Changes Required

No required schema change is necessary to keep Transition Outcome Ledger v0 viable.

That is the main validation result.

### Recommended clarifications, not mandatory schema expansion

1. Treat `timestamp` as:
   - observed event timestamp where available
   - otherwise explicitly unresolved, not silently fabricated

2. Treat `subject` as:
   - explicit where known
   - `unknown_subject` where not recoverable

3. Treat semantic transition mapping as acceptable in v0 only when:
   - the row notes make the mapping caveat explicit

No new required fields are justified by this single-workflow validation pass.

## Validation Verdict

Transition Outcome Ledger v0 survives validation against a real historical workflow.

What survived:

- compact event schema
- explicit evidence references
- explicit subject uncertainty
- separation between outcome recording and downstream consumers

What remains weak:

- subject attribution in existing repo artifacts
- timestamp capture for some upstream and inspection events
- literal transition mapping for pre-patch aggregate workflows

## Bottom Line

The ledger schema is good enough to stand as the baseline.

The next bottleneck is not ledger design.

It is better event capture in real workflow artifacts, especially for:

- subject attribution
- per-event timestamps
- direct persistence of inspection outcomes as event rows
