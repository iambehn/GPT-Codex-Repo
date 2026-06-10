# Workflow Instrumentation Adoption Plan v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Define the minimum rollout strategy for introducing identity, attribution, inspection, and outcome-emission fields into the existing onboarding workflow family so it can populate the evidence plane without retrospective reconstruction.

This plan states:

- which existing onboarding surfaces are in scope first
- what each surface must start emitting
- the smallest implementation slices that materially improve evidence quality
- how to validate each rollout step

## Scope

This plan defines:

- the current onboarding workflow surfaces under first adoption
- required emissions by surface
- gap analysis against the current workflow family
- minimal change slices
- validation strategy
- rollout order

This plan does not define:

- new control-plane states or transitions
- qualification policy changes
- planning, scheduling, staffing, inventory, or resource logic
- adoption outside the onboarding workflow family

## Guardrails

- Preserve the current control-plane, ledger, attribution, and instrumentation architectures as the baseline.
- Adopt instrumentation at the write surfaces where outcomes are already being recorded.
- Prefer the smallest rollout slice that converts weak evidence into strong evidence.
- Do not create parallel workflow files when an existing workflow surface can be extended.
- Treat weak attribution as acceptable transitional state, but not as the final target.

This plan must stay consistent with:

- [2026-06-11-operational-observation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-operational-observation-report-v0.md)
- [2026-06-11-attribution-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-validation-report-v0.md)
- [2026-06-11-workflow-instrumentation-specification-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-workflow-instrumentation-specification-v0.md)
- [2026-06-11-transition-event-capture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-event-capture-v0.md)
- [2026-06-11-attribution-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-architecture-v0.md)

## Why This Plan Exists

Recent validation converged on one stable diagnosis:

- the architecture survives
- workflow emission is weak

The current onboarding workflow family already produces:

- source fetch outcomes
- accepted binding outcomes
- publish-readiness outcomes

But it does not yet emit:

- explicit reviewer identity
- explicit actor identity where available
- canonical transition identifiers
- canonical input and output states
- normalized inspection results
- durable event-ready outcome rows

That makes adoption the next bottleneck.

## Adoption Target

Initial adoption target:

- onboarding draft workflow family under `assets/games/<game>/drafts/onboarding/...`

First adoption surfaces:

- `catalog/bindings.csv`
- `catalog/source_fetch_log.csv`
- publish-readiness outcome writes associated with `manifests/onboarding_state.json`

Supporting but not primary in the first pass:

- `catalog/qa_queue.csv`

## Current Workflow Surface

### `catalog/bindings.csv`

Current useful fields:

- `status`
- `derived_row_review_decision`
- `derived_row_review_status`
- `derived_row_reviewed_at`
- `review_notes`

Current limitation:

- accepted binding writes prove outcome timing and review context
- they do not prove reviewer identity
- they do not emit canonical transition or inspection fields

### `catalog/source_fetch_log.csv`

Current useful fields:

- `status`
- `source_page_url`
- `source_role`
- `source_title`

Current limitation:

- source fetch outcomes are structurally attributable
- but they are not written as event-grade rows

### Publish-readiness outcome surface

Current useful fields:

- workflow phase state in `onboarding_state.json`
- validator behavior in `pipeline.onboarding_publish_readiness.validate_onboarding_publish(...)`

Current limitation:

- workflow readiness is visible
- outcome history is not emitted as a direct event row with subject and canonical fields

### `catalog/qa_queue.csv`

Current useful fields:

- `item_type`
- `reason`
- `status`

Current limitation:

- this is supporting evidence of unresolved work
- it is not a primary transition-outcome emission surface

## Required Emissions

All adopted workflow surfaces should be able to provide these evidence-plane fields directly or through an immediately derivable event write:

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

Recommended:

- `artifact_ref`
- `workflow_ref`
- `capture_source`
- `notes`

## Gap Analysis

### Highest-leverage gap

The strongest current bottleneck is:

- accepted binding writes

Reason:

- they account for `112` observed successful outcomes in the validated workflow
- they already carry timing and decision context
- they are still weakly attributable because reviewer identity is missing

### Second gap

- publish-readiness outcomes are still reconstructed instead of emitted

Reason:

- they are deterministic and important
- they should become direct event rows

### Third gap

- source fetch outcomes are deterministic but still not event-grade

Reason:

- they are already structurally attributable
- they need explicit event-ready fields to move from inferred evidence to durable evidence

## Minimal Changes

### Slice 1: instrument accepted binding writes

Target surface:

- `catalog/bindings.csv`

Add at write time:

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
- `evidence_reference`

Reason:

- this converts the largest weak-attribution outcome class into a direct evidence surface

Expected impact:

- strongest improvement in qualification-evidence density
- strongest improvement in attribution quality
- largest reduction in retrospective reconstruction

### Slice 2: emit publish-readiness outcome records

Target surface:

- readiness write path adjacent to `onboarding_state.json`

Add at write time:

- one explicit readiness outcome record with:
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

Reason:

- readiness is currently operationally important but only reconstructable

Expected impact:

- removes the current workflow-level reconstruction gap
- gives deterministic system-owned inspection outcomes a direct event surface

### Slice 3: instrument source fetch outcomes

Target surface:

- `catalog/source_fetch_log.csv`

Add at write time:

- `fetched_at`
- `subject`
- `subject_kind`
- `subject_attribution_basis`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `event_id`
- `evidence_reference`

Reason:

- source fetch already has deterministic ownership
- the missing issue is event-grade emission, not model ambiguity

Expected impact:

- upgrades deterministic source-intake evidence from inferred to direct

### Slice 4: leave `qa_queue` as supporting evidence only

Target surface:

- `catalog/qa_queue.csv`

Change:

- no primary instrumentation change required in the first adoption pass

Reason:

- this surface is useful for context and failure pressure
- it is not the first place to invest if the goal is direct transition-outcome emission

## Validation Strategy

### Validation for Slice 1

Use one accepted binding write and verify:

- explicit reviewer identity is persisted
- canonical transition fields are present
- normalized inspection fields are present
- one ledger-grade event row can be derived without semantic reconstruction

### Validation for Slice 2

Use one publish-readiness evaluation and verify:

- the outcome is emitted directly
- deterministic subject attribution is explicit
- `rework_required` or `pass` is explicit without later inference

### Validation for Slice 3

Use one source-fetch write and verify:

- fetch outcome timestamp is explicit
- deterministic attribution is explicit
- source-intake transition mapping is direct

### Cross-slice validation

After Slices 1 to 3:

- rerun an operational observation pass on one onboarding workflow
- measure:
  - explicit subject attribution count
  - reduction in reconstructed events
  - increase in strong or medium evidence rows

## Rollout Order

### Step 1

- accepted binding writes

Reason:

- highest event volume
- highest leverage on attribution quality

### Step 2

- publish-readiness outcome emission

Reason:

- closes the workflow-level inspection reconstruction gap

### Step 3

- source fetch event-grade writes

Reason:

- deterministic and straightforward, but lower leverage than accepted binding identity

### Step 4

- optional secondary cleanup on supporting surfaces

Reason:

- only after primary event surfaces are working

## Rollout Constraints

- Do not require full workflow-family redesign before the first slice lands.
- Do not wait for perfect identity resolution across all surfaces.
- Do not let `workflow_owned_default` become stronger than weak fallback during rollout.
- Do not block event emission on missing identity if `unknown_subject` is the honest value.

## Success Criteria

This adoption plan is successful if the first rollout sequence can produce:

- explicit reviewer identity on accepted binding writes
- direct publish-readiness outcome emission
- direct source-fetch event emission
- fewer reconstructed events in the next operational observation pass
- stronger qualification evidence density without changing the underlying architecture

## Non-Goals

Do not add in this plan:

- planning or scheduling rollout
- inventory-aware workflow changes
- staffing assignment policy
- qualification promotion thresholds
- broad adoption outside onboarding

## Recommended Next Implementation Pressure Test

Implement Slice 1 first and verify one accepted binding write can produce:

- explicit reviewer identity
- bounded attribution basis
- canonical transition and inspection fields
- one direct ledger-grade event row

If that works, the architecture is no longer the blocker. The workflow adoption path is.
