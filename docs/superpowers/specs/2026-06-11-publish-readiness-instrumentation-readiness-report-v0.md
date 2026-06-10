# Publish Readiness Instrumentation Readiness Report v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Prioritize and prepare the publish-readiness instrumentation slice without implementing it.

This report identifies:

- the current publish-readiness write path
- the safest implementation seam
- the closest committed canonical transition and inspection mappings
- expected attribution quality
- required workflow-emission fields
- validation targets and implementation risks

## Scope

This report prepares only the publish-readiness instrumentation slice.

It does not:

- implement workflow changes
- redefine control-plane states or transitions
- redefine qualification policy
- expand into planning, inventory, routing policy, staffing, or unrelated workflow surfaces

## Evidence Surfaces

- [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- [pipeline/derived_row_review.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/derived_row_review.py)
- [pipeline/onboarding_publish_readiness.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/onboarding_publish_readiness.py)
- [tests/test_publish_readiness_goldset.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/tests/test_publish_readiness_goldset.py)
- [2026-06-11-workflow-instrumentation-adoption-plan-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-workflow-instrumentation-adoption-plan-v0.md)
- [2026-06-11-operational-observation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-operational-observation-report-v0.md)
- [2026-06-11-transition-outcome-ledger-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-validation-report-v0.md)
- [2026-06-11-attribution-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-validation-report-v0.md)

## Current Write Path

## Readiness evaluation

Publish readiness is computed in:

- [pipeline/onboarding_publish_readiness.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/onboarding_publish_readiness.py)
- function: `validate_onboarding_publish(...)`

That validator returns:

- `can_publish`
- `readiness`
- counts and findings

Current observed readiness values include:

- `ready_to_publish`
- `needs_binding_review`
- `needs_population_review`
- `structurally_invalid`

## Shared persistence seam

The cleanest implementation seam is:

- [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- function: `_refresh_phase_status_from_publish_readiness(...)`

Current behavior:

1. derives the latest detection manifest
2. runs `validate_onboarding_publish(...)`
3. maps `can_publish` into workflow state
4. persists the resulting `phase_status` through `_write_binding_review_artifacts(...)`

Current persisted workflow state is:

- `phase_status = ready_to_publish` when `can_publish = true`
- `phase_status = bindings_pending` when `can_publish = false`

This helper is already invoked from the main onboarding-mutating flows:

- onboarding draft build in [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- targeted onboarding refill in [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- accepted/rejected derived-row review application in [pipeline/derived_row_review.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/derived_row_review.py)

## Current limitation

This seam persists workflow state, not event history.

It does not currently emit:

- `event_id`
- `timestamp`
- canonical transition fields
- canonical inspection fields
- explicit attribution fields

It also contains an early return when the final `phase_status` did not change.

That is acceptable for state persistence.

It is not sufficient by itself for append-only event capture.

## Proposed Canonical Mapping

## Positive readiness outcome

Proposed first-pass mapping when:

- `readiness = ready_to_publish`

Closest committed control-plane interpretation:

- transition: `TRANS-002`
- input state: `review_pack_ready`
- output state: `review_pack_approved`
- inspection result: `pass`
- failure family: `none`
- rescue required: `false`

Reason:

- this is the closest committed aggregate acceptance decision
- the validator is confirming the reviewable aggregate is acceptable as downstream input
- it is not a terminal completion event

## Negative non-terminal readiness outcome

Proposed first-pass mapping when:

- `readiness = needs_binding_review`

Closest committed control-plane interpretation:

- transition: `TRANS-017`
- input state: `review_pack_mixed_status`
- output state: `review_pack_needs_rework`
- inspection result: `rework_required`
- failure family: `inspection`
- rescue required: `true`

Reason:

- this mapping is already the closest semantic fit used in the committed ledger-validation and operational-observation surfaces
- the workflow remains active and recoverable
- the validator is expressing a bounded non-terminal inspection outcome, not a hard reject

## Outcomes intentionally deferred in the first slice

The following validator outcomes should not be forced into the first implementation without a narrower mapping decision:

- `needs_population_review`
- `structurally_invalid`

Reason:

- `needs_population_review` is upstream source/population pressure, not the core workflow-level readiness outcome targeted by this slice
- `structurally_invalid` is a structural failure posture, not a straightforward review-pack readiness event

These can remain follow-on instrumentation pressure if the first publish-readiness slice is kept bounded to:

- `ready_to_publish`
- `needs_binding_review`

## Proposed Inspection Mapping

For the bounded first slice:

| readiness result | canonical transition | closest inspection posture | inspection_result | failure_family | rescue_required |
| --- | --- | --- | --- | --- | --- |
| `ready_to_publish` | `TRANS-002` | aggregate acceptance / approval outcome | `pass` | `none` | `false` |
| `needs_binding_review` | `TRANS-017` | inspected non-terminal readiness outcome | `rework_required` | `inspection` | `true` |

This keeps the slice aligned to the committed control-plane vocabulary without inventing new transition names.

## Expected Attribution Quality

Expected subject for the first publish-readiness slice:

- `system_validator`

Expected attribution source:

- deterministic validator-owned workflow step

Expected attribution basis:

- `deterministic_system_step`

Expected attribution quality:

- `medium`

Reason:

- attribution validation already showed `deterministic_system_step` is realistic under current workflow artifacts
- it supports `medium` attribution
- it does not support `strong` attribution by itself

This is still materially better than the current baseline, where publish-readiness outcomes are reconstructive rather than emitted.

## Required Workflow-Emission Fields

Minimum event-grade fields for this slice:

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

Recommended supporting fields:

- `capture_source = onboarding_publish_readiness`
- `notes`

## Preferred Evidence Reference

Preferred direct workflow evidence should point to the persisted readiness-emission surface itself.

If a separate readiness event row is emitted adjacent to state persistence, the reference should target that row.

If the first implementation writes the event through the same workflow update that persists readiness state, it should still preserve a stable path-addressable reference.

The important constraint is:

- `evidence_reference` must point to the execution-time persisted surface
- not to a later reconstruction report

## Implementation Readiness Assessment

## Safe identification of the write path

The write path is identifiable safely.

The correct implementation target is not `publish_onboarding_draft(...)`.

It is the shared readiness-refresh seam used by onboarding-mutating flows:

- `_refresh_phase_status_from_publish_readiness(...)`

This is the right place because it already has:

- the validated readiness payload
- the current draft root
- the current manifest and state payloads
- the workflow mutation context

## Main implementation risk

The current helper is state-change-oriented, while the evidence plane is append-only.

Current behavior:

- if the final `phase_status` did not change, the helper returns early and does not rewrite workflow state

Risk:

- if event emission is attached only to that state-change branch, repeated readiness evaluations with the same outcome will not be emitted
- that would under-capture meaningful readiness attempts after accepted binding writes or review updates

Required implementation posture:

- event emission must be keyed on readiness evaluation occurrence
- not only on `phase_status` mutation

This is not a blocker.

It is the main design constraint for the slice.

## Minimal persisted surface recommendation

The first implementation should use one append-only readiness outcome surface adjacent to `onboarding_state.json`.

Reason:

- `onboarding_state.json` is overwritten state
- the outcome ledger requires append-only history
- the slice should not overload mutable state with irreversible event history

This surface should remain event-only.

It should not become a second workflow-state source of truth.

## Required Validation Targets

The first implementation should be validated with narrow tests covering:

1. positive readiness emission
- when `validate_onboarding_publish(...)` returns `ready_to_publish`
- emit `system_validator`
- emit `TRANS-002`
- emit `inspection_result = pass`
- emit `failure_family = none`

2. negative readiness emission
- when `validate_onboarding_publish(...)` returns `needs_binding_review`
- emit `system_validator`
- emit `TRANS-017`
- emit `inspection_result = rework_required`
- emit `failure_family = inspection`
- emit `rescue_required = true`

3. append-only behavior
- repeated readiness evaluations should not overwrite previous emitted rows

4. unchanged-phase-status behavior
- a fresh readiness evaluation that keeps `phase_status = bindings_pending` must still be able to emit a new readiness event when the workflow step actually occurred

5. compatibility
- emitted fields must remain derivable into one Transition Event Capture v0 row and one Transition Outcome Ledger v0 row without semantic reconstruction

## Readiness Verdict

This slice is ready for bounded implementation.

Why:

- the write path is identifiable safely
- the likely subject is deterministic and stable
- the canonical mapping can be bounded to committed transition vocabulary
- the required workflow-emission fields are already known
- the main risk is implementation-local and explicit, not architectural

Recommended implementation scope:

- instrument only the publish-readiness outcomes for `ready_to_publish` and `needs_binding_review`
- emit deterministic `system_validator` attribution
- preserve append-only event history
- stop short of broader source-intake or structural-failure mapping in the first pass

## Non-Goals

Do not add in the first publish-readiness slice:

- new control-plane states or transitions
- qualification promotion policy
- planning, staffing, inventory, or routing policy changes
- source-fetch instrumentation
- QA queue primary-event instrumentation
