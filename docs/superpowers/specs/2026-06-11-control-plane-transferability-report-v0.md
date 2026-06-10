# Control Plane Transferability Report

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Apply the current control-plane baseline to three additional historical workflows and test whether:

- states
- transitions
- inspection logic
- failure attribution
- routing
- qualification framing

transfer without major modification.

This report does not add inventory, planning, resource modeling, or staffing logic.

## Baseline

Current baseline under test:

- `State`
- `Transition`
- `Inspection`
- `Failure`
- `Routing`
- `Qualification`

Previously validated workflow:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

That earlier validation drove the aggregate-review patch:

- `review_pack_mixed_status`
- `review_pack_needs_rework`
- `TRANS-016`
- `TRANS-017`
- `TRANS-018`

## Additional Workflows Tested

### Workflow A

- path: `assets/games/call_of_duty/drafts/onboarding/20260505T212727Z`
- game: `call_of_duty`
- readiness: `needs_population_review`
- can_publish: `false`

Observed evidence:

- required detection rows: 122
- accepted bindings: 0
- population findings: 4
- binding findings: 188
- top finding types:
  - `missing_accepted_binding` = 122
  - `weak_name_match` = 54
  - `missing_binding` = 8
  - `conflicting_identity_match` = 4

### Workflow B

- path: `assets/games/call_of_duty/drafts/onboarding/20260505T213409Z`
- game: `call_of_duty`
- readiness: `ready_to_publish`
- can_publish: `true`

Observed evidence:

- required detection rows: 112
- accepted bindings: 112
- population findings: 0
- binding findings: 0
- completeness findings: 0

### Workflow C

- path: `assets/games/marvel_rivals/drafts/onboarding/20260509T143952Z`
- game: `marvel_rivals`
- readiness: `ready_to_publish`
- can_publish: `true`

Observed evidence:

- required detection rows: 47
- accepted bindings: 47
- population findings: 0
- binding findings: 0
- completeness findings: 0

## Transferability Assessment

## 1. State Transfer

### Workflow B and Workflow C

The state model transfers acceptably for the fully publishable drafts.

Reason:

- both drafts behave like aggregate artifacts that have completed their required member resolution
- there is no mixed-status ambiguity
- the artifact can be treated as having passed bounded inspection and reached a whole-draft accepted condition

No new state candidates were exposed by these two workflows.

### Workflow A

The state model transfers only partially.

Reason:

- the draft has no accepted bindings
- it is not simply “mixed-status”
- it is not yet in the same review-stage posture as the previously validated `needs_binding_review` workflow
- the dominant issues are population and identity conflict upstream of accepted aggregate review

This means the aggregate review patch does not solve Workflow A directly, but that is not a failure of the patch. It is a signal that Workflow A stresses an earlier part of the lifecycle.

## 2. Transition Transfer

### Workflow B and Workflow C

The current transition framing transfers without major modification.

What transfers:

- reviewable aggregate artifact
- successful inspection
- downstream readiness / publishable outcome

The exact domain nouns differ from the gameplay-review language in the control-plane names, but the transition logic still holds:

- candidate artifact produced
- artifact inspected
- artifact accepted for downstream use

### Workflow A

The current transition set transfers only loosely.

What still works:

- failure can be attributed
- routing can remain non-terminal
- qualification can remain conservative

What does not map cleanly:

- there is no strong fit for a transition between “source fetched” and “review-ready aggregate artifact”
- most work appears to be failing before aggregate review states become meaningful

This suggests an earlier source/intake transition layer is still under-modeled for onboarding-style workflows.

## 3. Inspection Transfer

Inspection logic transfers well across all three workflows.

Why:

- each workflow has a bounded publish-readiness outcome
- the repo already records explicit findings and readiness
- inspection still answers the core question:
  - did the artifact reach the target state required for downstream progression?

Transfer result:

- strong

No inspection redesign is required from these three workflows.

## 4. Failure Transfer

Failure attribution also transfers well.

### Workflow B and Workflow C

These produce no negative findings, so the model can treat them as clean-pass examples.

### Workflow A

Its findings map directionally into existing families:

- `conflicting_identity_match`
  - mostly `requirement` or `source`
- `missing_accepted_binding`
  - mostly `inspection`, `support`, or `requirement`
- `missing_binding`
  - mostly `support`
- `weak_name_match`
  - mostly `source` or `requirement`

This is somewhat fuzzy, but still usable.

Transfer result:

- acceptable without top-level taxonomy redesign

## 5. Routing Transfer

Routing transfers cleanly for the two ready workflows and adequately for the population-review workflow.

### Workflow B and Workflow C

The routing pattern is simple:

- inspected aggregate artifact
- pass
- advance to publishable outcome

### Workflow A

The routing remains non-terminal and recoverable:

- findings do not justify archive
- work remains active
- further correction is required

This aligns with `control_hold` or bounded rework posture rather than terminal failure.

No major routing redesign is required from this sample.

## 6. Qualification Transfer

Qualification transfers conceptually, but evidence remains conservative.

### What transfers

- subject-by-transition trust framing
- support/control separation from execution trust
- conservative `Q1`/`Q2` posture

### What remains weak

- subject attribution is clearer for the aggregate review patch than for the earlier population workflow
- Workflow A still lacks a crisp transition boundary where trust can be confidently updated

Transfer result:

- acceptable, but still evidence-thin for promotion

## Required Exceptions And Ambiguities

### Exception 1: population-review workflows stress an earlier lifecycle slice

Workflow A shows that some historical workflows are dominated by:

- source conflict
- identity conflict
- no accepted bindings yet

That means they stress:

- source intake
- candidate population
- early binding preparation

more than aggregate review completion.

This is an exception to the current aggregate-review patch, not a contradiction of it.

### Exception 2: publishable onboarding drafts map semantically, not literally

The current control-plane language still uses:

- `review_pack`
- `approved_clips`
- `platform_package`

The onboarding workflows are not literally those artifacts.

They still transfer because the structure matches, but the names remain content-production-biased.

This is a semantic ambiguity, not yet a structural blocker.

## New State Candidates

These are candidates only. They are not recommended as immediate additions.

### Candidate A

- `source_population_incomplete`

Reason:

- Workflow A is upstream of mixed-status review and may need a first-class non-terminal source/intake condition if onboarding/population workflows become first-class control-plane lanes.

### Candidate B

- `source_population_conflicted`

Reason:

- conflicting identity matches behave differently from ordinary missing bindings and may deserve their own state if this class of workflow becomes common.

Current recommendation:

- do not add these yet

The transfer sample is still too small to justify expanding the state catalog again.

## Recommendation

### Keep / Patch / Redesign

Recommendation:

- keep the current control-plane baseline
- keep the aggregate mixed-status patch
- do not redesign any major layer yet

### Why

Because:

- Workflow B transfers cleanly
- Workflow C transfers cleanly
- Workflow A does not invalidate the aggregate patch; it reveals a different, earlier lifecycle slice

That means the current model is:

- good enough to survive additional workflows
- not yet broad enough to treat all onboarding-style source-population work as fully modeled

But that is still a patch pressure, not a redesign trigger.

## Bottom Line

Transferability result:

- the control-plane baseline transfers across multiple additional workflows without major modification
- the aggregate mixed-status patch appears general enough for aggregate review workflows
- the next pressure is not inventory or planning
- the next pressure, if repeated, is earlier source/intake state modeling for population-review workflows

Current recommendation:

- continue the validation phase
- keep the current control plane
- patch only if additional workflows keep exposing the same upstream population-state gap
