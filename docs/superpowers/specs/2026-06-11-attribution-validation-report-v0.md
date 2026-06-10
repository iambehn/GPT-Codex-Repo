# Attribution Validation Report v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Validate Attribution Architecture v0 against one real historical workflow and determine which attribution mechanisms actually produce:

- `strong`
- `medium`
- `weak`
- `unknown`

This report identifies:

- which attribution sources are realistic under current workflow artifacts
- which attribution sources are too optimistic
- what minimum workflow changes would materially improve attribution quality

This report preserves the current attribution architecture as the baseline.

## Evidence Surface

Validated workflow:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

Observed surfaces:

- `catalog/source_fetch_log.csv`
- `catalog/bindings.csv`
- `catalog/qa_queue.csv`
- `manifests/onboarding_state.json`
- [2026-06-11-operational-observation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-operational-observation-report-v0.md)
- [2026-06-11-attribution-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-architecture-v0.md)

## Validation Method

This pass uses one strict rule:

- attribution strength must come from what the workflow artifacts actually persist

That means:

- no credit for fields the architecture wants but the workflow does not emit
- no promotion from workflow ownership alone
- no treating review timestamps or notes as explicit actor identity

For this report:

- `realistic`
  - the attribution mechanism can be supported directly by current workflow surfaces
- `too_optimistic`
  - the attribution mechanism exists in the architecture but current workflow artifacts do not support it strongly enough
- `minimum_change`
  - the smallest workflow-surface improvement that would materially raise attribution quality

## Workflow Snapshot

Observed from current workflow artifacts:

- `5` source-fetch success rows
- `112` accepted binding rows
- `1` workflow-level non-terminal publish-readiness outcome

Relevant attribution facts:

- `source_fetch_log.csv` persists source metadata and status, but no actor identity
- `bindings.csv` persists:
  - `status`
  - `derived_row_review_decision`
  - `derived_row_review_status`
  - `derived_row_reviewed_at`
  - `review_notes`
  - but no reviewer identity
- `onboarding_state.json` persists workflow phase state, but no actor identity

## Attribution Mechanism Results

### 1. `deterministic_system_step`

Verdict:

- `realistic`

Observed support:

- source-fetch success rows are clearly system-owned
- publish-readiness evaluation is also structurally system-owned

Resulting confidence:

- `medium`

Why not `strong`:

- the workflow artifacts do not persist an explicit subject field
- system ownership is deterministic, but not explicitly recorded as a subject-emitting event

Real examples in this workflow:

- `5` source fetch outcomes
- `1` publish-readiness outcome

## 2. `explicit_review_record`

Verdict:

- `too_optimistic`

Observed support:

- accepted binding rows persist review decision metadata and timestamps
- accepted binding rows do not persist reviewer identity

Resulting confidence under current artifacts:

- not `strong`
- at best `weak`

Why:

- `derived_row_reviewed_at` proves review timing
- `derived_row_review_decision = accept_candidate` proves review outcome
- `review_notes = auto-applied recommended candidate` provides context
- none of those fields identify who performed the review

Operational consequence:

- accepted binding success is currently a recoverable transition outcome
- but not a strong subject-attributed qualification event

Real examples in this workflow:

- `112` accepted binding rows

## 3. `explicit_approval_record`

Verdict:

- `too_optimistic`

Observed support:

- none in this workflow

Why:

- there is no persisted approver identity surface
- there is no explicit approval event row

Resulting confidence:

- `unknown`

## 4. `explicit_actor_argument`

Verdict:

- `too_optimistic` for the observed workflow family

Observed support:

- none in persisted workflow artifacts

Why:

- the current draft surfaces do not preserve actor arguments as durable event fields
- even if an execution surface knew the actor in memory, that attribution is not surviving into the workflow artifacts used here

Resulting confidence:

- `unknown`

## 5. `workflow_owned_default`

Verdict:

- `realistic`, but only as a weak fallback

Observed support:

- workflow family and execution ownership provide contextual hints
- they do not prove who performed a specific accepted binding or review action

Resulting confidence:

- `weak`

Important limitation:

- this mechanism should not be allowed to produce `medium` or `strong` attribution by itself
- otherwise it turns contextual ownership into fictional subject identity

This is the main architectural risk in the current attribution model.

## 6. `unknown_at_capture`

Verdict:

- `realistic`

Observed support:

- most accepted binding outcomes do not carry enough identity evidence
- the workflow still contains real transition outcomes worth preserving

Resulting confidence:

- `unknown`

Why this is correct:

- using `unknown_subject` is more honest than inventing a reviewer or owner
- the architecture is right to preserve the event even when attribution is weak

## Confidence Distribution Under Current Workflow Evidence

### Strong

Observed:

- `0`

Reason:

- no workflow artifact in this validation surface persists explicit reviewer, approver, or actor identity for the observed transition outcomes

### Medium

Observed:

- `6`

Composition:

- `5` deterministic source-fetch outcomes
- `1` deterministic publish-readiness outcome

Reason:

- the execution ownership is structurally clear
- the actor is not explicitly persisted

### Weak

Observed:

- `112`

Composition:

- accepted binding outcomes

Reason:

- the workflow preserves review timing and decision context
- it does not preserve reviewer identity
- the best available attribution is weak contextual ownership, not explicit subject identity

### Unknown

Observed:

- no additional transition-outcome class beyond the weak binding set was needed in this workflow
- but `unknown` remains the correct fallback whenever even weak contextual ownership is not defensible

## Which Attribution Mechanisms Are Realistic

Realistic today:

- `deterministic_system_step`
  - supports `medium` attribution for clearly system-owned steps
- `workflow_owned_default`
  - supports `weak` attribution only
- `unknown_at_capture`
  - supports honest preservation of unattributed outcomes

These mechanisms survive the validation pass.

## Which Attribution Mechanisms Are Too Optimistic

Too optimistic under the current workflow family:

- `explicit_review_record`
- `explicit_approval_record`
- `explicit_actor_argument`

Reason:

- the architecture can describe them
- the current workflow artifacts do not yet emit enough identity information to realize them

This is not a model failure.

It is a workflow-emission gap.

## Minimum Workflow Changes That Would Materially Improve Attribution Quality

### 1. Persist reviewer identity on binding-review writes

Minimal change:

- add an explicit reviewer field when accepted binding decisions are written

Why this matters:

- it would convert the largest current weak-attribution class (`112` accepted bindings) from weak toward strong

### 2. Persist actor identity on source-intake writes when available

Minimal change:

- add explicit actor or execution-subject persistence to source-intake event writes

Why this matters:

- deterministic source steps would move from inferred-medium toward explicit-strong

### 3. Persist publish-readiness outcomes as explicit event rows

Minimal change:

- write a durable publish-readiness event row with subject, basis, and evidence reference at evaluation time

Why this matters:

- it would eliminate reconstruction for the current workflow-level non-terminal inspection outcome

### 4. Normalize `subject_attribution_basis` into bounded values at write time

Minimal change:

- emit bounded attribution-source values instead of relying on later interpretation of free-form notes

Why this matters:

- it would make attribution quality easier to audit and compare across workflow families

## Architectural Findings

### 1. `unknown_subject` should remain revisable

This validation supports a non-terminal reading of `unknown_subject`.

Reason:

- unattributed outcomes are still valuable history
- later workflow improvements may allow stronger attribution on future rows
- the correct response to weak attribution is better capture, not deletion of earlier evidence

### 2. `workflow_owned_default` is the weakest safe fallback

This validation supports keeping it only under one condition:

- it must remain explicitly `weak`

It should not imply:

- named human ownership
- deterministic system ownership
- qualification-grade subject certainty

If it is allowed to drift above `weak`, it becomes fictional attribution.

### 3. The attribution architecture is realistic, but the workflow family is not emitting enough identity

The architecture survived this pass.

The weak point is still:

- execution-time identity persistence

Not:

- state model
- transition model
- qualification model

## Verdict

Attribution Architecture v0 survives as a baseline.

What holds:

- deterministic system-owned attribution is realistic
- weak and unknown attribution remain necessary categories
- the current workflow family can support a conservative attribution model without fabricated certainty

What remains weak:

- explicit review attribution
- explicit approval attribution
- explicit actor-argument persistence

Operational conclusion:

- the next leverage point is not more qualification policy
- it is workflow-surface identity emission at the moment the event is written
