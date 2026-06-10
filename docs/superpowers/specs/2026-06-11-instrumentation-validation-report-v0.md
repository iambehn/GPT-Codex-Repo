# Instrumentation Validation Report v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Validate the accepted-binding instrumentation slice against real workflow execution and determine whether emitted reviewer identity, transition events, inspection results, and attribution fields now produce stronger qualification evidence than the pre-instrumentation baseline.

This report evaluates:

- whether accepted binding writes now emit native evidence-plane fields
- whether attribution quality improves relative to the pre-instrumentation baseline
- whether qualification evidence quality improves relative to the pre-instrumentation baseline
- whether the implemented binding-write mapping remains compatible with Transition Event Capture v0 and Transition Outcome Ledger v0

This report preserves the implementation as the baseline unless observed failures require change.

## Evidence Surface

Implementation under test:

- [pipeline/derived_row_review.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/derived_row_review.py)

Validation surfaces:

- [tests/test_game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/tests/test_game_onboarding.py)
- [tests/test_onboarding_review_goldset.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/tests/test_onboarding_review_goldset.py)
- [2026-06-11-operational-observation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-operational-observation-report-v0.md)
- [2026-06-11-attribution-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-validation-report-v0.md)
- [2026-06-11-transition-event-capture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-event-capture-v0.md)
- [2026-06-11-transition-outcome-ledger-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-v0.md)

Execution paths exercised:

- explicit accepted review path
  - `GameOnboardingTests.test_apply_derived_row_review_accept_candidate_resolves_selected_row`
- auto-applied accepted review path
  - `GameOnboardingTests.test_apply_derived_row_review_accept_recommended_uses_single_recommended_candidate`

## Validation Method

This pass uses one strict rule:

- only emitted workflow fields count

That means:

- no credit for fields still recoverable only through later reconstruction
- no credit for fields that exist in architecture docs but are not present on the accepted binding write surface
- no promotion from contextual ownership alone when explicit or deterministic evidence is not emitted

For this report:

- `strong`
  - explicit subject identity plus canonical event fields are emitted directly by the binding write
- `medium`
  - deterministic subject attribution plus canonical event fields are emitted directly by the binding write
- `weak`
  - accepted binding outcome still requires later subject or event interpretation
- `baseline`
  - pre-instrumentation observation and attribution results from the earlier workflow reports

## Pre-Instrumentation Baseline

From the operational observation and attribution validation baseline:

- accepted binding outcomes observed: `112`
- explicit subject-attributed accepted binding outcomes: `0`
- best available attribution quality for accepted binding outcomes: `weak`

Why the baseline was weak:

- bindings persisted:
  - review decision
  - review timestamp
  - review notes
- bindings did not persist:
  - reviewer identity
  - canonical `transition_id`
  - canonical `input_state`
  - canonical `output_state`
  - normalized `inspection_result`
  - normalized `failure_family`
  - explicit event identifier

Operational implication before implementation:

- accepted binding outcomes were useful
- but they were not strong qualification evidence

## Implemented Accepted-Binding Emissions

The binding-write path now stamps these fields on the accepted binding row:

- `reviewed_by_subject`
- `subject`
- `subject_kind`
- `subject_attribution_basis`
- `event_id`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`
- `capture_source`

Current canonical mapping in the implementation:

- `transition_id = TRANS-016`
- `input_state = review_pack_ready`
- `output_state = review_pack_mixed_status`
- `inspection_result = pass`
- `failure_family = none`

## Execution Results

### 1. Explicit accepted review path

Observed in test execution:

- `reviewed_by_subject = human_editor`
- `subject = human_editor`
- `subject_kind = explicit`
- `subject_attribution_basis = explicit_review_record`
- `transition_id = TRANS-016`
- `input_state = review_pack_ready`
- `output_state = review_pack_mixed_status`
- `inspection_result = pass`
- `failure_family = none`
- `rescue_required = False`
- `event_id` emitted
- `evidence_reference` emitted as the review file path

Attribution quality:

- `strong`

Qualification-evidence quality:

- `strong`

Reason:

- explicit reviewer identity is persisted at the write surface
- canonical transition and inspection fields are emitted directly
- no later reconstruction is required to explain who performed the accepted review or what transition outcome was recorded

### 2. Auto-applied accepted review path

Observed in test execution:

- `reviewed_by_subject = codex_structured_worker`
- `subject = codex_structured_worker`
- `subject_kind = deterministic`
- `subject_attribution_basis = deterministic_system_step`
- `transition_id = TRANS-016`
- `inspection_result = pass`
- canonical input/output state emitted

Attribution quality:

- `medium`

Qualification-evidence quality:

- `medium`

Reason:

- the workflow emits deterministic system-owned attribution directly
- subject identity is not human-explicit, but it is no longer weak or reconstructed

## Before / After Comparison

| accepted binding path | pre-instrumentation attribution | post-instrumentation attribution | pre-instrumentation qualification evidence | post-instrumentation qualification evidence |
| --- | --- | --- | --- | --- |
| manual accepted review | `weak` | `strong` | `weak` | `strong` |
| auto-applied accepted review | `weak` | `medium` | `weak` | `medium` |

## Contract Compatibility

### Transition Event Capture v0

The implemented binding-write slice now emits the core capture-aligned fields directly on the accepted binding row:

- `event_id`
- `subject`
- `subject_attribution_basis`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`

Remaining difference:

- the implementation uses `reviewed_by_subject` plus `subject_kind` as workflow-surface convenience fields in addition to the capture baseline

Conclusion:

- compatible with Transition Event Capture v0
- no conflicting second event schema was introduced

### Transition Outcome Ledger v0

The emitted fields are sufficient to derive a ledger-grade row for the accepted binding outcome without semantic reconstruction.

Conclusion:

- compatible with Transition Outcome Ledger v0

## Observed Improvements

### 1. Native evidence emission now exists for accepted binding writes

This is the main implementation milestone.

Before:

- accepted binding evidence had to be interpreted after the fact

After:

- accepted binding writes emit event-grade identity and transition fields directly

### 2. Explicit reviewer identity is now available where the workflow supplies it

This is the highest-leverage change in the slice.

It directly addresses the earlier bottleneck:

- many accepted outcomes
- weak attribution

### 3. Deterministic auto-accepts are no longer weakly attributable

They are not fully strong, but they are now:

- explicit deterministic
- canonical
- event-grade

That is materially better than the baseline.

## Remaining Risks

### 1. Transition semantics still need broader operational confirmation

Current implementation maps accepted binding writes to:

- `TRANS-016`
- `review_pack_ready -> review_pack_mixed_status`

This is coherent with the current aggregate mixed-status review model, but still deserves broader validation against production onboarding workflow behavior.

This is not a blocker for the slice.

It is the next semantic question worth pressure-testing.

### 2. The improvement is validated for the accepted-binding write path, not the whole workflow family

The current result should not be overclaimed.

Validated now:

- accepted binding instrumentation slice

Not yet validated:

- publish-readiness outcome emission
- source-fetch event-grade emission
- broader qualification-history accumulation over many real workflow runs

## Verdict

The accepted-binding instrumentation slice materially improves emitted evidence quality.

What changed:

- accepted binding writes now generate native evidence-plane fields
- explicit manual review can produce `strong` attribution and qualification evidence
- deterministic auto-accept can produce `medium` attribution and qualification evidence

What did not change:

- broader workflow-family instrumentation still remains incomplete
- the current slice does not yet prove full evidence-plane coverage across onboarding

Operational conclusion:

- the architecture is no longer waiting on this generator for accepted binding outcomes
- the next discovery should come from validating the semantic transition mapping in broader workflow execution and then extending instrumentation to the next write surfaces
