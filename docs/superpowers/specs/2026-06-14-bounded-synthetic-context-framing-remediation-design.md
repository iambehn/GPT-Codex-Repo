# Bounded Synthetic Context-Framing Remediation Design

Date: 2026-06-14
Status: proposed
Scope: bounded synthetic framing refinement only

## Objective

Reduce overuse of `setup_then_payoff_with_context_card` for retained synthetic
candidates that remain viable only because context retention preserved enough
post-payoff evidence to avoid rejection.

This design refines framing strategy only inside the existing
`context_salvageable` path.

It does not revise:

- `hook_mode`
- `synthetic_subtype`
- selection policy
- review semantics
- replayability contract
- export eligibility

## Problem Statement

After:

- bounded context retention
- bounded synthetic subtype routing
- bounded archetype routing

the approved bounded export set now has:

- `hook_mode = synthetic` for all `4` approved exports
- `hook_archetype = chaos` for all `4` approved exports

But packaging still clusters at:

- `setup_then_payoff_with_context_card = 3`
- `cold_open_payoff_then_context_caption = 1`

The repeated unresolved shape is not missing context in general.

It is:

- viable synthetic cases with no retained pre-context cue
- at least one retained post-context cue
- moderate clarity and title-thumbnail strength
- acceptable payoff readability

Those cases currently receive the same setup-first framing template as richer
setup-bearing cases, even though their retained evidence does not justify the
same framing posture.

## Design Boundary

This slice is a framing-strategy refinement, not a subtype-policy revision.

It preserves:

- current `reject` thresholds
- current `natural` thresholds
- current `synthetic` thresholds
- current synthetic subtype assignment logic
- current archetype logic
- current replayability and export-shape contracts

It changes only:

- the packaging strategy chosen after a case has already been classified as:
  - `hook_mode = synthetic`
  - `synthetic_subtype = context_salvageable`

## Core Invariants

1. `hook_mode` must remain unchanged.
2. `synthetic_subtype` must remain unchanged.
3. approval/export status must remain unchanged.
4. replayability scope must remain unchanged.
5. selection scope must remain unchanged.
6. fallback behavior outside `context_salvageable` must remain unchanged.
7. no second routing system may be introduced.

## Current Evidence Pattern

The current repeated setup-first cases are:

1. `call_of_duty-bootstrap-real-cod`
2. `call_of_duty-second-bounded-clip`
3. `marvel_rivals-transfer-kjera`

Shared retained-shape evidence:

- `context_pre_signal_types = []`
- one retained post-context cue
- `clarity_score = 0.6`
- `context_sufficiency_score = 0.63`
- `title_thumbnail_potential_score = 0.5`
- `authenticity_risk_score ~= 0.49`
- acceptable payoff readability

The stronger counterexample is:

1. `marvel_rivals-transfer-absolute-cinema`

Shape:

- `synthetic_subtype = near_natural_contextual`
- one retained pre-context cue:
  - `character_identity`
- one retained post-context cue:
  - `team_wipe_visibility`
- materially stronger context/clarity/title-thumbnail profile
- current strategy:
  - `cold_open_payoff_then_context_caption`

This means the repeated issue is localized to the internal framing chosen for
post-only `context_salvageable` cases.

## Proposed Change

Keep `context_salvageable` assignment unchanged.

Split only the framing strategy used inside `context_salvageable`:

### Strategy A: `setup_then_payoff_with_context_card`

Use only when:

- `context_pre_signal_types` contains at least one meaningful retained
  pre-context cue

Interpretation:

- the clip has enough setup evidence to justify explicit setup-first framing

### Strategy B: `low_claim_post_payoff`

Use when:

- `context_pre_signal_types` is empty
- and `context_post_signal_types` contains at least one retained post/payoff
  cue

Interpretation:

- the clip is viable
- the payoff or aftermath is real enough to preserve
- but setup is too thin to justify a full setup card
- framing should remain conservative and low-claim

## Decision Rule

This is a single post-subtype strategy split, not a second classification
layer.

Pseudo-rule:

```text
if hook_mode != synthetic:
  unchanged

if synthetic_subtype != context_salvageable:
  unchanged

if context_pre_signal_types is non-empty:
  packaging_strategy = setup_then_payoff_with_context_card

elif context_post_signal_types is non-empty:
  packaging_strategy = low_claim_post_payoff

else:
  packaging_strategy = setup_then_payoff_with_context_card
```

Important fallback rule:

- if both pre and post cues are absent, preserve current behavior rather than
  inventing a new fallback

## Why This Is The Smallest Useful Change

This design does not:

- add new synthetic subtypes
- alter top-level hook policy
- alter archetype logic
- alter context-retention timing
- alter export eligibility

It only recognizes that the current `context_salvageable` bucket still contains
two framing shapes:

1. setup-rich synthetic
2. post-only low-claim synthetic

The current system treats both as:

- `setup_then_payoff_with_context_card`

That is the overbroad behavior this design corrects.

## Expected Routing On The Current Bounded Set

### `call_of_duty-bootstrap-real-cod`

Current:

- `hook_mode = synthetic`
- `synthetic_subtype = context_salvageable`
- `packaging_strategy = setup_then_payoff_with_context_card`

Expected:

- `hook_mode = synthetic`
- `synthetic_subtype = context_salvageable`
- `packaging_strategy = low_claim_post_payoff`

Reason:

- no retained pre cue
- retained post cue:
  - `equipment_visibility`

### `call_of_duty-second-bounded-clip`

Current:

- `hook_mode = synthetic`
- `synthetic_subtype = context_salvageable`
- `packaging_strategy = setup_then_payoff_with_context_card`

Expected:

- `hook_mode = synthetic`
- `synthetic_subtype = context_salvageable`
- `packaging_strategy = low_claim_post_payoff`

Reason:

- no retained pre cue
- retained post cue:
  - `equipment_visibility`

### `marvel_rivals-transfer-kjera`

Current:

- `hook_mode = synthetic`
- `synthetic_subtype = context_salvageable`
- `packaging_strategy = setup_then_payoff_with_context_card`

Expected:

- `hook_mode = synthetic`
- `synthetic_subtype = context_salvageable`
- `packaging_strategy = low_claim_post_payoff`

Reason:

- no retained pre cue
- retained post cue:
  - `round_state_visibility`

### `marvel_rivals-transfer-absolute-cinema`

Current:

- `hook_mode = synthetic`
- `synthetic_subtype = near_natural_contextual`
- `packaging_strategy = cold_open_payoff_then_context_caption`

Expected:

- unchanged

Reason:

- this case is outside `context_salvageable`

## Validation Plan

Validation remains bounded to the current approved export set for:

- `call_of_duty`
- `marvel_rivals`

Compare before vs after on:

- `hook_mode`
- `synthetic_subtype`
- `packaging_strategy`
- `context_pre_signal_types`
- `context_post_signal_types`
- approval/export status unchanged

### Success Criteria

Primary:

- the three post-only `context_salvageable` cases no longer route to
  `setup_then_payoff_with_context_card`

Secondary:

- they reroute to `low_claim_post_payoff`
- `marvel_rivals-transfer-absolute-cinema` remains unchanged
- no case changes `hook_mode`
- no case changes `synthetic_subtype`
- no case changes approval/export status

## Guardrails Against Policy Drift

This slice must not:

- change `reject` thresholds
- change `natural` thresholds
- change synthetic subtype assignment rules
- create a second routing system
- reopen replayability work
- reopen export timing work
- reopen archetype routing work

If repeated future evidence suggests some post-only synthetic cases should move
to a different subtype or a different top-level mode, that should be recorded
as a later hypothesis, not changed in this slice.

## Artifact Set

This slice should produce:

1. one bounded design spec
2. one bounded implementation plan
3. one bounded validation report
4. one structured before/after comparison ledger

## Non-Goals

This design does not attempt to:

- make current outputs publish-ready
- widen to a third game
- revise hook taxonomy
- revise review criteria
- revise selection ranking

## Conclusion

The remaining framing issue is not that `context_salvageable` is misclassified.

It is that one setup-first framing template is currently serving two different
cue shapes.

The smallest correct intervention is therefore:

- keep `context_salvageable`
- preserve all top-level policy
- split only the packaging strategy used inside that subtype

That keeps the change:

- bounded
- explainable
- replay-safe
- review-safe
- selection-safe
