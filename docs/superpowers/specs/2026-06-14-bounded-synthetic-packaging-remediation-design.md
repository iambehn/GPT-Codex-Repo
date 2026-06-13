# Bounded Synthetic-Packaging Remediation Design

Date: 2026-06-14
Status: designed
Scope: bounded packaging-routing enhancement only

## Objective

Improve packaging usefulness for approved exported highlights that already clear
 rejection but still fall into `hook_mode = synthetic`.

This design targets the current post-context-retention bottleneck:

- `synthetic` is still too coarse

This design does not attempt to change the top-level hook policy. It only makes
 synthetic cases more specific, more archetype-aware, and more actionable for
 packaging.

## Problem Statement

The bounded context-retention remediation improved the current approved export
set from:

- `hook_mode = reject`

to:

- `hook_mode = synthetic`

across all four validated approved exports.

That means:

- export representation is no longer the dominant blocker
- authenticity risk is no longer the dominant blocker
- the remaining bottleneck is packaging classification inside `synthetic`

Observed remaining issues:

- `clarity_score` still misses the current `natural` threshold
- some cases still remain `hook_archetype = other`
- all synthetic cases currently collapse to the same fallback packaging
  strategy

## Design Classification

This is a `Packaging Routing Enhancement`.

It is not:

- a selection change
- a review change
- a replayability change
- a top-level hook policy change
- a threshold retune

## Policy Boundary

Keep these unchanged:

- `reject` gate
- `natural` gate
- top-level `hook_mode` definitions

Do not change:

- replayability contracts
- review semantics
- selection scope
- approval status
- publish-readiness posture

If repeated evidence later shows that `near_natural_contextual` cases should
 become `natural`, record that as a future hypothesis. Do not change that in
 this slice.

## Invariants

### Synthetic Mode Invariance

Subtype assignment must not change `hook_mode`.

Valid:

- `hook_mode = synthetic`
- `synthetic_subtype = near_natural_contextual`

Invalid:

- `hook_mode = natural`
- inferred only from subtype assignment

### Quality Status Invariance

Synthetic subtype routing may change:

- packaging strategy
- copy posture
- context framing

It may not change:

- approval status
- natural status
- publish readiness

### Additive Contract Invariance

The remediation must remain additive.

Required new surface:

- `synthetic_subtype`

Optional additive explanation surface:

- `synthetic_packaging_rationale`

Do not introduce a second scoring system parallel to hook scoring.

## Recommended Approach

Approach:

- `synthetic subtype routing`

Reason:

- the observed failure is that synthetic is too coarse
- not that the current natural or reject rules are wrong
- the smallest justified change is to refine routing behavior inside synthetic

## Proposed Synthetic Subtypes

The subtype set is intentionally small:

- `near_natural_contextual`
- `archetype_salvageable`
- `context_salvageable`
- `weak_synthetic`

## Subtype Definitions

### `near_natural_contextual`

Meaning:

- the candidate is close to natural packaging quality but still misses the
  current natural gate

Expected signal shape:

- strong context sufficiency
- strong payoff readability
- low enough authenticity risk
- misses natural mainly because clarity and/or title-thumbnail strength remain
  slightly below the current boundary

Packaging intent:

- package lightly
- use minimal scaffolding
- treat as almost self-sufficient, but not fully natural

### `archetype_salvageable`

Meaning:

- the moment is usable, but packaging is weakened mainly because the archetype
  assignment is still too generic

Expected signal shape:

- adequate context and payoff
- `hook_archetype = other`
- retained context suggests a more specific framing opportunity

Packaging intent:

- probe a stronger archetype cue
- frame explicitly around the likely moment type

### `context_salvageable`

Meaning:

- context retention helped enough to avoid rejection, but the candidate still
  depends heavily on explicit setup framing

Expected signal shape:

- synthetic after context retention
- acceptable payoff readability
- moderate clarity
- context is doing most of the rescue work

Packaging intent:

- explain setup more directly
- rely on explicit context framing to make the payoff legible

### `weak_synthetic`

Meaning:

- still synthetic, but not strong enough to justify confident packaging beyond
  a conservative fallback

Expected signal shape:

- above reject, but only barely
- weak title-thumbnail potential
- weak archetype support
- weak clarity even after context retention

Packaging intent:

- avoid overclaiming
- package conservatively

## Routing Order

Evaluate synthetic cases in this order:

1. `near_natural_contextual`
2. `archetype_salvageable`
3. `context_salvageable`
4. `weak_synthetic`

If no earlier subtype matches, default to:

- `weak_synthetic`

This precedence order is required so the same candidate does not route
 ambiguously across multiple subtype branches.

## Minimum Routing Logic

### 1. `near_natural_contextual`

Route here when:

- candidate is already `synthetic`
- context sufficiency is strong
- payoff readability is strong
- authenticity risk is comfortably below reject
- title-thumbnail and/or clarity are the only remaining blockers

Interpretation:

- almost natural, but still not eligible for natural packaging under the
  current gate

### 2. `archetype_salvageable`

Route here when:

- candidate is `synthetic`
- context and payoff are adequate
- `hook_archetype = other`
- retained context suggests a more specific framing opportunity

Interpretation:

- the packaging problem is primarily archetype specificity

### 3. `context_salvageable`

Route here when:

- candidate is `synthetic`
- context retention materially improved viability
- payoff is acceptable
- the candidate still depends on explicit setup framing to be legible

Interpretation:

- context is still the main rescue mechanism

### 4. `weak_synthetic`

Route here when:

- candidate remains `synthetic`
- and none of the stronger subtype conditions are met

Interpretation:

- do not force a stronger packaging claim than the evidence supports

## Packaging Strategy Mapping

### `near_natural_contextual`

Preferred strategies:

- `cold_open_payoff_then_context_caption`
- `tight_context_then_payoff`

Editorial posture:

- light scaffolding
- payoff-led
- minimal explanation

### `archetype_salvageable`

Preferred strategy:

- `archetype_probe_then_context_card`

Editorial posture:

- use the strongest plausible archetype cue
- support it with explicit context rather than broad hype

### `context_salvageable`

Preferred strategy:

- `setup_then_payoff_with_context_card`

Editorial posture:

- context-first
- explain why the moment matters
- avoid assuming the payoff is obvious on its own

### `weak_synthetic`

Preferred strategy:

- `low_claim_context_first`

Editorial posture:

- conservative
- low-claim
- avoid aggressive title or thumbnail implications

## Implementation Style

Use existing fields only:

- `clarity_score`
- `context_sufficiency_score`
- `payoff_readability_score`
- `title_thumbnail_potential_score`
- `authenticity_risk_score`
- `hook_archetype`
- `context_pre_signal_types`
- `context_post_signal_types`
- `context_signal_count`

Required additive field:

- `synthetic_subtype`

Optional additive field:

- `synthetic_packaging_rationale`

Do not add:

- a new top-level mode
- a parallel quality score
- a game-specific packaging ontology in this slice

## Validation Plan

### Validation Scope

Validate only on the current approved bounded export set for:

- `call_of_duty`
- `marvel_rivals`

### Validation Checks

Before vs after compare:

- `synthetic_subtype`
- `packaging_strategy`
- `hook_mode`
- `hook_archetype`
- `rejection_reason`

Required success conditions:

- synthetic cases become more specifically routed
- no case upgrades from `synthetic` to `natural`
- no case changes approval status
- no replayability regression
- no selection regression

### Observational Distribution Output

Record:

- `subtype_distribution`

Example:

- `near_natural_contextual = N`
- `archetype_salvageable = N`
- `context_salvageable = N`
- `weak_synthetic = N`

Purpose:

- observe whether the subtype system is actually differentiating cases
- not to change policy in this slice

## Guardrails Against Hidden Threshold Churn

- keep `reject` thresholds unchanged
- keep `natural` thresholds unchanged
- do not create a second scoring system parallel to hook scoring
- subtype routing must consume existing fields rather than invent latent
  quality states
- do not reinterpret subtype assignment as evidence of natural eligibility
- if repeated evidence suggests `near_natural_contextual` should enter natural,
  record a future hypothesis instead of changing the current policy

## Recommended Next Step

Implement bounded synthetic subtype routing inside the current `synthetic`
 branch only, then validate that:

- packaging strategy becomes more specific
- synthetic cases are differentiated meaningfully
- no top-level hook policy changed
