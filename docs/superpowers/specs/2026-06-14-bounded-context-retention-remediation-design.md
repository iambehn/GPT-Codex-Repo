# Bounded Context-Retention Remediation Design

Date: 2026-06-14
Status: designed
Scope: bounded export-representation enhancement only

## Objective

Improve hook-layer viability for already approved exported highlights by
retaining a small amount of surrounding context around the approved moment.

Targeted downstream metrics:

- `context_sufficiency_score`
- `title_thumbnail_potential_score`
- `authenticity_risk_score`

This design is intentionally narrow. It does not attempt to improve highlight
quality broadly. It addresses the specific observed failure that approved
exports are mechanically valid but too context-thin to package honestly.

## Problem Statement

Recent bounded editorial analysis established a repeated rejection pattern
across approved exported highlights for both `call_of_duty` and
`marvel_rivals`.

Shared downstream hook result:

- `hook_mode = reject`
- `hook_archetype = other`
- `packaging_strategy = None`
- `rejection_reason = authenticity_risk_too_high`

Observed metric pattern:

- `context_sufficiency_score = 0.33`
- `title_thumbnail_potential_score = 0.4`
- `authenticity_risk_score ~= 0.77`

Current interpretation:

- the approved anchor moments are not necessarily wrong
- the exported representation of those moments is too thin
- the hook layer is being asked to package a point-event or near-point-event
  with too little setup and too little aftermath

## Design Classification

This is an `Export Representation Enhancement`.

It is not:

- a selection change
- a review-semantic change
- a replayability-contract change
- a threshold-tuning change
- a publish-readiness change

## Boundaries

Applies only to:

- approved exported highlights
- bounded `call_of_duty` proof-path outputs
- bounded `marvel_rivals` proof-path outputs

Does not change:

- replayability contract objects
- review-bridge semantics
- selection approval rules
- hook thresholds
- publish-readiness posture

Explicit non-goals:

- no new review-authored offset fields
- no game-wide editorial template system
- no multi-game widening beyond the current bounded set
- no attempt to solve weak core moment selection in this slice

## Invariants

### Anchor Invariance

The approved event remains the canonical reviewed object.

Context expansion may add surrounding media, but may not move, replace, or
redefine the approved anchor event.

This preserves:

- review semantics
- replay semantics
- auditability

### Expansion Boundedness

Context enrichment may not become implicit reselection.

All added context must remain inside explicit per-game caps.

### Export-Layer Scope

The intervention is downstream of approval.

It may alter exported window shape and exported metadata shape, but it may not
silently mutate the approved anchor set.

## Proposed Model

### Dual Window Representation

Each exported highlight should preserve two distinct windows:

1. `anchor_window`
   - the original approved event start/end
2. `context_window`
   - the derived export window used for downstream packaging and hook
     evaluation

The exported media window becomes `context_window`, while `anchor_window`
remains durable metadata for audit and replay interpretation.

This preserves the answer to both:

- what was approved?
- what was exported?

### Timing Expansion Policy

Policy name:

- `signal_aware_bounded_v1`

Expansion uses fixed hard caps with signal-aware placement inside those caps.

The anchor remains fixed. Only the surrounding context window is expanded.

## Per-Game Timing Caps

### `call_of_duty`

- maximum pre-roll: `1.5s`
- maximum post-roll: `2.0s`

### `marvel_rivals`

- maximum pre-roll: `2.0s`
- maximum post-roll: `2.5s`

These caps are intentionally conservative. They are meant to improve
interpretability without allowing context retention to become a hidden
selection rewrite.

## Signal-Aware Expansion Rules

### Pre-Roll Bias

Use available nearby evidence before the anchor to capture setup context.

Preferred pre-roll signals:

- explicit entity mention
- explicit ability or action signal
- clustered runtime or fused evidence immediately preceding the anchor

Pre-roll should answer:

- why is this about to matter?

### Post-Roll Bias

Use available nearby evidence after the anchor to capture payoff or aftermath.

Preferred post-roll signals:

- follow-through confirmation
- reaction or completion signal
- immediately adjacent second signal that clarifies why the anchor mattered

Post-roll should answer:

- did it actually matter?

### Minimum Fallback Padding

If no nearby qualifying signals exist, still apply a minimum context pad:

- `0.5s` pre-roll
- `0.75s` post-roll

This prevents zero-duration and point-duration export shapes from reaching the
hook layer unchanged.

## Export Artifact Changes

The export artifact should preserve existing fields and add bounded derived
context fields.

Required added fields:

- `anchor_start_seconds`
- `anchor_end_seconds`
- `context_start_seconds`
- `context_end_seconds`
- `context_expansion_seconds`
- `context_expansion_policy`
- `context_expansion_reasons`
- `context_signal_count`
- `context_pre_signal_types`
- `context_post_signal_types`

Field intent:

- `anchor_*`
  - durable review/reference timing
- `context_*`
  - actual exported packaging timing
- `context_expansion_seconds`
  - total added duration relative to the anchor window
- `context_expansion_reasons`
  - auditable explanation for why expansion occurred
- `context_signal_count`
  - lightweight evidence density summary for the contextualized window
- `context_pre_signal_types`, `context_post_signal_types`
  - explain which signal families justified the added context

Derived definition:

- `context_expansion_seconds = context_window_duration - anchor_window_duration`

## Expected Metric Movement

### `context_sufficiency_score`

Expected movement:

- should increase first and most reliably

Reason:

- the exported shape will retain setup and aftermath instead of collapsing to
  point moments

### `title_thumbnail_potential_score`

Expected movement:

- should improve modestly

Reason:

- additional context should make the moment easier to label and thumbnail
  honestly
- weak core moments may still remain weak

### `authenticity_risk_score`

Expected movement:

- should decrease

Reason:

- the hook layer should need less editorial invention when the exported shape
  contains visible supporting context and clearer payoff

Important nuance:

- authenticity risk is the explicit rejection trigger today
- but the design assumes that risk is being inflated upstream by context-thin
  export shapes rather than by threshold miscalibration alone

## Validation Plan

### Validation Scope

Validate only on the currently approved bounded export set for:

- `call_of_duty`
- `marvel_rivals`

No new game widening is part of this slice.

### Comparison Method

For each currently approved exported candidate:

1. preserve the `baseline export shape`
2. generate the `context-retained export shape`
3. re-run the same downstream hook and export evaluation on both

Compare the delta on:

- `context_sufficiency_score`
- `title_thumbnail_potential_score`
- `authenticity_risk_score`
- `hook_mode`
- `hook_archetype`
- `packaging_strategy`
- `rejection_reason`

### Success Criteria

Primary success criteria:

- median `context_sufficiency_score` increases
- median `authenticity_risk_score` decreases

Secondary success criteria:

- at least some approved exports stop collapsing to:
  - `hook_mode = reject`
  - `hook_archetype = other`
  - `packaging_strategy = None`

Guardrails:

- no change to the underlying approved anchor set
- no replay regression
- no cap violations
- no export shape that omits the anchor window

### Failure Interpretation

If `context_sufficiency_score` does not improve:

- the expansion policy is too weak
- or the available nearby evidence is too sparse

If context improves but `authenticity_risk_score` stays flat:

- the bottleneck is likely packaging semantics rather than context alone

If scores improve but `hook_mode` still rejects everything:

- this remediation is necessary but insufficient
- the next bounded surface should be packaging logic rather than replayability

If improvement only appears when expansions consistently push near the cap:

- the export layer is compensating for weak selection
- that should be treated as a separate upstream issue, not solved by widening
  the caps here

## Minimal Implementation Surface

Primary behavior surface:

- export-shape derivation for approved exported highlights

Likely affected modules:

- [highlight_selection_export.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/highlight_selection_export.py)
- [highlight_export_batch.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/highlight_export_batch.py)

No change should be required to:

- replay contract persistence
- review-bridge state semantics
- selection approval state machine

## Recommended Output Artifacts

Implementation should eventually produce:

1. bounded before/after comparison report
2. structured comparison ledger for baseline vs contextualized export shapes
3. regression verification covering:
   - anchor preservation
   - cap enforcement
   - replay compatibility
   - export metadata persistence

## Decision

Recommended next step:

- implement `signal_aware_bounded_v1` as a bounded export-representation
  enhancement
- validate it only against the currently approved `call_of_duty` and
  `marvel_rivals` export set

Do not:

- widen replayability scope
- lower hook thresholds
- reinterpret approved anchors
- widen to a third game in the same slice
