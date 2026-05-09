# Fusion Low-Value Event Suppression Design

## Goal

Add a global fusion-layer suppression rule for low-value event types that should remain visible as evidence but should not survive as meaningful fused highlight candidates when they appear alone.

Initial target:
- `pov_character_identified`

This change applies to all games.

## Problem

Current runtime and fusion behavior allows identity-only detections to surface as fused events with strong scores when a pack has broad hero-portrait coverage but weak highlight-event coverage.

Observed failure mode in `marvel_rivals`:
- runtime emits many `pov_character_identified` events from `hero_portrait`
- fusion preserves them as full fused events
- fused review prep then sees identity-heavy candidates rather than meaningful highlight evidence

The review-app cleanup already suppresses these events at the review-prep layer, but that is too late. The fused artifact itself should record that these events are low-value and non-promotable unless stronger evidence is present.

## Scope

In scope:
- global code-level suppression in `pipeline/fusion_analysis.py`
- preserve suppressed events in fused output
- add explicit suppression metadata to fused events
- force suppressed events into a non-promotable action outcome
- focused regression tests in `tests/test_fusion_analysis.py`

Out of scope:
- runtime event suppression
- per-game manifest changes
- review-app changes
- registry schema redesign unless current fused event payload cannot carry suppression metadata

## Design

### Policy shape

Introduce a global low-value suppression policy in fusion with:

- a small global suppressed event-type set
- a classifier that decides whether the fused event is identity-only / low-value
- a suppression transform that preserves provenance while preventing promotion

Initial suppressed event types:
- `pov_character_identified`

### Suppression condition

A fused event is suppressed when all of the following are true:

1. its `event_type` is in the global low-value event-type set
2. its contributing evidence is only contextual / identity-like
3. it lacks stronger companion evidence from a different event or producer family

For the first implementation, “stronger companion evidence” should mean at least one non-identity signal type or event family beyond the identity cluster itself.

Practical interpretation:
- identity-only cluster -> suppress
- identity + stronger combat/objective/ability/medal evidence -> do not suppress

### Suppression effect

Suppression must not delete the event. Instead it should:

- keep the fused event in `fused_events`
- preserve contributing signals and metadata
- add explicit suppression fields
- force the event into a non-promotable outcome

Required fused-event fields after suppression:

```json
{
  "suppressed": true,
  "suppression_reason": "low_value_identity_only",
  "suppression_policy": "global_low_value_event_filter_v1"
}
```

Expected scoring/action impact:
- `recommended_action` becomes `skip`
- `final_score` is forced low enough that downstream candidate-selection/export logic cannot treat it as a meaningful highlight

The exact score floor can be implementation-defined, but it must be deterministic and testable.

### Placement

Apply suppression after the fused event has already been built and explained, but before downstream action selection or export-facing interpretation depends on it.

This keeps:
- runtime evidence unchanged
- fusion explainability intact
- downstream review prep simpler

### Explainability contract

Suppressed events must remain inspectable in the fused artifact:

- original `event_type`
- original contributing signals
- gate/synergy fields
- suppression marker
- suppression reason

This avoids silent deletion and keeps the artifact auditable.

## Approaches Considered

### 1. Global fusion suppression

Pros:
- solves the real problem at the correct layer
- applies consistently across games
- preserves runtime provenance
- does not require duplicated per-pack rules

Cons:
- introduces one more global policy surface in fusion

Recommendation:
- choose this approach

### 2. Per-pack manifest suppression

Pros:
- very explicit per game
- fully data-driven

Cons:
- repeats the same rule across packs
- delays cleanup for every new game until its pack is edited

### 3. Runtime-stage suppression

Pros:
- removes noise earlier

Cons:
- alters upstream evidence
- makes identity context less available for debugging and later companion use

## Implementation Notes

Expected touch points:
- `pipeline/fusion_analysis.py`
- `tests/test_fusion_analysis.py`

Likely implementation shape:
- add a small global suppressed-event-type constant
- add a helper that inspects one fused event plus its matched signal context
- apply suppression transform to qualifying fused events

The first version should be intentionally narrow:
- only `pov_character_identified`
- no generic weighting DSL
- no manifest contract change

## Verification

Required tests:

1. identity-only fused event is suppressed
- event remains present
- `suppressed == true`
- `suppression_reason == low_value_identity_only`
- action becomes non-promotable

2. identity + stronger evidence is not suppressed
- event remains promotable if other scoring supports it

3. suppression is global
- test should not depend on one specific game pack beyond the minimum fixture setup

4. fused artifact remains inspectable
- suppression fields are persisted alongside normal fused metadata

Artifact verification after implementation:
- rerun a bounded fused smoke pass against `marvel_rivals`
- confirm identity-only fused outputs carry suppression metadata
- confirm fused review prep sees no identity-only queue noise without relying on review-prep filtering

## Success Criteria

The change is successful when:

- identity-only fused events no longer surface as meaningful review/export candidates
- those events still remain visible in fused artifacts for provenance
- existing non-identity fused behavior remains unchanged
- review-prep filtering becomes a secondary safeguard, not the primary cleanup layer
