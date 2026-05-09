# Fusion Low-Value Event Suppression Implementation Plan

## Scope

Implement the approved global fusion-layer suppression rule from:

- [2026-05-10-fusion-low-value-event-suppression-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-10-fusion-low-value-event-suppression-design.md)

This plan adds a global code-level suppression pass for low-value fused event
types while preserving runtime evidence and fused artifact provenance.

Initial covered event type:

- `pov_character_identified`

## Desired End State

After this work:

- identity-only fused events still appear in fused artifacts
- those events are explicitly marked as suppressed
- those events cannot surface as meaningful fused candidates
- events with stronger companion evidence are not suppressed
- the behavior applies consistently across all games
- the fused artifact remains auditable and explainable

## Implementation Strategy

Implement this as a narrow fusion-layer policy in
[pipeline/fusion_analysis.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/fusion_analysis.py),
plus focused tests in
[tests/test_fusion_analysis.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/tests/test_fusion_analysis.py).

Keep the scope intentionally small:

1. define a global suppressed-event-type set
2. add a helper that classifies identity-only low-value fused events
3. apply a suppression transform after fused-event construction
4. persist suppression metadata on the fused event
5. add regression tests
6. rerun a bounded `marvel_rivals` smoke artifact to verify the real behavior

## Slice 1: Policy Constants

Update:

- [pipeline/fusion_analysis.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/fusion_analysis.py)

Add:

- one small global set of suppressed event types
- one policy identifier string, for example:
  - `global_low_value_event_filter_v1`

Initial membership:

- `pov_character_identified`

Do not add a manifest surface in this slice.

## Slice 2: Low-Value Classifier

Add one helper that evaluates whether a fused event should be suppressed.

Inputs should come from the already-built fused event plus its contributing
signal context. The helper should answer:

- is this event type globally suppressible?
- is the event supported only by contextual / identity-like evidence?
- does the event lack stronger companion evidence?

First-version rule:

- suppress when `event_type == pov_character_identified`
- and the matched/contributing evidence does not include stronger non-identity
  support

The helper should stay simple and deterministic. Do not create a general policy
DSL in this slice.

## Slice 3: Suppression Transform

Apply suppression after the fused event has already been built, but before
downstream promotion/action logic relies on the final event shape.

Suppression must:

- keep the fused event in `fused_events`
- preserve all existing contributing-signal and metadata fields
- add:
  - `suppressed: true`
  - `suppression_reason: low_value_identity_only`
  - `suppression_policy: global_low_value_event_filter_v1`

Suppression must also force a non-promotable outcome:

- `recommended_action` becomes `skip`
- `final_score` becomes a deterministic non-promotable value

If the current fused event payload does not already contain a
`recommended_action` field at this stage, update the relevant fusion/export
surface in the smallest possible way so the suppressed outcome is explicit and
testable.

## Slice 4: Companion-Evidence Preservation

Ensure suppression does not trigger when stronger evidence exists.

For the first implementation, “stronger evidence” should mean at least one
non-identity signal or event family present in the fused cluster/context.

Expected outcomes:

- identity-only -> suppressed
- identity + medal/combat/objective/ability support -> not suppressed

This rule must remain explainable from the fused event alone.

## Slice 5: Tests

Update:

- [tests/test_fusion_analysis.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/tests/test_fusion_analysis.py)

Add focused tests for:

1. identity-only fused event is suppressed
   - event remains present
   - `suppressed == true`
   - `suppression_reason == low_value_identity_only`
   - resulting action is non-promotable

2. identity + stronger evidence is not suppressed
   - event remains promotable if scoring supports it

3. suppression is global
   - behavior does not depend on a specific game-pack quirk

4. fused event remains inspectable
   - suppression fields coexist with existing gate/synergy/contributing-signal
     fields

Keep tests narrow and local to fusion behavior.

## Focused Verification

Run:

```bash
python3 -m py_compile \
  pipeline/fusion_analysis.py \
  tests/test_fusion_analysis.py
```

Run:

```bash
python3 -m unittest tests.test_fusion_analysis
```

If a narrower subset is sufficient during iteration, run the specific new test
cases first, then the full fusion test module before closing the task.

## Real Validation

After implementation:

1. rerun a bounded fused smoke path for `marvel_rivals`
2. inspect:
   - [absolute-cinema-source-match.fused_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_analysis/marvel_rivals/absolute-cinema-source-match.fused_analysis.json)
3. confirm identity-only fused outputs now carry:
   - suppression marker
   - suppression reason
   - non-promotable action outcome
4. confirm fused review prep returns no identity-only review queue noise even
   without relying on review-prep filtering

## Commit Strategy

Prefer one coherent implementation commit containing:

- fusion suppression logic
- suppression metadata shape
- focused fusion tests

Do not mix this change with unrelated runtime, registry, or review-app cleanup.

## Risks

### Risk: Suppression hides useful context

Mitigation:

- keep the event in the fused artifact
- suppress promotion, not evidence

### Risk: Over-broad rule harms future games

Mitigation:

- start with one event type only
- require stronger-evidence absence before suppression
- cover the behavior with explicit tests

### Risk: Suppressed outcome is only implicit

Mitigation:

- persist explicit suppression metadata
- force a deterministic non-promotable action/result
