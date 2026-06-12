# Post-Transfer Bounded Replayability Widening Decision Report v0

Date: 2026-06-14
Status: completed
Scope: post-transfer widening decision only

## Objective

Choose the smallest justified next validation step after:

- bounded `call_of_duty` multi-clip proof
- one bounded `marvel_rivals` transfer proof

Non-goals:

- execute another validation run
- change replay, review, or export behavior
- claim publish readiness

## Candidate Next Steps

1. validate a second bounded `marvel_rivals` clip
2. widen again to a third game
3. pivot to publish workflow validation

## Decision Inputs

### `call_of_duty`

Current bounded state:

- replayability = multi-clip proven

Interpretation:

- more `call_of_duty` repetition is now lower information gain

### `marvel_rivals`

Current bounded state:

- adjacent-game transfer = proven on `1` clip
- multi-clip stability = unproven

Interpretation:

- the current uncertainty is no longer first transfer
- it is repeatability inside the adjacent-game pack

### `valorant`

From [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md):

- `valorant` remains `not ready`

Interpretation:

- widening to a third game is not justified

## Option Assessment

### Option 1: second bounded `marvel_rivals` clip

Pros:

- highest remaining information gain
- tests whether the adjacent-game transfer is stable rather than accidental
- stays within a game already shown to transfer once

Cons:

- `marvel_rivals` pack is weaker than `call_of_duty`
- failures may still reflect pack maturity as well as replayability limits

### Option 2: widen to a third game

Pros:

- broader surface diversity

Cons:

- not justified while `marvel_rivals` is still single-sample only
- `valorant` remains explicitly not ready
- mixes transfer uncertainty with unsupported-pack uncertainty

### Option 3: pivot to publish workflow validation

Pros:

- exercises a different unproven surface

Cons:

- publish workflow remains a larger scope jump
- the replayability track still has an obvious lower-cost unresolved question
- publishing does not answer whether adjacent-game replayability is stable

## Decision

Recommended next widening step:

- validate a second bounded real-media `marvel_rivals` clip

Reason:

- `call_of_duty` is already multi-clip proven
- `marvel_rivals` has crossed first-transfer proof but has not yet crossed
  repeatability proof
- one more `marvel_rivals` clip is the narrowest way to convert
  adjacent-game transfer from `single-sample` to `multi-sample`

## Scope Guardrails

This recommendation is limited to:

- one additional bounded `marvel_rivals` clip
- the existing replayable proof-path surfaces only:
  - runtime analysis
  - fusion analysis
  - local review apply
  - local export
  - replay validation

Not justified:

- `valorant`
- multi-game rollout
- publish automation
- production-readiness claims

## Recommended Next Goal

```text
/goal Run a second bounded real-media marvel_rivals clip through the current runtime-analysis, fused-review, local export, and replay-validation path, and determine whether the replayable proof path is repeatable beyond the first adjacent-game sample. Capture runtime outputs, fusion outputs, review outputs, replay artifacts, export outputs, and observed failure modes. Do not treat outputs as publish-cleared.
```

## Conclusion

Classification:

- next widening decision = `second marvel_rivals clip justified`

The replayability track has moved past first transfer.

The next high-value question is whether the transfer is repeatable inside the
same adjacent-game family before any broader widening or publish-facing work.
