# Bounded Replayability Widening Decision Report v0

Date: 2026-06-14
Status: completed
Scope: bounded widening decision only

## Objective

Decide the smallest justified next widening step after bounded `call_of_duty`
multi-clip replayability validation.

Non-goals:

- execute adjacent-game validation
- execute publish workflow validation
- change runtime, replay, or review behavior

## Candidate Next Steps

1. validate one more bounded `call_of_duty` clip
2. validate one adjacent game

## Decision Inputs

### `call_of_duty` status

Current repo-backed state:

- runtime analysis: proven on `2` clips
- fusion analysis: proven on `2` clips
- local review application: proven on `2` clips
- editorial replay: proven on `2` clips
- historical export regeneration: proven on `2` clips

Interpretation:

- the replay contract itself is no longer the dominant uncertainty on the
  bounded `call_of_duty` path

### Adjacent-game status

From [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md):

- `marvel_rivals`:
  - `available but weaker`
  - canonical media contract is `partial`
- `valorant`:
  - `not ready`

Interpretation:

- `valorant` is not a justified next widening target
- `marvel_rivals` is the only plausible adjacent-game widening candidate

## Option Assessment

### Option 1: one more bounded `call_of_duty` clip

Pros:

- lowest operational risk
- increases confidence in clip-to-clip stability
- no new game-surface uncertainty

Cons:

- lower information gain
- risks overfitting validation effort to one already-proven game

### Option 2: one bounded `marvel_rivals` replayability validation

Pros:

- highest information gain
- directly tests whether the replay contract transfers across games
- exposes whether current proof is game-specific or contract-level

Cons:

- `marvel_rivals` pack is weaker than `call_of_duty`
- failures may mix replay issues with game-pack maturity issues

## Decision

Recommended next widening step:

- validate one bounded `marvel_rivals` clip

Reason:

- `call_of_duty` replayability is already multi-clip proven
- one more `call_of_duty` clip adds less information than one bounded adjacent
  game
- `marvel_rivals` is weaker, but it is still published and available, unlike
  `valorant`

## Scope Guardrails

This recommendation is limited to:

- one bounded `marvel_rivals` clip
- the existing replayable proof-path surfaces only:
  - runtime analysis
  - fusion analysis
  - local review application
  - local export
  - replay validation

Not justified:

- multi-game rollout
- `valorant`
- publish automation
- production-readiness claims

## Recommended Next Goal

```text
/goal Run one bounded real-media marvel_rivals clip through the current runtime-analysis, fused-review, local export, and replay-validation path, and determine whether the replayable proof path transfers beyond call_of_duty. Capture runtime outputs, fusion outputs, review outputs, replay artifacts, export outputs, and observed failure modes. Do not treat outputs as publish-cleared.
```

## Conclusion

Classification:

- next widening decision = `adjacent-game validation justified`

The bounded `call_of_duty` track has accumulated enough replayability evidence
that the next high-value question is cross-game transfer, not more same-game
repetition.
