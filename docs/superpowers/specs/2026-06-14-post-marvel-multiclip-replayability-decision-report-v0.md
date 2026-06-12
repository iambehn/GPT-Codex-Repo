# Post-Marvel Multi-Clip Replayability Decision Report v0

Date: 2026-06-14
Status: completed
Scope: replayability next-step decision only

## Objective

Decide the smallest justified next step after:

- bounded `call_of_duty` multi-clip replayability proof
- bounded `marvel_rivals` multi-clip replayability proof

Non-goals:

- execute another validation run
- change replay, review, or export behavior
- claim publish readiness

## Candidate Next Steps

1. widen to a third game
2. run more same-game clips
3. stop widening and checkpoint the replayability track

## Decision Inputs

### `call_of_duty`

Current bounded state:

- multi-clip proven

### `marvel_rivals`

Current bounded state:

- multi-clip proven

### Third-game readiness

From [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md):

- `valorant` remains `not ready`

Interpretation:

- there is no justified third-game widening target today

## Option Assessment

### Option 1: widen to a third game

Pros:

- highest theoretical breadth

Cons:

- no third game is currently justified
- would widen beyond the current evidence gate

### Option 2: run more same-game clips

Pros:

- increases within-game confidence

Cons:

- materially lower information gain now that both active games are already
  multi-clip proven
- risks turning bounded validation into indefinite repetition

### Option 3: stop widening and checkpoint

Pros:

- matches the current evidence boundary
- avoids forcing a third-game validation without a ready target
- preserves a clean replayability closeout point

Cons:

- leaves publish workflow and third-game transfer unresolved

## Decision

Recommended next step:

- stop widening the replayability track and checkpoint it

Reason:

- the active bounded target has been met on two games:
  - `call_of_duty`
  - `marvel_rivals`
- further widening is not justified while `valorant` remains not ready
- additional same-game repetition would add less information than the current
  two-game multi-clip proof already provides

## Scope Guardrails

Not justified now:

- `valorant`
- multi-game rollout claims
- publish automation
- production-readiness claims

Justified next work after checkpoint:

- status-only closeout of the replayability track
- or transition to a different unresolved surface such as publish workflow
  diagnosis, if explicitly chosen

## Recommended Interpretation

Classification:

- replayability widening cycle = `complete for current supported surfaces`

This is not a claim that the full pipeline is complete.

It is a claim that the current bounded replayability question has been answered
as far as the repo's supported game surfaces currently allow.

## Conclusion

The correct next step is not another widening run.

The correct next step is to checkpoint the replayability results and stop,
because the next unresolved constraints are no longer bounded replay transfer
inside currently supported games.
