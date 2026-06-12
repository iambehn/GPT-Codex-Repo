# Qualification Threshold Calibration Report v0

Date: 2026-06-11
Status: Baseline calibration complete
Scope: Conservative `Q1` / `Q2` / `Q3` evidence requirements for `subject x transition` records

## 1. Objective

Calibrate conservative qualification promotion thresholds using the current evidence-plane results without changing the committed qualification architecture.

This report defines:

- what minimum evidence should count toward `Q1`
- what additional evidence should be required for `Q2`
- what stronger evidence should be required for `Q3`

This report preserves:

- the current qualification ladder
- the current `subject x transition` qualification unit
- the existing support-versus-execution and control-versus-execution doctrine

It does not add:

- planning
- inventory
- staffing
- scheduling

## 2. Evidence Surface

Primary baseline:

- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)
- [2026-06-11-qualification-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-qualification-validation-report-v0.md)
- [2026-06-11-evidence-density-improvement-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-evidence-density-improvement-report-v0.md)

Key observed calibration facts:

- before instrumentation, qualification evidence for the measured `118` event classes was mixed:
  - `112 medium`
  - `6 weak`
- after the first three generator slices, the same event classes become:
  - `118 medium`
  - `0 weak`
- explicit strong human attribution remains narrow
- deterministic system-owned attribution is now stable and native for several surfaces

## 3. Calibration Doctrine

This pass uses four conservative rules.

### 3.1 Literal transition evidence beats semantic similarity

Promotion should only count evidence that maps to the literal transition being qualified.

That means:

- native canonical event emission counts
- compact ledger-compatible reconstruction may support `Q1`
- semantic "this looks similar" does not support `Q2` or `Q3`

### 3.2 Subject attribution remains mandatory for promotion

Unattributed outcomes may remain operationally useful, but they should not promote qualification.

That means:

- `unknown_subject` can support historical observation
- it does not support promotion above `Q0`

### 3.3 Medium deterministic attribution is now real evidence

The generator rollout established a stable middle class of evidence:

- canonical event emission
- bounded deterministic subject identity
- timestamp
- evidence reference

That class is strong enough for conservative `Q1` and guarded `Q2` consideration.
It is not strong enough by itself for broad `Q3` claims on human-judgment transitions.

### 3.4 `Q3` should stay materially harder than current rollout evidence

The evidence-density improvement report proves generator quality improved.
It does not prove routine production trust for most transitions.

`Q3` therefore should remain reserved for:

- repeated literal transition success
- bounded negative-inspection frequency
- multi-run stability
- limited rescue dependence

## 4. Minimum Qualifying Event Shape

For a `subject x transition` record to count toward any promotion threshold, the event should include:

- `subject`
- `transition_id`
- `inspection_result`
- `timestamp`
- `evidence_reference`

And should satisfy:

- subject attribution is not `unknown_subject`
- transition identity is not deferred
- the event maps to a literal committed transition, not only a semantic analogue

If any of those conditions fail:

- the event may remain in the operational history
- but it should not count toward promotion

## 5. Conservative Thresholds

## 5.1 `Q1` threshold

`Q1` should mean:

- one attributable literal transition success exists
- but evidence is still narrow enough that use remains trial-only

Minimum evidence requirement:

- at least `1` qualifying event for the exact `subject x transition`
- attribution at least `medium` or better
- at least one non-negative inspection outcome:
  - `pass`
  - or another explicitly accepted non-terminal outcome if the transition semantics define it as success
- no direct evidence that the same subject immediately failed the same transition for the same reason in the same bounded scenario

Interpretation:

- `Q1` is now evidence-backed trial trust
- not architectural plausibility alone

## 5.2 `Q2` threshold

`Q2` should mean:

- the subject has demonstrated limited repeatability for the exact transition
- guardrails are still required

Minimum evidence requirement:

- at least `3` qualifying successful events for the exact `subject x transition`
- those events must come from at least `2` distinct workflow runs
- all counted events must have native canonical emission or ledger-compatible literal reconstruction
- all counted events must have attribution at `medium` or `strong`
- at least `2` of the `3` counted events must have non-negative inspection outcomes without rescue
- no more than `1` counted negative inspected outcome in the same evidence set if that outcome is attributable to the subject rather than support/control conditions
- explicit `guardrails` and `demotion_signals` must be written for that record

Interpretation:

- `Q2` is the first level that should require repeat evidence
- one workflow family with one successful path is not enough

## 5.3 `Q3` threshold

`Q3` should mean:

- the subject performs the exact transition routinely enough for normal production trust

Minimum evidence requirement:

- at least `8` qualifying successful events for the exact `subject x transition`
- those events must come from at least `3` distinct workflow runs
- all counted events must have native canonical emission
- all counted events must have attribution at `medium` or `strong`
- for human-judgment transitions, at least `2` counted events must be `strong` rather than only deterministic `medium`
- the recent counted evidence set must show:
  - no attributable `blocked` outcome caused by the subject
  - no more than `1` attributable negative inspection outcome
  - no repeated unusual rescue pattern
- failure review must show no unresolved clustering in:
  - `inspection`
  - `execution`
  - `requirement`

Interpretation:

- `Q3` should require multi-run repeatability, not just improved capture quality
- deterministic system subjects may reach `Q3` without human-style strong identity, but only if literal repeat success and low negative-inspection frequency are both clear

## 6. Subject-Family Reading

Thresholds should be read slightly differently by subject family while preserving the same ladder.

### `system_validator`

Implication:

- deterministic `medium` attribution is a legitimate promotion input
- lack of human-style `strong` identity is not disqualifying by itself

Most realistic next use:

- conservative future `Q2` candidates once repeated literal success exists across multiple runs

### `codex_structured_worker`

Implication:

- deterministic attribution can support `Q1` and `Q2`
- `Q3` still requires repeated literal transition success without unstable rescue dependence

### `human_editor`

Implication:

- `medium` evidence can support `Q1`
- `Q2` can be considered with repeated medium-or-strong attributed outcomes
- `Q3` should not be claimed from medium-only evidence; explicit human-attributed rows are required

### `manager_approver`

Implication:

- lifecycle and governance transitions remain especially conservative
- explicit approval or authorization evidence should remain the main promotion path

## 7. Immediate Calibration Consequences

### 7.1 No broad matrix promotion is justified yet

The evidence-density improvement report shows:

- much better emitted evidence shape
- not yet broad multi-run transition trust

So:

- current thinly grounded `Q1` rows can become more operationally meaningful
- many existing `Q2` rows should still be treated as placeholders pending repeated literal evidence
- any live `Q3` interpretation remains too strong

### 7.2 The first realistic `Q2` candidates are deterministic surfaces

Because the rollout now emits stable deterministic evidence for:

- source-fetch success
- publish-readiness outcomes
- auto-applied accepted binding outcomes

the first practical `Q2` candidates will likely be:

- deterministic transitions with native emission
- repeated across multiple workflow runs
- with low attributable negative-inspection frequency

### 7.3 Human-judgment transitions still need stronger identity capture for `Q3`

The accepted-binding slice improved evidence materially, but the measured historical workflow family remained auto-applied.

That means:

- human-editor transitions are better evidenced than before
- but still not ready for `Q3` calibration from the current data

## 8. Recommended Record-Level Evaluation Order

When evaluating a `subject x transition` record for promotion:

1. Confirm the event is literal, not merely semantic.
2. Confirm subject attribution is not `unknown_subject`.
3. Confirm inspection/outcome semantics define the event as success for that transition.
4. Count only native or ledger-compatible qualifying events.
5. Separate support/control failures from execution trust failures.
6. Check whether the record meets the calibrated `Q1`, `Q2`, or `Q3` threshold.
7. If it does not, leave the current level unchanged.

## 9. Main Result

The generator rollout materially changes qualification calibration.

Before:

- many qualification discussions were architecture-ready but evidence-thin

After:

- `Q1` can now be grounded in native emitted event evidence
- `Q2` can now be defined as repeat literal success under guardrails
- `Q3` can now be defined more concretely and kept conservative

The main calibration outcome is:

- promotion thresholds should now be explicit
- but promotion itself should remain conservative until multi-run literal evidence accumulates

## 10. Recommended Next Step

Do not widen instrumentation scope first.

Preferred next move:

- apply these calibrated thresholds to newer instrumented workflow runs and test whether any specific `subject x transition` records now meet `Q1` or `Q2` under the stricter literal-evidence rule
