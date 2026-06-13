# Post-Synthetic Packaging Next-Step Decision Report v0

Date: 2026-06-14
Status: completed
Scope: bounded next-step decision only

## Objective

Determine the smallest justified next intervention surface after:

- bounded context retention
- bounded synthetic subtype routing

This report does not widen scope to:

- publish readiness
- third-game validation
- review-semantic changes
- replayability changes
- top-level hook policy changes

## Decision Surface

Primary evidence:

- [2026-06-14-bounded-synthetic-packaging-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-14-bounded-synthetic-packaging-validation-report-v0.md)
- [20260613T011541Z.bounded_synthetic_packaging_comparison_ledger.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T011541Z/20260613T011541Z.bounded_synthetic_packaging_comparison_ledger.json)
- [2026-06-14-synthetic-packaging-remediation-analysis-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-14-synthetic-packaging-remediation-analysis-report-v0.md)
- [2026-06-14-editorial-quality-assessment-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-14-editorial-quality-assessment-report-v0.md)

## Current State

What is now true:

- approved exports no longer collapse to `hook_mode = reject`
- approved exports no longer collapse to one coarse synthetic packaging
  strategy
- all four approved bounded exports remain:
  - `hook_mode = synthetic`

Observed subtype distribution:

- `archetype_salvageable = 2`
- `context_salvageable = 1`
- `near_natural_contextual = 1`

Observed trial packaging distribution:

- `archetype_probe_then_context_card = 2`
- `setup_then_payoff_with_context_card = 1`
- `cold_open_payoff_then_context_caption = 1`

## What Is Resolved

Resolved surfaces:

1. replayability
2. export context retention
3. synthetic packaging collapse

This means the next step should not reopen:

- replay contract work
- export timing work
- synthetic subtype routing itself

## Candidate Next Surfaces

### Option A: Top-Level Hook Policy Change

Examples:

- lower the `natural` clarity threshold
- add a `near_natural` top-level state
- move `near_natural_contextual` cases into `natural`

Assessment:

- not justified yet

Reason:

- only one bounded case lands in `near_natural_contextual`
- changing top-level policy now would be threshold churn disguised as progress

### Option B: Context-Retention Rework

Examples:

- larger timing caps
- second context-expansion pass
- review-authored offsets

Assessment:

- not justified

Reason:

- context retention already solved the reject boundary
- the remaining bounded failures are no longer dominated by export timing

### Option C: Archetype-Specificity Remediation

Examples:

- improve bounded archetype assignment for retained synthetic cases
- use retained context to distinguish stronger moment families from `other`

Assessment:

- justified

Reason:

- `archetype_salvageable` is the highest-frequency remaining subtype
- `2 / 4` approved exports land there
- both affected cases currently route to:
  - `hook_archetype = other`
  - `packaging_strategy = archetype_probe_then_context_card`
- this is the clearest repeated unresolved bounded surface

### Option D: Context-Framing Refinement Inside Synthetic

Examples:

- better setup-first packaging for `context_salvageable`

Assessment:

- plausible but not first

Reason:

- only `1 / 4` current bounded cases lands here
- it is a valid follow-up, but not the strongest repeated pattern

## Decision

The smallest justified next intervention surface is:

- bounded archetype-specificity remediation

Not because archetype is the only remaining issue.

Because it is the strongest repeated unresolved issue after the bounded
packaging split.

## Why Archetype Is Next

### 1. It is the highest-frequency remaining subtype

Observed:

- `archetype_salvageable = 2`
- all other unresolved subtypes occur only once

Meaning:

- the next bounded change should target the most repeated remaining shape, not
  the most interesting single case

### 2. It explains why two cases still need probing rather than confident packaging

Observed:

- `call_of_duty-second-bounded-clip`
- `marvel_rivals-transfer-kjera`

Both retain:

- adequate context
- adequate payoff
- acceptable authenticity risk

But still route through:

- `hook_archetype = other`

Meaning:

- packaging logic is still compensating for weak archetype specificity instead
  of consuming a stronger moment classification

### 3. It stays below the policy line

An archetype-specificity slice can remain:

- additive
- replay-safe
- review-safe
- threshold-safe

That makes it a better next step than top-level hook-policy revision.

## Recommended Next Goal

```text
/goal Produce a bounded archetype-specificity remediation analysis for the approved call_of_duty and marvel_rivals export set. Identify why retained synthetic candidates still remain hook_archetype=other, separate causes into missing event-family specificity, weak retained-context cues, and archetype-mapping gaps, and recommend the smallest additive archetype-routing changes without changing hook_mode thresholds, review semantics, replayability, or selection scope.
```

## Stop Conditions

Do not do these next:

- no `natural` threshold changes
- no new top-level hook states
- no third-game widening
- no publish-readiness claims
- no replayability reopening

## Conclusion

The bounded synthetic-packaging slice reached a clean stopping point.

Current interpretation:

- replayability: complete at bounded scope
- export representation: improved enough
- synthetic packaging: improved enough
- next repeated unresolved surface: archetype specificity

That makes archetype-specificity remediation the next justified bounded goal.
