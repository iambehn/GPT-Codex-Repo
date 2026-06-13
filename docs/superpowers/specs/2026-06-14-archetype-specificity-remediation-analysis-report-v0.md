# Archetype-Specificity Remediation Analysis Report v0

Date: 2026-06-14
Status: completed
Scope: bounded archetype-specificity analysis only

## Objective

Analyze the approved bounded `call_of_duty` and `marvel_rivals` export set and
identify why retained synthetic candidates still remain
`hook_archetype = other`.

Required cause buckets:

- missing event-family specificity
- weak retained-context cues
- archetype-mapping gaps

Non-goals:

- `hook_mode` threshold changes
- review-semantic changes
- replayability changes
- selection-scope changes
- publish-readiness claims

## Assessment Surface

Primary evidence:

- [2026-06-14-bounded-synthetic-packaging-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-14-bounded-synthetic-packaging-validation-report-v0.md)
- [20260613T011541Z.bounded_synthetic_packaging_comparison_ledger.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T011541Z/20260613T011541Z.bounded_synthetic_packaging_comparison_ledger.json)
- [hook_candidate_export.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/hook_candidate_export.py)

Bounded target cases:

1. `call_of_duty-second-bounded-clip`
2. `marvel_rivals-transfer-kjera`

These are the two approved retained synthetic cases that currently land in:

- `synthetic_subtype = archetype_salvageable`
- `hook_archetype = other`

## Current Archetype Logic

The current archetype assignment in
[hook_candidate_export.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/hook_candidate_export.py)
is narrow and mostly text-driven:

- `clutch` if `"clutch"` appears in `event_type`
- `reversal` if `"reversal"` or `"swing"` appears in `event_type`
- `fail` if `"fail"`, `"death"`, or `"whiff"` appears in `event_type`
- `comedy` if `"comedy"` or `"funny"` appears in `event_type`
- `flex` if `"combo"` appears in `event_type`
- `domination` if `"medal"` appears and `final_score >= 0.85`
- `chaos` if `signal_count >= 3 and final_score >= 0.8`
- otherwise `other`

Important constraint:

- retained context is already available on the hook row via:
  - `context_pre_signal_types`
  - `context_post_signal_types`
  - `context_signal_count`
  - `metadata_summary`

But the current archetype function does not consume those fields.

## Key Finding

The remaining `hook_archetype = other` results are not primarily a hook-mode
problem.

They are an archetype-input problem.

More specifically:

1. the event-family labels reaching `_hook_archetype(...)` are too coarse
2. the retained context cues are useful but under-consumed
3. the current mapping table does not cover the validated bounded event shapes

## Case Analysis

### `call_of_duty-second-bounded-clip`

Observed hook row:

- `event_type = ability_seen`
- `hook_archetype = other`
- `context_post_signal_types = ['equipment_visibility']`
- `context_pre_signal_types = []`
- `entity_id = None`
- `metadata_summary.equipment_id = redeploy_extraction_token`
- `matched_signal_types = ['equipment_visibility']`

Interpretation:

- `ability_seen` is too generic to carry editorial meaning by itself
- the retained context says this is an equipment-visibility moment
- the equipment type is known
- but the current mapping ignores both the retained post-context and the
  equipment identity

Conclusion:

- this case lands in `other` mainly because the event family is underspecified
  before archetype mapping runs

### `marvel_rivals-transfer-kjera`

Observed hook row:

- `event_type = team_wipe_seen`
- `hook_archetype = other`
- `context_post_signal_types = ['round_state_visibility']`
- `context_pre_signal_types = []`
- `entity_id = None`
- `matched_signal_types = ['team_wipe_visibility']`

Interpretation:

- `team_wipe_seen` is already a much stronger event family than
  `ability_seen`
- the retained post-context adds round-state confirmation
- the hook survives into `archetype_salvageable` because context and payoff are
  already adequate
- but the current archetype mapping has no branch for `team_wipe` semantics

Conclusion:

- this case lands in `other` mainly because the mapping table does not
  recognize a validated strong event family that should route to a more
  specific archetype

## Cause Breakdown

### 1. Missing Event-Family Specificity

Classification:

- `strong cause`

Observed shape:

- `ability_seen` is a coarse runtime-style label
- the richer semantic clue actually lives in:
  - `matched_signal_types`
  - `equipment_id`
  - retained context signal types

Why it matters:

- a generic event label forces the archetype layer to guess from too little
  structure
- that is why the `call_of_duty` case can be packaging-salvageable but still
  archetype-ambiguous

Assessment:

- some candidate rows arrive at archetype assignment with event families that
  are too broad for reliable editorial classification

### 2. Weak Retained-Context Cues

Classification:

- `moderate cause`

Observed shape:

- both `archetype_salvageable` cases have:
  - no pre-context cue
  - one post-context cue
  - no resolved `entity_id`

Why it matters:

- the context-retention slice improved hook viability
- but the strongest archetype rescue cases would usually benefit from either:
  - stronger pre-context
  - stronger identity cues
  - multi-cue context combinations

Important nuance:

- retained context is not absent
- it is just not rich enough, by itself, to disambiguate the moment type with
  confidence

Assessment:

- context is sufficient for packaging salvage
- it is not always sufficient for archetype specificity without better mapping

### 3. Archetype-Mapping Gaps

Classification:

- `dominant immediate cause`

Observed gap:

- `_hook_archetype(...)` consumes only:
  - `event_type`
  - `final_score`
  - `signal_count`

It does not use:

- `context_pre_signal_types`
- `context_post_signal_types`
- `matched_signal_types`
- `equipment_id`
- bounded event-family clues like `team_wipe_seen`

Specific bounded misses:

- no mapping for `team_wipe_seen`
- no mapping for equipment-visibility utility moments that are strong enough to
  survive into approved synthetic packaging
- no context-aware rescue from `other` when event family and retained context
  together imply a stronger archetype

Assessment:

- this is the most direct reason both bounded target cases remain `other`

## Root-Cause Ordering

Primary root causes:

1. archetype-mapping gaps
2. missing event-family specificity
3. weak retained-context cues

Important nuance:

- the strongest immediate failure is the mapping layer
- but the mapping layer is partly starved by coarse event-family labels on some
  rows

## Smallest Additive Remediation Options

### Option A: Event-Family Normalization Before Archetype Mapping

Description:

- derive a narrow normalized event-family hint before `_hook_archetype(...)`
- keep it bounded to the current validated shapes only

Examples:

- `ability_seen + equipment_visibility + equipment_id present`
  -> normalized utility/equipment family
- `team_wipe_seen + team_wipe_visibility`
  -> normalized team-wipe family

Pros:

- addresses the `ability_seen` underspecification directly
- remains additive

Cons:

- adds one more representational layer

### Option B: Context-Aware Archetype Mapping Extension

Description:

- keep the current event type as-is
- extend `_hook_archetype(...)` to consume retained context cues and a few
  bounded metadata fields

Examples:

- `team_wipe_seen` with `round_state_visibility`
  -> `chaos` or `domination` candidate branch
- `ability_seen` with `equipment_visibility` and known `equipment_id`
  -> bounded utility-chaos or bounded fail/flex branch depending cue family

Pros:

- smallest code-surface change
- directly addresses the current mapping gap

Cons:

- can become messy if too many ad hoc branches are added

### Option C: Archetype Rescue Only for `archetype_salvageable`

Description:

- leave base archetype assignment unchanged
- add one later rescue pass only for rows already classified as
  `synthetic_subtype = archetype_salvageable`

Pros:

- very bounded
- avoids changing archetype behavior broadly

Cons:

- creates a second archetype path
- weaker architecture than fixing the main assignment point

## Recommendation

Recommended next implementation target:

- `Option B: Context-aware archetype mapping extension`

Reason:

- it is the smallest additive change
- it fixes the strongest immediate cause
- it uses context fields that already exist on the hook row
- it avoids inventing a second archetype pipeline

## Recommended Bounded Changes

1. Extend `_hook_archetype(...)` to accept:
   - `context_pre_signal_types`
   - `context_post_signal_types`
   - bounded metadata hints such as `matched_signal_types`
2. Add explicit bounded branches for the currently validated unresolved shapes:
   - `team_wipe_seen + team_wipe_visibility`
   - `ability_seen + equipment_visibility + equipment_id`
3. Keep the change additive:
   - no hook-mode threshold changes
   - no packaging threshold changes
   - no selection or review changes
4. Validate only on the current approved bounded set first

## Recommended Next Goal

```text
/goal Produce a bounded archetype-routing remediation design for the approved call_of_duty and marvel_rivals export set. Define the smallest additive changes needed to improve archetype specificity for retained synthetic candidates by using existing event-family and retained-context cues, without changing hook_mode thresholds, review semantics, replayability, or selection scope.
```

## Conclusion

The remaining `hook_archetype = other` cases are not evidence that the current
synthetic packaging slice failed.

They show that:

- packaging routing is now specific enough to expose the next bottleneck
- the next bottleneck is archetype specificity

The smallest justified next change is therefore:

- additive context-aware archetype mapping

Not:

- top-level hook policy revision
- replayability reopening
- timing expansion
