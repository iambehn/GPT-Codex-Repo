# Bounded Synthetic-Packaging Remediation Plan

## Goal

Implement the smallest packaging-policy change that improves usefulness inside
`hook_mode = synthetic` by routing synthetic cases into a small set of bounded
subtypes without changing the top-level `reject` / `synthetic` / `natural`
decision surface.

## Scope

In scope:
- additive synthetic subtype routing inside the current synthetic branch only
- additive fields:
  - `synthetic_subtype`
  - optional `synthetic_packaging_rationale`
- subtype-specific packaging-strategy routing
- bounded validation against the current approved `call_of_duty` and
  `marvel_rivals` export set
- focused regression coverage proving that subtype routing does not change
  top-level hook mode or candidate quality status

Out of scope:
- `reject` threshold changes
- `natural` threshold changes
- replayability contract changes
- review-semantic changes
- selection-scope changes
- publish-readiness work
- multi-game widening beyond the current bounded set

## Implementation Steps

1. Extend the hook candidate packaging branch additively.
- update the current synthetic fallback path in:
  - `pipeline/hook_candidate_export.py`
- keep the existing top-level sequence unchanged:
  - reject
  - natural
  - synthetic
- add subtype routing only after a candidate is already classified as
  `synthetic`

2. Add bounded subtype classification helpers.
- implement one narrow helper that classifies synthetic rows into:
  - `near_natural_contextual`
  - `archetype_salvageable`
  - `context_salvageable`
  - `weak_synthetic`
- keep routing ordered and deterministic:
  1. `near_natural_contextual`
  2. `archetype_salvageable`
  3. `context_salvageable`
  4. `weak_synthetic`
- use existing signals only:
  - `clarity_score`
  - `context_sufficiency_score`
  - `payoff_readability_score`
  - `title_thumbnail_potential_score`
  - `authenticity_risk_score`
  - `hook_archetype`
  - `context_pre_signal_types`
  - `context_post_signal_types`
  - `context_signal_count`

3. Keep policy-preserving invariants explicit in code.
- subtype assignment must not change:
  - `hook_mode`
  - approval status
  - natural status
  - publish readiness
- implement explicit tests or assertions that subtype routing only happens after
  synthetic classification has already been chosen

4. Add subtype-specific packaging strategy mapping.
- map synthetic subtype to a more specific strategy:
  - `near_natural_contextual`
    - `cold_open_payoff_then_context_caption`
    - or `tight_context_then_payoff`
  - `archetype_salvageable`
    - `archetype_probe_then_context_card`
  - `context_salvageable`
    - `setup_then_payoff_with_context_card`
  - `weak_synthetic`
    - `low_claim_context_first`
- keep this mapping bounded to the current packaging surface
- do not add a larger packaging ontology in this slice

5. Persist the additive subtype contract.
- write `synthetic_subtype` onto synthetic hook candidate rows
- optionally write `synthetic_packaging_rationale` if the existing hook row can
  support a compact explanation without schema churn
- preserve existing fields and existing consumers

6. Add focused regression coverage.
- extend:
  - `tests/test_hook_candidate_export.py`
- cover:
  - synthetic subtype assignment for representative bounded cases
  - top-level `hook_mode` remains unchanged
  - natural cases do not receive synthetic subtype routing
  - reject cases do not receive synthetic subtype routing
  - packaging strategy changes with subtype as expected
- if useful, extend downstream export assertions in:
  - `tests/test_highlight_export_batch.py`
  to confirm export rows carry the more specific synthetic packaging strategy

7. Add bounded validation artifacts.
- reuse the current bounded approved export set for:
  - `call_of_duty`
  - `marvel_rivals`
- produce before/after comparison on:
  - `synthetic_subtype`
  - `packaging_strategy`
  - `hook_mode`
  - `hook_archetype`
  - `rejection_reason`
- record:
  - `subtype_distribution`

8. Write bounded validation outputs.
- produce:
  - one bounded synthetic-packaging validation report
  - one structured subtype-routing ledger
- keep the validation question narrow:
  - did synthetic become more specific without changing top-level policy?

## Verification

Implementation verification:

```bash
python3 -m py_compile \
  pipeline/hook_candidate_export.py \
  tests/test_hook_candidate_export.py \
  tests/test_highlight_export_batch.py
source .venv/bin/activate && python -m unittest \
  tests.test_hook_candidate_export \
  tests.test_highlight_export_batch
```

Repo closeout:

```bash
source .venv/bin/activate && python run.py --run-repo-quality-health
```

Operational validation should confirm:
- synthetic cases are more specifically routed
- `hook_mode` does not change for the validated set
- no case upgrades from `synthetic` to `natural`
- replayability and selection remain unaffected

## Risks And Guards

- Risk: synthetic subtype routing becomes hidden threshold churn.
  - Guard: keep `reject` and `natural` gates unchanged and route only after
    `synthetic` has already been chosen.

- Risk: subtype logic becomes a second scoring system.
  - Guard: use existing hook/export fields only and avoid introducing a new
    aggregate quality score.

- Risk: subtype taxonomy grows beyond bounded usefulness.
  - Guard: keep the set fixed at four subtypes in this slice.

- Risk: packaging strategy changes get misread as quality upgrades.
  - Guard: assert that subtype routing does not alter approval, natural status,
    or publish readiness.

- Risk: subtype distribution collapses into one bucket and adds no value.
  - Guard: record `subtype_distribution` as an explicit observational output
    and treat a highly collapsed distribution as evidence for future iteration,
    not for expanding scope in this slice.
