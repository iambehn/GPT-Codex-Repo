# Bounded Synthetic Context-Framing Remediation Plan

## Goal

Implement the smallest additive framing-policy change that reduces overuse of
`setup_then_payoff_with_context_card` by splitting only the packaging strategy
used inside `synthetic_subtype = context_salvageable`, without changing the
top-level `reject` / `synthetic` / `natural` decision surface.

## Scope

In scope:

- additive packaging-strategy refinement only after a candidate is already:
  - `hook_mode = synthetic`
  - `synthetic_subtype = context_salvageable`
- use only existing retained-context fields:
  - `context_pre_signal_types`
  - `context_post_signal_types`
  - `context_signal_count`
- bounded strategy split:
  - `setup_then_payoff_with_context_card`
  - `low_claim_post_payoff`
- bounded validation against the current approved `call_of_duty` and
  `marvel_rivals` export set
- focused regression coverage proving that framing refinement does not change:
  - `hook_mode`
  - `synthetic_subtype`
  - approval/export status

Out of scope:

- `reject` threshold changes
- `natural` threshold changes
- synthetic subtype assignment changes
- replayability contract changes
- review-semantic changes
- selection-scope changes
- export-timing changes
- archetype-routing changes
- publish-readiness work
- multi-game widening beyond the current bounded set

## Implementation Steps

1. Extend only the synthetic packaging strategy branch.

- update the packaging selection path in:
  - `pipeline/hook_candidate_export.py`
- keep the current top-level decision sequence unchanged:
  - reject
  - natural
  - synthetic
- keep synthetic subtype assignment unchanged
- apply the new split only after `context_salvageable` has already been chosen

2. Add one bounded context-framing split helper.

- implement one narrow helper or branch that decides between:
  - `setup_then_payoff_with_context_card`
  - `low_claim_post_payoff`
- consume only:
  - `context_pre_signal_types`
  - `context_post_signal_types`
- preserve current fallback behavior when both are absent

3. Preserve policy invariants explicitly in code.

- framing refinement must not change:
  - `hook_mode`
  - `synthetic_subtype`
  - approval/export status
  - replayability scope
  - selection scope
- keep all non-`context_salvageable` synthetic strategies unchanged

4. Keep the split rule deterministic and binary.

- if retained pre-context exists:
  - use `setup_then_payoff_with_context_card`
- else if retained post-context exists:
  - use `low_claim_post_payoff`
- else:
  - preserve `setup_then_payoff_with_context_card`

5. Propagate the refined packaging strategy through downstream surfaces.

- ensure hook rows persist the updated `packaging_strategy`
- ensure downstream export rows preserve the same additive strategy value
- do not add new schema fields unless required for validation

6. Add focused regression coverage.

- extend:
  - `tests/test_hook_candidate_export.py`
- cover:
  - post-only `context_salvageable` routes to `low_claim_post_payoff`
  - pre-context `context_salvageable` remains `setup_then_payoff_with_context_card`
  - non-`context_salvageable` cases remain unchanged
  - `hook_mode` remains unchanged
  - `synthetic_subtype` remains unchanged
- extend downstream assertions in:
  - `tests/test_highlight_export_batch.py`
  if needed to confirm export rows preserve the new strategy

7. Run bounded validation on the approved export set.

- reuse the current approved bounded export set for:
  - `call_of_duty`
  - `marvel_rivals`
- compare before vs after on:
  - `hook_mode`
  - `synthetic_subtype`
  - `packaging_strategy`
  - `context_pre_signal_types`
  - `context_post_signal_types`
  - approval/export status unchanged

8. Write bounded validation outputs.

- produce:
  - one bounded synthetic context-framing validation report
  - one structured before/after comparison ledger
- keep the validation question narrow:
  - did post-only `context_salvageable` cases stop overusing
    `setup_then_payoff_with_context_card` without changing policy?

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

- `call_of_duty-bootstrap-real-cod` moves:
  - `setup_then_payoff_with_context_card -> low_claim_post_payoff`
- `call_of_duty-second-bounded-clip` moves:
  - `setup_then_payoff_with_context_card -> low_claim_post_payoff`
- `marvel_rivals-transfer-kjera` moves:
  - `setup_then_payoff_with_context_card -> low_claim_post_payoff`
- `marvel_rivals-transfer-absolute-cinema` remains unchanged
- no case changes `hook_mode`
- no case changes `synthetic_subtype`
- no case changes approval/export status

## Risks And Guards

- Risk: framing refinement becomes a hidden policy rewrite.
  - Guard: split only inside `context_salvageable` after subtype assignment.

- Risk: the split turns into a second routing system.
  - Guard: use one binary strategy branch based only on retained pre/post cues.

- Risk: cases with no cues get forced into a new framing mode.
  - Guard: preserve current setup-first fallback when both cue sets are empty.

- Risk: a new strategy value gets misread as a quality upgrade.
  - Guard: assert unchanged `hook_mode`, `synthetic_subtype`, and approval/export
    status in tests and validation.

- Risk: scope drifts into timing, archetype, or threshold work.
  - Guard: keep validation and code changes confined to the packaging-strategy
    decision point only.
