# Bounded Synthetic Context-Framing Validation Report v0

Date: 2026-06-14
Status: completed
Scope: bounded synthetic context-framing remediation validation only

## Objective

Validate whether the bounded framing split reduces overuse of
`setup_then_payoff_with_context_card` by changing only the packaging strategy
used inside `synthetic_subtype = context_salvageable`, without changing:

- `hook_mode`
- `synthetic_subtype`
- approval/export status
- replayability scope
- selection scope

This validation stays bounded to the current approved `call_of_duty` and
`marvel_rivals` export set.

## Validation Surface

- fixture count: `4`
- comparison ledger:
  - [20260613T024155Z.bounded_synthetic_context_framing_comparison_ledger.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T024155Z/20260613T024155Z.bounded_synthetic_context_framing_comparison_ledger.json)
- bounded validation root:
  - [20260613T024155Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T024155Z)

## Aggregate Result

- old packaging strategy counts:
  - `setup_then_payoff_with_context_card = 3`
  - `cold_open_payoff_then_context_caption = 1`
- new packaging strategy counts:
  - `low_claim_post_payoff = 3`
  - `cold_open_payoff_then_context_caption = 1`
- unchanged `hook_mode` count:
  - `4`
- unchanged `synthetic_subtype` count:
  - `4`
- unchanged export status count:
  - `4`

## Interpretation

The bounded framing split passed its intended validation.

Success for this slice was not:

- higher-level hook-mode movement
- subtype movement
- replayability changes

It was:

- post-only `context_salvageable` cases stop overusing the setup-first
  packaging strategy
- richer or out-of-scope synthetic cases remain unchanged
- top-level policy stays fixed

That bounded success condition passed.

## Before/After Summary

| Fixture | Hook Mode | Synthetic Subtype | Old Strategy | New Strategy | Pre Cues | Post Cues | Approval/Export Unchanged |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `call_of_duty-bootstrap-real-cod` | `synthetic` | `context_salvageable` | `setup_then_payoff_with_context_card` | `low_claim_post_payoff` | none | `equipment_visibility` | `yes` |
| `call_of_duty-second-bounded-clip` | `synthetic` | `context_salvageable` | `setup_then_payoff_with_context_card` | `low_claim_post_payoff` | none | `equipment_visibility` | `yes` |
| `marvel_rivals-transfer-absolute-cinema` | `synthetic` | `near_natural_contextual` | `cold_open_payoff_then_context_caption` | `cold_open_payoff_then_context_caption` | `character_identity` | `team_wipe_visibility` | `yes` |
| `marvel_rivals-transfer-kjera` | `synthetic` | `context_salvageable` | `setup_then_payoff_with_context_card` | `low_claim_post_payoff` | none | `round_state_visibility` | `yes` |

## Detailed Findings

### `call_of_duty-bootstrap-real-cod`

- hook mode:
  - unchanged at `synthetic`
- synthetic subtype:
  - unchanged at `context_salvageable`
- framing strategy:
  - `setup_then_payoff_with_context_card -> low_claim_post_payoff`
- retained cue shape:
  - no pre cue
  - post cue:
    - `equipment_visibility`

Interpretation:

- this is the intended post-only `context_salvageable` target
- the candidate remains viable
- but it no longer inherits a full setup-card posture unsupported by retained
  pre-context

### `call_of_duty-second-bounded-clip`

- hook mode:
  - unchanged at `synthetic`
- synthetic subtype:
  - unchanged at `context_salvageable`
- framing strategy:
  - `setup_then_payoff_with_context_card -> low_claim_post_payoff`
- retained cue shape:
  - no pre cue
  - post cue:
    - `equipment_visibility`

Interpretation:

- this confirms the split is not isolated to one `call_of_duty` case
- the post-only utility/equipment shape now routes to the lower-claim framing
  path consistently

### `marvel_rivals-transfer-absolute-cinema`

- hook mode:
  - unchanged at `synthetic`
- synthetic subtype:
  - unchanged at `near_natural_contextual`
- framing strategy:
  - unchanged at `cold_open_payoff_then_context_caption`
- retained cue shape:
  - pre cue:
    - `character_identity`
  - post cue:
    - `team_wipe_visibility`

Interpretation:

- this case remained outside the remediation path exactly as intended
- the split did not disturb the stronger payoff-led synthetic case

### `marvel_rivals-transfer-kjera`

- hook mode:
  - unchanged at `synthetic`
- synthetic subtype:
  - unchanged at `context_salvageable`
- framing strategy:
  - `setup_then_payoff_with_context_card -> low_claim_post_payoff`
- retained cue shape:
  - no pre cue
  - post cue:
    - `round_state_visibility`

Interpretation:

- this is the intended `marvel_rivals` post-only target
- the framing split generalizes across the second game without changing the
  synthetic classification surface

## Guardrail Result

The bounded guardrails all held:

- `hook_mode` did not change for any approved export
- `synthetic_subtype` did not change for any approved export
- approval/export status stayed unchanged for every case
- no replayability surfaces were reopened
- no selection or review semantics were changed
- no archetype-routing or timing behavior was reopened

## Verification

Implementation verification:

```bash
python3 -m py_compile pipeline/hook_candidate_export.py tests/test_hook_candidate_export.py tests/test_highlight_export_batch.py
source .venv/bin/activate && python -m unittest tests.test_hook_candidate_export tests.test_highlight_export_batch
source .venv/bin/activate && python run.py --run-repo-quality-health
```

Observed result:

- targeted tests:
  - `22` passed
- repo quality gate:
  - `Ok: True`
  - `Maintenance ok: True`
  - `Decision regression ok: True`

## Conclusion

The bounded synthetic context-framing remediation passed its intended
validation.

What changed:

- the three post-only `context_salvageable` cases now route to:
  - `low_claim_post_payoff`

What did not change:

- `hook_mode`
- `synthetic_subtype`
- approval/export status
- replayability scope
- selection scope

This means the framing surface is now more specific and less overbroad without
becoming a disguised hook-policy rewrite.

Outputs remain local-only and are not publish-cleared.
