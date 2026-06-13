# Bounded Synthetic Packaging Validation Report v0

Date: 2026-06-14
Status: completed
Scope: bounded synthetic-packaging remediation validation only

## Objective

Validate whether bounded synthetic subtype routing makes approved exported
highlights more specifically packageable without changing:

- `hook_mode`
- review semantics
- selection scope
- replayability scope

This validation stays bounded to the current approved `call_of_duty` and
`marvel_rivals` export set.

## Validation Surface

- fixture count: `4`
- comparison ledger:
  - [20260613T011541Z.bounded_synthetic_packaging_comparison_ledger.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T011541Z/20260613T011541Z.bounded_synthetic_packaging_comparison_ledger.json)
- hook comparison report:
  - [20260613T011541Z.bounded_synthetic_packaging_hook_comparison.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T011541Z/20260613T011541Z.bounded_synthetic_packaging_hook_comparison.json)
- bounded validation root:
  - [20260613T011541Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T011541Z)

## Aggregate Result

- `hook_mode` unchanged:
  - baseline `synthetic = 4`
  - trial `synthetic = 4`
- baseline synthetic packaging collapse:
  - `setup_then_payoff_with_context_card = 4`
- trial packaging strategy distribution:
  - `archetype_probe_then_context_card = 2`
  - `cold_open_payoff_then_context_caption = 1`
  - `setup_then_payoff_with_context_card = 1`
- trial subtype distribution:
  - `archetype_salvageable = 2`
  - `context_salvageable = 1`
  - `near_natural_contextual = 1`
- guardrails:
  - `hook_mode_unchanged_count = 4`
  - `approval_surface_unchanged_count = 4`
- hook comparison recommendation:
  - `inconclusive`

## Interpretation

The `inconclusive` comparison recommendation is expected here.

The existing hook-comparison heuristic measures score and top-level mode
movement. This remediation was intentionally narrower:

- no `reject` threshold change
- no `natural` threshold change
- no `synthetic -> natural` upgrade

So the success condition for this slice is not a better top-level hook mode.
It is:

- more specific routing inside `hook_mode = synthetic`
- more specific packaging strategy selection
- no regression on approval or replay surfaces

That bounded success condition passed.

## Per-Case Summary

| Fixture | Baseline Strategy | Trial Strategy | Trial Subtype | Hook Mode Changed |
| --- | --- | --- | --- | --- |
| `call_of_duty-bootstrap-real-cod` | `setup_then_payoff_with_context_card` | `setup_then_payoff_with_context_card` | `context_salvageable` | `no` |
| `call_of_duty-second-bounded-clip` | `setup_then_payoff_with_context_card` | `archetype_probe_then_context_card` | `archetype_salvageable` | `no` |
| `marvel_rivals-transfer-absolute-cinema` | `setup_then_payoff_with_context_card` | `cold_open_payoff_then_context_caption` | `near_natural_contextual` | `no` |
| `marvel_rivals-transfer-kjera` | `setup_then_payoff_with_context_card` | `archetype_probe_then_context_card` | `archetype_salvageable` | `no` |

## Detailed Findings

### `call_of_duty-bootstrap-real-cod`

- baseline:
  - `hook_mode = synthetic`
  - `packaging_strategy = setup_then_payoff_with_context_card`
- trial:
  - `hook_mode = synthetic`
  - `synthetic_subtype = context_salvageable`
  - `packaging_strategy = setup_then_payoff_with_context_card`
- interpretation:
  - this case remained on the generic strategy because context is still doing
    most of the rescue work
  - the subtype is still useful because the reason is now explicit and
    queryable instead of implicit

### `call_of_duty-second-bounded-clip`

- baseline:
  - `hook_mode = synthetic`
  - `hook_archetype = other`
  - `packaging_strategy = setup_then_payoff_with_context_card`
- trial:
  - `hook_mode = synthetic`
  - `synthetic_subtype = archetype_salvageable`
  - `packaging_strategy = archetype_probe_then_context_card`
- interpretation:
  - this case now routes to a more specific synthetic strategy because context
    and payoff were adequate, but archetype remained too generic

### `marvel_rivals-transfer-absolute-cinema`

- baseline:
  - `hook_mode = synthetic`
  - `packaging_strategy = setup_then_payoff_with_context_card`
- trial:
  - `hook_mode = synthetic`
  - `synthetic_subtype = near_natural_contextual`
  - `packaging_strategy = cold_open_payoff_then_context_caption`
- interpretation:
  - this is the strongest bounded case for subtype routing
  - the candidate remains synthetic, but it now routes to a lighter,
    near-natural packaging posture without changing the top-level gate

### `marvel_rivals-transfer-kjera`

- baseline:
  - `hook_mode = synthetic`
  - `hook_archetype = other`
  - `packaging_strategy = setup_then_payoff_with_context_card`
- trial:
  - `hook_mode = synthetic`
  - `synthetic_subtype = archetype_salvageable`
  - `packaging_strategy = archetype_probe_then_context_card`
- interpretation:
  - this case confirms that the archetype-salvageable path is not isolated to
    one game

## Guardrail Result

The bounded guardrails all held:

- `hook_mode` did not change for any approved export
- approval surface stayed unchanged for every case
- no `synthetic` case was silently upgraded to `natural`
- replayability scope was untouched
- export timing behavior from the earlier context-retention slice was not
  reopened

## Conclusion

The bounded synthetic-packaging remediation passed its intended validation.

What changed:

- synthetic cases are now subtype-routed
- packaging strategy is no longer one coarse fallback for every synthetic case

What did not change:

- `reject` / `synthetic` / `natural` gates
- approved anchor identities
- replayability scope
- publish-readiness status

This means the packaging surface is now more specific and more inspectable
without becoming a disguised policy rewrite.

Outputs remain local-only and are not publish-cleared.
