# Bounded Archetype-Routing Validation Report v0

Date: 2026-06-14
Status: completed
Scope: bounded archetype-routing remediation validation only

## Objective

Validate whether bounded archetype refinement improves specificity for retained
synthetic candidates by extending the existing `_hook_archetype(...)`
assignment point without changing:

- `hook_mode`
- approval/export status
- replayability scope
- selection scope

This validation stays bounded to the current approved `call_of_duty` and
`marvel_rivals` export set.

## Validation Surface

- fixture count: `4`
- comparison ledger:
  - [20260613T015114Z.bounded_archetype_routing_comparison_ledger.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T015114Z/20260613T015114Z.bounded_archetype_routing_comparison_ledger.json)
- hook comparison report:
  - [20260613T015114Z.bounded_archetype_routing_hook_comparison.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T015114Z/20260613T015114Z.bounded_archetype_routing_hook_comparison.json)
- bounded validation root:
  - [20260613T015114Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T015114Z)

## Aggregate Result

- old hook archetype counts:
  - `chaos = 2`
  - `other = 2`
- new hook archetype counts:
  - `chaos = 4`
- changed archetype count:
  - `2`
- unchanged hook mode count:
  - `4`
- unchanged export status count:
  - `4`
- old packaging strategy counts:
  - `archetype_probe_then_context_card = 2`
  - `cold_open_payoff_then_context_caption = 1`
  - `setup_then_payoff_with_context_card = 1`
- new packaging strategy counts:
  - `cold_open_payoff_then_context_caption = 1`
  - `setup_then_payoff_with_context_card = 3`
- hook comparison recommendation:
  - `inconclusive`

## Interpretation

The `inconclusive` recommendation is expected here.

The existing hook-comparison heuristic measures score and top-level mode
movement. This remediation was intentionally narrower:

- no `hook_mode` threshold changes
- no replayability changes
- no selection or review changes

So success for this slice is not a better top-level mode. It is:

- `other -> specific archetype` where bounded retained cues are sufficient
- no hook-mode regression
- no approval/export regression

That bounded success condition passed.

## Before/After Summary

| Fixture | Hook Mode | Old Archetype | New Archetype | Cue Match | Packaging Strategy | Approval/Export Unchanged |
| --- | --- | --- | --- | --- | --- | --- |
| `call_of_duty-bootstrap-real-cod` | `synthetic` | `chaos` | `chaos` | `ability_seen + equipment_visibility + equipment_id` | `setup_then_payoff_with_context_card -> setup_then_payoff_with_context_card` | `yes` |
| `call_of_duty-second-bounded-clip` | `synthetic` | `other` | `chaos` | `ability_seen + equipment_visibility + equipment_id` | `archetype_probe_then_context_card -> setup_then_payoff_with_context_card` | `yes` |
| `marvel_rivals-transfer-absolute-cinema` | `synthetic` | `chaos` | `chaos` | `team_wipe_seen + team_wipe_visibility` | `cold_open_payoff_then_context_caption -> cold_open_payoff_then_context_caption` | `yes` |
| `marvel_rivals-transfer-kjera` | `synthetic` | `other` | `chaos` | `team_wipe_seen + team_wipe_visibility + round_state_visibility` | `archetype_probe_then_context_card -> setup_then_payoff_with_context_card` | `yes` |

## Detailed Findings

### `call_of_duty-bootstrap-real-cod`

- hook mode:
  - unchanged at `synthetic`
- archetype:
  - remained `chaos`
- cue match:
  - `ability_seen + equipment_visibility + equipment_id`
- packaging strategy:
  - unchanged

Interpretation:

- the new bounded branch also explains a case that was already classifying as
  `chaos`
- this is additive explainability, not a behavior change

### `call_of_duty-second-bounded-clip`

- hook mode:
  - unchanged at `synthetic`
- archetype:
  - `other -> chaos`
- cue match:
  - `ability_seen + equipment_visibility + equipment_id`
- archetype rationale:
  - bounded utility/equipment visibility shape is specific enough for
    contextual chaos
- packaging strategy:
  - `archetype_probe_then_context_card -> setup_then_payoff_with_context_card`

Interpretation:

- this is the expected `call_of_duty` bounded target
- the archetype no longer depends on a packaging salvage fallback

### `marvel_rivals-transfer-absolute-cinema`

- hook mode:
  - unchanged at `synthetic`
- archetype:
  - remained `chaos`
- cue match:
  - `team_wipe_seen + team_wipe_visibility`
- packaging strategy:
  - unchanged

Interpretation:

- the new bounded branch also explains an already-specific case
- again, this is additive explainability rather than new behavior

### `marvel_rivals-transfer-kjera`

- hook mode:
  - unchanged at `synthetic`
- archetype:
  - `other -> chaos`
- cue match:
  - `team_wipe_seen + team_wipe_visibility + round_state_visibility`
- archetype rationale:
  - validated team-wipe event family is specific enough for chaos within the
    current bounded taxonomy
- packaging strategy:
  - `archetype_probe_then_context_card -> setup_then_payoff_with_context_card`

Interpretation:

- this is the expected `marvel_rivals` bounded target
- the retained round-state cue strengthens the event-family explanation without
  changing top-level hook policy

## Guardrail Result

The bounded guardrails all held:

- `hook_mode` did not change for any approved export
- approval/export status stayed unchanged for every case
- no top-level hook-policy thresholds were changed
- no replayability surfaces were touched
- no selection or review semantics were changed

## Important Nuance

The new bounded cue match and rationale are present not only on the two changed
cases, but also on the two cases that were already `chaos`.

This is acceptable in this slice because:

- it does not change `hook_mode`
- it does not change already-correct archetype outcomes
- it makes existing specific outcomes more explainable from retained evidence

## Conclusion

The bounded archetype-routing remediation passed its intended validation.

What changed:

- two retained synthetic cases moved from `hook_archetype = other` to
  `hook_archetype = chaos`
- the assignment is now explainable from retained row evidence

What did not change:

- `hook_mode`
- approval/export status
- replayability scope
- selection scope
- publish-readiness status

This means the archetype surface is now more specific and more inspectable
without becoming a disguised hook-policy rewrite.

Outputs remain local-only and are not publish-cleared.
