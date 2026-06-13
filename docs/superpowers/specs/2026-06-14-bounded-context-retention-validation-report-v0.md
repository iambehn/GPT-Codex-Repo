# Bounded Context-Retention Validation Report v0

Date: 2026-06-14
Status: completed
Scope: bounded context-retention remediation validation only

## Objective

Validate whether bounded export-context retention improves hook-layer viability for the current approved `call_of_duty` and `marvel_rivals` export set without changing replayability scope.

## Validation Surface

- fixture count: `4`
- comparison ledger: [20260613T003613Z.bounded_context_retention_comparison_ledger.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T003613Z/20260613T003613Z.bounded_context_retention_comparison_ledger.json)
- hook comparison report: [20260613T003613Z.bounded_context_retention_hook_comparison.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_calibration/20260613T003613Z/20260613T003613Z.bounded_context_retention_hook_comparison.json)

## Aggregate Result

- median `context_sufficiency_score` delta: `0.3`
- median `title_thumbnail_potential_score` delta: `0.1`
- median `authenticity_risk_score` delta: `-0.2754`
- hook comparison recommendation: `prefer_trial`

## Per-Case Summary

| Fixture | Baseline Hook | Trial Hook | Context Delta | Title Delta | Authenticity Delta | Export Window |
| --- | --- | --- | --- | --- | --- | --- |
| `call_of_duty-bootstrap-real-cod` | `reject` | `synthetic` | `0.3` | `0.1` | `-0.2754` | `1.5->4.0` |
| `call_of_duty-second-bounded-clip` | `reject` | `synthetic` | `0.3` | `0.1` | `-0.2754` | `1.5->4.0` |
| `marvel_rivals-transfer-absolute-cinema` | `reject` | `synthetic` | `0.4` | `0.18` | `-0.3142` | `20.0->24.5` |
| `marvel_rivals-transfer-kjera` | `reject` | `synthetic` | `0.3` | `0.1` | `-0.2754` | `0.0->2.5` |

## Detailed Findings

### `call_of_duty-bootstrap-real-cod`

- baseline hook: `reject` / `other`
- trial hook: `synthetic` / `chaos`
- baseline export window: `2.0 -> 2.0`
- trial export window: `1.5 -> 4.0` anchored on `2.0 -> 2.0`
- context expansion: `2.5` seconds via `signal_aware_bounded_v1`
- context reasons: `fallback_pre_pad, post_signal:equipment_visibility`
- signal count: `3`
- pre signal types: `none`
- post signal types: `equipment_visibility`

### `call_of_duty-second-bounded-clip`

- baseline hook: `reject` / `other`
- trial hook: `synthetic` / `other`
- baseline export window: `2.0 -> 2.0`
- trial export window: `1.5 -> 4.0` anchored on `2.0 -> 2.0`
- context expansion: `2.5` seconds via `signal_aware_bounded_v1`
- context reasons: `fallback_pre_pad, post_signal:equipment_visibility`
- signal count: `2`
- pre signal types: `none`
- post signal types: `equipment_visibility`

### `marvel_rivals-transfer-absolute-cinema`

- baseline hook: `reject` / `other`
- trial hook: `synthetic` / `chaos`
- baseline export window: `22.0 -> 22.0`
- trial export window: `20.0 -> 24.5` anchored on `22.0 -> 22.0`
- context expansion: `4.5` seconds via `signal_aware_bounded_v1`
- context reasons: `pre_signal:character_identity, post_signal:team_wipe_visibility`
- signal count: `15`
- pre signal types: `character_identity`
- post signal types: `team_wipe_visibility`

### `marvel_rivals-transfer-kjera`

- baseline hook: `reject` / `other`
- trial hook: `synthetic` / `other`
- baseline export window: `0.0 -> 0.0`
- trial export window: `0.0 -> 2.5` anchored on `0.0 -> 0.0`
- context expansion: `2.5` seconds via `signal_aware_bounded_v1`
- context reasons: `post_signal:round_state_visibility`
- signal count: `2`
- pre signal types: `none`
- post signal types: `round_state_visibility`

## Conclusion

The bounded context-retention slice improves hook-layer scores on the approved export set without altering the approved anchor identities. The strongest movement is in `context_sufficiency_score` and `authenticity_risk_score`, while `title_thumbnail_potential_score` improves more modestly. This supports the design claim that the weak surface was export representation rather than replayability.

Outputs remain local-only and are not publish-cleared.
