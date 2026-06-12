# Cross-Game Bounded Replayability Status Report v0

Date: 2026-06-14
Status: completed
Scope: bounded cross-game replayability status only

## Objective

Record the current replayability status after:

- bounded `call_of_duty` multi-clip validation
- one bounded adjacent-game `marvel_rivals` transfer validation

Non-goals:

- publish-readiness claims
- production-readiness claims
- multi-game rollout claims
- runtime or policy changes

## Authority Surfaces

- [2026-06-14-call-of-duty-multi-clip-replayability-status-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-14-call-of-duty-multi-clip-replayability-status-report-v0.md)
- [2026-06-14-marvel-rivals-bounded-replay-transfer-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-14-marvel-rivals-bounded-replay-transfer-report-v0.md)
- [2026-06-14-bounded-replayability-widening-decision-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-14-bounded-replayability-widening-decision-report-v0.md)
- [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md)

## Validated Bounded Scope

### `call_of_duty`

Validated on `2` real-media clips:

1. [SVbTc2AZzYw.60s-70s.mp4](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4)
2. [SVbTc2AZzYw.10s.mp4](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.10s.mp4)

### `marvel_rivals`

Validated on `1` bounded real-media clip:

1. `/Users/tj/GPT-Codex-Repo/accepted/marvel_rivals/ABSOLUTE CINEMA_3522796292.mp4`

## Status Summary

| Capability | `call_of_duty` | `marvel_rivals` | Cross-Game Status |
| --- | --- | --- | --- |
| Runtime Analysis | Proven on 2 clips | Proven on 1 clip | Transfers |
| Fusion Analysis | Proven on 2 clips | Proven on 1 clip | Transfers |
| Local Review Apply | Proven on 2 clips | Proven on 1 clip | Transfers |
| Highlight Selection Export | Proven on 2 clips | Proven on 1 clip | Transfers |
| Editorial Replay | Proven on 2 clips | Proven on 1 clip | Transfers |
| Historical Export Regeneration | Proven on 2 clips | Proven on 1 clip | Transfers |
| Local Export Artifact Production | Proven on 2 clips | Proven on 1 clip | Transfers |
| Publish Workflow | Unproven | Unproven | Unproven |
| Multi-Clip Stability | Proven | Unproven | Partial |
| Multi-Game Rollout | N/A | N/A | Unproven |

## What Is Now Defensible

### 1. The replay contract is not `call_of_duty`-specific

Bounded repo-backed evidence now shows the same proof-path surfaces hold on:

- `2` real `call_of_duty` clips
- `1` bounded `marvel_rivals` clip

Interpretation:

- the replay contract is no longer supported only by same-game evidence
- transfer now appears contract-level rather than purely sample-specific

### 2. The adjacent-game transfer held on the weaker published pack

`marvel_rivals` remains explicitly weaker than `call_of_duty` in
[EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md),
but the bounded transfer still passed:

- runtime analysis: `ok`
- fusion analysis: `ok`
- runtime review apply: `approved_count = 1`
- fused review apply: `approved_count = 1`
- replayed export batch:
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`
- isolated replay validation:
  - runtime replay: `ok`
  - fused replay: `ok`
  - export replay: `ok`

Interpretation:

- the replay contract survives one transfer into a weaker adjacent game pack

## What Remains Unproven

### `marvel_rivals` multi-clip stability

Still not demonstrated:

- a second bounded `marvel_rivals` clip
- whether transfer survives beyond the first adjacent-game sample

### Further widening

Still not demonstrated:

- `valorant`
- broader multi-game sufficiency
- cross-game operator stability beyond one adjacent-game sample

### Publish workflow

Still not demonstrated:

- external posting
- post-ledger mutation under live posting conditions
- posted-metrics collection

## Recommended Interpretation

Classification:

- bounded replayability = `cross-game transfer demonstrated`

Boundary:

- `call_of_duty` is multi-clip proven
- `marvel_rivals` is single-clip transfer-proven
- the overall proof remains local-only
- the proof is still not publish-cleared

## Next Decision Surface

The dominant uncertainty is no longer:

- whether the replay contract works at all

The dominant uncertainty is now:

- whether the bounded transfer to `marvel_rivals` is repeatable across more
  than one adjacent-game clip

## Conclusion

Current strongest repo-backed claim:

- bounded replayability is multi-clip proven on `call_of_duty`
- bounded replayability transfers to at least one `marvel_rivals` clip

What remains missing is not another architecture change.

What remains missing is one more bounded adjacent-game sample to determine
whether `marvel_rivals` is also becoming multi-clip stable under the same
replay contract.
