# Call of Duty Replayability Status Report v1

Date: 2026-06-13
Status: completed
Scope: bounded `call_of_duty` proof path only

## Objective

Record the current replayability status of the bounded `call_of_duty` proof
path after:

- replay gap diagnosis
- minimum replay-contract definition
- bounded replay-contract implementation
- bounded cleanup execution
- post-cleanup operational validation

Non-goals:

- multi-game readiness claims
- publish-readiness claims
- production-readiness claims
- threshold or policy changes

## Authority Surfaces

Primary evidence:

- [2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md)
- [2026-06-13-call-of-duty-replay-artifact-cleanup-execution-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-replay-artifact-cleanup-execution-report-v0.md)

Supporting diagnosis:

- [2026-06-13-editorial-replay-gap-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-editorial-replay-gap-report-v0.md)
- [2026-06-13-export-regeneration-gap-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-export-regeneration-gap-report-v0.md)
- [2026-06-13-bounded-editorial-replay-minimum-artifact-contract-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-bounded-editorial-replay-minimum-artifact-contract-v0.md)

## Status Summary

| Capability | Status |
| --- | --- |
| Runtime Analysis | Proven |
| Fusion Analysis | Proven |
| Highlight Selection Export | Proven |
| Editorial Decision Persistence | Proven |
| Editorial Replay | Proven |
| Historical Export Regeneration | Proven |
| Local Export Artifact Production | Proven |
| Publish Workflow | Unproven |
| Multi-Game Generalization | Unproven |

## What Is Now Proven

### 1. Signal generation remains stable

Validated:

- runtime analysis rerun:
  - `ok = true`
  - `event_count = 3`
- fusion rerun:
  - `ok = true`
  - `fused_event_count = 3`

Interpretation:

- the bounded `call_of_duty` proof path still produces deterministic runtime
  and fused sidecars from the reference clip

### 2. Highlight selection export remains stable

Validated:

- selection export from canonical reviewed fused sidecar:
  - `selected_highlight_count = 3`
- selection export from fresh fused sidecar:
  - `selected_highlight_count = 3`

Interpretation:

- highlight-selection export remains mechanically reproducible on the bounded
  proof path

### 3. Editorial replay is operationally reproducible

Validated against repo-local artifacts:

- runtime replay:
  - `ok`
  - `review_status = approved`
- fused replay:
  - `ok`
  - `applied_count = 2`

Interpretation:

- editorial replay no longer depends on external GPT or human metadata as a
  required source of truth
- repo-local decision persistence is sufficient for bounded replay

### 4. Historical export regeneration is operationally reproducible

Validated:

- historical export regeneration:
  - `ok`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

Interpretation:

- the replay contract solved the original lifecycle-advance failure
- preserved export-ready snapshots now carry enough state to regenerate a
  historical export batch after the lifecycle has advanced beyond
  `selected_for_export`

### 5. Replay-root cleanup did not break replayability

Validated after moving the superseded artifact set:

- moved superseded artifacts:
  - `12`
- preserved active identities:
  - `6`
- preserved active decisions:
  - `6`
- preserved protected snapshots:
  - `1`
- post-move replay validation:
  - `passes`

Interpretation:

- replay-root hygiene is now improved without breaking the bounded replay path

## What Remains Local-Only

Current validation still stops at repo-local artifacts:

- repo-local sidecar mutation
- repo-local selection export
- repo-local highlight export batch creation
- repo-local replay validation roots

Not demonstrated:

- post ledger creation as part of a real publish workflow
- metrics snapshot capture from real published posts
- external platform upload or posting
- publish-readiness controls beyond local artifact generation

## What Remains Unproven

### Publish workflow

Not yet proven:

- end-to-end posting flow
- post-ledger mutation under real publishing conditions
- platform-specific upload success
- rights, policy, or publish-readiness compliance

### Bounded generalization

Not yet proven:

- a second `call_of_duty` clip through the same replayable proof path
- whether the replay contract generalizes beyond the current bounded reference
  clip

### Multi-game generalization

Not yet proven:

- adjacent-game replayability
- replay-contract sufficiency outside the bounded `call_of_duty` proof path

## Smallest Justified Widening Steps

The next widening steps should remain narrow and sequential.

Preferred order:

1. run a second bounded real-media `call_of_duty` clip through:
   - runtime analysis
   - fused analysis
   - review application
   - local export
   - repo-local replay validation
2. if that passes, write a bounded `call_of_duty` multi-clip status update
3. only then decide whether to validate one adjacent game

Not justified yet:

- multi-game rollout
- publish automation
- production-readiness claims

## Recommended Next Goal

The narrowest justified next validation target is:

```text
/goal Run a second bounded real-media call_of_duty clip through the current runtime-analysis, fused-review, local export, and replay-validation path, and determine whether the replayable proof path generalizes beyond the first validated clip. Capture runtime outputs, fusion outputs, review outputs, replay artifacts, export outputs, and observed failure modes. Do not treat outputs as publish-cleared.
```

## Conclusion

Classification:

- bounded `call_of_duty` replayability cycle = `completed`

Current repo-backed claim:

- signal generation is proven
- selection export is proven
- editorial replay is operationally reproducible
- historical export regeneration is operationally reproducible

Current boundary:

- the proof remains local-only and bounded to one validated `call_of_duty`
  reference path

The next decision should be about bounded generalization, not architecture.
