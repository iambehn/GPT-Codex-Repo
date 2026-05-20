# Open Questions

Status: active
Version: 0.1
Last updated: 2026-05-20

## P0 Questions

| Question | Priority | Owner | Resolution path |
| --- | --- | --- | --- |
| What is the first supported game? | `P0` | user or Codex recommendation | inventory current game support, then choose one |
| What sample input should be used? | `P0` | user or PM | provide local clip path or approve fixture bootstrap |
| What runnable command surfaces already exist for each stage? | `P0` | Codex | inventory config, scanning, fusion, review, calibration, export, and health surfaces |
| What artifacts and sidecars already exist for the chosen path? | `P0` | Codex | artifact inventory with stage ownership |
| What schemas and status enums own candidate, review, and export state? | `P0` | Codex | schema and status ownership audit |

## P1 Questions

| Question | Priority | Owner | Resolution path |
| --- | --- | --- | --- |
| What does local export-readiness mean in current repo code? | `P1` | Codex plus PM | define the local-only bundle and blocker semantics |
| Which review path is the minimal required one for the chosen happy path? | `P1` | Codex | map review bridge or app surface to the chosen input path |
| Which calibration or replay surface should be the first required proof? | `P1` | Codex | choose the narrowest stage that produces a meaningful inspectable result |

## Resolution Rules

- P0 questions block Phase 1 implementation when they affect game choice, input choice, or schema ownership.
- If Codex cannot resolve a P0 question from repo state, it should stop and escalate instead of guessing.
- P1 questions may be deferred only if the current phase does not require the answer yet.
