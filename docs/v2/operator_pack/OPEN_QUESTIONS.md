# Open Questions

Status: active
Version: 0.1
Last updated: 2026-05-20

## P0 Questions

| Question | Priority | Owner | Resolution path |
| --- | --- | --- | --- |
| What real sample input should be used for final happy-path proof? | `P0` | user or PM | provide a canonical local clip path for the provisional first game |
| If no real sample is available immediately, should Phase 1 proceed on documented `fixture_bootstrap` while media-backed stages remain blocked? | `P0` | operator or PM | explicitly accept bootstrap-only interim progress or hold for real media |

## P1 Questions

| Question | Priority | Owner | Resolution path |
| --- | --- | --- | --- |
| Which exact sidecar-generation command should be the first runtime proof once real input exists? | `P1` | Codex | choose from the inventoried runtime command surfaces for the provisional first game |
| What does local export-readiness mean in current repo code? | `P1` | Codex plus PM | define the local-only bundle and blocker semantics |
| Which review path is the minimal required one for the chosen happy path? | `P1` | Codex | map review bridge or app surface to the chosen input path |
| Which calibration or replay surface should be the first required proof? | `P1` | Codex | choose the narrowest stage that produces a meaningful inspectable result |

## Resolution Rules

- P0 questions block Phase 1 implementation when they affect game choice, input choice, or schema ownership.
- If Codex cannot resolve a P0 question from repo state, it should stop and escalate instead of guessing.
- P1 questions may be deferred only if the current phase does not require the answer yet.
