# Open Questions

Status: active
Version: 0.2
Last updated: 2026-05-20

## P0 Questions

| Question | Priority | Owner | Resolution path |
| --- | --- | --- | --- |
| What real sample input should be used for final happy-path proof? | `P0` | user or PM | provide a canonical local clip path for the provisional first game |

Resolved in current execution doctrine:

- proceed on documented `fixture_bootstrap` while media-backed stages remain blocked
- do not treat bootstrap-only success as final happy-path completion

## P1 Questions

| Question | Priority | Owner | Resolution path |
| --- | --- | --- | --- |
| Which exact sidecar-generation command should be the first runtime proof once real input exists? | `P1` | Codex | choose from the inventoried runtime command surfaces for the provisional first game |
| Which real-media export surface should replace onboarding publish-readiness after the bootstrap path? | `P1` | Codex plus PM | promote from onboarding bootstrap summary to the final local export bundle contract |
| Which exact review bridge or app surface should follow the first real sidecar command? | `P1` | Codex | map the first real-media artifact to the narrowest review surface that consumes it |
| Which exact replay or calibration surface should follow the first real sidecar command? | `P1` | Codex | choose the narrowest real-media replay or calibration proof once runtime artifacts exist |

## Resolution Rules

- P0 questions block final happy-path completion when they affect real input choice, game choice, or schema ownership.
- If Codex cannot resolve a P0 question from repo state, it should stop and escalate instead of guessing.
- P1 questions may be deferred only if the current phase does not require the answer yet.
