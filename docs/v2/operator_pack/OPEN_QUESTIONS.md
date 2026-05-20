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
| Which real-media export surface should replace onboarding publish-readiness after the bootstrap path? | `P1` | Codex plus PM | promote from onboarding bootstrap summary to the final local export bundle contract |

Resolved in current execution doctrine:

- first real-media sidecar command: `python run.py --analyze-roi-runtime <SOURCE> call_of_duty`
- first real-media review surface after that: `python run.py --prepare-runtime-review call_of_duty`
- first real-media calibration proof after that: `python run.py --calibrate-runtime-review outputs/runtime_analysis/call_of_duty --game call_of_duty`

## Resolution Rules

- P0 questions block final happy-path completion when they affect real input choice, game choice, or schema ownership.
- If Codex cannot resolve a P0 question from repo state, it should stop and escalate instead of guessing.
- P1 questions may be deferred only if the current phase does not require the answer yet.
