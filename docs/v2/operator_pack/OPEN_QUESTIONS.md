# Open Questions

Status: active
Version: 0.6
Last updated: 2026-05-21

## P0 Questions

No active P0 questions remain for the bounded `call_of_duty` local test path.

Resolved on 2026-05-21 by user decision:

- the bounded public `call_of_duty` clip is the canonical operator sample for local pipeline testing
- bootstrap GPT review labels remain test-only
- human review is required before calibration or export artifacts from this path are treated as canonical

Resolved later on 2026-05-21 by user-adopted fused-review decisions:

- the local export artifact path for the canonical `60s-70s` sample is now backed by user-adopted fused-review decisions

Resolved later on 2026-05-21 by user-adopted runtime-review decisions:

- the runtime calibration artifact is now backed by user-adopted runtime-review decisions across its four reviewed runtime sidecars
- the bounded `call_of_duty` local test path now has no unresolved review-governance blocker

Resolved in current execution doctrine:

- media-backed stages are no longer blocked on this branch
- the current bounded real-media path is now the canonical local test path
- do not treat bootstrap GPT review or downloaded public test media as publish-cleared for external posting

## P1 Questions

No active P1 questions are blocking the current bounded local test path.

Resolved in current execution doctrine:

- first real-media sidecar command: `python run.py --analyze-roi-runtime <SOURCE> call_of_duty`
- first real-media review surface after that: `python run.py --prepare-runtime-review call_of_duty`
- first real-media calibration proof after that: `python run.py --calibrate-runtime-review outputs/runtime_analysis/call_of_duty --game call_of_duty`
- current local export path after that: `python run.py --fuse-clip-signals <SOURCE> call_of_duty --runtime-sidecar <RUNTIME_SIDECAR>` then `python run.py --export-highlight-selection --fused-sidecar <FUSED_SIDECAR>` then `python run.py --create-highlight-export-batch ...`
- publication begins later at `python run.py --record-post-ledger ...`

## Resolution Rules

- P0 questions block final happy-path completion when they affect real input choice, game choice, or schema ownership.
- If Codex cannot resolve a P0 question from repo state, it should stop and escalate instead of guessing.
- P1 questions may be deferred only if the current phase does not require the answer yet.
