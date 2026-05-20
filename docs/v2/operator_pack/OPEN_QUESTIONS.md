# Open Questions

Status: active
Version: 0.3
Last updated: 2026-05-21

## P0 Questions

| Question | Priority | Owner | Resolution path |
| --- | --- | --- | --- |
| Should the bounded public `call_of_duty` test clip be promoted to a canonical operator sample, or replaced with a user-approved local gameplay clip? | `P0` | user or PM | either ratify the current bootstrap sample explicitly or replace it with a canonical gameplay clip |
| Are bootstrap GPT review decisions acceptable only for local execution proof, or should human review be required before treating calibration and export artifacts as canonical? | `P0` | user or PM | decide whether the current bootstrap labels remain test-only or need to be replaced by human review |

Resolved in current execution doctrine:

- media-backed stages are no longer blocked on this branch
- the current bounded real-media path is an execution proof only
- do not treat bootstrap GPT review or downloaded public test media as publish-cleared or canonical without explicit approval

## P1 Questions

No active P1 questions are blocking the current bounded bootstrap path.

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
