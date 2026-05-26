# Open Questions

Status: active
Version: 0.8
Last updated: 2026-05-27

## P0 Questions

No active P0 questions remain for the bounded `call_of_duty` local test path.
No active autonomy-governance blocker remains for local-only pipeline work.

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
- Codex may choose the next local-only execution target without waiting for explicit user selection as long as the work stays inside the gameplay highlight pipeline mission
- small local-only changes do not require a user approval checkpoint when they are regressions, docs clarifications, focused refactors, workflow hardening, quality or inspection improvements, or narrow behavior fixes inside existing contracts
- default reporting style is blockers and milestones rather than routine next-step permission
- standing delegation now applies to this repo:
  - Codex may choose and execute the next local-only tasks inside this gameplay highlight pipeline without asking first
  - Codex may batch multiple milestones into one work block
  - Codex may do docs, tests, regressions, refactors, workflow hardening, pack-coverage work, and narrow behavior fixes inside existing workflow families without separate approval

## P1 Questions

No active P1 questions are blocking the current bounded local test path.

Active P1 questions for the next `call_of_duty` pack-coverage cycle:

- does the current canonical `call_of_duty` sample set actually contain native medal badge icons in the ROI expected by the promoted `medal_icon` pack
- should the next `call_of_duty` coverage slice target text or banner signals rather than more medal icons for the current sample family

Current execution truth:

- the raw wiki bundle can now be bridged safely after pre-bridge curation
- the first `multikill` curation profile keeps `0` rows from the real bundle
- the filled medal packet was strong enough to create a manual curated medal bundle at `assets/games/call_of_duty/drafts/wiki_curated/20260526T233955Z`
- that bundle bridged into onboarding draft `assets/games/call_of_duty/drafts/onboarding/20260526T234014Z`
- all `15` medal rows were accepted through the standard derived-row review flow and published into the canonical pack
- the published pack now contains:
  - `medal_count: 15`
  - `event_count: 15`
  - `template_count: 127`
  - `runtime_rule_count: 3`
  - `fusion_rule_count: 4`
- post-promotion cross-clip measurement on the same four real public samples still shows the same practical ceiling:
  - `2 / 4` samples produced no runtime events
  - `2 / 4` samples produced equipment-only events
  - `0 / 4` samples produced medal-driven outcomes
  - `0 / 4` samples produced hook candidates
- frame probes now suggest the current sample set may expose text or banner signals more clearly than native medal badge icons:
  - `_PL_5qWwKtY` visibly shows `UAV`, `DOUBLE KI`, and `7TH KO`
  - the same sample does not show a clear medal badge icon in the probed frames

Resolved in current execution doctrine:

- first real-media sidecar command: `python run.py --analyze-roi-runtime <SOURCE> call_of_duty`
- first real-media review surface after that: `python run.py --prepare-runtime-review call_of_duty`
- first real-media calibration proof after that: `python run.py --calibrate-runtime-review outputs/runtime_analysis/call_of_duty --game call_of_duty`
- current local export path after that: `python run.py --fuse-clip-signals <SOURCE> call_of_duty --runtime-sidecar <RUNTIME_SIDECAR>` then `python run.py --export-highlight-selection --fused-sidecar <FUSED_SIDECAR>` then `python run.py --create-highlight-export-batch ...`
- publication begins later at `python run.py --record-post-ledger ...`

## Resolution Rules

Autonomous local-only work should stop only when one of these categories is active:

- the action would affect external systems:
  - posting
  - scheduling
  - external accounts
  - purchases or paid APIs
- the action is destructive or hard to reverse:
  - deleting source media
  - wiping state
  - irreversible migrations
- a real source-of-truth conflict exists:
  - code versus canonical docs
  - two competing schemas
  - conflicting lifecycle or status ownership
- the requested change would materially expand project scope:
  - a new product surface outside the gameplay highlight pipeline
  - a new external platform workflow
  - a new long-horizon subsystem not justified by the current pipeline mission
- local truth is too weak to continue responsibly:
  - artifact meaning is ambiguous
  - test behavior contradicts docs and code
  - multiple plausible contracts exist and none clearly governs

Everything else should proceed autonomously.
