# Operator Pack

Status: active
Version: 0.2
Last updated: 2026-05-25

This directory is the active execution-control pack for the first runnable happy path.

Use these files to keep Codex work phase-aware, inspectable, and autonomous within local-only pipeline boundaries:

- `EXECUTION_TARGET.md`: the current happy-path target, stop condition, non-goals, and escalation triggers
- `CODEX_AUTONOMY_DASHBOARD.md`: the thin live snapshot of current execution state, next transition, and exact missing input
- `CODEX_BACKLOG.md`: the ordered phase backlog Codex should work through
- `CODEX_RUN_LOG.md`: the append-only routine execution log for progress that does not need a chat interruption
- `PIPELINE_CONTRACTS.md`: minimal stage contracts and validation levels
- `CALL_OF_DUTY_EDITORIAL_REPLAY_PROCEDURE.md`: bounded operator procedure for repo-local editorial replay and historical export regeneration on the canonical `call_of_duty` proof path
- `CALL_OF_DUTY_REPLAY_ARTIFACT_SUPERSESSION_PROCEDURE.md`: bounded operator procedure for classifying `call_of_duty` replay artifacts as active, superseded, or protected before any cleanup slice
- `CALL_OF_DUTY_REPLAY_ARTIFACT_QUARANTINE_PROCEDURE.md`: bounded operator procedure for planning reversible quarantine of superseded `call_of_duty` replay artifacts before any cleanup implementation slice
- `OPEN_QUESTIONS.md`: unresolved P0 and P1 decisions that block or shape execution
- `FAILURE_TAXONOMY.md`: seed failure modes and required escalation behavior
- `CODEX_HEARTBEAT_AUTOMATION_POLICY.md`: the bounded recurring Codex automation rules
- `CODEX_HEARTBEAT_AUTOMATION_PROMPT.md`: the prompt text for the recurring heartbeat automation

This pack is intentionally compact. It is not a duplicate architecture doc.

Rules:

- Update this pack when execution doctrine changes.
- Do not treat unresolved fields as permission to improvise architecture.
- Prefer explicit blockers over silent assumptions.
- If repo behavior changes, update the relevant operator file in the same slice.
- Do not stop for routine next-step permission once a local-only target is complete.
- Prefer logging routine progress in `CODEX_RUN_LOG.md` instead of surfacing it in chat.
- Treat `CODEX_AUTONOMY_DASHBOARD.md` as a thin live snapshot only; do not let it become a second backlog, archive, or governance file.
- Apply the standing user delegation for this repo:
  - Codex may choose and execute the next local-only tasks inside this gameplay highlight pipeline without asking first.
  - Codex may batch multiple milestones into one work block.
  - Codex may do docs, tests, regressions, refactors, workflow hardening, pack-coverage work, and narrow behavior fixes inside existing workflow families without separate approval.
- Stop only for hard-stop categories:
  - external-risk actions
  - destructive or hard-to-reverse actions
  - real source-of-truth conflicts
  - material scope expansion beyond the pipeline mission
  - local truth that is too weak to continue responsibly
- Default reporting style is blockers and milestones, not frequent routine progress prompts.
- Default progress sink for non-blocking routine work is `CODEX_RUN_LOG.md`.
- Default live execution snapshot is `CODEX_AUTONOMY_DASHBOARD.md`.
- Default dashboard ownership is researcher-primary; Codex updates it only when the next execution branch materially changes.
