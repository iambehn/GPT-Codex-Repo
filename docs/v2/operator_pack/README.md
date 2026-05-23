# Operator Pack

Status: active
Version: 0.2
Last updated: 2026-05-24

This directory is the active execution-control pack for the first runnable happy path.

Use these files to keep Codex work phase-aware, inspectable, and autonomous within local-only pipeline boundaries:

- `EXECUTION_TARGET.md`: the current happy-path target, stop condition, non-goals, and escalation triggers
- `CODEX_BACKLOG.md`: the ordered phase backlog Codex should work through
- `PIPELINE_CONTRACTS.md`: minimal stage contracts and validation levels
- `OPEN_QUESTIONS.md`: unresolved P0 and P1 decisions that block or shape execution
- `FAILURE_TAXONOMY.md`: seed failure modes and required escalation behavior

This pack is intentionally compact. It is not a duplicate architecture doc.

Rules:

- Update this pack when execution doctrine changes.
- Do not treat unresolved fields as permission to improvise architecture.
- Prefer explicit blockers over silent assumptions.
- If repo behavior changes, update the relevant operator file in the same slice.
- Do not stop for routine next-step permission once a local-only target is complete.
- Stop only for hard-stop categories:
  - external-risk actions
  - destructive or hard-to-reverse actions
  - real source-of-truth conflicts
  - material scope expansion beyond the pipeline mission
  - local truth that is too weak to continue responsibly
- Default reporting style is blockers and milestones, not frequent routine progress prompts.
