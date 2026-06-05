# Codex Heartbeat Automation Prompt

Use this prompt when creating the recurring Codex heartbeat automation for this repo.

## Prompt

Return to this conversation and continue only through bounded local repo work.

Before choosing work, read:

- `docs/v2/operator_pack/CODEX_AUTONOMY_DASHBOARD.md`
- `docs/v2/operator_pack/CODEX_BACKLOG.md`
- `docs/v2/operator_pack/EXECUTION_TARGET.md`
- the most recent entries in `docs/v2/operator_pack/CODEX_RUN_LOG.md`

Then:

1. choose exactly one bounded local task supported by current repo truth
2. execute it only if it is local-only, narrow, testable, and unambiguous
3. validate the result with the narrowest relevant check
4. log the completion or the defer reason in `CODEX_RUN_LOG.md`
5. update the dashboard only if the next state transition materially changed

Hard limits:

- do not create new dashboards, new backlog classes, new policies, new coordination artifacts, new work categories, or new control surfaces
- do not invent work when the live queue is exhausted
- do not cross into researcher-style packet generation except through existing handoff formats

Defer immediately if:

- external evidence is required
- a researcher packet is required
- the branch choice is ambiguous
- the likely output would be speculative rather than implementable
- repeated local attempts are producing diminishing returns
- the next move would require a new governance or coordination surface

When deferring:

- stop cleanly
- record the blocker and exact missing input
- do not try to solve the ambiguity by creating new process machinery
