# Codex Heartbeat Automation Policy

Status: active
Last updated: 2026-06-05

## Purpose

Define the bounded recurring automation policy for Codex inside this repo.

This policy is for:

- recurring Codex wakeups
- local-only execution continuity
- strict stop/defer rules
- protection against automation drift

This policy is not:

- a second dashboard
- a second backlog
- a second governance surface
- permission to invent new work categories

## Canonical Inputs

The heartbeat may read only the existing execution-control surfaces before choosing work:

- [CODEX_AUTONOMY_DASHBOARD.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_AUTONOMY_DASHBOARD.md)
- [CODEX_BACKLOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_BACKLOG.md)
- [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md)
- recent [CODEX_RUN_LOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_RUN_LOG.md)

## Allowed Loop

The heartbeat may do only this:

1. read current operator surfaces
2. choose one bounded local task
3. execute it
4. validate it
5. log completion or defer reason

The heartbeat should prefer:

- narrow local fixes
- contract hardening
- tests
- deterministic exports
- existing workflow tooling
- existing handoff formats when packaging a blocker is necessary

## Hard Limits

The heartbeat may not create:

- new dashboards
- new backlog classes
- new policies
- new coordination artifacts
- new work categories
- new control surfaces

The heartbeat may update existing surfaces only when current repo truth materially requires it.

The heartbeat may not:

- invent work when the live queue is exhausted
- cross into researcher-style packet generation except through existing handoff formats
- continue speculative branches after diminishing returns are visible

## Stop / Defer Rules

Defer immediately when any of these are true:

- external evidence is required
- researcher packet or reviewer judgment is required
- branch choice is ambiguous
- likely output would be speculative rather than implementable
- repeated local attempts are producing diminishing returns
- the next move would require a new governance or coordination surface

## Output Contract

Each heartbeat run should produce one of:

- one bounded completed work slice with validation
- one explicit defer record

Preferred durable sink:

- [CODEX_RUN_LOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_RUN_LOG.md)

The dashboard should be updated only when the next execution branch materially changes.
