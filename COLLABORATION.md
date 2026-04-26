# Gaming Clip Farming Bot — Collaboration

> Parallel-agent operating rules for this repo.
>
> This file defines how concurrent work is handled. It is not a planning note, not a command reference, and not a speculative idea document.

## Purpose

This file is the operating agreement for parallel work from multiple agents on the same repo.

Its job is to reduce silent architecture drift, accidental reverts, overlapping refactors, and low-quality handoffs when Claude, Codex, or the user are editing at the same time.

## Source Of Truth

Use this precedence order:

- Current code and runtime behavior first
- `PLANNING.md` for current architecture and active workflow
- `Methodology.md` for signal, calibration, and evaluation principles
- `BRAINSTORMING.md` for speculative ideas only
- Older notes are reference material, not binding design docs

If a note conflicts with the code, trust the code first. Update the note later if needed.

## Default Parallel-Work Rule

Assume unexpected changes are intentional.

Do not revert them by default.

Only intervene when the changes:

- directly conflict with the current task
- break the current architecture
- invalidate an active contract, schema, or workflow

If none of those are true, adapt locally instead of undoing parallel work.

## Conflict Handling

When overlap is detected:

- inspect before editing
- understand what changed and why
- preserve both intents if possible
- prefer local adaptation over reversion
- avoid “cleaning up” adjacent work that is not required for the current task

If two implementations are structurally incompatible, stop and surface the exact conflict instead of merging blindly.

## Work-Slicing Rules

Preferred operating pattern:

- one agent per subsystem or feature slice when possible
- additive edits are preferred over broad refactors during concurrent work
- avoid file moves, large renames, and cleanup passes while multiple agents are active
- keep changes scoped to the current task instead of opportunistically reshaping nearby code

If a task requires cross-cutting refactoring, call that out explicitly before doing it.

## Architecture Guardrails

These are the current non-negotiables:

- game-pack-first structure stays
- sidecar-first `.meta.json` model stays
- quarantine is a recovery path, not a trash bin
- detectors emit evidence, not final keep or reject decisions
- replay/debug and review-feedback surfaces are part of the system, not optional extras
- new work should align with the current runtime order documented in `PLANNING.md`

Do not bypass these guardrails casually during parallel work.

## Testing Rules

Minimum expectations:

- run targeted tests for the touched area
- run the full suite when shared or core modules change
- say explicitly when tests were not run
- say explicitly when only partial validation was run

Do not claim stability from untested cross-cutting edits.

## Documentation Sync Rules

Update the right document for the right kind of change:

- update `PLANNING.md` when architecture or workflow changes
- update `Methodology.md` when signal or calibration thinking changes
- update `BRAINSTORMING.md` only for speculative or future ideas

Do not put active implementation truth into brainstorming notes.

## Handoff Format

Every substantial handoff should include:

- what changed
- files or subsystems touched
- tests run
- assumptions made
- known risks or follow-up items

Keep handoffs factual and compact.

## Escalation Cases

Stop and surface the issue when you hit:

- overlapping edits in the same module
- conflicting pipeline order
- incompatible metadata or schema changes
- detector output contract changes
- anything that could silently break replay, quarantine, evaluation, or dataset generation
- any temptation to revert unrelated work

If the conflict is real, name the file, describe the incompatibility, and avoid “helpful” silent resolution.
