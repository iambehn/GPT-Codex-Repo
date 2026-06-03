# Codex Autonomy Dashboard

Status: active-draft
Version: 0.1
Last updated: 2026-06-03

Purpose:

- keep one thin live snapshot of Codex execution state
- reduce pause time caused by queue ambiguity or context reconstruction
- answer "what should happen next?" with minimal interpretation burden

This file is a coordination snapshot, not a second source of truth.

Subordinate surfaces:

- chronology lives in [CODEX_RUN_LOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_RUN_LOG.md)
- queued work lives in [CODEX_BACKLOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_BACKLOG.md)
- phase intent lives in [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md)
- evidence and measured outcomes live in handoff docs under `docs/handoffs/`

Update rule:

- update this file only when the next state transition changes
- do not duplicate large narrative history here

## Active Execution Target

Active objective:

- choose the correct post-`reward_banner` branch for `call_of_duty`

Active workstream:

- `WS-001`
- `Call of Duty Native Surface Validation`

Current state:

- `reward_banner` is validated as a narrow native-HUD pilot

Desired next state:

- a more repeatable native `call_of_duty` surface is selected for the next runtime slice

Current blocker:

- no repeatable native surface has yet been confirmed beyond the one-clip `UAV` pilot

## Current Blocker

Blocking mechanic:

- repeatability failure

Blocking detail:

- the published `UAV` reward-banner family is clean but does not reproduce across same-family `MWIII Vista` probes
- the lower-center text in `_PL_5qWwKtY` behaves like editorial or overlay presentation and should not be promoted as pack truth

## Active Packet

Current packet:

- `none active`

Current packet status:

- no external packet is required for the immediate next local decision

Current decision target:

- decide whether to pivot to another native surface family or reopen sourcing only if stronger repeated evidence appears

Acceptance target:

- one next implementation direction is explicit without requiring more interpretation

Failure condition:

- Codex resumes low-yield `reward_banner` tuning or blind clip hunting

## Forecast Queue

Next packet:

- `ALTERNATIVE_NATIVE_SURFACE_PACKET`

Validation packet:

- `SURFACE_REPEATABILITY_VALIDATION_PACKET`

Fallback packet:

- `EXACT_PROVENANCE_CLIP_SOURCING_PACKET`

## Current Repo Truth

Last completed work:

- `reward_banner` template-specificity diagnosis completed
- result: same-family non-hits do not contain a visible `UAV` panel, so the current failure mode is missing target surface / timing visibility, not recoverable small alignment drift

Current repo branch:

- `codex-v2-registry-expansion`

Current repo focus:

- post-`reward_banner` surface selection for `call_of_duty`

Current measured behavior:

- current `UAV` pilot reproduces only on `_PL_5qWwKtY`
- it stays clean off-target
- it does not reproduce on same-family `MWIII Vista` no-commentary probes

## Operator View

What Codex should do next:

- pivot toward identifying a more repeatable native `call_of_duty` surface family

What Codex should not do next:

- do not continue threshold tuning, scale tuning, or asset expansion for the current `reward_banner` family

How we know it worked:

- a new candidate surface family is selected because it shows visible cross-clip repeatability evidence, not just one-clip success

Exact missing input:

- stronger candidate native surface family with repeatable visible examples across clips
