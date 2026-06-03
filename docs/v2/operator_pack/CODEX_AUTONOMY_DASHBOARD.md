# Codex Autonomy Dashboard

Status: active-draft
Version: 0.2
Last updated: 2026-06-03

Purpose:

- keep one thin live snapshot of Codex execution state
- reduce pause time caused by queue ambiguity or context reconstruction
- answer "what should happen next?" with minimal interpretation burden

This file is a coordination snapshot, not a second source of truth.

This file is not:

- a second backlog
- an archive
- a governance surface
- a replacement for evidence handoffs
- a replacement for the run log

This file is:

- the fastest current answer to "what should happen next?"
- a compact state snapshot for Codex and the human operator
- a coordination surface that summarizes, but does not replace, other operator-pack files

Subordinate surfaces:

- chronology lives in [CODEX_RUN_LOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_RUN_LOG.md)
- queued work lives in [CODEX_BACKLOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_BACKLOG.md)
- phase intent lives in [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md)
- evidence and measured outcomes live in handoff docs under `docs/handoffs/`

Update trigger:

- update this file only when at least one of these changes:
  - the next state transition changes
  - the active blocker changes
  - the active packet changes
  - the recommended next action changes
  - the exact missing input changes
  - the repo truth changes enough to alter the execution branch
- do not update it for:
  - routine milestone logging
  - minor progress within the same branch
  - historical completeness
  - generic documentation polish
- do not duplicate large narrative history here

Maintenance posture:

- primary updater: `Researcher`
- Codex updates this file only when a local execution decision materially changes
- this file should remain writable from current repo truth in under one minute
- every field should change Codex behavior if updated

## Active Execution Target

Active objective:

- prepare the first direct implementation slice for top-right native event cards in `call_of_duty`

Active workstream:

- `WS-001`
- `Call of Duty Native Surface Validation`

Current state:

- `reward_banner` is validated as a narrow native-HUD pilot, and the next surviving family is top-right native event cards rather than generic killfeed

Desired next state:

- the first top-right event-card runtime pilot is implemented and validated on the clip-backed evidence windows

## Current Blocker

Blocking mechanic:

- implementation execution

Blocking detail:

- the direct implementation handoff now exists
- the next blocker is executing the pilot without slipping into generic killfeed semantics or broad OCR expansion

## Active Packet

Current packet:

- `TOP_RIGHT_EVENT_CARD_CODEX_HANDOFF`

Current packet status:

- implementation_ready

Current decision target:

- implement the first top-right native event-card runtime slice using a shell-anchor strategy over the existing top-right ROI

Acceptance target:

- the pilot emits the new family on the two positive evidence windows and stays controlled on the existing negative pressure set

Failure condition:

- the pilot requires broad OCR, collapses into generic killfeed semantics, or produces uncontrolled false positives

## Forecast Queue

Next packet:

- `none active`

Validation packet:

- `TOP_RIGHT_EVENT_CARD_VALIDATION_PACKET`

Fallback packet:

- `EXACT_PROVENANCE_CLIP_SOURCING_PACKET`

## Current Repo Truth

Last completed work:

- direct implementation handoff completed
- result: the first pilot is now bounded around a template-compatible top-right card-shell anchor with conservative `hud_visibility -> high_action_sequence` mapping

Current repo branch:

- `codex-v2-registry-expansion`

Current repo focus:

- first top-right native event-card runtime slice for `call_of_duty`

Current measured behavior:

- current `UAV` pilot reproduces only on `_PL_5qWwKtY`
- it stays clean off-target
- it does not reproduce on same-family `MWIII Vista` no-commentary probes
- local scouting shows the visible repeated top-right family is better described as `objective_event_notifications` or top-right event-status cards than as generic killfeed
- same-title support exists at `gcAGS3R2t2o @ 27s-29s`

## Operator View

What Codex should do next:

- implement the first top-right event-card runtime pilot from the direct handoff

What Codex should not do next:

- do not continue threshold tuning, scale tuning, or asset expansion for the current `reward_banner` family, and do not keep doing blind local surface scouting

How we know it worked:

- the implemented pilot hits `_PL_5qWwKtY` and `gcAGS3R2t2o` in the expected windows without forcing broad OCR infrastructure

Exact missing input:

- none for the first pilot slice
