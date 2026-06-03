# Codex Autonomy Dashboard

Status: active-draft
Version: 0.2
Last updated: 2026-06-04

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

- choose the first discriminative anchor strategy for top-right native event cards in `call_of_duty`

Active workstream:

- `WS-001`
- `Call of Duty Native Surface Validation`

Current state:

- `reward_banner` is validated as a narrow native-HUD pilot, and the shell-anchor top-right event-card pilot has now been falsified

Desired next state:

- one anchor-specific packet identifies a narrower top-right event-card anchor that can separate positives from the existing negative set, or retires the family explicitly

## Current Blocker

Blocking mechanic:

- anchor specificity

Blocking detail:

- the shell anchor reproduces on the intended positive windows, but it also reproduces across all three negative pressure clips with the same confidence band
- the next blocker is no longer implementation mechanics
- the next blocker is finding an anchor narrower than generic top-right HUD chrome

## Active Packet

Current packet:

- `TOP_RIGHT_EVENT_CARD_ANCHOR_PACKET`

Current packet status:

- active

Current decision target:

- decide whether the top-right family has a discriminative icon, emblem, or text-fragment anchor that is specific enough for a narrow runtime pilot

Acceptance target:

- one anchor candidate is specific enough to separate the positive windows from the current negative pressure set, or the packet explicitly retires the family

Failure condition:

- the remaining visible anchors are still generic HUD chrome, require premature broad OCR expansion, or stay indistinguishable from the current negative set

## Forecast Queue

Next packet:

- `none active`

Validation packet:

- `TOP_RIGHT_EVENT_CARD_VALIDATION_PACKET`

Fallback packet:

- `EXACT_PROVENANCE_CLIP_SOURCING_PACKET`

## Current Repo Truth

Last completed work:

- top-right event-card shell pilot executed and rolled back
- result: the shell anchor matched the positive windows and all three negative pressure clips at nearly identical confidence, so it is not a valid published pilot

Current repo branch:

- `codex-v2-registry-expansion`

Current repo focus:

- discriminative anchor selection for top-right native event cards in `call_of_duty`

Current measured behavior:

- current `UAV` pilot reproduces only on `_PL_5qWwKtY`
- it stays clean off-target
- it does not reproduce on same-family `MWIII Vista` no-commentary probes
- local scouting still supports the top-right family as a plausible native surface
- the first shell-anchor top-right pilot is invalid:
  - positives hit at about `0.952`
  - negatives hit at about `0.953` to `0.956`
  - there is no usable threshold gap

## Operator View

What Codex should do next:

- use the new anchor-packet request to get one narrower, discriminative top-right anchor candidate

What Codex should not do next:

- do not keep threshold-tuning or republishing the shell anchor, and do not expand the family until an anchor-specific packet justifies it

How we know it worked:

- the next packet either names one narrower anchor with clip-backed evidence and explicit exclusions, or retires the top-right family without ambiguity

Exact missing input:

- one clip-backed anchor candidate inside the top-right card region that is more specific than the shell itself
