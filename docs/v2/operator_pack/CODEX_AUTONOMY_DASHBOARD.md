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

- choose the correct post-`reward_banner` branch for `call_of_duty`

Active workstream:

- `WS-001`
- `Call of Duty Native Surface Validation`

Current state:

- `reward_banner` is validated as a narrow native-HUD pilot, and `killfeed_events` is the next recommended family to scout

Desired next state:

- `killfeed_events` is either validated as the next runtime family or rejected with explicit evidence

## Current Blocker

Blocking mechanic:

- repeatability failure

Blocking detail:

- the published `UAV` reward-banner family is clean but does not reproduce across same-family `MWIII Vista` probes
- the lower-center text in `_PL_5qWwKtY` behaves like editorial or overlay presentation and should not be promoted as pack truth

## Active Packet

Current packet:

- `ALTERNATIVE_NATIVE_SURFACE_PACKET`

Current packet status:

- received and narrowed

Current decision target:

- choose the next repeatable native `call_of_duty` HUD surface family after capping `reward_banner`, with `killfeed_events` currently recommended first

Acceptance target:

- one alternative native surface family is concrete enough for a narrow runtime pilot, and the next local validation slice is explicit

Failure condition:

- the next branch remains ambiguous and Codex falls back into low-yield `reward_banner` tuning, blind surface scouting, or pack mutation without clip evidence

## Forecast Queue

Next packet:

- `ALTERNATIVE_NATIVE_SURFACE_PACKET`

Validation packet:

- `SURFACE_REPEATABILITY_VALIDATION_PACKET`

Fallback packet:

- `EXACT_PROVENANCE_CLIP_SOURCING_PACKET`

## Current Repo Truth

Last completed work:

- same-family local surface scout completed
- result: no strong repeatable native alternative surface emerged from local `MWIII Vista` clip inspection alone

Current repo branch:

- `codex-v2-registry-expansion`

Current repo focus:

- post-`reward_banner` surface selection for `call_of_duty`

Current measured behavior:

- current `UAV` pilot reproduces only on `_PL_5qWwKtY`
- it stays clean off-target
- it does not reproduce on same-family `MWIII Vista` no-commentary probes
- the received alternative-surface packet recommends `killfeed_events` as the next family to validate locally

## Operator View

What Codex should do next:

- scout `kill_feed` visibility and repeatability on the current `call_of_duty` sample family before any published-pack mutation

What Codex should not do next:

- do not continue threshold tuning, scale tuning, or asset expansion for the current `reward_banner` family, and do not keep doing blind local surface scouting

How we know it worked:

- `killfeed_events` is either advanced with clip-backed evidence and extraction posture, or rejected with explicit reasons and the fallback family becomes active

Exact missing input:

- exact clip-backed `killfeed_events` evidence with timestamps, visibility notes, and extraction-feasibility observations across the current sample family
