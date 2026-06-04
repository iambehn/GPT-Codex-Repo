# Call Of Duty Medal-Visible Replacement Samples Request

Date: 2026-05-27
Status: historical_fallback

Current live branch:

- [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md)

## Objective

Produce a fallback packet for medal-visible replacement samples for `call_of_duty`.

## Decision Target

Determine whether there are better public gameplay samples that visibly contain native medal badge icons in live gameplay HUD and are more suitable than the current measured sample family.

This packet is only a fallback if the current top-right anchor branch is explicitly retired or if the project intentionally pivots back to medal-visible replacement sourcing.

## Packet Contract

Follow:

- `docs/v2/RESEARCHER_INPUT_CONTRACT.md`
- `docs/v2/RESEARCH_PACKET_TEMPLATE.md`

Required sections:

- one exact decision target
- current repo truth
- evidence bundle
- structured findings
- recommendation
- acceptance target
- open uncertainties

## Current Repo Truth

Current published-pack truth:

- published `call_of_duty` pack now includes promoted `medal_icon` coverage for `15` medals

Current measured behavior after medal promotion:

- the current four-sample measurement set still produced:
  - `2 / 4` samples with no runtime events
  - `2 / 4` samples with equipment-only runtime events
  - `0 / 4` medal-driven outcomes
  - `0 / 4` hook candidates

Historical practical blocker at the time:

- the current measured sample family does not show clear enough native medal badge evidence to validate the new medal pack slice
- at the time of writing, the active first-path investigation was the `call_of_duty` text/banner signal packet
- that is no longer the live branch

Relevant repo evidence:

- `docs/handoffs/2026-05-27-call-of-duty-medal-packet-promotion-results.md`
- `docs/handoffs/2026-05-27-call-of-duty-sample-family-audit.md`
- `docs/handoffs/2026-05-27-call-of-duty-text-banner-frame-probes.md`
- `docs/handoffs/2026-05-27-call-of-duty-text-banner-runtime-recon.md`

## Required Evidence

For every candidate clip, include:

- exact URL or local path
- exact timestamps where medal badges are visible
- title-family notes
- whether the medal appears to be native HUD or overlay
- screenshot or crop references
- whether the current `medal_area` ROI assumptions appear compatible
- why this sample is useful

## Required Findings

Separate findings into:

- target clips to keep
- ambiguous samples
- false positives
- unusable clips
- title-family caveats

## Preferred Medal Families

Prefer samples that visibly show:

- multikill medals
- elimination-streak medals
- payoff or victory medals

## Avoid

Avoid:

- overlays added in editing
- compilation graphics
- clips where medals are hidden by compression
- clips without stable HUD visibility
- broad montage videos without timestamps

## Acceptance Target

The packet is good enough when Codex can:

- determine whether medal-visible replacement samples are a better implementation path than the currently active top-right anchor route if that route is retired
- select at least one replacement-grade clip family
- validate whether current `medal_area` ROI assumptions appear structurally compatible
- proceed into a runtime extraction packet without guessing
