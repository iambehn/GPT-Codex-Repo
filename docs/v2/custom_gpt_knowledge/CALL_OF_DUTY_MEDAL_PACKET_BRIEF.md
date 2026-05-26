# Call Of Duty Medal Packet Brief

Use this brief when asking the research GPT to gather the next high-value input for Codex.

## Objective

Produce a medal-specific asset promotion packet for `call_of_duty` that is specific enough for Codex to promote a first HUD medal subset into onboarding review.

## Why This Matters

Current repo truth:

- the published `call_of_duty` pack has `hero_portrait` and `equipment_icon` coverage
- it does not yet have promoted `medal_icon` coverage
- the wiki-to-onboarding bridge now works
- pre-bridge curation now works
- the current raw wiki source family is the wrong source family for medal promotion
- cross-clip measurement on four public samples showed:
  - `2 / 4` samples produced no runtime events
  - `2 / 4` samples produced equipment-only events
  - `0 / 4` samples produced medal-driven outcomes

This means the current bottleneck is upstream medal source quality, not pipeline routing or onboarding shape.

## Exact Decision Target

Identify the first `call_of_duty` gameplay HUD medal subset worth promoting into onboarding review.

## Required Scope

Focus only on true gameplay HUD medals that are visible during live gameplay.

Preferred first subset:

- multikill medals
- elimination-streak medals
- payoff or victory medals

## Explicit Exclusions

Do not include:

- contracts
- intel missions
- calling cards
- weapon blueprints
- watches
- logos
- operator skins
- map imagery
- season branding
- game-mode branding
- general Warzone key art

If a source page mixes medals with those other asset types, isolate the actual medal rows and call out the contamination explicitly.

## Required Output Shape

Follow [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md).

At minimum, the packet must include:

1. `decision target`
2. `current repo truth`
3. `evidence bundle`
4. `structured findings`
5. `recommendation`
6. `acceptance target`
7. `open uncertainties`

## Required Evidence

For each proposed medal, provide:

- exact display name
- aliases if any
- source page URL
- one stable image source or crop
- explanation of why it is a gameplay HUD medal
- whether it belongs to:
  - multikill
  - elimination-streak
  - payoff or victory

Strongly preferred:

- live HUD screenshots
- tight medal crops
- clip timestamps showing the medal in gameplay context

## Required Findings

The packet must separate:

- target medals to keep
- ambiguous candidates needing manual judgment
- exclusions and false positives

The packet must not collapse those into one mixed list.

## Acceptance Target For Codex

The packet is good enough when Codex can use it to:

1. create or curate a narrower medal source set
2. bridge it into onboarding
3. validate that onboarding gains real `medal_icon` candidates
4. rerun runtime measurement on the existing `call_of_duty` sample set

## Source Constraints

Prefer:

- medal-specific pages
- screenshot collections with medal-only focus
- gameplay HUD captures

Avoid:

- broad wiki dump pages
- pages centered on contracts, maps, or cosmetics
- image sets with no reliable label mapping

## Copy-Paste Prompt

```text
Produce a medal-specific asset promotion packet for call_of_duty using the repo's Research Packet Template.

Decision target:
Identify the first gameplay HUD medal subset worth promoting into onboarding review.

Current repo truth:
- published call_of_duty pack has hero_portrait and equipment_icon coverage but no promoted medal_icon coverage
- wiki-to-onboarding bridge works
- pre-bridge curation works
- current raw wiki source family is dominated by contracts, calling cards, blueprints, watches, logos, and map/season imagery
- cross-clip measurement on four public samples produced either no events or equipment-only events, with zero medal-driven outcomes

Required scope:
- true gameplay HUD medals only
- prioritize multikill medals, elimination-streak medals, and payoff/victory medals

Explicit exclusions:
- contracts
- intel missions
- calling cards
- weapon blueprints
- watches
- logos
- operator skins
- map imagery
- season branding
- game-mode branding

Required output:
- decision target
- current repo truth
- evidence bundle
- structured findings
- recommendation
- acceptance target
- open uncertainties

For each proposed medal, include:
- exact display name
- aliases if any
- source page URL
- stable image or crop
- why it is a gameplay HUD medal
- category: multikill, elimination-streak, or payoff/victory
```
