# Call Of Duty Wiki Source Audit

Date: 2026-05-25
Status: active

## Objective

Audit the raw `call_of_duty` wiki draft bundle to determine whether it actually contains gameplay HUD medal material worth promoting into the published pack.

Raw bundle:

- `assets/games/call_of_duty/drafts/wiki/20260430T015758Z`

## High-Level Finding

The current raw wiki bundle is not just noisy. It appears to be the wrong source family for the first medal-promotion target.

The bundle is dominated by:

- map and season imagery
- quest and intel prose
- contracts
- calling cards
- weapon blueprints
- watches
- logos and branding

It does not appear to contain recognizable gameplay HUD medal names for the first target families.

## Raw Surface Summary

From `catalog/assets.csv`:

- asset rows: `250`
- `qa_status: needs_manual_crop` on all `250`
- `source_role: events` on all `250`
- `asset_kind`: blank on all `250`
- `category`: blank on all `250`

From `catalog/events_or_medals.csv`:

- event rows: `265`

## Dominant Event/Label Patterns

Pattern counts from `catalog/events_or_medals.csv`:

- `map_or_season`: `34`
- `weapon_blueprint`: `27`
- `calling_card`: `15`
- `watch`: `11`
- `contract`: `9`
- `logo_branding`: `5`
- `intel_mission`: `1`
- unmatched by those coarse patterns: `163`

The unmatched pool is still largely non-medal material such as:

- intel story text
- mission instructions
- operator skins
- emblems
- weapon charms
- stickers
- game-mode labels

## Common Medal-Term Audit

The raw bundle does not contain the usual first-pass gameplay medal terms.

Direct string scans returned:

- `headshot`: `0`
- `longshot`: `0`
- `kingslayer`: `0`
- `collateral`: `0`
- `double kill`: `0`
- `triple kill`: `0`
- `quad kill`: `0`
- `bloodthirsty`: `0`
- `merciless`: `0`
- `first blood`: `0`

Apparent false positives or irrelevant hits:

- `elimination`: `1` but only inside contract prose
- `avenger`: `1` but only inside scavenger contract prose
- `frenzy`: `1` but only in a limited-time titan event description
- `ultra`: `1` but only in loot text

## What This Means

The current problem is not:

- onboarding draft shape
- wiki-to-onboarding bridge behavior
- pre-bridge curation mechanics

The current problem is upstream source selection.

The existing wiki bundle does not appear to contain the medal family we need for first-pass HUD detection work.

## Researcher Packet Needed Next

The next researcher packet should target a different source family and provide:

1. true gameplay HUD medal names
2. screenshots or crops of those medals in live gameplay HUD context
3. source pages or image sets that are medal-specific rather than broad mode/wiki aggregates
4. explicit first target subset:
   - multikill medals
   - elimination-streak medals
   - payoff or victory medals
5. explicit exclusions:
   - contracts
   - intel missions
   - calling cards
   - weapon blueprints
   - watches
   - logos
   - map or season imagery

## Recommendation

Do not spend the next slice broadening curation heuristics against this same raw bundle.

Use the current audits to request a narrower, medal-specific source packet first. Then rerun:

1. pre-bridge curation
2. wiki-to-onboarding bridge
3. onboarding review and publish-readiness checks
