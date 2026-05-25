# Call Of Duty Pre-Bridge Curation Findings

Date: 2026-05-25
Status: active

## Objective

Test whether a narrow pre-bridge curation profile can reduce the noisy `call_of_duty` wiki medal bundle into a useful onboarding input before rows enter the canonical onboarding workflow.

## Commands Run

```bash
python run.py --curate-wiki-medal-draft assets/games/call_of_duty/drafts/wiki/20260430T015758Z
python run.py --bridge-wiki-draft-to-onboarding assets/games/call_of_duty/drafts/wiki_curated/20260525T031910Z
python run.py --validate-onboarding-publish assets/games/call_of_duty/drafts/onboarding/20260525T032023Z
```

## Result

The pre-bridge curation workflow works mechanically, but the current `multikill` profile keeps zero real rows from the raw wiki bundle.

Curated wiki result:

- curated root: `assets/games/call_of_duty/drafts/wiki_curated/20260525T031910Z`
- profile: `multikill`
- raw asset count: `250`
- raw event count: `265`
- kept asset count: `0`
- kept event count: `0`
- dropped asset count: `250`
- dropped event count: `265`

Decision reason summary:

- `outside_profile`: `304`
- `intel_mission_text`: `77`
- `map_or_season_label`: `71`
- `weapon_blueprint`: `27`
- `calling_card`: `15`
- `logo_or_branding`: `12`
- `contract_text`: `9`

## Bridge Outcome

The curated bundle still bridges correctly into canonical onboarding shape:

- bridged draft root: `assets/games/call_of_duty/drafts/onboarding/20260525T032023Z`
- readiness: `ready_to_publish`
- structural findings: `0`

Bridged draft counts:

- heroes: `99`
- abilities: `13`
- events: `0`
- detection rows: `112`
- candidate assets: `112`
- binding candidates: `112`
- published baseline candidates: `112`
- wiki medal candidates: `0`

This means the pre-bridge curation step did not corrupt the onboarding flow. It simply collapsed back to the published baseline because the raw wiki bundle did not contain usable multikill rows under the current profile.

## Evidence That The Raw Bundle Lacks Multikill Signals

A direct scan of `catalog/events_or_medals.csv` showed:

- `double`: `2` matches, both weapon blueprints
- `triple`: `0`
- `quad`: `0`
- `multi`: `0`
- `collateral`: `0`
- `fury`: `0`
- `bloodthirsty`: `0`
- `merciless`: `0`
- `streak`: `0`
- `medal`: `0`

## Conclusion

The current blocker is no longer onboarding shape or bridge logic.

The blocker is source quality:

- the raw `call_of_duty` wiki draft bundle is dominated by non-HUD material
- the first useful curation profile proves that the desired multikill subset is not present in usable form
- further progress now depends on better medal-source research or a different upstream acquisition packet

## Next Research Packet Needed

The next high-value researcher packet should provide:

1. true gameplay HUD medal names for `call_of_duty`
2. screenshots or crops of those medals in live HUD context
3. a first promotion subset, preferably:
   - multikill medals
   - elimination-streak medals
   - payoff or victory medals
4. explicit exclusions for:
   - contracts
   - calling cards
   - blueprints
   - logos
   - map or season branding
