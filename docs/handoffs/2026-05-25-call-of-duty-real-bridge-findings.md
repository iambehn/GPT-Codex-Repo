# `call_of_duty` Real Wiki-Bridge Findings

Date: 2026-05-25
Status: complete

## Objective

Run the new wiki-to-onboarding bridge on the real `call_of_duty` wiki draft and inspect the resulting onboarding draft to determine whether the next blocker is workflow shape or source quality.

## Command

```bash
python run.py --bridge-wiki-draft-to-onboarding assets/games/call_of_duty/drafts/wiki/20260430T015758Z
```

## Result

Bridge output draft:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

Bridge counts:

- `heroes: 99`
- `abilities: 13`
- `events: 265`
- `detection_rows: 377`
- `candidate_assets: 362`
- `binding_candidates: 510`
- `qa_queue: 925`
- `manual_crop_required: 250`
- `published_baseline_candidates: 112`
- `wiki_medal_candidates: 250`

Publish-readiness result:

- `phase_status: bindings_pending`
- `readiness: needs_binding_review`
- `can_publish: false`
- `structural_findings: 0`

## What The Draft Actually Contains

Detection-family counts:

- `hero_portrait: 99`
- `equipment_icon: 13`
- `medal_icon: 265`

Binding-status counts:

- `accepted: 112`
- `pending_review: 398`

Derived medal rows:

- `with_candidates: 121`
- `without_candidates: 144`

Top QA categories:

- `binding_candidate: 510`
- `manual_crop_required: 250`
- `missing_binding: 144`

## Main Finding

The bridge succeeded, but the real wiki draft is heavily polluted.

Examples of high-frequency non-medal source material that became `medal_icon` rows:

- contracts and contract descriptions
- calling cards
- weapon blueprints
- maps and season branding
- franchise or game logos

Examples seen in the bridged draft:

- `Call of Duty`
- `Black Ops Cold War[]`
- `Vanguard[]`
- `A Big Game Bounty contract marks a player...`
- `"Aerial Pursuit" Epic Calling Card`
- `"Ancient Ruins" Legendary DP27 Weapon Blueprint`

## Practical Meaning

The current blocker has moved:

- before: wiki medal assets were stranded in non-onboarding draft shape
- now: the bridge puts them into canonical onboarding shape successfully
- current blocker: the source bundle is not a clean medal set

The next step should not be mass review of the current `265` bridged medal rows.

That would force review work over a source set that includes a large amount of non-medal material and prose-derived noise. The review queue is now inspectable, but it is not yet a good operator target.

## Recommended Next Target

Focus the next local-only cycle on `call_of_duty` medal-source curation, not on bridge mechanics.

Highest-leverage options:

1. add a curation pass that narrows the wiki medal or event bundle to likely true HUD medals before bridging or before derived-row review
2. create a researcher packet that identifies the true `call_of_duty` medal or streak subset worth promoting first
3. define a narrower first-promotion target such as multikill, elimination-streak, or victory medal families instead of attempting all `250` imported assets

## Conclusion

The new bridge is good enough to keep.

The next meaningful improvement is not more onboarding-shape work. It is better control over which wiki-derived rows are allowed to become `medal_icon` onboarding targets.
