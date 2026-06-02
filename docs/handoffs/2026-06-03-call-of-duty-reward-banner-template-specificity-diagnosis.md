# Call Of Duty Reward Banner Template Specificity Diagnosis

Date: 2026-06-03
Scope: decide whether the current published `UAV` reward-banner pilot fails to generalize because of recoverable template specificity, or because the same-family probe frames simply do not contain the target surface.

## Objective

Determine which of these is more likely:

1. the current template is too specific in scale, styling, or alignment and could be recovered with a narrow adjustment
2. the best-scoring same-family non-hits do not actually contain a `UAV` banner, so the current failure is primarily source/timing visibility rather than template tuning

## Inputs

True positive source:

- `outputs/inspection/call_of_duty_overlay_check/pl_11_full.png`
- `outputs/inspection/call_of_duty_reward_banner_specificity/pl_11_reward.png`

Best-scoring same-family non-hit frames:

- `outputs/inspection/call_of_duty_reward_banner_specificity/cx_60p06_full.png`
- `outputs/inspection/call_of_duty_reward_banner_specificity/cx_60p06_reward.png`
- `outputs/inspection/call_of_duty_reward_banner_specificity/gc_94p09_full.png`
- `outputs/inspection/call_of_duty_reward_banner_specificity/gc_94p09_reward.png`

Published detector surfaces:

- `assets/games/call_of_duty/templates/reward_banners/uav.png`
- `assets/games/call_of_duty/templates/reward_banners/uav.mask.png`
- `assets/games/call_of_duty/hud.yaml`

## Findings

### 1. The true positive crop contains a distinct structured banner

At `_PL_5qWwKtY @ 11.0s`, the reward-region crop shows:

- left-aligned aircraft icon
- wide dark title band
- centered `UAV` title
- blue divider line
- `PRESS 3 TO USE` text

This is a coherent structured panel, not a weak partial match.

### 2. The same-family non-hit crops do not show a comparable panel

At the best-scoring same-family candidate frames:

- `CXh9c8AUoZw @ 60.06s`
- `gcAGS3R2t2o @ 94.09s`

the reward-region crops are visually just scene content:

- sky
- wall edges
- player silhouette
- no dark streak panel
- no aircraft icon
- no horizontal blue divider
- no readable `UAV` title band

That means the approximate `0.85` scores are not “almost-there `UAV` banners.”
They are weak structural similarities against unrelated scene geometry.

### 3. This is not a narrow alignment problem

The visible difference between the true positive and the best same-family non-hits is categorical, not marginal:

- the true positive contains a real upper-middle banner asset
- the non-hits do not visibly contain that asset at all

So the current gap is not well explained by:

- a few pixels of ROI shift
- small scale mismatch
- minor threshold tightening/loosening

Those kinds of adjustments are useful only when the target surface is visibly present but under-matched. That is not what these best non-hit crops show.

## Decision

Diagnosis result:

- primary failure mode: `sample does not contain target surface at sampled moments`
- secondary interpretation: `current UAV template remains clip-specific until better repeated examples exist`
- not supported: `recoverable by small template adjustment`

## Recommended Next Step

Do not spend the next slice on threshold or scale tuning for this family.

Recommended branch:

1. cap the current `reward_banner` detector as a validated narrow pilot
2. pivot to a different native surface with better repeatability evidence

Only reopen `reward_banner` expansion if one of these happens:

- a new clip family is found that visibly contains the same upper-middle `UAV` panel
- a second template from the same native panel family is manually sourced and confirmed visible

## Practical Consequence

The current `reward_banner` work should now be treated as:

- useful proof that the runtime path can support a native non-equipment signal
- not yet a reliable family-expansion base for `call_of_duty`
