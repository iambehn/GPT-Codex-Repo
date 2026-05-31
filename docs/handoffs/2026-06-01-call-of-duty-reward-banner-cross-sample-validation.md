# Call Of Duty Reward Banner Cross-Sample Validation

Date: 2026-06-01
Scope: validate whether the masked published-pack `reward_banner` pilot generalizes across the existing four-sample `call_of_duty` measurement set.

## Objective

Decide whether the current `UAV` reward-banner pilot is:

- clean and reusable across the measured sample set
- noisy and not yet safe to keep
- structurally valid but clip-family-specific

## Inputs

Published pack state:

- `assets/games/call_of_duty/hud.yaml`
- `assets/games/call_of_duty/manifests/cv_templates.yaml`
- `assets/games/call_of_duty/manifests/detection_manifest.yaml`
- `assets/games/call_of_duty/manifests/runtime_cv_rules.yaml`

Measured sources:

- `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
- `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
- `outputs/public_gameplay_mining/call_of_duty_measurement_sources/v-SzAArdAfY.60s-70s.mp4`
- `outputs/public_gameplay_mining/call_of_duty_measurement_sources/Qop1sH70nHI.60s-70s.mp4`

Execution command shape:

```bash
.venv/bin/python run.py --analyze-roi-runtime <source> call_of_duty \
  --output-path <runtime_json> \
  --debug-output-dir <debug_dir> \
  --sample-fps 1 \
  --limit-frames 30
```

Output root:

- `outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/`

## Results

### 1. `SVbTc2AZzYw.60s-70s.mp4`

Runtime sidecar:

- `outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/SVbTc2AZzYw.60s-70s.runtime.json`

Observed result:

- status: `ok`
- event count: `3`
- event types: `ability_seen` only
- reward-banner events: `0`
- false-positive reward-banner hits: none observed

Interpretation:

- the masked `UAV` banner did not leak into this sample
- this remains an equipment-only sample under the current pack

### 2. `_PL_5qWwKtY.mp4`

Runtime sidecar:

- `outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/_PL_5qWwKtY.runtime.json`

Observed result:

- status: `ok`
- event count: `6`
- event types:
  - `ability_seen`: `5`
  - `high_action_sequence`: `1`
- reward-banner events: `1`
- confirmed reward-banner timestamp: `11.0s`
- matched asset: `call_of_duty.uav_reward_banner.reward_banner`
- confidence: `0.91073`

Interpretation:

- the masked `UAV` banner remains detectable in the one sample where local frame probes already showed the target surface
- this sample is still the only measured source that produces a non-equipment runtime event from the new family

### 3. `v-SzAArdAfY.60s-70s.mp4`

Runtime sidecar:

- `outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/v-SzAArdAfY.60s-70s.runtime.json`

Observed result:

- status: `no_events`
- event count: `0`
- reward-banner events: `0`
- false-positive reward-banner hits: none observed

Interpretation:

- the new family does not create spurious banner events on this no-event sample

### 4. `Qop1sH70nHI.60s-70s.mp4`

Runtime sidecar:

- `outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/Qop1sH70nHI.60s-70s.runtime.json`

Observed result:

- status: `no_events`
- event count: `0`
- reward-banner events: `0`
- false-positive reward-banner hits: none observed

Interpretation:

- the new family does not create spurious banner events on this no-event sample

## Summary Table

| Sample | Prior baseline | New result | Reward banner |
| --- | --- | --- | --- |
| `SVbTc2AZzYw.60s-70s` | equipment-only | equipment-only | no |
| `_PL_5qWwKtY` | equipment-only | equipment + `high_action_sequence` | yes |
| `v-SzAArdAfY.60s-70s` | no events | no events | no |
| `Qop1sH70nHI.60s-70s` | no events | no events | no |

## Decision

The masked `reward_banner` pilot is:

- clean across the current four-sample validation set
- not yet generalized across multiple clip families
- currently proven only for the `_PL_5qWwKtY` sample family

That is still a useful win. The family is no longer speculative and no longer obviously noisy. But it is not yet evidence for broader `call_of_duty` reward-banner coverage.

## Recommended Next Step

Do not add more reward-banner templates yet.

Take one of these next:

1. collect one or two additional clips that visibly contain the same `UAV`-style reward banner family and rerun this same masked validation path
2. move to a second narrow text family only if there is better local evidence than there is for a second reward banner

Current recommendation:

- stay within `reward_banner`
- validate on one more compatible clip family before expanding templates or starting OCR work
