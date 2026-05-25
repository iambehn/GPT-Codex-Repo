# Call Of Duty Cross-Clip Measurement Audit

Date: 2026-05-25
Status: active

## Objective

Measure how the current published `call_of_duty` pack behaves across multiple real public gameplay samples, using one consistent bounded runtime/fusion path.

## Samples

1. canonical local test sample
   - `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
2. existing editorial scout sample
   - `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
3. new no-commentary gameplay sample
   - `outputs/public_gameplay_mining/call_of_duty_measurement_sources/v-SzAArdAfY.60s-70s.mp4`
4. new no-commentary gameplay sample
   - `outputs/public_gameplay_mining/call_of_duty_measurement_sources/Qop1sH70nHI.60s-70s.mp4`

Measurement root:

- `outputs/measurement/call_of_duty/`

## Commands Used Per Sample

```bash
python run.py --analyze-roi-runtime <SOURCE> call_of_duty --sample-fps 1 --limit-frames 30 --output-path <RUNTIME_JSON>
python run.py --fuse-clip-signals <SOURCE> call_of_duty --runtime-sidecar <RUNTIME_JSON> --output-path <FUSED_JSON>
python run.py --export-highlight-selection --fused-sidecar <FUSED_JSON> --output-path <SELECTION_JSON>
python run.py --refresh-clip-registry <ROOT> --registry-path <REGISTRY_SQLITE>
python run.py --derive-hook-candidates <FUSED_JSON> --registry-path <REGISTRY_SQLITE> --output-path <HOOK_JSON>
```

## Results By Sample

### 1. `SVbTc2AZzYw.60s-70s`

- runtime status: `ok`
- runtime signals: `3`
- runtime event rows: `3`
- runtime event family: `ability_seen`
- runtime asset family: `equipment_icon`
- observed equipment:
  - `armor_satchel`
  - `armor_plates`
  - `redeploy_extraction_token`
- fused status: `ok`
- fused event rows: `3`
- fused rule pattern:
  - `equipment_visibility_atomic`
- selected highlights: `3`
- hook candidates: `0`
- registry lifecycle state:
  - `pending_review: 3`

### 2. `_PL_5qWwKtY`

- runtime status: `ok`
- runtime signals: `5`
- runtime event rows: `5`
- runtime event family: `ability_seen`
- runtime asset family: `equipment_icon`
- observed equipment:
  - `armor_satchel`
  - `armor_plates`
  - `redeploy_extraction_token`
- fused status: `ok`
- fused event rows: `5`
- fused rule pattern:
  - `equipment_visibility_atomic`
- selected highlights: `5`
- hook candidates: `0`
- registry lifecycle state:
  - `pending_review: 5`

### 3. `v-SzAArdAfY.60s-70s`

- runtime status: `no_events`
- runtime signals: `0`
- runtime event rows: `0`
- fused status: `no_fused_events`
- fused event rows: `0`
- selected highlights: `0`
- hook candidates: `0`
- registry lifecycle rows: none

### 4. `Qop1sH70nHI.60s-70s`

- runtime status: `no_events`
- runtime signals: `0`
- runtime event rows: `0`
- fused status: `no_fused_events`
- fused event rows: `0`
- selected highlights: `0`
- hook candidates: `0`
- registry lifecycle rows: none

## Cross-Clip Summary

Across all four measured samples:

- `2 / 4` samples produced runtime events
- `2 / 4` samples produced no runtime events
- `0 / 4` samples produced medal events
- `0 / 4` samples produced identity events
- `0 / 4` samples produced hook candidates
- every successful fused sample reduced to:
  - `event_type: ability_seen`
  - `rule_id: equipment_visibility_atomic`

This is stronger evidence than the earlier single-sample diagnosis.

The current published `call_of_duty` pack is not merely weak on one clip. It is structurally narrow across multiple measured samples:

- sometimes no events at all
- otherwise equipment-only evidence
- no medal corroboration
- no richer fused combinations
- no route into meaningful hook evaluation without manual review and stronger upstream evidence

## Interpretation

The main current bottleneck is upstream coverage, not operator sampling strategy.

The measured failure surface now looks like this:

1. the published pack has no promoted medal coverage
2. the raw wiki source family is not actually medal-specific
3. the current runtime layer therefore either:
   - emits only equipment visibility
   - or emits nothing
4. fusion cannot produce richer medal-driven combinations
5. hook derivation remains empty because candidates never become editorially meaningful enough to progress naturally

## Recommended Next Step

Do not spend the next local-only slice downloading more random gameplay clips.

That would improve confidence only marginally.

The highest-value next input is still a medal-specific researcher packet containing:

- true gameplay HUD medal names
- crops or screenshots
- stable source pages or image sets
- explicit exclusions for contracts, intel missions, calling cards, blueprints, watches, logos, and map or season art

Once that exists, rerun:

1. pre-bridge curation
2. wiki-to-onboarding bridge
3. onboarding review and publish-readiness
4. runtime measurement on the same cross-clip set
