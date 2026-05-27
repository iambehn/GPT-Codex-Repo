# Call Of Duty Text Banner Runtime Recon

Date: 2026-05-27
Status: active

## Objective

Map the existing repo surfaces that a future `call_of_duty` text or reward-banner slice would need to extend.

This is an implementation-side reconnaissance note. It does not choose the final signal family. It records where the current runtime path begins, what it can already host, and what would need to change once the external text/banner packet is complete.

## Current Published Runtime Truth

Published `call_of_duty` runtime rules currently map only these asset families:

- `equipment_icon -> equipment_visibility -> ability_seen`
- `hero_portrait -> character_identity -> pov_character_identified`
- `medal_icon -> medal_visibility -> medal_seen`

Published files:

- `assets/games/call_of_duty/manifests/runtime_cv_rules.yaml`
- `assets/games/call_of_duty/manifests/fusion_rules.yaml`
- `assets/games/call_of_duty/manifests/detection_manifest.yaml`
- `assets/games/call_of_duty/manifests/cv_templates.yaml`

Current result:

- there is no published `call_of_duty` runtime family for text or reward-banner signals
- the matcher path is still template-ROI based, not OCR-based

## Existing Runtime Entry Points

### 1. Detection schema family definition

Relevant file:

- `starter_assets/runtime_detection_schema.yaml`

Current default families:

- `hero_portrait`
- `ability_icon`
- `equipment_icon`
- `medal_icon`

Implication:

- a future text/banner path would likely require a new family rather than overloading `medal_icon`
- current schema defaults assume `requires_asset: true` template-based matching

### 2. Game-specific family overrides

Relevant file:

- `starter_assets/call_of_duty/game_detection_schema_overrides.yaml`

Current `call_of_duty` overrides:

- disables `ability_icon`
- lowers thresholds for:
  - `equipment_icon`
  - `hero_portrait`

Implication:

- there is no existing game-specific text/banner family override
- adding one would be a clean extension, not a conflict with a current published family

### 3. ROI matcher contract

Relevant file:

- `pipeline/roi_matcher.py`

Current behavior:

- decodes video frames
- crops named ROIs from `hud.yaml`
- performs template matching over published templates
- writes confirmed detections

Implication:

- the current runtime pipeline natively supports:
  - template-backed icon/image families
- it does not natively support:
  - OCR-first text extraction
  - raw text-region decoding without templates

### 4. Event mapper contract

Relevant file:

- `pipeline/event_mapper.py`

Current behavior:

- takes confirmed detections
- resolves runtime rules by `asset_family`
- emits runtime signals and events

Implication:

- once a new signal family produces matcher-compatible detections, the event-mapping path is already the correct downstream contract
- the real implementation question is upstream of event mapping:
  - how detections get produced
  - how target values are defined

### 5. Runtime ontology capacity

Relevant files:

- `starter_assets/runtime_signal_event_ontology.yaml`
- `pipeline/runtime_ontology.py`

Current ontology already allows signal families such as:

- `hud_visibility`
- `combat_feed_visibility`
- `round_state_visibility`

Implication:

- the ontology can host new visibility classes without inventing an entirely separate runtime contract
- but the current `call_of_duty` published pack does not instantiate any text/banner family yet

## Current HUD ROI Truth

Relevant file:

- `assets/games/call_of_duty/hud.yaml`

Published ROIs:

- `hero_portrait`
- `ability_hud`
- `medal_area`
- `kill_feed`

Current gap:

- there is no named ROI for:
  - upper-center reward banner text
  - lower-center kill-count text

## Provisional ROI Seeds From Local Frame Probes

Based on `_PL_5qWwKtY` crop artifacts at `1920x1080`:

### Reward banner seed

From:

- `outputs/measurement/call_of_duty_text_banner_probes/uav_banner_11.0_crop.png`

Crop box:

- `x=570`
- `y=195`
- `width=760`
- `height=170`

Normalized seed:

- `x_pct: 0.297`
- `y_pct: 0.181`
- `w_pct: 0.396`
- `h_pct: 0.157`

Interpretation:

- this is a plausible first `reward_banner_area`
- it overlaps the current broad `medal_area`, but is wider and slightly lower

### Lower-center kill-count seed

From:

- `outputs/measurement/call_of_duty_text_banner_probes/killcount_4th_11.0_crop.png`

Crop box:

- `x=810`
- `y=900`
- `width=260`
- `height=120`

Normalized seed:

- `x_pct: 0.422`
- `y_pct: 0.833`
- `w_pct: 0.135`
- `h_pct: 0.111`

Interpretation:

- this is a plausible first `kill_count_area`
- it is materially different from `ability_hud`

### Mid-right multikill text seed

From:

- `outputs/measurement/call_of_duty_text_banner_probes/doublekill_20.5_crop.png`

Crop box:

- `x=960`
- `y=430`
- `width=520`
- `height=120`

Normalized seed:

- `x_pct: 0.500`
- `y_pct: 0.398`
- `w_pct: 0.271`
- `h_pct: 0.111`

Interpretation:

- this is a plausible first `multikill_text_area`
- the local frame is partial-read, so this ROI should be treated as provisional only

## Likely Implementation Shapes

The next valid implementation slice will probably fall into one of these shapes.

### Option A: template-backed text/banner family

Use when:

- the external packet returns stable, repeated visual banners or stylized text surfaces
- local evidence suggests the text appearance is title-stable enough for templates

Likely shape:

- add one or more new schema families such as:
  - `reward_banner`
  - `kill_count_text`
  - `multikill_text`
- add new ROI names to `hud.yaml`
- add published templates and runtime rule mappings

Advantage:

- fits the current ROI matcher and event mapper with the least architectural disruption

Risk:

- weak if the text varies too much for template matching alone

### Option B: mixed OCR and template path

Use when:

- the external packet classifies signals as `mixed_ocr_template`
- the text is readable, but style or motion blur makes pure templates weak

Likely shape:

- add ROI-level extraction support beyond current template-only matching
- preserve the same downstream runtime signal/event contract

Advantage:

- better fit for text surfaces that are legible but not template-stable

Risk:

- this is a real runtime behavior extension, not just a new pack slice

### Option C: abandon current clip family for medal-visible replacement samples

Use when:

- the external packet cannot establish that the current visible text surfaces are native game UI
- or the chosen signal family does not look stable enough to support a clean first runtime slice

Advantage:

- avoids teaching the pack the wrong surface

Risk:

- pushes the bottleneck back to sample acquisition

## Current Best Local Recommendation

If the external packet comes back strong enough, the smallest likely first runtime slice is:

1. choose exactly one first family:
   - `reward_banner`, or
   - `kill_count_text`
2. prefer a template-compatible pilot if the packet supports it
3. treat OCR or mixed extraction as the next step only if the packet proves templates are too weak

Current local evidence suggests:

- `reward_banner` and `kill_count_text` are cleaner first candidates than `multikill_text`
- `multikill_text` is still useful, but the current local read is partial

## What This Recon Does Not Decide

This note does not decide:

- whether the visible text surfaces are definitely native game UI
- whether OCR is required
- whether the current clip family should remain canonical for the next runtime slice
- which exact signal family should be implemented first

Those answers should come from the external text/banner packet.
