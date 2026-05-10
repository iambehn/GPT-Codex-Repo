## Goal

Add the first published objective/round-state HUD slice to the `marvel_rivals` pack so the pipeline can emit stable non-identity round-state runtime and fused evidence.

## Scope

In scope:

- `assets/games/marvel_rivals/hud.yaml`
- `assets/games/marvel_rivals/manifests/detection_manifest.yaml`
- `assets/games/marvel_rivals/manifests/cv_templates.yaml`
- `assets/games/marvel_rivals/manifests/runtime_cv_rules.yaml`
- `assets/games/marvel_rivals/manifests/fusion_rules.yaml`
- `assets/games/marvel_rivals/masters/round_states/`
- `assets/games/marvel_rivals/templates/round_states/`
- bounded runtime/fused validation against sourced prompt assets

Out of scope:

- payload/cart progress bars
- capture-point semantics
- kill feed
- OCR
- global runtime/fusion policy changes

## Implementation Steps

### 1. Add round-state ontology rows

Extend the published `marvel_rivals` pack with three round-state event rows:

- `overtime`
- `victory`
- `defeat`

Preferred location:

- `assets/games/marvel_rivals/medals.yaml` is not the right semantic home long term, but if the current pack lacks a separate objective/round-state structured file, keep the first slice aligned with the existing event-row surfaces while preserving clear category and naming.

Implementation requirement:

- each row must be explicitly classified as round-state / objective context
- do not overload medal semantics silently

### 2. Add a dedicated round-state ROI

Update `assets/games/marvel_rivals/hud.yaml` with a new ROI for the round-state banner surface.

Requirements:

- top-center or center-top banner area
- sized to cover the stable placement of:
  - `OVERTIME`
  - `VICTORY`
  - `DEFEAT`
- separate from `medal_area`

### 3. Add detection manifest rows

Extend `assets/games/marvel_rivals/manifests/detection_manifest.yaml` with:

- `marvel_rivals.overtime.round_state_banner`
- `marvel_rivals.victory.round_state_banner`
- `marvel_rivals.defeat.round_state_banner`

Each row should include:

- `roi_ref` set to the new round-state ROI
- `asset_family` such as `round_state_banner`
- explicit `event_row_id`
- published asset binding fields
- runtime rule reference to the new round-state signal family
- fusion rule reference to the new atomic rule

### 4. Add published template rows and assets

Create real sourced assets:

- `assets/games/marvel_rivals/masters/round_states/overtime.png`
- `assets/games/marvel_rivals/masters/round_states/victory.png`
- `assets/games/marvel_rivals/masters/round_states/defeat.png`
- matching template copies under:
  - `assets/games/marvel_rivals/templates/round_states/`

Then extend `assets/games/marvel_rivals/manifests/cv_templates.yaml` with the corresponding template rows.

Requirements:

- source provenance captured
- patch tag present
- no synthetic fabricated banners

### 5. Add runtime mapping

Extend `assets/games/marvel_rivals/manifests/runtime_cv_rules.yaml` with:

- `round_state_banner -> round_state_visibility -> round_state_seen`

Requirements:

- target field: `event_row_id`
- contract shape should mirror the existing medal mapping where possible

### 6. Add fusion rule

Extend `assets/games/marvel_rivals/manifests/fusion_rules.yaml` with:

- `round_state_visibility_atomic`

Requirements:

- event type: `round_state_seen`
- signal type: `round_state_visibility`
- group by `event_row_id`
- confidence method: `max`

### 7. Validate the pack and run bounded runtime/fused probes

After the pack surfaces are updated:

- validate that the published pack still loads cleanly
- run a bounded runtime probe on a known source clip or sourced prompt media for:
  - `overtime`
  - or `victory`
  - or `defeat`
- confirm:
  - at least one runtime signal
  - at least one runtime event
- then run fused analysis and verify:
  - the new event family is preserved
  - it surfaces as non-identity evidence

## Verification Plan

### A. Static verification

Check that the modified manifests and asset references are loadable and consistent.

Suggested commands:

```bash
python3 -m py_compile run.py
python3 - <<'PY'
from pathlib import Path
from pipeline.simple_yaml import load_yaml_file
for path in [
    Path('assets/games/marvel_rivals/hud.yaml'),
    Path('assets/games/marvel_rivals/manifests/detection_manifest.yaml'),
    Path('assets/games/marvel_rivals/manifests/cv_templates.yaml'),
    Path('assets/games/marvel_rivals/manifests/runtime_cv_rules.yaml'),
    Path('assets/games/marvel_rivals/manifests/fusion_rules.yaml'),
]:
    load_yaml_file(path)
print('ok')
PY
```

### B. Runtime probe verification

Use a bounded known-positive media source for one round-state banner.

Verify:

1. matcher sees the new template
2. runtime emits:
   - `round_state_visibility`
   - `round_state_seen`
3. no invalid numeric state is introduced

### C. Fused verification

Run fused analysis using the bounded runtime sidecar and a minimal proxy payload.

Verify:

1. fused event survives as a round-state event
2. it is preserved as non-identity evidence

## Risks And Mitigations

### Risk: wrong ROI placement

Mitigation:

- keep ROI dedicated and broad enough for stable banner placement
- validate against real sourced frames before tuning tighter

### Risk: semantic confusion with medals

Mitigation:

- use distinct asset family and event row ids
- keep runtime/fusion rule names round-state-specific

### Risk: mode-dependent banner drift

Mitigation:

- start with prompts that are broadly stable
- keep the first slice narrow and auditable

## Done Criteria

This slice is done when:

- round-state ROI exists
- three round-state detection/template rows are published
- runtime rule and fusion rule exist
- at least one known-positive round-state banner is validated through runtime and fusion
- no unrelated runtime or review workflow contracts are changed
