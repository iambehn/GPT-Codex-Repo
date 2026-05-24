# Call Of Duty Editorial Scout Findings

Date: 2026-05-25
Status: active

## Objective

Test whether the current `call_of_duty` editorial weakness is mainly a bad-sample problem or a published-pack coverage problem.

## Scout Sample

- media: `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
- provenance: public short-form gameplay clip downloaded locally for operator testing
- isolated scout root: `outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/`
- isolated registry: `outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/editorial_scout.registry.sqlite`

## Commands Run

```bash
source .venv/bin/activate
python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4 call_of_duty --sample-fps 1 --output-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/runtime/_PL_5qWwKtY.runtime_analysis.json
python run.py --fuse-clip-signals outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4 call_of_duty --runtime-sidecar outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/runtime/_PL_5qWwKtY.runtime_analysis.json --output-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/fused/_PL_5qWwKtY.editorial_scout.fused_analysis.json
python run.py --export-highlight-selection --fused-sidecar outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/fused/_PL_5qWwKtY.editorial_scout.fused_analysis.json --output-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/selection/_PL_5qWwKtY.highlight_selection.json
python run.py --refresh-clip-registry outputs/editorial_scout/call_of_duty/_PL_5qWwKtY --registry-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/editorial_scout.registry.sqlite
python run.py --derive-hook-candidates outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/fused/_PL_5qWwKtY.editorial_scout.fused_analysis.json --registry-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/editorial_scout.registry.sqlite --output-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/hooks/_PL_5qWwKtY.hook_candidates.json
python run.py --query-clip-registry --mode candidate-lifecycles --registry-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/editorial_scout.registry.sqlite --game call_of_duty
python run.py --query-clip-registry --mode fused-events --registry-path outputs/editorial_scout/call_of_duty/_PL_5qWwKtY/editorial_scout.registry.sqlite --game call_of_duty
```

## Runtime Result

- `runtime_analysis_v1` completed successfully
- sampled frames: `45` at `1.0 FPS`
- confirmed detections: `5`
- event count: `5`
- signal families observed:
  - `equipment_visibility`
- ROI observed:
  - `ability_hud`
- notable absence:
  - no `medal_visibility`
  - no `character_identity`
  - no medal or streak event rows

Detected equipment IDs:

- `armor_satchel`
- `redeploy_extraction_token`
- `armor_plates`

## Fusion Result

- `fused_analysis_v1` completed successfully
- proxy signals existed, but fused candidate outcomes still reduced to runtime equipment evidence
- fused event count: `5`
- every fused event came from rule:
  - `equipment_visibility_atomic`
- every fused event type was:
  - `ability_seen`
- no `ability_plus_medal_combo` events were emitted
- no multi-signal medal corroboration occurred

## Registry Result

After selection export and registry refresh:

- candidate lifecycle row count: `5`
- every candidate lifecycle state: `pending_review`
- latest review status: `null`

Hook derivation result:

- `hook_candidate_count: 0`
- `eligible_lifecycle_states: ["approved", "selected_for_export"]`
- `ineligible_lifecycle_count: 5`

This means the scout path did not even reach hook comparison yet because the fused candidates never entered an approved or selected lifecycle state.

## Published-Pack Coverage Finding

The stronger bottleneck is the published `call_of_duty` pack itself.

Published pack facts:

- `assets/games/call_of_duty/medals.yaml` is `medals: []`
- published asset families in `assets/games/call_of_duty/manifests/assets_manifest.json`:
  - `hero_portrait: 99`
  - `equipment_icon: 13`
- `assets/games/call_of_duty/manifests/runtime_cv_rules.yaml` maps only:
  - `equipment_icon -> equipment_visibility / ability_seen`
  - `hero_portrait -> character_identity / pov_character_identified`

Draft-only evidence already exists:

- `assets/games/call_of_duty/drafts/wiki/20260430T015758Z/assets_manifest.json`
- draft asset family counts:
  - `medal_icon: 250`

## Existing Workflow Entry Points Checked

The repo already contains the right conceptual path:

- wiki enrichment maps `events -> medal_icon`
- onboarding adapters map `call_of_duty` `events -> medal_icon`
- onboarding publish is the existing published-pack promotion surface

But the current draft is stranded one layer earlier than that publish surface.

Commands checked:

```bash
python run.py --build-onboarding-draft assets/games/call_of_duty/drafts/wiki/20260430T015758Z
python run.py --derive-game-detection-manifest assets/games/call_of_duty/drafts/wiki/20260430T015758Z
```

Observed failures:

- `--build-onboarding-draft`
  - status: `invalid_onboarding_draft_build`
  - reason: expected `manifests/assets_manifest.json`, but wiki draft only has top-level `assets_manifest.json`
- `--derive-game-detection-manifest`
  - status: `invalid_derived_detection_manifest`
  - reason: expected `manifests/game_detection_schema.yaml`, but wiki draft does not carry onboarding-draft manifests

So the medal assets are not missing. They are present but stranded in `drafts/wiki` shape rather than the onboarding-draft shape that the existing publish workflow expects.

## Interpretation

The current `call_of_duty` editorial ceiling is not just a weak operator sample.

It is also a published-pack coverage problem:

- the active pack can emit equipment and identity signals
- the active pack cannot emit medal signals
- the main fusion rule that could create richer editorial candidates requires `medal_visibility`
- therefore medal-oriented scout clips will still collapse into equipment-only or identity-only evidence unless medal coverage is promoted into the published pack

## Recommended Next Target

Do not spend the next local-only cycle on more `call_of_duty` sample hunting alone.

Higher-leverage next target:

- close the published-pack medal coverage gap using the existing draft wiki medal assets
- likely by bridging `drafts/wiki/...` into the onboarding-draft or publishable shape the existing onboarding flow expects

Expected outcome if that succeeds:

- runtime can emit `medal_visibility`
- fusion can produce `ability_plus_medal_combo`
- hook classification has a real chance to move from `mechanics_only` toward `mixed` or `editorially_viable`
