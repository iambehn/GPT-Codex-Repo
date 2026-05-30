# Call of Duty Reward Banner Runtime Implementation Ticket

Status: draft_filled
Artifact type: implementation_ticket
Ticket id: `2026-05-31-call-of-duty-reward-banner-runtime-implementation-ticket`
Owner: Codex
Game: `call_of_duty`
Priority: `P0`
Input packet: [2026-05-30-call-of-duty-text-banner-bootstrap-packet.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-30-call-of-duty-text-banner-bootstrap-packet.md)

## Title

Add first `call_of_duty` `reward_banner` runtime family

## Goal

Add one narrow published-pack runtime family that tests whether the visible `UAV` banner is a more productive first detection surface than medal icons on the current measured `call_of_duty` clips.

## Why Now

- the medal onboarding path is structurally complete
- post-promotion measurement still produced zero medal-driven outcomes on the current four-clip sample set
- local frame probes show a more stable visible text/banner surface than native medal badges in `_PL_5qWwKtY.mp4`
- the current blocker is signal-surface mismatch, not onboarding mechanics

## Current Repo Truth

Governing surfaces:

- [assets/games/call_of_duty/hud.yaml](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/call_of_duty/hud.yaml)
- [assets/games/call_of_duty/manifests/assets_manifest.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/call_of_duty/manifests/assets_manifest.json)
- [assets/games/call_of_duty/manifests/cv_templates.yaml](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/call_of_duty/manifests/cv_templates.yaml)
- [assets/games/call_of_duty/manifests/detection_manifest.yaml](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/call_of_duty/manifests/detection_manifest.yaml)
- [assets/games/call_of_duty/manifests/runtime_cv_rules.yaml](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/call_of_duty/manifests/runtime_cv_rules.yaml)
- [starter_assets/runtime_signal_event_ontology.yaml](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/starter_assets/runtime_signal_event_ontology.yaml)
- [pipeline/roi_matcher.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/roi_matcher.py)
- [pipeline/event_mapper.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/event_mapper.py)
- [pipeline/runtime_analysis.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/runtime_analysis.py)

Observed implementation facts:

- the live runtime path is the published-pack ROI matcher plus event mapper
- published `call_of_duty` families are currently:
  - `hero_portrait`
  - `equipment_icon`
  - `medal_icon`
- the runtime ontology already allows a generic no-target lane:
  - `signal_type: hud_visibility`
  - `event_type: high_action_sequence`
  - `target_field: null`
- event mapping can handle `target_field: null`; it does not require every family to resolve a semantic target value
- local crop seeds already exist:
  - `outputs/measurement/call_of_duty_text_banner_probes/uav_banner_11.0_crop.png`
  - `outputs/measurement/call_of_duty_text_banner_probes/uav_11.0_full.png`

## Scope

Implement only:

- one new ROI surface for the `UAV` banner
- one new published asset family:
  - `reward_banner`
- one first template asset for:
  - `UAV`
- one runtime mapping that keeps the pilot inside the existing runtime matcher path

Do not implement:

- `kill_count_text`
- `multikill_text`
- OCR-wide runtime changes
- killfeed parsing
- new ontology terms unless the generic `hud_visibility -> high_action_sequence` pilot clearly proves insufficient

## Candidate Approach

Use the existing published-pack template path, modeled after banner-like families already present in `marvel_rivals`.

Recommended semantic posture for the first pilot:

- asset family: `reward_banner`
- runtime rule:
  - `signal_type: hud_visibility`
  - `event_type: high_action_sequence`
  - `target_field: null`
  - `target_id_source: asset_id_suffix` or unused
- fusion participation:
  - none in the first slice, unless a later pass shows a clean benefit

Reason:

- this is the smallest extension that fits the current ontology without inventing a new signal family
- it lets the runtime sidecar surface a new non-equipment detection lane
- it preserves the existing template-based matcher posture

## Likely File Changes

Published pack:

- `assets/games/call_of_duty/hud.yaml`
- `assets/games/call_of_duty/manifests/assets_manifest.json`
- `assets/games/call_of_duty/manifests/cv_templates.yaml`
- `assets/games/call_of_duty/manifests/detection_manifest.yaml`
- `assets/games/call_of_duty/manifests/runtime_cv_rules.yaml`

Likely new asset files:

- `assets/games/call_of_duty/masters/reward_banners/uav.png`
- `assets/games/call_of_duty/templates/reward_banners/uav.png`

Validation surfaces:

- `pipeline/roi_matcher.py`
- `pipeline/event_mapper.py`
- `tests/` coverage for published-pack validation or runtime analysis if behavior is extended

## Dependencies

Required inputs are already local:

- bootstrap packet
- local `UAV` banner crops
- current measured `call_of_duty` sample set

No external researcher packet is required for this first pilot if the repo accepts the generic `hud_visibility -> high_action_sequence` semantic lane.

## Acceptance Criteria

This ticket is ready to promote into a direct Codex handoff when:

- the repo change can name one exact ROI and one exact template seed
- the semantic mapping is fixed as:
  - `reward_banner -> hud_visibility -> high_action_sequence`
- the published-pack file edits are explicit
- the validation path is explicit

Implementation success should mean:

- `call_of_duty` published pack validates after the new family is added
- `python run.py --analyze-roi-runtime ... call_of_duty` emits at least one non-equipment runtime signal or event on the `_PL_5qWwKtY` sample, or fails with a more explicit ROI/template mismatch
- the failure mode is narrower than the current generic “no medal-driven outcomes” state

## Verification Shape

Minimum verification:

- `python run.py --validate-game-pack call_of_duty`
- `python run.py --run-repo-quality-health`
- one focused runtime run on:
  - `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`

Prefer also:

- direct inspection of the runtime sidecar
- direct inspection of matcher debug crops if no detection fires

## Open Questions

- Should the first `reward_banner` pilot use `target_id_source: asset_id_suffix` with no target field, or normalize the row more explicitly even though the target is currently semantic-free?
  - current recommendation: keep it semantic-free and minimal in the first pilot

- Should the first pilot also add a scoring weight for `high_action_sequence` in runtime export?
  - current recommendation: no, not in the same slice; first prove the runtime family can detect anything useful

- If the pilot detects the `UAV` banner, should the second slice expand to `kill_count_text` or to a stronger title-specific reward banner family?
  - defer until after the first pilot rerun
