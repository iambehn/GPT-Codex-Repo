title:
  Add first call_of_duty top-right event-card runtime pilot

objective:
  Implement one narrow published-pack pilot that detects the repeated native top-right event-card shell in `call_of_duty` clips using the existing `kill_feed` ROI, maps it conservatively to `hud_visibility -> high_action_sequence`, and validates it on the clip-backed evidence windows before any broader family expansion.

repo_context:
  - `assets/games/call_of_duty/hud.yaml` already defines the top-right ROI as `kill_feed`
  - `assets/games/call_of_duty/manifests/cv_templates.yaml` and `assets/games/call_of_duty/manifests/detection_manifest.yaml` already publish template-driven runtime families
  - `assets/games/call_of_duty/manifests/runtime_cv_rules.yaml` currently maps `reward_banner` through `hud_visibility -> high_action_sequence`
  - `docs/handoffs/2026-06-03-call-of-duty-top-right-event-card-scout.md` contains the corrected family semantics and clip-backed evidence
  - `docs/handoffs/2026-06-03-call-of-duty-top-right-event-card-runtime-ticket.md` contains the implementation boundary and non-goals

inputs_and_outputs:
  - evidence clips:
    - `_PL_5qWwKtY @ 12s-14s`
    - `gcAGS3R2t2o.0s-120s @ 27s-29s`
  - evidence stills:
    - `outputs/inspection/call_of_duty_killfeed_scout/stills/_PL_5qWwKtY.12s.png`
    - `outputs/inspection/call_of_duty_killfeed_scout/stills/_PL_5qWwKtY.13s.png`
    - `outputs/inspection/call_of_duty_killfeed_scout/stills/_PL_5qWwKtY.14s.png`
    - `outputs/inspection/call_of_duty_killfeed_scout/stills/gcAGS3R2t2o_0s-120s.27s.png`
    - `outputs/inspection/call_of_duty_killfeed_scout/stills/gcAGS3R2t2o_0s-120s.29s.png`
  - candidate output surfaces:
    - one new template asset family under `assets/games/call_of_duty/templates/`
    - one new published detection row
    - one new runtime mapping row
    - one focused validation note under `docs/handoffs/`

required_changes:
  - define one neutral first family label:
    - prefer `top_right_event_status_card`
    - avoid `killfeed_events` as the implementation label
  - create one template-compatible anchor around the repeated dark card shell in the top-right ROI
    - prefer shell or icon-block shape over text payload
    - do not require OCR for the first slice
  - bind the family through the existing `kill_feed` ROI
  - map the first pilot conservatively to:
    - `signal_type: hud_visibility`
    - `event_type: high_action_sequence`
  - validate on the two positive evidence windows first
  - pressure-test against the existing short-window negatives before broadening scope

constraints_and_invariants:
  - do not mislabel the family as generic elimination killfeed
  - do not broaden into OCR infrastructure in this slice
  - do not retune or expand `reward_banner`
  - do not promote lower-center overlay-adjacent text
  - keep the first pilot template-compatible and auditable
  - if the shell anchor is too weak, stop and record the failure mode rather than widening scope silently

verification:
  - `python3 run.py --validate-game-pack call_of_duty`
  - focused runtime probes on:
    - `_PL_5qWwKtY` around `12s-14s`
    - `gcAGS3R2t2o.0s-120s` around `27s-29s`
  - negative pressure on:
    - `SVbTc2AZzYw.60s-70s`
    - `v-SzAArdAfY.60s-70s`
    - `Qop1sH70nHI.60s-70s`
  - `python3 run.py --run-repo-quality-health`

out_of_scope:
  - OCR-backed objective text parsing
  - generic killfeed parser semantics
  - new ROI families outside the existing top-right region
  - family expansion beyond one first anchor
  - fusion-rule changes beyond the conservative `high_action_sequence` pilot mapping
