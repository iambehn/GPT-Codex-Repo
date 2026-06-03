title:
  Call of Duty top-right event-card shell pilot results

status:
  completed

decision_target:
  Determine whether the first template-compatible top-right event-card shell anchor is clean enough to keep as a published runtime pilot.

repo_truth:
  - the direct implementation handoff selected a conservative shell-anchor strategy over the existing `kill_feed` ROI
  - the pilot was intentionally constrained to template matching with `hud_visibility -> high_action_sequence`
  - the published-pack mutation was treated as provisional pending positive-window confirmation and negative-window pressure

evidence_bundle:
  positives:
    - clip: `outputs/measurement/call_of_duty_top_right_event_card_pilot/_PL_5qWwKtY_12s_15s.mp4`
      sidecar: `outputs/measurement/call_of_duty_top_right_event_card_pilot/_PL_5qWwKtY_12s_15s.runtime.json`
      result:
        peak_score: 0.95271
        confirmed_family: top_right_event_card
        event_type: high_action_sequence
    - clip: `outputs/measurement/call_of_duty_top_right_event_card_pilot/gcAGS3R2t2o_27s_30s.mp4`
      sidecar: `outputs/measurement/call_of_duty_top_right_event_card_pilot/gcAGS3R2t2o_27s_30s.runtime.json`
      result:
        peak_score: 0.95194
        confirmed_family: top_right_event_card
        event_type: high_action_sequence
  negatives:
    - clip: `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
      sidecar: `outputs/measurement/call_of_duty_top_right_event_card_pilot/SVbTc2AZzYw_60s_70s.runtime.json`
      result:
        peak_score: 0.95474
        confirmed_family: top_right_event_card
        event_type: high_action_sequence
        support_window: 0.0s-9.5s
    - clip: `outputs/public_gameplay_mining/call_of_duty_measurement_sources/v-SzAArdAfY.60s-70s.mp4`
      sidecar: `outputs/measurement/call_of_duty_top_right_event_card_pilot/v-SzAArdAfY_60s_70s.runtime.json`
      result:
        peak_score: 0.95557
        confirmed_family: top_right_event_card
        event_type: high_action_sequence
        support_window: 0.0s-9.5s
    - clip: `outputs/public_gameplay_mining/call_of_duty_measurement_sources/Qop1sH70nHI.60s-70s.mp4`
      sidecar: `outputs/measurement/call_of_duty_top_right_event_card_pilot/Qop1sH70nHI_60s_70s.runtime.json`
      result:
        peak_score: 0.95325
        confirmed_family: top_right_event_card
        event_type: high_action_sequence
        support_window: 0.0s-9.5s

structured_findings:
  - positive-window confirmation alone was misleading
  - the shell anchor reproduces on the intended windows, but it also reproduces across the full negative set at effectively the same confidence band
  - the score separation is not usable:
    - positives: `0.95194` to `0.95271`
    - negatives: `0.95325` to `0.95557`
  - this is not a threshold-tuning problem
  - this is not a near-miss scale problem
  - the selected shell is behaving like generic top-right HUD chrome rather than a discriminative event-card signal

recommendation:
  - retire the shell-anchor pilot from published-pack consideration
  - roll back the provisional published-pack mutation
  - do not continue threshold or scale tuning on this anchor
  - request one anchor-specific packet that targets a more discriminative sub-region inside the top-right card family:
    - icon block
    - emblem region
    - stable text fragment with explicit OCR posture
  - if no discriminative anchor is visible, retire the family and pivot to the fallback sourcing branch

acceptance_target:
  - the rollback restores the published `call_of_duty` pack to its pre-pilot state
  - the next packet names one exact anchor strategy that can plausibly separate positives from the current negative set

open_uncertainties:
  - whether the top-right family contains a reusable icon-block anchor that is narrower than the shell
  - whether a mixed OCR/template posture is required earlier than preferred for this family
  - whether same-provenance clips expose stronger variants of the top-right card than the current local windows
