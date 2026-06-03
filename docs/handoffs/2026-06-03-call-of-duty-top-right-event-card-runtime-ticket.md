title:
  Add first call_of_duty top-right event-card runtime pilot

goal:
  Add one narrow runtime pilot around the existing top-right `kill_feed` ROI to test whether native `call_of_duty` objective or status cards are a more repeatable runtime surface than the capped `reward_banner` family.

why_now:
  The `reward_banner` pilot is now capped as a narrow validated surface. The alternative-native-surface packet plus the local top-right scout show repeated native cards across `_PL_5qWwKtY` and one same-title `MWIII Vista` probe, but the family is better described as objective or status notifications than as generic elimination killfeed. The repo now needs one small implementation-facing slice that preserves that distinction and avoids speculative pack expansion.

current_repo_truth:
  - the current published `call_of_duty` pack has a top-right ROI named `kill_feed` in `assets/games/call_of_duty/hud.yaml`
  - the pack does not yet publish any `killfeed_events` or top-right event-card family
  - the `reward_banner` family remains published and valid as a narrow `UAV` pilot only
  - local scout evidence now exists at:
    - `docs/handoffs/2026-06-03-call-of-duty-top-right-event-card-scout.md`
    - `outputs/inspection/call_of_duty_killfeed_scout/`
  - repeated top-right native cards are visible at:
    - `_PL_5qWwKtY @ 12s-14s`
    - `gcAGS3R2t2o.0s-120s @ 27s-29s`
  - extraction posture is currently best treated as `mixed_ocr_template`
  - the runtime path is still template-ROI based, not OCR-native

scope:
  - one first pilot family only
  - use the existing top-right ROI as the starting region
  - preserve the family semantics as `objective_event_notifications` or neutral top-right event-status cards
  - keep the first semantic mapping narrow and auditable
  - produce a validation slice on the current local evidence set before any broader family expansion

non_goals:
  - generic elimination killfeed parsing
  - broad OCR platform expansion
  - full text extraction across the frame
  - reward_banner retuning
  - lower-center overlay-adjacent text work

candidate_approach:
  - treat the first pilot as a top-right event-card family, not as generic killfeed
  - start from one template-compatible anchor if possible:
    - card box shape
    - icon block
    - stable card region
  - keep the first event mapping conservative by reusing the existing `hud_visibility -> high_action_sequence` semantics unless a stronger native event type is justified during implementation
  - defer richer OCR or text-label semantics until the pilot proves the family is repeatable

dependencies:
  - `docs/handoffs/2026-06-03-call-of-duty-alternative-native-surface-packet.md`
  - `docs/handoffs/2026-06-03-call-of-duty-top-right-event-card-scout.md`
  - `_PL_5qWwKtY` top-right evidence at `12s-14s`
  - `gcAGS3R2t2o.0s-120s` top-right evidence at `27s-29s`
  - current `call_of_duty` pack surfaces:
    - `assets/games/call_of_duty/hud.yaml`
    - `assets/games/call_of_duty/manifests/cv_templates.yaml`
    - `assets/games/call_of_duty/manifests/detection_manifest.yaml`
    - `assets/games/call_of_duty/manifests/runtime_cv_rules.yaml`

acceptance_criteria:
  - a direct Codex handoff can name:
    - the first top-right event-card family label
    - the ROI surface
    - the first evidence clips and timestamps
    - the initial extraction posture
    - the first validation loop
  - the handoff does not mislabel the family as generic elimination killfeed
  - the handoff does not require broad OCR infrastructure as part of the first slice

verification_shape:
  - targeted repo health check
  - local artifact inspection against:
    - `_PL_5qWwKtY`
    - `gcAGS3R2t2o.0s-120s`
    - the existing short-window negatives for pressure testing
  - if implementation occurs later:
    - `python3 run.py --validate-game-pack call_of_duty`
    - focused `--analyze-roi-runtime` probes on the clip-backed evidence windows

open_questions:
  - whether one stable template anchor exists without OCR for the first pilot
  - whether the neutral implementation label should be `objective_event_notifications` or broader `top_right_event_status_cards`
  - whether the first semantic mapping should stay `high_action_sequence` or be widened only after repeatability is proven
