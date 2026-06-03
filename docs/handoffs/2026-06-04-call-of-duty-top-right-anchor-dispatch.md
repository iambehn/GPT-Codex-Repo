title:
  Call of Duty top-right anchor dispatch

purpose:
  Provide a compact researcher-facing dispatch for the current active blocker without requiring reconstruction from the full handoff chain.

active_blocker:
  `call_of_duty` top-right native event-card family still lacks one discriminative anchor candidate that can support the next runtime slice.

what_is_already_known:
  - the family itself is still plausible native HUD
  - the first shell-anchor pilot failed and was rolled back
  - the shell matched positives and negatives in the same score band:
    - positives: `0.95194` to `0.95271`
    - negatives: `0.95325` to `0.95557`
  - bounded local OCR evidence is weak:
    - `_PL_5qWwKtY` gives only partial text recovery
    - `gcAGS3R2t2o` gives no useful OCR recovery
    - negatives produce OCR noise
  - generic high-kill `MWIII Vista` probes from `vistastructions` are same-title but wrong-surface candidates:
    - they mostly show loadout labels, player-name overlays, or plain environment in the top-right ROI

exact_positive_windows:
  - `_PL_5qWwKtY @ 12s-14s`
  - `gcAGS3R2t2o @ 27s-29s`

exact_negative_pressure_set:
  - `SVbTc2AZzYw.60s-70s`
  - `v-SzAArdAfY.60s-70s`
  - `Qop1sH70nHI.60s-70s`

what_the_next_packet_must_do:
  - recommend one anchor strategy only
  - choose among:
    - icon or emblem block
    - compact left-side badge cluster
    - text fragment only if stronger than the current weak local OCR evidence
  - include exact clips, timestamps, crop references, and exclusions
  - explain why the recommended anchor is more discriminative than:
    - the failed shell anchor
    - the weak OCR path
    - the wrong-surface `vistastructions` family

what_should_be_explicitly_avoided:
  - shell-derived anchors
  - generic killfeed semantics
  - broad OCR rollout without stronger evidence
  - lower-center overlay-adjacent text
  - generic high-kill Vista sourcing that does not visibly show the event-card family

best_supporting_artifacts:
  - packet request:
    - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-packet-request.md`
  - appendix:
    - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-appendix.md`
  - local diagnostics:
    - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-local-diagnostics.md`
  - same-title exclusion scout:
    - `docs/handoffs/2026-06-04-call-of-duty-vista-anchor-candidate-scout.md`

success_condition:
  The next packet is good enough when Codex can either:
  - implement one narrower anchor without guessing
  - or retire the top-right family and switch cleanly to the fallback sourcing branch
