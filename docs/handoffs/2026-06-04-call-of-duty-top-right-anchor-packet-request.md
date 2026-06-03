title:
  Call of Duty top-right event-card anchor packet request

decision_target:
  Choose one discriminative anchor strategy for the `call_of_duty` top-right native event-card family after the shell-anchor pilot failed.

current_repo_truth:
  - the repeated top-right family remains a plausible native HUD surface
  - the first shell-anchor pilot was rolled back because it matched the positive windows and all three negative pressure clips with nearly identical confidence
  - the blocker is no longer family choice
  - the blocker is anchor specificity

required_output:
  - follow `RESEARCH_PACKET_TEMPLATE.md`
  - support one exact next repo action only
  - recommend one anchor strategy only
  - include explicit exclusions and false positives

required_evidence:
  - exact clip paths or URLs
  - exact timestamps
  - crop or still references
  - why the anchor matters
  - native UI vs overlay classification
  - extraction posture:
    - `template_first`
    - `ocr_first`
    - `mixed_ocr_template`
    - `unknown`

positive_windows:
  - `_PL_5qWwKtY @ 12s-14s`
  - `gcAGS3R2t2o @ 27s-29s`

negative_pressure_set:
  - `SVbTc2AZzYw.60s-70s`
  - `v-SzAArdAfY.60s-70s`
  - `Qop1sH70nHI.60s-70s`

required_findings:
  - separate:
    - recommended anchor
    - ambiguous anchors
    - false-positive-prone anchors
    - unusable anchors
  - explain why the shell anchor failed
  - identify whether a narrower icon block, emblem block, or text fragment is actually discriminative

non_goals:
  - generic killfeed semantics
  - broad OCR rollout
  - reward-banner tuning
  - overlay-adjacent lower-center text

acceptance_target:
  - the packet is good enough when Codex can either:
    - implement one narrower anchor without guessing
    - or explicitly retire the top-right family and switch to the fallback sourcing branch
