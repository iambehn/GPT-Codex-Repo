title:
  Call of Duty Vista anchor candidate scout

status:
  completed

decision_target:
  Determine whether two additional high-kill `MWIII Vista` public probes expose a cleaner top-right event-card anchor than the current local positives.

repo_truth:
  - the active blocker is still discriminative anchor evidence for the top-right native event-card family
  - current local positives are weak for shell templates and weak-to-mixed for OCR viability

candidate_clips:
  - url: https://www.youtube.com/watch?v=-unDN10cqgo
    local_probe: `outputs/measurement/call_of_duty_top_right_anchor_candidates/-unDN10cqgo.0s-120s.mkv`
    title: `52 Kills, my MOST KILLS in 1 match [Full Gameplay] | COD Modern Warfare 3 Gameplay in 2021`
    channel: `vistastructions`
    inspection_artifact: `outputs/inspection/call_of_duty_top_right_anchor_candidates/-unDN10cqgo_top_right_contact_sheet.png`
  - url: https://www.youtube.com/watch?v=vY8j3bLkkMI
    local_probe: `outputs/measurement/call_of_duty_top_right_anchor_candidates/vY8j3bLkkMI.0s-120s.mkv`
    title: `My new record: 67 kills in a single 6v6 match! [Full Gameplay] | COD Modern Warfare 3 in 2021`
    channel: `vistastructions`
    inspection_artifact: `outputs/inspection/call_of_duty_top_right_anchor_candidates/vY8j3bLkkMI_top_right_contact_sheet.png`

structured_findings:
  - both clips are same-title-family enough to be worth checking
  - neither clip strengthens the current top-right event-card branch
  - `-unDN10cqgo` top-right ROI samples show:
    - weapon or loadout text such as `ACR 1`
    - player-name overlays
    - plain environment or blood-splatter frames
  - `vY8j3bLkkMI` top-right ROI samples show:
    - loadout text such as `GRENADIER` and `SUP 1`
    - player-name overlays
    - environment-only frames
  - these are same-title but wrong-surface candidates for the current blocker
  - they do not expose a clearer emblem, badge cluster, or stable event-card text fragment than `_PL_5qWwKtY` and `gcAGS3R2t2o`

recommendation:
  - exclude this `vistastructions` high-kill clip family from the next top-right anchor packet as primary evidence
  - keep them only as negative or ambiguous examples of same-title-family but wrong-surface sourcing
  - bias the next packet toward clips that visibly show the event-card family itself rather than generic high-kill Vista gameplay

open_uncertainties:
  - whether a more targeted same-title search using event-card keywords can surface better clips than generic high-kill Vista gameplay
  - whether the correct next same-title source family is objective-focused rather than kill-count-focused
