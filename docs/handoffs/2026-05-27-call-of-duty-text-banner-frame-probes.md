# Call Of Duty Text Banner Frame Probes

Date: 2026-05-27
Status: active

## Objective

Turn the current local `call_of_duty` sample evidence into a starter bundle for the next text or reward-banner research packet.

## Source Clip

Primary clip:

- `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`

Clip resolution:

- `1920x1080`

## Extracted Frame Artifacts

Full-frame probes:

- `outputs/measurement/call_of_duty_text_banner_probes/uav_11.0_full.png`
- `outputs/measurement/call_of_duty_text_banner_probes/doubleki_20.5_full.png`

Tight crops:

- `outputs/measurement/call_of_duty_text_banner_probes/uav_banner_11.0_crop.png`
- `outputs/measurement/call_of_duty_text_banner_probes/killcount_4th_11.0_crop.png`
- `outputs/measurement/call_of_duty_text_banner_probes/doublekill_20.5_crop.png`
- `outputs/measurement/call_of_duty_text_banner_probes/killcount_7th_20.5_crop.png`

## Frame-Level Findings

### 11.0s

Visible signals:

- `UAV`
- `PRESS 3 TO USE`
- `4TH KILL`

Interpretation:

- `UAV` is a strong `reward_banner` candidate
- `4TH KILL` is a strong `kill_count_text` candidate
- both are visible in the same live gameplay frame

Likely signal status:

- `UAV`: probably native game UI
- `4TH KILL`: likely native game UI, but still worth checking against title-family references

### 20.5s

Visible signals:

- `DOUBLE KI`
- `7TH KO`

Interpretation:

- `DOUBLE KI` is a strong `multikill_text` candidate, but the visible crop is truncated and should be treated as a partial-read frame
- `7TH KO` is a strong `kill_count_text` candidate, but the crop does not prove whether the missing final letters are due to timing, motion blur, or title-specific styling

Likely signal status:

- the yellow `DOUBLE KI` text appears consistent with live reward text
- the red `7TH KO` counter appears consistent with a live kill-count overlay

## Current Ambiguities

- `_PL_5qWwKtY` includes a visible `medal.tv` watermark, so the researcher should verify that these signal surfaces are native game UI rather than post-production overlays
- the current extracted frame at `20.5s` captures partial text, not a clean full-string read
- the local frame probes are enough to justify a text/banner packet, but not enough to settle title-family truth

## Recommended Research Use

Use these probes as starter evidence for:

- `reward_banner`
  - `UAV`
- `kill_count_text`
  - `4TH KILL`
  - `7TH KILL` or close variant pending validation
- `multikill_text`
  - `DOUBLE KILL` or close variant pending validation

Do not treat these crops as authoritative final labels. Treat them as local evidence that the next packet should focus on text or banner surfaces, not more blind medal-icon promotion.
