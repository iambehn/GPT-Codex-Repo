# Codex Run Log

Status: active-draft
Version: 0.1
Last updated: 2026-05-30

This file is the append-only progress log for routine Codex execution inside the current operator pack.

Use it to record:

- current target
- current step
- next step
- blockers
- verification status
- commit boundaries worth preserving

Default rule:

- routine execution progress should go here
- chat updates should be reserved for blockers, milestones, or material contract decisions

Non-goals:

- this is not a second backlog
- this is not a second execution target
- this is not a governance ledger

## Entry Template

```md
## 2026-05-30T14:10Z

target:
- interaction routing v1

status:
- in_progress

current_step:
- align researcher contract and GPT instruction surfaces

next_step:
- run repo quality health
- stage only routing-slice docs

blockers:
- none

verification:
- pending

notes:
- no runtime behavior changes
```

## Milestone Template

```md
## 2026-05-30T14:32Z

target:
- interaction routing v1

status:
- completed

result:
- canonical routing doc added
- researcher contract updated
- GPT prompt updated

verification:
- python run.py --run-repo-quality-health
- ok: true

commit:
- 00174ef
```

## 2026-05-30T11:07Z

target:
- operator-pack run-log surface

status:
- completed

result:
- added append-only run log to the operator pack
- updated operator-pack docs to route routine progress into the log by default

verification:
- pending

notes:
- this slice is documentation-only and is intended to reduce routine chat interruptions, not replace blocker or milestone reporting

## 2026-05-30T11:18Z

target:
- operator-pack run-log consistency follow-up

status:
- completed

result:
- linked the run log from the V2 index operator-pack category
- added the run log to the operator-pack backlog deliverables list

verification:
- pending

notes:
- this keeps the new reporting surface discoverable from the canonical doc-routing layer

## 2026-05-30T11:34Z

target:
- custom GPT knowledge-pack artifact coverage

status:
- completed

result:
- added an upload-ready implementation-ticket template
- aligned knowledge-pack docs and builder checklist with the routed artifact set from interaction routing v1

verification:
- pending

notes:
- this closes the gap where `implementation_ticket` was allowed by the routing contract but had no knowledge-pack template

## 2026-05-30T11:42Z

target:
- custom GPT launch-flow artifact consistency

status:
- completed

result:
- aligned the launch runbook with the implementation-ticket route from interaction routing v1

verification:
- pending

notes:
- this keeps GPT 1 artifact selection aligned across the contract, the upload pack, and the launch workflow

## 2026-05-30T11:51Z

target:
- project-brief artifact-contract alignment

status:
- completed

result:
- updated the compact project brief so it reflects the full routed artifact set, including implementation tickets as an intermediate output

verification:
- pending

notes:
- this keeps the highest-level upload brief aligned with the lower-level researcher contract and knowledge-pack templates

## 2026-05-30T11:58Z

target:
- artifact-template naming consistency

status:
- completed

result:
- clarified which routed artifact names correspond to the upload-ready note, appendix, and Codex-handoff templates

verification:
- pending

notes:
- this reduces naming drift between file labels and the interaction-routing contract

## 2026-05-30T12:08Z

target:
- GPT 2 instruction-file durability

status:
- completed

result:
- promoted the referenced architecture/troubleshooting instruction file into version control
- aligned it with the current artifact flow so it can accept implementation tickets as intermediate inputs before final handoff generation

verification:
- pending

notes:
- this fixes a real durability gap because the builder checklist already pointed at this file path

## 2026-05-30T12:23Z

target:
- text-banner bootstrap packet from local evidence plus web search

status:
- completed

result:
- wrote a bootstrap runtime packet that recommends `reward_banner` as the first implementation family
- recorded the limit that current web search is weaker than the local clip probes for this question

verification:
- pending

notes:
- this is intended to unblock the next implementation slice without pretending that the web search produced a strong external packet

## 2026-06-04T07:18Z

target:
- runtime matcher coordinate-contract discoverability

status:
- completed

result:
- documented that `runtime_analysis_v1` matcher coordinates are reported in pack-normalized frame space
- added canonical pointers to `matcher.frame_dimensions` and `matcher.frame_coordinate_space`

verification:
- pending

notes:
- this is a doc-only follow-up to the matcher/runtime sidecar hardening in `fda13c9`

## 2026-06-04T07:43Z

target:
- unified replay viewer matcher-coordinate summary parity

status:
- completed

result:
- exposed `matcher.frame_dimensions` and `matcher.frame_coordinate_space` through the unified replay payload runtime summary
- added viewer regression coverage so replay surfaces preserve the normalized-frame contract from `runtime_analysis_v1`

verification:
- `.venv/bin/python -m unittest tests.test_unified_replay_viewer`
- pending repo health gate

notes:
- this keeps replay/debug consumers aligned with the matcher/runtime sidecar contract instead of forcing operators back to raw JSON

## 2026-06-04T08:05Z

target:
- clip registry runtime matcher metadata parity

status:
- completed

result:
- persisted `matcher.frame_dimensions` and `matcher.frame_coordinate_space` into the `runtime_analyses` mirror
- added registry regression coverage for the normalized-frame metadata fields

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- pending repo health gate

notes:
- this keeps the durable registry summary aligned with `runtime_analysis_v1` without turning registry rows into a full raw-detection mirror

## 2026-06-04T08:21Z

target:
- runtime export matcher metadata parity

status:
- completed

result:
- exposed `matcher.frame_dimensions` and `matcher.frame_coordinate_space` in exported clip rows
- added runtime export regression coverage for the normalized-frame fields

verification:
- `.venv/bin/python -m unittest tests.test_runtime_export`
- pending repo health gate

notes:
- this keeps exported runtime summaries aligned with the sidecar contract so downstream consumers do not need to reopen raw `runtime_analysis_v1` payloads for coordinate-space context

## 2026-06-04T08:38Z

target:
- runtime review bridge matcher metadata parity

status:
- completed

result:
- carried `matcher.frame_dimensions` and `matcher.frame_coordinate_space` into prepared review items and GPT bridge metadata
- persisted the same fields into applied `runtime_review` bridge metadata on the source sidecar

verification:
- `.venv/bin/python -m unittest tests.test_runtime_review_bridge`
- pending repo health gate

notes:
- this keeps review-bridge consumers aligned with the normalized-frame matcher contract without requiring separate raw-sidecar inspection

## 2026-06-04T08:55Z

target:
- runtime calibration matcher metadata parity

status:
- completed

result:
- carried `matcher.frame_dimensions` and `matcher.frame_coordinate_space` into reviewed clip diagnostics
- added runtime calibration regression coverage for the normalized-frame fields

verification:
- `.venv/bin/python -m unittest tests.test_runtime_calibration`
- pending repo health gate

notes:
- this keeps calibration diagnostics aligned with the runtime sidecar contract so reviewed clip summaries retain coordinate-space context

## 2026-06-04T09:08Z

target:
- runtime tuning matcher metadata parity

status:
- completed

result:
- carried `matcher.frame_dimensions` and `matcher.frame_coordinate_space` into reviewed comparison rows
- added runtime tuning regression coverage for the normalized-frame fields

verification:
- `.venv/bin/python -m unittest tests.test_runtime_tuning`
- pending repo health gate

notes:
- this keeps replay/tuning comparisons aligned with the runtime sidecar contract so moved-clip diagnostics preserve matcher coordinate-space context

## 2026-06-04T09:24Z

target:
- legacy replay viewer matcher metadata parity

status:
- completed

result:
- exposed `matcher.frame_dimensions` and `matcher.frame_coordinate_space` in the legacy replay viewer derived payload
- surfaced the normalized-frame context in replay viewer summary cards
- added replay viewer regression coverage for the rendered frame-space metadata

verification:
- `.venv/bin/python -m unittest tests.test_replay_viewer`
- pending repo health gate

notes:
- this keeps both replay viewers aligned with the same runtime matcher coordinate contract

## 2026-06-04T09:42Z

target:
- event mapper matcher metadata parity

status:
- completed

result:
- preserved `matcher.frame_dimensions` and `matcher.frame_coordinate_space` in `map_matcher_result()` output
- added event-mapper regression coverage for the normalized-frame fields

verification:
- `.venv/bin/python -m unittest tests.test_event_mapper`
- pending repo health gate

notes:
- this keeps direct matcher-report consumers aligned with the same coordinate contract as `runtime_analysis_v1`

## 2026-06-04T10:01Z

target:
- fusion analysis embedded runtime-summary matcher parity

status:
- completed

result:
- exposed `matcher.frame_dimensions` and `matcher.frame_coordinate_space` in the fused sidecar's embedded `runtime` summary
- added fusion-analysis regression coverage for the normalized-frame fields

verification:
- `.venv/bin/python -m unittest tests.test_fusion_analysis`
- pending repo health gate

notes:
- the touched fusion-analysis files already contained unrelated local deltas before this slice, so this commit will include both the new runtime-summary parity change and those pre-existing file-local edits

## 2026-06-04T10:19Z

target:
- fixture sidecar comparison runtime matcher metadata parity

status:
- completed

result:
- carried runtime `frame_dimensions` and `frame_coordinate_space` through fixture-sidecar comparison runtime summaries
- added comparison-row coverage for baseline/trial normalized-frame metadata

verification:
- `.venv/bin/python -m unittest tests.test_fixture_sidecar_comparison`
- pending repo health gate

notes:
- this keeps runtime fixture comparisons aligned with the matcher coordinate contract instead of reducing runtime rows to score/action only

## 2026-06-04T10:31Z

target:
- operator-pack pipeline contracts matcher-coordinate note

status:
- completed

result:
- added an operator-facing note in `PIPELINE_CONTRACTS.md` that `runtime_analysis_v1` matcher coordinates are pack-normalized
- pointed operators to `matcher.frame_dimensions` and `matcher.frame_coordinate_space` before interpreting `frame_match_x` or `frame_match_y`

verification:
- pending repo health gate

notes:
- this is a doc-only follow-up so the operator pack reflects the same coordinate contract already enforced in code, tests, and canonical V2 docs

## 2026-05-31T00:58Z

target:
- reward-banner first-slice implementation ticket

status:
- completed

result:
- converted the bootstrap text-banner packet into a direct implementation ticket
- fixed the first semantic recommendation as `reward_banner -> hud_visibility -> high_action_sequence`
- identified the minimum published-pack surfaces that would change in the first pilot

verification:
- pending

notes:
- this is the narrowest safe handoff before making a behavior-changing published-pack edit

## 2026-05-30T22:34Z

target:
- call_of_duty reward-banner runtime pilot

status:
- completed

result:
- added a published-pack `reward_banner` pilot for `call_of_duty`
- validated the pack and repo health after the new runtime family was added
- proved the new family emits `hud_visibility -> high_action_sequence` on a focused local `UAV` banner probe
- confirmed the broader 8s-24s probe is no longer equipment-only

verification:
- `python3 run.py --validate-game-pack call_of_duty`
- `python3 run.py --run-repo-quality-health`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_9p5s_12p5s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_9p5s_12p5s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_pilot/debug_3s --sample-fps 2`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_8s_24s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_8s_24s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_pilot/debug_8s_24s --sample-fps 2`

notes:
- initial threshold `0.94` was too strict for sampled runtime frames; lowering it to `0.90` unlocked the pilot
- the broader probe still appears to include at least one likely false positive, so the next slice should be reward-banner quality control rather than family expansion

## 2026-05-31T04:12Z

target:
- call_of_duty reward-banner false-positive control

status:
- completed

result:
- identified the broader-probe false positive as a structurally similar `MORTAR STRIKE` banner
- added a title-focused mask for the `UAV` reward-banner template
- tightened the final threshold to `0.91`
- confirmed the masked broader probe keeps the true `UAV` event and drops the late false-positive hit

verification:
- `python3 run.py --validate-game-pack call_of_duty`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_9p5s_12p5s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_9p5s_12p5s.masked.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_pilot/debug_3s_masked --sample-fps 2`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_8s_24s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_8s_24s.masked.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_pilot/debug_8s_24s_masked --sample-fps 2`

notes:
- the title-band mask is the smallest change that separates `UAV` from the visually similar `MORTAR STRIKE` banner in the current sample
- the next slice should validate this masked pilot across the broader measurement set rather than adding more banner families immediately

## 2026-06-01T15:06Z

target:
- call_of_duty reward-banner cross-sample validation

status:
- completed

result:
- reran the masked published-pack `reward_banner` pilot across the original four-sample `call_of_duty` measurement set
- confirmed the new family stays clean off-target:
  - `SVbTc2AZzYw.60s-70s` remained equipment-only
  - `v-SzAArdAfY.60s-70s` remained no-events
  - `Qop1sH70nHI.60s-70s` remained no-events
- confirmed `_PL_5qWwKtY` still emits one `reward_banner -> high_action_sequence` event at `11.0s`

verification:
- `.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/SVbTc2AZzYw.60s-70s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/debug_SVbTc2AZzYw.60s-70s --sample-fps 1 --limit-frames 30`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/_PL_5qWwKtY.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/debug__PL_5qWwKtY --sample-fps 1 --limit-frames 30`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_measurement_sources/v-SzAArdAfY.60s-70s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/v-SzAArdAfY.60s-70s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/debug_v-SzAArdAfY.60s-70s --sample-fps 1 --limit-frames 30`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_measurement_sources/Qop1sH70nHI.60s-70s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/Qop1sH70nHI.60s-70s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_reward_banner_cross_sample_masked/debug_Qop1sH70nHI.60s-70s --sample-fps 1 --limit-frames 30`

notes:
- the masked `UAV` banner family is now proven clean across the current measurement set, but only one sample family actually contains the target surface
- the next slice should validate the same reward-banner family on at least one more compatible clip before adding more templates or moving to OCR

## 2026-06-01T15:43Z

target:
- call_of_duty reward-banner candidate clip scout

status:
- completed

result:
- downloaded two additional public `Black Ops Cold War` high-streak gameplay sources
- converted each source into a 120-second local probe
- scanned the published masked `UAV` reward-banner template directly across the published `reward_banner` ROI at one frame per second
- neither probe produced a score near the published `0.91` threshold

verification:
- `yt-dlp -f "bv*[height<=480]+ba/b[height<=480]" -o "outputs/public_gameplay_mining/call_of_duty_reward_banner_candidates/%(id)s.%(ext)s" "https://www.youtube.com/watch?v=ZrWvsu5wjuM"`
- `yt-dlp -f "bv*[height<=480]+ba/b[height<=480]" -o "outputs/public_gameplay_mining/call_of_duty_reward_banner_candidates/%(id)s.%(ext)s" "https://www.youtube.com/watch?v=qryfXU7w2IQ"`
- `ffmpeg -y -ss 0 -t 120 -i outputs/public_gameplay_mining/call_of_duty_reward_banner_candidates/ZrWvsu5wjuM.webm -c:v libx264 -preset veryfast -crf 23 -c:a aac outputs/measurement/call_of_duty_reward_banner_candidate_probes/ZrWvsu5wjuM.0s-120s.mp4`
- `ffmpeg -y -ss 0 -t 120 -i outputs/public_gameplay_mining/call_of_duty_reward_banner_candidates/qryfXU7w2IQ.webm -c:v libx264 -preset veryfast -crf 23 -c:a aac outputs/measurement/call_of_duty_reward_banner_candidate_probes/qryfXU7w2IQ.0s-120s.mp4`
- direct masked frame scan using:
  - `assets/games/call_of_duty/templates/reward_banners/uav.png`
  - `assets/games/call_of_duty/templates/reward_banners/uav.mask.png`
  - ROI `reward_banner` from `assets/games/call_of_duty/hud.yaml`

notes:
- best observed scores were approximately `0.848` on both probes, well below the current published threshold
- the current `reward_banner` family still behaves like a narrow pilot rather than a reusable cross-clip family
- the next decision should focus on source-family verification or pivoting to a different text family, not adding another banner asset

## 2026-06-01T16:09Z

target:
- call_of_duty native-vs-overlay surface check

status:
- completed

result:
- extracted `_PL_5qWwKtY` source frames and ROI crops around `11.0s` and `20.5s`
- compared them against the two additional `Black Ops Cold War` candidate probes
- concluded the center-top `UAV` banner in `_PL_5qWwKtY` still looks like native game HUD
- concluded the lower-center count text (`4TH KILL`, `7TH KO`) behaves like an editorial or overlay family and should not currently be treated as pack truth
- reclassified the earlier candidate-probe failure as a HUD-family mismatch rather than proof that the `_PL_5qWwKtY` banner is non-native

verification:
- `ffmpeg -y -ss 11.0 -i outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4 -frames:v 1 outputs/inspection/call_of_duty_overlay_check/pl_11_full.png`
- `ffmpeg -y -ss 20.5 -i outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4 -frames:v 1 outputs/inspection/call_of_duty_overlay_check/pl_20p5_full.png`
- `ffmpeg -y -ss 99.1 -i outputs/measurement/call_of_duty_reward_banner_candidate_probes/ZrWvsu5wjuM.0s-120s.mp4 -frames:v 1 outputs/inspection/call_of_duty_overlay_check/zr_99p1_full.png`
- `ffmpeg -y -ss 104.1 -i outputs/measurement/call_of_duty_reward_banner_candidate_probes/qryfXU7w2IQ.0s-120s.mp4 -frames:v 1 outputs/inspection/call_of_duty_overlay_check/qr_104p1_full.png`
- ROI crop extraction from the published `reward_banner` and `ability_hud` regions

notes:
- the `medal.tv` watermark is real in `_PL_5qWwKtY`, but it does not invalidate the native-looking upper-middle `UAV` streak panel
- the lower-center count text is the weaker source family and should not be the next pack-expansion target
- the next acquisition pass should target the same title/HUD family as `_PL_5qWwKtY`, not generic `Black Ops Cold War` streak videos

## 2026-06-01T16:44Z

target:
- call_of_duty MWIII Vista reward-banner same-family scout

status:
- completed

result:
- downloaded two `Modern Warfare III` `Vista` no-commentary multiplayer sources
- converted both into 120-second local probes
- rescanned the published masked `UAV` reward-banner template directly over the published `reward_banner` ROI
- both same-family probes still topped out around `0.85`, below the current `0.91` threshold

verification:
- `yt-dlp -f "bv*[height<=480]+ba/b[height<=480]" -o "outputs/public_gameplay_mining/call_of_duty_reward_banner_mwiii_candidates/%(id)s.%(ext)s" "https://www.youtube.com/watch?v=CXh9c8AUoZw"`
- `yt-dlp -f "bv*[height<=480]+ba/b[height<=480]" -o "outputs/public_gameplay_mining/call_of_duty_reward_banner_mwiii_candidates/%(id)s.%(ext)s" "https://www.youtube.com/watch?v=gcAGS3R2t2o"`
- `ffmpeg -y -ss 0 -t 120 -i outputs/public_gameplay_mining/call_of_duty_reward_banner_mwiii_candidates/CXh9c8AUoZw.webm -c:v libx264 -preset veryfast -crf 23 -c:a aac outputs/measurement/call_of_duty_reward_banner_mwiii_probes/CXh9c8AUoZw.0s-120s.mp4`
- `ffmpeg -y -ss 0 -t 120 -i outputs/public_gameplay_mining/call_of_duty_reward_banner_mwiii_candidates/gcAGS3R2t2o.webm -c:v libx264 -preset veryfast -crf 23 -c:a aac outputs/measurement/call_of_duty_reward_banner_mwiii_probes/gcAGS3R2t2o.0s-120s.mp4`
- direct masked frame scan using the published `UAV` template, mask, and `reward_banner` ROI

notes:
- this result is stronger than the earlier `Black Ops Cold War` scout because it uses the same inferred title/HUD family as `_PL_5qWwKtY`
- the current `reward_banner` asset should now be treated as a narrow validated pilot, not an expanding family
- the next useful branch is either template-specificity diagnosis or a pivot to a more repeatable native surface

## 2026-06-03T03:08Z

target:
- call_of_duty reward-banner template-specificity diagnosis

status:
- completed

result:
- extracted the best-scoring same-family non-hit frames from the `MWIII` `Vista` scout
- compared their reward-region crops directly against the true `_PL_5qWwKtY` `UAV` banner crop
- confirmed the non-hit frames do not visibly contain a structured upper-middle `UAV` panel at all
- concluded the current failure mode is missing target surface / timing visibility, not recoverable small alignment or scale drift

verification:
- `ffmpeg -y -ss 60.06 -i outputs/measurement/call_of_duty_reward_banner_mwiii_probes/CXh9c8AUoZw.0s-120s.mp4 -frames:v 1 outputs/inspection/call_of_duty_reward_banner_specificity/cx_60p06_full.png`
- `ffmpeg -y -ss 94.09 -i outputs/measurement/call_of_duty_reward_banner_mwiii_probes/gcAGS3R2t2o.0s-120s.mp4 -frames:v 1 outputs/inspection/call_of_duty_reward_banner_specificity/gc_94p09_full.png`
- reward-region crop extraction using the published `reward_banner` ROI from `assets/games/call_of_duty/hud.yaml`

notes:
- the best-scoring same-family non-hits are structurally unrelated scene crops, not weak `UAV` banner variants
- the next slice should not be threshold or scale tuning for this family
- the strongest next move is to cap `reward_banner` as a narrow pilot and pivot to a more repeatable native surface

## 2026-06-03T03:34Z

target:
- call_of_duty post-reward-banner surface pivot

status:
- completed

result:
- locally scouted same-family `MWIII Vista` clip contact sheets for repeatable native HUD alternatives
- found stable HUD and objective clutter, but no strong alternative surface that is both clearly native and highlight-salient enough to implement directly from local evidence
- converted the next branch into an explicit research request for `ALTERNATIVE_NATIVE_SURFACE_PACKET`

verification:
- contact-sheet scout from:
  - `outputs/inspection/call_of_duty_alt_surface_scout/cx_contact.png`
  - `outputs/inspection/call_of_duty_alt_surface_scout/gc_contact.png`
- direct spot checks on repeated late-match frames

notes:
- the repo now has enough local evidence to stop blind surface scouting
- the next useful input is a decision-ready packet that ranks alternative native surface families by repeatability and extraction feasibility

## 2026-06-03T04:02Z

target:
- operator-pack alignment for post-reward-banner branch

status:
- completed

result:
- updated `CODEX_BACKLOG.md` so its active queue reflects the current `call_of_duty` blocker instead of the older bootstrap-only phases
- updated `EXECUTION_TARGET.md` so the active objective and phase gate match the capped `reward_banner` state
- kept the coordination split explicit: dashboard for live execution snapshot, run log for chronology, backlog for queued work

verification:
- consistency check across:
  - `docs/v2/operator_pack/CODEX_AUTONOMY_DASHBOARD.md`
  - `docs/v2/operator_pack/CODEX_BACKLOG.md`
  - `docs/v2/operator_pack/EXECUTION_TARGET.md`

notes:
- this was a contract-alignment slice only
- the next behavior-changing step still depends on `ALTERNATIVE_NATIVE_SURFACE_PACKET`

## 2026-06-03T04:21Z

target:
- knowledge-base placement split

status:
- completed

result:
- updated the canonical V2 index to explicitly separate:
  - canonical repo knowledge base
  - Custom GPT upload pack
  - material that should stay out of the system
- updated the Custom GPT knowledge-pack readme to clarify:
  - what the pack is for
  - when it should be updated
  - how ML-adjacent work should be scoped

verification:
- consistency check across:
  - `docs/v2/INDEX.md`
  - `docs/v2/custom_gpt_knowledge/README.md`
  - `FUTURE_FEATURES_ROADMAP.md`

notes:
- this slice clarifies placement and scope only
- it does not change runtime behavior or the active `call_of_duty` execution branch

## 2026-06-03T04:46Z

target:
- call_of_duty alternative native surface packet intake

status:
- completed

result:
- promoted the received `ALTERNATIVE_NATIVE_SURFACE_PACKET` into a durable handoff
- updated the dashboard and backlog so the next local branch is no longer a generic alternative-surface search
- narrowed the next local validation family to `killfeed_events`, with `objective_event_notifications` behind it

verification:
- consistency check across:
  - `docs/handoffs/2026-06-03-call-of-duty-alternative-native-surface-packet.md`
  - `docs/v2/operator_pack/CODEX_AUTONOMY_DASHBOARD.md`
  - `docs/v2/operator_pack/CODEX_BACKLOG.md`

notes:
- the packet is strong enough to choose the next local scout family
- it is not yet strong enough to justify published-pack mutation because clip-backed evidence is still missing

## 2026-06-03T05:12Z

target:
- call_of_duty top-right event-card scout

status:
- completed

result:
- generated top-right ROI contact sheets over the current `call_of_duty` sample set and same-title `MWIII Vista` probes
- confirmed the repeated visible family is not clean generic killfeed
- confirmed the stronger surviving family is native top-right event or status cards
- recorded clip-backed evidence at:
  - `_PL_5qWwKtY @ 12s-14s`
  - `gcAGS3R2t2o @ 27s-29s`
- promoted the next implementation-facing slice into a runtime ticket instead of mutating the published pack

verification:
- contact sheets under:
  - `outputs/inspection/call_of_duty_killfeed_scout/`
- exact stills under:
  - `outputs/inspection/call_of_duty_killfeed_scout/stills/`
- handoffs:
  - `docs/handoffs/2026-06-03-call-of-duty-top-right-event-card-scout.md`
  - `docs/handoffs/2026-06-03-call-of-duty-top-right-event-card-runtime-ticket.md`

notes:
- the `kill_feed` ROI name should not be treated as proof that the visible repeated family is elimination killfeed
- the next blocker is now the first stable anchor strategy for a narrow top-right event-card pilot

## 2026-06-04T00:14Z

target:
- call_of_duty top-right event-card direct handoff

status:
- completed

result:
- converted the top-right event-card runtime ticket into a direct Codex handoff
- locked the first conservative anchor strategy:
  - template-compatible top-right card-shell anchor
  - existing `kill_feed` ROI
  - conservative `hud_visibility -> high_action_sequence` mapping
- updated the operator pack so the branch is now implementation-ready rather than still waiting on boundary clarification

verification:
- consistency check across:
  - `docs/handoffs/2026-06-03-call-of-duty-top-right-event-card-runtime-ticket.md`
  - `docs/handoffs/2026-06-04-call-of-duty-top-right-event-card-codex-handoff.md`
  - `docs/v2/operator_pack/CODEX_AUTONOMY_DASHBOARD.md`
  - `docs/v2/operator_pack/CODEX_BACKLOG.md`

notes:
- this slice still stops before published-pack mutation
- broad OCR expansion remains explicitly out of scope for the first pilot

## 2026-06-04T03:20Z

target:
- call_of_duty top-right event-card shell pilot

status:
- completed

result:
- implemented the first provisional shell-anchor pilot against the existing top-right ROI
- confirmed both positive evidence windows around `_PL_5qWwKtY @ 12s-15s` and `gcAGS3R2t2o @ 27s-30s`
- pressure-tested the same anchor on the three short-window negatives
- rolled the published-pack mutation back after the shell matched all three negatives at the same score band as the positives
- promoted the next blocker into an anchor-specific packet request

verification:
- `python3 run.py --validate-game-pack call_of_duty`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/measurement/call_of_duty_top_right_event_card_pilot/_PL_5qWwKtY_12s_15s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_top_right_event_card_pilot/_PL_5qWwKtY_12s_15s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_top_right_event_card_pilot/debug_pl_12s_15s --sample-fps 2`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/measurement/call_of_duty_top_right_event_card_pilot/gcAGS3R2t2o_27s_30s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_top_right_event_card_pilot/gcAGS3R2t2o_27s_30s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_top_right_event_card_pilot/debug_gc_27s_30s --sample-fps 2`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_top_right_event_card_pilot/SVbTc2AZzYw_60s_70s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_top_right_event_card_pilot/debug_SVbTc2AZzYw_60s_70s --sample-fps 2`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_measurement_sources/v-SzAArdAfY.60s-70s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_top_right_event_card_pilot/v-SzAArdAfY_60s_70s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_top_right_event_card_pilot/debug_v-SzAArdAfY_60s_70s --sample-fps 2`
- `.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_measurement_sources/Qop1sH70nHI.60s-70s.mp4 call_of_duty --output-path outputs/measurement/call_of_duty_top_right_event_card_pilot/Qop1sH70nHI_60s_70s.runtime.json --debug-output-dir outputs/measurement/call_of_duty_top_right_event_card_pilot/debug_Qop1sH70nHI_60s_70s --sample-fps 2`

notes:
- the failure mode is anchor genericity, not threshold weakness
- the next useful input is a discriminative top-right anchor packet, or explicit family retirement if no such anchor exists

## 2026-06-04T04:05Z

target:
- call_of_duty top-right anchor local diagnostics

status:
- completed

result:
- generated full top-right ROI crops for the positive and negative pilot windows
- generated exact matched-patch crops from the highest-scoring detections
- confirmed the matcher was not locking onto a reusable event-card identity
- recorded the local evidence as a bounded diagnostic handoff

verification:
- local inspection artifacts under:
  - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/`
- handoff:
  - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-local-diagnostics.md`

notes:
- the strongest local positive still does not expose a clean reusable icon block
- the current best local evidence strengthens the need for a discriminative anchor packet rather than another local shell-derived template

## 2026-06-04T04:22Z

target:
- call_of_duty top-right anchor OCR viability

status:
- completed

result:
- ran bounded OCR probes over the two positive top-right cards and the three negative comparison crops
- confirmed `_PL_5qWwKtY` yields only partial text recovery
- confirmed `gcAGS3R2t2o` does not yield useful OCR signal at the current crop quality
- confirmed negatives generate OCR noise rather than a clean separable failure pattern
- tightened the active anchor-packet request so a text-fragment recommendation now needs stronger evidence than the current local clips provide

verification:
- OCR diagnostic crops under:
  - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/ocr/`
- updated handoffs:
  - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-local-diagnostics.md`
  - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-packet-request.md`

notes:
- this does not ban `mixed_ocr_template`
- it does rule out treating the current local positives as sufficient OCR-first evidence

## 2026-06-04T04:36Z

target:
- call_of_duty top-right anchor appendix packaging

status:
- completed

result:
- packaged the current local top-right anchor evidence into a compact appendix linked to the active anchor packet
- kept the appendix bounded to visual crops, matched patches, OCR preprocessing artifacts, and concise operator notes

verification:
- appendix:
  - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-appendix.md`

notes:
- this is a support artifact only
- it exists to reduce researcher interpretation burden without creating a second packet

## 2026-06-04T06:08Z

target:
- call_of_duty Vista anchor candidate scout

status:
- completed

result:
- downloaded two additional high-kill `MWIII Vista` probes from `vistastructions`
- generated sparse top-right contact sheets across the first two minutes
- confirmed both probes are same-title but wrong-surface candidates for the current blocker
- tightened the active anchor packet so these clips are now explicit exclusions rather than promising sources

verification:
- contact sheets:
  - `outputs/inspection/call_of_duty_top_right_anchor_candidates/-unDN10cqgo_top_right_contact_sheet.png`
  - `outputs/inspection/call_of_duty_top_right_anchor_candidates/vY8j3bLkkMI_top_right_contact_sheet.png`
- updated handoffs:
  - `docs/handoffs/2026-06-04-call-of-duty-vista-anchor-candidate-scout.md`
  - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-packet-request.md`

notes:
- these clips mostly show loadout labels, player-name overlays, or environment-only frames in the top-right ROI
- they should be treated as exclusion evidence, not as positive anchor sources

## 2026-06-04T06:22Z

target:
- call_of_duty top-right anchor dispatch packaging

status:
- completed

result:
- compressed the active blocker, exclusions, and supporting artifact chain into one short researcher-facing dispatch
- kept the dispatch bounded to the current anchor packet instead of creating a new decision surface

verification:
- dispatch:
  - `docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-dispatch.md`

notes:
- this is a packaging artifact only
- it exists to reduce external context reconstruction, not to change the current execution branch

## 2026-06-04T06:46Z

target:
- roi matcher normalized-frame debug contract

status:
- completed

result:
- made matcher results expose normalized frame dimensions and coordinate-space explicitly
- propagated the same fields into runtime-analysis matcher payloads
- added regression coverage for:
  - non-zero ROI origin frame-coordinate reconstruction
  - debug CSV visibility of normalized coordinate space

verification:
- `.venv/bin/python -m unittest tests.test_roi_matcher tests.test_runtime_analysis`
- `python3 run.py --run-repo-quality-health`

notes:
- this is a contract-clarity hardening slice
- it does not change detector scoring or runtime matching behavior

## 2026-06-04T07:14Z

target:
- contract-audit enforcement for matcher coordinate contract

status:
- completed

result:
- added `docs/v2/operator_pack/PIPELINE_CONTRACTS.md` to governance-surface auditing
- required the runtime-sidecar coordinate note to keep explicit anchors for:
  - `matcher.frame_dimensions`
  - `matcher.frame_coordinate_space`
  - pack-normalized frame-space interpretation
- extended contract-audit tests to cover both complete and incomplete operator-pack cases

verification:
- `.venv/bin/python -m unittest tests.test_contract_audit`
- `python3 run.py --run-repo-quality-health`

notes:
- this promotes the matcher coordinate rule from downstream propagation only to audited operator-contract status
- no runtime behavior changed

## 2026-06-04T07:31Z

target:
- runtime review bridge sidecar-shape hardening

status:
- completed

result:
- tightened `runtime_review_bridge` candidate intake so malformed `matcher` or `events` payloads are skipped instead of being normalized implicitly
- specifically guarded:
  - non-dict `matcher` or `events` sections
  - non-list `confirmed_detections`
  - non-list `events.rows`
  - non-dict `frame_dimensions`
  - non-string `frame_coordinate_space` when present
- added regression coverage proving malformed sidecars are excluded from prepared review sessions

verification:
- `.venv/bin/python -m unittest tests.test_runtime_review_bridge`
- `python3 run.py --run-repo-quality-health`

notes:
- this is a narrow bridge-contract hardening change
- valid older sidecars are still accepted as long as the relevant sections keep the expected shapes

## 2026-06-04T07:42Z

target:
- fused review bridge event-shape hardening

status:
- completed

result:
- tightened `fused_review_bridge` candidate intake so malformed fused-event payloads are skipped instead of being partially materialized
- specifically guarded:
  - non-list `fused_events`
  - non-dict fused-event rows
  - non-dict `metadata`
  - non-list `metadata.matched_signal_types`
- added regression coverage proving invalid fused-event rows are excluded from prepared review sessions

verification:
- `.venv/bin/python -m unittest tests.test_fused_review_bridge`
- `python3 run.py --run-repo-quality-health`

notes:
- this mirrors the runtime-review bridge hardening pattern on the fused-review entrypoint
- no fused scoring behavior changed

## 2026-06-04T07:53Z

target:
- proxy review bridge sidecar-shape hardening

status:
- completed

result:
- tightened `proxy_review_bridge` candidate intake so malformed proxy sidecars are skipped instead of being materialized into GPT review sessions
- specifically guarded:
  - non-list `windows`
  - non-dict top window rows
  - non-list `sources`
  - non-list `source_families`
- added regression coverage at the run-level proxy review entrypoint proving malformed sidecars are excluded from selection

verification:
- `.venv/bin/python -m unittest tests.test_run.RunTests.test_prepare_proxy_review_selects_download_candidates_and_writes_gpt_queue_files tests.test_run.RunTests.test_prepare_proxy_review_can_select_from_batch_report tests.test_run.RunTests.test_prepare_proxy_review_can_use_explicit_batch_report_candidates_without_windows tests.test_run.RunTests.test_prepare_proxy_review_skips_malformed_proxy_sidecars tests.test_run.RunTests.test_apply_proxy_review_updates_sidecars_and_is_idempotent tests.test_run.RunTests.test_cleanup_proxy_review_removes_generated_bridge_artifacts`
- `python3 run.py --run-repo-quality-health`

notes:
- this is the proxy-side equivalent of the runtime and fused review bridge hardening work
- no proxy scoring behavior changed

## 2026-06-04T08:02Z

target:
- proxy review explicit batch-row hardening

status:
- completed

result:
- tightened `proxy_review_bridge` explicit batch-report intake so malformed explicit rows no longer fall back to raw sidecar ingestion
- specifically guarded:
  - missing or non-file `sidecar_path` on explicit rows
  - non-list explicit `sources`
  - non-list explicit `source_families`
  - partially explicit rows now stay on the explicit path and fail closed if malformed
- added regression coverage proving malformed explicit batch rows are excluded from proxy review selection

verification:
- `.venv/bin/python -m unittest tests.test_run.RunTests.test_prepare_proxy_review_can_use_explicit_batch_report_candidates_without_windows tests.test_run.RunTests.test_prepare_proxy_review_skips_malformed_proxy_sidecars tests.test_run.RunTests.test_prepare_proxy_review_skips_malformed_explicit_batch_rows tests.test_run.RunTests.test_prepare_proxy_review_can_select_from_batch_report`
- `python3 run.py --run-repo-quality-health`

notes:
- this closes the remaining permissive fallback in proxy review candidate intake
- no proxy scoring behavior changed

## 2026-06-04T08:11Z

target:
- accepted proxy review prep bridge-result hardening

status:
- completed

result:
- tightened `accepted_proxy_review_prep` so malformed bridge items are ignored instead of being treated as valid prepared results
- specifically required bridge items to provide both:
  - `sidecar_path`
  - `gpt_meta_path`
- added regression coverage proving malformed bridge items degrade to partial prep results instead of breaking row mapping

verification:
- `.venv/bin/python -m unittest tests.test_accepted_proxy_review_prep`
- `python3 run.py --run-repo-quality-health`

notes:
- this is a prep-layer fail-closed guard around `prepare_proxy_review` integration
- no bridge selection or proxy scoring behavior changed

## 2026-06-04T08:22Z

target:
- runtime export sidecar-shape hardening

status:
- completed

result:
- tightened `runtime_export` so malformed `runtime_analysis_v1` payload shapes are skipped instead of being exported into dataset rows
- specifically guarded:
  - non-dict `events`
  - non-dict `matcher`
  - non-list `events.rows`
  - non-list `matcher.confirmed_detections`
  - non-dict `matcher.frame_dimensions` when present
  - non-string `matcher.frame_coordinate_space` when present
- added explicit skip accounting for `invalid_runtime_shape`
- added regression coverage proving malformed runtime sidecars are skipped with the new reason

verification:
- `.venv/bin/python -m unittest tests.test_runtime_export`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the fail-closed intake pattern from review/prep bridges into dataset export
- no runtime scoring behavior changed for valid sidecars

## 2026-06-04T08:34Z

target:
- runtime calibration and tuning sidecar-shape hardening

status:
- completed

result:
- tightened both `runtime_calibration` and `runtime_tuning` so malformed `runtime_analysis_v1` payload shapes are skipped instead of being treated as reviewed runtime evidence
- specifically guarded:
  - non-dict `events`
  - non-dict `matcher`
  - non-list `events.rows`
  - non-list `matcher.confirmed_detections`
  - non-dict `matcher.frame_dimensions` when present
  - non-string `matcher.frame_coordinate_space` when present
- added regression coverage proving malformed reviewed sidecars surface `invalid_runtime_shape` warnings in both calibration and tuning flows

verification:
- `.venv/bin/python -m unittest tests.test_runtime_calibration tests.test_runtime_tuning`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the same fail-closed intake rule into the reviewed-runtime analysis surfaces
- no scoring behavior changed for valid sidecars

## 2026-06-04T08:47Z

target:
- proxy training export and calibration sidecar-shape hardening

status:
- completed

result:
- tightened both `training_export` and `proxy_calibration` so malformed `proxy_scan_v1` payload shapes are skipped instead of being exported or calibrated
- specifically guarded:
  - non-list `windows`
  - non-dict `source_results`
  - non-dict window rows
  - non-list window `sources`
  - non-list window `source_families`
  - non-list window `signals`
- added explicit `invalid_proxy_shape` skip accounting for training export
- added regression coverage proving malformed proxy sidecars surface `invalid_proxy_shape` in export and calibration flows

verification:
- `.venv/bin/python -m unittest tests.test_training_export tests.test_proxy_calibration`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the fail-closed intake rule into the proxy-side dataset and review-calibration consumers
- no proxy scoring behavior changed for valid sidecars

## 2026-06-04T09:01Z

target:
- fused export sidecar-shape hardening

status:
- completed

result:
- tightened `fused_export` so malformed `fused_analysis_v1` payload shapes are skipped instead of being exported into fused candidate datasets
- specifically guarded:
  - non-list `normalized_signals`
  - non-list `fused_events`
  - non-dict normalized-signal rows
  - non-dict fused-event rows
  - non-dict fused-event `metadata` when present
  - non-list fused-event `contributing_signals`
- added explicit `invalid_fused_shape` skip accounting
- added regression coverage proving malformed fused sidecars are skipped with that reason

verification:
- `.venv/bin/python -m unittest tests.test_fused_export`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the fail-closed intake rule into the fused-side dataset export surface
- no fused scoring or selection behavior changed for valid sidecars

## 2026-06-04T09:14Z

target:
- highlight selection export sidecar-shape hardening

status:
- completed

result:
- tightened `highlight_selection_export` so malformed proxy or fused sidecar shapes fail closed before manifest and OTIO generation
- specifically guarded proxy selection inputs:
  - non-list `windows`
  - non-dict window rows
  - non-list window `sources`
  - non-list window `source_families`
- specifically guarded fused selection inputs:
  - non-list `normalized_signals`
  - non-list `fused_events`
  - non-dict normalized-signal rows
  - non-dict fused-event rows
  - non-dict fused-event `metadata` when present
  - non-list fused-event `contributing_signals`
- added regression coverage proving malformed proxy and fused sidecars return `invalid_*_sidecar` errors

verification:
- `.venv/bin/python -m unittest tests.test_highlight_selection_export`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the fail-closed intake rule into direct highlight-selection manifest generation
- no selection behavior changed for valid sidecars

## 2026-06-04T09:28Z

target:
- clip registry sidecar-shape hardening

status:
- completed

result:
- tightened `clip_registry` so malformed proxy, runtime, and fused sidecar shapes are skipped before they mutate durable registry state
- specifically guarded proxy ingestion:
  - non-list `windows`
  - non-dict window rows
  - non-list window `sources`
  - non-list window `source_families`
- specifically guarded runtime ingestion:
  - non-dict `matcher`
  - non-dict `events`
  - non-list `matcher.confirmed_detections`
  - non-list `events.rows`
  - non-dict `matcher.frame_dimensions` when present
  - non-string `matcher.frame_coordinate_space` when present
  - non-dict detection and event rows
- specifically guarded fused ingestion:
  - non-list `normalized_signals`
  - non-list `fused_events`
  - non-dict normalized-signal rows
  - non-dict fused-event rows
  - non-dict fused-event `metadata` when present
- added regression coverage proving invalid sidecars emit `invalid_proxy_shape`, `invalid_runtime_shape`, and `invalid_fused_shape` warnings without generating clip or child rows

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the fail-closed intake rule into the durable sidecar-to-registry ingestion layer
- valid registry ingestion behavior is unchanged

## 2026-06-04T09:36Z

target:
- clip registry review-session shape hardening

status:
- completed

result:
- tightened `clip_registry` so malformed runtime and fused review-session manifests are skipped before they create durable session or item rows
- specifically guarded:
  - non-list `items`
  - non-dict item rows
- added regression coverage proving invalid runtime and fused review-session manifests emit `invalid_*_review_session_shape` warnings without generating session or item rows

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the fail-closed intake rule one layer above the already-hardened review bridges
- valid review-session registry ingestion behavior is unchanged

## 2026-06-05T09:12Z

target:
- clip registry highlight-selection shape hardening

status:
- completed

result:
- tightened `clip_registry` so malformed highlight-selection manifests are skipped before they create durable selection rows used by lifecycle and export lineage
- specifically guarded:
  - non-list `selected_highlights`
  - non-dict selected-highlight rows
- added regression coverage proving invalid highlight-selection manifests emit `invalid_highlight_selection_shape` warnings without being ingested

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the fail-closed intake rule into a manifest that feeds candidate lifecycle and export-detail derivation
- valid highlight-selection registry ingestion behavior is unchanged

## 2026-06-05T09:31Z

target:
- clip registry downstream manifest shape hardening

status:
- completed

result:
- tightened `clip_registry` so malformed downstream durable manifests are skipped before they create persistent rows
- specifically guarded hook-candidate ingestion:
  - non-list `hook_candidates`
  - non-dict hook-candidate rows
- specifically guarded workflow-run ingestion:
  - non-list `items`
  - non-dict workflow item rows
- specifically guarded highlight-export-batch ingestion:
  - non-dict `linked_inputs` when present
  - non-list `linked_inputs.fused_sidecar_paths`
  - non-list `linked_inputs.hook_manifest_paths`
  - non-list `linked_inputs.selection_manifest_paths`
  - non-list `exports`
  - non-dict export rows
- specifically guarded posted-ledger ingestion:
  - non-list `posted_records`
  - non-dict posted-record rows
- specifically guarded posted-metrics ingestion:
  - non-list `snapshots`
  - non-dict snapshot rows
- added regression coverage proving invalid manifests emit:
  - `invalid_hook_candidate_shape`
  - `invalid_workflow_run_shape`
  - `invalid_highlight_export_batch_shape`
  - `invalid_posted_highlight_ledger_shape`
  - `invalid_posted_metrics_snapshot_shape`
  and do not generate durable rows

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- this closes most of the remaining top-level list-shape trust gaps in registry ingestion
- valid downstream manifest ingestion behavior is unchanged

## 2026-06-05T09:46Z

target:
- clip registry fixture-trial manifest shape hardening

status:
- completed

result:
- tightened `clip_registry` so malformed fixture-trial run and batch manifests are skipped before they create durable trial rows
- specifically guarded fixture-trial run ingestion:
  - non-list `fixtures`
  - non-dict fixture rows
- specifically guarded fixture-trial batch ingestion:
  - non-list `selected_trials`
  - non-list `trial_comparisons`
  - non-dict trial-comparison rows
- added regression coverage proving invalid manifests emit:
  - `invalid_fixture_trial_run_shape`
  - `invalid_fixture_trial_batch_shape`
  and do not generate durable rows

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- this extends the same fail-closed rule into the fixture-trial lineage surfaces
- valid fixture-trial registry ingestion behavior is unchanged

## 2026-06-05T10:08Z

target:
- clip registry deferred report and shadow manifest hardening

status:
- completed

result:
- tightened `clip_registry` so the remaining deferred comparison-report, shadow-manifest, and real-lineage summary surfaces fail closed on malformed list/object payloads
- specifically guarded comparison/report ingestion:
  - invalid fixture comparison `comparison.fixture_rows`
  - invalid hook comparison `comparison.fixture_rows`
  - invalid shadow ranking comparison `comparison.rows`
  - invalid shadow benchmark evidence comparison `rows`
- specifically guarded shadow row/slice ingestion:
  - invalid shadow ranking experiment ledger `slice_rows`
  - invalid shadow ranking replay `rows`
  - invalid shadow model family comparison `rows`
  - invalid shadow benchmark matrix `runs`
  - invalid shadow benchmark matrix `benchmark_config.model_families`
  - invalid shadow benchmark matrix `benchmark_config.training_targets`
  - invalid shadow benchmark review `target_reviews`
  - invalid shadow benchmark review `reviewed_targets`
  - invalid shadow benchmark review `reviewed_families`
  - invalid shadow benchmark review `source_benchmark_manifest_paths`
- specifically guarded real lineage summary ingestion:
  - invalid `source_roots`
  - invalid `scanned_roots`
  - invalid `source_root_summaries`
- added grouped regression coverage proving invalid manifests emit manifest-specific `invalid_*_shape` warnings and do not generate durable rows

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- this closes the remaining local `clip_registry` families that had been explicitly set aside as lower-yield follow-up work
- the blocked `call_of_duty` branch still remains deferred on external evidence and did not require an operator-surface change in this slice

## 2026-06-05T10:26Z

target:
- blocked `call_of_duty` researcher brief plus final local summary-manifest hardening

status:
- completed

result:
- added a full researcher-facing blocker brief at:
  - `docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md`
- the brief consolidates:
  - current blocker
  - exact positive and negative windows
  - failed shell-anchor evidence
  - weak OCR evidence
  - wrong-surface Vista exclusions
  - exact success condition for the next researcher packet
- tightened the remaining summary-only `clip_registry` ingestors so malformed persisted object/list fields fail closed:
  - `hook_evaluation_report`
  - `shadow_ranking_model`
  - `shadow_evaluation_policy`
  - `real_artifact_intake_dashboard`
- added grouped regression coverage proving invalid summary manifests emit:
  - `invalid_hook_evaluation_report_shape`
  - `invalid_shadow_ranking_model_shape`
  - `invalid_shadow_evaluation_policy_shape`
  - `invalid_real_artifact_intake_dashboard_shape`
  and do not generate durable rows

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- the local `clip_registry` hardening pass is now materially exhausted
- the main remaining deferred problem is external evidence for the `call_of_duty` top-right anchor branch

## 2026-06-05T11:01Z

target:
- final `clip_registry` durable-ingestion parity check

status:
- completed

result:
- added a full researcher-facing blocker brief at:
  - `docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md`
- closed the last remaining permissive durable `clip_registry` ingestor:
  - `shadow_ranking_experiment`
- `shadow_ranking_experiment` now fails closed on malformed summary object fields:
  - invalid `filters`
  - invalid `comparison_recommendation`
  - invalid `training_metrics`
  - invalid `evaluation_metrics`
  - invalid `comparison_summary`
- extended grouped regression coverage so invalid shadow-ranking experiment manifests emit:
  - `invalid_shadow_ranking_experiment_shape`
  and do not create durable rows
- verified that every current `clip_registry` `_ingest_*` path now has explicit invalid-shape rejection rather than silently normalizing malformed persisted payloads

verification:
- `.venv/bin/python -m unittest tests.test_clip_registry`
- `python3 run.py --run-repo-quality-health`

notes:
- the remaining blocked problem is unchanged: external evidence for the `call_of_duty` top-right anchor branch
- there is no comparable local `clip_registry` durable-ingestion gap left after this slice

## 2026-06-05T11:10Z

target:
- researcher unblock handoff packaging

status:
- completed

result:
- added a direct researcher-facing unblock handoff at:
  - `docs/handoffs/2026-06-05-project-researcher-unblock-handoff.md`
- the handoff compresses the active blocker into:
  - what is already exhausted
  - what the researcher should read first
  - what output format will actually unblock Codex
  - what output will not help
- the handoff is intended to be sent alongside:
  - `docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md`

verification:
- not run; doc-only packaging change

notes:
- this is not a new workflow surface
- it is a delivery wrapper for the already-blocked external evidence problem

## 2026-06-05T11:24Z

target:
- curated packet verification after wrong-surface researcher upload report

status:
- completed

result:
- verified that the correct local curated packet exists at:
  - `outputs/research_packets/call_of_duty/wiki_curated_20260526t233955z/`
- verified the exact requested local files:
  - `call_of_duty_wiki_curated_20260526t233955z_events_or_medals.csv`
  - `call_of_duty_wiki_curated_20260526t233955z_assets.csv`
- confirmed the curated `events_or_medals.csv` has:
  - `15` rows
  - curated medal seeds only
- confirmed the previously rejected wrong-surface Warzone event rows are absent:
  - `Armor Plate Bundle`
  - `Cash Drop`
  - `Cluster Strike`
  - `Heavy Weapons Crate`
  - `Jailbreak`
  - `Restock`
  - `Resurgence`
  - `Titan Frenzy`
- added a durable verification note at:
  - `docs/handoffs/2026-06-05-call-of-duty-curated-packet-verification.md`

verification:
- direct local file inspection only

notes:
- this indicates an external upload-selection mistake, not a bad local curated export

## 2026-06-05T12:02Z

target:
- operator-safe research packet naming/export hardening

status:
- completed

result:
- changed `export_wiki_research_packet` to emit semantic-first packet roots and filenames instead of timestamp-first generic names
- added packet-side identity artifacts:
  - `*_packet_identity.json`
  - `*_SEND_THESE_FILES_FIRST.txt`
- added fallback semantic inference for older curated bundles that lack `curation_summary.json`
- re-exported the real `call_of_duty` curated packet to:
  - `outputs/research_packets/call_of_duty/curated_medal_seed_packet__call_of_duty__20260526t233955z/`
- the exported packet now recommends the exact first-send files:
  - `curated_medal_seed_packet__call_of_duty__20260526t233955z_events_or_medals.csv`
  - `curated_medal_seed_packet__call_of_duty__20260526t233955z_assets.csv`
- updated curated packet verification docs to point at the operator-safe packet path

verification:
- `.venv/bin/python -m unittest tests.test_wiki_medal_curation`
- `python3 run.py --run-repo-quality-health`
- `python3 run.py --export-wiki-research-packet assets/games/call_of_duty/drafts/wiki_curated/20260526T233955Z`

notes:
- this hardening specifically addresses the first observed wrong-packet upload failure mode

## 2026-06-05T12:14Z

target:
- packet identity guidance propagation

status:
- completed

result:
- updated CLI help for `--export-wiki-research-packet` to describe semantic-first handoff files and packet identity artifacts
- updated `RESEARCHER_INPUT_CONTRACT.md` with a packet identity rule:
  - semantic-first selection
  - timestamp as provenance, not primary identity
  - use `*_packet_identity.json` and `*_SEND_THESE_FILES_FIRST.txt`
- updated `custom_gpt_knowledge/UPLOAD_ROUTING.md` so researcher-side routing explicitly checks packet identity artifacts instead of assuming the newest timestamped folder is correct

verification:
- not run; help-text and doc-only change

notes:
- this keeps the new export contract aligned across code and researcher-facing guidance

## 2026-06-05T12:23Z

target:
- research packet template alignment for exported bundle identity

status:
- completed

result:
- updated `RESEARCH_PACKET_TEMPLATE.md` so exported local bundles must be referenced by:
  - semantic-first packet root
  - exact first-send files
  - packet identity artifacts when present
- added an explicit example using the `call_of_duty` curated medal-seed packet path

verification:
- not run; doc-only change

notes:
- this places the packet identity rule on the packet-authoring surface itself, not only in routing and contract docs

## 2026-06-05T12:33Z

target:
- runner-level research packet export contract coverage

status:
- completed

result:
- added a positive integration test in `tests/test_run.py` for `run_export_wiki_research_packet`
- the runner contract now explicitly verifies:
  - semantic `packet_identity`
  - semantic-first `packet_root`
  - `recommended_handoff_files`
  - `packet_identity_json` artifact
  - `handoff_note_txt` artifact
- this closes the remaining test-layer gap between:
  - exporter unit coverage in `tests/test_wiki_medal_curation.py`
  - runner-facing contract behavior in `tests/test_run.py`

verification:
- `.venv/bin/python -m unittest tests.test_run.RunTests.test_run_export_wiki_research_packet_returns_semantic_identity_fields tests.test_wiki_medal_curation`
- `python3 run.py --run-repo-quality-health`

notes:
- no further concrete packet-identity workflow gap is currently visible after this slice

## 2026-06-05T12:48Z

target:
- pipeline current-state refresher memo

status:
- completed

result:
- added a compact current-state refresher at:
  - `docs/handoffs/2026-06-05-pipeline-current-state-refresher.md`
- the memo separates:
  - project scope
  - pipeline stages
  - mechanically proven path
  - evidence-limited areas
  - current blockers
  - source inventory reality
  - highest-value next confidence improvements

verification:
- not run; doc-only summary built from canonical docs and operator-pack truth

notes:
- intended as a user-facing refresher on what the project is, what has been built, and what remains uncertain

## 2026-06-05T13:15Z

target:
- strategic roadmap alignment with current operator-pack truth

status:
- completed

result:
- updated `FUTURE_FEATURES_ROADMAP.md` so the strategic roadmap now reflects:
  - the bounded local `call_of_duty` runtime-to-export path is mechanically proven
  - the current blocker is evidence quality for the next repeatable native runtime surface
  - `reward_banner` is capped as a narrow pilot
  - the first top-right event-card shell anchor is falsified
  - Phases 2 through 4 are now primarily hardening and coverage-expansion work, not greenfield pipeline construction

verification:
- not run; roadmap and run-log only change

notes:
- this keeps the strategic roadmap from lagging behind `EXECUTION_TARGET.md` and the current operator queue

## 2026-06-05T14:04Z

target:
- stale research-packet sibling deprecation for semantic-first exports

status:
- completed

result:
- updated `export_wiki_research_packet` so a newly exported semantic packet marks older sibling exports from the same source bundle as superseded instead of leaving them equally uploadable
- added regression coverage proving a stale `curated_wiki_packet__...` sibling is rewritten with:
  - `superseded_status: do_not_upload`
  - replacement packet identity and packet root
  - `SUPERSEDED_DO_NOT_UPLOAD.txt`
- re-exported the real `call_of_duty` curated packet and confirmed the older `curated_wiki_packet__call_of_duty__20260526t233955z` root is now explicitly deprecated in favor of `curated_medal_seed_packet__call_of_duty__20260526t233955z`

verification:
- `.venv/bin/python -m unittest tests.test_wiki_medal_curation tests.test_run.RunTests.test_run_export_wiki_research_packet_returns_semantic_identity_fields`
- `python3 run.py --run-repo-quality-health`
- `python3 run.py --export-wiki-research-packet assets/games/call_of_duty/drafts/wiki_curated/20260526T233955Z`

notes:
- this closes the remaining local operator hazard from the old timestamp-first packet naming failure without deleting provenance-bearing generated bundles

## 2026-06-05T14:12Z

target:
- researcher-facing superseded-packet routing rule propagation

status:
- completed

result:
- updated researcher-facing guidance so exported research bundles must reject any packet root marked by:
  - `SUPERSEDED_DO_NOT_UPLOAD.txt`
  - `superseded_status: do_not_upload`
- propagated the rule into:
  - `docs/v2/custom_gpt_knowledge/UPLOAD_ROUTING.md`
  - `docs/v2/RESEARCHER_INPUT_CONTRACT.md`
  - `docs/v2/RESEARCH_PACKET_TEMPLATE.md`

verification:
- not run; doc-only propagation after code and test validation

notes:
- this makes the stale-bundle rejection rule explicit on both the exporter side and the researcher packet-authoring side

## 2026-06-05T14:28Z

target:
- custom-gpt `call_of_duty` packet prompt alignment with current blocker

status:
- completed

result:
- rewrote the dormant `CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md` and `CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md` so they no longer present `reward_banner` or generic text-banner work as the live next branch
- updated `LAUNCH_SEQUENCE.md`, `BUILDER_CHECKLIST.md`, and `UPLOAD_ROUTING.md` so the researcher-facing default `call_of_duty` packet path now points to the top-right anchor brief
- the text-banner files now act as dormant-branch references only, to be used if new evidence explicitly reopens that line

verification:
- not run; doc-only routing and prompt correction

notes:
- this removes another stale researcher entrypoint that could have recreated the retired `reward_banner` or text-banner-first branch

## 2026-06-05T14:36Z

target:
- custom-gpt pack index alignment for the active `call_of_duty` blocker

status:
- completed

result:
- updated `docs/v2/custom_gpt_knowledge/README.md` so the pack contents and recommended upload set now:
  - include the top-right anchor researcher brief as the active `call_of_duty` blocker surface
  - mark the text-banner brief and stub as dormant-branch references only

verification:
- not run; doc-only index alignment

notes:
- this closes the remaining stale index-level surface in the current researcher-routing lane

## 2026-06-05T14:45Z

target:
- stale researcher-contract and template cleanup for the active `call_of_duty` blocker

status:
- completed

result:
- updated `RESEARCHER_INPUT_CONTRACT.md` so the highest-priority `call_of_duty` packet is now the top-right anchor packet rather than a text or reward-banner packet
- updated `IMPLEMENTATION_TICKET_TEMPLATE.md` so its example no longer uses the retired `reward_banner` branch as the default `call_of_duty` implementation example

verification:
- not run; doc-only contract and template correction

notes:
- this removes the last materially stale researcher-facing instructions found in the current `call_of_duty` routing lane

## 2026-06-05T14:55Z

target:
- stale backlog phase cleanup for `call_of_duty` signal targeting

status:
- completed

result:
- updated `CODEX_BACKLOG.md` so `Phase 8: Post-Promotion Signal Targeting` is no longer marked active
- the phase now explicitly reads as historical context that has been superseded by the current top-right anchor branch in the active queue

verification:
- not run; doc-only backlog correction

notes:
- this prevents the backlog from presenting two different active `call_of_duty` blocker stories at once

## 2026-06-05T15:03Z

target:
- final operator and fallback-handoff alignment for the active `call_of_duty` blocker

status:
- completed

result:
- updated `EXECUTION_TARGET.md` so its current governance limit now reflects the live top-right anchor blocker instead of the older medal-vs-text discriminator framing
- updated `2026-05-27-call-of-duty-medal-visible-replacement-samples-request.md` so it is explicitly marked as a historical fallback rather than a live first-path packet

verification:
- not run; doc-only operator and handoff correction

notes:
- this removes one remaining operator-pack mismatch and one remaining historical handoff that still read like current guidance

## 2026-06-05T16:40Z

target:
- bounded Codex heartbeat implementation plus local conversation archive contract

status:
- completed

result:
- added a local conversation archive toolchain with:
  - archive record capture
  - topic-batch append and rotation
  - local archive ledger persistence
  - uploaded/superseded batch state updates
- added durable operator/archive docs for:
  - heartbeat stop/defer rules
  - conversation archive batching policy
  - archive topic taxonomy
  - archive ledger contract
  - heartbeat and archive automation prompts
- created one active thread heartbeat automation for bounded local repo work

verification:
- `.venv/bin/python -m unittest tests.test_conversation_archive tests.test_run.RunTests.test_run_conversation_archive_flow_records_batches_and_marks_upload tests.test_run.RunTests.test_run_record_conversation_archive_returns_invalid_status_for_missing_body`
- `python3 run.py --run-repo-quality-health`
- result: `OK` / `Ok: True`

notes:
- the desktop app currently allows only one active heartbeat automation per thread
- the repo-side archive system is implemented, but the recurring archive worker remains constrained by that platform limit for thread-attached context capture

## 2026-06-05T17:05Z

target:
- compact operator inspector for the conversation archive ledger

status:
- completed

result:
- added `tools/inspect_conversation_archive_ledger.py`
- added CLI support in `run.py` for `--inspect-conversation-archive-ledger`
- added focused tests for compact rendering, JSON mode, and invalid-ledger failure
- linked the inspector from the archive ledger contract doc

verification:
- `.venv/bin/python -m unittest tests.test_inspect_conversation_archive_ledger tests.test_run.RunTests.test_run_inspect_conversation_archive_ledger_renders_compact_summary`
- `python3 run.py --run-repo-quality-health`
- result: `OK` / `Ok: True`

notes:
- the archive workflow now has the same compact-inspector posture used by the repo's other ledger-driven operator surfaces

## 2026-06-05T17:22Z

target:
- deterministic publication-prep helper for closed conversation archive batches

status:
- completed

result:
- added `tools/prepare_conversation_archive_upload.py`
- added CLI support in `run.py` for `--prepare-conversation-archive-upload`
- the helper now turns a `closed_pending_upload` ledger row into one upload-ready manifest with:
  - batch markdown path
  - suggested semantic-first Google Docs title
  - suggested Drive folder path
  - conversation/date-range summary
- updated archive docs so the automation prompt and ledger contract point at the helper explicitly

verification:
- `.venv/bin/python -m unittest tests.test_prepare_conversation_archive_upload tests.test_run.RunTests.test_run_prepare_conversation_archive_upload_emits_manifest`
- `python3 run.py --run-repo-quality-health`
- result: `OK` / `Ok: True`

notes:
- this closes the last local gap between closed archive batches and a deterministic Google Docs upload handoff

## 2026-06-05T17:36Z

target:
- Google Docs source materializer for prepared conversation archive uploads

status:
- completed

result:
- added `tools/materialize_conversation_archive_doc_source.py`
- added CLI support in `run.py` for `--materialize-conversation-archive-doc-source`
- the helper now converts a prepared archive upload manifest into:
  - one Google Docs-importable text source
  - one doc-source manifest carrying the suggested title and folder routing
- updated archive docs so the automation flow now points at:
  - upload manifest preparation
  - doc-source materialization
  - then Drive import

verification:
- `.venv/bin/python -m unittest tests.test_materialize_conversation_archive_doc_source tests.test_run.RunTests.test_run_materialize_conversation_archive_doc_source_emits_text_file`
- `python3 run.py --run-repo-quality-health`
- result: `OK` / `Ok: True`

notes:
- the repo now has a complete local path from conversation archive record to Google Docs-importable source material

## 2026-06-05T17:55Z

target:
- real Google Docs import check for the archive publication path plus upload-manifest inspection support

status:
- partial_external_blocker

result:
- exercised the local publication path against a real demo archive batch:
  - created demo archive records
  - created a `closed_pending_upload` exception batch
  - prepared an upload manifest
  - materialized a Google Docs-importable text source
- attempted a real Google Drive import from the generated text source
- the Drive import failed with external auth error:
  - `token_expired`
  - `Provided authentication token is expired. Please try signing in again.`
- added `tools/inspect_conversation_archive_upload_manifest.py`
- added CLI support in `run.py` for `--inspect-conversation-archive-upload-manifest`
- updated the archive ledger contract doc to include the upload-manifest inspector

verification:
- `.venv/bin/python -m unittest tests.test_inspect_conversation_archive_upload_manifest tests.test_run.RunTests.test_run_inspect_conversation_archive_upload_manifest_renders_compact_summary`
- `python3 run.py --run-repo-quality-health`
- result: `OK` / `Ok: True`

notes:
- the repo-side archive publication path is locally complete through upload-ready source material
- the next external step is to refresh Google Drive authentication, then retry `_import_document` on the prepared text source

## 2026-06-05T18:15Z

target:
- expose the existing archive publication-queue reporter through the canonical runner surface

status:
- completed

result:
- wired `tools/report_conversation_archive_publication_queue.py` into `run.py`
- added CLI support for `--report-conversation-archive-publication-queue`
- added one run-level regression covering the ready-for-import path
- updated the archive ledger contract doc with the publication-queue command

verification:
- `.venv/bin/python -m unittest tests.test_report_conversation_archive_publication_queue tests.test_run.RunTests.test_run_report_conversation_archive_publication_queue_renders_ready_status`
- `python3 run.py --run-repo-quality-health`

notes:
- this closes the local operator gap between raw archive ledger state and the external Drive import step

## 2026-06-05T18:28Z

target:
- harden the archive upload-mark contract so the ledger cannot claim published state prematurely

status:
- completed

result:
- `mark_conversation_archive_uploaded()` now fails closed unless:
  - the batch status is `closed_pending_upload`
  - `drive_doc_id` is non-empty
  - `drive_url` is non-empty
  - `measured_pages` is positive when provided
- updated archive unit and run-level tests to reflect the stricter contract
- documented the upload-mark rule in the archive ledger contract

verification:
- `.venv/bin/python -m unittest tests.test_conversation_archive tests.test_run.RunTests.test_run_conversation_archive_flow_records_batches_and_marks_upload tests.test_run.RunTests.test_run_mark_conversation_archive_uploaded_rejects_open_batch`
- `python3 run.py --run-repo-quality-health`

notes:
- this reduces the chance of recording a false uploaded state before the external Drive import is actually complete

## 2026-06-05T18:40Z

target:
- harden the archive supersede contract so replacement lineage cannot collapse into empty or self-referential values

status:
- completed

result:
- `supersede_conversation_archive_batch()` now fails closed unless:
  - `superseded_by` is non-empty
  - `superseded_by` does not equal the current `batch_id`
- added archive unit coverage for empty and self-referential replacement ids
- added one run-level regression for the empty replacement-id case
- documented the supersede rule in the archive ledger contract

verification:
- `.venv/bin/python -m unittest tests.test_conversation_archive tests.test_run.RunTests.test_run_supersede_conversation_archive_batch_rejects_empty_replacement_id`
- `python3 run.py --run-repo-quality-health`

notes:
- this keeps replacement lineage explicit and prevents a malformed supersede operation from mutating durable archive state

## 2026-06-05T21:10Z

target:
- make the archive publication-queue report distinguish superseded batches from generic not-ready rows

status:
- completed

result:
- `report_conversation_archive_publication_queue()` now emits `publication_status: superseded` for superseded ledger rows
- added focused coverage proving superseded batches render explicitly in the compact report

verification:
- `.venv/bin/python -m unittest tests.test_report_conversation_archive_publication_queue`
- `python3 run.py --run-repo-quality-health`

notes:
- this keeps the publication queue readable when rebuilt or replaced batches are still present in durable ledger history

## 2026-06-06T00:10Z

target:
- harden the archive upload-manifest inspector so it rejects wrong-schema payloads instead of only checking field presence

status:
- completed

result:
- `inspect_conversation_archive_upload_manifest()` now validates `schema_version` explicitly against `conversation_archive_upload_manifest_v1`
- added focused coverage proving a wrong-schema payload fails clearly

verification:
- `.venv/bin/python -m unittest tests.test_inspect_conversation_archive_upload_manifest`
- `python3 run.py --run-repo-quality-health`

notes:
- this keeps archive inspection aligned with the same explicit schema checks already enforced by the materialization path

## 2026-06-06T03:10Z

target:
- harden the archive doc-source materializer so upload-manifest type errors fail clearly before file generation

status:
- completed

result:
- `materialize_conversation_archive_doc_source()` now validates:
  - `conversation_ids` is a list
  - `batch_markdown_path` is non-empty
  - `suggested_drive_folder` is non-empty
  - `suggested_doc_title` is non-empty
- added focused coverage proving malformed upload-manifest field types fail before materialization

verification:
- `.venv/bin/python -m unittest tests.test_materialize_conversation_archive_doc_source`
- `python3 run.py --run-repo-quality-health`

notes:
- this keeps the doc-source generation contract aligned with the fail-closed posture already applied to the other archive publication steps

## 2026-06-06T06:10Z

target:
- harden archive upload preparation so malformed ledger-row shapes fail before manifest generation

status:
- completed

result:
- `prepare_conversation_archive_upload()` now validates:
  - `conversation_ids` is a list
  - `record_paths` is a list
  - `topic` is non-empty
  - `local_batch_markdown_path` is non-empty
- added focused coverage proving malformed ledger-row shape fails with `invalid_batch_shape`

verification:
- `.venv/bin/python -m unittest tests.test_prepare_conversation_archive_upload`
- `python3 run.py --run-repo-quality-health`

notes:
- this keeps upload-manifest generation aligned with the same fail-closed archive contract used by inspection and doc-source materialization

## 2026-06-06T09:10Z

target:
- harden the archive ledger inspector so it rejects wrong-schema payloads explicitly instead of relying only on missing-field checks

status:
- completed

result:
- `inspect_conversation_archive_ledger()` now validates `schema_version` explicitly against `conversation_archive_ledger_v1`
- added focused coverage proving a wrong-schema ledger payload fails clearly

verification:
- `.venv/bin/python -m unittest tests.test_inspect_conversation_archive_ledger`
- `python3 run.py --run-repo-quality-health`

notes:
- this aligns the ledger inspector with the same explicit schema-check posture now used across the archive inspection and publication helpers

## 2026-06-06T18:10Z

target:
- post-heartbeat reassessment after the archive publication-contract defer state

status:
- deferred_external_blocker

result:
- repo truth is unchanged from the previous defer checkpoint
- no new bounded local task currently clears the usefulness threshold without drifting into low-yield archive tightening
- the remaining material blockers are still:
  - external researcher evidence for the `call_of_duty` top-right anchor branch
  - external Google Drive authentication refresh for real archive import

verification:
- none

notes:
- the recurring heartbeat was already retired
- this entry records that the same defer decision still holds under the current repo state

## 2026-06-06T12:10Z

target:
- heartbeat reassessment after the archive publication-contract hardening pass

status:
- deferred_external_blocker

result:
- no further bounded local archive task remains above the current usefulness threshold
- the remaining material blockers are unchanged:
  - external researcher evidence for the `call_of_duty` top-right anchor branch
  - external Google Drive authentication refresh for real archive import

verification:
- none

notes:
- the archive lane has reached diminishing returns for heartbeat-sized local hardening slices
- the recurring heartbeat should be retired until one of the external blockers changes

## 2026-06-08T10:15Z

target:
- harden the archive publication-queue report so malformed ledger-row shapes fail explicitly instead of being summarized loosely

status:
- completed

result:
- `report_conversation_archive_publication_queue()` now validates:
  - each ledger row is a mapping
  - each row `conversation_ids` field is a list
- added focused coverage proving malformed ledger-row shape fails clearly

verification:
- `.venv/bin/python -m unittest tests.test_report_conversation_archive_publication_queue`
- `python3 run.py --run-repo-quality-health`

notes:
- this aligns the publication-queue report with the stricter fail-closed posture already used by the other archive inspection and publication helpers

## 2026-06-08T10:40Z

target:
- harden the archive publication-queue report so required row identifiers fail explicitly before derived path inspection

status:
- completed

result:
- `report_conversation_archive_publication_queue()` now validates:
  - each row `batch_id` is non-empty
  - each row `topic` is non-empty
  - each row `status` is non-empty
- added focused coverage proving empty required row identifiers fail clearly

verification:
- `.venv/bin/python -m unittest tests.test_report_conversation_archive_publication_queue`
- `python3 run.py --run-repo-quality-health`

notes:
- this prevents the queue report from deriving misleading artifact paths from malformed ledger rows
