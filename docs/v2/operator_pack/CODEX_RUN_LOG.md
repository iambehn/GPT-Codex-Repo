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
