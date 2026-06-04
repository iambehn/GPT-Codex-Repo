# Custom GPT Knowledge Pack

This directory is the upload-ready knowledge pack for the repo's two Custom GPTs.

Use it for:

- selecting which files each GPT should receive
- giving both GPTs one compact shared project brief
- standardizing the handoff note shape between the research GPT and the architecture/troubleshooting GPT
- standardizing Codex-ready handoff formatting

Do not use it as a replacement for the canonical V2 docs.

The canonical truth still lives in:

- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [INTERACTION_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/INTERACTION_ROUTING.md)
- [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)
- [RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md)
- [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md)
- [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
- [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)

## What This Pack Is For

Use this pack to help the GPT workers do four things well:

- turn messy notes or web findings into decision-ready packets
- keep researcher output aligned to the repo's packet contract
- hand work cleanly from the researcher GPT to Codex
- give the architecture/troubleshooting GPT a compact repo-aware context set

This pack is not for:

- replacing canonical repo truth
- storing live execution state
- acting as a second roadmap or backlog
- preserving large theory dumps or stale planning notes

## What To Update And When

Update this pack when:

- the researcher GPT should ingest a different set of files
- the researcher-to-Codex artifact contract changes
- the builder or launch workflow changes
- a blocker-specific brief or stub needs sharper evidence or instructions

Do not update this pack just because:

- a local execution state changed
- a run log entry changed
- a dashboard field changed
- a new theory artifact exists but has not changed packet quality or operator behavior

## ML And Learned-System Scope

This pack may support ML-adjacent work, but only in bounded forms such as:

- dataset-readiness packets
- label-quality packets
- shadow-model evaluation packets
- learned-component adoption or retirement decisions

Do not use the pack as a generic "manage the ML system" surface.

The roadmap still treats learned systems as later-phase work. Until those phases become active, prefer:

- runtime and fusion contract hardening
- review and calibration release-gate clarity
- training-readiness and label capture only when a packet or experiment explicitly needs them

## Pack Contents

- [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [INTERACTION_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/INTERACTION_ROUTING.md)
- [RESEARCH_DRAFT_NOTE_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_DRAFT_NOTE_TEMPLATE.md)
- [RESEARCH_PACKET_APPENDIX_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_PACKET_APPENDIX_TEMPLATE.md)
- [IMPLEMENTATION_TICKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/IMPLEMENTATION_TICKET_TEMPLATE.md)
- [CODEX_HANDOFF_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CODEX_HANDOFF_TEMPLATE.md)
- [CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md)
- [CALL_OF_DUTY_MEDAL_PACKET_STUB.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_STUB.md)
- [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md)
- [CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md) (dormant branch only)
- [CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md) (dormant branch only)
- [UPLOAD_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/UPLOAD_ROUTING.md)
- [BUILDER_CHECKLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/BUILDER_CHECKLIST.md)
- [LAUNCH_SEQUENCE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/LAUNCH_SEQUENCE.md)

## Recommended Upload Sets

### Pipeline Research and Development

Upload:

- [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [INTERACTION_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/INTERACTION_ROUTING.md)
- [RESEARCH_DRAFT_NOTE_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_DRAFT_NOTE_TEMPLATE.md)
- [RESEARCH_PACKET_APPENDIX_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_PACKET_APPENDIX_TEMPLATE.md)
- [IMPLEMENTATION_TICKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/IMPLEMENTATION_TICKET_TEMPLATE.md)
- [CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md)
- [CALL_OF_DUTY_MEDAL_PACKET_STUB.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_STUB.md)
- [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md) for the active `call_of_duty` blocker
- [CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md) only if new evidence explicitly reopens the dormant text/banner branch
- [CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md) only if new evidence explicitly reopens the dormant text/banner branch
- [UPLOAD_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/UPLOAD_ROUTING.md)
- reviewed or provisional browser/Drive notes you want normalized

Optional supporting uploads:

- [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md)
- [BACKLOG_OPERATING_MODEL.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/BACKLOG_OPERATING_MODEL.md)
- [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)
- [RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md)

### Pipeline Architecture and Troubleshooting

Upload:

- [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
- [IMPLEMENTATION_TICKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/IMPLEMENTATION_TICKET_TEMPLATE.md)
- [CODEX_HANDOFF_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CODEX_HANDOFF_TEMPLATE.md)
- [UPLOAD_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/UPLOAD_ROUTING.md)
- [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)
- [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md)
- [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
- [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)
- reviewed research draft notes from the first GPT

## Operator Rule

Do not upload every repo note blindly.

Default external artifact rule:

- if the work is meant to change the repo, the researcher GPT should return one decision-ready packet
- use an appendix only for overflow evidence
- use the draft-note template only for exploratory material or secondary support

Prefer:

- compact canonical docs
- one project brief
- one note template
- one handoff template
- selected reviewed draft notes

Avoid:

- large unreviewed note dumps
- duplicate copies of the same architecture truth
- stale planning files with overlapping authority
