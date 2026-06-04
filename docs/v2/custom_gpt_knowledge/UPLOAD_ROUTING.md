# Upload Routing

This file defines which knowledge files belong with which GPT and how to route draft notes between them.

## GPT Assignment

### Pipeline Research and Development

Best inputs:

- messy Google Drive notes
- browser research findings
- repo/paper/tool comparisons
- external docs and references
- [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [INTERACTION_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/INTERACTION_ROUTING.md)
- [RESEARCH_DRAFT_NOTE_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_DRAFT_NOTE_TEMPLATE.md)
- [RESEARCH_PACKET_APPENDIX_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_PACKET_APPENDIX_TEMPLATE.md)
- [IMPLEMENTATION_TICKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/IMPLEMENTATION_TICKET_TEMPLATE.md)
- [CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md) when the current blocker is `call_of_duty` medal coverage
- [CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md) when the current blocker is text or reward-banner visibility on the measured `call_of_duty` clips

Do not overload it with:

- every repo doc
- raw runtime code dumps
- stale planning files with overlapping authority

Default expectation:

- GPT 1 should return one decision-ready packet that supports one next repo action.
- GPT 1 may return one appendix only when overflow evidence is needed.
- internal state-classification or governance systems may guide GPT 1 reasoning, but should not appear in the external artifact unless explicitly requested.
- GPT 1 should choose the target artifact before choosing the next interaction move.
- If a packet is broad, mixed-layer, or missing exclusions and acceptance targets, it is not ready for Codex.

### Pipeline Architecture and Troubleshooting

Best inputs:

- canonical repo docs
- reviewed research draft notes
- runtime artifacts
- logs and tracebacks
- operator outputs
- [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
- [CODEX_HANDOFF_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CODEX_HANDOFF_TEMPLATE.md)
- [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md)
- [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
- [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)

Do not rely on:

- unreviewed browser research as if it were canonical repo truth
- broad “AI best practices” that conflict with local repo contracts

## Handoff Rule

The research GPT should hand off structured research packets by default when the work is meant to change the repo.

When Codex has exported a researcher packet bundle locally, choose the packet by semantic identity first, not by timestamp alone.

For exported wiki research packets:

- use the semantic-first packet root and filenames
- check `*_packet_identity.json` for what the packet is
- check `*_SEND_THESE_FILES_FIRST.txt` for the exact first-send files

Do not assume that the newest timestamped folder is the right bundle.

The routing doctrine also allows these internal target artifacts when they are the smallest truthful next output:

- `research_note`
- `decision_ready_packet`
- `implementation_ticket`
- `codex_handoff_brief`

Required packet sections:

- `Decision Target`
- `Current Repo Truth`
- `Evidence Bundle`
- `Structured Findings`
- `Recommendation`
- `Acceptance Target`
- `Open Uncertainties`

Required draft-note sections are still:

- `topic`
- `source_set`
- `key_findings`
- `project_relevance`
- `recommended_implications`
- `uncertainties`
- `follow_up_questions`

When the task is meant to directly drive Codex implementation, use the packet shape from:

- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md)

Use the appendix template only for overflow support:

- [RESEARCH_PACKET_APPENDIX_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_PACKET_APPENDIX_TEMPLATE.md)

Use the implementation-ticket template when implementation-facing task framing is needed but the work is not yet a direct Codex handoff:

- [IMPLEMENTATION_TICKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/IMPLEMENTATION_TICKET_TEMPLATE.md)

The architecture/troubleshooting GPT should:

1. accept the note as useful input
2. verify its implications against repo-local truth
3. reject or narrow any implication that conflicts with local contracts
4. convert the usable result into a plan, debug path, or Codex handoff

## Promotion Rule

Draft research notes do not become canonical automatically.

Promotion should happen only when:

- the note has been reviewed
- the implications have been checked against repo reality
- the durable truth has been written to the right canonical surface

In most cases, that means:

- stable architecture truth goes to `docs/v2/`
- repeated procedures go to skills or tooling
- implementation truth goes to code, tests, manifests, or runtime artifacts
