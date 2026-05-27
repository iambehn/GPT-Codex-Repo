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
- [RESEARCH_DRAFT_NOTE_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_DRAFT_NOTE_TEMPLATE.md)
- [CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md) when the current blocker is `call_of_duty` medal coverage
- [CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md) when the current blocker is text or reward-banner visibility on the measured `call_of_duty` clips

Do not overload it with:

- every repo doc
- raw runtime code dumps
- stale planning files with overlapping authority

Default expectation:

- GPT 1 should return one decision-ready packet that supports one next repo action.
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

The research GPT should hand off structured draft notes or structured research packets only.

Required sections:

- `topic`
- `source_set`
- `key_findings`
- `project_relevance`
- `recommended_implications`
- `uncertainties`
- `follow_up_questions`

When the task is meant to directly drive Codex implementation, prefer the packet shape from:

- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md)

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
