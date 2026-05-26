# Call Of Duty Medal Research Request

Date: 2026-05-26
Status: active

## Request

Produce a medal-specific research packet for `call_of_duty` gameplay HUD medals.

Use:

- [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md)
- [CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md)

## Supporting Repo Evidence

The researcher should read these first:

- [2026-05-25-call-of-duty-wiki-source-audit.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-25-call-of-duty-wiki-source-audit.md)
- [2026-05-25-call-of-duty-pre-bridge-curation-findings.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-25-call-of-duty-pre-bridge-curation-findings.md)
- [2026-05-25-call-of-duty-cross-clip-measurement-audit.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-25-call-of-duty-cross-clip-measurement-audit.md)

## Decision Target

Identify the first true gameplay HUD medal subset worth promoting into onboarding review.

## Priority Subset

Prefer:

- multikill medals
- elimination-streak medals
- payoff or victory medals

## Explicit Exclusions

Exclude:

- contracts
- intel missions
- calling cards
- weapon blueprints
- watches
- logos
- operator skins
- map imagery
- season branding
- game-mode branding

## Expected Outcome

The packet should be specific enough that Codex can:

1. build or curate a narrower medal source set
2. bridge it into onboarding
3. validate real `medal_icon` candidates
4. rerun runtime measurement on the existing `call_of_duty` sample set
