# Research Draft Note Template

Use this template when `Pipeline Research and Development` hands off work to `Pipeline Architecture and Troubleshooting`.

This file is the upload-ready template for the routed exploratory artifact `research_note`.

This is a draft-first format. It is useful input, not canonical repo truth by itself.
Do not use this as the primary handoff when the work is meant to directly drive the next repo action. In that case, use the packet contract from:

- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md)

## Template

```text
topic:
  One sentence naming the decision, tool, workflow, or concept being researched.

source_set:
  A compact list of the sources actually used.
  Include URLs, repo names, paper titles, or document names.

key_findings:
  3-7 factual findings from the source set.
  Keep them concrete and attributable.

project_relevance:
  Why these findings matter for this gameplay highlight pipeline specifically.
  Do not restate the findings; explain the relevance.

recommended_implications:
  The tentative architecture, workflow, or implementation implications.
  These are draft implications, not final repo decisions.

uncertainties:
  What remains unclear, unverified, or environment-dependent.

follow_up_questions:
  The next questions the architecture/troubleshooting GPT or operator should resolve.
```

## Rules

- treat this template as exploratory or appendix-like support, not the default repo-changing output
- separate observed facts from inferred implications
- include only the sources actually consulted
- avoid broad recap prose
- do not pretend promotion to canonical truth
- if a point is speculative, move it to `uncertainties`
- if a claim matters architecturally, make sure the source set is specific enough to revisit later

## Example Skeleton

```text
topic:
  Whether LangGraph or a simpler local orchestrator is the smallest practical control plane for research-runtime v1.

source_set:
  - LangGraph docs page: <url>
  - Repo note: ResearchAgent
  - Local runtime files reviewed: pipeline/research_runtime/*

key_findings:
  - LangGraph is strongest when durable state and bounded multi-step orchestration are needed.
  - The repo already has a staged research-runtime contract with checkpoints and retries.
  - The current local fallback executor already covers some v1 needs without full provider coupling.

project_relevance:
  The project benefits from explicit state, retries, and checkpoints, but does not yet need broad multi-agent orchestration.

recommended_implications:
  - keep LangGraph as the intended control-plane abstraction
  - preserve optional dependency posture
  - avoid expanding to planner subgraphs yet

uncertainties:
  - real provider-backed latency and trace verbosity are not yet measured
  - DSPy optimization needs are still unproven

follow_up_questions:
  - should the first real provider run become a saved regression fixture?
  - how much trace detail is needed for operator review?
```
