# ChatGPT Agent Operator Workflow

This document defines the manual operator workflow for using ChatGPT agent mode against the repo's research-agent contract.

Use this workflow when:

- you want a real model run without enabling API billing yet
- you want browser-assisted or file-assisted research inside ChatGPT agent mode
- you still want the output to stay aligned to the dossier, protocol, and evaluator already defined in this repo

Do not use this workflow as a replacement for the repo runtime contract. The source of truth remains:

- [RESEARCH_PROTOCOL.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/RESEARCH_PROTOCOL.md)
- [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)
- [pipeline/research_runtime/dossier.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/research_runtime/dossier.py)

## Purpose

Use ChatGPT agent mode as the manual research worker.

Keep this repo as the control plane for:

- dossier shape
- section order
- section acceptance criteria
- evaluation rules
- durable docs and runtime artifacts

This split is deliberate:

- ChatGPT agent mode handles browsing, reading, and drafting
- the repo defines what a valid research output looks like

## Current Dossier Contract

Default dossier template:

- `pipeline_design_research_v1`

Current section order:

1. `domain_framing`
2. `question_decomposition`
3. `architecture_dataflow`

Current section criteria are defined in:

- [pipeline/research_runtime/dossier.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/research_runtime/dossier.py)

The operating rule remains:

> one turn, one lens, one artifact section, one contract

## Operator Inputs

Before starting a ChatGPT agent run, prepare:

1. A bounded topic.
2. The assigned section for the current turn.
3. The repo files or docs the turn is allowed to use.
4. The acceptance criteria for that section.
5. The output path where you will save the returned JSON envelope.

Recommended output directory pattern:

- `outputs/research_runtime/manual_agent_runs/<timestamp_slug>/`

Recommended per-turn filenames:

- `turn_01_domain_framing.envelope.json`
- `turn_02_question_decomposition.envelope.json`
- `turn_03_architecture_dataflow.envelope.json`

These are operator files, not a new runtime schema.

## Session Setup

Start one ChatGPT agent conversation per dossier run.

Paste the session prompt from:

- [chatgpt_agent_session_prompt.txt](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/research_runtime/prompts/operator/chatgpt_agent_session_prompt.txt)

Then replace the placeholders with:

- the topic
- the assigned section
- the turn lens
- the acceptance criteria
- the allowed repo paths
- any evidence bundle you want the model to use

Do not ask the agent to complete the whole dossier in one response.

## Turn Procedure

For each section:

1. Assign exactly one target section.
2. Provide only the relevant repo paths and evidence for that section.
3. Instruct the agent to return one JSON `TurnOutputEnvelope` and nothing else.
4. Save the JSON output exactly as returned.
5. Inspect the payload for obvious schema drift before moving to the next section.

The response must contain:

- `target_section`
- `artifact_payload`
- `source_refs`
- `runtime_meta`

The response must not contain:

- prose before or after the JSON
- recap summaries
- edits to any other section
- invented keys outside the section contract

## Section-Specific Guidance

### `domain_framing`

Use this turn to bound the pipeline problem and the research goal.

Minimum expected coverage:

- repo-specific `problem_statement`
- bounded `research_goal`
- explicit assumptions
- explicit uncertainties
- explicit failure modes

### `question_decomposition`

Use this turn to define the next research questions.

Minimum expected coverage:

- one `primary_question`
- at least two non-overlapping `subquestions`
- assumptions, uncertainties, and failure modes

### `architecture_dataflow`

Use this turn to map the current pipeline path relevant to the topic.

Minimum expected coverage:

- current pipeline summary
- ordered stages
- crossmodal dependencies
- assumptions, uncertainties, and failure modes

## Local Review Loop

After saving a turn envelope, do a local operator check:

1. Is `target_section` correct?
2. Does the payload clearly stay inside one section?
3. Are `source_refs` specific enough to audit later?
4. Are assumptions, uncertainties, and failure modes explicit?

If not, rerun that turn in ChatGPT agent mode with a tighter instruction.

Recommended retry language:

- "Return JSON only."
- "Do not modify any section except `<section_name>`."
- "Strengthen source specificity and remove speculative language."
- "Satisfy every listed acceptance criterion explicitly."

## Repo Integration Path

This workflow does not yet automatically ingest ChatGPT agent responses into the local runtime.

Until that ingestion surface exists, use ChatGPT agent mode for:

- section drafting
- evidence gathering
- bounded synthesis

Use the local runtime for:

- stubbed protocol verification
- evaluator development
- feedback-loop design

Useful local commands:

```bash
source .venv/bin/activate && python tools/research_runtime_skeleton.py \
  --topic "Bounded pipeline research topic" \
  --output-path outputs/research_runtime/local_stub_run/artifact.json \
  --checkpoint-every 1 \
  --use-stub-adapter
```

```bash
source .venv/bin/activate && python tools/research_runtime_feedback_loop.py \
  --topic "Bounded pipeline research topic" \
  --output-dir outputs/research_runtime/local_feedback_run \
  --use-stub-adapter \
  --max-feedback-loops 1
```

```bash
source .venv/bin/activate && python tools/evaluate_research_runtime_artifacts.py \
  --artifact outputs/research_runtime/local_feedback_run/iteration_1.artifact.json \
  --trace outputs/research_runtime/local_feedback_run/iteration_1.trace.json
```

## Failure Handling

If the ChatGPT agent output is weak, do not smooth it into prose locally.

Instead:

- save the bad output
- mark why it failed
- rerun the same section with stricter instructions

Common failure modes:

- wrong section targeted
- speculative language
- vague citations
- missing uncertainties
- architecture sprawl outside the assigned section

## Recommended First Real Run

Use a narrow topic first.

Recommended shape:

- topic: one bounded design question inside this pipeline
- sections: the default three-section dossier only
- external evidence: minimal
- repo evidence: only the directly relevant docs and modules

That keeps the first manual real-model run inspectable enough to compare against the stub runtime and evaluator.
