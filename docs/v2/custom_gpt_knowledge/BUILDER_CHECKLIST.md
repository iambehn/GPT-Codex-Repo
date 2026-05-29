# Custom GPT Builder Checklist

This is the literal step-by-step checklist for creating the repo's two Custom GPTs in the ChatGPT builder UI.

Use this when you want to:

- create the GPTs in the builder without interpretation
- know exactly which files to upload
- know exactly which instruction file to paste
- run one small smoke test for each GPT immediately after creation

The authority for role design still lives in:

- [RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md)

The authority for file routing still lives in:

- [README.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/README.md)

## Before You Start

Have these files ready:

- GPT 1 instructions:
  - [pipeline_research_and_development_instructions.txt](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/research_runtime/prompts/custom_gpts/pipeline_research_and_development_instructions.txt)
- GPT 2 instructions:
  - [pipeline_architecture_and_troubleshooting_instructions.txt](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/research_runtime/prompts/custom_gpts/pipeline_architecture_and_troubleshooting_instructions.txt)
- shared upload pack:
  - [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
  - [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
  - [INTERACTION_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/INTERACTION_ROUTING.md)
  - [RESEARCH_DRAFT_NOTE_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_DRAFT_NOTE_TEMPLATE.md)
  - [RESEARCH_PACKET_APPENDIX_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_PACKET_APPENDIX_TEMPLATE.md)
  - [IMPLEMENTATION_TICKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/IMPLEMENTATION_TICKET_TEMPLATE.md)
  - [CODEX_HANDOFF_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CODEX_HANDOFF_TEMPLATE.md)
  - [CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md)
  - [CALL_OF_DUTY_MEDAL_PACKET_STUB.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_STUB.md)
  - [UPLOAD_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/UPLOAD_ROUTING.md)
  - [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)
  - [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md)
  - [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
  - [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)

## Product Boundary

Build these GPTs in **ChatGPT**, not in the Codex app.

If you are looking for a place inside Codex to “switch to” a Custom GPT configuration, that is the wrong surface.

Current OpenAI help guidance for GPT creation points to:

- `https://chatgpt.com/create`
- `https://chatgpt.com/gpts/editor`

## GPT 1 Checklist: Pipeline Research Strategist

### Builder Fields

1. Open the GPT builder and create a new GPT.
2. Set the **Name** field to:
   - `Pipeline Research Strategist`
3. Set the **Description** field to:

```text
Research, brainstorming, documentation, and task-specification assistant for an automated gameplay video editing and highlight detection pipeline.
```

4. Paste this file into the **Instructions** field:
   - [pipeline_research_and_development_instructions.txt](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/research_runtime/prompts/custom_gpts/pipeline_research_and_development_instructions.txt)

### Capabilities

Turn **on**:

- Web browsing/search
- File uploads / file analysis
- Code interpreter / data analysis
- Image understanding

Turn **off** or leave disabled:

- Custom actions
- Image generation

### Upload Order

Upload these files in this order:

1. [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
2. [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
3. [INTERACTION_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/INTERACTION_ROUTING.md)
4. [RESEARCH_DRAFT_NOTE_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_DRAFT_NOTE_TEMPLATE.md)
5. [RESEARCH_PACKET_APPENDIX_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/RESEARCH_PACKET_APPENDIX_TEMPLATE.md)
6. [IMPLEMENTATION_TICKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/IMPLEMENTATION_TICKET_TEMPLATE.md)
7. [CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_BRIEF.md)
8. [CALL_OF_DUTY_MEDAL_PACKET_STUB.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_MEDAL_PACKET_STUB.md)
9. [CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_BRIEF.md)
10. [CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md)
11. [UPLOAD_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/UPLOAD_ROUTING.md)
12. [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)
13. [RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md)

Optional upload after initial creation:

- one messy Google Drive export or research note you want normalized
- the active medal-research request:
  - [2026-05-26-call-of-duty-medal-research-request.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-26-call-of-duty-medal-research-request.md)

### Current Recommended First Real Task

If the current repo blocker is still `call_of_duty` text or reward-banner signal visibility, use this as the first real GPT 1 task after the smoke test:

```text
Fill out the call_of_duty text banner packet stub using the text banner packet brief and the supporting repo evidence.
Return one decision-ready research packet for the next repo action.
Focus on visible in-clip text or reward-banner signals, exact timestamps, crops, false positives, and the likely extraction method.
```

### Conversation Starters

Add these starters:

- `Use Research Brief Mode. Help me investigate a concept or tool for my automated video highlight pipeline.`
- `Use Repo Analysis Mode. Analyze this GitHub repo and identify reusable patterns for my project.`
- `Use Task Specification Mode. Turn this messy idea into a Codex-ready implementation ticket.`
- `Use Note Cleanup Mode. Organize these notes into a clean project planning section.`
- `Use Decision Memo Mode. Compare these options and recommend the smallest practical path.`

### Smoke Test

Run this prompt first:

```text
Turn the following into a structured draft note with topic, source_set, key_findings, project_relevance, recommended_implications, uncertainties, and follow_up_questions.

Topic: whether a custom GPT should be treated as the runtime backend for this pipeline

Findings:
- Custom GPTs are good at control-room planning and note cleanup
- they are not the right place to run long video jobs or queue workers
- architecture decisions still need repo-local validation
```

Pass criteria:

- output uses the exact note structure
- it separates findings from implications
- it does not claim canonical project truth

Then run this packet-facing check:

```text
Fill out the call_of_duty text banner packet stub using the text banner packet brief and the supporting repo evidence.

Return exactly one decision-ready packet for one next repo action.
Use an appendix only if overflow evidence is necessary.
Do not return a generic summary.
```

Packet pass criteria:

- output supports one exact next repo action
- output includes recommendation and acceptance target
- output makes the next Codex step obvious
- output does not hide the recommendation inside long prose

## GPT 2 Checklist: Pipeline Architecture and Troubleshooting

### Builder Fields

1. Create a second GPT.
2. Set the **Name** field to:
   - `Pipeline Architecture and Troubleshooting`
3. Set the **Description** field to:

```text
A repo-aware architecture and debugging assistant for the gameplay highlight pipeline. It uses curated notes plus local repo context to design runtime features, review implementation boundaries, interpret errors, and produce executable Codex or script handoffs.
```

4. Paste this file into the **Instructions** field:
   - [pipeline_architecture_and_troubleshooting_instructions.txt](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/research_runtime/prompts/custom_gpts/pipeline_architecture_and_troubleshooting_instructions.txt)

### Capabilities

Turn **on**:

- File uploads / file analysis
- Code interpreter / data analysis
- Image understanding

Turn **off** or do not rely on by default:

- Custom actions
- Image generation

If the builder requires browsing to stay globally enabled, keep it enabled but treat it as secondary to repo-local truth.

### Upload Order

Upload these files in this order:

1. [PIPELINE_PROJECT_BRIEF.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/PIPELINE_PROJECT_BRIEF.md)
2. [CODEX_HANDOFF_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CODEX_HANDOFF_TEMPLATE.md)
3. [UPLOAD_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/UPLOAD_ROUTING.md)
4. [RESEARCH_AGENT_FRAMEWORK.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_FRAMEWORK.md)
5. [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md)
6. [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
7. [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)
8. one reviewed packet or note from GPT 1

### Conversation Starters

Add these starters:

- `Turn this feature idea into a repo-compatible implementation plan.`
- `Use the provided notes and this repo context to design the smallest safe v1.`
- `Analyze this traceback or ffmpeg failure and propose a debug path.`
- `Review this architecture idea against the pipeline’s contracts and identify risks.`
- `Write a Codex-ready handoff for this runtime change.`

### Smoke Test

Run this prompt first:

```text
Use the following draft research note and turn it into a Codex-ready handoff.

topic:
  whether the research runtime should keep a one-section write barrier

source_set:
  - local research framework doc
  - local runtime artifact contract

key_findings:
  - one-section writes reduce recap drift
  - schema validation is already part of the runtime posture
  - freeform multi-section synthesis is a failure mode

project_relevance:
  The runtime is intended to produce bounded, evaluable research artifacts.

recommended_implications:
  - preserve one-section writes
  - keep retry-once then quarantine
  - avoid adding freeform synthesis shortcuts

uncertainties:
  - whether future synthesis sections need a separate controlled exception

follow_up_questions:
  - should synthesis be a dedicated section instead of a general behavior?
```

Pass criteria:

- output is implementation-oriented
- it references repo-context verification
- it does not simply restate the research note

## First-Run Operator Notes

After both GPTs are created:

1. use GPT 1 to normalize one messy note
2. manually review that draft note
3. pass the reviewed note to GPT 2
4. ask GPT 2 for a Codex-ready handoff
5. compare the handoff against:
   - [CODEX_HANDOFF_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CODEX_HANDOFF_TEMPLATE.md)

If GPT 2 still behaves too much like a broad research assistant, tighten its instructions before uploading more files.

## If You Are Focusing Only On GPT 1

That is fine.

You can stop after:

1. building `Pipeline Research Strategist`
2. running its smoke test
3. running one real note-normalization task
4. tightening its instructions based on the first failure pattern

You do not need to build GPT 2 yet.

## Builder Completion Criteria

The setup is complete when:

- both GPTs exist
- both have the correct name and description
- both use the correct pasted instruction file
- both have the intended capabilities enabled
- both have the correct upload set
- both pass their smoke test
- GPT 1 produces draft notes
- GPT 2 produces implementation or debugging outputs rather than broad summaries
