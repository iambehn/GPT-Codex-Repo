# Research Agent Framework

This document is the canonical V2 home for the repo-specific research-agent operating framework.

## Canonical Scope

Use this doc for:

- the operating thesis for in-depth LLM-assisted research on this pipeline
- the runtime contract that should shape research-agent outputs
- the role split between LangGraph, Pydantic/PydanticAI, and DSPy
- checkpoint, failure, and evidence-handling policy for research loops
- the adoption path from note pile to durable research system

Do not use this doc for:

- pipeline runtime or fusion mechanics
- one-off implementation scaffolds or generated code dumps
- experiment outcomes or benchmark logs
- general-purpose “AI personality” advice detached from this repo

## Current V2 Position

The right model for this repo is not a long prompt with a “research personality.” It is a **stateful research machine** that produces bounded, typed artifacts.

The core rule is:

> **One turn, one lens, one artifact section, one contract.**

That rule exists to prevent the two failure modes that dominate long research chats:

- recap drift, where the model keeps rewriting or summarizing prior work
- scope drift, where the model blends architecture, implementation, evidence, and synthesis into one unstable blob

For this project, the research agent should optimize for:

- auditable artifacts over conversational helpfulness
- temporal correctness and pipeline-specific rigor
- bounded loops over open-ended ideation
- explicit assumptions, uncertainties, and failure modes
- low-clutter workflow hygiene

“Personality” is therefore a thin behavior layer, not the framework itself.

## Purpose And Operating Posture

The research agent exists to support high-level design and investigation work for this pipeline project:

- architecture and subsystem reasoning
- detection, runtime, and fusion research
- evaluation and calibration policy design
- governance, drift, and lifecycle analysis
- implementation planning grounded in repo reality

Its posture should be:

- skeptical
- artifact-first
- concise
- evidence-seeking
- drift-resistant
- explicit about uncertainty

This means the agent should not aim to sound expansive or persuasive. It should aim to leave behind durable, inspectable research outputs that can be reviewed, compared, and implemented.

## Framework Stack And Tool Roles

The stack decision should be explicit.

### LangGraph

Use LangGraph as the **control plane**.

Its role is:

- turn routing
- durable state and checkpoints
- persistence
- interrupts and human review gates
- bounded multi-step execution

LangGraph owns workflow shape, not schema meaning.

### Pydantic And PydanticAI

Use Pydantic and, where useful, PydanticAI as the **contract layer**.

Their role is:

- typed artifact schemas
- validation of node outputs
- section-level output enforcement
- retry-on-validation-failure behavior inside nodes
- testable output contracts and eval hooks

Pydantic/PydanticAI own artifact correctness, not orchestration.

### DSPy

Defer DSPy to a later phase as the **optimizer layer**.

Its role should begin only after:

- stable artifact schemas exist
- evaluator metrics exist
- labeled datasets or review-backed targets exist
- there is evidence that a specific turn or prompt module is worth optimizing

DSPy is not part of the v1 runtime contract. It is reserved for later prompt-program optimization once the research system is already stable and measurable.

### Recommended Position

Use:

- **LangGraph now**
- **Pydantic/PydanticAI now**
- **DSPy later**

Do not treat these as interchangeable alternatives.

## Research Runtime Contract

Each research turn should be a constrained write to one artifact section.

### Research Artifact Contract

Use one durable research artifact with named sections. A minimal shape is:

```yaml
research_artifact:
  topic_id: ...
  schema_version: ...
  sections:
    domain_framing: ...
    system_boundary: ...
    architecture_dataflow: ...
    question_map: ...
    blindspots: ...
    adversarial_cases: ...
    evidence_expansion: ...
    crossmodal_dependencies: ...
    governance_lifecycle: ...
    economics_reliability: ...
    synthesis_note: ...
    editorial_qa_next_loop: ...
```

Each section should carry:

- `source_refs`
- `claims`
- `uncertainties`
- `failure_modes`

### Turn Output Envelope

Each node should return one typed envelope:

```yaml
turn_output_envelope:
  target_section: ...
  artifact_payload: ...
  source_refs: ...
  runtime_meta: ...
```

`artifact_payload` must validate against the assigned target schema.

### One-Section Write Barrier

A node may write only its assigned `target_section`.

Rules:

- extra sections are invalid
- extra non-schema fields are invalid
- cross-section rewrites are invalid
- freeform prose outside the schema is invalid

This is the primary defense against recap drift and uncontrolled synthesis.

### Retry And Quarantine

If a node fails schema validation:

1. retry once with a stricter corrective prompt
2. if it fails again, quarantine the run
3. emit a reviewable failure artifact rather than forcing progress

Quarantine should preserve:

- the failed output
- the validation error
- the relevant evidence bundle
- the turn identity

### Evidence Handling

Treat retrieved evidence as **untrusted input**.

Rules:

- evidence may enter only through a structured evidence channel
- evidence should populate schema fields, not instruction layers
- retrieved text should not be copied into system or developer prompts
- instruction-bearing evidence should be treated as adversarial input

## Output-Control Layer

The framework should shape LLM outputs through explicit operating rules, not through aesthetic “tone.”

Required behavior defaults:

- no conversational filler
- no recap summaries of previous turns unless the current section explicitly requires synthesis
- no rewriting of other sections
- no uncontrolled freeform synthesis
- explicit assumptions when evidence is incomplete
- explicit uncertainties and failure modes
- explicit citations or source references where claims depend on evidence

This is the output-control layer that should actually change how conversations behave.

## Evaluation And Governance

The research system needs contract metrics, not just subjective satisfaction.

### Contract Metrics

Use these as baseline runtime quality checks:

- schema pass rate
- one-section violation rate
- extra-field rate

Expected posture:

- schema pass rate should be very high
- one-section violation rate should be zero
- extra-field rate should be zero

### Artifact Quality Metrics

Track:

- citation coverage
- unsupported claim count
- unresolved assumption count

These are better indicators of research quality than generic “helpfulness.”

### Operational Metrics

Track:

- turn latency
- token cost per run
- retry count
- quarantine count

### Checkpoint Policy

Checkpoints should be immutable validated state snapshots, not rolling summaries.

Use checkpoints at stable boundaries such as:

- Turn 3
- Turn 6
- Turn 9

Checkpoints exist to anchor later turns to validated state without forcing the model to re-summarize its own prior work.

### Governance Position

Research outputs should be treated as decision-support artifacts, not self-authorizing truth.

That means:

- changes to stable pipeline logic still require normal review
- research loops should be inspectable and replayable
- measured outcomes belong in experiment records, not inside the research framework doc

## Adoption Path For This Repo

This repo should adopt the framework in stages.

### Immediate

- commit the framework and protocol rules as durable repo guidance
- use [RESEARCH_PROTOCOL.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/RESEARCH_PROTOCOL.md) as the operational prompt/runtime layer
- define the artifact shape and turn output envelope
- define the output-control defaults for research conversations
- keep the framework repo-specific and focused on this pipeline’s constraints

### Near Term

- define Pydantic schemas for the highest-value research sections
- add a minimal LangGraph runtime for bounded multi-turn research loops
- add contract tests for section ownership and schema validity
- add traceability and evaluation hooks

### Later

- expand to a full multi-turn runtime only after the early schemas and metrics are stable
- introduce DSPy only where labeled evaluator signals justify optimization

## What Belongs Elsewhere

- Pipeline architecture belongs in [ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md).
- Runtime, detection, and fusion mechanics belong in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Review, replay, and calibration release-gate logic belongs in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md).
- Registry and orchestration state belong in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Historical note traceability belongs in [archive/RAW_NOTES_CATALOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/archive/RAW_NOTES_CATALOG.md).
