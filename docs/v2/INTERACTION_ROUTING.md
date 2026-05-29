# Interaction Routing V1

This document defines the thin interaction-routing layer for the repo's researcher GPT workflow.

Use this doc for:

- routing vague or messy inputs into the right next artifact
- choosing the next interaction mode from the current blocker
- defining the minimum internal state classification that guides researcher behavior
- setting stop conditions before packet or task production

Do not use this doc for:

- runtime pipeline routing
- packet envelope fields
- backlog queue mechanics
- ledger, spreadsheet, or lifecycle-tracking infrastructure

The surrounding source-of-truth surfaces remain:

- [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md)
- [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md)
- [BACKLOG_OPERATING_MODEL.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/BACKLOG_OPERATING_MODEL.md)
- [RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_AGENT_CUSTOM_GPT_CONFIGS.md)

## Routing Objective

The routing layer exists to:

- reduce ambiguity
- preserve uncertainty honestly
- improve decomposition quality
- maximize decision-improving signal per unit of effort

This layer is internal to the researcher GPT. It should improve packet and handoff quality without turning the external artifact contract into a governance-heavy system.

## Core Loop

Use this loop for researcher-side routing:

1. classify state
2. identify target artifact
3. identify missing information
4. choose the next interaction mode
5. choose the next question type
6. stop when decision sufficiency is reached

Explicit rule:

- internal routing logic may guide the researcher's reasoning
- external artifacts should not expose the internal routing framework unless explicitly requested

## Minimum Internal State Classification

Use the following small, behavior-linked internal fields:

```yaml
intent_mode: understand | compare | decide | plan | test | explore | diagnose | audit
abstraction_level: vision | architecture | workflow | implementation
uncertainty_state: known | likely | hypothesized | unresolved
execution_readiness: idea | decomposing | structured | implementation_ready
weakest_blocking_axis: intent | scope | evidence | decomposition | risk | readiness | articulation
```

These fields are internal routing aids only. They should not become mandatory external packet fields in this slice.

## Routed Artifact Set

The first routing layer should target exactly four artifacts:

1. `research_note`
2. `decision_ready_packet`
3. `implementation_ticket`
4. `codex_handoff_brief`

### `research_note`

Use when:

- the work is exploratory
- the inputs are still broad
- execution readiness is low
- the goal is to stabilize findings before execution-facing framing

Minimum information:

- topic
- source set
- key findings
- project relevance
- tentative implications
- uncertainties

Typical blockers:

- low intent clarity
- low scope clarity
- weak evidence posture
- low execution readiness

Typical interaction modes:

- open question
- contrastive choice
- scenario walkthrough

### `decision_ready_packet`

Use when:

- the next repo action is known
- the goal is to support Codex execution directly
- the missing work is primarily evidence and recommendation completion

Minimum information:

- exact decision target
- current repo truth
- operational evidence
- recommendation
- acceptance target

Typical blockers:

- low evidence
- low exclusion clarity
- unresolved uncertainty around the next repo action

Typical interaction modes:

- source or evidence request
- recognition task
- constrained clarification

### `implementation_ticket`

Use when:

- the next work item is implementation-facing
- task framing is still needed before direct handoff
- the objective is stable but repo-facing work still needs sharper boundaries

Minimum information:

- objective
- inputs and outputs
- acceptance criteria
- non-goals
- dependencies

Typical blockers:

- low decomposition quality
- incomplete boundary definition
- unclear input or output contract

Typical interaction modes:

- decomposition prompt
- task-spec framing
- constraint capture

Gating rule:

- do not route here until execution readiness is at least `structured`

### `codex_handoff_brief`

Use when:

- the implementation boundary is already stable
- repo-context translation is the next step
- the work is ready to be handed to Codex as an implementation-ready brief

Minimum information:

- concrete outcome
- governing repo surfaces
- required changes
- constraints and invariants
- verification

Typical blockers:

- unresolved repo-context mapping
- unclear invariants
- weak verification definition

Typical interaction modes:

- repo-context clarification
- implementation-boundary review

Gating rule:

- do not route here until execution readiness is `implementation_ready`

## Artifact-First Routing Rules

Start from the target artifact and work backward.

For every interaction:

1. decide which of the four artifacts is actually needed next
2. identify the minimum required information for that artifact
3. identify the weakest missing requirement
4. choose the smallest interaction mode that can fill that gap

If execution readiness is too low for `implementation_ticket` or `codex_handoff_brief`, route back to:

- `research_note`
- or packet-collection behavior

## Question-Selection Rules

Map the weakest blocking axis to the next interaction move:

- low `intent` -> intent question or contrastive choice
- low `scope` -> boundary question
- low `evidence` -> source or evidence request
- low `decomposition` -> decomposition prompt
- low `risk` -> exception or adversarial question
- low `readiness` -> input or output clarification, not tasking
- low `articulation` -> recognition task, examples, ranking, or constrained choices instead of open prose

Interaction modes should stay small:

- open question
- contrastive choice
- recognition task
- scenario walkthrough
- exception probe
- task-spec framing

Rule:

- use the smallest interaction mode that can change the next artifact decision

## Checkpoint Cards

Use a small internal checkpoint-card family before choosing the next question.

### `intent card`

Use when:

- the user outcome is unclear

Resolves:

- `intent`

Minimum fields:

- desired outcome
- decision vs exploration posture
- what should happen next
- what should not happen next

Can unlock:

- `research_note`
- `decision_ready_packet`

### `scope card`

Use when:

- boundaries are broad, mixed, or unstable

Resolves:

- `scope`
- `decomposition`

Minimum fields:

- in-scope surface
- out-of-scope surface
- target layer
- main boundary

Can unlock:

- `research_note`
- `implementation_ticket`

### `evidence card`

Use when:

- the next artifact is blocked by weak source truth

Resolves:

- `evidence`

Minimum fields:

- exact paths or URLs
- timestamps if relevant
- why the evidence matters
- trust level

Can unlock:

- `decision_ready_packet`
- `codex_handoff_brief`

### `risk card`

Use when:

- the change may hide contradictions, edge cases, or unsafe assumptions

Resolves:

- `risk`

Minimum fields:

- likely failure mode
- contradiction or exception case
- blocking vs non-blocking risk

Can unlock:

- `decision_ready_packet`
- `implementation_ticket`

### `readiness card`

Use when:

- the conversation is drifting toward tasking too early

Resolves:

- `readiness`

Minimum fields:

- objective
- constraints
- inputs
- outputs
- unresolved blockers

Can unlock:

- `implementation_ticket`
- `codex_handoff_brief`

## Stop Conditions

Stop elicitation when:

- no new requirement, contradiction, risk, or decision is emerging
- the next step is clearly artifact production
- repeated questioning would only create low-yield restatement

Hard rule:

- `decompose before tasking`

Do not produce `implementation_ticket` or `codex_handoff_brief` while critical objective, constraint, or boundary information is still unresolved.

## Behavioral Examples

### Example 1: vague exploratory input

- state:
  - `intent_mode=explore`
  - `execution_readiness=idea`
  - `weakest_blocking_axis=intent`
- route:
  - target artifact -> `research_note`
  - interaction mode -> contrastive choice

### Example 2: known repo-changing next action

- state:
  - `intent_mode=decide`
  - `execution_readiness=structured`
  - `weakest_blocking_axis=evidence`
- route:
  - target artifact -> `decision_ready_packet`
  - interaction mode -> evidence request

### Example 3: implementation-facing but not repo-grounded enough

- state:
  - `intent_mode=plan`
  - `execution_readiness=structured`
  - `weakest_blocking_axis=decomposition`
- route:
  - target artifact -> `implementation_ticket`
  - interaction mode -> task-spec framing

### Example 4: stable implementation boundary

- state:
  - `intent_mode=plan`
  - `execution_readiness=implementation_ready`
  - `weakest_blocking_axis=scope`
- route:
  - target artifact -> `codex_handoff_brief`
  - interaction mode -> repo-context clarification

### Example 5: low articulation support

- state:
  - `weakest_blocking_axis=articulation`
- route:
  - use recognition task, examples, ranking, or constrained choices
  - do not use open-ended prose as the default move

### Example 6: low readiness blocks tasking

- state:
  - `execution_readiness=idea`
  - `weakest_blocking_axis=readiness`
- route:
  - do not produce `implementation_ticket`
  - do not produce `codex_handoff_brief`
  - return to clarification or research-note capture

## Current Defaults

- this slice is documentation and instruction-contract work only
- the routing layer is internal to the researcher GPT
- no ledger, spreadsheet schema, taxonomy engine, or lifecycle-tracking subsystem is part of this v1
- additional user-facing intake cards can be added later if this layer improves packet quality in practice
