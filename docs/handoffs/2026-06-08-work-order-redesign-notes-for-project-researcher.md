# Work-Order Redesign Notes For Project Researcher

## Purpose

This note is a preparation memo for the upcoming redesign of the gameplay video editing pipeline.

The redesign direction is:

- think about the pipeline like an ERP system processing work orders
- treat the video editor node as a specialized worker
- build the support system around that worker so editing work can move reliably through stations

This note is not the final protocol.

Its purpose is to give the project researcher a clean starting point for refining the protocol and the business/task model behind it.

## Core Reframe

The current repo is strong at:

- evidence extraction
- review and calibration
- export packaging
- provenance
- registry and ledger state

The current repo is weaker at:

- expressing the exact job being processed
- expressing who or what owns the next step
- separating blocked orders from merely incomplete artifact trees

The redesign should preserve the current evidence architecture and add a work-order layer above it.

## The Important Constraint

Do not frame this as:

- replacing the current detector pipeline
- replacing registries and artifacts with a general ERP framework
- inventing a second architecture beside the existing one

Frame it as:

- the current pipeline becomes the production machinery
- work orders become the operational unit moving through that machinery

The correct strategy is additive, not replacement-first.

## Current Codex Design Direction

Two design artifacts already exist locally:

1. [2026-06-08-video-editing-work-order-model-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-08-video-editing-work-order-model-design.md)
2. [2026-06-08-short-clip-edit-order-station-transitions-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-08-short-clip-edit-order-station-transitions-design.md)

Current recommended first canonical order type:

- `short_clip_edit_order_v1`

Meaning:

- one request to produce one publishable short-form FPS gaming clip from source gameplay media

Current recommended first station chain:

1. `intake`
2. `source_readiness`
3. `signal_extraction`
4. `candidate_generation`
5. `editorial_selection`
6. `edit_packaging`
7. `approval`
8. `delivery`
9. `post_completion`

## What The Researcher Should Help Clarify

The researcher should not start by proposing tools or code.

The researcher should help answer business and workflow questions that the architecture depends on.

### 1. What is the first real monetizable unit of work?

Codex currently recommends:

- one publishable vertical FPS highlight clip from source gameplay media

The researcher should pressure-test whether that is the best first atomic unit versus alternatives such as:

- prepared clip batch from a VOD
- candidate timestamp package
- full raw-to-published short
- review-ready clip package for a human editor

The output should not be a broad list.

The output should recommend one first work-order type.

### 2. What should the video editor node actually do?

The researcher should help separate:

- tasks the pipeline can do deterministically
- tasks that require editorial judgment
- tasks that require manager approval

The output should identify:

- what the video editor node owns
- what Codex or pipeline automation owns
- what the manager owns

### 3. What is the minimum information required to complete the first work order?

The output should identify:

- required inputs
- optional inputs
- blockers
- evidence that the job is done

This should be stated in operational terms, not only in business terms.

### 4. What downstream markets or buyers matter for the first order type?

The researcher should not solve full go-to-market strategy yet.

The useful question is narrower:

- for the chosen first work-order type, who values that deliverable and in what form?

Examples the researcher may need to compare:

- creator-facing editing service
- internal speculative content production
- prepared inventory for later publishing
- clip-packaging as an intermediate service

The output should recommend one first market-facing framing, even if provisional.

## What The Researcher Should Not Do

Do not send back:

- a giant industry overview without a recommendation
- a generic AI video editing plan
- a broad theory of ERP systems
- a full marketplace map without tying it to one order type
- implementation details that assume a rewrite of the repo

Do not recommend:

- replacing the current evidence/review/export architecture
- collapsing provenance and review boundaries into a single business layer
- inventing a project-management system that is disconnected from artifacts and workflow state

## Desired Researcher Output

The best next researcher packet should answer these questions directly:

1. What is the first canonical monetizable work order?
2. What does a buyer or consumer of that order actually expect as the finished output?
3. What work should the video editor node own versus the pipeline architect versus the manager?
4. What minimum fields must exist on that work order for it to move through the system reliably?
5. What market or operational framing makes the most sense for the first order type?

## Output Format Recommendation

The researcher packet should ideally contain:

- one recommended first order type
- one recommended first buyer/use-case framing
- one responsibility split:
  - editor node
  - Codex/pipeline
  - manager
- one required-input checklist
- one done-definition checklist
- one list of explicit non-goals for v1

## Current Architectural Guardrails

Any recommendation should preserve these guardrails:

- the repo stays artifact- and provenance-driven
- review and approval checkpoints remain explicit
- workflow runs stay execution artifacts, not the top-level business object
- post ledger remains downstream of local export
- human approval is still required before privileged external actions

## Bottom Line

The useful researcher job now is not to invent a new pipeline.

It is to help define:

- what work orders are worth doing
- what the first worker should be doing
- what support the pipeline must provide

The architecture should then adapt to those decisions while preserving the current repo's strongest properties:

- explicit evidence
- explicit review
- explicit lineage
- explicit delivery boundaries
