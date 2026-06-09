# Pipeline Redesign Goal Stack

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Create one canonical goal stack for the gameplay pipeline redesign.

This note is the shared planning surface for:

- manager decisions
- pipeline architecture
- researcher pressure-testing

It assumes the pipeline is being reframed as a work-order / MRP-style system that helps a worker layer complete bounded gameplay-content jobs reliably over time.

## Why This Note Exists

The repo already has:

- a first-pass work-order model
- a first-pass station-transition model
- multiple researcher notes exploring business shape, order qualification, sourcing, inventory, approval, and capability measurement

Those materials are useful, but they are distributed across:

- architectural specs
- researcher handoff notes
- external researcher documents

This note turns them into one explicit hierarchy of goals and subgoals so the redesign can progress without drifting into:

- general “improve the pipeline” work
- broad editing-agency assumptions
- disconnected market research
- disconnected implementation work

## Operating Assumptions

- The internal architecture should continue using the work-order / MRP mindset even if the external commercial wrapper later becomes productized deliverables, recurring support, or a hybrid.
- States should describe artifact condition, not process steps.
- The first commercial lane is not yet final.
- The current repo evidence, review, export, and lineage architecture should be extended, not replaced.
- The first pass should optimize for decision clarity, not brevity.
- Partial-fulfillment outputs may be real products, but that is still a research-dependent question.

## Goal Work-Order Template

Each top-level goal in this note should be treated as a goal work order rather than only a planning heading.

Every goal work order should declare:

- Goal Type
- Current State
- Desired State
- Desired Transition
- Current Qualification
- Target Qualification
- Artifact Produced
- Future Transition Enabled
- Consequence Horizon

The intent is to make each goal legible as:

- a transition the system is trying to complete
- a qualification target the system is trying to earn
- an artifact-producing unit of work

This keeps the goal stack aligned with the work-order / MRP framing and the researcher bundle's emphasis on qualification-driven operating logic.

## Goal Hierarchy

### Goal 0: Define The State Model

#### Goal Type

INFRASTRUCTURE

#### Current State

The redesign has implied artifact conditions such as raw source, review-ready outputs, approved clip sets, and published assets, but those conditions are not yet defined as one canonical control-plane state model.

#### Desired State

The redesign has one compact state foundation that defines:

- canonical production states
- canonical cross-artifact control states
- state-level entry conditions
- state-level exit rules
- state-level inspection requirements

#### Desired Transition

Implicit artifact conditions
→
Canonical state foundation

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

State Catalog v0

#### Future Transition Enabled

Descriptive workflow discussion
→
State-grounded transition, routing, inspection, and qualification design

#### Consequence Horizon

Years

#### Objective

Define the first canonical state model for the redesigned control plane.

#### Why It Matters

States are the atomic conditions that the rest of the redesign will attach to.

Without canonical states:

- transitions stay descriptive instead of explicit
- routing remains ambiguous
- inspection criteria drift
- failure attribution becomes inconsistent
- qualification cannot attach to stable targets

#### Success Criteria

- one compact state schema is defined
- production states are separated from cross-artifact control states
- each state is defined as an artifact condition, not a process step
- each state has entry conditions, exit rules, and inspection requirements
- the first state catalog is explicit enough to support later transition design

#### Subgoals

1. Define the state schema for v0.
2. Define the first production states for the likely short-form workflow.
3. Define the first cross-artifact control states for non-happy-path handling.
4. State the guardrail that transitions and work orders may reference states, but states themselves must not encode actions.
5. Define the first rules for how later transition design will attach to the state catalog.

#### Dependencies

- current redesign notes
- current short-clip order and station-transition specs

#### Blocked By

- unresolved disagreement about whether states should describe artifact condition or process steps

#### Non-Goals

- no routing model yet
- no qualification engine yet
- no inventory or planning model yet
- no process-step verbs encoded as states

#### `/goal` Candidate

```text
/goal Add Goal 0 — State Model to the pipeline redesign goal stack and create State Catalog v0 as the first control-plane artifact, verified by a compact state table with canonical state names, definitions, entry conditions, valid next states, and inspection requirements. Preserve the MRP/work-order framing. Do not implement routing, qualification, or inventory yet; only create the state foundation they will attach to. If state definitions start collapsing into process steps, stop and report the ambiguity.
```

### Goal 1: Choose The First Business Lane

#### Goal Type

DISCOVERY

#### Current State

No commercially qualified first lane has been selected.

Multiple plausible lanes exist, but the business does not yet have a defensible first-lane decision strong enough to govern architecture, sourcing, and build priorities.

#### Desired State

One commercially qualified first lane is chosen with explicit:

- scope
- comparative rationale
- buyer fit
- channel fit
- capability consequence

#### Desired Transition

Unqualified lane ambiguity
→
Qualified first-lane decision

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

Lane Decision Package

#### Future Transition Enabled

Generic redesign planning
→
Lane-specific architecture, qualification, offer, and build work

#### Consequence Horizon

Months / Years

#### Objective

Determine the first commercially viable FPS-adjacent lane the pipeline should be built around over the next few quarters.

#### Why It Matters

This is the root decision.

It controls:

- which order class becomes the first anchor
- which buyer persona matters most
- which channels matter first
- which capability build matters first
- what kinds of work should be explicitly rejected

#### Success Criteria

- one first lane is chosen
- one second lane is chosen
- one lane is explicitly deferred or rejected
- one strongest counterargument is recorded
- one highest-leverage capability gap is tied to the chosen lane

#### Subgoals

1. Compare the candidate first lanes:
   - downstream approved-clip packaging
   - upstream review-pack / discovery
   - narrow known-moment finished-output work
   - hybrid creator workflow support
2. Determine whether partial-fulfillment outputs are true external products or mainly internal support outputs.
3. Determine whether the commercial wrapper should be:
   - per-order productized deliverables
   - recurring workflow support
   - a staged hybrid
4. Identify the best-fit buyer persona for the chosen lane.
5. Identify the best-fit sourcing channels for the chosen lane.
6. Identify what work the business should explicitly avoid even if demand exists.

#### Dependencies

- Goal 0 state foundation
- researcher notes
- market reasoning
- comparison of alternative business lanes

#### Blocked By

- unresolved market-fit evidence about what buyers actually pay for

#### Non-Goals

- no broad video-editing-agency positioning
- no attempt to support many commercial lanes at once
- no assumption that “gaming editing demand” is the same thing as a qualified lane

#### `/goal` Candidate

```text
/goal Determine the first commercially viable FPS gameplay work-order lane for the pipeline, verified by one chosen first lane, one chosen second lane, one highest-leverage capability build, and explicit out-of-scope work. Preserve the work-order/MRP framing and do not broaden into a general editing-agency model. If the answer depends on unresolved market evidence, stop with the exact unanswered question.
```

### Goal 2: Define The Canonical Work-Order System

#### Goal Type

INFRASTRUCTURE

#### Current State

The repo has strong artifact, review, export, and lineage systems, but business/job state remains mostly implicit and must be inferred from technical artifacts.

#### Desired State

The repo has one explicit work-order operating model above the artifact layer, with:

- named order classes
- named stations
- explicit ownership
- explicit routing for approval, revision, defer, and cancel

#### Desired Transition

Implicit operational state
→
Explicit work-order operating model

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

Canonical Work-Order System Spec

#### Future Transition Enabled

Artifact-driven workflow inference
→
Explicit order routing, ownership, and closeout logic

#### Consequence Horizon

Years

#### Objective

Translate the chosen lane into a work-order-driven operating model that sits above the current artifact pipeline.

#### Why It Matters

The current repo is strong at artifact generation, review, export, and lineage.

It is weaker at expressing:

- what exact job is being processed
- what class of job it is
- who owns the next step
- whether a job is blocked, deferred, cancelled, or complete

This goal creates the operational object that lets the business reason about real work instead of inferring business state from artifacts.

#### Success Criteria

- canonical order classes are named
- first-order class hierarchy is decided
- work-order record shape is defined
- event log shape is defined
- station ownership is defined
- approval, revision, defer, and cancel routes are defined

#### Subgoals

1. Keep the current repo evidence/review/export architecture intact.
2. Separate business truth from artifact truth.
3. Define class-specific done states.
4. Define new-order versus revision rules.
5. Define explicit approval-owner logic.
6. Define station ownership across:
   - Codex / structured worker
   - human/editor
   - manager
7. Define source-archetype handling if source condition proves more important than deliverable alone.

#### Dependencies

- Goal 0 state foundation
- Goal 1 chosen lane
- existing work-order and station-transition specs in this repo

#### Blocked By

- first-lane choice changing materially after research review

#### Non-Goals

- no giant ERP rewrite
- no replacement of the current detector/review/export machinery
- no parallel source of truth beside the current repo architecture

#### `/goal` Candidate

```text
/goal Translate the chosen first lane into a first-pass work-order architecture, verified by explicit order-class definitions, state fields, approval routing, revision boundaries, and qualification-envelope structure in repo notes. Extend existing pipeline surfaces rather than creating parallel ones. If the lane changes materially, stop with the dependency.
```

### Goal 3: Define Qualified Work Envelopes

#### Goal Type

QUALIFICATION

#### Current State

The pipeline has only broad or partially implied ideas of what the agents can do.

Qualification is not yet stated as explicit work envelopes tied to order class, source condition, approval complexity, revision scope, and rescue burden.

#### Desired State

The pipeline has explicit qualified work envelopes for the top order classes, with:

- fixed qualification ladder
- explicit guardrails
- explicit class/source/approval combinations
- explicit separation between technical capability and commercial qualification

#### Desired Transition

Broad assumed capability
→
Explicit qualified work envelopes

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

Qualified Work Envelope Model

#### Future Transition Enabled

Generic confidence in agent ability
→
Evidence-backed lane qualification and specialization control

#### Consequence Horizon

Years

#### Objective

Specify what the agents are actually qualified to do, inside what exact order-class and source/approval conditions.

#### Why It Matters

The business should not compete for “gaming editing” in general.

It should compete only inside proven qualified envelopes.

Qualification must be tied to:

- order class
- source archetype if relevant
- approval complexity if relevant
- revision scope
- platform scope
- rescue burden

#### Success Criteria

- qualification model exists for top order classes
- qualification ladder is fixed
- success envelopes are stated explicitly
- class/source/approval combinations can be marked as:
  - core
  - adjacent
  - trial
  - reject

#### Subgoals

1. Define the qualification ladder:
   - Q0 not qualified
   - Q1 trial only
   - Q2 qualified with strict guardrails
   - Q3 qualified for normal production
   - Q4 qualified for scaled core production
2. Define the qualified envelope by:
   - order class
   - source archetype if needed
   - approval model if needed
   - revision scope
   - platform scope
3. Define rescue burden as a first-class qualification metric.
4. Separate technical capability from commercial qualification.
5. Define what counts as:
   - strong core
   - guardrailed adjacent
   - trial only
   - reject

#### Dependencies

- Goal 0 state foundation
- Goal 2 work-order system

#### Blocked By

- unresolved uncertainty about whether source archetype or approval complexity is the primary specialization boundary

#### Non-Goals

- no blanket “qualified for gaming editing” claims
- no broad capability assumptions based on one-off success
- no promotion of a class based only on technical completion

#### `/goal` Candidate

```text
/goal Define and validate the qualified envelope for the chosen first lane, verified by explicit order-class constraints, source/approval assumptions, qualification levels, and a recommendation to keep the class trial, adjacent, strategic-core, or stable-throughput core. Stop if the lane cannot yet be qualified without unresolved boundary questions.
```

### Goal 4: Build The Measurement System

#### Goal Type

VALIDATION

#### Current State

The redesign direction is rich in concepts, but the measurement layer is still not encoded as one coherent evidence system for:

- order outcomes
- qualification drift
- sourcing quality
- promotion/demotion decisions

#### Desired State

The pipeline has one explicit measurement model that can:

- score order classes
- score qualification
- classify friction
- drive weekly and monthly management review

#### Desired Transition

Conceptual redesign logic
→
Evidence-driven operating measurement

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

Measurement System Definition

#### Future Transition Enabled

Opinion-driven specialization
→
Evidence-driven sourcing, qualification, and roadmap control

#### Consequence Horizon

Years

#### Objective

Make specialization, sourcing, and qualification evidence-driven.

#### Why It Matters

Without structured measurement, the business will drift into optimism and anecdote.

The system needs durable ways to measure:

- what work completed
- what work failed
- what required rescue
- what outputs were useful
- what should be sourced more
- what should be narrowed or rejected

#### Success Criteria

- order-class scorecard defined
- agent qualification matrix defined
- reason-code taxonomy defined
- weekly review defined
- monthly promotion/demotion review defined

#### Subgoals

1. Define order closeout fields:
   - completion
   - cancellation
   - defer
   - rescue
   - revision
   - approval friction
   - downstream usefulness
   - regret rate
2. Define reason-code families:
   - source
   - scope
   - support
   - editorial
   - approval
   - revision
   - turnaround
   - customer
   - policy
   - commercial
   - queue
3. Define promotion/demotion criteria for:
   - order classes
   - buyer personas
   - channels
   - qualification levels
4. Define the weekly manager scorecard sections:
   - intake quality
   - active portfolio mix
   - order outcomes by class
   - qualification drift
   - sourcing quality
   - friction map
   - one forced recommendation
5. Define the monthly structural review:
   - what gets promoted
   - what gets narrowed
   - what gets built
   - what gets paused

#### Dependencies

- Goal 0 state foundation
- Goal 2 work-order system
- Goal 3 qualified envelopes

#### Blocked By

- uncertainty about which outputs count as valid terminal work products

#### Non-Goals

- no dashboard sprawl before the underlying logic is fixed
- no conflation of technical capability with commercial qualification
- no free-text-only failure analysis

#### `/goal` Candidate

```text
/goal Define the measurement model for the chosen lane, verified by an order-class scorecard, agent qualification matrix, reason-code taxonomy, and weekly/monthly review rules that support sourcing decisions. Do not broaden into unrelated classes while the first measurement model is still being shaped.
```

### Goal 5: Define The Offer And Sourcing Model

#### Goal Type

DISCOVERY

#### Current State

The likely internal architecture is getting clearer, but the external commercial wrapper is still unresolved.

The system does not yet have:

- one first offer
- one second offer
- one preferred buyer/channel mix
- one explicit set of rejected market postures

#### Desired State

The business has a narrow external wrapper that:

- makes the chosen lane legible to buyers
- attracts good-fit work
- rejects weak-fit work early
- stays aligned with qualified internal envelopes

#### Desired Transition

Unresolved commercial wrapper
→
Lane-aligned offer and sourcing model

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

Offer And Sourcing Model Package

#### Future Transition Enabled

Internal architectural clarity
→
Externally legible, qualified market positioning

#### Consequence Horizon

Months / Years

#### Objective

Shape the external wrapper so it attracts work the pipeline can actually close.

#### Why It Matters

The internal architecture may be work-order and content-operations driven, but the external wrapper must still:

- make sense to buyers
- attract good-fit work
- repel weak-fit work
- preserve narrow qualified envelopes

#### Success Criteria

- first offer defined
- second offer defined
- explicit non-goal list defined
- preferred personas and channels defined
- bad-fit personas and channels defined

#### Subgoals

1. Decide whether the external wrapper is:
   - productized deliverables
   - recurring workflow support
   - a staged hybrid
2. Define first likely offer copy around the chosen lane.
3. Define what the offer must exclude.
4. Define how stable-throughput and strategic-core lanes differ in sourcing intensity.
5. Define whether broad marketplaces are:
   - scale channels
   - pilot channels
   - learning-only channels
   - noise
6. Define whether content operations should be sold directly or hidden behind concrete deliverables.

#### Dependencies

- Goal 0 state foundation
- Goal 1 lane selection
- Goal 3 qualified envelopes

#### Blocked By

- unresolved question of whether content operations should be sold directly or hidden behind deliverables

#### Non-Goals

- no broad creator-agency bundle
- no “do everything” offer language
- no hiding of non-goals that would later create revision churn

#### `/goal` Candidate

```text
/goal Shape the first external wrapper for the chosen lane, verified by one first offer, one second offer, explicit non-goals, preferred buyer personas, and preferred channel classes. Preserve the narrow qualified envelope and stop if the wrapper cannot be made buyer-legible without broadening the business.
```

### Goal 6: Define Portfolio And Queue Operating Rules

#### Goal Type

GOVERNANCE

#### Current State

The redesign now distinguishes:

- stable-throughput work
- strategic-core work
- adjacent work
- trial work
- internal inventory work

but these categories are not yet encoded into explicit portfolio and queue rules.

#### Desired State

The business has explicit rules for:

- portfolio balance
- queue truth
- trial caps
- defer/cancel hygiene
- widening control

so the workers can stay productive without letting the business drift into noise.

#### Desired Transition

Implicit queue and portfolio behavior
→
Explicit flow-governance rules

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

Portfolio And Queue Operating Rules

#### Future Transition Enabled

Ad hoc flow management
→
Controlled throughput, learning balance, and widening discipline

#### Consequence Horizon

Years

#### Objective

Keep the workers busy with clean work while preserving room for strategic learning.

#### Why It Matters

The business is not just a queue of orders.

It is a portfolio of work with different roles:

- stable-throughput work
- strategic-core learning work
- adjacent work
- trial work
- internal inventory/base-load work

Queue and portfolio rules keep the system from either:

- becoming too narrow to learn
- or becoming too experimental to operate

#### Success Criteria

- portfolio categories fixed
- queue lanes fixed
- trial capacity policy fixed
- defer/cancel hygiene fixed
- widening rules fixed

#### Subgoals

1. Define portfolio categories:
   - stable-throughput core
   - strategic core
   - adjacent
   - trial
   - internal inventory/base-load
2. Define live queue, blocked queue, approval queue, deferred queue, and completed queue expectations.
3. Define hard caps for trial and rescue-heavy work.
4. Define intake-freeze conditions.
5. Define when widening is allowed and on which dimension.
6. Define when queue imbalance should trigger sourcing tightening.

#### Dependencies

- Goal 0 state foundation
- Goals 2 through 5

#### Blocked By

- uncertainty about recurring support volume versus per-order volume

#### Non-Goals

- no portfolio growth by opportunistic demand alone
- no mixing of actionable work with dead or pseudo-active backlog
- no silent widening across multiple dimensions at once

#### `/goal` Candidate

```text
/goal Define the portfolio and queue operating rules for the chosen lane mix, verified by explicit portfolio categories, queue lanes, trial caps, defer/cancel rules, and widening conditions. Preserve queue truth and stop if the commercial wrapper still changes the portfolio model materially.
```

### Goal 7: Define The First Capability Build Agenda

#### Goal Type

GOVERNANCE

#### Current State

There are several plausible build candidates, but the redesign does not yet have one lane-specific, leverage-ranked capability agenda tied to:

- good-fit lost value
- stable-throughput protection
- strategic-core strengthening

#### Desired State

The business has one explicit first capability agenda with:

- one chosen build
- ranked next builds
- explicit non-builds
- lane-unlock logic

#### Desired Transition

General improvement ambition
→
Lane-specific leverage-ranked capability agenda

#### Current Qualification

Q1 plausible

#### Target Qualification

Q3 operational

#### Artifact Produced

Capability Build Agenda

#### Future Transition Enabled

Generic feature discussion
→
Qualified build sequencing tied to business leverage

#### Consequence Horizon

Months / Years

#### Objective

Choose the first few builds by business leverage, not technical novelty.

#### Why It Matters

The roadmap should be driven by:

- repeated lost value from good-fit work
- repeated friction inside strategically important lanes
- protection of stable throughput

It should not be driven by:

- broad feature ambition
- rescuing weak-fit demand
- generic “make the system smarter” goals

#### Success Criteria

- first capability build chosen
- second and third build candidates ranked
- explicit “do not build yet” list written

#### Subgoals

1. Compare likely early build categories:
   - intake/classification hardening
   - review-pack usefulness
   - approval/handoff structure
   - packaging repeatability
   - inventory promotion logic
2. Tie each build to one lane-unlock story.
3. Reject builds that mainly rescue weak-fit demand.
4. Separate:
   - protection builds
   - growth builds
   - stability builds

#### Dependencies

- Goal 0 state foundation
- Goal 1 lane selection
- Goal 4 measurement logic
- Goal 5 offer model

#### Blocked By

- unclear evidence about which lane is truly first

#### Non-Goals

- no broad creative-capability build without a lane-specific unlock story
- no build agenda chosen before the first lane and measurement logic are stable enough to justify it

#### `/goal` Candidate

```text
/goal Choose the first capability build agenda for the chosen lane, verified by one highest-leverage build, ranked second and third candidates, and an explicit do-not-build-yet list. Only count build candidates that unlock good-fit work and stop if lane evidence remains too weak to choose rationally.
```

## Cross-Goal Dependencies

The dependency order is:

0. Define the state model.
1. Choose the first business lane.
2. Define the canonical work-order system around that lane and state foundation.
3. Define qualified envelopes for the lane and adjacent classes.
4. Build the measurement system for those envelopes.
5. Shape the external wrapper and sourcing model to match them.
6. Set portfolio and queue rules around the resulting lane mix.
7. Choose capability builds only after the lane, state foundation, envelopes, and measurement model are stable enough to support rational investment.

## Immediate Working Interpretation Of The Research Bundle

The researcher bundle reinforces these working conclusions:

- order performance and agent capability must remain separate systems
- specialization should be calibrated by completed-order evidence, not category labels
- rescue burden and regret rate are first-class signals
- strategic-core and stable-throughput lanes should be treated as different portfolio roles
- qualification should be stated inside narrow success envelopes, not broad editing categories

The bundle does not yet fully settle:

- the first commercial lane
- whether partial-fulfillment outputs are strong external products
- whether the external wrapper should be deliverables, recurring support, or hybrid

Those remain the top unresolved decisions.

## Quality Check

This goal stack is complete only if:

- each goal has explicit success criteria
- each goal has explicit blocked-by conditions
- each goal has explicit non-goals
- dependencies between goals are clear and directional
- the first-lane decision visibly controls downstream goals
- research-dependent goals are distinguishable from architecture-dependent goals
- each top-level goal can later be converted into a standalone `/goal` command without rewriting the intent

## Bottom Line

The redesign should not progress as:

- broad pipeline improvement
- broad market exploration
- broad feature building

It should progress as:

- choose one lane
- define the work-order system around that lane
- define the qualified envelope for that lane
- measure the lane honestly
- shape the offer around what the lane can actually close
- protect the queue while learning
- build only what unlocks more of the right work
