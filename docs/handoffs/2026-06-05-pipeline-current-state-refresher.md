# Pipeline Current-State Refresher

## Purpose

This memo is a compact refresher on:

- the scope of the project
- the main stages of the pipeline
- what is already mechanically proven
- what is still evidence-limited
- what work remains to make the system more complete

It is written from current repo truth, not from aspirational plans.

## Project Scope

The project is an auditable gameplay highlight pipeline.

Its job is to move from raw gameplay media to local highlight artifacts and, later, downstream posting and metrics.

The intended system is not a single “find highlights” script.

It is a staged multimodal pipeline with these responsibilities:

1. accept gameplay media or fixture-like inputs
2. generate cheap proposals or candidate windows
3. extract game/runtime signals with provenance
4. normalize signals onto one time base
5. fuse signals into candidate highlight claims
6. review and calibrate those claims
7. export locally inspectable highlight artifacts
8. later connect export lineage to posting and downstream analytics

The current implementation also includes:

- game-pack onboarding
- published asset/template management
- review bridges
- calibration/replay workflows
- registry-backed workflow state
- local export and candidate lifecycle surfaces

## Core Architectural Idea

The system is intentionally separated into layers.

That separation matters because each layer answers a different question:

### 1. Ingest and proposal generation

Question:

- what media are we analyzing, and where is it worth spending more compute?

### 2. Runtime signal extraction and normalization

Question:

- what concrete signals seem present, where, and when?

### 3. Fusion and candidate scoring

Question:

- do the normalized signals support a meaningful highlight candidate?

### 4. Review, calibration, and replay

Question:

- are the system’s decisions defensible against reviewed evidence?

### 5. Selection and local export packaging

Question:

- which reviewed candidates become local highlight artifacts?

### 6. Posting and downstream metrics

Question:

- what was posted, and how did it perform?

That is the core project shape.

## What We Have Actually Been Working On

The current branch of work has mostly been about:

- highlight detection
- multimodal signal mapping
- runtime sidecar generation
- fusion into candidate events
- review and calibration infrastructure
- game-pack onboarding for richer signal families
- local export-readiness

In practical terms, yes:

- “highlight detection and multimodal signal mapping” is still the correct summary of the main active work

The repo has also grown a lot of surrounding support systems needed to make that credible:

- pack validation
- registry state
- replay viewers
- review bridges
- export lineage
- contract audits

## What Is Mechanically Proven

The repo has a bounded local `call_of_duty` path that is already mechanically proven.

Current proven path:

1. real sample clip is available locally for testing
2. `runtime_analysis_v1` can be generated from that clip
3. runtime review can be prepared and applied
4. runtime calibration can run on reviewed runtime sidecars
5. fusion can generate `fused_analysis_v1`
6. fused review can be prepared and applied
7. highlight selection can be exported locally
8. workflow/export batch artifacts can be created locally
9. repo-quality health gate is green

This means:

- the pipeline can run locally
- the multimodal path is not hypothetical
- at least one end-to-end local path from media to local export artifact exists

## What Is Not Yet Proven

Several important things are still not proven in the stronger sense.

### 1. Broad source coverage

We do not have enough representative clips to fully understand the requirements of every pipeline feature across:

- multiple games
- multiple HUD families
- multiple editing styles
- multiple overlay/no-overlay conditions
- multiple event families

### 2. Generalized `call_of_duty` runtime surface coverage

We have enough `call_of_duty` evidence to prove and falsify some things, but not enough to generalize all runtime features confidently.

Examples:

- `reward_banner` produced one clean narrow pilot, but did not generalize
- the top-right event-card shell anchor was falsified
- medal assets were structurally promoted, but still did not validate well on the measured clip set

### 3. Publish-cleared operation

The current local path is:

- mechanically proven
- locally testable
- not equivalent to a publish-cleared production workflow

### 4. Full downstream loop

The repo has downstream ledger and metrics surfaces, but the strongest currently proven path is still local export readiness, not a complete production posting loop.

## Current Main Blockers

There are two different classes of blocker.

### A. Evidence blockers

These are the biggest current blockers.

The main one today is:

- insufficient discriminative external evidence for the next `call_of_duty` top-right native event-card anchor

More generally:

- some feature questions are blocked by source coverage, not by code

### B. Confidence blockers

Even when the code runs, we still lack confidence for some features because:

- source inventory is narrow
- some signal families do not repeat cleanly
- some clips are mechanically usable but editorially weak

## Current Source Inventory Reality

Current source truth is mixed.

### What we do have

- one bounded real-media `call_of_duty` operator sample used for local testing
- additional downloaded `call_of_duty` measurement clips and probes
- lots of static pack assets and templates
- review/calibration/export artifacts on disk
- strong fixture coverage for many non-media workflow surfaces

### What we do not have

- a large canonical local library of representative gameplay clips across all needed conditions
- enough clip diversity to claim full understanding of every individual feature requirement
- enough same-family evidence to close the active top-right `call_of_duty` runtime branch

So the answer to “do we have enough source clips for full understanding?” is:

- no

We have enough to prove parts of the system and identify real blockers.
We do not have enough to treat all requirements as settled.

## Current Stage Map

This is the practical stage map for the project as it exists now.

### Stage 1. Game-pack and asset onboarding

Work:

- source wiki or other reference material
- curate usable signal/asset rows
- bridge curated rows into onboarding drafts
- publish validated assets/templates into canonical packs

Why it matters:

- runtime cannot emit signals the published pack cannot represent

### Stage 2. Proposal and runtime extraction

Work:

- analyze clips
- run ROI/template/runtime extraction
- emit sidecars with explicit evidence and timing

Why it matters:

- this is where raw media becomes inspectable signal evidence

### Stage 3. Runtime review

Work:

- inspect reviewed runtime decisions
- accept/reject or classify ambiguous runtime evidence

Why it matters:

- runtime scoring alone is not enough

### Stage 4. Calibration and replay

Work:

- compare reviewed evidence against scoring behavior
- check whether thresholds and logic are defensible

Why it matters:

- a system can be internally consistent and still wrong

### Stage 5. Fusion

Work:

- combine normalized signals
- produce candidate highlight claims with provenance

Why it matters:

- runtime evidence is not the same as a highlight candidate

### Stage 6. Fused review

Work:

- review candidate highlight claims
- approve or reject them

Why it matters:

- this is the decision surface for candidate advancement

### Stage 7. Highlight selection and local export

Work:

- create selection manifests
- create export batches
- propagate candidate lifecycle state

Why it matters:

- proves the pipeline can produce local highlight artifacts

### Stage 8. Posting and downstream analytics

Work:

- posted ledgers
- metrics snapshots
- performance reporting

Why it matters:

- eventually closes the loop from detection to real downstream outcomes

This later stage exists, but it is not the strongest currently proven operating path.

## Current Priorities

The current repo focus is not “build everything at once.”

It is:

1. preserve the proven local `call_of_duty` path
2. improve repeatable runtime surface coverage without speculative detector expansion
3. keep review/calibration/export contracts hard and inspectable
4. avoid pretending that fixture success or one-clip pilots equal broad feature truth

## What Would Most Improve Confidence Next

If the goal is to improve understanding of feature requirements, the highest-value additions are:

### 1. Better representative source inventory

Especially:

- same-title, same-HUD-family clips that visibly contain the unresolved surface family
- clips that clearly separate native HUD from overlay/editorial surfaces
- clips that repeat the target signal across multiple examples

### 2. Better signal-family-specific packets

The current active example is:

- a packet that either recommends one discriminative top-right anchor or retires that family

### 3. More clip-backed validation for promoted pack features

Especially where:

- assets were structurally promoted
- but runtime validation is still weak on actual clips

## Bottom Line

The project is a staged, multimodal gameplay highlight pipeline with onboarding, runtime extraction, fusion, review, calibration, registry state, and local export surfaces.

The system is real and partially proven.

What is already true:

- the local pipeline can run
- the multimodal path can produce runtime, fused, review, and export artifacts
- the repo has strong auditability and contract surfaces

What is not yet true:

- we do not have full source coverage
- we do not fully understand the requirements of every feature from real clip evidence
- some active signal branches are still blocked by external evidence rather than code

So the correct current framing is:

- architecture and mechanics are substantially in place
- source-backed requirements understanding is still incomplete
- multimodal highlight detection and signal mapping remains the main active mission
