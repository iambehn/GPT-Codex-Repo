# Research Packet Template

This file defines the exact packet envelope.

For the broader operating contract between the researcher GPT and Codex, use [RESEARCHER_INPUT_CONTRACT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCHER_INPUT_CONTRACT.md).

Use this template when a researcher is gathering information for the gameplay highlight pipeline.

The goal is not to produce a long note dump. The goal is to produce a packet that is decision-complete enough to drive the next repo change.

## What A Good Packet Does

A good packet answers five questions:

1. what decision is this research for
2. what layer of the pipeline it affects
3. what evidence supports the conclusion
4. what Codex should do next
5. how the repo will know the work succeeded

If a packet does not answer those five questions, it is usually not ready for implementation.

## Required Metadata

```yaml
packet_id: YYYY-MM-DD-short-slug
status: draft | ready | superseded
owner: researcher_name_or_surface
decision_target: short sentence naming the decision
pipeline_layer: onboarding | detection | runtime | fusion | review | calibration | export | hook | downstream
game: call_of_duty | marvel_rivals | valorant | other
priority: P0 | P1 | P2
confidence: authoritative | strong_candidate | exploratory
supersedes: optional prior packet ids
```

## Packet Structure

### 1. Decision Target

State the exact decision this packet is meant to support.

Good:

- promote a first `call_of_duty` medal subset into onboarding review
- decide whether a hook heuristic should stay advisory or become blocking
- choose the first runtime signal family for a new game pack

Bad:

- improve the pipeline
- find better assets
- research Call of Duty medals

### 2. Current Repo Truth

State what the repo already says or does today.

Include:

- relevant paths
- current behavior
- current blocker
- current validation or test state if known

Example:

```text
Current published call_of_duty pack has hero_portrait and equipment_icon coverage but no promoted medal_icon coverage.
Raw wiki draft bundle exists at assets/games/call_of_duty/drafts/wiki/20260430T015758Z.
Bridge and pre-bridge curation now work mechanically, but the current raw bundle does not contain usable multikill medal rows.
```

### 3. Evidence Bundle

List the evidence that supports the recommendation.

Preferred evidence types:

- live HUD screenshots
- cropped medal or UI images
- source pages with stable URLs
- clip timestamps showing the target signal in context
- controlled lists of names and aliases
- explicit false-positive examples

For each item, include:

```yaml
- evidence_id: short-id
  type: screenshot | crop | source_page | clip_timestamp | table | note
  path_or_url: exact path or URL
  why_it_matters: one sentence
  trust_level: authoritative | strong_candidate | exploratory
```

### 4. Structured Findings

Convert raw research into compact facts.

Use bullets or small tables, not long narrative.

Required categories when relevant:

- target names
- aliases
- exclusions
- source quality notes
- ambiguity notes
- negative examples
- patch or version caveats

Example:

```text
Target medals:
- Double Kill
- Triple Kill
- Fury Kill

Exclusions:
- calling cards
- contracts
- weapon blueprints
- watches
- logos
- map or season images
```

### 5. Recommendation

State the exact action Codex should take next.

Good:

- import these 12 medal names as the first onboarding subset
- add a new curation profile named `elimination_streak`
- leave the current hook threshold unchanged and add a regression instead

Bad:

- probably do something with these
- these might be useful later

### 6. Acceptance Target

Define what repo-visible outcome should change if the recommendation is implemented.

Examples:

- onboarding draft gains `medal_icon` candidates for the listed subset
- runtime emits `medal_visibility` on the provided sample clip
- fused analysis can produce `ability_or_equipment_plus_medal_combo`
- hook rollup changes from `mechanics_only` to `mixed`

### 7. Open Uncertainties

List what still is not known.

Do not hide uncertainty inside recommendations.

For each uncertainty, say whether it blocks implementation:

```yaml
- question: short question
  blocks_implementation: true | false
  recommended_next_step: one sentence
```

## Packet Quality Bar

Use this checklist before handing a packet to Codex.

The packet should be:

- specific enough to support one next repo action
- scoped to one pipeline layer or one boundary between layers
- explicit about exclusions and false positives
- backed by evidence, not just prose
- clear about whether the recommendation is authoritative or exploratory

The packet should not be:

- a long general summary
- a mixed bag of unrelated ideas
- a dump of screenshots with no conclusion
- a recommendation with no acceptance target

## Packet Types

### A. Asset Promotion Packet

Use for onboarding, templates, HUD assets, medals, icons, portraits.

Must include:

- exact names
- crops
- source pages
- exclusion set
- recommended first subset

### B. Runtime Signal Packet

Use for event families, timing expectations, and runtime evidence behavior.

Must include:

- signal name
- where it appears
- timing expectations
- positive and negative examples

### C. Fusion Packet

Use for multi-signal combinations that should count as meaningful events.

Must include:

- input signal families
- required timing relationship
- what counts as corroboration
- what should not fuse

### D. Editorial Packet

Use for hook logic, packaging, and truthful opener guidance.

Must include:

- examples of natural hooks
- examples of synthetic but acceptable hooks
- examples that are mechanically real but editorially weak

## Recommended Output Shape

When a researcher is using another model or tool, ask it to return:

1. `decision target`
2. `current repo truth`
3. `evidence bundle`
4. `structured findings`
5. `recommendation`
6. `acceptance target`
7. `open uncertainties`

Anything longer should be treated as appendix material, not the main handoff.
