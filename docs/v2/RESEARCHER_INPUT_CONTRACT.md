# Researcher GPT Input Contract

This document defines the operating contract between the external researcher GPT and Codex.

The goal is not to gather “research” in the abstract. The goal is to produce decision-ready packets that support one concrete next repo action with enough evidence to implement, validate, and stop cleanly.

Use this doc when you need to answer:

1. what kind of packet the researcher should produce next
2. how specific that packet must be
3. what inputs keep Codex autonomous for longer stretches
4. what packet queue shape keeps the local backlog healthy

The exact packet envelope still lives in [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md). The queue rules still live in [BACKLOG_OPERATING_MODEL.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/BACKLOG_OPERATING_MODEL.md). The internal researcher-side routing doctrine lives in [INTERACTION_ROUTING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/INTERACTION_ROUTING.md). This file governs the higher-level input contract.

## Core Rule

Every researcher packet should:

- support exactly one next repo action
- be specific enough that Codex does not need to choose the next interpretation step
- include enough evidence to implement, validate, and know when to stop

The important constraint is not volume. It is decision completeness.

## Packet Identity Rule

When Codex exports a researcher-facing packet bundle, the bundle should be selected by semantic identity first, not by timestamp alone.

Operational rule:

- prefer semantic-first packet roots and filenames
- keep timestamps as provenance, not as the primary human discriminator
- include an explicit packet-identity artifact when the export surface supports it

For exported wiki research packets, the handoff should use:

- the semantic-first packet root
- the `*_packet_identity.json` file
- the `*_SEND_THESE_FILES_FIRST.txt` file
- and it should reject any bundle marked by:
  - `SUPERSEDED_DO_NOT_UPLOAD.txt`
  - `superseded_status: do_not_upload`

If a human could plausibly upload the wrong packet without opening files, the packet identity is still too weak.

## Internal Routing Boundary

The researcher may use internal routing logic to improve the next interaction move before producing an external artifact.

That internal routing layer may classify:

- `intent_mode`
- `abstraction_level`
- `uncertainty_state`
- `execution_readiness`
- `weakest_blocking_axis`

It may also use:

- target-artifact selection
- checkpoint cards
- weakest-blocker question selection

Those routing aids are internal. They should improve output quality without becoming mandatory external packet fields in this slice.

## External Output Contract

The researcher should produce exactly two external artifact types:

1. `decision_ready_packet`
2. `appendix`

Default rule:

- if the work is meant to change the repo, the final artifact should be a packet
- not a summary
- not a memo
- not a long narrative report

The appendix exists only for:

- overflow evidence
- long source excerpts
- secondary examples

It should never replace the primary packet handoff.
Internal classification frameworks may guide researcher reasoning, but they should not leak into Codex-facing artifacts unless explicitly requested.

## Required Metadata

Every packet should include:

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

## Required Sections

Every packet should fill these sections:

1. `Decision Target`
2. `Current Repo Truth`
3. `Evidence Bundle`
4. `Structured Findings`
5. `Recommendation`
6. `Acceptance Target`
7. `Open Uncertainties`

If any of those sections are missing, the packet is usually not implementation-ready.

## Mandatory Recommendation Standard

Every packet recommendation should answer:

- what Codex should do next
- what Codex should not do next
- how the repo will know it worked

A recommendation without an acceptance target is incomplete.

## Information That Maximizes Codex Autonomy

### A. Decision-complete source packets

Use for onboarding, asset promotion, and signal sourcing.

Required information:

- exact target names
- exact aliases or text variants
- source URLs
- stable image URLs when possible
- live HUD screenshots
- tight crops
- clip timestamps
- explicit exclusions
- explicit false positives
- title-family notes
- confidence level per candidate

Without this, Codex spends time converting vague findings into operational inputs instead of changing the repo.

### B. Sample-set packets

Use when a question depends on real gameplay clips.

Required information:

- exact clip URL or local path
- why the clip matters
- start and end timestamps
- what signal is expected to be visible
- whether the signal is native HUD, killfeed, reward banner, OCR text, or post-production overlay
- whether the clip is representative or exploratory

This prevents structurally correct packs from being tested against the wrong sample family.

### C. Runtime signal packets

Use for visible in-clip signals and extraction strategy.

Required information:

- signal name
- where it appears on screen
- when it appears
- how long it remains visible
- whether it is icon, text, or banner
- what similar-looking negatives exist
- what clips clearly contain it
- what clips clearly do not

This is what lets Codex decide whether the next slice belongs in onboarding, ROI, OCR, or runtime logic.

### D. Fusion packets

Use for multi-signal corroboration and event logic.

Required information:

- exact input signal families
- exact timing relationship
- what should fuse
- what should not fuse
- what a meaningful corroborated event looks like
- example clips or timestamps

Without this, fusion work becomes guesswork.

### E. Editorial packets

Use for hook and packaging decisions.

Required information:

- what counts as a strong opener
- what counts as mechanically real but editorially weak
- examples of natural hooks
- examples of acceptable synthetic hooks
- examples that should still be rejected
- why

This is what allows the pipeline to move beyond “the system exported something” into “the output is actually good.”

## Packet Types The Researcher Should Produce

The routed artifact set that should exist before packet production includes:

1. `research_note`
2. `decision_ready_packet`
3. `implementation_ticket`
4. `codex_handoff_brief`

Default rule:

- use `research_note` when the work is still exploratory or execution readiness is low
- use `decision_ready_packet` when the next repo action is known and Codex execution is the goal
- use `implementation_ticket` when implementation-facing task framing is needed but the work is not yet a direct Codex handoff
- use `codex_handoff_brief` when the implementation boundary is already stable and repo-context translation is the next step

Do not route to `implementation_ticket` or `codex_handoff_brief` until execution readiness is high enough.

### 1. Asset Promotion Packet

Use for medals, icons, portraits, HUD widgets, and onboarding candidate promotion.

Must include:

- exact names
- crops
- source pages
- exclusion set
- recommended first subset

### 2. Runtime Signal Packet

Use for visible in-clip signals.

Must include:

- signal location
- visibility duration
- extraction method candidates
- positive examples
- negative examples

### 3. Fusion Packet

Use for multi-signal combinations that should count as meaningful events.

Must include:

- timing relationship
- corroboration rule
- non-fuse cases

### 4. Editorial Packet

Use for hook logic and packaging truthfulness.

Must include:

- strong opener examples
- rejection cases
- acceptance criteria

### 5. Sample Validation Packet

Use for proving whether the current sample set is even appropriate for the intended signal family.

Must include:

- clips or VOD references
- timestamps
- visible-signal notes
- title-family caveats
- recommendation on whether to keep or replace the sample family

## Readiness Rubric

Before handing a packet to Codex, score it against this checklist.

A packet is `ready` only if all seven pass:

1. `Decision Completeness`
- one exact decision
- no broad topic framing

2. `Repo Truth Quality`
- current repo state is described with paths and the current blocker
- no invented repo truth

3. `Evidence Sufficiency`
- enough concrete evidence exists to support implementation
- not just source links or prose

4. `Recommendation Clarity`
- the next repo action is obvious
- no “maybe do something with this”

5. `Acceptance Target Quality`
- success is observable in repo behavior, artifacts, tests, or outputs

6. `Uncertainty Hygiene`
- blocking vs non-blocking uncertainty is explicit
- uncertainties do not leak into the recommendation

7. `Layer Purity`
- the packet stays in one main layer or one explicit boundary

Default pass or fail rule:

- if any category materially fails, mark the packet `draft`, not `ready`

## Packet Routing Matrix

Choose the packet type by blocker type:

- source ambiguity or asset promotion need:
  - `Asset Promotion Packet`
- wrong or unknown sample family:
  - `Sample Validation Packet`
- visible signal exists but extraction path is unclear:
  - `Runtime Signal Packet`
- signal exists but combined event meaning is unclear:
  - `Fusion Packet`
- signal works mechanically but output quality is unclear:
  - `Editorial Packet`

Default routing rule:

- choose the earliest layer that still contains the real blocker
- do not jump to fusion or editorial while runtime ambiguity is unresolved

## What The Researcher Should Not Hand To Codex

These inputs slow implementation down:

- long narrative summaries with no decision target
- broad “research Call of Duty medals” notes
- screenshot dumps with no labels
- URLs with no statement about why they matter
- source sets with no exclusions
- recommendations with no acceptance target
- mixed packets that combine onboarding, runtime, fusion, and editorial in one blob
- “possible ideas” lists with no ranking or next action

Do not build yet:

- packet lifecycle governance
- packet lineage tracking as a first-class system
- stale-packet governance
- semantic-governance integration for packets
- large packet analytics systems

Those may be valid later, but they are not current throughput bottlenecks.

## Queue Shape That Keeps Codex Autonomous

The researcher should maintain a queue of packets in this order:

1. `current blocker packet`
2. `next-layer packet`
3. `validation packet`
4. `fallback packet`

A healthy steady-state queue for Codex is:

- `1` active execution target
- `3-5` `ready_now` tasks
- `3-5` `needs_research_packet` tasks
- each `needs_research_packet` item naming the exact packet required

When `ready_now` reaches zero, progress stalls. When packet needs are vague, progress becomes noisy.

Do not maintain a large abstract research backlog for Codex. Maintain a small, live, execution-facing queue.

## Stop Conditions And Escalation

Stop research when:

- the next repo action is obvious
- evidence is sufficient for implementation
- acceptance criteria are measurable
- remaining uncertainty does not block execution

Escalate to another packet when:

- the current sample family looks wrong
- the extraction method is still unclear
- the signal seems mechanically valid but not semantically meaningful
- the output may be mechanically valid but editorially weak

## Current Highest-Value Packets

### 1. Highest priority: `call_of_duty` top-right anchor packet

Reason:

- the bounded local `call_of_duty` runtime path is mechanically proven
- `reward_banner` is capped as a narrow validated pilot
- the first top-right shell anchor is falsified
- the current blocker is now discriminative anchor specificity inside the top-right native event-card family

Required contents:

- one recommended anchor candidate only
- exact clip windows
- screenshots and crops
- timestamps
- whether the candidate is native UI or overlay
- explicit exclusions
- why the recommendation is stronger than:
  - the failed shell anchor
  - weak local OCR evidence
  - wrong-surface same-title candidates

Supporting repo files:

- [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md)
- [2026-06-04-call-of-duty-top-right-anchor-packet-request.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-packet-request.md)

### 2. Alternative path: medal-visible sample packet

Reason:

- the promoted medal icons may already be valid
- the current sample set may simply be the wrong signal family

Required contents:

- clips that visibly contain native medal badge icons
- timestamps
- screenshots and crops
- title-family notes
- notes on whether the current ROI assumptions look compatible

### 3. Validation or fallback packet for the chosen signal family

After packet 1 or 2, the next packet should cover:

- repeatability across additional clips
- false positives and exclusions
- whether the branch should expand or retire

### 4. Fusion packet for the chosen signal family

Once runtime is plausible, the next packet should cover:

- what combined event the signal should support
- whether it should corroborate equipment, killfeed, or reward context
- what counts as meaningful
- what should not count

### 5. Editorial packet for the same sample family

After runtime and fusion proof, the next packet should cover:

- whether the signal helps hook quality
- what an editorially useful opener looks like for that signal

## Prompting Guidance For The Researcher GPT

Use instructions like:

- produce a packet that supports one next repo action
- classify state before asking another question
- identify the target artifact before choosing the next interaction move
- do not give generic background
- give exact names, timestamps, URLs, and exclusions
- state what Codex should do next
- state how the repo will know it worked

Good packet behavior:

- focus on one decision only
- separate facts from recommendations
- include exact evidence references
- include exclusions and false positives
- include an acceptance target
- mark uncertainties explicitly
- prefer 10 strong examples over 100 weak ones

Good routing behavior before packet production:

- choose the smallest interaction mode that can change the next artifact decision
- map the weakest blocker to the next question type
- use recognition tasks or constrained choices when articulation support is low
- decompose before tasking

## Default Operating Loop

Use this loop:

1. identify the current blocker
2. commission one packet for that blocker
3. hand Codex the completed packet
4. let Codex implement and validate
5. commission the next-layer packet while Codex is working
6. keep one fallback packet in reserve

That is the simplest way to keep Codex autonomous for the longest time without guesswork.

Before packet production, the researcher should use this internal loop:

1. classify state
2. identify the target artifact
3. identify the weakest blocking axis
4. choose the smallest interaction mode that can resolve it
5. stop when decision sufficiency is reached

## Self-Test Before Handoff

Before handing a packet to Codex, ask:

1. `Could Codex act immediately?`
- if not, the packet is not ready

2. `Could Codex validate immediately?`
- if not, the acceptance target is weak

3. `Would Codex ask a clarification question first?`
- if yes, the packet still contains interpretation burden

4. `Is there one exact next repo action?`
- if not, the packet is too broad

5. `Are exclusions and false positives explicit?`
- if not, the packet will create downstream ambiguity

6. `Did I route too early into tasking?`
- if critical objective, constraint, or boundary information is unresolved, do not produce `implementation_ticket` or `codex_handoff_brief`

## Current Defaults

- [RESEARCH_PACKET_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/RESEARCH_PACKET_TEMPLATE.md) remains the canonical envelope and field template.
- [BACKLOG_OPERATING_MODEL.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/BACKLOG_OPERATING_MODEL.md) remains the canonical queue-shape and readiness doc.
- The current highest-value blocker is no longer medal onboarding mechanics; it is signal-surface mismatch on the measured `call_of_duty` clips.
- The next best external input is a top-right anchor packet unless the project intentionally pivots to medal-visible replacement samples or explicitly retires the top-right family.
- The researcher should optimize for decision-ready packets, not exhaustive topic coverage.
