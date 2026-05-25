# Backlog Operating Model

This document defines how to keep a healthy queue of work for Codex in this repo.

The goal is not to estimate hours. The goal is to keep enough decision-ready tasks available that local work does not stall waiting for clarification.

## Core Idea

Track work by decision readiness, not by abstract priority alone.

Use four buckets:

1. `ready_now`
2. `needs_research_packet`
3. `blocked_on_conflict`
4. `deferred`

## Bucket Definitions

### `ready_now`

Use when:

- the objective is clear
- the governing repo surface is known
- the acceptance criteria are explicit
- no outside input is required

These are the tasks Codex should consume first.

### `needs_research_packet`

Use when:

- the next step is known
- but the repo lacks enough source truth to act responsibly

This is the most important bucket to manage well.

These tasks should always name the exact packet needed.

Example:

```text
Task: promote first call_of_duty medal subset
Needs: medal-specific researcher packet with HUD medal names and crops
```

### `blocked_on_conflict`

Use when:

- two repo surfaces disagree
- ownership is ambiguous
- a contract change would silently expand scope

These are real blockers. They should be few.

### `deferred`

Use when:

- the work is real but not currently valuable
- or the current execution target does not need it yet

Examples:

- downstream platform posting expansion
- new game support before the current game is stable
- optional dashboards or analytics layers

## Healthy Queue Shape

A good steady-state queue usually looks like:

- `1` current execution target
- `3-5` `ready_now` tasks
- `3-5` `needs_research_packet` tasks
- `0-2` `blocked_on_conflict` tasks
- a larger `deferred` pool

When `ready_now` reaches zero, progress slows.
When `needs_research_packet` is vague, progress becomes noisy.

## Task Record Template

Each backlog item should include:

```yaml
task_id: short-stable-id
status: ready_now | needs_research_packet | blocked_on_conflict | deferred
objective: one sentence
pipeline_layer: onboarding | detection | runtime | fusion | review | calibration | export | hook | downstream
current_truth: short summary
needed_inputs: list of exact missing inputs
acceptance_criteria:
  - concrete observable outcome
dependencies:
  - optional task ids or packet ids
non_goals:
  - optional scope boundary
```

## What Makes A Task Actionable

A task is actionable when:

- the boundary is narrow
- the owning layer is clear
- the next observable repo change is obvious

Good task:

```text
Bridge medal-specific wiki packet into onboarding draft and verify medal rows appear as pending review work.
```

Bad task:

```text
Improve Call of Duty highlights.
```

The bad task spans too many layers:

- asset sourcing
- runtime detection
- fusion
- review
- editorial hook quality

## When To Split A Task

Split a task when:

- it crosses multiple pipeline layers
- it has multiple independent failure modes
- it mixes source acquisition with downstream validation

Example split:

1. acquire medal-specific source packet
2. bridge packet into onboarding
3. publish first medal subset
4. verify runtime medal emission
5. verify richer fused events
6. re-evaluate editorial viability on the canonical sample

## What The Researcher Should Receive

For any `needs_research_packet` item, give the researcher:

- the task objective
- the current repo truth
- the exact packet type needed
- the acceptance target
- explicit exclusions

Do not send only a broad topic.

Good:

```text
Need a medal-specific asset promotion packet for call_of_duty.
Current repo truth: raw wiki bundle is dominated by contracts, calling cards, blueprints, watches, logos, and map imagery.
Need: true HUD medal names, crops, and exclusions.
Acceptance target: onboarding draft gains a clean first medal subset.
```

## Signs The Backlog Is Unhealthy

The backlog needs cleanup when:

- most tasks say “improve” or “research” without a decision target
- many tasks depend on missing inputs that are not named
- there are many blocked items but no explicit conflict owner
- completed work does not produce a new actionable next step

## Current Practical Use

For this repo, the backlog model should be used to keep:

- one active local execution target in the operator pack
- a short list of immediately executable hardening or contract tasks
- a separate short list of researcher packets needed next

That is enough to keep Codex moving without turning the queue into project-management clutter.
