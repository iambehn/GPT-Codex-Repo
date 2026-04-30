# Orchestrator Layer MVP Action Roadmap

This roadmap translates [Scheduler_OrchestratorNotes.md](/Users/tj/Downloads/Scheduler_OrchestratorNotes.md) into a repo-facing plan for an MVP orchestrator layer.

It stays narrow and operational. The focus is the minimal coordination layer needed to keep the current and planned pipeline stages flowing safely:

- stage and state tracking
- task and job contracts
- retries and failure handling
- non-overlapping work execution
- safe handoff between automated stages and human review

It does not become a full workflow-platform plan or a large distributed-systems design.

---

## Already Covered

These ideas are already implied by the existing docs:

- ingestion and staged detector flow
- proxy and fusion stages as future work units
- quarantine and review as a human recovery path
- training export and ML scoring as later attachable stages

The orchestrator roadmap should build on that baseline rather than restate it.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. Orchestrator Vocabulary And Boundaries

Turn the note’s broad scheduler language into precise project guidance.

Required roadmap behavior:

- define the orchestrator as the traffic controller for existing pipeline stages
- distinguish:
  - `task` as a single runnable unit
  - `job` as a multi-step workflow
  - `clip/window state` as the canonical lifecycle record
- keep the first orchestrator local, file-aware, and state-machine-driven
- explicitly treat human review as a blocking external state, not a background worker step

This keeps the orchestrator scoped to coordination, not feature logic.

### B. State Machine And Core Schemas

Make the state model the first concrete backlog item.

Required backlog items:

- define a canonical clip or window lifecycle with explicit stages and statuses
- define a minimal `task` schema for runnable units
- define a minimal `job` schema for ordered multi-step workflows
- define a minimal `scheduler_state` view for operational visibility

Required behavior:

- one clip or window has one canonical current stage
- downstream stages depend on prior stage completion
- transitions are explicit and auditable

Important boundary:

- reuse existing window and clip artifacts where possible
- do not invent a large event bus or workflow DSL

### C. Worker Loop, Locks, And Retry Semantics

Translate the note’s “things should keep flowing smoothly” goal into a safe MVP worker design.

Required backlog items:

- a single local worker loop as the first implementation target
- file or record locking to prevent duplicate processing
- bounded retry semantics
- dead or failure states for tasks that exceed retry policy
- explicit stage advancement only after successful completion

Important boundary:

- no multi-process worker pool is required for v1
- avoid concurrent writers to the same artifact paths in the first implementation

### D. Human Review And Queue Handoff

Define how the orchestrator interacts with quarantine and review without breaking the human-in-the-loop loop.

Required backlog items:

- enqueue-to-review as a first-class stage transition
- stop automated processing while an item is awaiting human review
- resume or route based on review outcome
- keep review-priority refresh as a planned orchestrator concern, but not a full optimization system

This workstream should make the handoff between automation and manual review explicit instead of implicit.

### E. Failure Handling, Logging, And Recovery Boundaries

Make failure behavior part of the design, not an afterthought.

Required backlog items:

- structured logging expectations for orchestrated tasks
- explicit failure categories:
  - retryable
  - terminal
  - waiting on human input
- recovery rules for crashed or interrupted work
- enough status visibility to detect silent stalls

Important boundary:

- no full observability platform
- just enough state and logs to understand what ran, what failed, and what is waiting

---

## Deferred

These ideas from the note are valid, but should stay out of the first orchestrator implementation phase:

- large multi-worker pool design
- cloud or distributed queue infrastructure
- broad maintenance scheduling beyond basic hooks
- full distribution and retraining orchestration
- full platform-style observability and dashboards

---

## Implementation Order

Build in this order:

1. Define orchestrator vocabulary and scope boundaries.
2. Define the state machine plus task and job schemas.
3. Add the single-worker loop, locking, and retry model.
4. Add human-review handoff and resume behavior.
5. Add failure, logging, and recovery rules around the same state machine.

This order keeps the orchestrator grounded in state correctness before adding throughput or concurrency ambitions.

---

## Future Test Targets

The implementation following this roadmap should prove:

- every clip or window has one canonical stage and status
- a task cannot run before its declared dependency is complete
- duplicate work on the same clip or window is prevented by locks or state checks
- retryable failures do not corrupt state
- human-review items stop automated advancement until review outcome is recorded
- silent stalls are detectable from scheduler state and logs
- no distributed queue or large worker pool is required for the first orchestrator implementation pass

---

## Assumptions

- this document is a roadmap, not code
- the correct file name is `ORCHESTRATOR_ACTIONS.md`
- this pass is limited to an MVP local orchestrator for the current gaming pipeline and planned proxy or ML bridge
- the roadmap should complement the existing blueprint and roadmap stack by defining the coordination layer those documents assume, not replace them
