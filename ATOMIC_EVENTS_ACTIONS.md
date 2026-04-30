# Atomic Event Layer Action Roadmap

This roadmap translates [AtomicEventsSignalMapping.md](/Users/tj/Downloads/AtomicEventsSignalMapping.md) into a repo-facing plan for the current-pipeline subset of atomic-event work.

It stays intentionally narrow. The focus is:

- turning detector outputs into a canonical event layer
- standardizing signal-to-event mapping
- making later decision logic more auditable

It is not a full editing-system plan. Transformative editing features remain deferred until the event layer is stable and useful.

---

## Already Covered

These high-level ideas are already present in [NICESHOT_YOLO_BLUEPRINT.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/NICESHOT_YOLO_BLUEPRINT.md) and should not become new backlog by themselves:

- layered detector pipeline
- explainable scoring and composite worthiness thinking
- config-driven game support
- quarantine and review as recovery paths

The atomic-event roadmap should build on that baseline rather than restating it.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. Canonical Atomic Event Contract

Introduce an atomic-event layer as the stable interface between detector evidence and later clip decisions.

Define a minimal event shape:

- `event_id`
- `event_type`
- `start_ts`
- `end_ts`
- `anchors`
- `signals`
- `contributing_sources`
- `confidence`
- `entropy`
- `tags`
- `metadata`

Required behavioral rules:

- event types must stay small and controlled
- start with current-pipeline-safe categories:
  - `KILL`
  - `MULTIKILL`
  - `OBJECTIVE`
  - `REACTION`
  - `ACTION_SPIKE`
  - `INSIGHT`
  - `PUNCHLINE`
- every future decision must be explainable from event ids plus contributing signals

This event contract is the semantic layer for the current pipeline, not a full editing schema.

### B. Signal-To-Event Mapping

Turn detector outputs into event candidates through deterministic mapping and fusion.

Map these source families first:

- visual detectors
- audio spikes
- proxy signals
- transcript or semantic signals only if they already exist in the pipeline

Required mapping rules:

- normalize detector evidence before event emission
- apply entropy or quality penalties before emitting events
- group temporally nearby signals into event candidates
- allow multiple sources to support the same event
- store compact summaries of contributing evidence rather than raw detector payloads

Important boundary:

- do not assume a learned event classifier in the first pass
- the initial event mapper stays heuristic and auditable

### C. Event-Level Judge Integration

Use atomic events to improve later clip decisions without replacing deterministic judging.

Required behavior:

- downstream judgment should reason about what happened, not only which detector fired
- agreement across sources should increase trust
- weak single-source events should lower trust or force quarantine
- explanations should record:
  - which event types were present
  - which sources supported them
  - which penalties reduced trust

This keeps the judgment path explainable and aligned with the existing blueprint.

### D. Debug And Audit Surfaces

Expose atomic events through the existing lightweight debugging surfaces.

Required behavior:

- sidecar or debug output should show emitted events and contributing signals
- event traces should be inspectable without a separate dashboard
- event output should be structured so replay or review tools can surface it later

Do not add in this phase:

- a standalone editor UI
- a renderer
- a dashboard
- brand or template infrastructure

---

## Deferred

These ideas from the note are valid, but should stay out of the first atomic-event implementation phase:

- full transformative editing stack
- B-roll selection systems
- commentary, TTS, and auto-narration
- branded template rendering
- visual metaphor libraries
- full renderer or planner orchestration
- learned edit ranking and broader feedback loops beyond simple auditability

---

## Implementation Order

Build in this order:

1. Define the atomic-event vocabulary and minimal contract.
2. Define the signal-to-event mapper rules for current detector families.
3. Add event-level explanation and audit output.
4. Integrate atomic events into later judgment logic.
5. Defer transformative editing features until the event layer proves stable.

This keeps the work anchored to detector quality and decision clarity instead of prematurely building editing systems.

---

## Future Test Targets

The implementation following this roadmap should prove:

- event vocabulary remains small and controlled
- one detector source can emit an event with explicit uncertainty
- multi-source agreement produces stronger event confidence than a lone weak source
- noisy or contradictory signals produce penalties or quarantine-biased outcomes
- sidecar or debug outputs can show:
  - event ids
  - event types
  - contributing sources
  - confidence and entropy
  - penalty reasons
- no editing-system features are required for the first atomic-event implementation

---

## Assumptions

- this document is a roadmap, not a code spec
- the correct file name is `ATOMIC_EVENTS_ACTIONS.md`
- this pass is limited to the current-pipeline subset of the note
- the atomic-event layer is being introduced as a semantic interface for detector fusion and later judgment, not yet as a full edit planner or renderer contract
