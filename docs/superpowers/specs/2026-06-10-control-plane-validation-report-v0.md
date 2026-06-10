# Control Plane Validation Report v0

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Apply the committed control-plane v0 architecture to one real historical workflow example and identify:

- where the model fits cleanly
- where the model breaks or becomes ambiguous
- the minimum v1 changes required

This report preserves the current v0 control-plane artifacts as the baseline. It does not add inventory, planning, or resource-model design.

## Selected Workflow

Representative workflow:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

Why this workflow was selected:

- it is a real persisted historical draft
- it has concrete source, candidate, derived-manifest, binding, QA, and publish-readiness artifacts
- it includes both successful sub-results and unresolved work
- it reaches a non-terminal inspected outcome: `needs_binding_review`

## Workflow Evidence

Observed evidence from the selected draft:

- `manifests/onboarding_state.json`
  - `phase_status = bindings_pending`
  - `source_count = 5`
- `manifests/assets_manifest.json`
  - `source_fetch_log` contains 5 fetched sources
- `manifests/derived_detection_manifest.yaml`
  - `row_count = 377`
  - `resolved_row_count = 112`
  - `unresolved_required_row_count = 265`
  - `hero_portrait`, `equipment_icon`, and `medal_icon` are active required families
- `catalog/bindings.csv`
  - 510 binding rows
  - accepted rows exist for resolved detections
- `catalog/qa_queue.csv`
  - 925 QA rows
  - unresolved rows include `missing_binding` and review-pending cases
- `pipeline.onboarding_publish_readiness.validate_onboarding_publish(...)`
  - `readiness = needs_binding_review`
  - `can_publish = false`
  - `binding_findings = 680`
  - `completeness_findings = 265`

## Represented Lifecycle

### Observed historical lifecycle

1. Source pages were fetched successfully.
2. Candidate assets were generated and stored.
3. A derived detection manifest was produced for 377 required rows.
4. 112 rows reached accepted binding state.
5. 265 required rows remained unresolved.
6. Publish-readiness inspection concluded the draft was not publishable and remained in `needs_binding_review`.

### Closest v0 control-plane representation

| Observed workflow condition | Closest v0 state or transition | Fit |
| --- | --- | --- |
| fetched source pages and populated candidate set | `raw_vod -> review_pack_ready` | weak |
| derived detection manifest plus bindings/QA surface ready for review | `review_pack_ready` | partial |
| accepted binding decisions on some rows | `review_pack_approved -> approved_clips_ready` | partial at row level only |
| unresolved required rows plus QA backlog | `blocked` or rejected review surface | partial |
| publish-readiness result `needs_binding_review` | inspection failure followed by non-terminal routing | strong |

## Where The Model Fit Cleanly

### 1. Inspection as target-state verification

The publish-readiness check behaves like a real inspection surface:

- it evaluates evidence already produced by earlier work
- it decides whether the target state is valid
- it produces a bounded outcome

This matches the v0 inspection doctrine well.

### 2. Failure attribution as a separate layer

The workflow already distinguishes failure causes instead of using one generic “not ready” status:

- missing binding
- unresolved required derived row
- needs better reference
- completeness shortfall

Those map directionally into the v0 failure architecture, even though not all of them land cleanly in the current family set.

### 3. Non-terminal routing after inspection

The final outcome is not “success” or “archived.”

It is a recoverable, still-active state:

- `needs_binding_review`

That aligns with the v0 idea that not all negative inspection outcomes should collapse into archive.

### 4. Qualification should attach to transitions, not people broadly

The workflow evidence is transition-specific:

- accepted binding creation
- publish-readiness inspection
- unresolved derived-row handling

This supports the current `subject x transition` qualification doctrine rather than broad worker-level trust.

## Where The Model Broke Or Became Ambiguous

### 1. State granularity is too coarse

The current v0 states assume a mostly single-artifact linear path:

- `review_pack_ready`
- `review_pack_approved`
- `approved_clips_ready`

The historical workflow is a mixed aggregate:

- one draft contains both resolved and unresolved rows at the same time
- the draft is neither simply “ready” nor “rejected”
- partial approval exists without whole-draft approval

This is the first major mismatch.

### 2. The current transition model is too linear for mixed sets

The workflow does not move as one artifact through one clean transition chain.

Instead it behaves like:

- batch population
- per-row acceptance
- per-row unresolved handling
- whole-draft publish inspection

The current transition catalog does not yet model:

- partial progress inside one aggregate artifact
- member-level state changes inside a batch-level state

### 3. Failure attribution overlaps between requirement and support

Some unresolved rows are clearly support-like:

- no candidate source image matched this detection row

Some are closer to requirement or completeness:

- required derived row remains unresolved

In practice the selected workflow shows that:

- missing support
- incomplete review surface
- incomplete population

can overlap inside one active draft. The current family set is still workable, but the boundaries are not always clean when applied to aggregate review workflows.

### 4. Qualification evidence is not yet strong enough

The draft contains evidence that accepted bindings were created and some review decisions were auto-applied, but it does not yet produce a clean qualification record for:

- which exact subject performed which transition
- whether acceptance was human-only, system-assisted, or auto-applied under policy
- what evidence threshold would justify a `Q2` or `Q3` update for this exact transition

This is the biggest unresolved trust gap exposed by the example.

## Missing States Revealed By The Example

Minimum missing-state candidates exposed by this workflow:

- `review_surface_partially_resolved`
  - aggregate review artifact contains both accepted and unresolved members
- `review_surface_needs_binding_review`
  - inspected outcome says draft is active but not publishable
- `reference_gap_active`
  - unresolved because no acceptable candidate/reference exists yet

The first two are stronger candidates than the third. The third may remain a failure/routing concept rather than a state.

## Missing Transitions Revealed By The Example

Minimum missing-transition candidates exposed by this workflow:

- aggregate review surface -> partially resolved surface
- partially resolved surface -> needs binding review
- needs binding review -> revised review surface

The current v0 catalog has rework only for a rejected review pack. It does not yet model an aggregate artifact that remains active while only some members are accepted.

## Missing Failure Classes Revealed By The Example

No new top-level family is required yet, but the example suggests future v1 subtype pressure around:

- incomplete_required_coverage
- no_candidate_reference_found
- candidate_exists_but_not_accepted

Those should remain v1 refinements under existing families, not a v0 taxonomy expansion.

## Missing Qualification Evidence Revealed By The Example

The selected workflow does not yet justify a strong qualification update because it lacks:

- explicit subject attribution per accepted binding transition
- explicit subject attribution for unresolved review closure
- repeated transition outcome history in a compact qualification ledger

This means the control plane can describe trust conceptually, but the repo does not yet persist enough evidence to update trust confidently from this workflow alone.

## Minimum Proposed v1 Changes

### 1. Add one aggregate partial-review state

Adopt:

- `review_pack_mixed_status`

Meaning:

- active aggregate review artifact with both resolved and unresolved required members

This is the smallest state-granularity correction exposed by the example.

### 2. Add one inspected non-terminal readiness state

Adopt:

- `review_pack_needs_rework`

Meaning:

- aggregate review artifact has been inspected and remains active, but is not yet acceptable downstream without bounded correction

This prevents overloading `blocked` for every non-terminal negative inspection result.

### 3. Add one aggregate rework transition

Adopt:

- `review_pack_needs_rework -> review_pack_ready`

with supporting aggregate review transitions:

- `review_pack_ready -> review_pack_mixed_status`
- `review_pack_mixed_status -> review_pack_needs_rework`

This is the smallest transition patch that captures aggregate mixed-status review without widening into member-level workflow architecture.

### 4. Keep failure families, add subtype pressure only in v1

Do not redesign the top-level taxonomy yet.

Instead, treat this workflow as evidence that v1 may need subtypes under:

- `support`
- `requirement`
- `inspection`

### 5. Tighten qualification evidence requirements before promotion logic grows

Before any qualification engine grows further, require:

- subject attribution per transition
- explicit attribution for aggregate mixed-status and needs-rework decisions
- transition outcome history
- clearer separation of auto-applied versus human-approved decisions

## Validation Verdict

The control-plane v0 architecture is internally coherent and partially survives contact with reality.

It succeeds at:

- inspection framing
- failure/routing separation
- non-terminal recovery posture
- transition-scoped trust framing

It does not yet fully describe this real workflow without strain because:

- aggregate mixed-status artifacts are under-modeled
- partial acceptance inside one artifact is under-modeled
- qualification evidence remains thinner than the trust architecture expects

## Bottom Line

The selected historical workflow is sufficiently concrete to validate the architecture.

Result:

- control-plane v0 survives as a baseline
- but it needs v1 refinement in aggregate state modeling, aggregate rework transitions, and qualification evidence capture before larger second-order systems should be built
