# Source Intake Architecture v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Define the first upstream source-intake architecture for the validated control-plane model.

This artifact states:

- what source artifact families currently enter the system
- what source states exist before normal control-plane execution begins
- what rules make a source qualified for entry
- what explicit transitions move qualified source artifacts into the existing control plane

## Scope

This spec defines:

- a source catalog
- a source-state model
- source qualification rules
- explicit entry transitions into the current control-plane baseline

This spec does not define:

- inventory strategy
- planning doctrine
- staffing policy
- queue policy
- broad redesign of the current control-plane artifacts

## Guardrails

- Preserve the existing committed control-plane baseline:
  - [2026-06-10-state-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-state-catalog-v0.md)
  - [2026-06-10-transition-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-transition-catalog-v0.md)
  - [2026-06-10-inspection-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-inspection-architecture-v0.md)
  - [2026-06-10-failure-taxonomy-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-failure-taxonomy-v0.md)
  - [2026-06-10-routing-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-routing-architecture-v0.md)
  - [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)
- Source intake should explain how reality enters the control plane, not replace the control plane.
- Source states must describe source condition, not process instructions.
- Use current repo contracts and statuses where they already exist:
  - `source_fetch_log`
  - `needs_source_review`
  - `needs_population_review`
  - `ingestion_ready`
  - `invalid_source_root`

## Why This Artifact Exists

The transferability report showed that the current control plane generalizes across multiple workflows, but that one recurrent stress point appears before normal control-plane execution:

- source fetch failure
- source ambiguity
- identity conflict
- incomplete population

That pressure did not invalidate:

- state
- transition
- inspection
- failure
- routing
- qualification

It exposed a missing upstream layer:

- how source artifacts become control-plane inputs

Evidence surface:

- [2026-06-11-control-plane-transferability-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-control-plane-transferability-report-v0.md)
- [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- [pipeline/onboarding_publish_readiness.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/onboarding_publish_readiness.py)
- [pipeline/accepted_clip_source_manifest_adapter.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/accepted_clip_source_manifest_adapter.py)
- [docs/superpowers/specs/2026-05-07-accepted-clip-inventory-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-clip-inventory-design.md)

## Core Principle

Source intake v0 should answer:

- what source artifact exists
- whether it is structurally valid
- whether it is populated enough for downstream use
- which control-plane state it is allowed to enter, if any

## Source Catalog

Use these source artifact families in v0.

| source_family_id | source_family_name | primary_repo_surface | unit_of_intake | dominant qualification concern | intended control-plane entry |
| --- | --- | --- | --- | --- | --- |
| `SRCFAM-001` | `onboarding_remote_source` | [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py) | declared source row with `role` and `url` | fetchability and source validity | upstream only; does not enter control plane directly |
| `SRCFAM-002` | `onboarding_populated_draft_source` | `assets_manifest.json`, `source_fetch_log`, `qa_queue.csv`, publish-readiness output | populated onboarding draft as an aggregate source artifact | source coverage, identity reconciliation, population sufficiency | `review_pack_ready` when population is resolved enough for review-stage use |
| `SRCFAM-003` | `accepted_clip_inventory_source` | accepted-clip inventory rows with `ingestion_ready` | canonical accepted clip row | structural clip-id/path validity | `raw_vod` |
| `SRCFAM-004` | `accepted_clip_fixture_source_manifest` | [pipeline/accepted_clip_source_manifest_adapter.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/accepted_clip_source_manifest_adapter.py) output fixture manifest | adapted manifest of ready clip fixtures | manifest validity and row-level source readiness | `raw_vod` through the referenced canonical clip rows |

## Source State Model

Source states in v0 are intentionally narrow and sit before the committed control-plane state catalog.

### Source State Catalog

| source_state_id | source_state_name | source_scope | definition | entry_condition | exit_allowed_when | valid_next_states | inspection_required | terminal_state |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `SRCSTATE-001` | `source_declared` | source_intake | A source artifact has been declared and recorded, but not yet confirmed fetched or structurally usable. | Source row, source manifest, or accepted clip inventory row exists. | The source is fetched, found invalid, or deferred. | `source_fetched`, `source_invalid` | required | no |
| `SRCSTATE-002` | `source_fetched` | source_intake | The declared source is structurally reachable and fetched or locally present, but not yet proven population-ready for downstream use. | Fetch succeeded or a local canonical source path exists. | The source is found population-ready, found population-incomplete, found invalid, or deferred. | `source_population_ready`, `source_population_review_required`, `source_invalid` | required | no |
| `SRCSTATE-003` | `source_population_review_required` | source_intake | The source exists, but population or identity findings prevent safe downstream entry without further review or correction. | Fetch or local presence succeeded, but readiness/finding evidence shows unresolved source or population issues. | Population issues are resolved, the source is invalidated, or the source is explicitly archived outside this spec. | `source_population_ready`, `source_invalid` | required | no |
| `SRCSTATE-004` | `source_population_ready` | source_intake | The source has enough validated coverage, identity stability, and structural readiness to enter the existing control plane. | Fetch/local presence succeeded and no blocking population/source issue remains for the intended entry path. | The source is consumed by an entry transition or invalidated by later evidence. | `raw_vod`, `review_pack_ready`, `invalid_source` | required | no |
| `SRCSTATE-101` | `source_invalid` | source_intake_control | The source is unsupported, unusable, malformed, or otherwise not fit for the intended path. | Structural validation, fetch validation, or population validation proves the source unfit. | The invalid condition is explicitly corrected through re-entry or the source is archived outside this spec. | `source_declared`, `invalid_source` | required | no |

## State Semantics

### `source_declared`

Use when the repo knows a source exists, but no reliable evidence yet shows it can be fetched or used.

Examples:

- onboarding source manifest row with role and URL
- accepted clip inventory row before checking `ingestion_ready`

### `source_fetched`

Use when the source exists and is reachable or locally present, but downstream use is still uncertain.

Examples:

- source appears in `source_fetch_log` with successful fetch status
- accepted clip inventory row has a canonical clip path that exists locally

### `source_population_review_required`

Use when the source is present, but population findings prevent safe control-plane entry.

Examples from existing repo statuses and findings:

- `needs_population_review`
- `needs_source_review`
- `missing_required_source_coverage`
- `conflicting_identity_match`
- `ambiguous_identity_match`
- `source_fetch_failed`

This state is the main upstream patch identified by the transferability report.

### `source_population_ready`

Use when the source is sufficiently qualified for a specific entry path into the control plane.

This does not imply:

- publish readiness
- full downstream qualification
- broad strategic source quality

It only means:

- safe enough to enter the next defined control-plane state

For direct entry to `review_pack_ready`, `source_population_ready` additionally requires a minimum evidence bundle:

- no blocking population findings remain for the intended review path
- the aggregate review artifact already exists
- member or source identity is stable enough to inspect
- the artifact is already in a reviewable form rather than only being a candidate source population

If those conditions are not met, the source is not yet ready for direct `review_pack_ready` entry and should remain upstream or enter a different earlier path such as `raw_vod` where that path is valid.

### `source_invalid`

Use when the source should not proceed through normal source-intake progression.

This state maps naturally to the existing cross-artifact control state `invalid_source`, but source-intake v0 keeps the source-specific invalidation visible before that handoff.

## Source Qualification Rules

Source qualification in v0 is not workforce qualification. It is entry qualification.

Source qualification should answer:

- is this source structurally valid?
- is this source populated enough for the intended entry path?
- is this source ambiguous in a way that requires upstream review before control-plane execution?

### Qualification Dimensions

Use these dimensions in v0:

- `structural_validity`
  - path exists, URL or manifest row is well-formed, schema is readable
- `fetch_or_presence_validity`
  - source fetch succeeded or local canonical source exists
- `population_validity`
  - source produced enough stable coverage to support the intended next state
- `identity_stability`
  - source-derived identities are not still in unresolved conflict for the intended path
- `entry_path_fit`
  - the source is suitable for the exact control-plane entry path being requested

### Source Qualification Outcomes

Use this compact outcome set in v0:

- `not_qualified`
  - source cannot enter the control plane
- `qualified_for_review_prep`
  - source can stay in source-intake processing but not yet enter the control plane
- `qualified_for_control_plane_entry`
  - source can enter a specific committed control-plane state

Source qualification ends when the entry transition into the committed control plane fires.

After that handoff:

- downstream trust is not governed by source qualification
- downstream trust is governed by the committed control-plane qualification architecture in [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)

### Qualification Rules By Source Family

#### `onboarding_remote_source`

- `not_qualified`
  - malformed source row
  - unsupported source role
  - fetch failure without successful alternate evidence
- `qualified_for_review_prep`
  - fetch succeeded, but unresolved population or identity findings remain
- `qualified_for_control_plane_entry`
  - only after the populated aggregate draft reaches `source_population_ready`

#### `onboarding_populated_draft_source`

- `not_qualified`
  - structurally invalid draft
  - zero fetched source coverage where coverage is required
- `qualified_for_review_prep`
  - `needs_population_review`
  - `needs_source_review`
- `qualified_for_control_plane_entry`
  - readiness evidence indicates the draft has crossed from source/population uncertainty into a review-ready aggregate artifact
  - no blocking population findings remain
  - the aggregate review artifact already exists
  - identity is stable enough for inspection
  - the artifact is already in a reviewable form

#### `accepted_clip_inventory_source`

- `not_qualified`
  - `ingestion_ready: false`
  - unresolved canonical clip path
  - unresolved clip identity
- `qualified_for_control_plane_entry`
  - `ingestion_ready: true`

#### `accepted_clip_fixture_source_manifest`

- `not_qualified`
  - invalid adapted manifest
  - no usable rows
- `qualified_for_control_plane_entry`
  - adapted manifest is valid and points only to canonically ready rows

## Entry Transition Model

These source-intake transitions sit upstream of the committed transition catalog and hand off into it. They are defined here so the current control-plane artifacts can stay stable.

| source_transition_id | source_transition_name | input_source_state | output_state | transition_type | triggering_artifact | completion_event | inspection_event | known_failure_families | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `SITRANS-001` | `source_declared_to_source_fetched` | `source_declared` | `source_fetched` | `production` | source row or source artifact | source fetch or local presence recorded | source presence validation | `source`, `support`, `control` | Covers both remote fetch success and local accepted-media presence. |
| `SITRANS-002` | `source_fetched_to_source_population_review_required` | `source_fetched` | `source_population_review_required` | `inspection` | fetched or locally present source artifact | upstream findings recorded as unresolved | source-intake inspection | `source`, `requirement`, `inspection`, `support` | Used when findings show the source is present but not yet safe for downstream entry. |
| `SITRANS-003` | `source_fetched_to_source_population_ready` | `source_fetched` | `source_population_ready` | `inspection` | fetched or locally present source artifact | source-intake pass recorded | source-intake pass inspection | `source`, `inspection`, `support` | Used when the source satisfies the intended entry-path conditions. |
| `SITRANS-004` | `source_population_ready_to_raw_vod` | `source_population_ready` | `raw_vod` | `production` | accepted clip inventory row or adapted fixture source | control-plane source accepted as raw media input | entry-path readiness validation | `source`, `support`, `control` | Main entry path for accepted-clip ingestion into the existing control plane. |
| `SITRANS-005` | `source_population_ready_to_review_pack_ready` | `source_population_ready` | `review_pack_ready` | `production` | populated onboarding draft source | review-stage aggregate artifact accepted for downstream review | aggregate entry readiness validation | `source`, `requirement`, `inspection`, `support` | Main entry path for onboarding/population workflows that already produce aggregate review artifacts. |
| `SITRANS-006` | `source_declared_to_source_invalid` | `source_declared` | `source_invalid` | `control` | declared source artifact | invalid-source decision recorded before fetch or local use | invalid-source review | `source`, `requirement`, `control` | Source-specific invalidation when the source is unfit before successful fetch or local-use validation. |
| `SITRANS-007` | `source_fetched_to_source_invalid` | `source_fetched` | `source_invalid` | `control` | fetched or locally present source artifact | invalid-source decision recorded after fetch or local presence validation | invalid-source review | `source`, `requirement`, `control` | Source-specific invalidation after fetch or local presence succeeds but later validation fails. |
| `SITRANS-008` | `source_invalid_to_invalid_source` | `source_invalid` | `invalid_source` | `control` | invalidated source artifact | cross-artifact invalid-state handoff recorded | invalid-state handoff review | `source`, `control`, `lifecycle` | Bridges source-intake invalidation into the committed control-plane control state without changing the current catalog. |

## Inspection Expectations For Source Intake

Source-intake inspection should answer:

- did the source reach the intended source state?
- is the source safe enough for the requested control-plane entry path?

Use these inspection subjects in v0:

- `system_validator`
  - schema, path, manifest, and fetch/presence checks
- `human_editor`
  - population or identity judgment when source ambiguity cannot be resolved mechanically
- `manager_approver`
  - invalidation or exceptional control decisions where needed

## Failure Mapping

Source-intake v0 should reuse the committed top-level failure families rather than create a parallel taxonomy.

Dominant source-intake mappings:

- `source`
  - unusable source input, unsupported source path, fetch failure
- `requirement`
  - source does not satisfy the requested entry-path assumptions
- `inspection`
  - source looks present but did not actually meet the source-state pass criteria
- `support`
  - missing helper artifact, weak alias support, missing canonical source linkage
- `control`
  - explicit hold or invalidation pending upstream decision

## Routing Expectations

Source-intake routing in v0 remains intentionally compact.

- `pass`
  - advance to the next source state or enter the existing control plane
- `rework_required`
  - remain in source-intake and correct the source or population issue
- `blocked`
  - hold in source-intake; do not enter the committed control plane
- `archive`
  - remain outside the current scope; lifecycle treatment is deferred

## Explicit Deferrals

These are intentionally deferred beyond source-intake v0:

- inventory strategy
  - future `Inventory Model`
- queueing, prioritization, and capacity management
  - future `Planning Doctrine`
- staffing and workforce policy
  - future staffing or operating-model artifacts
- broad member-level source inventories
  - future `Resource Model` or `Inventory Model` if needed
- detailed archive semantics for source artifacts
  - future lifecycle refinement if repeated source retirement patterns matter

## Minimum v0 Outcome

Source-intake v0 is successful if it makes these points explicit:

- why `needs_population_review` is upstream of the current committed control plane
- how accepted-clip readiness enters `raw_vod`
- how populated onboarding drafts enter `review_pack_ready`
- why direct `review_pack_ready` entry requires an already existing reviewable aggregate artifact rather than mere source population
- how invalid source handling reaches the existing `invalid_source` control state
- how source qualification is distinct from later transition qualification

## Recommended Next Pressure Test

Validate this source-intake model against the same historical onboarding workflow that stressed the transferability pass:

- `assets/games/call_of_duty/drafts/onboarding/20260505T212727Z`

and one accepted-clip intake example, to verify that:

- source-population ambiguity stays upstream
- accepted-media ingestion enters `raw_vod` cleanly
- no inventory or planning logic is required to explain entry into the control plane
