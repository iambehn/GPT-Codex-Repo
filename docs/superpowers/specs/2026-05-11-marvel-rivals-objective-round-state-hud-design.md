## Summary

Add a first objective/round-state HUD detection slice to the published `marvel_rivals` pack so the pipeline can produce non-identity, non-KO-streak runtime evidence from stable round-state prompts.

The current pack has:

- `hero_portrait` identity detection
- `medal_area` KO-streak prompt detection
- no objective or round-state runtime path

The next useful expansion is a narrow, stable HUD family that is easier to source than more KO-streak variants and more semantically meaningful than identity-only evidence.

## Goal

Add one small, auditable objective/round-state HUD family that can yield meaningful runtime and fused evidence on local clips.

Success means:

- the published `marvel_rivals` pack gains a real round-state/event surface
- the new surface uses explicit ROI, detection, runtime, and fusion contracts
- the slice is small enough to validate quickly on real clips

## Recommended First Slice

Target:

- `overtime`
- `victory`
- `defeat`

Why this slice:

- these are stable, high-signal round-state prompts
- they are easier to source than dynamic objective progress bars or kill feed semantics
- they create strong non-identity evidence for highlight context
- they fit the existing ROI-matcher and runtime/fusion architecture cleanly

## Non-Goals

This slice does not:

- model payload/cart progress bars
- model capture-point percentages
- model kill feed
- redesign the current KO-streak path
- add OCR as a new detector family
- create a full objective ontology in one pass

## Approach Options

### Option 1: Round-state text prompts

Detect stable prompts like:

- `OVERTIME`
- `VICTORY`
- `DEFEAT`

Pros:

- easiest to source
- high semantic clarity
- stable visual surface
- best first slice

Cons:

- narrower than a full objective model

### Option 2: Objective progress/status HUD

Detect payload/capture state such as:

- contested
- objective secured
- progress bar states

Pros:

- closer to live match tension

Cons:

- more dynamic and mode-specific
- harder to source and normalize
- likely needs more than one ROI and more asset variants

### Option 3: Match-end result only

Detect only `victory` and `defeat`.

Pros:

- smallest scope

Cons:

- misses a strong in-match tension signal like `overtime`
- weaker highlight-context coverage than Option 1

## Decision

Adopt Option 1.

First published round-state slice:

- `overtime`
- `victory`
- `defeat`

## Proposed Design

### Ontology

Add three round-state entities to the published `marvel_rivals` pack:

- `overtime`
- `victory`
- `defeat`

These should be modeled as event-like HUD states, not medals and not character entities.

Recommended classification:

- collection: `round_states`
- category: `objective`

This keeps them distinct from both medals and hero identity.

### ROI

Add one new ROI in `assets/games/marvel_rivals/hud.yaml` for the round-state prompt surface.

Recommended initial ROI:

- a top-center or center-top banner region that covers the stable placement used by:
  - `OVERTIME`
  - `VICTORY`
  - `DEFEAT`

This should be a dedicated ROI, not a reuse of `medal_area`, because the semantics and placement contract are different even if they overlap visually.

### Detection manifest

Add three detection rows in `assets/games/marvel_rivals/manifests/detection_manifest.yaml`:

- `marvel_rivals.overtime.round_state_banner`
- `marvel_rivals.victory.round_state_banner`
- `marvel_rivals.defeat.round_state_banner`

Each row should:

- require a published template asset
- bind to the new round-state ROI
- use the current ROI matcher path
- carry explicit `event_row_id` semantics

### Templates

Add three published template rows in `assets/games/marvel_rivals/manifests/cv_templates.yaml` once real assets are sourced:

- `marvel_rivals.overtime.round_state_banner`
- `marvel_rivals.victory.round_state_banner`
- `marvel_rivals.defeat.round_state_banner`

These should live under a new asset family, for example:

- `round_state_banner`

Expected asset layout:

- `assets/games/marvel_rivals/masters/round_states/...`
- `assets/games/marvel_rivals/templates/round_states/...`

### Runtime mapping

Extend `assets/games/marvel_rivals/manifests/runtime_cv_rules.yaml` with one new mapping:

- `round_state_banner -> round_state_visibility -> round_state_seen`

Target field:

- `event_row_id`

This matches the current medal pattern and keeps the runtime shape interpretable.

### Fusion rule

Extend `assets/games/marvel_rivals/manifests/fusion_rules.yaml` with one new atomic rule:

- `round_state_visibility_atomic`

Behavior:

- event type: `round_state_seen`
- signal type: `round_state_visibility`
- group by `event_row_id`
- confidence method: `max`

This mirrors the medal path and keeps the first slice simple.

## Asset Sourcing Strategy

Use real gameplay or trustworthy public image sources for:

- `OVERTIME`
- `VICTORY`
- `DEFEAT`

Requirements:

- match the actual on-screen HUD surface
- clean enough for ROI matching
- publishable provenance recorded in the pack

Do not fabricate text banners synthetically.

## Output Contract

This slice should create:

- runtime signals of type `round_state_visibility`
- runtime events of type `round_state_seen`
- fused atomic round-state events

No schema redesign is needed; only additive rows and rules are required.

## Verification

Verification for this slice should be bounded and practical:

1. confirm the pack validates with the new rows and assets
2. run a bounded runtime probe on a known `overtime`, `victory`, or `defeat` source clip
3. verify at least one runtime signal and one runtime event are emitted
4. run fused analysis and verify the new event family is preserved as non-identity evidence

## Risks

### Risk: banner placement differs across game modes

Mitigation:

- start with prompts that are visually stable across common modes
- keep the first ROI broad enough for the stable banner surface

### Risk: objective family gets overloaded too quickly

Mitigation:

- keep the first slice limited to three round-state prompts
- defer payload/capture semantics to a later slice

### Risk: semantics are too generic

Mitigation:

- preserve explicit `event_row_id`
- keep ontology rows separate for `overtime`, `victory`, and `defeat`

## Resulting Operator Posture

After this slice:

1. `marvel_rivals` has a second non-identity runtime family besides medals
2. runtime/fusion can emit round-state context from real HUD prompts
3. future objective work can build outward from this stable base toward richer mode-specific HUD events

This is the smallest reliable objective/round-state HUD expansion for the current pack.
