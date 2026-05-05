# Manifest Contracts / Game Packs

This document is the canonical V2 home for manifest and game-pack rules.

## Canonical Scope

Use this doc for:

- game-pack structure
- canonical manifest categories
- required vs optional manifest rows
- publish validation and completeness policy
- asset provenance, versioning, and patch tracking

Do not use this doc for:

- runtime scoring heuristics
- hook packaging logic
- source-enrichment identity scoring

## Current V2 Position

The repo stays manifest-driven.

Stable expectations:

- game-specific knowledge should be expressed through packs and manifests, not global config sprawl
- required rows must be explicit
- unresolved required rows should block publish or promotion
- asset provenance, patch validity, and hashes are first-class fields
- ROI references and template metadata belong to manifests, not detector code
- onboarding draft artifacts are operational state unless explicitly promoted into the published pack

## Current Hardening Priorities

The manifest layer is implemented, but still active as a V2 hardening surface.

The current priorities are:

- keep baseline-to-derived manifest production explicit
- block publish whenever required rows remain unresolved
- mark unsupported families explicitly optional in the derived layer rather than silently omitting them
- keep draft readiness, review application, and published outputs in sync
- make publish-blocking output actionable enough for operators to remediate row-level gaps directly

Expected game-pack categories:

- game metadata
- entities
- abilities / UI-affecting semantics
- medals / event badges
- HUD and ROI maps
- runtime templates and masks

## Derived Detection Manifest V1

The onboarding path now has an explicit intermediate contract:

- `manifests/derived_detection_manifest.yaml`

This artifact is derived from:

- `game.yaml`
- `entities.yaml`
- `manifests/game_detection_schema.yaml`
- current draft binding state when available

Its purpose is to answer, for one draft:

- which detection rows exist for this game
- which rows are required versus optional
- which rows are already resolved by accepted bindings
- which rows remain unresolved and would later block publish-completeness gating

Expected row-level fields include:

- `detection_id`
- `asset_family`
- `target_id`
- `target_display_name`
- `required`
- `status`
- `reason`
- `semantic_ids`
- `roi_ref` through `template_defaults`
- `provenance_basis`

Expected family-level summaries include:

- active versus disabled family state
- target count per family
- `supported`, `optional_family_disabled`, or `optional_unsupported` status

Current policy:

- this derived manifest is the reviewable checklist layer
- provenance gates still validate accepted assets directly
- publish now derives or refreshes this manifest before publish validation
- unresolved `required` rows in the derived manifest block publish
- unsupported families should resolve to explicit optional status rather than disappear from the checklist

## Targeted Row Fill Workflow V1

The onboarding path now has a targeted remediation surface for unresolved required rows.

Operator entrypoints:

- `--report-unresolved-derived-rows DRAFT_ROOT`
- `--fill-derived-detection-rows DRAFT_ROOT --detection-id <id> --fill-source-manifest <path>`
- `--prepare-derived-row-review DRAFT_ROOT --detection-id <id>`
- `--summarize-derived-row-review DRAFT_ROOT|REVIEW_DIR`
- `--apply-derived-row-review REVIEW_FILE_OR_DIR`

Current workflow behavior:

- unresolved `detection_id` values are the unit of work
- extra source manifests can be added without rebuilding the whole draft from scratch
- candidate collection is narrowed to the selected detection rows
- binding candidates are rebuilt for the selected rows and then written back into the existing draft artifacts
- the derived detection manifest is refreshed after the fill step
- draft-local row reviews are stored under:
  - `review/derived_row_reviews/<detection_id>.review.json`
- each selected row gets its own review file with:
  - row snapshot from `derived_detection_manifest.yaml`
  - candidate options sourced from the current draft
  - review status and decision fields
- review summary can be generated from either:
  - the draft root
  - the `review/derived_row_reviews/` directory
- summary reports make pending review work explicit:
  - pending review count
  - decision-ready count
  - auto-accept-eligible count
- apply decisions are draft-local and row-scoped:
  - `accept_candidate`
  - `reject_all_candidates`
  - `defer_row`
- `--accept-recommended` is intentionally strict:
  - it only auto-applies rows with one deterministic recommended candidate
  - it does not infer a winner from multi-option rows
- batch helpers stay review-file-driven:
  - `--only-auto-populated`
  - `--reject-zero-candidate`
  - `--defer-zero-candidate`
- `--only-auto-populated` is also intentionally strict:
  - it applies only rows auto-populated in the current run
  - it does not sweep in previously approved manual review files
- applying a row review updates the draft authority files directly:
  - `catalog/bindings.csv`
  - `catalog/qa_queue.csv`
  - `manifests/derived_detection_manifest.yaml`

Current limitation:

- V1 stays draft-local and file-driven; it does not use the GPT review bridge
- review application is row-scoped, but candidate collection still depends on the current onboarding ingestion and binding builders
- rows only become `resolved` after an `accept_candidate` decision produces an accepted binding
- readiness and persisted state must be treated as one contract, not separate advisory outputs

## Canonical Media Contract V1

The repo now treats the published pack as a six-layer contract:

1. `game.yaml`
2. `entities.yaml`
3. abilities inside `entities.yaml`
4. `medals.yaml`
5. `hud.yaml`
6. CV asset manifests through `manifests/cv_templates.yaml` and `manifests/assets_manifest.json`

Required provenance fields for published CV assets:

- `source_url`
- `patch_tag`
- `file_hash`
- `qa_status`
- `source_license_note`

Current compatibility note:

- `medals.yaml` is now the primary published medal or event-badge layer.
- `entities.yaml.events` remains as a compatibility surface for existing runtime consumers and onboarding lineage.
- published-pack validation should treat a missing `medals.yaml` as contract drift.

## What Belongs Elsewhere

- Runtime detector and fusion behavior belongs in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Registry storage and lifecycle state belong in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Enrichment-source contracts belong in [SOURCE_ENRICHMENT_IDENTITY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/SOURCE_ENRICHMENT_IDENTITY.md).

## Current Repo Anchors

- starter assets: `/starter_assets`
- current game assets: `/assets/games`
