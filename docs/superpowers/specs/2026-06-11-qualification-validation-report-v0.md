# Qualification Validation Report v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Apply the committed qualification architecture to historical workflow evidence and reconstruct transition trust levels using observed evidence only.

This report evaluates:

- where the current `Q0` to `Q4` ladder is supported
- where the current transition qualification matrix is too strong
- where the matrix is too weak
- where the evidence is ambiguous or missing

This report preserves the current qualification architecture as the baseline.

It does not add:

- planning
- inventory
- staffing
- resource-allocation policy

## Evidence Surface

Historical workflow evidence used in this report:

- [2026-06-10-control-plane-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-control-plane-validation-report-v0.md)
- [2026-06-11-control-plane-transferability-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-control-plane-transferability-report-v0.md)
- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)

Historical workflows under evidence:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`
- `assets/games/call_of_duty/drafts/onboarding/20260505T212727Z`
- `assets/games/call_of_duty/drafts/onboarding/20260505T213409Z`
- `assets/games/marvel_rivals/drafts/onboarding/20260509T143952Z`

## Validation Method

This pass uses one strict rule:

- only observed workflow evidence counts

That means:

- no promotion from architectural elegance alone
- no inferred subject trust without explicit attributable evidence
- no treating semantic similarity as literal transition evidence when the underlying artifact is different

For this report:

- `supported`
  - observed evidence supports the current level or at least does not contradict it
- `too_strong`
  - current level claims more trust than the observed evidence justifies
- `too_weak`
  - current level is lower than the observed evidence now supports
- `ambiguous`
  - the evidence is real but does not map cleanly enough to the current transition semantics
- `unsupported`
  - the workflows do not provide enough evidence to justify a meaningful current level claim

## Baseline Qualification Doctrine Under Test

Current baseline from the committed qualification architecture:

- `Q1`
  - plausible or trial-only
- `Q2`
  - demonstrated in limited examples with guardrails
- `Q3`
  - repeatable without unusual rescue
- `Q4`
  - trusted core with strong repeat evidence

Current baseline also requires:

- qualification attaches to `subject x transition`
- support and control failures should not automatically demote execution trust
- unattributed outcomes should remain conservative

## What The Historical Evidence Actually Shows

### Strongest observed facts

- multiple workflows reached a clean inspected publish-readiness outcome
- one workflow exposed a recoverable upstream population gap rather than a downstream execution collapse
- one workflow exposed aggregate mixed-status review pressure and required a patch
- explicit subject attribution remains thin in the historical onboarding surfaces

### Weakest observed facts

- there is still little direct evidence for whole-transition subject attribution on most downstream control-plane transitions
- there is almost no observed evidence for terminal delivery or posting transitions
- there is little evidence that a single literal control-plane transition has repeated attributable success across multiple workflows

## Transition Trust Reconstruction

### Summary Table

| transition_id | primary_subject | current_level_in_baseline | evidence reconstruction | verdict | reason |
| --- | --- | --- | --- | --- | --- |
| `TRANS-001` | `codex_structured_worker` | `Q1` | `Q1` | supported | Historical workflows show repeat artifact-generation and review-surface creation, but mapping remains semantically loose and attribution is still thin. |
| `TRANS-002` | `human_editor` | `Q2` | `Q1` | too_strong | The workflows do not show repeated explicit whole-artifact approval decisions with clean attributable evidence for this literal transition. |
| `TRANS-003` | `human_editor` | `Q2` | `Q1` | too_strong | Rejection logic exists conceptually, but the evidence surface does not show repeat attributable whole-artifact rejection decisions for this literal transition. |
| `TRANS-004` | `manager_approver` | `Q1` | `Q1` | supported | Negative outcomes remain non-terminal and recoverable, but governance attribution is still sparse. |
| `TRANS-005` | `manager_approver` | `Q1` | `Q0-Q1` | ambiguous | Archive behavior is not central in the observed workflows, so the current conservative level is not contradicted but is weakly evidenced. |
| `TRANS-006` | `codex_structured_worker` | `Q1` | `Q1` | supported | Accepted sub-results exist, but mostly at row or member level rather than clean aggregate transition closure. |
| `TRANS-007` | `codex_structured_worker` | `Q2` | `Q1` | too_strong | Publish-ready drafts exist, but the evidence does not cleanly prove the literal `approved_clips_ready -> platform_package_ready` transition with attributable repeat success. |
| `TRANS-008` | `manager_approver` | `Q1` | `Q1` | supported | Block or hold posture is visible conceptually, and the current level is already conservative. |
| `TRANS-009` | `manager_approver` | `Q1` | `Q0-Q1` | ambiguous | Archive handling is not the main observed outcome, so current trust is not disproven but remains weakly grounded. |
| `TRANS-010` | `manager_approver` | `Q1` | `Q0-Q1` | ambiguous | There is not enough observed evidence of explicit reject-then-archive lifecycle handling. |
| `TRANS-011` | `manager_approver` | `Q2` | `Q0-Q1` | too_strong | Workflows show `ready_to_publish`, not local terminal completion with attributable final acceptance evidence. |
| `TRANS-012` | `manager_approver` | `Q2` | `Q0-Q1` | too_strong | No observed workflow supplies posted-completion evidence for this literal transition. |
| `TRANS-013` | `manager_approver` | `Q2` | `Q0-Q1` | too_strong | There is no repeated attributable evidence for local completion lifecycle retirement. |
| `TRANS-014` | `manager_approver` | `Q2` | `Q0-Q1` | too_strong | There is no repeated attributable evidence for posted completion lifecycle retirement. |
| `TRANS-015` | `codex_structured_worker` | `Q1` | `Q1` | supported | The aggregate mixed-status patch is validated conceptually, but rework remains narrow and not yet broad enough for a stronger claim. |
| `TRANS-016` | `human_editor` | `Q1` | `Q1` | supported | One validated workflow exposed the need for mixed-status review; current trust remains appropriately conservative. |
| `TRANS-017` | `human_editor` | `Q1` | `Q1` | supported | Needs-rework judgment is now modeled, but evidence remains thin and should stay guarded. |
| `TRANS-018` | `codex_structured_worker` | `Q1` | `Q1` | supported | Aggregate rework loop is validated as a needed control-plane path, but not yet strongly evidenced for broader trust. |

## Where Current Q-Level Definitions Are Too Strong

### 1. `Q2` is being used where the evidence is still semantic rather than literal

Transitions most exposed:

- `TRANS-002`
- `TRANS-003`
- `TRANS-007`
- `TRANS-011`
- `TRANS-012`
- `TRANS-013`
- `TRANS-014`

Why:

- the workflows often show semantically similar outcomes
- but they do not show repeated attributable success for the exact committed transition
- inspection and publish-readiness evidence exist, but final subject-by-transition trust evidence is still weak

This means the current matrix still contains some normative placeholders that have not yet survived evidence-only reconstruction.

### 2. terminal and lifecycle trust is overclaimed relative to the evidence surface

The workflows validated:

- non-terminal review
- publish readiness
- recoverable upstream failure

They did not validate:

- local completion acceptance
- posted completion acceptance
- post-terminal archival discipline

That makes several `Q2` lifecycle placements too strong for now.

## Where Current Q-Level Definitions Are Too Weak

No strong case for `too_weak` was found in this evidence pass.

Reason:

- the current matrix was already made conservative in earlier review
- observed evidence is still thinner than it first appeared once strict subject attribution and literal transition matching are required

## Where Current Q-Level Definitions Are Ambiguous

### 1. semantic transfer versus literal transition transfer

The onboarding workflows transfer structurally, but not literally.

Example:

- `ready_to_publish` is evidence that some downstream inspection and readiness logic works
- it is not the same as proving `platform_package_ready -> completed_local`

That ambiguity affects:

- `TRANS-007`
- `TRANS-011`
- `TRANS-012`
- `TRANS-013`
- `TRANS-014`

### 2. control transitions remain thinly evidenced

For:

- `TRANS-004`
- `TRANS-005`
- `TRANS-008`
- `TRANS-009`
- `TRANS-010`

the conservative levels are acceptable, but actual attributable governance evidence remains sparse.

These are not wrong so much as lightly grounded.

## Where Current Q-Level Definitions Are Unsupported

### 1. `Q3` and `Q4` are still architectural definitions, not validated operating levels

No historical workflow in this evidence set justifies a live reconstruction at:

- `Q3`
- `Q4`

Reason:

- there is not enough repeated attributable transition success without unusual rescue
- there is not enough compact qualification-ledger evidence
- there is not enough separation between semantic workflow success and literal transition success

This does not mean the definitions are wrong.

It means they remain:

- policy-ready
- not evidence-proven

### 2. qualification evidence thresholds remain underdefined in practice

The current architecture says:

- `Q2` requires demonstrated limited evidence with guardrails
- `Q3` requires repeatable transition success

But the repo still lacks a compact reconstruction surface that answers:

- how many attributable successes count as “demonstrated”
- how much negative inspection or rework blocks promotion
- how to weigh semantic transfer versus literal transition identity

That makes the ladder coherent, but not yet operationally calibrated.

## Subject Attribution Findings

This remains the largest qualification gap.

Observed evidence supports:

- subject attribution matters
- unattributed outcomes should stay conservative

Observed evidence does not yet provide:

- consistent subject attribution for aggregate approval decisions
- consistent subject attribution for publish-readiness closure
- consistent subject attribution for downstream terminal transitions

Conclusion:

- the qualification architecture is correct to insist on subject attribution
- the repo evidence is not yet strong enough to promote many transitions beyond guarded levels

## Support And Control Separation Check

This validation pass supports the existing doctrine that:

- `support` failures should not automatically demote execution trust
- `control` failures should not automatically demote production trust

Why:

- the transferability stress appeared in source and population modeling
- the control-plane core did not collapse
- the historical workflows frequently expose missing coverage, missing accepted bindings, or ambiguous identity conditions that are not clean proof of worker inability

This part of the qualification architecture survives evidence review well.

## Recommended Qualification Interpretation After This Pass

### Keep as baseline

Keep:

- the `Q0` to `Q4` ladder
- `subject x transition` as the qualification unit
- conservative promotion posture
- support/control separation from execution trust

### Treat as normative placeholders until more evidence exists

Treat current matrix entries as normative placeholders wherever:

- current level is `Q2`
- evidence is only semantic or weakly attributable
- terminal transitions are being inferred from readiness rather than directly observed

### Safe evidence-backed posture now

For the current historical evidence set:

- `Q1`
  - is the most defensible current level for most validated transitions
- `Q2`
  - is defensible only in a narrow, guarded, and still arguable way
- `Q3` and `Q4`
  - should not be treated as currently evidenced in the validated workflows

## Minimum Recommended v1 Pressure

This validation does not justify redesigning the qualification architecture.

It does justify tightening the evidence surface around:

- subject attribution per transition
- compact transition outcome history
- explicit distinction between semantic-transfer evidence and literal-transition evidence
- promotion thresholds for `Q2`
- stronger proof requirements before any `Q3` claim

## Validation Verdict

The qualification architecture is structurally coherent, but only partially reality-qualified.

What survived:

- qualification attaches to `subject x transition`
- support and control should remain separate from execution trust
- conservative `Q1` posture is appropriate for thinly evidenced transitions

What did not yet survive:

- many current `Q2` placements
- any live `Q3` or `Q4` interpretation
- implicit confidence that semantically similar workflows prove literal transition trust

## Bottom Line

Observed evidence supports the qualification architecture as a baseline.

Observed evidence does not yet support treating most current higher trust placements as empirically proven.

Result:

- keep the current qualification architecture
- keep the current conservative framework
- treat several `Q2` rows as normative placeholders pending better evidence
- do not build planning, inventory, or resource-allocation layers on top of qualification as if it were already fully evidence-calibrated
