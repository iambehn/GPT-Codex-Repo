# Call Of Duty Medal Packet Stub

Use this as the starting document for the next `call_of_duty` medal research packet.

Fill in the missing sections. Do not delete the current repo-truth section unless it becomes stale and you replace it with a more current equivalent.

## Metadata

```yaml
packet_id: 2026-05-26-call-of-duty-medal-packet
status: draft
owner: Pipeline Research Strategist
decision_target: Identify the first gameplay HUD medal subset worth promoting into onboarding review for call_of_duty.
pipeline_layer: onboarding
game: call_of_duty
priority: P0
confidence: exploratory
supersedes:
```

## 1. Decision Target

Identify the first true gameplay HUD medal subset worth promoting into onboarding review for `call_of_duty`.

Priority subset:

- multikill medals
- elimination-streak medals
- payoff or victory medals

## 2. Current Repo Truth

Current published-pack truth:

- published `call_of_duty` pack has `hero_portrait` and `equipment_icon` coverage
- published `call_of_duty` pack does not yet have promoted `medal_icon` coverage

Current workflow truth:

- wiki-to-onboarding bridge works
- pre-bridge curation works
- current raw wiki source family is the wrong source family for medal promotion

Current measured behavior:

- cross-clip measurement on four public `call_of_duty` samples showed:
  - `2 / 4` samples produced no runtime events
  - `2 / 4` samples produced equipment-only events
  - `0 / 4` samples produced medal-driven outcomes
  - `0 / 4` samples produced hook candidates

Current sample-family truth:

- the current local sample set is mixed rather than title-pure
- three sampled clips look Warzone-family
- one sampled clip looks multiplayer-style
- therefore the first medal title seed should be treated as a strong candidate, not final title truth, until visual comparison is done

Relevant repo evidence:

- `docs/handoffs/2026-05-25-call-of-duty-wiki-source-audit.md`
- `docs/handoffs/2026-05-25-call-of-duty-pre-bridge-curation-findings.md`
- `docs/handoffs/2026-05-25-call-of-duty-cross-clip-measurement-audit.md`

## 3. Evidence Bundle

Add one entry per source item.

```yaml
- evidence_id:
  type: screenshot | crop | source_page | clip_timestamp | table | note
  path_or_url:
  why_it_matters:
  trust_level: authoritative | strong_candidate | exploratory
```

## 4. Structured Findings

### Target Medals To Keep

- 

### Aliases Or Naming Variants

- 

### Category Mapping

```text
multikill:
- 

elimination-streak:
- 

payoff_or_victory:
- 
```

### Explicit Exclusions

- contracts
- intel missions
- calling cards
- weapon blueprints
- watches
- logos
- operator skins
- map imagery
- season branding
- game-mode branding
- general Warzone key art

### Ambiguous Candidates

- 

### Negative Examples / False Positives

- 

### Source Quality Notes

- 
- note explicitly whether the proposed title-specific medal family is authoritative or only a strong candidate visual seed for the current samples

## 5. Recommendation

State the exact action Codex should take next.

Example form:

```text
Promote the following medal subset into onboarding review first: ...
Use source set X as the authoritative seed and exclude source set Y.
```

Recommendation:

- 

## 6. Acceptance Target

The packet is good enough when Codex can use it to:

1. create or curate a narrower medal source set
2. bridge it into onboarding
3. validate that onboarding gains real `medal_icon` candidates
4. rerun runtime measurement on the existing `call_of_duty` sample set

Concrete expected outcome:

- 

## 7. Open Uncertainties

```yaml
- question:
  blocks_implementation: true | false
  recommended_next_step:
```

## Researcher Reminder

Prefer:

- medal-specific pages
- live gameplay HUD captures
- tight crops
- stable label mappings

Avoid:

- broad wiki dump pages
- cosmetics-heavy pages
- map/season overview pages
- source sets that mix medals with contracts, blueprints, or branding without clear separation
