# Call of Duty Replay Artifact Supersession Audit Report v0

Date: 2026-06-13
Status: completed
Scope: bounded replay-artifact supersession audit only

## Objective

Apply the bounded `call_of_duty` replay-artifact supersession procedure to the
current replay roots and classify replay artifacts as:

- active
- superseded
- protected
- unresolved

Non-goals:

- cleanup
- deletion
- relocation
- manifest rewrite
- widening beyond `call_of_duty`

## Authority Surfaces

Procedure:

- [CALL_OF_DUTY_REPLAY_ARTIFACT_SUPERSESSION_PROCEDURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CALL_OF_DUTY_REPLAY_ARTIFACT_SUPERSESSION_PROCEDURE.md)

Canonical proof-path manifests:

- [call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json)
- [call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json)

Historical replay anchor:

- `workflow-11aea2937311834b`

Replay roots:

- [outputs/editorial_replay/identities/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty)
- [outputs/editorial_replay/decisions/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty)
- [outputs/editorial_replay/snapshots/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/snapshots/call_of_duty)

## Classification Summary

Pass result:

- `passes = true`

Counts:

- active identities:
  - `6`
- active decisions:
  - `6`
- protected snapshots:
  - `1`
- superseded identities:
  - `6`
- superseded decisions:
  - `6`
- unresolved identities:
  - `0`
- unresolved decisions:
  - `0`
- unresolved snapshots:
  - `0`

## Active Artifacts

### Active identities

The current canonical proof-path session manifests actively reference these
identity artifacts:

- [editorial-720d60e3051431ba.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-720d60e3051431ba.editorial_identity.json)
- [editorial-a5bfee828490d879.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-a5bfee828490d879.editorial_identity.json)
- [editorial-43ffbdea7504b6d0.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-43ffbdea7504b6d0.editorial_identity.json)
- [editorial-d389f1a7b685702d.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-d389f1a7b685702d.editorial_identity.json)
- [editorial-dad698b25d4f9ddc.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-dad698b25d4f9ddc.editorial_identity.json)
- [editorial-9f40f0df5e3700ff.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-9f40f0df5e3700ff.editorial_identity.json)

Classification:

- `active`
- `protected`

### Active decisions

The current canonical proof-path session manifests actively reference these
decision artifacts:

- [decision-e9be683f7ce18cfe.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-e9be683f7ce18cfe.editorial_decision.json)
- [decision-5bb6504e5eccacf2.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-5bb6504e5eccacf2.editorial_decision.json)
- [decision-9c4ef4f1d15ef030.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-9c4ef4f1d15ef030.editorial_decision.json)
- [decision-14994a461547efb0.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-14994a461547efb0.editorial_decision.json)
- [decision-454ba0e1300e53ab.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-454ba0e1300e53ab.editorial_decision.json)
- [decision-f4ca91358cee029d.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-f4ca91358cee029d.editorial_decision.json)

Classification:

- `active`
- `protected`

### Protected snapshots

Historical replay protection applies to:

- [snapshot-498838e69d868f43.export_ready_snapshot.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/snapshots/call_of_duty/snapshot-498838e69d868f43.export_ready_snapshot.json)

Why protected:

- required for authoritative historical export replay
- tied to:
  - `workflow-11aea2937311834b`
- editorial object id:
  - `editorial-dad698b25d4f9ddc`

Classification:

- `protected`

## Superseded Artifacts

### Superseded identities

The following identity artifacts are not referenced by the current canonical
proof-path manifests and are superseded by normalized active artifacts:

- [editorial-56a590f358b9de69.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-56a590f358b9de69.editorial_identity.json)
- [editorial-704e50f8cf277339.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-704e50f8cf277339.editorial_identity.json)
- [editorial-835703f533661752.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-835703f533661752.editorial_identity.json)
- [editorial-acdeb5a2db5decb3.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-acdeb5a2db5decb3.editorial_identity.json)
- [editorial-dab838c7b7dbf9f1.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-dab838c7b7dbf9f1.editorial_identity.json)
- [editorial-df8c6bab60d6b41f.editorial_identity.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty/editorial-df8c6bab60d6b41f.editorial_identity.json)

Shared reason:

- pre-normalization absolute-path identity replaced by canonical normalized
  active artifact

Classification:

- `superseded`

### Superseded decisions

The following decision artifacts are linked to superseded pre-normalization
editorial object ids:

- [decision-7de18a7394c04e55.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-7de18a7394c04e55.editorial_decision.json)
- [decision-7f1e829b64494981.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-7f1e829b64494981.editorial_decision.json)
- [decision-8545c0b7de0edb4b.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-8545c0b7de0edb4b.editorial_decision.json)
- [decision-d05a2ce1e1b2f4e9.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-d05a2ce1e1b2f4e9.editorial_decision.json)
- [decision-dfb4cd6ad0181c5c.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-dfb4cd6ad0181c5c.editorial_decision.json)
- [decision-ffbf921eabf356fb.editorial_decision.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty/decision-ffbf921eabf356fb.editorial_decision.json)

Shared reason:

- decision artifact linked to superseded pre-normalization editorial object id

Classification:

- `superseded`

## Unresolved Artifacts

Unresolved identities:

- `0`

Unresolved decisions:

- `0`

Unresolved snapshots:

- `0`

Assessment:

- no artifact remained ambiguous under the bounded supersession procedure

## Pass / Fail Result

Result:

- `pass`

Why:

1. every canonical session-linked identity is present and classifiable as
   active
2. every canonical session-linked decision is present and classifiable as
   active
3. the historical export snapshot anchor remains protected
4. all non-linked replay artifacts were cleanly classifiable as superseded
5. no unresolved artifact remained

## Conclusion

The bounded `call_of_duty` replay roots are now fully classifiable under the
supersession procedure.

Current state:

- active replay artifacts:
  - coherent
- protected historical snapshot anchor:
  - coherent
- superseded pre-normalization artifacts:
  - explicitly identified
- unresolved artifacts:
  - none

That means the next bounded step, if desired, is now justified:

- define a quarantine procedure
- or define a cleanup implementation slice

This report itself does not authorize cleanup. It only establishes that cleanup
can now be reasoned about without ambiguity.
