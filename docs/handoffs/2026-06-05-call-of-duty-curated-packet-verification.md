# Call of Duty Curated Packet Verification

## Purpose

Record the current state of the local `call_of_duty` curated research packet after the researcher reported a wrong-surface upload.

This document answers one question:

> Does the correct curated packet exist locally, and if so, what exact files should be sent?

## Result

Yes.

The correct curated packet exists locally at:

- `outputs/research_packets/call_of_duty/wiki_curated_20260526t233955z/`

The exact files are:

- `outputs/research_packets/call_of_duty/wiki_curated_20260526t233955z/call_of_duty_wiki_curated_20260526t233955z_events_or_medals.csv`
- `outputs/research_packets/call_of_duty/wiki_curated_20260526t233955z/call_of_duty_wiki_curated_20260526t233955z_assets.csv`

## Local Verification

### Events or medals CSV

Verified row count:

- `15`

Verified local rows:

- `Double Kill`
- `Triple Kill`
- `Fury Kill`
- `Frenzy Kill`
- `Mega Kill`
- `Ultra Kill`
- `Kill Chain`
- `Bloodthirsty`
- `Merciless`
- `Ruthless`
- `Relentless`
- `Brutal`
- `Nuclear`
- `Unstoppable`
- `Victor`

Observed section groupings:

- `multikill`
- `elimination_streak`
- `payoff_or_victory`

### Assets CSV

Verified row count:

- `15`

Verified asset IDs begin with:

- `call_of_duty.event_badge_or_medal.double_kill.packet0001`
- `call_of_duty.event_badge_or_medal.triple_kill.packet0002`
- `call_of_duty.event_badge_or_medal.fury_kill.packet0003`

and continue through the same 15 curated seeds.

## Wrong-Surface Check

The following wrong-surface Warzone event rows are **not present** in the curated packet:

- `Armor Plate Bundle`
- `Cash Drop`
- `Cluster Strike`
- `Heavy Weapons Crate`
- `Jailbreak`
- `Restock`
- `Resurgence`
- `Titan Frenzy`

Local verification found:

- `0` matches for all eight of those names inside the curated `events_or_medals.csv`

## Interpretation

The researcher report is consistent with:

- the wrong file bundle being uploaded or attached externally

It is not consistent with:

- the local curated export itself being contaminated with the previously rejected Warzone events packet

## What To Send

Send these two exact files:

- `outputs/research_packets/call_of_duty/wiki_curated_20260526t233955z/call_of_duty_wiki_curated_20260526t233955z_events_or_medals.csv`
- `outputs/research_packets/call_of_duty/wiki_curated_20260526t233955z/call_of_duty_wiki_curated_20260526t233955z_assets.csv`

## Bottom Line

The local curated packet is correct.

The problem is external packet selection or upload, not the local curated export contents.
