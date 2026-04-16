# Gaming Clip Farming Bot — Brainstorming

Ideas, frameworks, and future directions that aren't current implementation priorities.
Nothing here is committed to — it's reference material for when the time comes.

---

## Future Archetypes (Not Current Scope)

**Lore Narration Shorts:** 20–60s narrated history or fictional lore (League, WoW, Warhammer; historical: mafia/gangsters/Yakuza, ancient empires). Slideshow style, AI voiceover (ElevenLabs), cinematic music. Higher CPM than gaming; strong brand deal potential.

**Interactive Poker Clips:** Short poker hands with hidden hole cards — viewer guesses the hand via multiple choice overlay. Requires CapCut (not FFmpeg) for interactive UI layers (countdown timer, elimination animation, reveal). Very high CPM. Data tagging system (hand type, action, result, hook format, engagement) feeds into existing Google Sheets analytics.

**Poker CapCut Template:** 9-track layout: base video → mask layer → question prompt → multiple choice UI → timer → elimination animation → reveal → result highlight → audio. Assembly time 2–5 min per video once built. Card masking tiers: manual coordinates → FFmpeg overlay from config → full OpenCV/YOLO automation (overkill initially).

---

## Systems Thinking — Filters and Templates

Filters and templates are two mechanisms for removing human decision points from a workflow so work becomes repeatable instead of constantly re-decided.

**Filters solve the selection problem.** They define what gets processed and what doesn't, based on rules — automatically. Without filters, someone evaluates every incoming item manually: should I act on this? That micro-decision, repeated at scale, is where most cognitive load accumulates.

Examples:
- Only process clips longer than 20 seconds with engagement above threshold
- Only route support tickets tagged "billing" to a specific queue
- Only ingest clips from streamers with ≥ 50k followers

**Templates solve the execution problem.** Once something qualifies, a template defines exactly how it gets handled — title format, editing steps, hashtag rules, posting schedule, required fields. Execution becomes filling in variables, not re-inventing the process each time.

**Together:**

> Filters reduce *what* you think about. Templates reduce *how* you think about it.

That combination creates scalability:
- New items flow in continuously — no manual sorting bottleneck
- Processing output is standardised — no variability from operator to operator
- Volume increases translate to throughput increases, not decision fatigue

The goal in any automated pipeline is to push as much work as possible into:

```
if condition_met → execute_predefined_action
```

### Layered Architecture

```
Input → Filter Layer 1 → Filter Layer 2 → Transform → Template → Delivery
```

**Filter layers narrow scope progressively:**

| Layer | Job | Example |
|---|---|---|
| Schema / validity | Only well-formed data enters | Format checks, duration minimums, resolution threshold |
| Business logic | Only high-value items proceed | Engagement score, profitability, rank threshold |
| Contextual | Only items matching current goal | Viral mode vs. retention mode vs. budget audience |

**Template layers handle output variation without multiplying complexity:**
- **Base template** — universal structure (hook → content → CTA)
- **Variant templates** — structural adjustments per context (short-form vs. long-form, TikTok vs. Shorts)
- **Dynamic fields** — placeholders filled from filtered attributes (tone, topic, score)

**The transformation layer** sits between filters and templates. Filters decide *what survives*. Transforms decide *what matters*. Templates decide *how it is expressed*.

**Separation of concerns is what makes this scalable.** Each layer is independently tunable. Each layer reduces complexity until the final step is almost trivial: fill structured fields and ship.

### Designing and Refining Combinations

Systems don't start with optimal filter-template pairings. They start with reasonable assumptions and improve through measurement.

**Stage 1 — Human-designed pairings.** Chosen by domain understanding: "high-engagement clips → viral short-form template." This already outperforms ad-hoc decision-making even before any optimisation.

**Stage 2 — Feedback loops.** Track outcomes: which filter thresholds produce better results? Which template gets higher retention? Adjustment becomes evidence-based. A/B testing and manual comparison work fine — no ML required.

**Stage 3 — ML (only when necessary).** Useful when there are too many combinations to test manually, or the system needs to adapt continuously. Conceptually it's the same loop — filter → transform → template → outcome → feedback → adjustment — ML just compresses the feedback and decision step.

**Filters and templates are coupled, not independent.** A filter is designed with a specific template in mind:
- "Fast viral short" template → filter for high-energy moments
- "Educational breakdown" template → filter for clarity and structure

**The scaling trick is constraint design.** Instead of 50 filters × 50 templates = 2,500 combinations, design 5 filters each mapped to 1–2 templates. The system becomes stable and predictable without needing intelligence overhead to manage it.
