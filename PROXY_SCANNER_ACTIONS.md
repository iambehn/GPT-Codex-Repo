# Proxy Scanner Next-Stage Roadmap

This roadmap translates [ProxyScannerNotes .md](/Users/tj/Downloads/ProxyScannerNotes%20.md) into a repo-facing plan for the next proxy-scanner stage.

It is written against the intended proxy baseline:

- chat velocity scanning already exists
- a shared `ProxySignal -> ProxyWindow` layer already exists
- `--scan-chat-log` already emits debug-oriented output

If this working tree does not yet contain that baseline, treat this document as the execution plan for the branch or repo state where those pieces land first.

---

## Already Covered

These ideas from the note should be treated as already established baseline, not new backlog:

- chat velocity as a cheap upstream proxy signal
- weighted signal scoring instead of raw hit counts
- shared proxy window merging and same-source deduplication
- JSON-first debug output for proxy analysis

These are part of the assumed starting point for the next stage.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. Proxy Source Registry And Contracts

Turn the current chat-only path into a source-pluggable subsystem.

Required contract changes:

- `ProxySignal` becomes the stable cross-source event shape
- extend it with:
  - `kind`
  - `source`
  - `timestamp`
  - `confidence`
  - `strength`
  - `evidence`
  - `tier`
  - optional `quality_flags`
- `ProxyWindow` remains the fused output, but add:
  - `trigger_reasons`
  - `quality_penalties`
  - `recommended_action`
  - optional `estimated_cost`

Required behavior:

- add a registry-style source interface so each cheap scanner returns `list[ProxySignal]`
- the chat scanner becomes the first registered source
- future sources plug into the same fusion path without changing the chat scanner again

Acceptance target:

- the registry can enable or disable sources cleanly
- source failures can be reported without collapsing the whole scan

### B. Cheap Source Expansion

Add the next two low-cost proxy sources before any heavy download workflow.

#### 1. Playlist / HLS Scanner

Extract cheap proxy signals from playlist behavior:

- manifest churn
- segment cadence changes
- discontinuities
- bursty segment patterns
- bitrate or variant shifts when available

Required constraints:

- no media decode required
- synthetic playlist fixtures should drive the tests

#### 2. Audio-Only Prepass

Add a low-cost audio scanner:

- low-rate mono decode
- rolling baseline / z-score spike detection
- explicit low-SNR or noisy-segment penalties

Required constraints:

- audio stays a cheap prepass
- no transcript or Whisper step in this phase

#### 3. Cheap Visual Proxies Later

Do not make cheap visual proxies a blocking dependency for this stage.

Keep them as a later active extension only if:

- playlist plus audio still leave obvious recall gaps
- the fusion layer needs another cheap signal family before heavy download

Acceptance target:

- chat, playlist, and audio can all emit `ProxySignal` events through the same contract

### C. Fusion And Cost-Aware Gating

Extend the current proxy window builder into a real multi-source fusion layer.

Required behavior:

- strongest signal per source contributes to the fused score
- agreement across different source types increases trust
- noisy or low-quality evidence adds explicit penalties
- every window gets a deterministic `recommended_action`:
  - `skip`
  - `inspect`
  - `download_candidate`

Scoring boundaries:

- stay heuristic and config-driven
- do not introduce a probabilistic model in this phase
- keep the first `--scan-vod` pass analysis-only

Important boundary:

- the first VOD scan stage stops at:
  - raw signals
  - fused windows
  - source failures
  - recommended actions
- do not add automatic segment download yet

Acceptance target:

- mixed-source windows get agreement bonuses
- weak or noisy evidence shows up as penalty reasons
- windows can be triaged without downloading media

### D. CLI, Config, And Debug Surfaces

Add an analysis-first entrypoint:

- `--scan-vod URL GAME`
- optional `--chat-log PATH`

Expected behavior:

- collect enabled cheap sources
- emit raw signals plus fused windows
- report per-source failures unless every source failed

Config additions should stay global for now:

- `proxy_scanner.sources.<source_name>`
- `proxy_scanner.weights`
- `proxy_scanner.candidate_selection`
- `proxy_scanner.cost_gates`

Visibility rules:

- use JSON output first
- use logs for failure notes
- add sidecar or debug artifacts only if needed for diagnosis
- do not add a dashboard in this phase

Acceptance target:

- `--scan-vod` is usable as an analysis tool before any download orchestration exists

---

## Deferred

These ideas from the note are valid, but should stay out of the next implementation phase:

- cost dashboard or separate reporting UI
- automatic segment download orchestration
- full heavy-signal pipeline integration
- broad YouTube-first parity if it complicates the Twitch/HLS-first path
- generic business or product theory

---

## Implementation Order

Build in this order:

1. Stabilize shared proxy contracts and add the source registry.
2. Register the existing chat scanner under that interface.
3. Add the playlist / HLS scanner with synthetic fixtures.
4. Add the audio-only prepass with synthetic fixtures.
5. Add `--scan-vod` analysis mode that fuses all enabled cheap sources.
6. Add deterministic `recommended_action` output.
7. Add cheap visual proxies only if the first three source families remain too weak.

This order keeps the proxy scanner analysis-first and prevents early coupling to download orchestration.

---

## Future Test Targets

The implementation following this roadmap should prove:

- the registry accepts multiple sources and skips disabled sources cleanly
- chat-only scans still produce the same spikes and windows as the current baseline
- playlist fixtures produce deterministic proxy signals without media decode
- audio fixtures produce deterministic spike signals with baseline-relative gating
- mixed-source windows receive agreement bonuses and noise penalties
- `--scan-vod` returns:
  - raw signals
  - fused windows
  - source failure notes
  - recommended actions
- no automatic media download occurs in the first `--scan-vod` implementation
- no dashboard or separate UI surface is introduced

---

## Assumptions

- this document is a roadmap, not a code change spec
- the current proxy baseline is assumed even if this working tree does not yet contain it
- Twitch / HLS is the first-class path for the next stage
- YouTube parity is deferred unless it falls out naturally from the same interfaces
- the next proxy stage is analysis-first, with download orchestration intentionally deferred until cheap-signal quality is proven
