# Highlight Detection Research and Adoption Shortlist

This document is a focused research-and-triage pass for improving the repo's highlight-detection stack.

It is optimized for:

- cross-game shooter / esports-style gameplay
- decision-ready adoption choices, not generic exploration
- the current staged `hf_multimodal` architecture in [config.yaml](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/config.yaml)

The goal is to identify which external models, datasets, papers, and repos are worth keeping, trialing, or ignoring for the next highlight-quality upgrades.

## Current Baseline

The current `hf_multimodal` stack is:

- proposal generation: `georgesung/shot-boundary-detection-transnet-v2`
- transcript / audio cues: `openai/whisper-large-v3-turbo`
- semantic video scoring: `microsoft/xclip-base-patch32`
- visual novelty / redundancy: `google/siglip-so400m-patch14-384`
- top-N reranking: `HuggingFaceTB/SmolVLM2-2.2B-Instruct`

This shortlist judges alternatives against that staged baseline, not against an abstract end-to-end video AI stack.

## Search Surfaces Used

Primary surfaces:

- Hugging Face model cards and docs
- Hugging Face dataset cards
- Hugging Face paper pages
- GitHub repos with maintained inference or evaluation code

The searches were filtered toward:

- shot / scene segmentation
- ASR with timestamps
- video-text relevance
- action / event classification
- image or video embeddings for novelty
- highlight-detection and moment-retrieval benchmarks

## Triage Rubric

Each candidate was judged on:

- task fit for proposal, transcript, semantic, novelty, rerank, or evaluation
- domain fit for broadcast-like gameplay or esports footage
- inference fit for local or selective windowed execution
- I/O fit for the repo's existing structured stage outputs
- evaluation evidence from model cards, papers, or benchmark code
- operational fit: license, maintenance, model size, and runtime complexity

Early reject rules:

- primarily video generation or editing
- requires heavy full-video inference with no selective path
- unclear licensing
- no maintained inference path
- solves a different problem, like generic video chat or captioning-only

## Ranked Adoption Shortlist

### 1. Proposal Generation

| Rank | Candidate | Source | Recommendation | Why it fits | Runtime posture | Acceptance gate | Reject if |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `TransNetV2` | [HF model card](https://huggingface.co/Sn4kehead/TransNetV2) and [PyTorch port](https://huggingface.co/magnusdtd/TransNetV2) | `keep` | Best fit for cheap shot-boundary-first proposal generation. Maps directly to the repo's proposal-window flow. | local, first-stage only | Keep if proposal recall remains strong on real gameplay clips and latency stays low. | Reject only if replay data shows consistent missed transitions that a stronger cheap proposal source fixes. |
| 2 | `PySceneDetect` | [GitHub](https://github.com/Breakthrough/PySceneDetect) | `trial` | Strong cheap non-neural counterpoint for hard-cut and transition sanity checks. Useful as an ensemble or fallback source, not a replacement. | local, first-stage only | Trial if you want a low-cost A/B baseline against TransNetV2 on broadcast-heavy footage. | Reject if HUD motion and effects create excessive false cuts. |
| 3 | query-conditioned MR/HD models | [Moment-DETR repo](https://github.com/jayleicn/moment_detr), [UMT repo](https://github.com/TencentARC/UMT) | `ignore for proposal stage` | They solve highlight detection or moment retrieval, not cheap first-pass proposal generation. | too heavy for stage 1 | None for immediate adoption. | Reject for now because they violate the cheap-proposal-first design. |

Decision:

- keep `TransNetV2` as the proposal backbone
- only add `PySceneDetect` if you want a low-cost ensemble benchmark

### 2. Transcript / Audio Cues

| Rank | Candidate | Source | Recommendation | Why it fits | Runtime posture | Acceptance gate | Reject if |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `openai/whisper-large-v3-turbo` | [HF model card](https://huggingface.co/openai/whisper-large-v3) and [HF model listing](https://huggingface.co/models?search=whisper-large-v3) | `keep` | Strong general ASR, broad language support, and already integrated. Good default for transcript salience and keyword cues. | local | Keep if transcript quality is adequate on commentary-heavy clips and runtime is acceptable. | Reject only if runtime or memory is the dominant bottleneck. |
| 2 | `distil-whisper/distil-large-v3.5` | [HF model card](https://huggingface.co/distil-whisper/distil-large-v3.5) | `trial` | Best near-term ASR swap candidate. Explicitly optimized for long-form and chunked transcription, with documented speed-oriented paths. | local | Trial if ASR latency is a real pain point or if you want better long-form throughput without abandoning Whisper-style outputs. | Reject if commentary accuracy drops materially on noisy gameplay audio. |
| 3 | Whisper assistant / faster variants | [HF model search](https://huggingface.co/models?search=whisper-large-v3) | `trial later` | Good optimization surface, but more of a runtime optimization family than a semantic upgrade. | local | Trial only after baseline transcript quality is validated. | Reject if the complexity only buys small wall-clock gains. |

Decision:

- keep `whisper-large-v3-turbo` as the main baseline
- trial `distil-large-v3.5` first if ASR speed becomes the main constraint

### 3. Semantic Video Scoring

| Rank | Candidate | Source | Recommendation | Why it fits | Runtime posture | Acceptance gate | Reject if |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `microsoft/xclip-base-patch32` / zero-shot family | [HF current family listing](https://huggingface.co/models?filter=video-classification) and [zero-shot card](https://huggingface.co/microsoft/xclip-base-patch16-zero-shot) | `keep` | Best direct fit for the current query-conditioned segment scoring stage. Designed for video-language transfer and already aligned to proposal-window scoring. | local, per-window | Keep if generic queries like `clutch play` and `objective swing` remain useful ranking features. | Reject only if real-clip evaluation shows weak separation on gameplay-specific events. |
| 2 | `google/videoprism` `LvT-B` | [HF model card](https://huggingface.co/google/videoprism) | `trial` | Strongest candidate for a higher-quality video-text embedding upgrade. The released `LvT` variants are explicitly video-text encoders with strong benchmark claims. | local or optional endpoint, per-window | Trial if you want a stronger semantic encoder and can tolerate more memory/runtime than X-CLIP. | Reject if the embedding cost is too high for shortlist-time scoring. |
| 3 | `VideoMAE` family | [HF docs](https://huggingface.co/docs/transformers/main/model_doc/videomae) | `trial as supplement, not replacement` | Good action-recognition backbone, but weaker fit for text-conditioned highlight queries. Best used as a supplemental action prior, not the main semantic scorer. | local, per-window | Trial only if you want explicit action-label features in addition to text matching. | Reject if it adds label noise without improving score separation. |
| 4 | MR/HD transformer families | [VideoLights paper](https://huggingface.co/papers/2412.01558), [Saliency-Guided DETR paper](https://huggingface.co/papers/2410.01615), [UMT paper](https://huggingface.co/papers/2203.12745) | `ignore for direct drop-in scoring` | Important research references, but they are training-and-benchmark systems, not clean low-friction semantic stage swaps. | offline research only | Revisit only after evaluation data and training loops exist. | Reject for now because they are not lightweight inference replacements for X-CLIP. |

Decision:

- keep `X-CLIP` as the current practical default
- trial `VideoPrism-LvT-B` as the first serious semantic-upgrade candidate
- treat `VideoMAE` as optional action-side enrichment, not the main query scorer

### 4. Visual Novelty / Redundancy

| Rank | Candidate | Source | Recommendation | Why it fits | Runtime posture | Acceptance gate | Reject if |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `google/siglip-so400m-patch14-384` | [HF docs](https://huggingface.co/docs/transformers/model_doc/siglip) | `keep` | Strong fit for image embedding, similarity, and redundancy suppression. Already matches the repo's midpoint-keyframe novelty design. | local, per-window keyframe | Keep if it continues to separate repeated HUD/combat shots from visually distinct moments. | Reject only if replay analysis shows poor novelty signal on gameplay-specific repetition. |
| 2 | `google/videoprism` embeddings | [HF model card](https://huggingface.co/google/videoprism) | `trial` | If adopted for semantic scoring, the same family can provide stronger video-aware embeddings for novelty and clustering than single-frame image-only embeddings. | heavier local or optional endpoint | Trial only after a `VideoPrism` semantic experiment exists. | Reject if the added complexity does not improve candidate diversity. |

Decision:

- current evidence supports keeping `SigLIP` as the novelty default
- no clearly better low-friction HF-native replacement surfaced for this stage

### 5. Top-N Reranking

| Rank | Candidate | Source | Recommendation | Why it fits | Runtime posture | Acceptance gate | Reject if |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `HuggingFaceTB/SmolVLM2-2.2B-Instruct` | [HF model card](https://huggingface.co/HuggingFaceTB/SmolVLM2-2.2B-Instruct) | `keep` | Best current fit for lightweight top-N multimodal reranking. The model card explicitly supports image and video reasoning and is small enough for local selective use. | local, top-N only | Keep if rerank quality beats base-score ordering on reviewed clips without blowing latency. | Reject if rerank quality is flat relative to cost. |
| 2 | `Lighthouse` MR/HD stack | [GitHub](https://github.com/line/lighthouse) and [paper](https://huggingface.co/papers/2408.02901) | `trial as benchmark harness, not as reranker` | Most useful as an offline comparison framework covering MR/HD models, datasets, and a Gradio demo, not as a drop-in per-candidate reranker. | offline evaluation | Trial if you want to compare repo heuristics against benchmarked MR/HD systems. | Reject as a direct reranker replacement because it changes the architecture, not just the last stage. |
| 3 | end-to-end HD/MR model families | [QVHighlights paper](https://huggingface.co/papers/2107.09609), [Moment-DETR repo](https://github.com/jayleicn/moment_detr), [UMT repo](https://github.com/TencentARC/UMT) | `trial later` | Valuable longer-term benchmark targets, but they are whole-task systems. They are not clean drop-ins for the current shortlist-only VLM stage. | offline research / training | Revisit only after you have a benchmark loop and labeled comparisons. | Reject for now because they would replace too much of the pipeline at once. |

Decision:

- keep `SmolVLM2` as the default reranker
- use `Lighthouse` as the main offline comparison harness if you want to benchmark against dedicated highlight-detection systems

### 6. Evaluation Datasets / Benchmarks

| Rank | Candidate | Source | Recommendation | Why it fits | Runtime posture | Acceptance gate | Reject if |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `QVHighlights` | [HF paper page](https://huggingface.co/papers/2107.09609), [HF dataset mirror](https://huggingface.co/datasets/VLM2Vec/QVHighlight), [Moment-DETR repo](https://github.com/jayleicn/moment_detr) | `trial first` | Best single benchmark for joint highlight detection and moment retrieval with saliency scores. Strongest fit for query-conditioned highlight ranking. | offline evaluation | Adopt first if you want a benchmark that directly resembles "find the best moments" logic. | Reject only if the query-conditioned setup proves too far from your operator workflow. |
| 2 | `Lighthouse` benchmark coverage | [GitHub](https://github.com/line/lighthouse) | `trial first` | Best evaluation harness because it already supports QVHighlights, TVSum, YouTube Highlights, and multiple MR/HD models. | offline evaluation | Adopt if you want comparable evaluation and inference APIs without assembling a benchmark stack from scratch. | Reject if the feature-extraction dependencies are too heavy for current priorities. |
| 3 | `TVSum` and `YouTube Highlights` | [Lighthouse supported datasets](https://github.com/line/lighthouse) | `trial` | Useful classic highlight-detection baselines. Domain mismatch exists, but they are still useful for regression checks and general highlight ranking. | offline evaluation | Use as secondary baselines after QVHighlights. | Reject as primary guidance for gameplay if they mislead the ranking decisions. |
| 4 | `MultiSports` / `SportsAction` | [HF paper page](https://huggingface.co/papers/2105.07404), [HF dataset card](https://huggingface.co/datasets/MCG-NJU/SportsAction) | `trial for sports action transfer` | Strong sports action localization benchmark. Better for event/action priors than for final highlight scoring. | offline evaluation | Use if you want to test whether sports-action features improve gameplay event priors. | Reject if action localization signals do not transfer to game footage. |
| 5 | `SoccerHigh` and `SoccerNet` | [SoccerHigh paper listing](https://huggingface.co/papers?q=soccer+video+datasets), [SoccerNet community datasets](https://huggingface.co/SoccerNet/datasets) | `trial later` | Best sports-specific summarization direction surfaced in search. Good for sports highlight structure, but less transferable than QVHighlights for generic gameplay. | offline evaluation | Trial after the generic benchmark loop exists. | Reject as an early primary benchmark because soccer broadcast structure is too narrow. |

Decision:

- first benchmark adoption target: `QVHighlights`
- best evaluation harness: `Lighthouse`
- best secondary sports-transfer benchmark: `MultiSports`
- best sports-specific later benchmark: `SoccerHigh` / `SoccerNet`

## Keep / Trial / Ignore Summary

### Keep

- `TransNetV2`
- `openai/whisper-large-v3-turbo`
- `microsoft/xclip-base-patch32`
- `google/siglip-so400m-patch14-384`
- `HuggingFaceTB/SmolVLM2-2.2B-Instruct`

### Trial Next

- `PySceneDetect` as a cheap proposal ensemble baseline
- `distil-whisper/distil-large-v3.5` as the first ASR latency/efficiency trial
- `google/videoprism` `LvT-B` as the first serious semantic upgrade candidate
- `QVHighlights` as the first highlight benchmark
- `Lighthouse` as the first offline benchmark harness
- `MultiSports` as the best sports action-transfer benchmark

### Ignore For Now

- video-generation or avatar-generation skills and models
- full end-to-end highlight systems as direct drop-ins for proposal or rerank stages
- generic video chat systems that do not map cleanly to structured scoring
- robotics or unrelated multimodal datasets that surfaced in broad dataset search

## Trial Order

1. Low-cost stage upgrades
   - `PySceneDetect` A/B against `TransNetV2`
   - `distil-whisper/distil-large-v3.5` A/B against current Whisper
2. Stronger evaluation data
   - adopt `QVHighlights`
   - stand up `Lighthouse` for offline MR/HD comparison
   - add `MultiSports` as a sports-transfer reference set
3. Heavier semantic or multimodal upgrades
   - `VideoPrism-LvT-B` semantic trial
   - deeper MR/HD model comparison via `Lighthouse`
   - only then reconsider bigger architectural changes

## High-Noise Search Categories To Avoid

Avoid spending time on:

- Codex skills for video generation, avatars, or editing
- text-to-video or image-to-video model searches
- generic multimodal chat-over-video models with no evaluation path
- datasets with no temporal labels or no highlight relevance
- training-heavy architectures before benchmark adoption exists

## Final Recommendation

The current stack is directionally sound. The highest-value next work is not replacing it wholesale.

The next disciplined upgrade path is:

1. keep the current staged architecture
2. add a real benchmark loop around `QVHighlights` and `Lighthouse`
3. run one low-cost ASR trial and one low-cost proposal baseline trial
4. only then test `VideoPrism` as a heavier semantic upgrade

That path improves decision quality without collapsing the repo into a monolithic end-to-end highlight model experiment.
