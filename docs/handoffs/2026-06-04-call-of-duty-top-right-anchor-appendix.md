appendix_id:
  CALL_OF_DUTY_TOP_RIGHT_ANCHOR_APPENDIX_2026_06_04

related_packet_id:
  2026-06-04-call-of-duty-top-right-anchor-packet-request

purpose:
  Provide the current local visual and OCR evidence bundle for the active top-right anchor packet without changing the packet recommendation.

primary_decision_target:
  Choose one discriminative anchor strategy for the `call_of_duty` top-right native event-card family after the shell-anchor pilot failed.

supporting_evidence:
  - evidence_id: full_roi_montage
    path_or_url: /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/inspection/call_of_duty_top_right_anchor_diagnostics/top_right_roi_montage.png
    timestamp: mixed
    description: Side-by-side full top-right ROI crops for the two positives and three negatives.
    why_it_matters: Shows that `_PL_5qWwKtY` contains a clear left badge cluster and readable lower text, while the negatives still expose top-right HUD structure that can confuse shell-level matching.
  - evidence_id: matched_patch_montage
    path_or_url: /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/inspection/call_of_duty_top_right_anchor_diagnostics/matched_patch_montage.png
    timestamp: mixed
    description: Exact highest-scoring matched patches from the failed shell-anchor pilot across positives and negatives.
    why_it_matters: Shows the matcher latched onto generic local contrast and edge patterns rather than a stable event-card identity.
  - evidence_id: pos_pl_full_crop
    path_or_url: /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_pl_12.75.png
    timestamp: `_PL_5qWwKtY @ 12.75s`
    description: Stronger positive with visible left badge cluster, username line, and lower text line.
    why_it_matters: Best local candidate for a narrower left-side badge or emblem anchor.
  - evidence_id: pos_gc_full_crop
    path_or_url: /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_gc_28.0.png
    timestamp: `gcAGS3R2t2o @ 28.0s`
    description: Weaker same-family positive with small, partially smeared card content.
    why_it_matters: Sets the lower bound on what any reusable anchor must tolerate across same-family examples.
  - evidence_id: local_ocr_positive_pl
    path_or_url: /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/inspection/call_of_duty_top_right_anchor_diagnostics/ocr/pos_pl_upper_name.png
    timestamp: `_PL_5qWwKtY @ 12.75s`
    description: Preprocessed OCR crop from the stronger positive upper-name region.
    why_it_matters: Local OCR recovered only partial signal such as `AkuRenatashiki`, which is not enough by itself to justify an OCR-first move.
  - evidence_id: local_ocr_positive_gc
    path_or_url: /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/inspection/call_of_duty_top_right_anchor_diagnostics/ocr/pos_gc_full.png
    timestamp: `gcAGS3R2t2o @ 28.0s`
    description: Preprocessed OCR crop from the weaker same-family positive full-card region.
    why_it_matters: Local OCR recovered no useful output on this clip, which weakens the case for a text-fragment anchor unless a better clip exists.

secondary_examples:
  - `_PL_5qWwKtY @ 12s-14s` is currently the strongest local evidence for a badge-cluster or emblem anchor.
  - `gcAGS3R2t2o @ 27s-29s` confirms the family exists in a second clip, but also shows how quickly text readability collapses at smaller presentation scale.

negative_examples:
  - `SVbTc2AZzYw.60s-70s` full ROI and matched patch: flat bright region still scored like a positive under the shell pilot.
  - `v-SzAArdAfY.60s-70s` full ROI and matched patch: dark bar plus bright edge highlight still scored like a positive under the shell pilot.
  - `Qop1sH70nHI.60s-70s` full ROI and matched patch: wall-edge texture boundary still scored like a positive under the shell pilot.
  - local OCR on negatives produced noise such as `omy`, `@iso By`, and scoreboard-like fragments rather than a clean separable failure pattern.

source_excerpts:
  - shell-pilot score band:
    - positives: `0.95194` to `0.95271`
    - negatives: `0.95325` to `0.95557`
  - local OCR viability:
    - `_PL_5qWwKtY` partial signal only
    - `gcAGS3R2t2o` no useful OCR recovery
    - negatives produce OCR noise

operator_notes:
  - treat the appendix as support for the anchor packet only
  - it does not change the current recommendation against more shell-derived template work
  - if the next packet recommends a text fragment, it should explain why that fragment is stronger than the current local OCR evidence
