title:
  Call of Duty top-right anchor local diagnostics

status:
  completed

decision_target:
  Determine whether the current local positive and negative clips already reveal a narrower top-right anchor candidate before additional external packet work.

repo_truth:
  - the top-right shell-anchor pilot was already rolled back
  - the remaining question is whether the current clips expose a more discriminative sub-anchor inside the same family

local_artifacts:
  - full ROI montage:
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/top_right_roi_montage.png`
  - exact matched-patch montage:
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/matched_patch_montage.png`
  - per-sample crops:
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_pl_12.75.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_gc_28.0.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_sv_4.75.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_vs_4.75.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_qop_4.75.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_pl_matched_patch.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_gc_matched_patch.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_sv_matched_patch.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_vs_matched_patch.png`
    - `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_qop_matched_patch.png`

structured_findings:
  - the exact matched patches clarify why the shell pilot failed:
    - `_PL_5qWwKtY` positive patch is mostly terrain/background rather than a clean emblem or icon block
    - `gcAGS3R2t2o` positive patch contains a small text-bearing slice, but it is horizontally truncated and low-resolution
    - `SVbTc2AZzYw` negative patch is essentially flat bright background
    - `v-SzAArdAfY` negative patch is a dark bar plus a bright edge highlight
    - `Qop1sH70nHI` negative patch is a wall edge / flat texture boundary
  - this means the current highest-scoring matches are not locking onto a stable event-card identity
  - they are locking onto generic local contrast patterns inside the top-right ROI

recommendation:
  - do not attempt another local shell-derived template from the current crops
  - the next packet should explicitly bias toward one of these narrower anchor classes:
    - icon or emblem block
    - compact left-side badge cluster
    - stable text fragment, if and only if it is large enough to justify `mixed_ocr_template`
  - if the researcher cannot surface one of those with exact clip-backed evidence, retire the top-right family rather than continuing local template experiments

open_uncertainties:
  - whether higher-resolution same-family clips expose a reusable badge or emblem region
  - whether the current `gcAGS3R2t2o` positive is too truncated to support a stable text-fragment anchor
  - whether the family is intrinsically too presentation-variant for a template-first pilot
