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
  - a bounded local OCR viability pass also weakens the text-fragment path on the current clips:
    - `_PL_5qWwKtY` full-card OCR recovers only partial signal such as `AkuRenatashiki` and weak fragments like `5 @ -`
    - `_PL_5qWwKtY` lower-text OCR does not recover a stable phrase from `ON A 5 KILL STREAK!`
    - `gcAGS3R2t2o` produces no OCR output on the tested full-card, badge, upper-name, or lower-text crops
    - negatives produce OCR noise such as `omy`, `@iso By`, and scoreboard-like fragments
  - this does not rule out `mixed_ocr_template` completely
  - it does mean the current local positives do not justify a broad OCR-first move on their own

recommendation:
  - do not attempt another local shell-derived template from the current crops
  - the next packet should explicitly bias toward one of these narrower anchor classes:
    - icon or emblem block
    - compact left-side badge cluster
    - stable text fragment, if and only if it is larger and cleaner than the current `gcAGS3R2t2o` positive and can outperform OCR noise on the current negative set
  - if the researcher cannot surface one of those with exact clip-backed evidence, retire the top-right family rather than continuing local template experiments

open_uncertainties:
  - whether higher-resolution same-family clips expose a reusable badge or emblem region
  - whether the current `gcAGS3R2t2o` positive is too truncated to support a stable text-fragment anchor
  - whether the family is intrinsically too presentation-variant for a template-first pilot
