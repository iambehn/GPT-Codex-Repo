# Codex Skills and Plugins for This Repo

This repo uses a small set of Codex skills and plugins to keep HF evaluation, review flows, and external research more consistent.

## Current posture

Use installed plugins first:
- `Hugging Face`
- `Google Drive`
- `GitHub`
- `Browser Use`

Do not create a new plugin for this repo unless a repeated workflow cannot be handled by:
- an existing plugin
- a user-global skill
- a repo-local skill under `.agents/skills/`

## Repo-local custom skills

Local skills live in:
- `.agents/skills/`

Current repo-local skills:

| Skill | Use for |
| --- | --- |
| `hf-eval-operator` | Proxy/runtime calibration and replay runs, report reading, trial recommendations |
| `review-bridge-operator` | Proxy/runtime/fused/onboarding identity review session prep/apply/cleanup |
| `fullstack-developer` | General full-stack app work when the repo grows a UI or service surface |

Repo-local skills now also include minimal `agents/openai.yaml` metadata for cleaner Codex presentation. The metadata is intentionally minimal for now, and icon assets are deferred until these skills prove stable and repeatedly useful.

## Recommended user-global skills and plugins

These are the best fit for the current project shape.

| Tool | Why it fits |
| --- | --- |
| `hugging-face:hf-cli` | Pinned model pulls, Hub metadata checks, artifact hygiene |
| `hugging-face:huggingface-community-evals` | Repeatable model/backend comparison workflows |
| `hugging-face:huggingface-gradio` | Lightweight local review UIs for shortlist and rerank inspection |
| `github:gh-fix-ci` | CI debugging once PR volume increases |
| `github:gh-address-comments` | Faster review-thread closure and PR cleanup |
| `firecrawl` | Current external model/docs/benchmark research when local context is insufficient |

Optional follow-ons:
- `spreadsheets` or `google-drive:google-sheets` for experiment logs and calibration tracking
- `browser-use:browser` when a local review UI needs regular in-app testing

## How to discover skills

Browse:
- [skills.sh](https://skills.sh/)

Search:

```bash
npx skills find "hugging face"
npx skills find "evaluation"
npx skills find "gradio"
npx skills find "github ci"
npx skills find "firecrawl"
```

Local skill locations:
- `~/.codex/skills`
- `~/.agents/skills`
- `.agents/skills/`

This repo also tracks installed skill metadata in:
- `skills-lock.json`

## Which tool for which task

| Task | Preferred tool |
| --- | --- |
| HF model/runtime work | `Hugging Face` plugin, `hugging-face:hf-cli` |
| Calibration/replay work | `hf-eval-operator` |
| PR review and CI work | `GitHub` plugin, `github:gh-fix-ci`, `github:gh-address-comments` |
| Review-bridge sessions | `review-bridge-operator` |
| External docs and current model research | `firecrawl` |
| Local review UI testing | `Browser Use` |

For the shadow operator wrapper:
- use `python3 run.py --run-shadow-operator ...`
- the canonical target-specific runbook for `approved_or_selected_probability` lives in [docs/v2/REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)

## Validation standard

When adding or relying on a skill or plugin for this repo:
- confirm it is available in the current Codex session
- validate it on one realistic task
- prefer the smallest tool that cleanly covers the workflow
- avoid introducing a new plugin when a skill or existing plugin already covers the job
