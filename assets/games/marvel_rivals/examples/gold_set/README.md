# Marvel Rivals Gold Set

Use this folder to measure whether detector metadata and `clip_judge` decisions
match human-labeled expectations before training YOLO or tuning weights.

Recommended first batch:
- 10 clear accepts with the payoff or promise visible inside the first 1.5s.
- 10 rejects with weak action, unclear payoff, or poor narrative value.
- 10 quarantines with missing hero/event context, UI drift, hook issues, or missing ROI templates.

Run:
`python run.py --evaluate-game-pack marvel_rivals`

Use `--skip-eval-detectors` when sidecar fixtures already contain detector
outputs and you only want to test `hook_enforcer` plus `clip_judge`.
