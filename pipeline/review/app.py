"""
Stage 7 — Manual Review UI

Flask web app for reviewing processed clips before distribution.
Clips sit in processing/{game}/ after the AI Scoring stage. The reviewer
watches each clip, sees the virality score and suggested metadata, optionally
makes trim/crop adjustments, then approves (→ accepted/) or rejects (→ rejected/).

Routes:
  GET  /                        — queue view: list all clips pending review
  GET  /clip/<clip_id>          — review a single clip (video player + score + toolbar)
  POST /clip/<clip_id>/approve  — approve clip, move to accepted/, load next
  POST /clip/<clip_id>/reject   — reject clip, move to rejected/, load next
"""

from flask import Flask

app = Flask(__name__)


@app.route("/")
def queue():
    """Display the list of clips pending manual review."""
    pass


@app.route("/clip/<clip_id>")
def review_clip(clip_id: str):
    """Show the video player and metadata for a single clip."""
    pass


@app.route("/clip/<clip_id>/approve", methods=["POST"])
def approve_clip(clip_id: str):
    """Approve a clip: move to accepted/{game}/, load the next clip."""
    pass


@app.route("/clip/<clip_id>/reject", methods=["POST"])
def reject_clip(clip_id: str):
    """Reject a clip: move to rejected/{game}/, load the next clip."""
    pass


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
