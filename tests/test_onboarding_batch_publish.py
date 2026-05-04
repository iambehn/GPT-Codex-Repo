from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.onboarding_batch_publish import publish_onboarding_batch
from pipeline.simple_yaml import dump_yaml_file


class OnboardingBatchPublishTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        headers = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            if not rows:
                writer.writerow({"empty": ""})
                return
            for row in rows:
                writer.writerow(row)

    def _write_draft(
        self,
        root: Path,
        *,
        game: str,
        slug: str,
        updated_at: str,
        accepted: bool,
        blocking_qa: str | None = None,
        info_qa: str | None = None,
    ) -> Path:
        draft_root = root / "assets" / "games" / game / "drafts" / "onboarding" / slug
        manifests_root = draft_root / "manifests"
        catalog_root = draft_root / "catalog"
        masters_root = draft_root / "masters"
        manifests_root.mkdir(parents=True, exist_ok=True)
        catalog_root.mkdir(parents=True, exist_ok=True)
        masters_root.mkdir(parents=True, exist_ok=True)

        asset_path = masters_root / "asset.png"
        asset_path.write_bytes(b"fakepng")

        dump_yaml_file(draft_root / "entities.yaml", {"heroes": [], "abilities": [], "events": []})
        dump_yaml_file(
            manifests_root / "detection_manifest.yaml",
            {
                "schema_version": "game_detection_manifest_v1",
                "game_id": game,
                "row_count": 1,
                "required_row_count": 1,
                "ready_row_count": 1,
                "rows_needing_assets": 0,
                "rows": [{"detection_id": f"{game}.hero", "target_id": "hero", "requires_asset": True}],
            },
        )
        (manifests_root / "onboarding_state.json").write_text(
            json.dumps(
                {
                    "schema_version": "game_onboarding_state_v1",
                    "game_id": game,
                    "phase_status": "bindings_pending",
                    "schema_path": "manifests/game_detection_schema.yaml",
                    "source_count": 1,
                    "updated_at": updated_at,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (manifests_root / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": game,
                    "phase_status": "bindings_pending",
                    "source_count": 1,
                    "source_fetch_log": [{"status": "fetched", "source_role": "roster"}],
                    "candidates": [{"candidate_id": "candidate-1", "master_path": str(asset_path)}],
                    "bindings": [],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        binding_rows = []
        if accepted:
            binding_rows.append({"detection_id": f"{game}.hero", "candidate_id": "candidate-1", "status": "accepted"})
        qa_rows = []
        if blocking_qa:
            qa_rows.append({"item_type": blocking_qa, "reason": "blocking finding"})
        if info_qa:
            qa_rows.append({"item_type": info_qa, "reason": "informational finding"})
        self._write_csv(catalog_root / "bindings.csv", binding_rows)
        self._write_csv(catalog_root / "qa_queue.csv", qa_rows)
        return draft_root

    def test_publish_onboarding_batch_dry_run_selects_latest_per_game(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            older = self._write_draft(
                root,
                game="marvel_rivals",
                slug="20260503T120000Z",
                updated_at="2026-05-03T12:00:00+00:00",
                accepted=True,
            )
            newer = self._write_draft(
                root,
                game="marvel_rivals",
                slug="20260503T130000Z",
                updated_at="2026-05-03T13:00:00+00:00",
                accepted=True,
            )
            blocked = self._write_draft(
                root,
                game="call_of_duty",
                slug="20260503T140000Z",
                updated_at="2026-05-03T14:00:00+00:00",
                accepted=True,
                blocking_qa="conflicting_identity_match",
            )

            result = publish_onboarding_batch(root / "assets" / "games")

            self.assertTrue(result["ok"])
            self.assertEqual(result["summary"]["ready"], 1)
            self.assertEqual(result["summary"]["blocked"], 1)
            self.assertEqual(result["summary"]["skipped"], 1)
            self.assertEqual(Path(result["ready"][0]["draft_root"]).resolve(), newer.resolve())
            self.assertEqual(Path(result["blocked"][0]["draft_root"]).resolve(), blocked.resolve())
            self.assertEqual(Path(result["skipped"][0]["draft_root"]).resolve(), older.resolve())
            self.assertTrue(result["blocked"][0]["identity_blocked"])
            self.assertEqual(result["blocked"][0]["identity_blocker_counts"]["conflicting_identity_match"], 1)
            self.assertEqual(result["blocked"][0]["identity_blocker_examples"][0]["type"], "conflicting_identity_match")

    def test_publish_onboarding_batch_apply_publishes_ready_drafts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ready = self._write_draft(
                root,
                game="marvel_rivals",
                slug="20260503T130000Z",
                updated_at="2026-05-03T13:00:00+00:00",
                accepted=True,
            )
            self._write_draft(
                root,
                game="call_of_duty",
                slug="20260503T140000Z",
                updated_at="2026-05-03T14:00:00+00:00",
                accepted=False,
            )

            with patch(
                "pipeline.onboarding_batch_publish.publish_onboarding_draft",
                return_value={"published_root": "/tmp/published/marvel_rivals", "template_count": 3},
            ) as mock_publish:
                result = publish_onboarding_batch(root / "assets" / "games", apply=True)

            self.assertTrue(result["ok"])
            self.assertEqual(result["summary"]["published"], 1)
            self.assertEqual(result["summary"]["blocked"], 1)
            self.assertEqual(Path(result["published"][0]["draft_root"]).resolve(), ready.resolve())
            self.assertEqual(mock_publish.call_count, 1)
            self.assertEqual(Path(mock_publish.call_args.args[0]).resolve(), ready.resolve())

    def test_publish_onboarding_batch_keeps_informational_identity_activity_non_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            ready = self._write_draft(
                root,
                game="call_of_duty",
                slug="20260503T150000Z",
                updated_at="2026-05-03T15:00:00+00:00",
                accepted=True,
                info_qa="canonical_identity_preference_applied",
            )

            result = publish_onboarding_batch(root / "assets" / "games")

            self.assertTrue(result["ok"])
            self.assertEqual(result["summary"]["ready"], 1)
            self.assertEqual(result["summary"]["blocked"], 0)
            self.assertEqual(Path(result["ready"][0]["draft_root"]).resolve(), ready.resolve())


if __name__ == "__main__":
    unittest.main()
