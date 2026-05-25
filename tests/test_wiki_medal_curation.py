from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.game_onboarding import bridge_wiki_draft_to_onboarding, curate_wiki_medal_draft
from pipeline.simple_yaml import load_yaml_file
from tests.test_wiki_draft_onboarding_bridge import WikiDraftOnboardingBridgeTests


class WikiMedalCurationTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[dict[str, object]]) -> None:
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

    def _write_png(self, path: Path) -> str:
        png = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
            b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
            b"\x00\x00\x00\x0cIDATx\x9cc``\xf8\x0f\x00\x01\x01\x01\x00\x18\xdd\x8d\xb1"
            b"\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(png)
        return path.resolve().as_uri()

    def _write_raw_wiki_fixture(self, repo_root: Path) -> Path:
        wiki_root = repo_root / "assets" / "games" / "call_of_duty" / "drafts" / "wiki" / "raw-curation-fixture"
        (wiki_root / "catalog").mkdir(parents=True, exist_ok=True)
        media_root = repo_root / "wiki_media"
        triple_kill_url = self._write_png(media_root / "triple_kill.png")
        contract_url = self._write_png(media_root / "contract.png")
        card_url = self._write_png(media_root / "calling_card.png")
        logo_url = self._write_png(media_root / "logo.png")

        assets = [
            {
                "asset_id": "call_of_duty.event_badge_or_medal.triple_kill.keep1111",
                "game_id": "call_of_duty",
                "entity_id": "call_of_duty.triple_kill",
                "asset_family": "medal_icon",
                "display_name": "Triple Kill",
                "source_url": triple_kill_url,
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "draft_local_path": "",
                "template_path": "",
                "mask_path": "",
                "roi_ref": "hud.event_badge",
                "match_method": "TM_CCORR_NORMED",
                "threshold": 0.93,
                "scale_set": [0.9, 1.0, 1.1],
                "temporal_window": 3,
                "license_note": "internal_review_required",
                "qa_status": "needs_manual_crop",
            },
            {
                "asset_id": "call_of_duty.event_badge_or_medal.contract.drop2222",
                "game_id": "call_of_duty",
                "entity_id": "call_of_duty.big_game_contract",
                "asset_family": "medal_icon",
                "display_name": "A Big Game Bounty contract marks a player...",
                "source_url": contract_url,
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "draft_local_path": "",
                "template_path": "",
                "mask_path": "",
                "roi_ref": "hud.event_badge",
                "match_method": "TM_CCORR_NORMED",
                "threshold": 0.93,
                "scale_set": [0.9, 1.0, 1.1],
                "temporal_window": 3,
                "license_note": "internal_review_required",
                "qa_status": "needs_manual_crop",
            },
            {
                "asset_id": "call_of_duty.event_badge_or_medal.calling_card.drop3333",
                "game_id": "call_of_duty",
                "entity_id": "call_of_duty.aerial_pursuit",
                "asset_family": "medal_icon",
                "display_name": "\"Aerial Pursuit\" Epic Calling Card",
                "source_url": card_url,
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "draft_local_path": "",
                "template_path": "",
                "mask_path": "",
                "roi_ref": "hud.event_badge",
                "match_method": "TM_CCORR_NORMED",
                "threshold": 0.93,
                "scale_set": [0.9, 1.0, 1.1],
                "temporal_window": 3,
                "license_note": "internal_review_required",
                "qa_status": "needs_manual_crop",
            },
            {
                "asset_id": "call_of_duty.event_badge_or_medal.logo.drop4444",
                "game_id": "call_of_duty",
                "entity_id": "call_of_duty.logo",
                "asset_family": "medal_icon",
                "display_name": "Logo",
                "source_url": logo_url,
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "draft_local_path": "",
                "template_path": "",
                "mask_path": "",
                "roi_ref": "hud.event_badge",
                "match_method": "TM_CCORR_NORMED",
                "threshold": 0.93,
                "scale_set": [0.9, 1.0, 1.1],
                "temporal_window": 3,
                "license_note": "internal_review_required",
                "qa_status": "needs_manual_crop",
            },
        ]
        events = [
            {
                "event_id": "call_of_duty.triple_kill",
                "game_id": "call_of_duty",
                "display_name": "Triple Kill",
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "section_heading": "Medals[]",
            },
            {
                "event_id": "call_of_duty.big_game_contract",
                "game_id": "call_of_duty",
                "display_name": "A Big Game Bounty contract marks a player...",
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "section_heading": "Contracts[]",
            },
            {
                "event_id": "call_of_duty.aerial_pursuit",
                "game_id": "call_of_duty",
                "display_name": "\"Aerial Pursuit\" Epic Calling Card",
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "section_heading": "Calling Cards[]",
            },
            {
                "event_id": "call_of_duty.logo",
                "game_id": "call_of_duty",
                "display_name": "Logo",
                "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                "source_role": "events",
                "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                "section_heading": "Branding[]",
            },
        ]
        qa_rows = [
            {
                "asset_id": row["asset_id"],
                "display_name": row["display_name"],
                "qa_status": "needs_manual_crop",
                "source_url": row["source_url"],
                "source_role": row["source_role"],
                "source_page_url": row["source_page_url"],
            }
            for row in assets
        ]
        (wiki_root / "assets_manifest.json").write_text(
            json.dumps(
                {
                    "game_id": "call_of_duty",
                    "source_count": 1,
                    "assets": assets,
                    "qa_queue": qa_rows,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        self._write_csv(wiki_root / "catalog" / "assets.csv", assets)
        self._write_csv(wiki_root / "catalog" / "events_or_medals.csv", events)
        self._write_csv(
            wiki_root / "catalog" / "source_fetch_log.csv",
            [
                {
                    "source_page_url": "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone",
                    "source_role": "events",
                    "source_title": "Call of Duty: Warzone | Call of Duty Wiki | Fandom",
                    "status": "fetched",
                    "content_type": "text/html",
                    "section_count": 4,
                }
            ],
        )
        self._write_csv(wiki_root / "catalog" / "qa_queue.csv", qa_rows)
        return wiki_root

    def test_curate_wiki_medal_draft_drops_noise_and_keeps_multikill_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            wiki_root = self._write_raw_wiki_fixture(repo_root)
            output_root = repo_root / "assets" / "games" / "call_of_duty" / "drafts" / "wiki_curated" / "curated"

            result = curate_wiki_medal_draft(wiki_root, output_path=output_root, repo_root=repo_root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["counts"]["kept_asset_count"], 1)
            self.assertEqual(result["counts"]["kept_event_count"], 1)
            with (output_root / "catalog" / "assets.csv").open(encoding="utf-8", newline="") as handle:
                assets = list(csv.DictReader(handle))
            with (output_root / "catalog" / "events_or_medals.csv").open(encoding="utf-8", newline="") as handle:
                events = list(csv.DictReader(handle))
            with (output_root / "catalog" / "curation_decisions.csv").open(encoding="utf-8", newline="") as handle:
                decisions = list(csv.DictReader(handle))
            summary = json.loads((output_root / "catalog" / "curation_summary.json").read_text(encoding="utf-8"))

            self.assertEqual([row["display_name"] for row in assets], ["Triple Kill"])
            self.assertEqual([row["display_name"] for row in events], ["Triple Kill"])
            dropped_reasons = {row["reason"] for row in decisions if row["status"] == "dropped"}
            self.assertIn("contract_text", dropped_reasons)
            self.assertIn("calling_card", dropped_reasons)
            self.assertIn("logo_or_branding", dropped_reasons)
            self.assertEqual(summary["kept_asset_count"], 1)
            self.assertEqual(summary["kept_event_count"], 1)

    def test_curated_bundle_remains_bridge_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            bridge_helpers = WikiDraftOnboardingBridgeTests()
            bridge_helpers._copy_call_of_duty_starter_seed(repo_root)
            bridge_helpers._write_published_pack_fixture(repo_root)
            wiki_root = self._write_raw_wiki_fixture(repo_root)
            curated_root = repo_root / "assets" / "games" / "call_of_duty" / "drafts" / "wiki_curated" / "curated"
            onboarding_root = repo_root / "outputs" / "curated_bridge"

            curate_wiki_medal_draft(wiki_root, output_path=curated_root, repo_root=repo_root)
            result = bridge_wiki_draft_to_onboarding(curated_root, output_path=onboarding_root, repo_root=repo_root)

            self.assertTrue(result["ok"])
            manifest = json.loads((onboarding_root / "manifests" / "assets_manifest.json").read_text(encoding="utf-8"))
            detection_manifest = load_yaml_file(onboarding_root / "manifests" / "detection_manifest.yaml")
            self.assertEqual(len(manifest["candidates"]), 3)
            self.assertEqual(
                {row["asset_family"] for row in detection_manifest["rows"]},
                {"hero_portrait", "equipment_icon", "medal_icon"},
            )
            medal_rows = [row for row in detection_manifest["rows"] if row["asset_family"] == "medal_icon"]
            self.assertEqual(len(medal_rows), 1)
            self.assertEqual(medal_rows[0]["detection_id"], "call_of_duty.triple_kill.medal_icon")


if __name__ == "__main__":
    unittest.main()
