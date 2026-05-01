from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.event_mapper import (
    EventMapperError,
    load_matcher_report,
    map_matcher_result,
    map_roi_events,
)
from run import main as run_main
from run import run_map_roi_events


class EventMapperTests(unittest.TestCase):
    def _write_published_pack(self, root: Path) -> None:
        game_root = root / "assets" / "games" / "marvel_rivals"
        (game_root / "manifests").mkdir(parents=True, exist_ok=True)
        (game_root / "game.yaml").write_text(
            "\n".join(
                [
                    "game_id: marvel_rivals",
                    'display_name: "Marvel Rivals"',
                    "resolution_profiles:",
                    '  normalize_to: "64x36"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "entities.yaml").write_text(
            "\n".join(
                [
                    "heroes:",
                    "  - hero_id: punisher",
                    '    display_name: "Punisher"',
                    "abilities:",
                    "  - ability_id: frag-grenade",
                    '    display_name: "Frag Grenade"',
                    "events:",
                    "  - event_id: triple-kill",
                    '    display_name: "Triple Kill"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "hud.yaml").write_text(
            "\n".join(
                [
                    "rois:",
                    "  hero_portrait:",
                    "    x_pct: 0.0",
                    "    y_pct: 0.0",
                    "    w_pct: 0.5",
                    "    h_pct: 0.5",
                    "  medal_area:",
                    "    x_pct: 0.5",
                    "    y_pct: 0.0",
                    "    w_pct: 0.5",
                    "    h_pct: 0.5",
                    "  ability_hud:",
                    "    x_pct: 0.0",
                    "    y_pct: 0.5",
                    "    w_pct: 0.5",
                    "    h_pct: 0.5",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        (game_root / "weights.yaml").write_text("weights: {}\nthresholds: {}\ngates: {}\n", encoding="utf-8")
        (game_root / "manifests" / "assets_manifest.json").write_text(
            json.dumps({"game_id": "marvel_rivals", "published_assets": []}, indent=2),
            encoding="utf-8",
        )
        (game_root / "manifests" / "cv_templates.yaml").write_text(
            "\n".join(
                [
                    "templates:",
                    "  - asset_id: marvel_rivals.punisher.hero_portrait",
                    "    asset_family: hero_portrait",
                    '    display_name: "Punisher"',
                    "    roi_ref: hero_portrait",
                    '    template_path: "templates/heroes/punisher.png"',
                    '    match_method: "TM_CCOEFF_NORMED"',
                    "    threshold: 0.9",
                    "    temporal_window: 3",
                    "    scale_set: [1.0]",
                    "  - asset_id: marvel_rivals.triple-kill.medal_icon",
                    "    asset_family: medal_icon",
                    '    display_name: "Triple Kill"',
                    "    roi_ref: medal_area",
                    '    template_path: "templates/medals/triple-kill.png"',
                    '    match_method: "TM_CCORR_NORMED"',
                    "    threshold: 0.95",
                    "    temporal_window: 2",
                    "    scale_set: [1.0]",
                    "  - asset_id: marvel_rivals.frag-grenade.ability_icon",
                    "    asset_family: ability_icon",
                    '    display_name: "Frag Grenade"',
                    "    roi_ref: ability_hud",
                    '    template_path: "templates/abilities/frag-grenade.png"',
                    '    match_method: "TM_CCOEFF_NORMED"',
                    "    threshold: 0.93",
                    "    temporal_window: 3",
                    "    scale_set: [1.0]",
                    "  - asset_id: marvel_rivals.alt-hero.hero_portrait",
                    "    asset_family: hero_portrait",
                    '    display_name: "Alt Hero"',
                    "    roi_ref: hero_portrait",
                    '    template_path: "templates/heroes/alt-hero.png"',
                    '    match_method: "TM_CCOEFF_NORMED"',
                    "    threshold: 0.9",
                    "    temporal_window: 3",
                    "    scale_set: [1.0]",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    def _matcher_report(self, confirmed_detections: list[dict[str, object]]) -> dict[str, object]:
        return {
            "ok": True,
            "status": "ok",
            "game": "marvel_rivals",
            "source": "/tmp/example.mp4",
            "frame_count": 12,
            "sample_fps": 4.0,
            "template_count": 4,
            "detections": [],
            "confirmed_detections": confirmed_detections,
        }

    def test_map_matcher_result_emits_identity_medal_and_ability_events(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            confirmed = [
                {
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.0,
                    "last_timestamp": 1.5,
                    "peak_score": 0.98,
                    "supporting_frames": 4,
                    "temporal_window": 3,
                },
                {
                    "asset_id": "marvel_rivals.triple-kill.medal_icon",
                    "roi_ref": "medal_area",
                    "first_timestamp": 2.0,
                    "last_timestamp": 2.25,
                    "peak_score": 0.99,
                    "supporting_frames": 2,
                    "temporal_window": 2,
                },
                {
                    "asset_id": "marvel_rivals.frag-grenade.ability_icon",
                    "roi_ref": "ability_hud",
                    "first_timestamp": 3.0,
                    "last_timestamp": 3.5,
                    "peak_score": 0.96,
                    "supporting_frames": 3,
                    "temporal_window": 3,
                },
            ]
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = map_matcher_result("marvel_rivals", self._matcher_report(confirmed))
        self.assertTrue(result["ok"])
        self.assertEqual(result["event_count"], 3)
        event_types = {row["event_type"] for row in result["events"]}
        self.assertEqual(event_types, {"pov_character_identified", "medal_seen", "ability_seen"})
        identity_event = next(row for row in result["events"] if row["event_type"] == "pov_character_identified")
        medal_event = next(row for row in result["events"] if row["event_type"] == "medal_seen")
        ability_event = next(row for row in result["events"] if row["event_type"] == "ability_seen")
        self.assertEqual(identity_event["entity_id"], "punisher")
        self.assertEqual(medal_event["event_row_id"], "triple-kill")
        self.assertEqual(ability_event["ability_id"], "frag-grenade")

    def test_map_matcher_result_collapses_adjacent_confirmed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            confirmed = [
                {
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.0,
                    "last_timestamp": 1.5,
                    "peak_score": 0.96,
                    "supporting_frames": 3,
                    "temporal_window": 3,
                },
                {
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.75,
                    "last_timestamp": 2.25,
                    "peak_score": 0.98,
                    "supporting_frames": 4,
                    "temporal_window": 3,
                },
            ]
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = map_matcher_result("marvel_rivals", self._matcher_report(confirmed))
        self.assertEqual(result["event_count"], 1)
        event = result["events"][0]
        self.assertEqual(event["start_timestamp"], 1.0)
        self.assertEqual(event["end_timestamp"], 2.25)
        self.assertEqual(event["source_detection_count"], 7)

    def test_map_matcher_result_prefers_stronger_overlapping_identity_event(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            confirmed = [
                {
                    "asset_id": "marvel_rivals.punisher.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.0,
                    "last_timestamp": 2.0,
                    "peak_score": 0.94,
                    "supporting_frames": 3,
                    "temporal_window": 3,
                },
                {
                    "asset_id": "marvel_rivals.alt-hero.hero_portrait",
                    "roi_ref": "hero_portrait",
                    "first_timestamp": 1.5,
                    "last_timestamp": 2.25,
                    "peak_score": 0.99,
                    "supporting_frames": 4,
                    "temporal_window": 3,
                },
            ]
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = map_matcher_result("marvel_rivals", self._matcher_report(confirmed))
        self.assertEqual(result["event_count"], 1)
        self.assertEqual(result["events"][0]["asset_id"], "marvel_rivals.alt-hero.hero_portrait")

    def test_map_matcher_result_returns_no_events_for_empty_confirmed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = map_matcher_result("marvel_rivals", self._matcher_report([]))
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "no_events")
        self.assertEqual(result["event_count"], 0)

    def test_load_matcher_report_raises_for_missing_path(self) -> None:
        with self.assertRaises(EventMapperError) as exc:
            load_matcher_report("/tmp/does-not-exist.json")
        self.assertEqual(exc.exception.status, "missing_matcher_report")

    def test_map_roi_events_uses_existing_matcher_report_without_rerunning_matcher(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            report_path = root / "report.json"
            report_path.write_text(
                json.dumps(
                    self._matcher_report(
                        [
                            {
                                "asset_id": "marvel_rivals.triple-kill.medal_icon",
                                "roi_ref": "medal_area",
                                "first_timestamp": 2.0,
                                "last_timestamp": 2.25,
                                "peak_score": 0.99,
                                "supporting_frames": 2,
                                "temporal_window": 2,
                            }
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch("pipeline.event_mapper.match_roi_templates") as mock_match:
                result = map_roi_events("/tmp/example.mp4", "marvel_rivals", matcher_report=report_path)
            mock_match.assert_not_called()
        self.assertTrue(result["ok"])
        self.assertEqual(result["event_count"], 1)

    def test_map_roi_events_writes_debug_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            self._write_published_pack(root)
            debug_root = root / "debug"
            with patch("pipeline.game_pack.ASSETS_ROOT", root / "assets" / "games"), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", root / "starter_assets"
            ), patch(
                "pipeline.event_mapper.match_roi_templates",
                return_value=self._matcher_report(
                    [
                        {
                            "asset_id": "marvel_rivals.punisher.hero_portrait",
                            "roi_ref": "hero_portrait",
                            "first_timestamp": 1.0,
                            "last_timestamp": 1.5,
                            "peak_score": 0.98,
                            "supporting_frames": 4,
                            "temporal_window": 3,
                        }
                    ]
                ),
            ):
                result = map_roi_events("/tmp/example.mp4", "marvel_rivals", debug_output_dir=debug_root)
                self.assertTrue(result["ok"])
                self.assertTrue((debug_root / "event_report.json").exists())
                self.assertTrue((debug_root / "event_summary.json").exists())
                self.assertTrue((debug_root / "events.csv").exists())

    def test_run_map_roi_events_returns_structured_error(self) -> None:
        with patch("run.map_roi_events", side_effect=EventMapperError("missing_matcher_report", "missing report")):
            result = run_map_roi_events("/tmp/example.mp4", "marvel_rivals")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "missing_matcher_report")

    def test_cli_routes_to_map_roi_events(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--map-roi-events", "/tmp/example.mp4", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_map_roi_events",
                return_value={"ok": True, "status": "ok", "event_count": 0, "events": []},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv
