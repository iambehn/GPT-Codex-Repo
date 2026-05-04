from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline import game_pack
from tools.preview_roi import preview_roi
from tools.snip_roi import snip_roi


def _ffmpeg_path() -> str:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    if not Path(ffmpeg).exists():
        raise RuntimeError("ffmpeg not found for ROI tool tests")
    return ffmpeg


def _write_pgm_frame(path: Path, width: int, height: int, pixels: bytes) -> None:
    header = f"P5\n{width} {height}\n255\n".encode("ascii")
    path.write_bytes(header + pixels)


def _write_test_video(path: Path, frame_pixels: list[bytes], width: int = 64, height: int = 36, fps: int = 4) -> None:
    with tempfile.TemporaryDirectory() as frames_dir:
        frame_dir = Path(frames_dir)
        for index, pixels in enumerate(frame_pixels):
            _write_pgm_frame(frame_dir / f"frame{index:03d}.pgm", width, height, pixels)

        command = [
            _ffmpeg_path(),
            "-v",
            "error",
            "-y",
            "-framerate",
            str(fps),
            "-i",
            str(frame_dir / "frame%03d.pgm"),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ]
        subprocess.run(command, check=True, capture_output=True)


def _solid_frame(value: int, width: int = 64, height: int = 36) -> bytes:
    return bytes([value]) * (width * height)


def _write_published_pack(root: Path) -> None:
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
    (game_root / "entities.yaml").write_text("heroes: []\nabilities: []\nevents: []\n", encoding="utf-8")
    (game_root / "hud.yaml").write_text(
        "\n".join(
            [
                "rois:",
                "  hero_portrait:",
                "    x_pct: 0.0",
                "    y_pct: 0.0",
                "    w_pct: 0.5",
                "    h_pct: 0.5",
                "  event_badge:",
                "    x_pct: 0.5",
                "    y_pct: 0.0",
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
                "    entity_id: punisher",
                "    roi_ref: hero_portrait",
                '    template_path: "templates/heroes/punisher.png"',
                '    match_method: "TM_CCOEFF_NORMED"',
                "    threshold: 0.9",
                "    temporal_window: 3",
                "    scale_set: [1.0]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (game_root / "manifests" / "runtime_cv_rules.yaml").write_text(
        "\n".join(
            [
                "event_mappings:",
                "  hero_portrait:",
                "    signal_type: character_identity",
                "    event_type: pov_character_identified",
                "    target_field: entity_id",
                "    target_id_source: template_field",
                "    target_value_field: entity_id",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (game_root / "manifests" / "fusion_rules.yaml").write_text(
        "\n".join(
            [
                "schema_version: fusion_rules_v1",
                "rules:",
                "  - rule_id: character_identity_atomic",
                "    event_type: pov_character_identified",
                '    signal_types: ["character_identity"]',
                '    required_signal_types: ["character_identity"]',
                "    window_seconds: 0.5",
                "    min_signal_count: 1",
                "    confidence_method: max",
                '    group_by: ["entity_id"]',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (game_root / "manifests" / "detection_manifest.yaml").write_text(
        "\n".join(
            [
                "schema_version: game_detection_manifest_v1",
                "baseline_schema_version: runtime_detection_schema_v1",
                "game_id: marvel_rivals",
                "row_count: 1",
                "required_row_count: 1",
                "ready_row_count: 1",
                "rows_needing_assets: 0",
                "rows: []",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    template_path = game_root / "templates" / "heroes" / "punisher.png"
    template_path.parent.mkdir(parents=True, exist_ok=True)
    template_path.write_bytes(b"template")


class RoiToolsTests(unittest.TestCase):
    def test_preview_roi_renders_annotated_image(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            _write_published_pack(root)
            source = root / "fixture.mp4"
            _write_test_video(source, [_solid_frame(100) for _ in range(8)])

            with patch.object(game_pack, "ASSETS_ROOT", root / "assets" / "games"), patch.object(
                game_pack, "STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = preview_roi(source, "marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["roi_count"], 2)
            self.assertTrue(Path(result["output_path"]).is_file())

    def test_preview_roi_fails_cleanly_for_unknown_game(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            source = Path(tempdir) / "fixture.mp4"
            _write_test_video(source, [_solid_frame(100) for _ in range(8)])

            result = preview_roi(source, "unknown_game")

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_game_pack")

    def test_snip_roi_writes_crop_and_provenance_without_mutating_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            _write_published_pack(root)
            source = root / "fixture.mp4"
            _write_test_video(source, [_solid_frame(120) for _ in range(8)])
            hud_path = root / "assets" / "games" / "marvel_rivals" / "hud.yaml"
            before_hud = hud_path.read_text(encoding="utf-8")

            with patch.object(game_pack, "ASSETS_ROOT", root / "assets" / "games"), patch.object(
                game_pack, "STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                result = snip_roi(
                    source,
                    "marvel_rivals",
                    "hero_portrait",
                    "marvel_rivals.test.hero_crop",
                    "0,0,16,12",
                    write_debug_frame=True,
                )

            self.assertTrue(result["ok"])
            self.assertTrue(Path(result["crop_png_path"]).is_file())
            self.assertTrue(Path(result["source_frame_png_path"]).is_file())
            self.assertTrue(Path(result["provenance_path"]).is_file())
            self.assertTrue(Path(result["manifest_snippet_path"]).is_file())
            self.assertTrue(Path(result["debug_frame_png_path"]).is_file())
            provenance = json.loads(Path(result["provenance_path"]).read_text(encoding="utf-8"))
            self.assertEqual(provenance["roi_ref"], "hero_portrait")
            self.assertEqual(provenance["crop"], {"x": 0, "y": 0, "w": 16, "h": 12})
            self.assertEqual(hud_path.read_text(encoding="utf-8"), before_hud)

    def test_snip_roi_rejects_invalid_crop_and_unknown_roi(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            _write_published_pack(root)
            source = root / "fixture.mp4"
            _write_test_video(source, [_solid_frame(120) for _ in range(8)])

            with patch.object(game_pack, "ASSETS_ROOT", root / "assets" / "games"), patch.object(
                game_pack, "STARTER_ASSETS_ROOT", root / "starter_assets"
            ):
                bad_crop = snip_roi(source, "marvel_rivals", "hero_portrait", "asset", "0,0,999,999")
                bad_roi = snip_roi(source, "marvel_rivals", "missing_roi", "asset", "0,0,10,10")

            self.assertFalse(bad_crop["ok"])
            self.assertEqual(bad_crop["status"], "invalid_crop")
            self.assertFalse(bad_roi["ok"])
            self.assertEqual(bad_roi["status"], "unknown_roi_ref")


if __name__ == "__main__":
    unittest.main()
