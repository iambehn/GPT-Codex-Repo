from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.roi_matcher import (
    FrameBundle,
    RoiMatcherError,
    TemplateSpec,
    _best_match_for_template,
    _confirm_detections,
    check_roi_runtime,
    list_pack_templates,
    load_template_trial_overrides,
    match_roi_templates,
    validate_published_pack,
)
from run import (
    main as run_main,
    run_check_roi_runtime,
    run_list_pack_templates,
    run_match_roi_templates,
    run_validate_published_pack,
)


class _FakeImage:
    def __init__(self, shape: tuple[int, ...], score: float = 0.0) -> None:
        self.shape = shape
        self.score = score

    def __getitem__(self, key):  # noqa: ANN001
        return self


class _FakeCv2:
    IMREAD_UNCHANGED = 1
    IMREAD_GRAYSCALE = 0
    INTER_LINEAR = 1
    INTER_NEAREST = 2
    TM_CCOEFF_NORMED = 3
    COLOR_RGB2GRAY = 4
    COLOR_BGRA2BGR = 5
    COLOR_BGR2RGB = 6

    def __init__(self) -> None:
        self.mask_calls = 0

    def imread(self, path: str, mode: int) -> _FakeImage | None:
        if path.endswith(".mask.png"):
            return _FakeImage((10, 10))
        return _FakeImage((10, 10, 4) if mode == self.IMREAD_UNCHANGED else (10, 10), score=0.0)

    def resize(self, image: _FakeImage, _none, fx: float, fy: float, interpolation: int) -> _FakeImage:
        height = max(1, int(image.shape[0] * fy))
        width = max(1, int(image.shape[1] * fx))
        channels = image.shape[2:] if len(image.shape) > 2 else ()
        return _FakeImage((height, width, *channels), score=fx)

    def cvtColor(self, image: _FakeImage, code: int) -> _FakeImage:
        if code == self.COLOR_RGB2GRAY:
            return _FakeImage((image.shape[0], image.shape[1]), score=image.score)
        return _FakeImage((image.shape[0], image.shape[1], 3), score=image.score)

    def matchTemplate(self, roi_image: _FakeImage, template_image: _FakeImage, method: int, mask=None) -> _FakeImage:  # noqa: ANN001
        if mask is not None:
            self.mask_calls += 1
        return _FakeImage((1, 1), score=template_image.score or 1.0)

    def minMaxLoc(self, result: _FakeImage) -> tuple[float, float, tuple[int, int], tuple[int, int]]:
        return (0.0, result.score, (0, 0), (0, 0))


class RoiMatcherTests(unittest.TestCase):
    def _write_published_pack(self, root: Path, *, template_rows: list[dict[str, object]]) -> Path:
        game_root = root / "assets" / "games" / "marvel_rivals"
        (game_root / "manifests").mkdir(parents=True, exist_ok=True)
        (game_root / "templates" / "heroes").mkdir(parents=True, exist_ok=True)
        (game_root / "templates" / "medals").mkdir(parents=True, exist_ok=True)
        (game_root / "templates" / "abilities").mkdir(parents=True, exist_ok=True)
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
        (game_root / "weights.yaml").write_text("weights: {}\nthresholds: {}\ngates: {}\n", encoding="utf-8")
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
        template_lines = ["templates:"]
        for row in template_rows:
            template_lines.extend(
                [
                    f"  - asset_id: {row['asset_id']}",
                    f"    asset_family: {row.get('asset_family', 'hero_portrait')}",
                    *([f"    entity_id: {row['entity_id']}"] if row.get("entity_id") is not None else []),
                    *([f"    ability_id: {row['ability_id']}"] if row.get("ability_id") is not None else []),
                    *([f"    equipment_id: {row['equipment_id']}"] if row.get("equipment_id") is not None else []),
                    *([f"    event_row_id: {row['event_row_id']}"] if row.get("event_row_id") is not None else []),
                    f"    roi_ref: {row['roi_ref']}",
                    f'    template_path: "{row["template_path"]}"',
                    f'    mask_path: "{row.get("mask_path", "")}"',
                    f'    match_method: "{row.get("match_method", "TM_CCOEFF_NORMED")}"',
                    f"    threshold: {row.get('threshold', 0.9)}",
                    f"    temporal_window: {row.get('temporal_window', 2)}",
                    f'    scale_set: {json.dumps(row.get("scale_set", [1.0]))}',
                ]
            )
        (game_root / "manifests" / "cv_templates.yaml").write_text("\n".join(template_lines) + "\n", encoding="utf-8")
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
                    "    identity_competition: strongest_overlap",
                    "  medal_icon:",
                    "    signal_type: medal_visibility",
                    "    event_type: medal_seen",
                    "    target_field: event_row_id",
                    "    target_id_source: template_field",
                    "    target_value_field: event_row_id",
                    "  ability_icon:",
                    "    signal_type: ability_visibility",
                    "    event_type: ability_seen",
                    "    target_field: ability_id",
                    "    target_id_source: template_field",
                    "    target_value_field: ability_id",
                    "  equipment_icon:",
                    "    signal_type: equipment_visibility",
                    "    event_type: ability_seen",
                    "    target_field: equipment_id",
                    "    target_id_source: template_field",
                    "    target_value_field: equipment_id",
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
                    "  - rule_id: medal_visibility_atomic",
                    "    event_type: medal_seen",
                    '    signal_types: ["medal_visibility"]',
                    '    required_signal_types: ["medal_visibility"]',
                    "    window_seconds: 0.5",
                    "    min_signal_count: 1",
                    "    confidence_method: max",
                    '    group_by: ["event_row_id"]',
                    "  - rule_id: ability_visibility_atomic",
                    "    event_type: ability_seen",
                    '    signal_types: ["ability_visibility"]',
                    '    required_signal_types: ["ability_visibility"]',
                    "    window_seconds: 0.5",
                    "    min_signal_count: 1",
                    "    confidence_method: max",
                    '    group_by: ["ability_id"]',
                    "  - rule_id: equipment_visibility_atomic",
                    "    event_type: ability_seen",
                    '    signal_types: ["equipment_visibility"]',
                    '    required_signal_types: ["equipment_visibility"]',
                    "    window_seconds: 0.5",
                    "    min_signal_count: 1",
                    "    confidence_method: max",
                    '    group_by: ["equipment_id"]',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        detection_lines = [
            "schema_version: game_detection_manifest_v1",
            "baseline_schema_version: runtime_detection_schema_v1",
            "game_id: marvel_rivals",
            f"row_count: {len(template_rows)}",
            f"required_row_count: {len(template_rows)}",
            f"ready_row_count: {len(template_rows)}",
            "rows_needing_assets: 0",
            "rows:",
        ]
        for row in template_rows:
            asset_family = str(row.get("asset_family", "hero_portrait"))
            target_id = (
                str(row.get("entity_id") or row.get("ability_id") or row.get("equipment_id") or row.get("event_row_id") or "unknown")
            )
            semantic_field = (
                "entity_id"
                if row.get("entity_id") is not None
                else "ability_id"
                if row.get("ability_id") is not None
                else "equipment_id"
                if row.get("equipment_id") is not None
                else "event_row_id"
            )
            detection_lines.extend(
                [
                    f"  - detection_id: marvel_rivals.{target_id}.{asset_family}",
                    "    game_id: marvel_rivals",
                    f"    target_kind: {'hero' if asset_family == 'hero_portrait' else 'event' if asset_family == 'medal_icon' else 'equipment' if asset_family == 'equipment_icon' else 'ability'}",
                    f"    target_id: {target_id}",
                    f"    target_display_name: {target_id}",
                    "    ontology_collection: derived",
                    f"    asset_family: {asset_family}",
                    "    requires_asset: true",
                    f'    required_semantic_fields: ["{semantic_field}"]',
                    "    template_semantics:",
                    f"      {semantic_field}: {target_id}",
                    "    status: ready_for_binding",
                    "    binding_status: accepted",
                    "    asset_status: published",
                    f"    published_asset_id: {row['asset_id']}",
                ]
            )
        (game_root / "manifests" / "detection_manifest.yaml").write_text("\n".join(detection_lines) + "\n", encoding="utf-8")
        (game_root / "manifests" / "assets_manifest.json").write_text(
            json.dumps({"game_id": "marvel_rivals", "published_assets": []}, indent=2),
            encoding="utf-8",
        )
        for row in template_rows:
            template_path = game_root / str(row["template_path"])
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.write_bytes(b"template")
            mask_path = str(row.get("mask_path", ""))
            if mask_path:
                full_mask_path = game_root / mask_path
                full_mask_path.parent.mkdir(parents=True, exist_ok=True)
                full_mask_path.write_bytes(b"mask")
        return game_root

    def test_matcher_returns_no_templates_for_empty_published_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(root, template_rows=[])
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root):
                result = match_roi_templates("/tmp/example.mp4", "marvel_rivals")
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "no_templates")

    def test_matcher_errors_for_unknown_roi_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "roi_ref": "unknown_roi",
                        "template_path": "templates/heroes/punisher.png",
                    }
                ],
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root):
                with self.assertRaises(RoiMatcherError) as exc:
                    match_roi_templates("/tmp/example.mp4", "marvel_rivals")
            self.assertEqual(exc.exception.status, "invalid_roi_ref")

    def test_matcher_errors_for_missing_template_file(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            game_root = self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                    }
                ],
            )
            (game_root / "templates" / "heroes" / "punisher.png").unlink()
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root):
                with self.assertRaises(RoiMatcherError) as exc:
                    match_roi_templates("/tmp/example.mp4", "marvel_rivals")
            self.assertEqual(exc.exception.status, "missing_template_file")

    def test_validate_published_pack_reports_roi_fit_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.punisher.hero_portrait",
                        "entity_id": "punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "scale_set": [1.0, 4.0],
                    }
                ],
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._template_dimensions",
                return_value=(20, 20),
            ):
                result = validate_published_pack("marvel_rivals")
            self.assertTrue(result["ok"])
            self.assertTrue(result["templates_with_roi_fit_warnings"])

    def test_validate_published_pack_reports_canonical_contract_status(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.punisher.hero_portrait",
                        "entity_id": "punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "asset_family": "hero_portrait",
                    }
                ],
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._template_dimensions",
                return_value=(10, 10),
            ):
                result = validate_published_pack("marvel_rivals")
            self.assertTrue(result["ok"])
            self.assertEqual(result["contract_status"], "canonical")
            self.assertFalse(result["active_legacy_modes"])

    def test_list_pack_templates_groups_rows_by_roi(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "entity_id": "punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "asset_family": "hero_portrait",
                    },
                    {
                        "asset_id": "marvel_rivals.medal.headshot",
                        "event_row_id": "headshot",
                        "roi_ref": "medal_area",
                        "template_path": "templates/medals/headshot.png",
                        "asset_family": "medal_icon",
                    },
                ],
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root):
                result = list_pack_templates("marvel_rivals")
            self.assertTrue(result["ok"])
            self.assertIn("hero_portrait", result["templates_by_roi"])
            self.assertIn("medal_area", result["templates_by_roi"])
            self.assertEqual(result["templates_by_roi"]["hero_portrait"][0]["event_type"], "pov_character_identified")
            self.assertEqual(result["templates_by_roi"]["hero_portrait"][0]["signal_type"], "character_identity")
            self.assertEqual(result["templates_by_roi"]["hero_portrait"][0]["entity_id"], "punisher")

    def test_validate_published_pack_reports_missing_runtime_rule_for_family(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            game_root = self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.status.objective",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/objective.png",
                        "asset_family": "hud_icon",
                    }
                ],
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
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._template_dimensions",
                return_value=(10, 10),
            ):
                result = validate_published_pack("marvel_rivals")
            self.assertFalse(result["ok"])
            self.assertIn("hud_icon", result["missing_runtime_rules_for_families"])

    def test_validate_published_pack_reports_semantic_target_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.frag-grenade.ability_icon",
                        "roi_ref": "ability_hud",
                        "template_path": "templates/abilities/frag-grenade.png",
                        "asset_family": "ability_icon",
                    }
                ],
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._template_dimensions",
                return_value=(10, 10),
            ):
                result = validate_published_pack("marvel_rivals")
            self.assertFalse(result["ok"])
            self.assertEqual(result["failures"][0]["status"], "template_rule_target_mismatch")

    def test_validate_published_pack_reports_ontology_status_for_canonical_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.punisher.hero_portrait",
                        "entity_id": "punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "asset_family": "hero_portrait",
                    }
                ],
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._template_dimensions",
                return_value=(10, 10),
            ):
                result = validate_published_pack("marvel_rivals")
            self.assertEqual(result["ontology_status"], "ok")
            self.assertTrue(result["ontology_version"])

    def test_runtime_rules_fail_on_unknown_ontology_signal_type(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            game_root = self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.punisher.hero_portrait",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "asset_family": "hero_portrait",
                    }
                ],
            )
            (game_root / "manifests" / "runtime_cv_rules.yaml").write_text(
                "\n".join(
                    [
                        "event_mappings:",
                        "  hero_portrait:",
                        "    signal_type: unknown_signal",
                        "    event_type: pov_character_identified",
                        "    target_field: entity_id",
                        "    target_id_source: template_field",
                        "    target_value_field: entity_id",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root):
                with self.assertRaises(RoiMatcherError) as exc:
                    match_roi_templates("/tmp/example.mp4", "marvel_rivals")
            self.assertEqual(exc.exception.status, "invalid_runtime_cv_rules")

    def test_validate_published_pack_flags_legacy_target_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            game_root = self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.punisher.hero_portrait",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "asset_family": "hero_portrait",
                    }
                ],
            )
            (game_root / "manifests" / "runtime_cv_rules.yaml").write_text(
                "\n".join(
                    [
                        "event_mappings:",
                        "  hero_portrait:",
                        "    signal_type: character_identity",
                        "    event_type: pov_character_identified",
                        "    target_field: entity_id",
                        "    target_id_source: asset_id_suffix",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._template_dimensions",
                return_value=(10, 10),
            ):
                result = validate_published_pack("marvel_rivals")
            self.assertTrue(result["ok"])
            self.assertTrue(result["legacy_target_id_rules"])
            self.assertEqual(result["contract_status"], "legacy_assisted")
            self.assertEqual(result["legacy_findings"][0]["status"], "legacy_target_id_source")

    def test_matcher_reports_missing_runtime_rules_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            game_root = self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                    }
                ],
            )
            (game_root / "manifests" / "runtime_cv_rules.yaml").unlink()
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root):
                with self.assertRaises(RoiMatcherError) as exc:
                    match_roi_templates("/tmp/example.mp4", "marvel_rivals")
            self.assertEqual(exc.exception.status, "missing_runtime_cv_rules")

    def test_best_match_chooses_highest_scale_and_uses_mask(self) -> None:
        fake_cv2 = _FakeCv2()
        template = TemplateSpec(
            asset_id="marvel_rivals.hero.punisher",
            roi_ref="hero_portrait",
            template_path=Path("/tmp/punisher.png"),
            mask_path=Path("/tmp/punisher.mask.png"),
            threshold=0.9,
            scale_set=[0.8, 1.2, 1.0],
            temporal_window=2,
            match_method="TM_CCOEFF_NORMED",
            asset_family="hero_portrait",
        )
        result = _best_match_for_template(
            roi_image=_FakeImage((36, 64, 3)),
            template=template,
            cv2_module=fake_cv2,
            np_module=None,
        )
        self.assertIsNotNone(result)
        self.assertEqual(round(float(result["score"]), 2), 1.2)
        self.assertGreater(fake_cv2.mask_calls, 0)

    def test_confirm_detections_requires_temporal_window(self) -> None:
        detections = [
            {"asset_id": "asset.one", "roi_ref": "hero_portrait", "timestamp": 0.0, "score": 0.95, "frame_index": 0},
            {"asset_id": "asset.one", "roi_ref": "hero_portrait", "timestamp": 0.25, "score": 0.96, "frame_index": 1},
            {"asset_id": "asset.two", "roi_ref": "medal_area", "timestamp": 0.5, "score": 0.99, "frame_index": 2},
        ]
        templates = [
            TemplateSpec("asset.one", "hero_portrait", Path("/tmp/a.png"), None, 0.9, [1.0], 2, "TM_CCOEFF_NORMED", "hero_portrait"),
            TemplateSpec("asset.two", "medal_area", Path("/tmp/b.png"), None, 0.9, [1.0], 2, "TM_CCOEFF_NORMED", "medal_icon"),
        ]
        confirmed = _confirm_detections(detections, templates)
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(confirmed[0]["asset_id"], "asset.one")

    def test_matcher_emits_raw_and_confirmed_detections(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "temporal_window": 2,
                    },
                    {
                        "asset_id": "marvel_rivals.medal.headshot",
                        "roi_ref": "medal_area",
                        "template_path": "templates/medals/headshot.png",
                        "temporal_window": 2,
                    },
                ],
            )
            frames = [
                FrameBundle(0, 0.0, _FakeImage((36, 64, 3))),
                FrameBundle(1, 0.25, _FakeImage((36, 64, 3))),
                FrameBundle(2, 0.50, _FakeImage((36, 64, 3))),
            ]

            def fake_matcher(*, roi_image, template, cv2_module, np_module):  # noqa: ANN001
                if template.asset_id == "marvel_rivals.hero.punisher":
                    return {"score": 0.97, "scale": 1.0}
                if template.asset_id == "marvel_rivals.medal.headshot" and roi_image is not None:
                    return {"score": 0.40, "scale": 1.0}
                return None

            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._load_cv_runtime",
                return_value=(object(), object()),
            ), patch("pipeline.roi_matcher._decode_video_frames", return_value=frames), patch(
                "pipeline.roi_matcher._best_match_for_template",
                side_effect=fake_matcher,
            ):
                result = match_roi_templates("/tmp/example.mp4", "marvel_rivals", sample_fps=4.0)

            self.assertTrue(result["ok"])
            self.assertEqual(result["template_count"], 2)
            self.assertGreaterEqual(len(result["detections"]), 3)
            self.assertEqual(len(result["confirmed_detections"]), 1)
            self.assertEqual(result["confirmed_detections"][0]["asset_id"], "marvel_rivals.hero.punisher")
            self.assertEqual(result["confirmed_detections"][0]["asset_family"], "hero_portrait")

    def test_load_template_trial_overrides_rejects_invalid_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            trial_path = root / "trial.yaml"
            trial_path.write_text("templates:\n  marvel_rivals.hero.punisher:\n    unsupported: 1\n", encoding="utf-8")
            with self.assertRaises(RoiMatcherError) as exc:
                load_template_trial_overrides(trial_path)
        self.assertEqual(exc.exception.status, "invalid_template_trial")

    def test_matcher_applies_template_overrides_without_mutating_pack(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            game_root = self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "entity_id": "punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "asset_family": "hero_portrait",
                        "threshold": 0.9,
                        "temporal_window": 2,
                    }
                ],
            )
            original_manifest = (game_root / "manifests" / "cv_templates.yaml").read_text(encoding="utf-8")
            frames = [
                FrameBundle(frame_index=0, timestamp=0.0, image=_FakeImage((36, 64, 3))),
                FrameBundle(frame_index=1, timestamp=0.25, image=_FakeImage((36, 64, 3))),
            ]
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch(
                "pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root
            ), patch(
                "pipeline.roi_matcher._load_cv_runtime",
                return_value=(_FakeCv2(), object()),
            ), patch(
                "pipeline.roi_matcher._decode_video_frames",
                return_value=frames,
            ), patch(
                "pipeline.roi_matcher._best_match_for_template",
                return_value={"score": 0.85, "scale": 1.0},
            ):
                current = match_roi_templates("/tmp/example.mp4", "marvel_rivals", sample_fps=4.0)
                trial = match_roi_templates(
                    "/tmp/example.mp4",
                    "marvel_rivals",
                    sample_fps=4.0,
                    template_overrides={
                        "marvel_rivals.hero.punisher": {
                            "threshold": 0.8,
                            "scale_set": [1.25],
                            "temporal_window": 1,
                        }
                    },
                )
            self.assertEqual(current["confirmed_detections"], [])
            self.assertEqual(len(trial["confirmed_detections"]), 1)
            self.assertEqual(trial["confirmed_detections"][0]["entity_id"], "punisher")
            self.assertEqual(trial["confirmed_detections"][0]["temporal_window"], 1)
            self.assertEqual((game_root / "manifests" / "cv_templates.yaml").read_text(encoding="utf-8"), original_manifest)

    def test_matcher_writes_output_report(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                    }
                ],
            )
            output_path = root / "report.json"
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._load_cv_runtime",
                return_value=(object(), object()),
            ), patch(
                "pipeline.roi_matcher._decode_video_frames",
                return_value=[],
            ), patch(
                "pipeline.roi_matcher._best_match_for_template",
                return_value=None,
            ):
                result = match_roi_templates("/tmp/example.mp4", "marvel_rivals", output_path=output_path)
            self.assertTrue(result["ok"])
            self.assertTrue(output_path.exists())

    def test_matcher_debug_output_dir_writes_reports_and_csvs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            assets_root = root / "assets" / "games"
            starter_root = root / "starter_assets"
            self._write_published_pack(
                root,
                template_rows=[
                    {
                        "asset_id": "marvel_rivals.hero.punisher",
                        "roi_ref": "hero_portrait",
                        "template_path": "templates/heroes/punisher.png",
                        "temporal_window": 2,
                    }
                ],
            )
            frames = [
                FrameBundle(0, 0.0, _FakeImage((36, 64, 3))),
                FrameBundle(1, 0.25, _FakeImage((36, 64, 3))),
            ]
            debug_dir = root / "debug"
            with patch("pipeline.game_pack.ASSETS_ROOT", assets_root), patch("pipeline.game_pack.STARTER_ASSETS_ROOT", starter_root), patch(
                "pipeline.roi_matcher._load_cv_runtime",
                return_value=(_FakeCv2(), object()),
            ), patch(
                "pipeline.roi_matcher._decode_video_frames",
                return_value=frames,
            ), patch(
                "pipeline.roi_matcher._best_match_for_template",
                return_value={"score": 0.97, "scale": 1.0},
            ), patch(
                "pipeline.roi_matcher._write_confirmed_crops",
                return_value=None,
            ):
                result = match_roi_templates("/tmp/example.mp4", "marvel_rivals", debug_output_dir=debug_dir)
            self.assertTrue(result["ok"])
            self.assertTrue((debug_dir / "match_report.json").exists())
            self.assertTrue((debug_dir / "pack_summary.json").exists())
            self.assertTrue((debug_dir / "detections.csv").exists())
            self.assertTrue((debug_dir / "confirmed_detections.csv").exists())
            self.assertIn("summary", result)
            self.assertIn("top_scores", result)
            self.assertIn("unseen_templates", result)

    def test_check_roi_runtime_reports_missing_modules(self) -> None:
        real_import = __import__

        def fake_import(name, *args, **kwargs):  # noqa: ANN001
            if name in {"cv2", "numpy"}:
                raise ModuleNotFoundError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import), patch("pipeline.roi_matcher.shutil.which", return_value=""):
            result = check_roi_runtime()
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "runtime_missing")
        self.assertFalse(result["checks"]["cv2"]["ok"])
        self.assertFalse(result["checks"]["numpy"]["ok"])
        self.assertIn("ffmpeg", result["checks"])

    def test_run_match_roi_templates_returns_structured_error(self) -> None:
        with patch("run.match_roi_templates", side_effect=RoiMatcherError("opencv_unavailable", "missing cv2")):
            result = run_match_roi_templates("/tmp/example.mp4", "marvel_rivals")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "opencv_unavailable")

    def test_run_check_roi_runtime_returns_structured_payload(self) -> None:
        with patch("run.check_roi_runtime", return_value={"ok": False, "status": "runtime_missing"}):
            result = run_check_roi_runtime()
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "runtime_missing")

    def test_run_validate_published_pack_returns_structured_error(self) -> None:
        with patch("run.validate_published_pack", side_effect=RoiMatcherError("published_pack_required", "missing pack")):
            result = run_validate_published_pack("marvel_rivals")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "published_pack_required")

    def test_run_list_pack_templates_returns_structured_error(self) -> None:
        with patch("run.list_pack_templates", side_effect=RoiMatcherError("published_pack_required", "missing pack")):
            result = run_list_pack_templates("marvel_rivals")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "published_pack_required")

    def test_cli_routes_to_roi_matcher(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--match-roi-templates", "/tmp/example.mp4", "marvel_rivals"]
            with patch(
                "run.run_match_roi_templates",
                return_value={"ok": True, "status": "ok", "detections": [], "confirmed_detections": []},
            ):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv

    def test_cli_routes_to_runtime_check(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--check-roi-runtime"]
            with patch("run.run_check_roi_runtime", return_value={"ok": True, "status": "ok"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"status": "ok"', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv

    def test_cli_routes_to_pack_validation(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--validate-published-pack", "marvel_rivals"]
            with patch("run.run_validate_published_pack", return_value={"ok": True, "status": "ok"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"status": "ok"', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv

    def test_cli_routes_to_template_listing(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--list-pack-templates", "marvel_rivals"]
            with patch("run.run_list_pack_templates", return_value={"ok": True, "status": "ok"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"status": "ok"', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv
