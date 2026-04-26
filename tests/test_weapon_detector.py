from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

_IMPORT_CWD = tempfile.TemporaryDirectory()
_ORIGINAL_CWD = os.getcwd()
os.chdir(_IMPORT_CWD.name)
try:
    import pipeline.weapon_detector as weapon_detector
finally:
    os.chdir(_ORIGINAL_CWD)


@unittest.skipUnless(getattr(weapon_detector, "_CV2_AVAILABLE", False), "opencv required")
class WeaponDetectorUnitTests(unittest.TestCase):
    def test_trim_transparent_padding_removes_alpha_border(self) -> None:
        color = np.zeros((10, 10, 3), dtype=np.uint8)
        color[2:8, 3:7] = (255, 255, 255)
        alpha = np.zeros((10, 10), dtype=np.uint8)
        alpha[2:8, 3:7] = 255

        trimmed = weapon_detector._trim_transparent_padding(color, alpha)

        self.assertEqual(trimmed.shape[:2], (6, 4))

    def test_hybrid_match_finds_template_with_scale_adjustment(self) -> None:
        search = np.zeros((64, 64, 3), dtype=np.uint8)
        search[20:44, 24:40] = (240, 240, 240)
        search_variants = weapon_detector._search_variants(search)

        template_image = np.zeros((20, 14, 4), dtype=np.uint8)
        template_image[:, :, :3] = (240, 240, 240)
        template_image[:, :, 3] = 255
        template = {
            "weapon_id": "hero_one",
            "display_name": "Hero One",
            "variants": weapon_detector._template_variants(template_image),
        }

        match = weapon_detector._best_template_match(
            search_variants,
            template,
            "hybrid",
            [0.8, 1.0, 1.2],
        )

        self.assertIsNotNone(match)
        self.assertGreater(match["confidence"], 0.8)
        self.assertIn(match["variant"], {"color", "grayscale", "edges"})
        self.assertIn(round(match["scale"], 1), {0.8, 1.0, 1.2})

    def test_match_variants_for_mode(self) -> None:
        self.assertEqual(weapon_detector._match_variants_for_mode("color"), ["color"])
        self.assertEqual(weapon_detector._match_variants_for_mode("grayscale"), ["grayscale"])
        self.assertEqual(weapon_detector._match_variants_for_mode("edges"), ["edges"])
        self.assertEqual(
            weapon_detector._match_variants_for_mode("hybrid"),
            ["color", "grayscale", "edges"],
        )


if __name__ == "__main__":
    unittest.main()
