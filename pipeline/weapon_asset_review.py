from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from pipeline.game_pack import get_weapon_detector_game_config, load_game_pack, resolve_asset_path
from pipeline.weapon_detector_audit import get_weapon_detector_report_dir

try:
    import cv2
    import numpy as np

    _CV2_AVAILABLE = True
except ImportError:
    _CV2_AVAILABLE = False


_TILE_SIZE = 180
_HEADER_HEIGHT = 64
_LABEL_HEIGHT = 28
_PANEL_GAP = 16
_PANEL_MARGIN = 20
_FONT = 1


def render_weapon_audit_review(
    game: str,
    config: dict,
    *,
    report_path: str | Path | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    if not _CV2_AVAILABLE:
        return {"ok": False, "error": "opencv not installed"}

    report = _load_report(game, config, report_path)
    if "error" in report:
        return report

    top_k = max(1, int(top_k))
    candidates = list(report.get("ranked_candidates") or [])[:top_k]
    if not candidates:
        return {
            "ok": False,
            "error": "report has no ranked candidates",
            "report_path": str(report["report_path"]),
        }

    game_pack = load_game_pack(game, config)
    wd_cfg = get_weapon_detector_game_config(game, config, game_pack)
    pack_root = Path(game_pack.get("pack_root", "."))
    icon_dir = resolve_asset_path(
        wd_cfg.get("icon_dir") or f"assets/weapon_icons/{game}",
        pack_root,
    )

    report_file = Path(report["report_path"])
    output_dir = report_file.with_suffix("")
    output_dir.mkdir(parents=True, exist_ok=True)

    rendered_items: list[dict[str, Any]] = []
    row_images = []
    for index, item in enumerate(candidates, start=1):
        rendered = _render_item(item, icon_dir, output_dir, index)
        rendered_items.append(rendered)
        if rendered.get("comparison_path"):
            image = cv2.imread(rendered["comparison_path"])
            if image is not None:
                row_images.append(image)

    sheet_path = None
    if row_images:
        sheet = _build_sheet(row_images)
        sheet_path = output_dir / "review_sheet.png"
        cv2.imwrite(str(sheet_path), sheet)

    manifest = {
        "game": game,
        "report_path": str(report_file),
        "rendered_at": report.get("generated_at"),
        "top_k": top_k,
        "output_dir": str(output_dir),
        "sheet_path": str(sheet_path) if sheet_path else None,
        "items": rendered_items,
    }
    manifest_path = output_dir / "review_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    html_path = output_dir / "review.html"
    html_path.write_text(_render_html(manifest, report))
    return {
        "ok": True,
        "game": game,
        "report_path": str(report_file),
        "output_dir": str(output_dir),
        "sheet_path": str(sheet_path) if sheet_path else None,
        "manifest_path": str(manifest_path),
        "html_path": str(html_path),
        "rendered_items": len(rendered_items),
    }


def _render_item(item: dict[str, Any], icon_dir: Path, output_dir: Path, index: int) -> dict[str, Any]:
    entity_id = str(item.get("candidate_weapon_id") or "unknown")
    display_name = str(item.get("candidate_display_name") or entity_id.replace("_", " ").title())
    confidence = float(item.get("candidate_confidence", 0.0) or 0.0)
    exported = item.get("exported_assets") or {}
    asset_path = icon_dir / f"{entity_id}.png"
    candidate_path = Path(exported["candidate_crop_path"]) if exported.get("candidate_crop_path") else None
    roi_path = Path(exported["roi_crop_path"]) if exported.get("roi_crop_path") else None

    tiles = [
        ("Current Asset", asset_path if asset_path.exists() else None),
        ("Candidate Crop", candidate_path if candidate_path and candidate_path.exists() else None),
        ("ROI Crop", roi_path if roi_path and roi_path.exists() else None),
    ]

    comparison = _render_comparison_image(
        title=f"{index:02d}. {display_name}",
        subtitle=f"candidate={entity_id}  confidence={confidence:.3f}  clip={item.get('clip_stem', 'unknown')}",
        tiles=tiles,
    )
    filename = f"{index:02d}_{_safe_slug(entity_id)}_compare.png"
    comparison_path = output_dir / filename
    cv2.imwrite(str(comparison_path), comparison)

    return {
        "rank": index,
        "entity_id": entity_id,
        "display_name": display_name,
        "candidate_confidence": round(confidence, 3),
        "clip_stem": item.get("clip_stem"),
        "comparison_path": str(comparison_path),
        "current_asset_path": str(asset_path) if asset_path.exists() else None,
        "candidate_crop_path": str(candidate_path) if candidate_path and candidate_path.exists() else None,
        "roi_crop_path": str(roi_path) if roi_path and roi_path.exists() else None,
    }


def _render_comparison_image(*, title: str, subtitle: str, tiles: list[tuple[str, Path | None]]):
    panel_count = len(tiles)
    width = (_PANEL_MARGIN * 2) + (panel_count * _TILE_SIZE) + ((panel_count - 1) * _PANEL_GAP)
    height = _HEADER_HEIGHT + _LABEL_HEIGHT + (_TILE_SIZE + _LABEL_HEIGHT) + _PANEL_MARGIN
    canvas = np.full((height, width, 3), 24, dtype=np.uint8)

    cv2.putText(canvas, title, (_PANEL_MARGIN, 26), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (240, 240, 240), 2, cv2.LINE_AA)
    cv2.putText(canvas, subtitle[:110], (_PANEL_MARGIN, 50), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (180, 180, 180), 1, cv2.LINE_AA)

    y0 = _HEADER_HEIGHT
    for index, (label, path) in enumerate(tiles):
        x0 = _PANEL_MARGIN + index * (_TILE_SIZE + _PANEL_GAP)
        panel = _load_tile(path)
        canvas[y0:y0 + _TILE_SIZE, x0:x0 + _TILE_SIZE] = panel
        cv2.rectangle(canvas, (x0, y0), (x0 + _TILE_SIZE, y0 + _TILE_SIZE), (70, 70, 70), 1)
        cv2.putText(
            canvas,
            label,
            (x0, y0 + _TILE_SIZE + 20),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.48,
            (220, 220, 220),
            1,
            cv2.LINE_AA,
        )
    return canvas


def _load_tile(path: Path | None):
    if path is None:
        return _placeholder_tile("missing")
    image = cv2.imread(str(path), cv2.IMREAD_COLOR)
    if image is None:
        return _placeholder_tile("unreadable")
    return _fit_image(image, _TILE_SIZE, _TILE_SIZE)


def _fit_image(image, width: int, height: int):
    canvas = np.full((height, width, 3), 36, dtype=np.uint8)
    src_h, src_w = image.shape[:2]
    if src_h == 0 or src_w == 0:
        return canvas
    scale = min(width / src_w, height / src_h)
    resized_w = max(1, int(round(src_w * scale)))
    resized_h = max(1, int(round(src_h * scale)))
    interpolation = cv2.INTER_AREA if scale < 1.0 else cv2.INTER_LINEAR
    resized = cv2.resize(image, (resized_w, resized_h), interpolation=interpolation)
    x0 = (width - resized_w) // 2
    y0 = (height - resized_h) // 2
    canvas[y0:y0 + resized_h, x0:x0 + resized_w] = resized
    return canvas


def _placeholder_tile(label: str):
    tile = np.full((_TILE_SIZE, _TILE_SIZE, 3), 48, dtype=np.uint8)
    cv2.rectangle(tile, (0, 0), (_TILE_SIZE - 1, _TILE_SIZE - 1), (90, 90, 90), 1)
    cv2.putText(tile, label, (20, _TILE_SIZE // 2), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (180, 180, 180), 1, cv2.LINE_AA)
    return tile


def _build_sheet(rows: list):
    per_row = 2
    row_count = math.ceil(len(rows) / per_row)
    row_height, row_width = rows[0].shape[:2]
    gap = 20
    width = (per_row * row_width) + ((per_row + 1) * gap)
    height = (row_count * row_height) + ((row_count + 1) * gap)
    canvas = np.full((height, width, 3), 18, dtype=np.uint8)
    for index, image in enumerate(rows):
        grid_x = index % per_row
        grid_y = index // per_row
        x0 = gap + grid_x * (row_width + gap)
        y0 = gap + grid_y * (row_height + gap)
        canvas[y0:y0 + row_height, x0:x0 + row_width] = image
    return canvas


def _load_report(game: str, config: dict, report_path: str | Path | None) -> dict[str, Any]:
    path = Path(report_path) if report_path else _latest_report_path(game, config)
    if path is None:
        return {"ok": False, "error": f"no weapon-detector audit reports found for {game}"}
    if not path.exists():
        return {"ok": False, "error": f"report not found: {path}"}
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {"ok": False, "error": f"report is not valid JSON: {path}"}
    payload["report_path"] = path
    return payload


def _latest_report_path(game: str, config: dict) -> Path | None:
    report_dir = get_weapon_detector_report_dir(game, config)
    reports = sorted(report_dir.glob("*.json"))
    return reports[-1] if reports else None


def _safe_slug(value: str) -> str:
    slug = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in str(value).lower())
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_") or "item"


def _render_html(manifest: dict[str, Any], report: dict[str, Any]) -> str:
    output_dir = Path(manifest["output_dir"])
    sheet_path = manifest.get("sheet_path")
    recommended = list(report.get("recommended_targets") or [])
    recommended_lookup = {
        str(item.get("weapon_id")): item
        for item in recommended
        if item.get("weapon_id")
    }

    sections = []
    for item in manifest.get("items") or []:
        entity_id = str(item.get("entity_id") or "unknown")
        recommendation = recommended_lookup.get(entity_id, {})
        confidence = item.get("candidate_confidence")
        sections.append(
            f"""
            <section class="candidate">
              <div class="candidate-head">
                <div>
                  <h2>#{item.get("rank")} · {item.get("display_name")}</h2>
                  <p class="meta">
                    <code>{entity_id}</code>
                    · candidate confidence <strong>{confidence:.3f}</strong>
                    · clip <code>{_escape(item.get("clip_stem") or "unknown")}</code>
                  </p>
                  <p class="meta">
                    Audit target frequency: <strong>{recommendation.get("count", 0)}</strong>
                    · max confidence: <strong>{recommendation.get("max_confidence", 0.0)}</strong>
                  </p>
                </div>
              </div>
              <div class="compare">
                {_image_card("Current Asset", _rel(item.get("current_asset_path"), output_dir))}
                {_image_card("Candidate Crop", _rel(item.get("candidate_crop_path"), output_dir))}
                {_image_card("ROI Crop", _rel(item.get("roi_crop_path"), output_dir))}
              </div>
              <p class="path">Comparison image: <a href="{_escape(_rel(item.get("comparison_path"), output_dir))}">{_escape(Path(item.get("comparison_path") or "").name)}</a></p>
            </section>
            """
        )

    recommended_html = "".join(
        f"<li><code>{_escape(str(item.get('weapon_id') or 'unknown'))}</code> · count {int(item.get('count', 0))} · max {float(item.get('max_confidence', 0.0)):.3f}</li>"
        for item in recommended[:10]
    )
    sheet_block = ""
    if sheet_path:
        sheet_rel = _rel(sheet_path, output_dir)
        sheet_block = f"""
        <section class="sheet">
          <h2>Review Sheet</h2>
          <p><a href="{_escape(sheet_rel)}">{_escape(Path(sheet_path).name)}</a></p>
          <img src="{_escape(sheet_rel)}" alt="Review sheet">
        </section>
        """

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Weapon Audit Review · { _escape(str(manifest.get("game") or "game")) }</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #121417;
      color: #e7ebf0;
      margin: 0;
      padding: 32px;
      line-height: 1.4;
    }}
    h1, h2 {{ margin: 0 0 8px; }}
    a {{ color: #9fd3ff; }}
    code {{
      background: #1f252c;
      border-radius: 4px;
      padding: 2px 6px;
    }}
    .summary {{
      margin-bottom: 28px;
      padding: 18px 20px;
      border: 1px solid #28313b;
      border-radius: 12px;
      background: #171c22;
    }}
    .summary ul {{
      margin: 10px 0 0 18px;
      padding: 0;
    }}
    .sheet {{
      margin-bottom: 28px;
    }}
    .sheet img {{
      width: min(100%, 1200px);
      border: 1px solid #28313b;
      border-radius: 12px;
      display: block;
    }}
    .candidate {{
      margin-bottom: 28px;
      padding: 18px 20px;
      border: 1px solid #28313b;
      border-radius: 12px;
      background: #171c22;
    }}
    .meta {{
      color: #b2bcc8;
      margin: 4px 0;
    }}
    .compare {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
      margin-top: 16px;
    }}
    .card {{
      background: #11161b;
      border: 1px solid #28313b;
      border-radius: 10px;
      padding: 12px;
    }}
    .card h3 {{
      margin: 0 0 10px;
      font-size: 14px;
      color: #dfe6ee;
    }}
    .card img {{
      width: 100%;
      height: auto;
      background: #0d1014;
      border-radius: 8px;
      border: 1px solid #25303a;
      display: block;
    }}
    .missing {{
      height: 180px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #8995a3;
      border: 1px dashed #3a4550;
      border-radius: 8px;
      background: #0f1318;
    }}
    .path {{
      margin-top: 12px;
      color: #a8b3bf;
      font-size: 14px;
    }}
    @media (max-width: 960px) {{
      .compare {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <section class="summary">
    <h1>Weapon Audit Review · {_escape(str(manifest.get("game") or "game"))}</h1>
    <p class="meta">Report: <code>{_escape(Path(str(manifest.get("report_path") or "")).name)}</code> · rendered items: <strong>{int(manifest.get("top_k", 0))}</strong></p>
    <p class="meta">Recommended correction targets are ranked from the latest audit. Promote only after visual inspection; direct gameplay crops can overmatch.</p>
    <ul>{recommended_html}</ul>
  </section>
  {sheet_block}
  {''.join(sections)}
</body>
</html>
"""


def _image_card(label: str, rel_path: str | None) -> str:
    if rel_path:
        return f"""
        <div class="card">
          <h3>{_escape(label)}</h3>
          <a href="{_escape(rel_path)}"><img src="{_escape(rel_path)}" alt="{_escape(label)}"></a>
        </div>
        """
    return f"""
    <div class="card">
      <h3>{_escape(label)}</h3>
      <div class="missing">missing</div>
    </div>
    """


def _rel(path: str | Path | None, output_dir: Path) -> str | None:
    if not path:
        return None
    try:
        return str(Path(path).relative_to(output_dir))
    except ValueError:
        return Path(path).name


def _escape(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
