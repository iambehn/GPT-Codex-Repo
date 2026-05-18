from __future__ import annotations

from typing import Any


def build_dual_layer_comparison_selection(
    replay_comparison: Any,
    candidate_comparison: Any,
) -> dict[str, Any] | None:
    replay_value = replay_comparison if isinstance(replay_comparison, dict) else None
    candidate_value = candidate_comparison if isinstance(candidate_comparison, dict) else None
    if replay_value is not None:
        result: dict[str, Any] = {
            "comparison": replay_value,
            "source": "replay_run",
        }
        if candidate_value is not None and comparison_layers_differ_materially(replay_value, candidate_value):
            result["secondary_comparison"] = candidate_value
            result["secondary_source"] = "crop_candidate"
            result["difference_summary"] = build_comparison_difference_summary(replay_value, candidate_value)
        return result
    if candidate_value is not None:
        return {"comparison": candidate_value, "source": "crop_candidate"}
    return None


def comparison_layers_differ_materially(primary: dict[str, Any], secondary: dict[str, Any]) -> bool:
    primary_overlap = primary.get("spatial_overlap") if isinstance(primary.get("spatial_overlap"), dict) else None
    secondary_overlap = secondary.get("spatial_overlap") if isinstance(secondary.get("spatial_overlap"), dict) else None
    if (primary_overlap is None) != (secondary_overlap is None):
        return True
    if primary_overlap is None and secondary_overlap is None:
        return False
    assert isinstance(primary_overlap, dict)
    assert isinstance(secondary_overlap, dict)
    for key in (
        "reference_source",
        "reference_crop",
        "iou",
        "revised_coverage_ratio",
        "reference_coverage_ratio",
    ):
        if primary_overlap.get(key) != secondary_overlap.get(key):
            return True
    return False


def build_comparison_difference_summary(primary: dict[str, Any], secondary: dict[str, Any]) -> str:
    primary_overlap = primary.get("spatial_overlap") if isinstance(primary.get("spatial_overlap"), dict) else {}
    secondary_overlap = secondary.get("spatial_overlap") if isinstance(secondary.get("spatial_overlap"), dict) else {}
    primary_source = str(primary_overlap.get("reference_source") or "").strip()
    secondary_source = str(secondary_overlap.get("reference_source") or "").strip()
    primary_crop = str(primary_overlap.get("reference_crop") or "").strip()
    secondary_crop = str(secondary_overlap.get("reference_crop") or "").strip()
    primary_iou = primary_overlap.get("iou")
    secondary_iou = secondary_overlap.get("iou")
    if primary_source != secondary_source and primary_source and secondary_source:
        summary = f"Replay changed overlap source from {secondary_source} to {primary_source}"
        if primary_iou is not None and secondary_iou is not None and primary_iou != secondary_iou:
            summary += f" and changed IoU from {secondary_iou} to {primary_iou}"
        return summary + "."
    if primary_crop != secondary_crop and primary_crop and secondary_crop:
        return f"Replay changed overlap reference crop from {secondary_crop} to {primary_crop}."
    if primary_iou is not None and secondary_iou is not None and primary_iou != secondary_iou:
        return f"Replay changed IoU from {secondary_iou} to {primary_iou}."
    primary_revised = primary_overlap.get("revised_coverage_ratio")
    secondary_revised = secondary_overlap.get("revised_coverage_ratio")
    if primary_revised != secondary_revised and primary_revised is not None and secondary_revised is not None:
        return f"Replay changed revised coverage from {secondary_revised} to {primary_revised}."
    primary_reference = primary_overlap.get("reference_coverage_ratio")
    secondary_reference = secondary_overlap.get("reference_coverage_ratio")
    if primary_reference != secondary_reference and primary_reference is not None and secondary_reference is not None:
        return f"Replay changed reference coverage from {secondary_reference} to {primary_reference}."
    return "Replay changed the detector calibration comparison relative to the candidate-time baseline."
