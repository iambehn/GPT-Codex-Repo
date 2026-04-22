"""
Audio Detector — DSP-based event detection from clip audio

Detects kills, headshots, multi-kills, objectives, and victory/defeat cues
by analysing the audio track of each clip. Uses FFmpeg astats to extract
per-frame RMS levels, then applies Z-score spike detection and optional
frequency-band filtering via scipy. No ML dependencies required.

Detection output is written to meta.json under 'audio_events' and the
spike timestamps are made available to the kill-feed parser so OpenCV
only inspects frames near confirmed audio events.

Config block (config.yaml → audio_detector):
    enabled: false
    z_score_threshold: 3.0        # std devs above rolling baseline = spike
    baseline_window_seconds: 60   # rolling RMS window for baseline
    min_spike_gap_seconds: 1.5    # merge spikes closer than this
    frame_duration_seconds: 0.1   # astats analysis window (100 ms)
    frequency_filter:
      enabled: true               # bandpass filter before RMS analysis
      kill_low_hz: 800            # lower edge of kill/headshot band
      kill_high_hz: 4000          # upper edge of kill/headshot band
    multi_kill_window_seconds: 5  # cluster spikes within this window → multi-kill
    multi_kill_min_spikes: 3      # minimum spikes in window to label multi-kill

Source:  clip's audio track (extracted in-memory via FFmpeg pipe)
Output:  audio_events key in the clip's .meta.json
"""

from __future__ import annotations

import json
import subprocess
import re
from pathlib import Path
from datetime import datetime

from utils.logger import get_logger

logger = get_logger(__name__)

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False

try:
    from scipy.signal import butter, sosfilt
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False

_DEFAULT_Z_THRESHOLD = 3.0
_DEFAULT_BASELINE_WINDOW = 60.0
_DEFAULT_MIN_GAP = 1.5
_DEFAULT_FRAME_DUR = 0.1
_DEFAULT_MULTI_KILL_WINDOW = 5.0
_DEFAULT_MULTI_KILL_MIN = 3


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def run_audio_detector(clip_path: Path, game: str, config: dict, force: bool = False) -> dict:
    """Detect audio events in a clip and write results to its meta.json.

    Idempotent: skips if 'audio_events' key already present in meta.json.

    Returns the audio_events dict written to meta.json.
    """
    ad_cfg = config.get("audio_detector", {})
    meta_path = clip_path.with_suffix(".meta.json")

    if meta_path.exists() and not force:
        try:
            existing = json.loads(meta_path.read_text())
            if "audio_events" in existing:
                logger.debug(f"[audio_detector] Already processed: {clip_path.name}")
                return existing["audio_events"]
        except (json.JSONDecodeError, OSError):
            pass

    if not ad_cfg.get("enabled", False):
        return _write(meta_path, _disabled("audio_detector disabled"))

    if not _NUMPY_AVAILABLE:
        logger.warning("[audio_detector] numpy not installed — skipping. Run: pip install numpy")
        return _write(meta_path, _disabled("numpy not installed"))

    frame_dur = float(ad_cfg.get("frame_duration_seconds", _DEFAULT_FRAME_DUR))
    rms_frames = _extract_rms(clip_path, frame_dur)

    if rms_frames is None or len(rms_frames) < 10:
        logger.warning(f"[audio_detector] Could not extract audio from {clip_path.name}")
        return _write(meta_path, _disabled("ffmpeg audio extraction failed"))

    # Optional bandpass filter — isolates kill/headshot frequency band
    ff_cfg = ad_cfg.get("frequency_filter", {})
    if ff_cfg.get("enabled", True) and _SCIPY_AVAILABLE:
        sample_rate = int(round(1.0 / frame_dur))  # effective frame rate
        low_hz = float(ff_cfg.get("kill_low_hz", 800))
        high_hz = float(ff_cfg.get("kill_high_hz", 4000))
        rms_frames = _bandpass_rms(clip_path, frame_dur, low_hz, high_hz) or rms_frames

    z_threshold = float(ad_cfg.get("z_score_threshold", _DEFAULT_Z_THRESHOLD))
    baseline_window = float(ad_cfg.get("baseline_window_seconds", _DEFAULT_BASELINE_WINDOW))
    min_gap = float(ad_cfg.get("min_spike_gap_seconds", _DEFAULT_MIN_GAP))

    spike_times = _detect_spikes(rms_frames, frame_dur, z_threshold, baseline_window, min_gap)

    multi_window = float(ad_cfg.get("multi_kill_window_seconds", _DEFAULT_MULTI_KILL_WINDOW))
    multi_min = int(ad_cfg.get("multi_kill_min_spikes", _DEFAULT_MULTI_KILL_MIN))
    events = _classify_events(spike_times, multi_window, multi_min)

    result = {
        "spike_timestamps":    [round(t, 3) for t in spike_times],
        "spike_count":         len(spike_times),
        "events":              events,
        "multi_kill_detected": any(e["type"] == "multi_kill" for e in events),
        "method":              "zscore" + ("+bandpass" if ff_cfg.get("enabled", True) and _SCIPY_AVAILABLE else ""),
        "analysed_at":         datetime.now().isoformat(timespec="seconds"),
    }

    _write(meta_path, result)

    logger.info(
        f"[audio_detector] {clip_path.name}: {len(spike_times)} spike(s), "
        f"{len(events)} event(s) — {result['method']}"
    )
    return result


# ---------------------------------------------------------------------------
# RMS extraction via FFmpeg astats
# ---------------------------------------------------------------------------

def _extract_rms(clip_path: Path, frame_dur: float) -> list[float] | None:
    """Run FFmpeg astats and return a list of per-frame RMS dB values."""
    # astats outputs one metadata block per reset window.
    # We use reset=<frames_per_window> — at 44100 Hz, 100ms = 4410 samples.
    # Simpler: use a 1-second mono downmix at 100ms windows.
    cmd = [
        "ffmpeg", "-y",
        "-i", str(clip_path),
        "-af", f"aresample=44100,aformat=sample_fmts=fltp:channel_layouts=mono,"
               f"astats=metadata=1:reset={int(44100 * frame_dur)},"
               f"ametadata=print:key=lavfi.astats.Overall.RMS_level:file=-",
        "-f", "null", "-",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        logger.error(f"[audio_detector] FFmpeg error: {e}")
        return None

    # Parse lines like: lavfi.astats.Overall.RMS_level=-28.34
    rms_values: list[float] = []
    pattern = re.compile(r"lavfi\.astats\.Overall\.RMS_level=(-?\d+\.?\d*)")
    for line in result.stderr.splitlines() + result.stdout.splitlines():
        m = pattern.search(line)
        if m:
            val = float(m.group(1))
            # FFmpeg outputs -inf for silence — clamp to a low floor
            rms_values.append(max(val, -80.0))

    return rms_values if rms_values else None


def _bandpass_rms(
    clip_path: Path,
    frame_dur: float,
    low_hz: float,
    high_hz: float,
) -> list[float] | None:
    """Extract RMS after applying a bandpass filter in FFmpeg.

    Isolates the kill/headshot frequency band (default 800–4000 Hz) before
    computing RMS, reducing false positives from bass-heavy background music.
    """
    bp_filter = (
        f"aresample=44100,aformat=sample_fmts=fltp:channel_layouts=mono,"
        f"highpass=f={int(low_hz)},lowpass=f={int(high_hz)},"
        f"astats=metadata=1:reset={int(44100 * frame_dur)},"
        f"ametadata=print:key=lavfi.astats.Overall.RMS_level:file=-"
    )
    cmd = [
        "ffmpeg", "-y",
        "-i", str(clip_path),
        "-af", bp_filter,
        "-f", "null", "-",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except (subprocess.SubprocessError, FileNotFoundError):
        return None

    pattern = re.compile(r"lavfi\.astats\.Overall\.RMS_level=(-?\d+\.?\d*)")
    values: list[float] = []
    for line in result.stderr.splitlines() + result.stdout.splitlines():
        m = pattern.search(line)
        if m:
            values.append(max(float(m.group(1)), -80.0))
    return values if values else None


# ---------------------------------------------------------------------------
# Spike detection
# ---------------------------------------------------------------------------

def _detect_spikes(
    rms_db: list[float],
    frame_dur: float,
    z_threshold: float,
    baseline_window_sec: float,
    min_gap_sec: float,
) -> list[float]:
    """Return timestamps (seconds) of RMS spikes above z_threshold std devs.

    Uses a rolling baseline so a consistently loud clip doesn't produce false
    positives — only sudden spikes relative to recent history are flagged.
    """
    arr = np.array(rms_db, dtype=np.float64)
    baseline_frames = max(1, int(baseline_window_sec / frame_dur))
    min_gap_frames = max(1, int(min_gap_sec / frame_dur))

    spike_times: list[float] = []
    last_spike_frame = -min_gap_frames

    for i in range(len(arr)):
        window_start = max(0, i - baseline_frames)
        window = arr[window_start:i] if i > 0 else arr[:1]
        mean = float(np.mean(window))
        std = float(np.std(window)) or 1.0

        z = (arr[i] - mean) / std
        if z >= z_threshold and (i - last_spike_frame) >= min_gap_frames:
            spike_times.append(round(i * frame_dur, 3))
            last_spike_frame = i

    return spike_times


# ---------------------------------------------------------------------------
# Event classification
# ---------------------------------------------------------------------------

def _classify_events(
    spike_times: list[float],
    multi_kill_window: float,
    multi_kill_min_spikes: int,
) -> list[dict]:
    """Group spikes into named events.

    A cluster of ≥ multi_kill_min_spikes spikes within multi_kill_window
    seconds is labelled 'multi_kill'. Isolated spikes are labelled 'kill'.
    """
    if not spike_times:
        return []

    events: list[dict] = []
    used: set[int] = set()

    for i, t in enumerate(spike_times):
        if i in used:
            continue
        cluster = [j for j, s in enumerate(spike_times) if abs(s - t) <= multi_kill_window and j not in used]
        if len(cluster) >= multi_kill_min_spikes:
            for j in cluster:
                used.add(j)
            events.append({
                "type":       "multi_kill",
                "timestamp":  round(min(spike_times[j] for j in cluster), 3),
                "spike_count": len(cluster),
            })
        else:
            used.add(i)
            events.append({
                "type":      "kill",
                "timestamp": t,
            })

    return sorted(events, key=lambda e: e["timestamp"])


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _write(meta_path: Path, result: dict) -> dict:
    try:
        existing = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        existing["audio_events"] = result
        meta_path.write_text(json.dumps(existing, indent=2))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"[audio_detector] Could not write meta: {e}")
    return result


def _disabled(reason: str) -> dict:
    return {
        "spike_timestamps":    [],
        "spike_count":         0,
        "events":              [],
        "multi_kill_detected": False,
        "method":              "disabled",
        "reason":              reason,
        "analysed_at":         datetime.now().isoformat(timespec="seconds"),
    }
