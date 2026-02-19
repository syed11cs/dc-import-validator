#!/usr/bin/env python3
"""Shared fluctuation extraction and technical signals (deterministic).

Used by generate_html_report.py and ui.server. No AI; no impact on validation severity or exit code.
"""

import re
from datetime import datetime
from pathlib import Path

FLUCTUATION_COUNTER_KEYS = (
    "StatsCheck_MaxPercentFluctuationGreaterThan500",
    "StatsCheck_MaxPercentFluctuationGreaterThan100",
)


def _value_at(pt: dict) -> float | str | None:
    """Extract numeric or string value from a problem point."""
    vals = pt.get("values") or []
    if not vals:
        return None
    v = vals[0].get("value") if isinstance(vals[0], dict) else None
    if v is None:
        return None
    if isinstance(v, dict):
        v = v.get("value")
    if v is None:
        return None
    return v


def _numeric(v) -> float | None:
    """Coerce to float if possible."""
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _scaling_at(pt: dict) -> str | None:
    """Extract scaling factor from point if present (e.g. values[0].scalingFactor or point.scalingFactor)."""
    vals = pt.get("values") or []
    if vals and isinstance(vals[0], dict) and vals[0].get("scalingFactor") is not None:
        return str(vals[0].get("scalingFactor"))
    if pt.get("scalingFactor") is not None:
        return str(pt.get("scalingFactor"))
    return None


def _unit_at(pt: dict) -> str | None:
    """Extract unit from point if present."""
    vals = pt.get("values") or []
    if vals and isinstance(vals[0], dict) and vals[0].get("unit") is not None:
        return str(vals[0].get("unit"))
    if pt.get("unit") is not None:
        return str(pt.get("unit"))
    return None


def _locs_at(pt: dict) -> list[dict]:
    """Extract locations (file, lineNumber) from point if present."""
    vals = pt.get("values") or []
    if not vals or not isinstance(vals[0], dict):
        return []
    locs = (vals[0].get("locations") or []) if isinstance(vals[0], dict) else []
    return [{"file": loc.get("file"), "lineNumber": loc.get("lineNumber")} for loc in locs]


def _parse_date(s: str) -> datetime | None:
    """Parse date string to datetime for gap computation. Handles YYYY, YYYY-MM, YYYY-MM-DD."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    for fmt, length in (("%Y-%m-%d", 10), ("%Y-%m", 7), ("%Y", 4)):
        if len(s) >= length:
            try:
                return datetime.strptime(s[:length], fmt)
            except (ValueError, TypeError):
                continue
    return None


def _observation_period_days(observation_period: str) -> int | None:
    """Convert ISO 8601 duration (e.g. P1Y, P1M, P1D) to approximate days. Returns None if unparseable."""
    if not observation_period or not isinstance(observation_period, str):
        return None
    s = observation_period.strip().upper()
    # P1Y, P2M, P7D, etc.
    m = re.match(r"P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?", s)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    days = 0
    if y:
        days += int(y) * 365
    if mo:
        days += int(mo) * 30
    if d:
        days += int(d)
    return days if days > 0 else None


def _compute_technical_signals(
    previous_point: dict,
    current_point: dict,
    observation_period: str,
    all_sorted_points: list[dict],
) -> dict:
    """
    Compute deterministic technical signals from previous and current point.
    previous_point/current_point are normalized {date, value, scalingFactor?, unit?}.
    """
    prev_val = previous_point.get("value")
    curr_val = current_point.get("value")
    prev_num = _numeric(prev_val)
    curr_num = _numeric(curr_val)
    prev_date = _parse_date(previous_point.get("date") or "")
    curr_date = _parse_date(current_point.get("date") or "")

    # percent_change = ((current - previous) / abs(previous)) * 100
    percent_change = None
    if prev_num is not None and curr_num is not None and abs(prev_num) >= 1e-9:
        percent_change = ((curr_num - prev_num) / abs(prev_num)) * 100.0
    elif prev_num is not None and curr_num is not None and abs(prev_num) < 1e-9:
        percent_change = None  # previous is zero

    previous_near_zero = prev_num is not None and abs(prev_num) < 1
    first_valid_after_placeholder = (
        prev_num is not None and curr_num is not None and prev_num <= 0 and curr_num > 0
    )

    # scaling_changed / unit_changed only if present
    prev_scale = previous_point.get("scalingFactor")
    curr_scale = current_point.get("scalingFactor")
    scaling_changed = None
    if prev_scale is not None and curr_scale is not None:
        scaling_changed = prev_scale != curr_scale

    prev_unit = previous_point.get("unit")
    curr_unit = current_point.get("unit")
    unit_changed = None
    if prev_unit is not None and curr_unit is not None:
        unit_changed = prev_unit != curr_unit

    # missing_intermediate_periods
    missing_intermediate_periods = None
    if prev_date and curr_date:
        gap_days = (curr_date - prev_date).days
        period_days = _observation_period_days(observation_period)
        if period_days is not None and period_days > 0:
            missing_intermediate_periods = gap_days > period_days
        elif len(all_sorted_points) >= 3:
            # Infer typical step from other consecutive pairs
            gaps = []
            for i in range(1, len(all_sorted_points)):
                a = _parse_date(all_sorted_points[i - 1].get("date") or "")
                b = _parse_date(all_sorted_points[i].get("date") or "")
                if a and b:
                    gaps.append((b - a).days)
            if gaps:
                typical = max(1, int(sum(gaps) / len(gaps)))
                missing_intermediate_periods = gap_days > typical

    return {
        "previous_value": prev_num,
        "current_value": curr_num,
        "percent_change": percent_change,
        "previous_near_zero": previous_near_zero,
        "first_valid_after_placeholder": first_valid_after_placeholder,
        "missing_intermediate_periods": missing_intermediate_periods,
        "scaling_changed": scaling_changed,
        "unit_changed": unit_changed,
    }


def extract_fluctuation_samples(report: dict) -> list[dict]:
    """
    Parse report.json statsCheckSummary and return structured fluctuation samples.
    Each sample includes technical_signals when >= 2 chronological points exist.
    """
    samples = []
    summary = report.get("statsCheckSummary") or []
    for item in summary:
        place_dcid = item.get("placeDcid") or item.get("observationAbout") or ""
        stat_var = item.get("statVarDcid") or ""
        observation_period = item.get("observationPeriod") or ""
        for counter in item.get("validationCounters") or []:
            key = counter.get("counterKey") or ""
            if key not in FLUCTUATION_COUNTER_KEYS:
                continue
            points = counter.get("problemPoints") or []
            if not points:
                continue

            sorted_points = sorted(
                [p for p in points if p.get("date")],
                key=lambda p: p.get("date") or "",
            )
            point_list = []
            for p in sorted_points:
                pt = {
                    "date": p.get("date"),
                    "value": _value_at(p),
                }
                scale = _scaling_at(p)
                unit = _unit_at(p)
                locs = _locs_at(p)
                if scale is not None:
                    pt["scalingFactor"] = scale
                if unit is not None:
                    pt["unit"] = unit
                if locs:
                    pt["locations"] = locs
                point_list.append(pt)

            if len(point_list) < 2:
                technical_signals = None
            else:
                prev_pt = point_list[-2]
                curr_pt = point_list[-1]
                technical_signals = _compute_technical_signals(
                    prev_pt,
                    curr_pt,
                    observation_period,
                    point_list,
                )

            pct = counter.get("percentDifference")
            if pct is not None and isinstance(pct, (int, float)):
                try:
                    pct = float(pct)
                except (TypeError, ValueError):
                    pct = None
            elif pct is not None:
                try:
                    pct = float(pct)
                except (TypeError, ValueError):
                    pct = None

            samples.append({
                "statVar": stat_var,
                "location": place_dcid,
                "observationPeriod": observation_period,
                "counterKey": key,
                "problemPoints": point_list,
                "percentDifference": pct,
                "technical_signals": technical_signals,
            })
    return samples
