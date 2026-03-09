"""Fluctuation sample extraction (from report.json) and optional Gemini interpretation."""

import json
import logging
import os
import sys
from pathlib import Path

# Allow import of scripts when run from project root (server.py adds APP_ROOT to path)
_APP_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from scripts.fluctuation_utils import extract_fluctuation_samples as _extract_impl

logger = logging.getLogger(__name__)

# Human-readable labels for common ISO 8601 observation periods (prompt readability)
_OBSERVATION_FREQUENCY_LABELS = {
    "P1Y": "yearly",
    "P1M": "monthly",
    "P1D": "daily",
    "P3M": "quarterly",
    "P1W": "weekly",
}


def _observation_frequency_label(observation_period: str) -> str:
    """Return a prompt-friendly label: 'yearly (P1Y)' or the raw value if unknown."""
    raw = (observation_period or "").strip().upper()
    if not raw:
        return "N/A"
    label = _OBSERVATION_FREQUENCY_LABELS.get(raw)
    if label:
        return f"{label} ({raw})"
    return raw


def extract_fluctuation_samples(report: dict) -> list[dict]:
    """Parse report.json statsCheckSummary and return structured fluctuation samples (with technical_signals)."""
    return _extract_impl(report)


def get_gemini_api_key() -> str | None:
    return os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")


def interpret_fluctuation(
    stat_var: str,
    location: str,
    period: str,
    percent_change: float | None,
    technical_signals: dict,
    observation_period: str = "",
    period_gap_years: float | None = None,
    series_length: int | None = None,
) -> str | None:
    """Call Gemini for a short, advisory-only interpretation of technical signals. Returns None if unavailable."""
    api_key = get_gemini_api_key()
    if not api_key:
        return None
    try:
        from google import genai
    except ImportError:
        return None
    client = genai.Client(api_key=api_key)
    ts = json.dumps(technical_signals, indent=2) if technical_signals else "{}"
    pct = f"{percent_change:+.2f}%" if percent_change is not None else "N/A"
    freq_label = _observation_frequency_label(observation_period)
    gap_years_str = str(period_gap_years) if period_gap_years is not None else "N/A"
    series_length_str = str(series_length) if series_length is not None else "N/A"
    prompt = f"""You are interpreting a data fluctuation anomaly. Classify it and explain briefly using the provided data.

Avoid speculation about real-world causes such as policy or economic events unless strongly implied by the data.

StatVar: {stat_var}
Location ID: {location}
Period: {period}
Observation frequency: {freq_label}
Period gap (years): {gap_years_str}
Percent change: {pct}
Total data points in series: {series_length_str}

Technical signals:
{ts}

Use the full context to assess the fluctuation.

Technical signals (e.g. previous_near_zero, first_valid_after_placeholder, missing_intermediate_periods, scaling_changed, unit_changed) are hints that may support or weaken an interpretation—they are not strict rules.

Mention technical signals only when they are relevant to the reasoning.
Avoid repeating generic statements such as "no technical flags indicate an error."

Do NOT guess or infer real-world place names from the Location ID.

Consider together:
• the magnitude of the change
• the time period
• the statistical plausibility
• the number of data points in the series
• what the technical signals suggest

If the series contains only two data points, prefer "Insufficient Context".
However, if the magnitude of change is extremely large or statistically unusual, it may still warrant "Needs Review".

Then choose the assessment that best fits.

Reply with exactly this format in plain text (no markdown):

Assessment: <exactly one of the four labels below, no other words>
Explanation: <1–3 sentences>

The Assessment line MUST be exactly one of:

Likely Valid
Needs Review
Possible Data Issue
Insufficient Context

Do not add extra words to the Assessment line (for example write only "Needs Review", not "The spike likely needs review").

Keep the explanation concise and specific to this case.

Keep total length under 700 characters."""
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
        )
        text = (getattr(response, "text", None) or "").strip()
        if not text and getattr(response, "candidates", None):
            for c in response.candidates:
                if getattr(c, "content", None) and getattr(c.content, "parts", None):
                    for p in c.content.parts:
                        if getattr(p, "text", None):
                            text = (text + " " + p.text).strip()
                            break
                if text:
                    break
        if not text or text.lower() in ("null", "n/a", "none"):
            logger.info("fluctuation-interpretation: no usable text from model (got %r)", text[:80] if text else "")
            return None
        return text[:700]
    except Exception as e:
        logger.warning("fluctuation-interpretation: %s", e)
        return None
