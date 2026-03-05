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
    prompt = f"""You are interpreting technical signals for a data fluctuation anomaly. Classify it and explain briefly using the provided data. Do NOT speculate about real-world causes, policy, or economics.

StatVar: {stat_var}
Location: {location}
Period: {period}
Percent change: {pct}
Technical signals:
{ts}

The Assessment must be based on the technical flags in technical_signals (e.g. previous_near_zero, first_valid_after_placeholder, missing_intermediate_periods, scaling_changed, unit_changed). If none of these technical flags indicate issues, prefer "Likely Valid" even if the percent change is large. Percent change magnitude alone should not cause "Needs Review" or "Possible Data Issue".

Reply with exactly this format in plain text (no markdown):
Assessment: <exactly one of the four labels below, no other words>
Explanation: <1–3 sentences based only on the numbers and flags above>

The Assessment line MUST be exactly one of:
Likely Valid
Needs Review
Possible Data Issue
Insufficient Context

Do not add extra words to the Assessment line (e.g. do not write "The spike likely needs review"—write only "Needs Review"). Keep explanation concise. Avoid listing all flags unless necessary. Keep total length under 700 characters."""
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
