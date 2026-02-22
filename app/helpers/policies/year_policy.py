"""
Year policy: how to pick the right year(s) for a query.

Rules:
- Default (no year specified) -> 2020 (latest available)
- "latest" / "most recent"   -> 2020
- Explicit year (2019/2020)  -> use that year
- Compare / trend / vs       -> show 2019 AND 2020 side-by-side
- Years outside availability -> explain only 2019 & 2020 exist, show both
"""

import re
from typing import Optional, Tuple


def is_year_followup(text: str) -> Optional[str]:
    """
    Detect bare year-related follow-ups.

    Returns:
        "2019" or "2020" for single-year requests,
        "compare" for comparison requests,
        None otherwise.
    """
    t = text.lower().strip()

    # accept: 2019 / show 2019 / show 2019 too / show 2019 also
    if re.fullmatch(r"(show\s+)?2019(\s+(too|also))?", t):
        return "2019"
    if re.fullmatch(r"(show\s+)?2020(\s+(too|also))?", t):
        return "2020"

    if any(p in t for p in [
        "compare", "both years", "2019 and 2020",
        "2019 vs 2020", "2020 vs 2019",
    ]):
        return "compare"

    return None


def detect_year_mode(text: str) -> Tuple[str, Optional[int]]:
    """
    Parse the user's text for year signals.

    Returns:
        ("single", <year>) or ("compare", None).
    """
    t = text.lower()
    years = re.findall(r"\b(2019|2020)\b", t)

    if any(w in t for w in ["compare", "trend", "change", "over time", "vs", "versus"]):
        return "compare", None
    if years:
        return "single", int(years[-1])
    if "latest" in t or "most recent" in t:
        return "single", 2020

    return "single", None