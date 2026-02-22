"""
Query spec: schema definition and normalization.

Every component downstream of the planner can assume ``normalize_spec``
has been called, so required keys always exist with sane defaults.
"""

from typing import Any, Dict

QUERY_SPEC_SCHEMA = {
    "type": "object",
    "properties": {
        "mode":     {"type": "string", "enum": ["answer", "clarify", "refuse"]},
        "filters":  {"type": "object"},
        "order_by": {"type": "object"},
        "limit":    {"type": "integer"},
        "notes":    {"type": "string"},
    },
    "required": ["mode"],
}


def normalize_spec(
    raw: Dict[str, Any],
    topic: str,
    geo: str,
) -> Dict[str, Any]:
    """
    Merge planner output with router-determined *topic*/*geo* and
    fill missing keys with safe defaults.

    Returns a spec dict that the compiler can consume directly.
    """
    return {
        "mode":           raw.get("mode", "answer"),
        "topic":          topic,
        "geo":            geo,
        "filters":        raw.get("filters") or {},
        "order_by":       raw.get("order_by") or {},
        "limit":          raw.get("limit", 15),
        "show_two_lists": raw.get("show_two_lists", False),
    }
