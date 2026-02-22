"""
Migration policy: constants and helpers for migration-related output.

Migration output rules:
- ALWAYS show both Metric A (any inflow) and Metric B (outside-area inflow)
- "Top / highest" requests produce TWO side-by-side tables:
    1. Ranked by inflow count
    2. Ranked by inflow rate
- Results are county/county-equivalents (includes independent cities)
- When user asks for "cities", clarify the geography limitation first
"""

from typing import Any, Dict

MIGRATION_CLARIFICATION_MSG = (
    "I can't provide true city-level results from this dataset. "
    "I can show **county / county-equivalent (includes independent cities)** "
    "migration instead. Want me to show counties?"
)

MIGRATION_INFO_MSG = (
    "Migration results are county/county-equivalents (includes independent cities). "
    "**Any inflow** (inflow_any) = people who moved in from anywhere "
    "(within the same state or from outside). "
    "**Outside inflow** (inflow_outside) = people who moved in from a "
    "different metro/micro area or from abroad."
)

LANGUAGE_INFO_MSG = (
    "Language metrics are household-based (share of households), "
    "not person-level language."
)

CHOOSE_ALT_MSG = (
    "I can't compute **female non-English speaking population** from the "
    "views in this dataset, because the language table here is "
    "**household-based** (C16002), not person-based by sex. "
    "I can do either:\n"
    "1) **Top states by non-English households** (household share), or\n"
    "2) **Top states by female population**.\n\n"
    "Reply with **1** or **2**."
)

CHOOSE_ALT_PENDING = {
    "type": "choose_alt",
    "alts": ["language_households", "female_population"],
}


def build_migration_city_pending_spec() -> Dict[str, Any]:
    """Return the pending-spec stored when user asks for city-level migration."""
    return {
        "mode":           "answer",
        "topic":          "migration",
        "geo":            "county",
        "filters":        {},
        "order_by":       {"field": "inflow_any_total", "direction": "DESC"},
        "limit":          15,
        "show_two_lists": True,
    }
