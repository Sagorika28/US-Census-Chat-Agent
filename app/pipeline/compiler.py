"""
SQL compiler: maps a query spec → (SQL string, source view, column list).

All SQL is built deterministically from the curated view registry.
The LLM never writes SQL — only the compiler does.
"""

from typing import Any, Dict, List, Optional, Tuple

from app.config import ALLOWED_VIEWS


def view_for(topic: str, geo: str, year: int) -> str:
    """Resolve the fully-qualified view name for *topic*/*geo*/*year*."""
    views = ALLOWED_VIEWS.get(year, {})

    # Core four topics
    if topic == "rent" and geo == "county":
        return views["rent_county"]
    if topic == "migration" and geo == "county":
        return views["migration_county"]
    if topic == "commute" and geo == "state":
        return views["commute_state"]
    if topic == "language" and geo == "state":
        return views["language_state"]

    # Broader topics (2020 state-level only)
    if year == 2020 and geo == "state":
        mapping = {
            "population": "pop_sex_state",
            "tenure":     "tenure_state",
            "labor":      "labor_state",
            "race":       "race_state",
            "hispanic":   "hispanic_state",
            "education":  "edu_state",
        }
        key = mapping.get(topic)
        if key and key in views:
            return views[key]

    raise ValueError(f"Unsupported topic/geo/year: {topic}/{geo}/{year}")


def enforce_limit(n: Optional[int]) -> int:
    """Clamp *n* to [1, 50], defaulting to 15."""
    if not n:
        return 15
    return max(1, min(int(n), 50))


# Column / order definitions per topic
_TOPIC_DEFS = {
    "rent": {
        "cols": ["STATE", "COUNTY", "computed_renters", "pct_over_30", "pct_over_40"],
        "default_where": ["computed_renters >= 1000"],
        "order": "pct_over_30 DESC",
    },
    "commute": {
        "cols": ["STATE", "workers_total", "avg_commute_minutes"],
        "default_where": [],
        "order": "avg_commute_minutes DESC",
    },
    "migration": {
        "cols": [
            "STATE", "COUNTY", "pop_1p_total",
            "inflow_any_total", "inflow_any_rate",
            "inflow_outside_total", "inflow_outside_rate",
        ],
        "default_where": ["pop_1p_total >= 5000"],
        "order": None,  # determined by spec.order_by
    },
    "language": {
        "cols": [
            "STATE", "total_households",
            "non_english_households", "pct_non_english",
            "limited_english_households", "pct_limited_english",
        ],
        "default_where": [],
        "order": "pct_non_english DESC",
    },
    "population": {
        "cols": ["STATE", "total_pop", "male_pop", "female_pop", "pct_male", "pct_female"],
        "order": "total_pop DESC",
    },
    "tenure": {
        "cols": ["STATE", "occupied_units", "owner_occupied", "renter_occupied", "pct_renter"],
        "order": "pct_renter DESC",
    },
    "labor": {
        "cols": ["STATE", "pop_16_over", "labor_force", "employed", "unemployed", "unemployment_rate"],
        "order": "unemployment_rate DESC",
    },
    "race": {
        "cols": [
            "STATE", "total_pop", "white_alone", "black_alone", "asian_alone",
            "aian_alone", "nhpi_alone", "other_race_alone", "two_or_more_races",
        ],
        "order": "total_pop DESC",
    },
    "hispanic": {
        "cols": ["STATE", "total_pop", "hispanic_latino", "pct_hispanic_latino", "not_hispanic_latino"],
        "order": "pct_hispanic_latino DESC",
    },
    "education": {
        "cols": ["STATE", "pop_25_over", "bachelors_plus", "pct_bachelors_plus"],
        "order": "pct_bachelors_plus DESC",
    },
}


def compile_sql(spec: Dict[str, Any], year: int) -> Tuple[str, str, List[str]]:
    """
    Build a SELECT statement from *spec* and *year*.

    Returns ``(sql, source_view, columns_used)``.
    """
    topic = spec.get("topic")
    geo = spec.get("geo")
    filters = spec.get("filters") or {}
    order_by = spec.get("order_by") or {}
    limit = enforce_limit(spec.get("limit"))

    v = view_for(topic, geo, year)
    defn = _TOPIC_DEFS.get(topic)
    if defn is None:
        raise ValueError(f"Unsupported topic: {topic}")

    cols = defn["cols"]
    where = list(defn.get("default_where", []))

    # Optional state filter from planner
    if "state" in filters:
        where.append(f"STATE = '{filters['state']}'")

    # Order clause
    if topic == "migration":
        order_field = order_by.get("field", "inflow_any_total")
        direction = (order_by.get("direction", "DESC") or "DESC").upper()
        if order_field not in cols:
            order_field = "inflow_any_total"
        if direction not in ("ASC", "DESC"):
            direction = "DESC"
        order = f"{order_field} {direction}"
    else:
        order = defn["order"]

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    sql = f"SELECT {', '.join(cols)} FROM {v} {where_sql} ORDER BY {order} LIMIT {limit}"
    return sql, v, cols
