"""
Deterministic router: maps user text → (topic, geo) pair.

No LLM calls.  Pure keyword matching, same logic as the monolith.
"""

from typing import Tuple


def route_topic_geo(text: str) -> Tuple[str, str]:
    """
    Return ``(topic, geo)`` for the user's question.

    ``topic`` is one of: rent, commute, migration, migration_city_request,
    language, population, tenure, labor, race, hispanic, education.

    ``geo`` is "state" or "county".
    """
    t = text.lower()

    # Core four topics 
    if any(k in t for k in ["rent", "rent burden", "rent-burden", "gross rent"]):
        return "rent", "county"
    if any(k in t for k in ["commute", "travel time to work"]):
        return "commute", "state"
    if any(k in t for k in ["migration", "moved in", "moving in", "inflow", "migrants"]):
        if "city" in t or "cities" in t:
            return "migration_city_request", "county"
        return "migration", "county"
    if any(k in t for k in ["non-english", "limited english", "english speaking", "language"]):
        return "language", "state"

    # Broader state-level topics (2020 only)
    import re
    if any(re.search(rf"\b{k}\b", t) for k in ["male", "female", "sex", "gender", "population", "men", "women", "age", "people", "demographic"]):
        return "population", "state"
    if any(k in t for k in ["renter", "owner", "tenure"]):
        return "tenure", "state"
    if any(k in t for k in ["unemployment", "labor force", "employed", "unemployed"]):
        return "labor", "state"
    if any(k in t for k in ["race", "white", "black", "asian", "ethnic", "ethnicity"]):
        return "race", "state"
    if any(k in t for k in ["hispanic", "latino"]):
        return "hispanic", "state"
    if any(k in t for k in ["bachelor", "masters", "phd", "education"]):
        return "education", "state"

    # Fallback
    return "rent", "county"


def wants_top_two_lists(text: str) -> bool:
    """Return True if the user is asking for a ranked/top list."""
    t = text.lower()
    return any(w in t for w in ["highest", "top", "most", "rank", "ranking"])
