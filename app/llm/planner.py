"""
LLM planner: calls Snowflake Cortex COMPLETE to extract query filters.

The planner does NOT write SQL. It returns a JSON spec with
filters / order / limit / mode that the deterministic compiler uses.
"""

import json
from typing import Any, Dict

from app.config import SQL_MODEL
from app.helpers.query_spec import QUERY_SPEC_SCHEMA


def cortex_complete(session, model: str, prompt: str) -> str:
    """
    Call Snowflake Cortex COMPLETE with *model* and return the raw text.

    *session* is a Snowpark Session injected by the UI layer.
    """
    sql = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      '{model}',
      $$ {prompt} $$
    ) AS response
    """
    return session.sql(sql).collect()[0]["RESPONSE"]


def plan_query_filters_only(session, user_text: str) -> Dict[str, Any]:
    """
    Ask the LLM to return a JSON spec (filters, order, limit, mode).

    Uses PLANNER_MODEL first; falls back to FALLBACK_MODEL if the primary
    fails (including remote service / region errors).
    """
    system_rules = f"""
You output ONLY valid JSON (no markdown, no extra text).

SCOPE:
- Only help with Census questions and output mode="refuse" for off-topic/NSFW.

OUTPUT:
- Return JSON that matches this schema:
{json.dumps(QUERY_SPEC_SCHEMA)}

NOTES:
- For language, metrics are household-based.
- For rent, the app uses renter-occupied households with computed rent-to-income.
- For migration, return results with Metric A + Metric B (the app always includes both).
"""
    prompt = f"""{system_rules}

USER QUESTION:
{user_text}

Return JSON only.
"""

    # Try primary model
    try:
        raw = cortex_complete(session, SQL_MODEL, prompt)
        return json.loads(raw)
    except Exception:
        pass

    # Fallback: return safe default spec
    try:
        raw2 = cortex_complete(session, SQL_MODEL, prompt)
        return json.loads(raw2)
    except Exception:
        # Final safe fallback: keep pipeline running deterministically
        return {
            "mode": "clarify",
            "filters": {},
            "order_by": {},
            "limit": 15,
            "notes": "Planner unavailable (model/region). Clarify metric and geography.",
        }