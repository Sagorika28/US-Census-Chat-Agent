"""
SQL Agent: uses Snowflake Cortex to generate SQL from natural language.

The LLM receives the complete view catalog (view names + columns +
descriptions) and returns structured JSON with the SQL query.

Falls back to the deterministic router + compiler if the LLM's SQL
fails validation.
"""

import json
from typing import Any, Dict

from app.config import APP_DB, APP_SCHEMA, SQL_MODEL


# View catalog embedded in the prompt so the LLM knows exactly what's available.
_VIEW_CATALOG_PROMPT = f"""
AVAILABLE VIEWS (you may ONLY use these views and columns):

-- Rent burden (county-level, 2019 + 2020)
VIEW: {APP_DB}.{APP_SCHEMA}.V_RENT_BINS_COUNTY_{{YEAR}}
COLUMNS: STATE, COUNTY, computed_renters, lt_10, p10_14, p15_19, p20_24, p25_29, p30_34, p35_39, p40_49, p50_plus, pct_over_30, pct_over_40
DESCRIPTION: Rent-to-income burden by county. pct_over_30 = share of computed renters paying 30%+ of income on rent. pct_over_40 = share paying 40%+. Each p*_* column is a count of renters in that bin.

-- Commute (state-level, 2019 + 2020)
VIEW: {APP_DB}.{APP_SCHEMA}.V_COMMUTE_STATE_{{YEAR}}
COLUMNS: STATE, workers_total, avg_commute_minutes
DESCRIPTION: Estimated average commute time by state (weighted midpoint from ACS travel-time bins).

-- Migration (county-level, 2019 + 2020)
VIEW: {APP_DB}.{APP_SCHEMA}.V_IN_MIGRATION_COUNTY_{{YEAR}}
COLUMNS: STATE, COUNTY, pop_1p_total, inflow_any_total, inflow_any_rate, inflow_outside_total, inflow_outside_rate
DESCRIPTION: In-migration by county/county-equivalent. Metric A = any inflow (inflow_any_*). Metric B = outside-area inflow (inflow_outside_*). Counties include independent cities. ALWAYS include BOTH Metric A and Metric B columns.

-- Language (state-level, 2019 + 2020)
VIEW: {APP_DB}.{APP_SCHEMA}.V_LANGUAGE_STATE_{{YEAR}}
COLUMNS: STATE, total_households, non_english_households, pct_non_english, limited_english_households, pct_limited_english
DESCRIPTION: Household-based language stats from C16002. NOT person-level — cannot break down by sex/gender.

-- Population by sex (state-level, 2020 ONLY)
VIEW: {APP_DB}.{APP_SCHEMA}.V_POP_SEX_STATE_2020
COLUMNS: STATE, total_pop, male_pop, female_pop, pct_male, pct_female
DESCRIPTION: Statewide demographics split by male and female population. Use this for general population counts or queries specifically asking for men/women.

-- Housing tenure (state-level, 2020 ONLY)
VIEW: {APP_DB}.{APP_SCHEMA}.V_TENURE_STATE_2020
COLUMNS: STATE, occupied_units, owner_occupied, renter_occupied, pct_renter
DESCRIPTION: Housing tenure based on occupied units. Distinguishes between renters and owners. Use this for general renter/owner counts (not rent burden).

-- Labor force (state-level, 2020 ONLY)
VIEW: {APP_DB}.{APP_SCHEMA}.V_LABOR_STATE_2020
COLUMNS: STATE, pop_16_over, labor_force, employed, unemployed, unemployment_rate
DESCRIPTION: Labor force statistics for the population aged 16 and over, including overall unemployment rate.

-- Race (state-level, 2020 ONLY)
VIEW: {APP_DB}.{APP_SCHEMA}.V_RACE_STATE_2020
COLUMNS: STATE, total_pop, white_alone, black_alone, asian_alone, aian_alone, nhpi_alone, other_race_alone, two_or_more_races
DESCRIPTION: Statewide population strictly by major racial categories. Note: Does not include Hispanic/Latino (use the HISPANIC view for that).

-- Hispanic/Latino (state-level, 2020 ONLY)
VIEW: {APP_DB}.{APP_SCHEMA}.V_HISPANIC_STATE_2020
COLUMNS: STATE, total_pop, hispanic_latino, pct_hispanic_latino, not_hispanic_latino
DESCRIPTION: Statewide demographic split specifically for Hispanic/Latino vs Not Hispanic/Latino.

-- Education (state-level, 2020 ONLY)
VIEW: {APP_DB}.{APP_SCHEMA}.V_EDU_STATE_2020
COLUMNS: STATE, pop_25_over, bachelors_plus, pct_bachelors_plus
DESCRIPTION: Educational attainment for the population aged 25 and over. Tracks bachelor's degree or higher.
"""

_SYSTEM_PROMPT = f"""You are an expert SQL assistant with 20 years of experience. You work with US Census data stored in Snowflake.
You write SELECT queries and output ONLY valid JSON (no markdown, no extra text).

{_VIEW_CATALOG_PROMPT}

STRICT RULES:
1. Write ONLY SELECT statements. No INSERT/UPDATE/DELETE/DROP/ALTER/CREATE.
2. ONLY use views and columns listed above. Do NOT invent column names. Before returning JSON, verify every column in SELECT/WHERE/ORDER BY exists in the chosen view's column list. If not, return clarify.
3. Always include LIMIT. Default to LIMIT 15 unless the user explicitly asks for a different number (e.g. "top 5", "show 3"). Max 50.
4. No comments (--), no semicolons, no multi-statements.
5. No INFORMATION_SCHEMA or ACCOUNT_USAGE.
6. Replace {{YEAR}} with 2019 or 2020. Default to 2020 unless the user specifies otherwise.
7. Broader views (pop, tenure, labor, race, hispanic, edu) are 2020 ONLY.
8. For migration queries: ALWAYS SELECT both Metric A (inflow_any_total, inflow_any_rate) AND Metric B (inflow_outside_total, inflow_outside_rate).
9. "Cities" in migration context = county/county-equivalents (this dataset has no city-level granularity). If the user asks for city-level data for rent, commute, language, etc., return clarify and propose state/county options.
10. Language data is household-based, NOT person-level. Cannot filter by sex/gender.
11. For rent burden queries, filter computed_renters >= 1000 to exclude tiny counties.
12. For migration queries, filter pop_1p_total >= 5000 to exclude tiny counties.
13. STATE values are two-letter USPS abbreviations (e.g., TX, CA). If user types a full state name, convert it to the abbreviation.
14. You may use GROUP BY / aggregation only if all referenced columns exist and the query stays within a single view.
15. Set "view" in your JSON to the exact full resolved view name used in your SQL.
16. If the user asks for a specific rank (e.g. "5th", "3rd", "10th"), use ORDER BY + LIMIT 1 OFFSET N-1 to return ONLY that single row.
17. NEVER use rent, commute, or migration views to answer general population questions. If they ask for population in a year where a generic pop view doesn't exist, return clarify.
18. ALWAYS use `AS "Alias"` for every column in your SELECT statement. Convert the raw column names into clean, readable layman titles (Title Case). (e.g. `SELECT avg_commute_minutes AS "Average Commute Time (Mins)", pop_1p_total AS "Total Population (1yr+)"`).

OUTPUT FORMAT:
First, think step-by-step (briefly):
1. Which view has the columns needed for this question?
2. Verify every column in your SELECT/WHERE/ORDER BY exists in that view's column list.
3. Does the requested year match the view's availability?
Then output raw JSON on its own line (no markdown fences):
{{"mode": "sql", "year_mode": "single", "sql": "SELECT ... LIMIT 15", "year": 2020, "view": "<full view name>", "explanation": "brief note on what this query does"}}
OR
{{"mode": "sql", "year_mode": "compare", "sql": "SELECT ...", "year": 2020, "view": "<full view name>", "explanation": "..."}}

If the question is off-topic or unsafe, return: {{"mode": "refuse"}}
If the question is ambiguous or references non-existent columns, return: {{"mode": "clarify", "explanation": "what you need clarified"}}
"""


def _cortex_complete(session, model: str, prompt: str) -> str:
    """Call Snowflake Cortex COMPLETE and return the raw response text."""
    sql = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      '{model}',
      $$ {prompt} $$
    ) AS response
    """
    return session.sql(sql).collect()[0]["RESPONSE"]


def generate_sql(session, user_text: str, year: int = 2020) -> Dict[str, Any]:
    """
    Ask the LLM to write a SQL query for the user's question.

    Returns a dict with keys: mode, sql, year, view, explanation.
    Raises on LLM/network failure (caller should catch and fall back).
    """
    prompt = f"""{_SYSTEM_PROMPT}

USER QUESTION: {user_text}
PREFERRED YEAR: {year}

Return JSON only.
"""
    raw = _cortex_complete(session, SQL_MODEL, prompt)

    # The model may output COT reasoning before the JSON.
    # Extract the first valid JSON object from the response.
    cleaned = raw.strip()

    # Strip markdown fences if present
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

    # Find the JSON object: first '{' to last '}'
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1:
        cleaned = cleaned[start:end + 1]

    return json.loads(cleaned)
