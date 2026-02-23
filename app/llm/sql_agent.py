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
DESCRIPTION: Household-based language stats from C16002. NOT person-level - cannot break down by sex/gender.

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
3. ALWAYS INCLUDE LIMIT AT THE VERY END OF YOUR QUERY. Default to LIMIT 15 unless the user explicitly asks for a different number. Max 50. If you use CTEs or subqueries, the LIMIT must go on the FINAL, outermost SELECT statement.
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
14. You may use GROUP BY / aggregation only if all referenced columns exist and the query stays within a single view (unless JOINing, see rule 22).
15. Set "view" in your JSON to the exact full resolved view name used in your SQL. If JOINing multiple views, set "view" to a comma-separated list of the view names used.
16. If the user asks for a specific rank (e.g. "5th", "3rd", "10th"), use ORDER BY + LIMIT 1 OFFSET N-1 to return ONLY that single row.
17. NEVER use rent, commute, or migration views to answer general population questions. If they ask for population in a year where a generic pop view doesn't exist, return clarify.
18. ALWAYS use `AS "Alias"` for every column in your SELECT statement. Convert the raw column names into clean, readable layman titles (Title Case). (e.g. `SELECT avg_commute_minutes AS "Average Commute Time (Mins)", pop_1p_total AS "Total Population (1yr+)"`).
19. Pay close attention to "mean" or "average" vs "percentage". If asked for "average male population", compute `AVG(male_pop)`. Do NOT just return the `pct_male` column unless they ask for "percent", "proportion", or "share".
20. MULTI-PART QUESTIONS: If the user asks a compound question (e.g. "which state has the 7th longest commute AND by how much is it smaller than the 2nd?"), write a SINGLE SQL query that returns ALL the data needed to answer every part. Use subqueries, window functions (ROW_NUMBER), or CTEs to fetch the required ranked rows and computed differences. Never answer only the first part of a compound question.
21. CONTEXT BOUNDARIES: The <user_question> is your PRIMARY objective. Answer it based ONLY on the text inside the <user_question> tags. You may ONLY consult <conversation_history> if the <user_question> contains pronouns ("it", "they") or explicit references to previous results ("that state", "the 2nd position", "show 2019 too"). If the <user_question> is self-contained (e.g. "show 2019 vs 2020 commute comparison"), IGNORE the history completely.
22. JOINS: If a question requires data from MULTIPLE views (e.g. "states with highest commute AND highest rent burden"), you MAY JOIN views on the STATE column. Only JOIN views listed above. Set "view" to a comma-separated list of all views used.
23. HONESTY: If you are NOT confident you can answer the question accurately with the available views and columns, return {{"mode": "clarify", "explanation": "reason"}}. NEVER guess or fabricate data. It is better to say you can't answer than to give a wrong answer.
24. COUNTY NAMES: The COUNTY column stores full names including the suffix (e.g. 'King County', 'Travis County', 'Miami-Dade County'). When filtering by county, always use COUNTY LIKE '%King%' or the full name 'King County'. NEVER use just 'King' without the suffix.
25. CROSS-YEAR COMPARISONS: When comparing 2019 vs 2020 data, use separate CTEs for each year and join them. In the SELECT of each CTE, use the ORIGINAL column names from the view catalog (e.g. avg_commute_minutes, inflow_any_rate). Only apply aliases in the final outer SELECT. Do NOT invent new column names inside CTEs.

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


def generate_sql(session, user_text: str, year: int = 2020, chat_history: str = "") -> Dict[str, Any]:
    """
    Ask the LLM to write a SQL query for the user's question.

    Returns a dict with keys: mode, sql, year, view, explanation.
    Raises on LLM/network failure (caller should catch and fall back).
    """
    import re

    # If the user is explicitly asking for a comparison (e.g., clicking the sidebar button
    # "Show 2019 vs 2020 commute comparison"), we must isolate the prompt.
    # LLMs struggle to ignore chat history even when instructed to.
    is_compare = bool(re.search(
        r"(?i)\b(compare|comparison|vs\.?|versus|side.by.side|"
        r"2019\s*(and|&|vs\.?)\s*2020)\b",
        user_text
    ))

    # Force empty history for comparisons to guarantee no context bleeding
    if is_compare:
        chat_history = ""

    history_block = ""
    if chat_history:
        history_block = (
            "\n<conversation_history>\n"
            f"{chat_history}\n"
            "</conversation_history>\n"
        )

    prompt = f"""{_SYSTEM_PROMPT}

<user_question>
{user_text}
</user_question>

PREFERRED YEAR: {year}
{history_block}
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

    parsed = json.loads(cleaned, strict=False)
    
    # Auto-append LIMIT if the LLM forgot it, to prevent the strict local
    # validator from rejecting massive but otherwise perfectly valid CTE queries.
    if "sql" in parsed and parsed["sql"]:
        sql = parsed["sql"].strip()
        
        # Check if the query already has a LIMIT clause using regex
        # This prevents appending to queries that end in "LIMIT 1" or "LIMIT 1;"
        if not re.search(r'\blimit\s+\d+', sql.lower()):
            sql = sql.rstrip(";").rstrip() + " LIMIT 15"
            parsed["sql"] = sql

    return parsed
