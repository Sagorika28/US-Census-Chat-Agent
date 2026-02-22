"""
Answer Synthesizer: uses a lightweight LLM to produce a natural-language
answer from query results.

Falls back to a deterministic one-liner if the LLM call fails.
"""

from typing import Optional

import pandas as pd

from app.config import SUMMARY_MODEL


def _cortex_complete(session, model: str, prompt: str) -> str:
    """Call Snowflake Cortex COMPLETE and return the raw response text."""
    sql = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
      '{model}',
      $$ {prompt} $$
    ) AS response
    """
    return session.sql(sql).collect()[0]["RESPONSE"]


def synthesize_answer(
    session,
    user_question: str,
    sql_used: str,
    view_name: str,
    df: pd.DataFrame,
    year: int,
) -> str:
    """
    Generate a natural-language answer from query results.

    Returns a concise answer with key findings and caveats.
    Falls back to a simple one-liner if the LLM fails.
    """
    if df is None or df.empty:
        return f"No results found for {year}."

    # Prepare a text table of results (top 10 rows max)
    results_text = df.head(10).to_string(index=False)

    prompt = f"""You are a data analyst explaining US Census results to a non-technical user.

USER QUESTION: {user_question}
YEAR: {year}
SOURCE VIEW: {view_name}
SQL USED: {sql_used}

RESULTS (top rows):
{results_text}

Write a response with:
1. A 1-2 sentence direct answer to the question in the first sentence, including the year and the metric used (e.g., pct_over_30, avg_commute_minutes, inflow_any_total). Cite specific numbers.
2. A short bullet list of 3-5 key findings from the data.
3. Any important caveats (e.g. "counties include independent cities", "language data is household-based, not person-level").
4. A one-line "Source: {view_name}" at the end.

Be concise and factual. Only reference numbers that appear in the results above. Do not add any numbers not present in RESULTS. If unsure, say you can't tell from the table.
If the RESULTS are empty or missing, say 'No rows returned' and suggest a next query.
Do NOT use markdown headers. Use plain text with bullet points (- ).
"""

    try:
        return _cortex_complete(session, SUMMARY_MODEL, prompt)
    except Exception:
        # Deterministic fallback: simple one-liner from top row
        return _fallback_summary(df, year)


def _fallback_summary(df: pd.DataFrame, year: int) -> str:
    """Deterministic one-line summary from the top row (no LLM needed)."""
    if df.empty:
        return f"No results found for {year}."

    r = df.iloc[0]
    cols = [c.lower() for c in df.columns]

    # Try to produce a meaningful summary based on available columns
    if "pct_over_30" in cols:
        return (
            f"Highest rent-burden area in {year}: "
            f"{r.get('COUNTY', r.get('county', '?'))}, {r.get('STATE', r.get('state', '?'))} — "
            f"{float(r.get('PCT_OVER_30', r.get('pct_over_30', 0))):.1%} of renters pay 30%+ on rent."
        )
    if "avg_commute_minutes" in cols:
        return (
            f"Longest average commute in {year}: "
            f"{r.get('STATE', r.get('state', '?'))} at "
            f"{float(r.get('AVG_COMMUTE_MINUTES', r.get('avg_commute_minutes', 0))):.1f} minutes."
        )
    if "inflow_any_total" in cols:
        return (
            f"Top migration destination in {year}: "
            f"{r.get('COUNTY', r.get('county', '?'))}, {r.get('STATE', r.get('state', '?'))} — "
            f"{int(r.get('INFLOW_ANY_TOTAL', r.get('inflow_any_total', 0))):,} total inflow."
        )
    if "pct_non_english" in cols:
        return (
            f"Highest non-English household share in {year}: "
            f"{r.get('STATE', r.get('state', '?'))} at "
            f"{float(r.get('PCT_NON_ENGLISH', r.get('pct_non_english', 0))):.1%}."
        )

    return f"Results for {year} shown above."
