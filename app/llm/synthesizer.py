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
    chat_history: str = "",
) -> str:
    """
    Generate a natural-language answer from query results.

    Returns a concise answer with key findings and caveats.
    Falls back to a simple one-liner if the LLM fails.
    """
    if df is None or df.empty:
        return f"No results found for your query. Try adjusting your filters."

    # Prepare a text table of results (top 10 rows max)
    results_text = df.head(10).to_string(index=False)

    history_block = ""
    if chat_history:
        history_block = f"\nCONVERSATION HISTORY (recent exchanges):\n{chat_history}\n"

    prompt = f"""You are a data analyst explaining US Census results to a non-technical user.
{history_block}
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
5. If the user asked a multi-part or compound question, make sure you address EVERY part explicitly. For example, if they asked "X and by how much more than Y", calculate and state the difference. If they reference previous results ("the 2nd position", "that state"), use CONVERSATION HISTORY to resolve what they mean.

Be concise and factual. Only reference numbers that appear in the results above. Do not add any numbers not present in RESULTS. If unsure, say you can't tell from the table.
If the RESULTS are empty or missing, say 'No rows returned' and suggest a next query.
Do NOT use markdown headers. Use plain text with bullet points (- ).

STRICT TONE RULES (CRITICAL):
- NO EMOJIS under any circumstances.
- NO AI BUZZWORDS like "delve", "explore", "uncover", "here is", "in this data".
- NO em-dashes (—). If you need a dash, use a single standard hyphen (-).
- NO long dashed lines (---- or ____).
- Speak plainly and directly like a human analyst.
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
    cols_lower = [c.lower() for c in df.columns]

    # Try to produce a meaningful summary based on available columns (checking both raw names and aliases)
    if "pct_over_30" in cols_lower or any("rent" in c for c in cols_lower):
        county_col = next((c for c in df.columns if c.lower() == "county"), "?")
        state_col = next((c for c in df.columns if c.lower() == "state"), "?")
        val_col = next((c for c in df.columns if "30" in c.lower()), "?")
        val = r.get(val_col, 0)
        
        return (
            f"Highest rent-burden area in {year}: "
            f"{r.get(county_col, '?')}, {r.get(state_col, '?')} - "
            f"{float(val):.1%} of renters pay 30%+ on rent."
        )
        
    if "avg_commute_minutes" in cols_lower or any("commute" in c for c in cols_lower):
        state_col = next((c for c in df.columns if c.lower() == "state"), "?")
        val_col = next((c for c in df.columns if "commute" in c.lower() or "minutes" in c.lower()), "?")
        val = r.get(val_col, 0)
        
        return (
            f"Longest average commute in {year}: "
            f"{r.get(state_col, '?')} at "
            f"{float(val):.1f} minutes."
        )
        
    if "inflow_any_total" in cols_lower or any("inflow" in c for c in cols_lower):
        county_col = next((c for c in df.columns if c.lower() == "county"), "?")
        state_col = next((c for c in df.columns if c.lower() == "state"), "?")
        val_col = next((c for c in df.columns if "inflow" in c.lower() and "rate" not in c.lower() and "outside" not in c.lower()), "?")
        val = r.get(val_col, 0)
        
        return (
            f"Top migration destination in {year}: "
            f"{r.get(county_col, '?')}, {r.get(state_col, '?')} - "
            f"{int(val):,} total inflow."
        )
        
    if "pct_non_english" in cols_lower or any("non-english" in c or "english" in c for c in cols_lower):
        state_col = next((c for c in df.columns if c.lower() == "state"), "?")
        val_col = next((c for c in df.columns if "%" in c.lower() or "pct" in c.lower()), "?")
        val = r.get(val_col, 0)
        
        return (
            f"Highest non-English household share in {year}: "
            f"{r.get(state_col, '?')} at "
            f"{float(val):.1%}."
        )

    # Generic fallback for any other state/county level data (e.g. population, education)
    state_col = next((c for c in df.columns if c.lower() == "state"), None)
    county_col = next((c for c in df.columns if c.lower() == "county"), None)
    
    geo_name = []
    if county_col and r.get(county_col): geo_name.append(str(r.get(county_col)))
    if state_col and r.get(state_col): geo_name.append(str(r.get(state_col)))
    
    if geo_name:
        # Find the first column that isn't a geography column
        val_cols = [c for c in df.columns if c.lower() not in ["state", "county"]]
        if val_cols:
            val_col = val_cols[0]
            val = r.get(val_col, 0)
            
            # Format nicely based on value
            if isinstance(val, (int, float)):
                if abs(val) < 2 and val != 0 and val != 1:  # Likely a percentage
                    val_str = f"{float(val):.1%}"
                elif val > 1000:
                    val_str = f"{int(val):,}"
                else:
                    val_str = f"{float(val):.1f}"
            else:
                val_str = str(val)
                
            geo_str = ", ".join(geo_name)
            return f"{val_col} for {geo_str} in {year}: {val_str}."

    return f"Results for {year} shown above."
