"""
Explainer: generates a deterministic one-line summary of query results.

No LLM call (the summary is built from the top row of the DataFrame).
"""

import pandas as pd


def one_line_summary(topic: str, year: int, df: pd.DataFrame) -> str:
    """
    Return a human-readable one-liner describing the top result for *topic*.
    """
    if df is None or df.empty:
        return f"No results found for {year}."

    r = df.iloc[0]

    if topic == "rent":
        return (
            f"Highest rent-burden areas in {year} (county/county-equivalent): "
            f"{r['COUNTY']}, {r['STATE']} has "
            f"{float(r['PCT_OVER_30']):.1%} of computed renters paying 30%+ of income on rent."
        )
    if topic == "commute":
        return (
            f"Longest estimated average commutes in {year}: "
            f"{r['STATE']} is highest at "
            f"{float(r['AVG_COMMUTE_MINUTES']):.1f} minutes."
        )
    if topic == "migration":
        return (
            f"Top destinations by inflow in {year} (county/county-equivalent): "
            f"{r['COUNTY']}, {r['STATE']} has "
            f"{int(r['INFLOW_ANY_TOTAL']):,} inflow (Metric A) and "
            f"{int(r['INFLOW_OUTSIDE_TOTAL']):,} outside-area inflow (Metric B)."
        )
    if topic == "language":
        return (
            f"Top states by share of non-English households in {year}: "
            f"{r['STATE']} is highest at "
            f"{float(r['PCT_NON_ENGLISH']):.1%} of households."
        )

    return f"Results for {topic} in {year}."
