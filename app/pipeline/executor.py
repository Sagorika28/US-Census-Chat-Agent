"""
Executor: runs validated SQL against Snowflake via a Snowpark session.
"""

import pandas as pd


def run_query(session, sql: str) -> pd.DataFrame:
    """
    Execute *sql* through *session* and return a pandas DataFrame.

    The session is injected by the UI layer (``get_active_session()``).
    """
    return session.sql(sql).to_pandas()
