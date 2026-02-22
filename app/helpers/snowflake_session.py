"""
Snowflake session helper: works both inside Streamlit-in-Snowflake
and locally via environment variables.

Usage:
    from app.helpers.snowflake_session import get_session
    session = get_session()
"""

import os

from dotenv import load_dotenv
load_dotenv()  # loads .env if present; no-op in Snowflake Streamlit


def get_session():
    """
    Return a Snowpark Session.

    - Inside Streamlit-in-Snowflake: uses get_active_session().
    - Locally: creates a Session from SNOWFLAKE_* env vars.
    """
    # Try Streamlit-in-Snowflake first
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        pass

    # Local fallback: build session from env vars
    from snowflake.snowpark import Session

    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR", "")

    connection_params = {
        "account": account,
        "user": user,
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "SNOWFLAKE_LEARNING_DB"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }

    # Optional: explicit host override
    host = os.environ.get("SNOWFLAKE_HOST")
    if host:
        connection_params["host"] = host

    # Auth strategy
    if authenticator == "externalbrowser":
        connection_params["authenticator"] = "externalbrowser"
    else:
        connection_params["password"] = os.environ["SNOWFLAKE_PASSWORD"]

    return Session.builder.configs(connection_params).create()
