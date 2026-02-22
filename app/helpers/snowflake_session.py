"""
Snowflake session helper: works both inside Streamlit-in-Snowflake
and locally via environment variables.

Usage:
    from app.helpers.snowflake_session import get_session
    session = get_session()
"""

import os


def _maybe_load_dotenv():
    """
    Loads .env locally if python-dotenv exists.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


def get_session():
    """
    Return a Snowpark Session.

    - Inside Streamlit-in-Snowflake: uses get_active_session().
    - Locally: creates a Session from SNOWFLAKE_* env vars.
    """
    # 1) Streamlit-in-Snowflake path
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        pass

    # 2) Local path
    _maybe_load_dotenv()

    from snowflake.snowpark import Session

    connection_params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "SNOWFLAKE_LEARNING_DB"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }

    # Optional host override
    host = os.environ.get("SNOWFLAKE_HOST")
    if host:
        connection_params["host"] = host

    # Auth strategy
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR", "").strip().lower()
    if authenticator == "externalbrowser":
        connection_params["authenticator"] = "externalbrowser"
    else:
        connection_params["password"] = os.environ["SNOWFLAKE_PASSWORD"]

    return Session.builder.configs(connection_params).create()