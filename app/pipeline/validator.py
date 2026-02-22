"""
SQL validator: enforces safety rules before any query hits Snowflake.

Rules:
- SELECT-only (no DDL/DML)
- No comments / multi-statements
- No metadata schema access
- Must reference an approved view
- Must include LIMIT
- All referenced columns must exist in the view's catalog
"""

import re

from app.config import ALLOWED_VIEWS, VIEW_CATALOG

_BANNED_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter",
    "create", "merge", "grant", "revoke", "call",
]


def validate_sql(sql: str, year: int) -> None:
    """
    Raise ValueError if *sql* violates any safety rule for *year*.

    This function is intentionally strict: it is the last gate before
    a query reaches Snowflake.
    """
    s = sql.strip().lower()

    if not s.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")

    if "information_schema" in s or "account_usage" in s:
        raise ValueError("Metadata access is not allowed.")

    if "--" in s or "/*" in s or "*/" in s or ";" in s:
        raise ValueError("Comments and multi-statements are not allowed.")

    if any(b in s for b in _BANNED_KEYWORDS):
        raise ValueError("DDL/DML is not allowed.")

    # At least one approved view must appear in the query
    allowed = set(ALLOWED_VIEWS.get(year, {}).values())
    if not any(v.lower() in s for v in allowed):
        raise ValueError("Query references a non-approved object.")

    if " limit " not in s:
        raise ValueError("LIMIT is required.")


def validate_columns(sql: str) -> None:
    """
    Raise ValueError if the SQL references columns not in the view's catalog.

    This catches the #1 text-to-SQL failure mode: hallucinated column names
    like 'pct_over40' instead of 'pct_over_40'.
    """
    s = sql.strip().lower()

    # Find which catalog view is referenced
    matched_view = None
    for view_name, valid_cols in VIEW_CATALOG.items():
        if view_name.lower() in s:
            matched_view = view_name
            break

    if matched_view is None:
        # No catalog match — validate_sql already checks approved views
        return

    valid_cols = VIEW_CATALOG[matched_view]

    # Extract column names from SELECT clause (before FROM)
    select_match = re.match(r"select\s+(.+?)\s+from\s+", s, re.DOTALL)
    if not select_match:
        return

    select_part = select_match.group(1)

    # Handle SELECT *
    if select_part.strip() == "*":
        return

    # Parse individual column references
    # Split on commas, then extract the base column name
    for token in select_part.split(","):
        token = token.strip()
        
        # Remove anything after ' as ' or ' AS ' (the alias part)
        # This prevents "Friendly Name" from being parsed as columns "friendly" and "name".
        if " as " in token:
            token = token.split(" as ")[0]

        # Extract identifiers that look like column names
        col_names = re.findall(r"\b([a-z_][a-z0-9_]*)\b", token)
        for col in col_names:
            # Skip SQL keywords and functions
            if col in {
                "sum", "count", "avg", "min", "max", "round",
                "case", "when", "then", "else", "end", "and", "or",
                "not", "null", "is", "in", "between", "like", "cast",
                "float", "int", "integer", "varchar", "number",
                "nullif", "coalesce", "distinct", "desc", "asc", "limit"
            }:
                continue
            # Check if this column exists in the view
            if col not in valid_cols:
                raise ValueError(
                    f"Column '{col}' not found in {matched_view}. "
                    f"Valid columns: {', '.join(sorted(valid_cols))}"
                )
