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
    "truncate", "execute", "commit", "rollback",
    "describe", "show"
]


def validate_sql(sql: str, year: int) -> None:
    """
    Raise ValueError if *sql* violates any safety rule for *year*.

    This function is intentionally strict: it is the last gate before
    a query reaches Snowflake.
    """
    s = sql.strip().lower()

    if not (s.startswith("select") or s.startswith("with")):
        raise ValueError("Only SELECT queries (including CTEs) are allowed.")

    if "information_schema" in s or "account_usage" in s:
        raise ValueError("Metadata access is not allowed.")

    if "--" in s or "/*" in s or "*/" in s or ";" in s:
        raise ValueError("Comments and multi-statements are not allowed.")

    if "system$" in s:
        raise ValueError("System functions are not allowed.")

    pattern = r"\b(" + "|".join(_BANNED_KEYWORDS) + r")\b"
    if re.search(pattern, s):
        raise ValueError("DDL/DML is not allowed.")

    # At least one approved view must appear in the query.
    # Check BOTH years since cross-year CTEs reference 2019 and 2020 views.
    allowed = set()
    for yr in ALLOWED_VIEWS:
        allowed |= set(ALLOWED_VIEWS[yr].values())
    if not any(v.lower() in s for v in allowed):
        raise ValueError("Query references a non-approved object.")

    if not re.search(r'\blimit\s+\d+', s):
        raise ValueError("LIMIT is required.")


def validate_columns(sql: str) -> None:
    """
    Raise ValueError if the SQL references columns not in the view's catalog.

    This catches the #1 text-to-SQL failure mode: hallucinated column names
    like 'pct_over40' instead of 'pct_over_40'.

    Handles simple queries AND CTEs / subqueries by scanning every
    SELECT...FROM block in the SQL.
    """
    s = sql.strip().lower()

    # Strip all double-quoted identifiers (aliases) before scanning.
    # Aliases like "Average Commute (Mins)" are NOT real column names
    # and must be invisible to column validation.
    s = re.sub(r'"[^"]*"', '', s)

    # Find ALL catalog views referenced (supports JOINs across views)
    matched_views = []
    for view_name in VIEW_CATALOG:
        if view_name.lower() in s:
            matched_views.append(view_name)

    if not matched_views:
        # No catalog match - validate_sql already checks approved views
        return

    # Union of valid columns from all matched views
    valid_cols: set = set()
    for v in matched_views:
        valid_cols |= VIEW_CATALOG[v]

    # Dynamically extract all query-defined aliases (CTE names, column aliases).
    # Critical Security Note: We must ONLY extract the alias name, never the source expression.
    cte_aliases = re.findall(r"\b([a-z_][a-z0-9_]*)\s+as\s*\(", s)
    col_aliases = re.findall(r"\bas\s+([a-z_][a-z0-9_]*)\b", s)

    defined_aliases = cte_aliases + col_aliases
    for alias in defined_aliases:
        valid_cols.add(alias)

    # Find ALL "SELECT ... FROM" blocks (handles CTEs, subqueries)
    select_blocks = re.findall(r"select\s+(.+?)\s+from\s+", s, re.DOTALL)
    if not select_blocks:
        return

    # Extended skip-list: SQL keywords, functions, window functions, CTE syntax
    _SQL_KEYWORDS = {
        # Math & Aggregates
        "sum", "count", "avg", "min", "max", "round", "abs", "greatest", "least",
        "ceil", "ceiling", "floor", "power", "sqrt", "mod", "trunc", "truncate",
        "stddev", "variance", "median", "mode",
        # String
        "concat", "substring", "substr", "trim", "upper", "lower", "length", 
        "replace", "split", "coalesce", "nullif", "iff", "nvl",
        # Conditionals & Types
        "case", "when", "then", "else", "end", "and", "or",
        "not", "null", "is", "in", "between", "like", "ilike", "cast", "try_cast",
        "float", "int", "integer", "varchar", "number", "numeric", "decimal", "string", "boolean", "date",
        # Query Structure
        "distinct", "desc", "asc", "limit",
        # Window functions & CTE syntax
        "row_number", "rank", "dense_rank", "over", "partition", "by",
        "as", "with", "offset", "union", "all",
        # Clauses used inside subqueries
        "from", "where", "order", "group", "having",
        "join", "on", "left", "right", "inner", "outer", "cross",
        # Common CTE aliases
        "rn", "ranked", "cte", "sub", "t1", "t2",
        # Aggregation misc
        "rows", "preceding", "following", "unbounded", "current", "row",
        "select", "top",
    }

    for select_part in select_blocks:
        # Handle SELECT *
        if select_part.strip() == "*":
            continue

        # Parse individual column references
        for token in select_part.split(","):
            token = token.strip()

            # Remove anything after ' as ' (the alias part)
            if " as " in token:
                token = token.split(" as ")[0]

            # Remove table aliases like 'l.', 'c.', 't1.'
            token = re.sub(r'\b[a-z_][a-z0-9_]*\.', '', token)

            # Extract identifiers that look like column names
            col_names = re.findall(r"\b([a-z_][a-z0-9_]*)\b", token)
            for col in col_names:
                if col in _SQL_KEYWORDS:
                    continue
                # Check if this column exists in the view
                if col not in valid_cols:
                    raise ValueError(
                        f"Column '{col}' not found in {', '.join(matched_views)}. "
                        f"Valid columns: {', '.join(sorted(valid_cols))}"
                    )
