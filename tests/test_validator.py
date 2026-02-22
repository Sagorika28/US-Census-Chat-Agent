"""
Tests for pipeline.validator — pure Python, no Snowflake session.
"""

import unittest

from app.pipeline.validator import validate_sql, validate_columns
from app.config import ALLOWED_VIEWS


class TestValidateSql(unittest.TestCase):
    """validate_sql must raise ValueError for every violation."""

    # A known-good query to use as a base
    GOOD_SQL = (
        f"SELECT STATE, pct_over_30 "
        f"FROM {ALLOWED_VIEWS[2020]['rent_county']} "
        f"WHERE computed_renters >= 1000 "
        f"ORDER BY pct_over_30 DESC LIMIT 15"
    )

    def test_accepts_valid_select(self):
        validate_sql(self.GOOD_SQL, 2020)  # should not raise

    def test_rejects_non_select(self):
        with self.assertRaises(ValueError):
            validate_sql("INSERT INTO foo VALUES (1)", 2020)

    def test_rejects_drop(self):
        with self.assertRaises(ValueError):
            validate_sql(
                f"SELECT drop FROM {ALLOWED_VIEWS[2020]['rent_county']} LIMIT 1",
                2020,
            )

    def test_rejects_information_schema(self):
        with self.assertRaises(ValueError):
            validate_sql(
                "SELECT * FROM information_schema.tables LIMIT 10",
                2020,
            )

    def test_rejects_comments(self):
        with self.assertRaises(ValueError):
            validate_sql(
                f"SELECT STATE -- hack\n"
                f"FROM {ALLOWED_VIEWS[2020]['rent_county']} LIMIT 1",
                2020,
            )

    def test_rejects_multi_statement(self):
        with self.assertRaises(ValueError):
            validate_sql(
                f"SELECT 1 FROM {ALLOWED_VIEWS[2020]['rent_county']} LIMIT 1;"
                f"DROP TABLE foo",
                2020,
            )

    def test_rejects_non_approved_view(self):
        with self.assertRaises(ValueError):
            validate_sql(
                "SELECT * FROM some_random_table LIMIT 10",
                2020,
            )

    def test_rejects_missing_limit(self):
        with self.assertRaises(ValueError):
            validate_sql(
                f"SELECT STATE FROM {ALLOWED_VIEWS[2020]['rent_county']}",
                2020,
            )


class TestValidateColumns(unittest.TestCase):
    """validate_columns must catch hallucinated column names."""

    RENT_VIEW = ALLOWED_VIEWS[2020]["rent_county"]

    def test_accepts_valid_columns(self):
        sql = f"SELECT STATE, COUNTY, pct_over_30 FROM {self.RENT_VIEW} LIMIT 15"
        validate_columns(sql)  # should not raise

    def test_accepts_star(self):
        sql = f"SELECT * FROM {self.RENT_VIEW} LIMIT 15"
        validate_columns(sql)  # should not raise

    def test_rejects_hallucinated_column(self):
        sql = f"SELECT STATE, pct_over40 FROM {self.RENT_VIEW} LIMIT 15"
        with self.assertRaises(ValueError):
            validate_columns(sql)

    def test_rejects_invented_column(self):
        sql = f"SELECT STATE, median_income FROM {self.RENT_VIEW} LIMIT 15"
        with self.assertRaises(ValueError):
            validate_columns(sql)

    def test_accepts_migration_columns(self):
        mig_view = ALLOWED_VIEWS[2020]["migration_county"]
        sql = (
            f"SELECT STATE, COUNTY, inflow_any_total, inflow_outside_total "
            f"FROM {mig_view} LIMIT 15"
        )
        validate_columns(sql)  # should not raise


if __name__ == "__main__":
    unittest.main()