"""
Tests for helpers.policies.year_policy
"""

import unittest

from app.helpers.policies.year_policy import is_year_followup, detect_year_mode


class TestIsYearFollowup(unittest.TestCase):
    def test_single_2019(self):
        self.assertEqual(is_year_followup("2019"), "2019")
        self.assertEqual(is_year_followup("show 2019"), "2019")
        self.assertEqual(is_year_followup("show 2019 too"), "2019")
        self.assertEqual(is_year_followup("show 2019 also"), "2019")

    def test_single_2020(self):
        self.assertEqual(is_year_followup("2020"), "2020")
        self.assertEqual(is_year_followup("show 2020 also"), "2020")

    def test_compare(self):
        self.assertEqual(is_year_followup("compare"), "compare")
        self.assertEqual(is_year_followup("both years"), "compare")
        self.assertEqual(is_year_followup("2019 and 2020"), "compare")
        self.assertEqual(is_year_followup("2019 vs 2020"), "compare")

    def test_not_year_followup(self):
        self.assertIsNone(is_year_followup("show me rent burden"))
        self.assertIsNone(is_year_followup("yes"))
        self.assertIsNone(is_year_followup("1"))


class TestDetectYearMode(unittest.TestCase):
    def test_explicit_year(self):
        self.assertEqual(detect_year_mode("rent burden in 2019"), ("single", 2019))
        self.assertEqual(detect_year_mode("commute 2020"), ("single", 2020))

    def test_latest(self):
        self.assertEqual(detect_year_mode("show me the latest data"), ("single", 2020))
        self.assertEqual(detect_year_mode("most recent"), ("single", 2020))

    def test_compare(self):
        self.assertEqual(detect_year_mode("compare rent burden over time"), ("compare", None))
        self.assertEqual(detect_year_mode("rent trend"), ("compare", None))
        self.assertEqual(detect_year_mode("2019 vs 2020 commute"), ("compare", None))

    def test_no_year_signal(self):
        self.assertEqual(detect_year_mode("top states by rent burden"), ("single", None))


if __name__ == "__main__":
    unittest.main()