"""
Tests for pipeline.router — pure Python, no Snowflake session.
"""

import unittest

from app.pipeline.router import route_topic_geo, wants_top_two_lists


class TestRouteTopicGeo(unittest.TestCase):
    # Core four topics
    def test_rent(self):
        self.assertEqual(route_topic_geo("Where is rent burden highest?"), ("rent", "county"))
        self.assertEqual(route_topic_geo("gross rent by county"), ("rent", "county"))

    def test_commute(self):
        self.assertEqual(route_topic_geo("longest commute times"), ("commute", "state"))
        self.assertEqual(route_topic_geo("travel time to work"), ("commute", "state"))

    def test_migration(self):
        self.assertEqual(route_topic_geo("migration inflow by county"), ("migration", "county"))

    def test_migration_city_request(self):
        self.assertEqual(
            route_topic_geo("Which cities have highest migration?"),
            ("migration_city_request", "county"),
        )

    def test_language(self):
        self.assertEqual(route_topic_geo("non-english households"), ("language", "state"))
        self.assertEqual(route_topic_geo("limited english states"), ("language", "state"))

    #  Broader topics
    def test_population(self):
        self.assertEqual(route_topic_geo("male vs female population"), ("population", "state"))

    def test_tenure(self):
        self.assertEqual(route_topic_geo("tenure by state"), ("tenure", "state"))

    def test_labor(self):
        self.assertEqual(route_topic_geo("unemployment by state"), ("labor", "state"))

    def test_race(self):
        self.assertEqual(route_topic_geo("race composition"), ("race", "state"))

    def test_hispanic(self):
        self.assertEqual(route_topic_geo("latino community"), ("hispanic", "state"))

    def test_education(self):
        self.assertEqual(route_topic_geo("bachelor degree rates"), ("education", "state"))

    # Fallback
    def test_fallback(self):
        self.assertEqual(route_topic_geo("something unknown"), ("rent", "county"))


class TestWantsTopTwoLists(unittest.TestCase):
    def test_top(self):
        self.assertTrue(wants_top_two_lists("top counties by migration"))
        self.assertTrue(wants_top_two_lists("highest inflow"))
        self.assertTrue(wants_top_two_lists("rank states"))

    def test_no_ranking(self):
        self.assertFalse(wants_top_two_lists("show me migration for Texas"))


if __name__ == "__main__":
    unittest.main()