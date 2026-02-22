"""
Tests for pipeline.guardrails — pure Python, no Snowflake session.
"""

import unittest

from app.pipeline.guardrails import (
    is_nsfw,
    is_affirmation,
    is_on_topic,
    is_person_language_by_sex_request,
)


class TestIsNsfw(unittest.TestCase):
    def test_catches_nsfw_words(self):
        for word in ["porn", "nude", "xxx", "explicit"]:
            self.assertTrue(is_nsfw(f"Show me {word} content"), word)

    def test_passes_clean_text(self):
        self.assertFalse(is_nsfw("What is the average commute in Texas?"))
        self.assertFalse(is_nsfw("Top states by rent burden"))

    def test_case_insensitive(self):
        self.assertTrue(is_nsfw("PORN"))
        self.assertTrue(is_nsfw("Nude photos"))


class TestIsAffirmation(unittest.TestCase):
    def test_affirmative_replies(self):
        for reply in ["yes", "y", "sure", "ok", "okay", "go ahead", "do it"]:
            self.assertTrue(is_affirmation(reply), reply)

    def test_non_affirmative(self):
        self.assertFalse(is_affirmation("no"))
        self.assertFalse(is_affirmation("tell me about rent"))


from unittest.mock import MagicMock

class TestIsOnTopic(unittest.TestCase):
    def setUp(self):
        self.mock_session = MagicMock()
        
        # Helper to set the similarity score the mock session will return
        def set_similarity(score):
            mock_collect = MagicMock()
            mock_collect.return_value = [{"SIMILARITY": score}]
            self.mock_session.sql.return_value.collect = mock_collect
            
        self.set_similarity = set_similarity

    def test_semantic_match_passes(self):
        self.set_similarity(0.85)
        self.assertTrue(is_on_topic(self.mock_session, "Show me rent burden by county"))
        
        self.set_similarity(0.70)
        self.assertTrue(is_on_topic(self.mock_session, "what is the mean income of males"))

    def test_rejects_random_text(self):
        self.set_similarity(0.15)
        self.assertFalse(is_on_topic(self.mock_session, "What is the weather today?"))
        
        self.set_similarity(0.05)
        self.assertFalse(is_on_topic(self.mock_session, "Tell me a joke"))

    def test_numeric_choice_allowed_with_choose_alt(self):
        """Numeric '1'/'2' replies must pass when choose_alt is pending."""
        pending = {"type": "choose_alt", "alts": ["a", "b"]}
        self.assertTrue(is_on_topic(self.mock_session, "1", pending))
        self.assertTrue(is_on_topic(self.mock_session, "2", pending))

    def test_numeric_choice_rejected_without_pending(self):
        """Bare '1'/'2' without pending should be off-topic."""
        self.set_similarity(0.10)
        self.assertFalse(is_on_topic(self.mock_session, "1"))
        self.assertFalse(is_on_topic(self.mock_session, "2"))

    def test_affirmation_allowed_with_any_pending(self):
        pending = {"mode": "answer", "topic": "migration"}
        self.assertTrue(is_on_topic(self.mock_session, "yes", pending))
        self.assertTrue(is_on_topic(self.mock_session, "sure", pending))

    def test_year_followup_always_on_topic(self):
        self.assertTrue(is_on_topic(self.mock_session, "show 2019"))
        self.assertTrue(is_on_topic(self.mock_session, "2020"))
        self.assertTrue(is_on_topic(self.mock_session, "compare both years"))


class TestIsPersonLanguageBySex(unittest.TestCase):
    def test_detects_female_nonenglish(self):
        self.assertTrue(is_person_language_by_sex_request(
            "female non-English speaking population"
        ))
        self.assertTrue(is_person_language_by_sex_request(
            "How many women in the population speak limited English?"
        ))

    def test_passes_plain_language_query(self):
        self.assertFalse(is_person_language_by_sex_request(
            "Top states by non-English households"
        ))

    def test_passes_plain_population_query(self):
        self.assertFalse(is_person_language_by_sex_request(
            "female population by state"
        ))


if __name__ == "__main__":
    unittest.main()