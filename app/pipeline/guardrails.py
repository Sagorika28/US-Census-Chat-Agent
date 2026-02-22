"""
Guardrails: NSFW filter, on-topic gating, affirmation detection,
and capability-aware request detection.

All functions are pure (no Streamlit session-state access) so they
can be unit-tested without a running Streamlit app.
"""

import re
from typing import Any, Dict, Optional

from app.config import NSFW_PATTERNS, AFFIRMATIONS
from app.helpers.policies.year_policy import is_year_followup


def is_nsfw(text: str) -> bool:
    """Return True if *text* matches any NSFW regex pattern."""
    t = text.lower()
    return any(re.search(pat, t) for pat in NSFW_PATTERNS)


def is_affirmation(text: str) -> bool:
    """Return True if *text* is a simple affirmative reply."""
    return text.lower().strip() in AFFIRMATIONS


from app.config import BASELINE_TOPIC, EMBEDDING_MODEL

# Threshold for semantic match (0-1). 
# >0.4 is generally a safe cutoff for "snowflake-arctic-embed-m" 
# to distinguish census/demographic intent from random chatter.
SIMILARITY_THRESHOLD = 0.4


def is_on_topic(
    session,
    text: str, 
    pending_clarification: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Return True if *text* is semantically relevant to the census domain.
    Uses Snowflake Cortex VECTOR_COSINE_SIMILARITY to compare the user's
    message against a baseline demographics concept.

    Special cases handled BEFORE vector matching:
    1. Numeric choices ("1" / "2") when a choose_alt clarification is
       pending.
    2. Affirmative replies when *any* clarification is pending.
    3. Year follow-ups ("show 2019", "compare both years").
    """
    t = text.lower().strip()

    # Allow numeric choices for choose_alt
    if (
        isinstance(pending_clarification, dict)
        and pending_clarification.get("type") == "choose_alt"
        and t in {"1", "2"}
    ):
        return True

    # Allow affirmations when a clarification is pending
    if pending_clarification and is_affirmation(t):
        return True

    # Allow year follow-ups
    if is_year_followup(t) is not None:
        return True

    # Vector Semantic Search Check
    # We escape single quotes in the user text to prevent SQL injection in the Cortex call.
    safe_text = text.replace("'", "''")
    safe_baseline = BASELINE_TOPIC.replace("'", "''")
    
    sql = f"""
    SELECT VECTOR_COSINE_SIMILARITY(
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('{EMBEDDING_MODEL}', '{safe_text}'),
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('{EMBEDDING_MODEL}', '{safe_baseline}')
    ) as similarity
    """
    
    try:
        # If the query fails (e.g., model unavailable), we default to True
        # to fail open and let the subsequent LLM call decide if it can answer.
        result = session.sql(sql).collect()
        score = float(result[0]["SIMILARITY"])
        return score >= SIMILARITY_THRESHOLD
    except Exception as e:
        print(f"Warning: Cortex embedding check failed ({e}). Defaulting to True.")
        return True


def is_person_language_by_sex_request(text: str) -> bool:
    """
    Detect questions like "female non-English speaking population".

    The language view (C16002) is household-based, not person-by-sex,
    so we must intercept and offer supported alternatives.
    """
    t = text.lower()
    sex_words = any(w in t for w in ["female", "male", "women", "men", "gender", "sex"])
    lang_words = any(w in t for w in [
        "non-english", "english", "spanish", "language", "limited english",
    ])
    pop_words = any(w in t for w in ["population", "people", "persons", "residents"])
    return sex_words and lang_words and pop_words
