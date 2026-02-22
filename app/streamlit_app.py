"""
US Census Chat Agent - Streamlit UI orchestrator.

This is the entry point deployed to Snowflake Streamlit.
All logic lives in pipeline/, llm/, and helpers/.

Pipeline:  User -> Guardrails -> SQL Agent (LLM) -> Validator -> Execute
           -> Answer Synthesizer (LLM) -> UI

Fallback:  If the LLM's SQL fails validation, the deterministic
           router + compiler produces a guaranteed-valid query.
"""

import copy
import re
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st

# Internal imports
from app.config import DEFAULT_YEAR
from app.helpers.snowflake_session import get_session
from app.helpers.conversation import (
    init_session_state,
    get_messages,
    append_message,
    get_pending,
    set_pending,
    clear_pending,
    get_last_spec,
    set_last_spec,
    get_last_question,
    set_last_question,
)
from app.helpers.query_spec import normalize_spec
from app.helpers.policies.year_policy import is_year_followup, detect_year_mode
from app.helpers.policies.migration_policy import (
    MIGRATION_CLARIFICATION_MSG,
    MIGRATION_INFO_MSG,
    LANGUAGE_INFO_MSG,
    CHOOSE_ALT_MSG,
    CHOOSE_ALT_PENDING,
    build_migration_city_pending_spec,
)
from app.pipeline.guardrails import (
    is_nsfw,
    is_affirmation,
    is_on_topic,
    is_person_language_by_sex_request,
)
from app.pipeline.router import route_topic_geo
from app.pipeline.compiler import compile_sql
from app.pipeline.validator import validate_sql, validate_columns
from app.pipeline.executor import run_query
from app.llm.sql_agent import generate_sql
from app.llm.synthesizer import synthesize_answer


# Snowpark session
session = get_session()


# Core query execution: LLM path with deterministic fallback

def _run_llm_path(user_msg: str, year: int):
    """
    Try the LLM text-to-SQL path. Returns (df, sql, view, explanation)
    or raises on failure so the caller can fall back.
    """
    result = generate_sql(session, user_msg, year)

    mode = result.get("mode", "sql")
    if mode == "refuse":
        return "refuse", None, None, None
    if mode == "clarify":
        return "clarify", None, None, result.get("explanation", "Can you be more specific?")

    sql = result.get("sql", "")
    llm_year = result.get("year", year)
    view = result.get("view", "")
    explanation = result.get("explanation", "")

    # Validate the LLM's SQL (raises ValueError if bad)
    validate_sql(sql, llm_year)
    validate_columns(sql)

    df = run_query(session, sql)
    return "ok", df, sql, view


def _run_fallback_path(user_msg: str, year: int):
    """
    Deterministic fallback: router + compiler. Always produces valid SQL.
    """
    route_topic, route_geo = route_topic_geo(user_msg)
    from app.llm.planner import plan_query_filters_only
    try:
        planned = plan_query_filters_only(session, user_msg)
    except Exception:
        planned = {"mode": "answer", "filters": {}, "order_by": {}, "limit": 15}
    spec = normalize_spec(planned, route_topic, route_geo)
    sql, src_view, src_cols = compile_sql(spec, year)
    validate_sql(sql, year)
    df = run_query(session, sql)
    return df, sql, src_view, spec


def _execute_query(user_msg: str, year: int):
    """
    Try the LLM path first, fall back to deterministic if it fails.
    Returns (status, df, sql, view_or_explanation).
    """
    try:
        status, df, sql, view = _run_llm_path(user_msg, year)
        if status in ("refuse", "clarify"):
            return status, None, None, view
        return "ok", df, sql, view
    except Exception:
        # LLM failed — use deterministic fallback
        df, sql, src_view, spec = _run_fallback_path(user_msg, year)
        return "ok", df, sql, src_view


# Rendering helpers

def _render_results(user_msg, df, sql, view, year, year_note, topic=None):
    """Render query results and return the answer text for chat history."""
    st.markdown(year_note)

    # LLM answer synthesis
    answer = synthesize_answer(session, user_msg, sql, view, df, year)
    st.markdown(answer)

    st.dataframe(df, width="stretch")
    st.caption(f"Source: `{view}`")

    # Topic-specific info banners
    if topic == "migration" or (view and "migration" in view.lower()):
        st.info(MIGRATION_INFO_MSG)
    if topic == "language" or (view and "language" in view.lower()):
        st.info(LANGUAGE_INFO_MSG)

    # Build persistent chat text: answer + markdown table of top rows
    table_md = df.head(10).to_markdown(index=False) if df is not None and not df.empty else ""
    return f"{year_note}\n\n{answer}\n\n{table_md}\n\nSource: `{view}`"


def _render_compare(user_msg, year_note):
    """Side-by-side 2019 vs 2020 comparison. Returns combined answer text."""
    st.markdown(year_note)
    results = {}
    for y in (2019, 2020):
        status, df, sql, view = _execute_query(user_msg, y)
        if status != "ok":
            st.warning(f"Could not generate query for {y}.")
            continue
        results[y] = (df, sql, view)

    if not results:
        st.error("Could not generate queries for comparison.")
        return year_note

    answers = [year_note]
    c1, c2 = st.columns(2)
    for col, y in [(c1, 2019), (c2, 2020)]:
        if y not in results:
            continue
        df, sql, view = results[y]
        with col:
            st.subheader(str(y))
            answer = synthesize_answer(session, user_msg, sql, view, df, y)
            st.markdown(answer)
            st.dataframe(df, width="stretch")
            st.caption(f"Source: `{view}`")
            answers.append(f"**{y}**: {answer}")

    return "\n\n".join(answers)


# Page config + chat history

st.set_page_config(page_title="US Census Chat Agent", layout="wide")
st.title("US Census Chat Agent")
init_session_state()

for m in get_messages():
    with st.chat_message(m["role"]):
        st.markdown(m["content"])


# Chat input + main pipeline

user_msg = st.chat_input(
    "Ask a question about US population "
    "(rent, commute, migration, language, demographics)..."
)

if user_msg:
    append_message("user", user_msg)
    with st.chat_message("user"):
        st.markdown(user_msg)

    with st.chat_message("assistant"):
        pending = get_pending()

        # 0) Numeric choice replies (1/2) — BEFORE any gating
        if (
            pending
            and isinstance(pending, dict)
            and pending.get("type") == "choose_alt"
        ):
            choice = user_msg.strip()
            if choice in ("1", "2"):
                alt = pending["alts"][int(choice) - 1]
                clear_pending()

                if alt == "language_households":
                    spec = {
                        "mode": "answer",
                        "topic": "language",
                        "geo": "state",
                        "filters": {},
                        "order_by": {"field": "pct_non_english", "direction": "DESC"},
                        "limit": 15,
                    }
                else:  # female_population
                    spec = {
                        "mode": "answer",
                        "topic": "population",
                        "geo": "state",
                        "filters": {},
                        "order_by": {"field": "female_pop", "direction": "DESC"},
                        "limit": 15,
                    }

                year = DEFAULT_YEAR
                sql, src_view, src_cols = compile_sql(spec, year)
                validate_sql(sql, year)
                df = run_query(session, sql)
                answer = synthesize_answer(
                    session, user_msg, sql, src_view, df, year,
                )
                st.markdown(f"Using {year}.")
                st.markdown(answer)
                st.dataframe(df, width="stretch")
                st.caption(f"Source: `{src_view}` | Columns: {', '.join(src_cols)}")
                set_last_spec(spec)
                append_message("assistant", "Results shown above.")
                st.stop()

        # 1) NSFW guardrail
        if is_nsfw(user_msg):
            reply = "I can't help with that request."
            st.markdown(reply)
            append_message("assistant", reply)
            st.stop()

        # 1.5) Conversational patterns (before on-topic check)
        t_lower = user_msg.lower().strip()

        # Identity questions (regex handles u/you, can/do variations)
        if re.search(r"who\s+are\s+(you|u)|what\s+(are\s+(you|u)|can\s+(you|u)\s+do|tasks?\s+can\s+(you|u))", t_lower):
            reply = (
                "I'm the **US Census Chat Agent**! I can answer questions about "
                "US population, rent burden, commute times, migration patterns, "
                "language demographics, labor, education, race, and more - "
                "using ACS 5-year estimates for 2019 and 2020.\n\n"
                "Try asking something like:\n"
                "- *Which states have the longest commute?*\n"
                "- *Top counties by rent burden*\n"
                "- *Migration inflow for California*"
            )
            st.markdown(reply)
            append_message("assistant", reply)
            st.stop()

        # Greetings
        if t_lower in {"hi", "hello", "hey", "sup", "yo", "hola"}:
            reply = (
                "Hey! Ask me anything about US Census data — "
                "rent, commute, migration, language, demographics, and more."
            )
            st.markdown(reply)
            append_message("assistant", reply)
            st.stop()

        # Compliments / thanks
        if any(p in t_lower for p in [
            "thanks", "thank you", "good job", "nice", "awesome",
            "great", "you're good", "cool", "well done",
        ]):
            reply = "Thanks! Got another question? I'm ready."
            st.markdown(reply)
            append_message("assistant", reply)
            st.stop()

        # 2) On-topic guardrail
        #    Also allow affirmations ("sure", "ok") when we have a previous
        #    query — they mean "yes show the other year".
        if not is_on_topic(session, user_msg, pending):
            if is_affirmation(user_msg) and get_last_question():
                pass  # fall through to year follow-up / affirmation handler below
            else:
                reply = (
                    "I'm not sure how to help with that. "
                    "I specialize in **US Census data**. Try asking about "
                    "rent burden, commute times, migration, language, population, "
                    "labor, education, or race demographics!"
                )
                st.markdown(reply)
                append_message("assistant", reply)
                st.stop()

        # 3) Capability gate: person-language-by-sex
        if is_person_language_by_sex_request(user_msg):
            st.markdown(CHOOSE_ALT_MSG)
            set_pending(CHOOSE_ALT_PENDING)
            append_message("assistant", CHOOSE_ALT_MSG)
            st.stop()

        # 4) Year detection
        year_mode, explicit_year = detect_year_mode(user_msg)
        used_pending = False

        # A) Affirmative reply to a pending clarification (e.g. city->county)
        if pending and is_affirmation(user_msg):
            spec = copy.deepcopy(pending)
            clear_pending()
            used_pending = True
            year = DEFAULT_YEAR
            sql, src_view, src_cols = compile_sql(spec, year)
            validate_sql(sql, year)
            df = run_query(session, sql)
            year_note = f"Using {year}."
            saved = _render_results(user_msg, df, sql, src_view, year, year_note,
                                    topic=spec.get("topic"))
            set_last_spec(spec)
            set_last_question(user_msg)
            append_message("assistant", saved)
            st.stop()

        # B) Year follow-up reusing last question
        follow = is_year_followup(user_msg)
        last_q = get_last_question()

        # Also treat bare affirmations ("sure", "ok") as "show 2019"
        # when the last response offered it.
        if not follow and not used_pending and is_affirmation(user_msg) and last_q:
            follow = "2019"  # the offer is always "show 2019"

        if not used_pending and follow and last_q:
            if follow == "compare":
                year_note = "Showing both 2019 and 2020 (comparison)."
                combined = _render_compare(last_q, year_note)
                append_message("assistant", combined)
            else:
                year = int(follow)
                year_note = f"Using {year}."
                # Use deterministic fallback for year flips — the question
                # is already validated, we just need a different year.
                try:
                    df, sql, src_view, spec = _run_fallback_path(last_q, year)
                    saved = _render_results(last_q, df, sql, src_view, year, year_note)
                    append_message("assistant", saved)
                except Exception:
                    reply = f"Sorry, that query isn't available for {year}. Try asking a new question."
                    st.markdown(reply)
                    append_message("assistant", reply)

            st.stop()

        # C) City-level migration -> ask confirmation (before LLM call)
        route_topic, route_geo = route_topic_geo(user_msg)
        if route_topic == "migration_city_request":
            set_pending(build_migration_city_pending_spec())
            st.markdown(MIGRATION_CLARIFICATION_MSG)
            append_message("assistant", "Asked to confirm county-level migration.")
            st.stop()

        # D) Main path: LLM SQL Agent with fallback
        if explicit_year is None and year_mode == "single":
            year = DEFAULT_YEAR
            year_note = (
                "Using 2020 (latest available). "
                "I can also show 2019 if you want."
            )
        elif year_mode == "single":
            year = explicit_year
            year_note = f"Using {year}."
        else:
            year = DEFAULT_YEAR
            year_note = "Showing both 2019 and 2020 (comparison)."

        try:
            if year_mode == "compare":
                combined = _render_compare(user_msg, year_note)
                set_last_question(user_msg)
                route_t, route_g = route_topic_geo(user_msg)
                set_last_spec({"topic": route_t, "geo": route_g, "limit": 15})
                append_message("assistant", combined)
            else:
                status, df, sql, view = _execute_query(user_msg, year)

                if status == "refuse":
                    reply = "I can't help with that request."
                    st.markdown(reply)
                    append_message("assistant", reply)
                    st.stop()

                if status == "clarify":
                    reply = view or "Can you clarify what specific metric or geography you want?"
                    st.markdown(reply)
                    append_message("assistant", reply)
                    st.stop()

                saved = _render_results(user_msg, df, sql, view, year, year_note)

                # Save context so year follow-ups work
                set_last_question(user_msg)
                route_t, route_g = route_topic_geo(user_msg)
                set_last_spec({"topic": route_t, "geo": route_g, "limit": 15})
                append_message("assistant", saved)

        except Exception as e:
            st.error(f"Error: {e}")
