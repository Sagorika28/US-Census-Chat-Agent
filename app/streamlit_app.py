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
import time
import traceback
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
    get_recent_history,
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
    Try the LLM text-to-SQL path. Returns (df, sql, view, explanation, debug)
    or raises on failure so the caller can fall back.
    """
    t0 = time.time()
    result = generate_sql(session, user_msg, year, chat_history=get_recent_history())
    sql_gen_time = time.time() - t0

    mode = result.get("mode", "sql")
    llm_year_mode = result.get("year_mode", "single")
    
    if mode == "refuse":
        return "refuse", None, None, result.get("explanation"), llm_year_mode, {"llm_response": result, "sql_gen_sec": sql_gen_time}
    if mode == "clarify":
        return "clarify", None, None, result.get("explanation", "Can you be more specific?"), llm_year_mode, {"llm_response": result, "sql_gen_sec": sql_gen_time}

    sql = result.get("sql", "")
    llm_year = result.get("year", year)
    view = result.get("view", "")
    explanation = result.get("explanation", "")

    # Validate the LLM's SQL (raises ValueError if bad)
    validate_sql(sql, llm_year)
    validate_columns(sql)

    t1 = time.time()
    df = run_query(session, sql)
    query_exec_time = time.time() - t1

    return "ok", df, sql, view, llm_year_mode, {
        "llm_response": result, "sql": sql, "explanation": explanation,
        "sql_gen_sec": sql_gen_time, "query_exec_sec": query_exec_time,
    }


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
    return df, sql, src_view, spec, "single"


def _execute_query(user_msg: str, year: int):
    """
    Try the LLM path first, fall back to deterministic if it fails.
    Returns (status, df, sql, view_or_explanation, debug_info).
    """
    debug_info = {"path": "llm", "errors": []}
    try:
        status, df, sql, view, llm_year_mode, llm_debug = _run_llm_path(user_msg, year)
        debug_info.update(llm_debug)
        if status in ("refuse", "clarify"):
            return status, None, None, view, llm_year_mode, debug_info
        return "ok", df, sql, view, llm_year_mode, debug_info
    except Exception as llm_err:
        debug_info["path"] = "fallback"
        debug_info["llm_error"] = f"{type(llm_err).__name__}: {llm_err}"
        debug_info["llm_traceback"] = traceback.format_exc()
        debug_info["errors"].append(f"LLM path failed: {llm_err}")
        # LLM failed -- use deterministic fallback (simpler query)
        try:
            df, sql, src_view, spec, fb_year_mode = _run_fallback_path(user_msg, year)
            debug_info["fallback_sql"] = sql
            debug_info["fallback_spec"] = str(spec)
            return "fallback", df, sql, src_view, fb_year_mode, debug_info
        except Exception as fb_err:
            debug_info["fallback_error"] = f"{type(fb_err).__name__}: {fb_err}"
            debug_info["fallback_traceback"] = traceback.format_exc()
            debug_info["errors"].append(f"Fallback path also failed: {fb_err}")
            return "error", None, None, str(fb_err), debug_info


# Rendering helpers

def _show_debug(debug_info: dict, sql: str = None) -> None:
    """Show debug panel when debug mode is enabled."""
    if not st.session_state.get("debug_mode"):
        return
    with st.expander("Debug Info", expanded=True):
        # Timing summary
        parts = []
        if debug_info.get("sql_gen_sec"):
            parts.append(f"SQL generation: {debug_info['sql_gen_sec']:.1f}s")
        if debug_info.get("query_exec_sec"):
            parts.append(f"Query execution: {debug_info['query_exec_sec']:.1f}s")
        if debug_info.get("synth_sec"):
            parts.append(f"Answer synthesis: {debug_info['synth_sec']:.1f}s")
        if parts:
            st.caption("Timing: " + " · ".join(parts))
        if sql:
            st.code(sql, language="sql")
        path = debug_info.get("path", "unknown")
        st.caption(f"**Path used:** `{path}`")
        if debug_info.get("explanation"):
            st.caption(f"**LLM explanation:** {debug_info['explanation']}")
        if debug_info.get("llm_response"):
            st.json(debug_info["llm_response"])
        if debug_info.get("llm_error"):
            st.warning(f"LLM error: {debug_info['llm_error']}")
        if debug_info.get("llm_traceback"):
            st.code(debug_info["llm_traceback"], language="text")
        if debug_info.get("fallback_sql"):
            st.caption("**Fallback SQL:**")
            st.code(debug_info["fallback_sql"], language="sql")
        if debug_info.get("fallback_error"):
            st.error(f"Fallback error: {debug_info['fallback_error']}")
        if debug_info.get("fallback_traceback"):
            st.code(debug_info["fallback_traceback"], language="text")


def _render_results(user_msg, df, sql, view, year, year_note, topic=None, debug_info=None):
    """Render query results and return the answer text for chat history."""
    st.markdown(year_note)

    # Show debug info when enabled
    if debug_info:
        _show_debug(debug_info, sql)

    # LLM answer synthesis
    try:
        t0 = time.time()
        answer = synthesize_answer(session, user_msg, sql, view, df, year, chat_history=get_recent_history())
        synth_time = time.time() - t0
        if debug_info:
            debug_info["synth_sec"] = synth_time
    except Exception as e:
        answer = f"Could not generate a summary: {e}"
        synth_time = 0.0
        if st.session_state.get("debug_mode"):
            st.warning(f"Synthesizer error: {e}")
    st.markdown(answer)

    # Always show timing as a subtle caption
    total = (debug_info or {}).get("sql_gen_sec", 0) + (debug_info or {}).get("query_exec_sec", 0) + synth_time
    if total > 0:
        st.caption(f"...Thought for {total:.1f}s")

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
    """2019 vs 2020 comparison. Let the LLM write a single cross-year CTE."""
    # Let the LLM handle it directly - it knows both 2019 and 2020 views
    # and can write a CTE that joins them and computes differences.
    status, df, sql, view, debug_info = _execute_query(user_msg, 2020)

    if status in ("ok", "fallback") and df is not None and not df.empty:
        if status == "fallback":
            st.warning(
                "The comparison was too complex, so I'm showing "
                "a simplified version. Try a more specific question."
            )
        return _render_results(user_msg, df, sql, view, 2020, year_note,
                               debug_info=debug_info)

    if status in ("refuse", "clarify"):
        reply = view or "Could you clarify what you'd like to compare?"
        st.markdown(reply)
        _show_debug(debug_info)
        return reply

    # LLM couldn't do it -- fall back to running each year separately
    results = {}
    for y in (2019, 2020):
        s, d, q, v, dbg = _execute_query(user_msg, y)
        if s in ("ok", "fallback") and d is not None and not d.empty:
            results[y] = (d, q, v, dbg)

    if not results:
        st.error("Could not generate queries for comparison.")
        return year_note

    st.markdown(year_note)

    answers = [year_note]
    c1, c2 = st.columns(2)
    for col, y in [(c1, 2019), (c2, 2020)]:
        if y not in results:
            continue
        d, q, v, dbg = results[y]
        with col:
            st.subheader(str(y))
            _show_debug(dbg, q)
            try:
                answer = synthesize_answer(session, user_msg, q, v, d, y,
                                           chat_history=get_recent_history())
            except Exception as e:
                answer = f"Could not generate a summary for {y}: {e}"
            st.markdown(answer)
            st.dataframe(d, width="stretch")
            st.caption(f"Source: `{v}`")
            answers.append(f"**{y}**: {answer}")

    return "\n\n".join(answers)


# Page config + chat history

st.set_page_config(page_title="US Census Chat Agent", layout="wide")
st.title("US Census Chat Agent")
init_session_state()

# Demo questions sidebar
_DEMO_QUESTIONS = [
    # 4 required (rent, commute, migration, language)
    "In which areas of the country do residents spend, on average, over 30% of their income on rent?",
    "Which states have the longest average commutes?",
    "Which counties have the highest amount of migration (people moving in)?",
    "What are the top states with non-English speaking populations?",
    # Extra demos
    "Which of the top 5 non-english speaking states have the longest commute time amongst them?",
    "Based on 2019-2020 Census data, which state ranked 3rd for the largest increase or decrease in average commute time? Additionally, which state saw the 2nd largest change in non-English speaking households during that same period?",
    "Compare unemployment rates across states",
    "Which state has the highest female population?",
    "Top states by bachelor's degree attainment",
    "Show 2019 vs 2020 commute comparison",
]

with st.sidebar:
    st.header("Try a Question")
    st.caption("Click any question below to get started:")

    def _set_demo(q):
        st.session_state["demo_q"] = q

    for q in _DEMO_QUESTIONS:
        st.button(q, key=f"demo_{q}", use_container_width=True,
                  on_click=_set_demo, args=(q,))

    st.divider()
    if "debug_mode" not in st.session_state:
        st.session_state["debug_mode"] = False
    st.toggle("Show SQL & Debug", key="debug_mode")

for m in get_messages():
    with st.chat_message(m["role"]):
        st.markdown(m["content"])


# Chat input + main pipeline

user_msg = st.chat_input(
    "Ask a question about US population "
    "(rent, commute, migration, language, demographics)..."
)

# Accept demo question from sidebar click
if not user_msg and "demo_q" in st.session_state:
    user_msg = st.session_state.pop("demo_q")

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
                    chat_history=get_recent_history(),
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
                "Hey! Ask me anything about US Census data - "
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
            with st.spinner("Thinking..."):
                sql, src_view, src_cols = compile_sql(spec, year)
                validate_sql(sql, year)
                df = run_query(session, sql)
            year_note = f"Using {year}."
            saved = _render_results(user_msg, df, sql, src_view, year, year_note,
                                    topic=spec.get("topic"), debug_info={"path": "pending", "sql": sql})
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
                with st.spinner("Thinking..."):
                    combined = _render_compare(last_q, year_note)
                append_message("assistant", combined)
            else:
                year = int(follow)
                year_note = f"Using {year}."
                # Use deterministic fallback for year flips - the question
                # is already validated, we just need a different year.
                try:
                    with st.spinner("Thinking..."):
                        df, sql, src_view, spec = _run_fallback_path(last_q, year)
                    saved = _render_results(last_q, df, sql, src_view, year, year_note,
                                            debug_info={"path": "fallback (year flip)", "sql": sql})
                    append_message("assistant", saved)
                except Exception as e:
                    reply = f"Sorry, that query isn't available for {year}. Try asking a new question."
                    st.markdown(reply)
                    if st.session_state.get("debug_mode"):
                        st.error(f"Detail: {e}")
                        st.code(traceback.format_exc(), language="text")
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
                with st.spinner("Thinking..."):
                    combined = _render_compare(user_msg, year_note)
                set_last_question(user_msg)
                route_t, route_g = route_topic_geo(user_msg)
                set_last_spec({"topic": route_t, "geo": route_g, "limit": 15})
                append_message("assistant", combined)
            else:
                with st.spinner("Thinking..."):
                    status, df, sql, view, llm_year_mode, debug_info = _execute_query(user_msg, year)

                # If the LLM organically decided to write a cross-year CTE comparison,
                # override the UI header to show it is a comparison, instead of "Using 2020."
                if llm_year_mode == "compare":
                    year_note = "Showing both 2019 and 2020 (comparison)."

                if status == "refuse":
                    reply = view or "I can't help with that request."
                    st.markdown(reply)
                    _show_debug(debug_info)
                    append_message("assistant", reply)
                    st.stop()

                if status == "clarify":
                    reply = view or "Can you clarify what specific metric or geography you want?"
                    st.markdown(reply)
                    _show_debug(debug_info)
                    append_message("assistant", reply)
                    st.stop()

                if status == "error":
                    reply = (
                        "I'm sorry, I wasn't able to answer that question with the available data. "
                        "Could you rephrase or try a simpler question?"
                    )
                    st.markdown(reply)
                    _show_debug(debug_info)
                    append_message("assistant", reply)
                    st.stop()

                if status == "fallback":
                    # Tell the user exactly what simplified data they're seeing
                    topic_label = "related"
                    if view:
                        vl = view.lower()
                        if "rent" in vl:
                            topic_label = "general rent burden"
                        elif "commute" in vl:
                            topic_label = "general commute"
                        elif "migration" in vl:
                            topic_label = "general migration"
                        elif "language" in vl:
                            topic_label = "general language"
                        elif "pop_sex" in vl or "demographic" in vl:
                            topic_label = "general demographics"
                        elif "labor" in vl or "employ" in vl:
                            topic_label = "general labor/employment"
                        elif "edu" in vl:
                            topic_label = "general education"
                        elif "hispanic" in vl or "race" in vl:
                            topic_label = "general race/ethnicity"
                        elif "tenure" in vl:
                            topic_label = "general housing tenure"
                    st.warning(
                        f"Your question was too complex for me to answer precisely, "
                        f"so I'm showing {topic_label} results instead. "
                        f"Try breaking your question into simpler parts for a more accurate answer."
                    )

                saved = _render_results(user_msg, df, sql, view, year, year_note,
                                        debug_info=debug_info)

                # Save context so year follow-ups work
                set_last_question(user_msg)
                route_t, route_g = route_topic_geo(user_msg)
                set_last_spec({"topic": route_t, "geo": route_g, "limit": 15})
                append_message("assistant", saved)

        except Exception as e:
            reply = (
                "An unexpected error occurred. "
                "Please try rephrasing your question."
            )
            st.error(reply)
            if st.session_state.get("debug_mode"):
                st.error(f"Detail: {e}")
                st.code(traceback.format_exc(), language="text")
            append_message("assistant", reply)
