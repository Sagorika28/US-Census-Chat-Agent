"""
Conversation memory: thin wrappers around ``st.session_state``.

Keeps all Streamlit coupling in one place so the rest of the
pipeline stays pure / testable.
"""

from typing import Any, Dict, List, Optional

import streamlit as st


def init_session_state() -> None:
    """Ensure all required session-state keys exist."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "last_spec" not in st.session_state:
        st.session_state.last_spec = None
    if "last_question" not in st.session_state:
        st.session_state.last_question = None
    if "pending_clarification" not in st.session_state:
        st.session_state.pending_clarification = None


# Messages
def get_messages() -> List[Dict[str, str]]:
    return st.session_state.messages


def append_message(role: str, content: str) -> None:
    st.session_state.messages.append({"role": role, "content": content})


# Pending clarification
def get_pending() -> Optional[Dict[str, Any]]:
    return st.session_state.get("pending_clarification")


def set_pending(value: Any) -> None:
    st.session_state.pending_clarification = value


def clear_pending() -> None:
    st.session_state.pending_clarification = None


# Last spec (for year follow-ups)
def get_last_spec() -> Optional[Dict[str, Any]]:
    return st.session_state.get("last_spec")


def set_last_spec(spec: Dict[str, Any]) -> None:
    st.session_state["last_spec"] = {
        "topic":          spec.get("topic"),
        "geo":            spec.get("geo"),
        "filters":        spec.get("filters") or {},
        "order_by":       spec.get("order_by") or {},
        "limit":          spec.get("limit", 15),
        "mode":           "answer",
        "show_two_lists": spec.get("show_two_lists", False),
    }


# Last question (for re-running with a different year)
def get_last_question() -> Optional[str]:
    return st.session_state.get("last_question")


def set_last_question(q: str) -> None:
    st.session_state.last_question = q
