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


def get_recent_history(n: int = 6) -> str:
    """Return the last *n* messages as a formatted string for LLM context.

    Default n=6 = 3 user-assistant exchanges.  Assistant messages are
    trimmed aggressively: SQL, data tables, and source citations are
    stripped, and only the first ~120 chars are kept (roughly one sentence)
    so previous topics don't overwhelm the current question.
    """
    import re
    msgs = st.session_state.get("messages", [])
    recent = msgs[-n:] if msgs else []
    lines = []
    for m in recent:
        role = "User" if m["role"] == "user" else "Assistant"
        content = m["content"]
        if m["role"] == "assistant":
            # Strip SQL blocks, markdown tables, and source lines
            content = re.sub(r"```.*?```", "", content, flags=re.DOTALL)
            content = re.sub(r"\|.*\|", "", content)
            content = re.sub(r"Source:.*", "", content)
            content = content.strip()
            # Keep only the first ~120 chars (one sentence of context)
            content = content[:120].rsplit(" ", 1)[0] + "..." if len(content) > 120 else content
        lines.append(f"{role}: {content}")
    return "\n".join(lines)
