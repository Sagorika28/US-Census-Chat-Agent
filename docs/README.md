# US Census Chat Agent (Snowflake + Streamlit)

A chat-based agent that answers US Census questions using the Snowflake Marketplace dataset **US Open Census Data & Neighborhood Insights (Free Dataset)**. The agent writes SQL against curated gold views, validates it, executes it, and synthesizes a natural-language answer.

## How the agent works

```
User -> Guardrails (NSFW + off-topic) -> SQL Agent (Claude 3.5 Sonnet)
     -> SQL Validator (safety + column check) -> Execute (Snowpark)
     -> Answer Synthesizer (OpenAI o4-mini) -> UI
```

- **SQL Agent**: LLM receives the full view catalog (names + columns + descriptions) and writes a SELECT query directly. The prompt natively requests readable layman titles (aliases) for table headers.
- **Validator**: SELECT-only, approved views only, LIMIT required, column allowlist enforced (stripping aliases safely). Non-negotiable.
- **Fallback**: If the LLM's SQL fails validation, a deterministic router + compiler produces a guaranteed-valid backend query.
- **Synthesizer**: Second LLM call turns results into a natural-language answer with key findings and caveats.

## What it can answer

1. **Rent burden**: Where do residents spend over 30% of income on rent?
2. **Commute**: Which states have the longest average commutes?
3. **Migration**: Which places have the highest inflow? (county/county-equivalents, clarifies when "city" is requested)
4. **Language**: Top states with non-English speaking populations (household-based)
5. **Plus**: population by sex, tenure, labor force, race, hispanic, education (2020 state-level)
6. **Year compare**: Side-by-side 2019 vs 2020

## Guardrails

- NSFW filter (regex) before any LLM call
- **Semantic Topic Guardrail**: Uses Snowflake Cortex vector embeddings (`VECTOR_COSINE_SIMILARITY`) to measure the mathematical semantic closeness between the user's prompt and a baseline demographics concept. Dynamically rejects off-topic queries without relying on brittle keyword lists.
- **Conversational Handlers**: Recognizes greetings, identity questions, and gratitude without triggering errors or hitting the SQL agent.
- SQL validator: SELECT-only, banned DDL/DML, no metadata schemas, approved views only, LIMIT required
- Column allowlist: catches hallucinated column names (the #1 text-to-SQL failure mode), correctly splitting out dynamic AS aliases.
- Capability gate: explains when household-based language data can't answer person-by-sex questions

## Repo structure

```
app/
├── streamlit_app.py        # UI orchestrator (entry point)
├── config.py               # Views, models, column catalog, constants
├── pipeline/               # Deterministic components
│   ├── guardrails.py       # NSFW, on-topic, capability gate
│   ├── router.py           # Topic/geo routing (fallback path)
│   ├── compiler.py         # Deterministic SQL builder (fallback path)
│   ├── validator.py        # SQL safety + column allowlist
│   ├── executor.py         # Snowpark query runner
│   └── explainer.py        # One-line summary (fallback for synthesizer)
├── llm/
│   ├── sql_agent.py        # Text-to-SQL via Cortex (Claude 3.5 Sonnet)
│   ├── synthesizer.py      # Answer synthesis via Cortex (OpenAI o4-mini)
│   └── planner.py          # Legacy JSON-spec planner (fallback path)
└── helpers/
    ├── conversation.py     # Session-state wrappers
    ├── query_spec.py       # Spec schema (fallback path)
    └── policies/
        ├── year_policy.py
        └── migration_policy.py
tests/                      # 49 tests, pure Python
sql/                        # View DDL + verification queries
docs/                       # This README + dev_process.md
```

## Running tests

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

## Production upgrade path

For production, we can consider **Snowflake Cortex Analyst** - Snowflake's purpose-built text-to-SQL service with a semantic model YAML. It handles SQL generation, validation, and grounding internally. The curated views built here would map directly to a Cortex Analyst semantic model.