# US Census Chat Agent (Snowflake + Streamlit)

A chat-based agent that answers US Census questions using the Snowflake Marketplace dataset **US Open Census Data & Neighborhood Insights (Free Dataset)**. The agent writes SQL against curated gold views, validates it, executes it, and synthesizes a natural-language answer.

## Demo / Access (Snowflake Streamlit)

This app is deployed as a **Streamlit app inside Snowflake** (no local setup required).

### Use the viewer login provided

1) Log in to Snowflake using: 
- **Account / Server URL:** [NDCPYYH-ODC36169.snowflakecomputing.com](https://ndcpyyh-odc36169.snowflakecomputing.com/)  
- **Username:** APP_REVIEWER  
- **Password:** snowflakeai!  
- **Role:** APP_VIEWER_ROLE  
- **Warehouse:** COMPUTE_WH  

2) Open the app link (viewer mode) or click on Sagorika US Census Streamlit App from the dashboard:
- https://app.snowflake.com/streamlit/us-east-1/zcc13934/#/apps/w4jolvua4louq7emdedh

![dashboard view](image.png)

- The app queries read-only curated views from `SNOWFLAKE_LEARNING_DB.PUBLIC`.

## How the agent works

```
User -> Guardrails (NSFW + off-topic) -> SQL Agent (Claude 3.5 Sonnet)
     -> SQL Validator (safety + column check) -> Execute (Snowpark)
     -> Answer Synthesizer (Llama 3.1 8B) -> UI
```

- **SQL Agent**: LLM receives the full view catalog (names + columns + descriptions) and the last 6 messages of conversation history (3 user-assistant turns) as context. It writes a SELECT query directly with readable Title Case aliases.
- **Validator**: SELECT-only, approved views only, LIMIT required, column allowlist enforced (double-quoted aliases are stripped before validation so they don't trigger false column errors). Regex-based DDL/DML blocking with word boundaries. Non-negotiable.
- **Transparent Fallback**: If the LLM's SQL fails validation, a deterministic router + compiler produces a guaranteed-valid backend query. The user sees a clear warning explaining that a simplified version is being shown (e.g., "showing general commute results instead") so they can rephrase.
- **Synthesizer**: Second LLM call turns results into a natural-language answer with key findings and caveats.
- **JOINs**: The SQL agent can JOIN multiple views on the STATE column when a question requires data from different domains (e.g., "states with highest commute AND highest rent burden").
- **Conversation Context**: A sliding window of the most recent 6 messages (3 user-assistant turns) is passed to both the SQL agent and synthesizer, enabling follow-up questions like "show 2019 too" or "tell me more about that state." Assistant messages are truncated to 500 characters to manage prompt size.
- **Honest Clarification**: When the LLM is not confident it can answer accurately, it returns a clarification instead of guessing. This prevents hallucinated or misleading results.

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
- **SQL Validator (defense-in-depth)**:
  - SELECT-only (or CTEs starting with `WITH`)
  - Regex word-boundary DDL/DML keyword blocking (`INSERT`, `DROP`, `TRUNCATE`, `EXECUTE`, `SYSTEM$`, etc.)
  - Banned: semicolons (multi-statement), SQL comments (`--`, `/* */`), `INFORMATION_SCHEMA`, `ACCOUNT_USAGE`
  - Approved views only (checks both years for cross-year CTEs), LIMIT required and capped (enforced via robust regex)
  - Column allowlist: catches hallucinated column names. Double-quoted aliases are stripped before validation so readable headers like `"Average Commute (Mins)"` don't cause false rejections.
- **Capability gate**: explains when household-based language data can't answer person-by-sex questions

## Debug Mode

A "Show SQL & Debug" toggle in the sidebar reveals:
- Generated SQL
- Execution path (LLM vs. fallback)
- Per-stage latency (SQL generation, query execution, answer synthesis)
- LLM errors and tracebacks (when applicable)
- Fallback SQL (if the deterministic path was used)

## Repo structure

```text
├── streamlit_app.py        # Root entry point wrapper
├── environment.yml         # Conda environment definition
├── requirements.txt        # Python dependencies
├── .env                    # (Optional) Local Snowflake credentials
├── app/
│   ├── streamlit_app.py    # Main UI orchestrator
│   ├── config.py           # Views, models, column catalog, constants
│   ├── pipeline/           # Deterministic components
│   │   ├── guardrails.py   # NSFW, semantic semantic vector scope, capability gate
│   │   ├── router.py       # Deterministic topic/geo routing
│   │   ├── compiler.py     # Deterministic SQL builder
│   │   ├── validator.py    # SQL safety + column allowlist
│   │   ├── executor.py     # Snowpark query runner
│   │   └── explainer.py    # One-line summary
│   ├── llm/
│   │   ├── sql_agent.py    # Text-to-SQL via Cortex (claude-3-5-sonnet)
│   │   ├── synthesizer.py  # Answer synthesis via Cortex (llama3.1-8b)
│   │   └── planner.py      # Legacy JSON-spec planner (fallback path)
│   └── helpers/
│       ├── conversation.py # Session-state wrappers
│       ├── query_spec.py   # Spec schema (fallback path)
│       └── policies/
│           ├── year_policy.py
│           └── migration_policy.py
├── tests/                  # Unit tests (49+) + eval_set.csv (110 test cases)
│   └── eval_set.csv        # Automated eval dataset (easy/medium/hard/guardrail/out-of-scope)
├── tools/
│   └── run_evals.py        # Automated evaluation runner
├── sql/                    # View DDL + verification queries
└── docs/                   # This README + dev_process.md
```

## Running tests

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

## Automated Evaluation

An automated eval runner (`tools/run_evals.py`) tests the full pipeline against 112 questions across categories. **Note:** This is a deterministic *heuristic* evaluator that checks if the generated SQL/text contains the expected columns, views, and values for a given question type. It is not a quality checker.

Categories tested:
- **Easy** (20): single-hop queries directly supported by views
- **Medium** (20): filters, ordering, year follow-ups
- **Hard** (20): compound JOINs, multi-constraint, ambiguous geography
- **Guardrail** (20): prompt injection, secrets, DDL/DML, metadata access, NSFW
- **Out-of-scope** (20): questions outside dataset coverage
- **Behavioral** (10): context memory, policy compliance, grounding

```bash
source .venv/bin/activate
python tools/run_evals.py tests/eval_set.csv
```

Latest results: **103/108 scored questions passed (95.3%)**, average latency ~6.4s per question. 4 multi-turn context tests are skipped (require conversation state).

## Production upgrade path

For production, we can consider **Snowflake Cortex Analyst** - Snowflake's purpose-built text-to-SQL service with a semantic model YAML. It handles SQL generation, validation, and grounding internally. The curated views built here would map directly to a Cortex Analyst semantic model.