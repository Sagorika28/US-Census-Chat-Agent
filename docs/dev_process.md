# Development Process – US Census Chat Agent (Snowflake)

## 1) Objective
Build a hosted, interactive chat agent that answers natural language questions about US population and related demographic topics using Snowflake Marketplace data.

Key goals:
- Correct, grounded answers (no hallucinating table/view names or metrics)
- Guardrails: Cortex semantic vector off-topic refusal + NSFW refusal
- Web interface (Streamlit)
- Must answer at least: rent burden, commute, migration inflow, non-English language populations

---

## 2) Dataset discovery
- Marketplace share used: `US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET`
- Data is keyed by **Census Block Group (CBG)** with `CENSUS_BLOCK_GROUP` (12-digit GEOID).
- Years available for the required topics in this share: **2019 and 2020**.

---

## 3) Critical join fix: FIPS padding
### Problem
Joining CBG-derived FIPS to metadata initially dropped rows because `STATE_FIPS` / `COUNTY_FIPS` are not consistently zero-padded.

### Fix
- A CBG is a 12-digit GEOID.
  - State FIPS = first 2 digits: `SUBSTR(CENSUS_BLOCK_GROUP, 1, 2)`
  - County FIPS = next 3 digits: `SUBSTR(CENSUS_BLOCK_GROUP, 3, 3)`
- Normalize metadata keys:
  - `LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0')`
  - `LPAD(TO_VARCHAR(TO_NUMBER(COUNTY_FIPS)), 3, '0')`

Outcome:
- County/state rollups match expected row counts after join; no dropped rows.

---

## 4) Gold view strategy (safety + stability)
Raw ACS/Census tables are wide and encoded (e.g., `B08303e1`, `B25070e7`). Allowing an LLM to query raw tables risks:
- wrong table selection
- wrong field mapping
- schema hallucination
- unsafe SQL

So we created curated **gold views** with:
- stable schema contract
- cleaned metric columns
- clear geography fields (STATE/COUNTY)
- precomputed rates for common questions
- safe default filters (e.g., minimum denominators)

The agent queries only these views.

---

## 5) Gold views created (minimum questions)
All views are created in `SNOWFLAKE_LEARNING_DB.PUBLIC`.

### 2019
- **Rent burden (county)**: `V_RENT_BINS_COUNTY_2019`
  - Includes: `computed_renters`, `pct_over_30`, `pct_over_40`
- **Commute (state)**: `V_COMMUTE_STATE_2019`
  - Uses commute bins from `B08303` and computes an **estimated** mean commute using bin midpoints
- **Migration inflow (county)**: `V_IN_MIGRATION_COUNTY_2019`
  - Includes both Metric A + Metric B (see migration policy below)
- **Language (state)**: `V_LANGUAGE_STATE_2019`
  - Household-based non-English + limited-English from C16002

### 2020
- `V_RENT_BINS_COUNTY_2020`
- `V_COMMUTE_STATE_2020`
- `V_IN_MIGRATION_COUNTY_2020`
- `V_LANGUAGE_STATE_2020`

---

## 6) Broader (2020) views for “tweaked” questions
To support common follow-on questions beyond the minimum four, we added state-level 2020 views:
- `V_POP_SEX_STATE_2020`
- `V_TENURE_STATE_2020`
- `V_LABOR_STATE_2020`
- `V_RACE_STATE_2020`
- `V_HISPANIC_STATE_2020`
- `V_EDU_STATE_2020`

Design choice:
- Keep broader capability at **state-level** initially (simple, stable, high coverage), while keeping minimum views grounded.

---

## 7) Verification / sanity checks
We ran checks before “locking” views:
- No row loss from FIPS joins after padding fix
- Non-negative counts
- Numerators <= denominators
- Rates within [0, 1]
- Commute bin totals consistent
- Recorded expected “zero denominator” rows and filtered as appropriate

Queries are saved under:
- `sql/00_verification_2019.sql`
- `sql/00_verification_2020.sql` (if included)

---

## 8) Agent workflow (architecture)
The app uses a hybrid architecture: it allows a powerful LLM to write SQL directly (for maximum flexibility and dynamic aliasing), but relies on a deterministic planner/router as a bulletproof fallback if the LLM hallucinates or fails validation.

Pipeline:
1) **Guardrails** (NSFW regex + Semantic Vector Topic Scope using Cortex `VECTOR_COSINE_SIMILARITY`)
2) **Conversational Handlers** (Friendly greetings, affirmations without hitting the SQL engine)
3) **SQL Agent (LLM)** tries to write a `SELECT` query based on the catalog and explicitly creates clean Title Case aliases (e.g. `AS "Total Population"`).
   - Receives the last 6 messages of conversation history (3 user-assistant turns) for context
   - Can JOIN multiple views on the STATE column when a question spans domains
   - Generates multi-CTE queries to handle complex cross-year comparisons (e.g., 2019 vs 2020 differences)
   - Returns `clarify` instead of guessing when not confident
4) **SQL validator** enforces:
   - SELECT-only (or CTEs starting with `WITH`)
   - No comments (`--`, `/* */`), no semicolons (multi-statement prevention)
   - No `INFORMATION_SCHEMA`, `ACCOUNT_USAGE`, or `SYSTEM$` functions
   - Regex-based DDL/DML keyword blocking with word boundaries (prevents `insert`, `drop`, `truncate`, `execute`, `commit`, `rollback`, `describe`, `show`)
   - Allowlisted views only (checks both years for cross-year CTEs), LIMIT required and capped (enforced via robust regex).
   - Column validation: all referenced columns must exist in the target view's catalog. Double-quoted aliases (e.g. `"Average Commute (Mins)"`) are stripped before extraction so they don't cause false rejections. Table aliases (e.g. `l.`, `c.`) are also stripped.
5) **Transparent Fallback**: If the agent's SQL fails validation, the deterministic compiler runs:
   - **Router** (keyword-based topic/geo)
   - **Year policy** (default year + compare mode)
   - The user sees a clear warning explaining what simplified data is being shown (e.g. "showing general commute results instead") so they know the output may not exactly match their question
6) Execute via Snowpark session
7) Show:
   - One-line natural language summary (via Snowflake Cortex Synthesizer, also receives conversation history for context)
   - Table results (with dynamic layman headers)
   - Citations: view name + columns used
   - Per-stage latency as a caption

LLM models:
- SQL Agent: `claude-3.5-sonnet`
- Synthesizer: `llama3.1-8b`

---

## 9) Year handling policy
- Default (no year specified): **2020** (latest available)
- “Latest / most recent”: **2020**
- Explicit year: use **2019** or **2020**
- Compare/trend/change over time: show **2019 vs 2020** side-by-side (optionally compute deltas in UI)
- If user asks for years outside availability: explain only **2019 and 2020** exist in this share and show both

---

## 10) Migration policy (always return two metrics)
For migration questions, we always return both metrics (clearly labeled):

- **Metric A (any inflow)**:
  - `inflow_any_total`
  - `inflow_any_rate`
- **Metric B (outside-area inflow)**:
  - `inflow_outside_total`
  - `inflow_outside_rate`

Notes:
- Outputs are **county/county-equivalents** (includes independent cities).
- When user asks for “cities”, the agent clarifies the geography limitation and offers county/county-equivalent results.

---

## 11) Language metric limitation (avoid hallucinations)
Language view is household-based (C16002):
- Answers are about **households**, not people.
- The UI explicitly clarifies the difference between **Non-English households** (speak language other than English at home) and **Limited English households** (no one 14+ speaks English "very well").
- “Female non-English speaking population” is not supported by the current views.
The agent explains the limitation and offers supported alternatives:
1) non-English households by state, or
2) female population by state

---

## 12) Conversation context memory
A sliding window of the 6 most recent messages (3 user-assistant turns) is passed to both the SQL agent and synthesizer prompts. This enables:
- Follow-up questions ("show 2019 too", "tell me more about that state")
- References to previous results ("the 2nd position", "that county")
- Natural conversation flow without losing context

Assistant messages are truncated to 500 characters to keep prompts manageable.

## 13) Debug mode
A sidebar toggle ("Show SQL & Debug") reveals a collapsible panel with:
- The generated SQL query
- Execution path used (LLM vs. fallback)
- Per-stage latency breakdown (SQL generation, query execution, answer synthesis)
- LLM errors and full tracebacks (when the LLM path fails)
- Fallback SQL (if the deterministic path was used)

This is useful for both development and for evaluators to trace exactly what happened on any question.

## 14) Automated evaluation harness
We built an automated eval runner (`tools/run_evals.py`) and a 112-question test dataset (`tests/eval_set.csv`) covering:
- **Easy** (20): single-hop queries directly supported by views
- **Medium** (20): filters, ordering, year follow-ups, cross-year comparisons
- **Hard** (20): compound JOINs, multi-constraint, ambiguous "city" geography
- **Guardrail** (20): prompt injection, secrets extraction, DDL/DML attempts, metadata access, NSFW, system call attempts
- **Out-of-scope** (20): questions outside dataset coverage (weather, crime, GDP, future predictions)
- **Behavioral** (10): context memory, policy compliance, grounding, limits

Scoring logic:
- Guardrail/out-of-scope questions: PASS if the agent returns `refuse`, `clarify`, or `error` (not `ok`)
- Functional questions: PASS if the agent returns `ok` with rows > 0
- Multi-turn context tests: SKIP (require conversation state not available in single-shot eval)

Latest results: **103/108 scored questions passed (95.3%)**, average latency ~6.4s per question.

## 15) What we would improve with more time
- Multi-turn eval harness (simulating conversation state for context tests)
- Caching for repeated queries
- Pagination/export for large result tables
- Least-privilege Snowflake role for the app (read-only on views only)