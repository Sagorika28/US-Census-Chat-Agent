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
4) **SQL validator** enforces:
   - SELECT-only
   - no comments/multi-statements
   - no INFORMATION_SCHEMA/ACCOUNT_USAGE
   - allowlisted views only
   - LIMIT required and capped
   - Base columns used in SELECT/WHERE must exist in the target view (stripping dynamic aliases).
5) **Fallback**: If the agent's SQL fails validation, we invoke the deterministic compiler:
   - **Router** (keyword-based topic/geo)
   - **Year policy** (default year + compare mode)
6) Execute via Snowpark session
7) Show:
   - one-line natural language summary (via Snowflake Cortex Synthesizer)
   - table results (with dynamic layman headers)
   - citations: view name + columns used

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
- “Female non-English speaking population” is not supported by the current views.
The agent explains the limitation and offers supported alternatives:
1) non-English households by state, or
2) female population by state

---

## 12) Deployment + reproducibility
Deployment options:
- Snowflake Streamlit (preferred for this project: uses Snowflake compute; no local machine required)
- Git integration: connect a private GitHub repo and deploy from `app/streamlit_app.py`

Repo includes:
- App code
- View DDL SQL files
- Verification SQL files
- This dev process doc
- README with demo URL + access instructions

---

## 13) What we would improve with more time
- Add an evaluation harness with:
  - required 4 questions
  - edge cases (year followups, city migration clarification, household vs person language)
  - regression tests for routing + SQL validation
- Add caching for repeated queries
- Add richer explanation layer (second-pass LLM) with strict grounding to returned rows
- Add least-privilege Snowflake role for the app (read-only on views)
- Add pagination/export for large tables