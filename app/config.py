"""
Centralized configuration for the US Census Chat Agent.

All allowed views, model names, column catalogs, and constants live here.
No other module should hardcode view names or model identifiers.
"""

# Snowflake object coordinates
APP_DB = "SNOWFLAKE_LEARNING_DB"
APP_SCHEMA = "PUBLIC"
DEFAULT_YEAR = 2020

# LLM models (Snowflake Cortex)
SQL_MODEL = "claude-3-5-sonnet"        # primary: text-to-SQL
SUMMARY_MODEL = "llama3.1-8b"          # cheap/fast: answer synthesis
EMBEDDING_MODEL = "snowflake-arctic-embed-m"  # for vector semantic search

# Allowed gold views
# The app may ONLY query these views. Keys follow the pattern
# "<topic>_<geo>" so the compiler can look them up deterministically.
ALLOWED_VIEWS = {
    2019: {
        "rent_county":      f"{APP_DB}.{APP_SCHEMA}.V_RENT_BINS_COUNTY_2019",
        "commute_state":    f"{APP_DB}.{APP_SCHEMA}.V_COMMUTE_STATE_2019",
        "migration_county": f"{APP_DB}.{APP_SCHEMA}.V_IN_MIGRATION_COUNTY_2019",
        "language_state":   f"{APP_DB}.{APP_SCHEMA}.V_LANGUAGE_STATE_2019",
    },
    2020: {
        "rent_county":      f"{APP_DB}.{APP_SCHEMA}.V_RENT_BINS_COUNTY_2020",
        "commute_state":    f"{APP_DB}.{APP_SCHEMA}.V_COMMUTE_STATE_2020",
        "migration_county": f"{APP_DB}.{APP_SCHEMA}.V_IN_MIGRATION_COUNTY_2020",
        "language_state":   f"{APP_DB}.{APP_SCHEMA}.V_LANGUAGE_STATE_2020",
        # Broader (2020 only)
        "pop_sex_state":    f"{APP_DB}.{APP_SCHEMA}.V_POP_SEX_STATE_2020",
        "tenure_state":     f"{APP_DB}.{APP_SCHEMA}.V_TENURE_STATE_2020",
        "labor_state":      f"{APP_DB}.{APP_SCHEMA}.V_LABOR_STATE_2020",
        "race_state":       f"{APP_DB}.{APP_SCHEMA}.V_RACE_STATE_2020",
        "hispanic_state":   f"{APP_DB}.{APP_SCHEMA}.V_HISPANIC_STATE_2020",
        "edu_state":        f"{APP_DB}.{APP_SCHEMA}.V_EDU_STATE_2020",
    },
}

# View catalog: view name -> set of valid column names (lowercased).
# Used by the validator to catch hallucinated columns.
VIEW_CATALOG = {
    f"{APP_DB}.{APP_SCHEMA}.V_RENT_BINS_COUNTY_2019": {
        "state", "county", "computed_renters",
        "lt_10", "p10_14", "p15_19", "p20_24", "p25_29",
        "p30_34", "p35_39", "p40_49", "p50_plus",
        "pct_over_30", "pct_over_40",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_RENT_BINS_COUNTY_2020": {
        "state", "county", "computed_renters",
        "lt_10", "p10_14", "p15_19", "p20_24", "p25_29",
        "p30_34", "p35_39", "p40_49", "p50_plus",
        "pct_over_30", "pct_over_40",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_COMMUTE_STATE_2019": {
        "state", "workers_total", "avg_commute_minutes",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_COMMUTE_STATE_2020": {
        "state", "workers_total", "avg_commute_minutes",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_IN_MIGRATION_COUNTY_2019": {
        "state", "county", "pop_1p_total",
        "inflow_any_total", "inflow_any_rate",
        "inflow_outside_total", "inflow_outside_rate",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_IN_MIGRATION_COUNTY_2020": {
        "state", "county", "pop_1p_total",
        "inflow_any_total", "inflow_any_rate",
        "inflow_outside_total", "inflow_outside_rate",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_LANGUAGE_STATE_2019": {
        "state", "total_households",
        "non_english_households", "pct_non_english",
        "limited_english_households", "pct_limited_english",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_LANGUAGE_STATE_2020": {
        "state", "total_households",
        "non_english_households", "pct_non_english",
        "limited_english_households", "pct_limited_english",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_POP_SEX_STATE_2020": {
        "state", "total_pop", "male_pop", "female_pop", "pct_male", "pct_female",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_TENURE_STATE_2020": {
        "state", "occupied_units", "owner_occupied", "renter_occupied", "pct_renter",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_LABOR_STATE_2020": {
        "state", "pop_16_over", "labor_force", "employed", "unemployed", "unemployment_rate",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_RACE_STATE_2020": {
        "state", "total_pop", "white_alone", "black_alone", "asian_alone",
        "aian_alone", "nhpi_alone", "other_race_alone", "two_or_more_races",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_HISPANIC_STATE_2020": {
        "state", "total_pop", "hispanic_latino", "pct_hispanic_latino", "not_hispanic_latino",
    },
    f"{APP_DB}.{APP_SCHEMA}.V_EDU_STATE_2020": {
        "state", "pop_25_over", "bachelors_plus", "pct_bachelors_plus",
    },
}

# Guardrail patterns / keywords
NSFW_PATTERNS = [
    r"\bporn\b",
    r"\bnude\b",
    r"\bfetish\b",
    r"\bblowjob\b",
    r"\bhandjob\b",
    r"\bexplicit\b",
    r"\bxxx\b",
]

BASELINE_TOPIC = (
    "US Census demographics including population, housing, rent burden, commute times, "
    "migration, inflow, language, education, labor force, employment, race, ethnicity, "
    "age, poverty, income, and geographical boundaries like state, county, and city."
)

AFFIRMATIONS = {
    "yes", "y", "yeah", "yep", "sure", "ok", "okay",
    "sounds good", "please", "go ahead", "go on", "continue", "do it",
}