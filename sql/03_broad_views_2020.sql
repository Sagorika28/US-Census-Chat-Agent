-- Broad curated views (2020) for common “tweaked” census questions
-- Target schema: SNOWFLAKE_LEARNING_DB.PUBLIC

-- 1) Population by sex (state)
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_POP_SEX_STATE_2020 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_METADATA_CBG_FIPS_CODES"
),
cbg_pop AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B01001e1" AS total_pop,
    "B01001e2" AS male_pop,
    "B01001e26" AS female_pop
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B01"
  WHERE "B01001e1" IS NOT NULL AND "B01001e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(p.total_pop) AS total_pop,
    SUM(p.male_pop) AS male_pop,
    SUM(p.female_pop) AS female_pop,
    SUM(p.male_pop) / NULLIF(SUM(p.total_pop), 0) AS pct_male,
    SUM(p.female_pop) / NULLIF(SUM(p.total_pop), 0) AS pct_female
  FROM cbg_pop p
  JOIN state_dim s
    ON SUBSTR(p.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;

-- 2) Housing tenure (state)
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_TENURE_STATE_2020 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_METADATA_CBG_FIPS_CODES"
),
cbg_tenure AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B25003e1" AS occupied_units,
    "B25003e2" AS owner_occupied,
    "B25003e3" AS renter_occupied
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B25"
  WHERE "B25003e1" IS NOT NULL AND "B25003e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(t.occupied_units) AS occupied_units,
    SUM(t.owner_occupied) AS owner_occupied,
    SUM(t.renter_occupied) AS renter_occupied,
    SUM(t.renter_occupied) / NULLIF(SUM(t.occupied_units), 0) AS pct_renter
  FROM cbg_tenure t
  JOIN state_dim s
    ON SUBSTR(t.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;

-- 3) Labor force + unemployment (state)
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_LABOR_STATE_2020 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_METADATA_CBG_FIPS_CODES"
),
cbg_labor AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B23025e1" AS pop_16_over,
    "B23025e2" AS labor_force,
    "B23025e4" AS employed,
    "B23025e5" AS unemployed
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B23"
  WHERE "B23025e1" IS NOT NULL AND "B23025e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(l.pop_16_over) AS pop_16_over,
    SUM(l.labor_force) AS labor_force,
    SUM(l.employed) AS employed,
    SUM(l.unemployed) AS unemployed,
    SUM(l.unemployed) / NULLIF(SUM(l.labor_force), 0) AS unemployment_rate
  FROM cbg_labor l
  JOIN state_dim s
    ON SUBSTR(l.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;

-- 4) Race composition (state)
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_RACE_STATE_2020 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_METADATA_CBG_FIPS_CODES"
),
cbg_race AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B02001e1" AS total_pop,
    "B02001e2" AS white_alone,
    "B02001e3" AS black_alone,
    "B02001e5" AS asian_alone,
    "B02001e4" AS american_indian_alaska_native_alone,
    "B02001e6" AS native_hawaiian_pacific_islander_alone,
    "B02001e7" AS some_other_race_alone,
    "B02001e8" AS two_or_more_races
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B02"
  WHERE "B02001e1" IS NOT NULL AND "B02001e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(r.total_pop) AS total_pop,
    SUM(r.white_alone) AS white_alone,
    SUM(r.black_alone) AS black_alone,
    SUM(r.asian_alone) AS asian_alone,
    SUM(r.american_indian_alaska_native_alone) AS aian_alone,
    SUM(r.native_hawaiian_pacific_islander_alone) AS nhpi_alone,
    SUM(r.some_other_race_alone) AS other_race_alone,
    SUM(r.two_or_more_races) AS two_or_more_races
  FROM cbg_race r
  JOIN state_dim s
    ON SUBSTR(r.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;

-- 5) Hispanic/Latino composition (state)
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_HISPANIC_STATE_2020 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_METADATA_CBG_FIPS_CODES"
),
cbg_hisp AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B03002e1" AS total_pop,
    "B03002e12" AS hispanic_latino,
    ("B03002e1" - "B03002e12") AS not_hispanic_latino
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B03"
  WHERE "B03002e1" IS NOT NULL AND "B03002e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(h.total_pop) AS total_pop,
    SUM(h.hispanic_latino) AS hispanic_latino,
    SUM(h.hispanic_latino) / NULLIF(SUM(h.total_pop), 0) AS pct_hispanic_latino,
    SUM(h.not_hispanic_latino) AS not_hispanic_latino
  FROM cbg_hisp h
  JOIN state_dim s
    ON SUBSTR(h.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;

-- 6) Education: Bachelor's or higher (age 25+) (state)
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_EDU_STATE_2020 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_METADATA_CBG_FIPS_CODES"
),
cbg_edu AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B15003e1" AS pop_25_over,
    ("B15003e22" + "B15003e23" + "B15003e24" + "B15003e25") AS bachelors_plus
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B15"
  WHERE "B15003e1" IS NOT NULL AND "B15003e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(e.pop_25_over) AS pop_25_over,
    SUM(e.bachelors_plus) AS bachelors_plus,
    SUM(e.bachelors_plus) / NULLIF(SUM(e.pop_25_over), 0) AS pct_bachelors_plus
  FROM cbg_edu e
  JOIN state_dim s
    ON SUBSTR(e.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;