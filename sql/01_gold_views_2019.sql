-- Gold curated views (2019) used by the app
-- Target schema: SNOWFLAKE_LEARNING_DB.PUBLIC

-- Migration (county/county-equivalent) with Metric A + Metric B
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_IN_MIGRATION_COUNTY_2019 AS
WITH fips_norm AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    LPAD(TO_VARCHAR(TO_NUMBER(COUNTY_FIPS)), 3, '0') AS county_fips3,
    STATE,
    COUNTY
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
),
cbg_inflow AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B07201e1" AS pop_1p_total,
    ("B07201e1" - "B07201e2") AS inflow_any,
    ("B07201e7" + "B07201e13" + "B07201e14") AS inflow_outside
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B07"
  WHERE "B07201e1" IS NOT NULL AND "B07201e1" > 0
),
county_rollup AS (
  SELECT
    f.STATE,
    f.COUNTY,
    SUM(i.pop_1p_total) AS pop_1p_total,
    SUM(i.inflow_any) AS inflow_any_total,
    SUM(i.inflow_outside) AS inflow_outside_total,
    SUM(i.inflow_any) / NULLIF(SUM(i.pop_1p_total), 0) AS inflow_any_rate,
    SUM(i.inflow_outside) / NULLIF(SUM(i.pop_1p_total), 0) AS inflow_outside_rate
  FROM cbg_inflow i
  JOIN fips_norm f
    ON SUBSTR(i.cbg, 1, 2) = f.state_fips2
   AND SUBSTR(i.cbg, 3, 3) = f.county_fips3
  GROUP BY f.STATE, f.COUNTY
)
SELECT * FROM county_rollup;

-- Language (state) from household language table
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_LANGUAGE_STATE_2019 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
),
cbg_lang AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "C16002e1" AS total_households,
    "C16002e2" AS english_only,
    ("C16002e1" - "C16002e2") AS non_english_households,
    ("C16002e4" + "C16002e7" + "C16002e10" + "C16002e13") AS limited_english_households
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_C16"
  WHERE "C16002e1" IS NOT NULL AND "C16002e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(l.total_households) AS total_households,
    SUM(l.non_english_households) AS non_english_households,
    SUM(l.non_english_households) / NULLIF(SUM(l.total_households), 0) AS pct_non_english,
    SUM(l.limited_english_households) AS limited_english_households,
    SUM(l.limited_english_households) / NULLIF(SUM(l.total_households), 0) AS pct_limited_english
  FROM cbg_lang l
  JOIN state_dim s
    ON SUBSTR(l.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;

-- Commute (state) estimated average from bins
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_COMMUTE_STATE_2019 AS
WITH state_dim AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    STATE
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
),
cbg_commute AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "B08303e1" AS workers_total,
    (
      2.5  * "B08303e2"  +  7.0  * "B08303e3"  + 12.0 * "B08303e4"  + 17.0 * "B08303e5" +
      22.0 * "B08303e6"  + 27.0 * "B08303e7"  + 32.0 * "B08303e8"  + 37.0 * "B08303e9" +
      42.0 * "B08303e10" + 52.0 * "B08303e11" + 74.5 * "B08303e12" + 100.0 * "B08303e13"
    ) AS weighted_minutes_sum
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B08"
  WHERE "B08303e1" IS NOT NULL AND "B08303e1" > 0
),
state_rollup AS (
  SELECT
    s.STATE,
    SUM(c.workers_total) AS workers_total,
    SUM(c.weighted_minutes_sum) / NULLIF(SUM(c.workers_total), 0) AS avg_commute_minutes
  FROM cbg_commute c
  JOIN state_dim s
    ON SUBSTR(c.cbg, 1, 2) = s.state_fips2
  GROUP BY s.STATE
)
SELECT * FROM state_rollup;

-- Rent bins + pct_over_30 (county)
CREATE OR REPLACE VIEW SNOWFLAKE_LEARNING_DB.PUBLIC.V_RENT_BINS_COUNTY_2019 AS
WITH fips_norm AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    LPAD(TO_VARCHAR(TO_NUMBER(COUNTY_FIPS)), 3, '0') AS county_fips3,
    STATE,
    COUNTY
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
),
cbg_rent AS (
  SELECT
    "CENSUS_BLOCK_GROUP" AS cbg,
    "Total: Renter-occupied housing units" AS total_renters,
    "Not computed: Renter-occupied housing units" AS not_computed,
    "Less than 10.0 percent: Renter-occupied housing units" AS lt_10,
    "10.0 to 14.9 percent: Renter-occupied housing units" AS p10_14,
    "15.0 to 19.9 percent: Renter-occupied housing units" AS p15_19,
    "20.0 to 24.9 percent: Renter-occupied housing units" AS p20_24,
    "25.0 to 29.9 percent: Renter-occupied housing units" AS p25_29,
    "30.0 to 34.9 percent: Renter-occupied housing units" AS p30_34,
    "35.0 to 39.9 percent: Renter-occupied housing units" AS p35_39,
    "40.0 to 49.9 percent: Renter-occupied housing units" AS p40_49,
    "50.0 percent or more: Renter-occupied housing units" AS p50_plus
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_RENT_PERCENTAGE_HOUSEHOLD_INCOME"
  WHERE "Total: Renter-occupied housing units" IS NOT NULL
    AND "Total: Renter-occupied housing units" > 0
),
county_rollup AS (
  SELECT
    f.STATE,
    f.COUNTY,
    SUM(total_renters - not_computed) AS computed_renters,
    SUM(lt_10) AS lt_10,
    SUM(p10_14) AS p10_14,
    SUM(p15_19) AS p15_19,
    SUM(p20_24) AS p20_24,
    SUM(p25_29) AS p25_29,
    SUM(p30_34) AS p30_34,
    SUM(p35_39) AS p35_39,
    SUM(p40_49) AS p40_49,
    SUM(p50_plus) AS p50_plus,
    (SUM(p30_34)+SUM(p35_39)+SUM(p40_49)+SUM(p50_plus)) / NULLIF(SUM(total_renters - not_computed),0) AS pct_over_30,
    (SUM(p40_49)+SUM(p50_plus)) / NULLIF(SUM(total_renters - not_computed),0) AS pct_over_40
  FROM cbg_rent r
  JOIN fips_norm f
    ON SUBSTR(r.cbg, 1, 2) = f.state_fips2
   AND SUBSTR(r.cbg, 3, 3) = f.county_fips3
  GROUP BY f.STATE, f.COUNTY
)
SELECT * FROM county_rollup;