-- Verification queries (run during development)

-- Join coverage (rent) with padded FIPS
WITH fips_norm AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    LPAD(TO_VARCHAR(TO_NUMBER(COUNTY_FIPS)), 3, '0') AS county_fips3
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
),
base AS (
  SELECT COUNT(*) AS cbg_rows
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_RENT_PERCENTAGE_HOUSEHOLD_INCOME"
),
joined AS (
  SELECT COUNT(*) AS joined_rows
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_RENT_PERCENTAGE_HOUSEHOLD_INCOME" r
  JOIN fips_norm f
    ON SUBSTR(r."CENSUS_BLOCK_GROUP", 1, 2) = f.state_fips2
   AND SUBSTR(r."CENSUS_BLOCK_GROUP", 3, 3) = f.county_fips3
)
SELECT * FROM base
UNION ALL
SELECT * FROM joined;

-- Join coverage (migration)
WITH fips_norm AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    LPAD(TO_VARCHAR(TO_NUMBER(COUNTY_FIPS)), 3, '0') AS county_fips3
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
),
base AS (
  SELECT COUNT(*) AS cbg_rows
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B07"
),
joined AS (
  SELECT COUNT(*) AS joined_rows
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B07" m
  JOIN fips_norm f
    ON SUBSTR(m."CENSUS_BLOCK_GROUP", 1, 2) = f.state_fips2
   AND SUBSTR(m."CENSUS_BLOCK_GROUP", 3, 3) = f.county_fips3
)
SELECT * FROM base
UNION ALL
SELECT * FROM joined;

-- Join coverage (language)
WITH fips_norm AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    LPAD(TO_VARCHAR(TO_NUMBER(COUNTY_FIPS)), 3, '0') AS county_fips3
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_METADATA_CBG_FIPS_CODES"
),
base AS (
  SELECT COUNT(*) AS cbg_rows
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_C16"
),
joined AS (
  SELECT COUNT(*) AS joined_rows
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_C16" l
  JOIN fips_norm f
    ON SUBSTR(l."CENSUS_BLOCK_GROUP", 1, 2) = f.state_fips2
   AND SUBSTR(l."CENSUS_BLOCK_GROUP", 3, 3) = f.county_fips3
)
SELECT * FROM base
UNION ALL
SELECT * FROM joined;

-- Rent sanity: numerator <= denom, denom >= 0
WITH r AS (
  SELECT
    "Total: Renter-occupied housing units" - "Not computed: Renter-occupied housing units" AS computed_renters,
    (
      "30.0 to 34.9 percent: Renter-occupied housing units" +
      "35.0 to 39.9 percent: Renter-occupied housing units" +
      "40.0 to 49.9 percent: Renter-occupied housing units" +
      "50.0 percent or more: Renter-occupied housing units"
    ) AS over_30
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_RENT_PERCENTAGE_HOUSEHOLD_INCOME"
)
SELECT
  COUNT(*) AS rows_checked,
  COUNT_IF(computed_renters < 0) AS neg_denoms,
  COUNT_IF(over_30 < 0) AS neg_num,
  COUNT_IF(over_30 > computed_renters) AS num_gt_denom,
  COUNT_IF(computed_renters = 0) AS zero_denoms
FROM r;

-- Migration sanity: outside <= any <= total
WITH m AS (
  SELECT
    "B07201e1" AS total,
    ("B07201e1" - "B07201e2") AS inflow_any,
    ("B07201e7" + "B07201e13" + "B07201e14") AS inflow_outside
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B07"
  WHERE "B07201e1" IS NOT NULL
)
SELECT
  COUNT(*) AS rows_checked,
  COUNT_IF(inflow_any < 0) AS neg_inflow_any,
  COUNT_IF(inflow_any > total) AS inflow_any_gt_total,
  COUNT_IF(inflow_outside < 0) AS neg_inflow_outside,
  COUNT_IF(inflow_outside > inflow_any) AS inflow_outside_gt_inflow_any,
  COUNT_IF(total = 0) AS zero_total
FROM m;

-- Language sanity
WITH l AS (
  SELECT
    "C16002e1" AS total_households,
    ("C16002e1" - "C16002e2") AS non_english,
    ("C16002e4" + "C16002e7" + "C16002e10" + "C16002e13") AS limited_english
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_C16"
  WHERE "C16002e1" IS NOT NULL
)
SELECT
  COUNT(*) AS rows_checked,
  COUNT_IF(non_english > total_households) AS non_english_gt_total,
  COUNT_IF(limited_english > total_households) AS limited_gt_total,
  COUNT_IF(total_households = 0) AS zero_total
FROM l;

-- Commute sanity: sum of bins equals total
WITH c AS (
  SELECT
    "B08303e1" AS total_workers,
    ("B08303e2" + "B08303e3" + "B08303e4" + "B08303e5" + "B08303e6" + "B08303e7" +
     "B08303e8" + "B08303e9" + "B08303e10" + "B08303e11" + "B08303e12" + "B08303e13") AS bins_sum
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2019_CBG_B08"
  WHERE "B08303e1" IS NOT NULL
)
SELECT
  COUNT(*) AS rows_checked,
  COUNT_IF(bins_sum != total_workers AND total_workers != 0) AS mismatch_rows,
  COUNT_IF(total_workers = 0) AS zero_total
FROM c;