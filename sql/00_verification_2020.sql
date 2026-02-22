-- Join coverage (migration + language)
WITH fips_norm AS (
  SELECT DISTINCT
    LPAD(TO_VARCHAR(TO_NUMBER(STATE_FIPS)), 2, '0') AS state_fips2,
    LPAD(TO_VARCHAR(TO_NUMBER(COUNTY_FIPS)), 3, '0') AS county_fips3
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_METADATA_CBG_FIPS_CODES"
),
base_m AS (
  SELECT COUNT(*) AS rows_migration
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B07"
),
join_m AS (
  SELECT COUNT(*) AS joined_migration
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B07" t
  JOIN fips_norm f
    ON SUBSTR(t."CENSUS_BLOCK_GROUP", 1, 2) = f.state_fips2
   AND SUBSTR(t."CENSUS_BLOCK_GROUP", 3, 3) = f.county_fips3
),
base_l AS (
  SELECT COUNT(*) AS rows_language
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_C16"
),
join_l AS (
  SELECT COUNT(*) AS joined_language
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_C16" t
  JOIN fips_norm f
    ON SUBSTR(t."CENSUS_BLOCK_GROUP", 1, 2) = f.state_fips2
   AND SUBSTR(t."CENSUS_BLOCK_GROUP", 3, 3) = f.county_fips3
)
SELECT * FROM base_m
UNION ALL SELECT * FROM join_m
UNION ALL SELECT * FROM base_l
UNION ALL SELECT * FROM join_l;

-- Sanity checks summary (rent/migration/language/commute)
WITH r AS (
  SELECT
    ("B25070e1" - "B25070e11") AS computed_renters,
    ("B25070e7" + "B25070e8" + "B25070e9" + "B25070e10") AS over_30
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B25"
  WHERE "B25070e1" IS NOT NULL
),
m AS (
  SELECT
    "B07201e1" AS total,
    ("B07201e1" - "B07201e2") AS inflow_any,
    ("B07201e7" + "B07201e13" + "B07201e14") AS inflow_outside
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B07"
  WHERE "B07201e1" IS NOT NULL
),
l AS (
  SELECT
    "C16002e1" AS total_households,
    ("C16002e1" - "C16002e2") AS non_english,
    ("C16002e4" + "C16002e7" + "C16002e10" + "C16002e13") AS limited_english
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_C16"
  WHERE "C16002e1" IS NOT NULL
),
c AS (
  SELECT
    "B08303e1" AS total_workers,
    ("B08303e2" + "B08303e3" + "B08303e4" + "B08303e5" + "B08303e6" + "B08303e7" +
     "B08303e8" + "B08303e9" + "B08303e10" + "B08303e11" + "B08303e12" + "B08303e13") AS bins_sum
  FROM US_OPEN_CENSUS_DATA__NEIGHBORHOOD_INSIGHTS__FREE_DATASET.PUBLIC."2020_CBG_B08"
  WHERE "B08303e1" IS NOT NULL
)
SELECT
  'rent' AS check_name,
  COUNT(*) AS rows_checked,
  COUNT_IF(computed_renters < 0) AS issue_1,
  COUNT_IF(over_30 > computed_renters) AS issue_2,
  COUNT_IF(computed_renters = 0) AS issue_3
FROM r
UNION ALL
SELECT
  'migration' AS check_name,
  COUNT(*) AS rows_checked,
  COUNT_IF(inflow_any > total) AS issue_1,
  COUNT_IF(inflow_outside > inflow_any) AS issue_2,
  COUNT_IF(total = 0) AS issue_3
FROM m
UNION ALL
SELECT
  'language' AS check_name,
  COUNT(*) AS rows_checked,
  COUNT_IF(non_english > total_households) AS issue_1,
  COUNT_IF(limited_english > total_households) AS issue_2,
  COUNT_IF(total_households = 0) AS issue_3
FROM l
UNION ALL
SELECT
  'commute' AS check_name,
  COUNT(*) AS rows_checked,
  COUNT_IF(bins_sum != total_workers AND total_workers != 0) AS issue_1,
  0 AS issue_2,
  COUNT_IF(total_workers = 0) AS issue_3
FROM c;