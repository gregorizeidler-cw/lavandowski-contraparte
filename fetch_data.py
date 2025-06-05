fetch_combined_query = """
WITH TransactionSums AS (
  SELECT
    t.merchant_id AS user_id,
    SUM(t.amount) AS TransactionTotalAmount
  FROM `infinitepay-production.maindb.transactions` t
  WHERE t.status = 'approved'
  GROUP BY t.merchant_id
),

TransactionSums90 AS (
  SELECT
    t.merchant_id AS user_id,
    SUM(t.amount) AS TransactionTotalAmount90
  FROM `infinitepay-production.maindb.transactions` t
  WHERE
    t.status = 'approved'
    AND t.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY t.merchant_id
),

high_value_users AS (
  SELECT user_id
  FROM TransactionSums
  WHERE TransactionTotalAmount >= 1000000
  UNION DISTINCT
  SELECT user_id
  FROM TransactionSums90
  WHERE TransactionTotalAmount90 >= 300000
),

excluded_users AS (
  -- 1) quem teve offense de money_laundering nos últimos 30 dias, COM as condições originais
  SELECT DISTINCT a.user_id
  FROM maindb.offense_analyses a
  JOIN maindb.offenses o
    ON a.offense_id = o.id
  WHERE 
    o.name = 'money_laundering'
    AND (
      (a.conclusion = 'normal'     AND a.priority IN ('low', 'mid', 'high'))
      OR (a.conclusion = 'suspicious' AND a.priority IN ('mid', 'high'))
      OR (a.conclusion = 'offense'    AND a.priority IN ('mid', 'high'))
    )
    AND a.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 DAY)
    AND a.automatic_pipeline = TRUE

  UNION DISTINCT

  -- 2) high-value users
  SELECT user_id
  FROM high_value_users
),

traditional_alerts AS (
  SELECT
    DISTINCT an.user_id,
    FORMAT_TIMESTAMP('%d-%m-%Y', an.created_at) AS alert_date,
    CASE
      WHEN an.analyst_id = 8423054  THEN 'CH Alert'
      WHEN an.analyst_id = 8832903  THEN 'Pep_Pix Alert'
      WHEN an.analyst_id = 15858378 THEN 'GAFI Alert'
      WHEN an.analyst_id = 16368511 THEN 'Merchant_Pix Alert'
      WHEN an.analyst_id = 18758930 THEN 'International_Cards_Alert'
      WHEN an.analyst_id = 19897830 THEN 'Bank_Slips_Alert'
      WHEN an.analyst_id = 20583019 THEN 'Goverment_Corporate_Cards_Alert'
      WHEN an.analyst_id = 20698248 THEN 'Betting_Houses_Alert'
      WHEN an.analyst_id = 25071066 THEN 'GAFI Alert [US]'
      WHEN an.analyst_id = 25261377 THEN 'international_cards_alert [US]'
      WHEN an.analyst_id = 24954170 THEN 'ted_transfers_alert'
      WHEN an.analyst_id = 34767121 THEN 'Pf_Merchant_Pix Alert'
      WHEN an.analyst_id = 25769012 THEN 'Issuing Transactions Alert'
      WHEN an.analyst_id = 27951634 THEN 'Foreigners_Alert'
      WHEN an.analyst_id = 28279057 THEN 'acquiring_jim_us_alert [US]'
      WHEN an.analyst_id = 28320827 THEN 'aml_acquiring_prohibited_countries_jim_us_alert [US]'
      WHEN an.analyst_id = 29865856 THEN 'international_location_attempts_alert'
      WHEN an.analyst_id = 29842685 THEN 'aml_prison_areas_alert'
      WHEN an.analyst_id = 30046553 THEN 'aml_pix_change_atm_alert'
      WHEN an.analyst_id = 29840096 THEN 'aml_blocked_contacts_alert'
    END AS alert_type,
    CAST(NULL      AS FLOAT64) AS score,
    CAST(NULL      AS STRING ) AS features
  FROM `infinitepay-production.maindb.offense_analyses` an
  JOIN `infinitepay-production.maindb.offenses` o
    ON an.offense_id = o.id
  LEFT JOIN `infinitepay-production.maindb.offense_actions` act
    ON act.offense_analysis_id = an.id
  WHERE 
    o.name = 'money_laundering'
    AND an.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND an.analyst_id IN (
      8423054, 8832903, 15858378, 16368511, 18758930,
      19897830, 20583019, 20698248, 25071066, 25261377,
      24954170, 25769012, 27951634, 28279057, 28320827,
      29865856, 29842685, 30046553, 29840096, 34767121
    )
),

ai_alerts AS (
  SELECT
    user_id,
    FORMAT_TIMESTAMP('%d-%m-%Y', TIMESTAMP(timestamp)) AS alert_date,
    'AI Alert'               AS alert_type,
    CAST(score   AS FLOAT64) AS score,
    CAST(features AS STRING ) AS features
  FROM `ai-services-sae.aml_model.predictions`
  WHERE
    timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND label = 1
),

all_alerts AS (
  SELECT * FROM traditional_alerts
  UNION ALL
  SELECT * FROM ai_alerts
)

SELECT *
FROM all_alerts
WHERE user_id NOT IN (SELECT user_id FROM excluded_users)
ORDER BY alert_date DESC;

""" 
