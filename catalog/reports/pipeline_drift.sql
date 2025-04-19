-- ================================================
-- 1. raw_info CTE
--    Retrieves information about the latest "run"
--    from the raw_customers table, including:
--    - A formatted timestamp of the MAX(inserted_at)
--    - How many records were inserted BEFORE that timestamp
--    - How many records were inserted AT that timestamp
-- ================================================
WITH raw_info AS (
  SELECT
    -- Format the maximum inserted_at as "MM/DD Day, HH12:MI:SS PM"
    to_char(MAX(inserted_at), 'MM/DD FMDay, HH12:MI:SS PM') AS last_run,

    -- Count all rows whose inserted_at is less than the overall max(inserted_at) => "customer_count_before_run"
    COUNT(*) FILTER (
      WHERE inserted_at < (SELECT MAX(inserted_at) FROM bridgecart_customer_data.raw_customers)
    ) AS customer_count_before_run,

    -- Count all rows whose inserted_at is exactly equal to the max(inserted_at) => "customer_count_after_run"
    COUNT(*) FILTER (
      WHERE inserted_at = (SELECT MAX(inserted_at) FROM bridgecart_customer_data.raw_customers)
    ) AS customer_count_after_run
  FROM bridgecart_customer_data.raw_customers
),

-- ================================================
-- 2. silver_info CTE
--    Same pattern as above, but for silver_customers.
-- ================================================
silver_info AS (
  SELECT
    to_char(MAX(inserted_at), 'MM/DD FMDay, HH12:MI:SS PM') AS last_run,
    COUNT(*) FILTER (
      WHERE inserted_at < (SELECT MAX(inserted_at) FROM bridgecart_customer_data.silver_customers)
    ) AS customer_count_before_run,
    COUNT(*) FILTER (
      WHERE inserted_at = (SELECT MAX(inserted_at) FROM bridgecart_customer_data.silver_customers)
    ) AS customer_count_after_run
  FROM bridgecart_customer_data.silver_customers
),

-- ================================================
-- 3. gold_info CTE
--    Same pattern, but for gold_customers_segments.
-- ================================================
gold_info AS (
  SELECT
    to_char(MAX(inserted_at), 'MM/DD FMDay, HH12:MI:SS PM') AS last_run,
    COUNT(*) FILTER (
      WHERE inserted_at < (SELECT MAX(inserted_at) FROM bridgecart_customer_data.gold_customers_segments)
    ) AS customer_count_before_run,
    COUNT(*) FILTER (
      WHERE inserted_at = (SELECT MAX(inserted_at) FROM bridgecart_customer_data.gold_customers_segments)
    ) AS customer_count_after_run
  FROM bridgecart_customer_data.gold_customers_segments
),

-- ================================================
-- 4. drift_info CTE
--    Checks the SCD2 table for gold segment metrics
--    and calculates the difference in avg_clv between
--    the current row and the previous row (lag).
--    This is considered the "CLV drift".
-- ================================================
drift_info AS (
  SELECT
    (avg_clv - LAG(avg_clv) OVER (ORDER BY id)) AS clv_drift
  FROM bridgecart_customer_data.gold_segment_metrics_scd2
  WHERE scd_is_current = TRUE
  ORDER BY id DESC
  LIMIT 1
)

-- ================================================
-- Final SELECT:
-- - We label each row ("Raw Layer", "Silver Layer", "Gold Layer").
-- - For Raw and Silver, we do NOT have CLV drift, so we put NULL.
-- - For Gold, we CROSS JOIN with drift_info so we can attach that drift value.
-- - Then we UNION them all together in the final result set.
-- ================================================
SELECT
  'Raw Layer' AS data_layer,
  r.last_run,
  r.customer_count_before_run,
  r.customer_count_after_run,
  NULL::numeric AS clv_drift
FROM raw_info r

UNION ALL

SELECT
  'Silver Layer' AS data_layer,
  s.last_run,
  s.customer_count_before_run,
  s.customer_count_after_run,
  NULL::numeric AS clv_drift
FROM silver_info s

UNION ALL

SELECT
  'Gold Layer' AS data_layer,
  g.last_run,
  g.customer_count_before_run,
  g.customer_count_after_run,
  d.clv_drift
FROM gold_info g
CROSS JOIN drift_info d;
