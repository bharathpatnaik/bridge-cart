WITH raw_info AS (
  SELECT
    to_char(MAX(inserted_at), 'MM/DD FMDay, HH12:MI:SS PM') AS last_run,
    COUNT(*) FILTER (
      WHERE inserted_at < (SELECT MAX(inserted_at) FROM bridgecart_customer_data.raw_customers)
    ) AS customer_count_before_run,
    COUNT(*) FILTER (
      WHERE inserted_at = (SELECT MAX(inserted_at) FROM bridgecart_customer_data.raw_customers)
    ) AS customer_count_after_run
  FROM bridgecart_customer_data.raw_customers
),
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
drift_info AS (
  SELECT
    (avg_clv - LAG(avg_clv) OVER (ORDER BY id)) AS clv_drift
  FROM bridgecart_customer_data.gold_segment_metrics_scd2
  WHERE scd_is_current = TRUE
  ORDER BY id DESC
  LIMIT 1
)
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
