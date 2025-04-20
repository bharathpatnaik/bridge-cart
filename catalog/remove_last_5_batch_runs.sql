-- 1) Remove last 5 batch runs from RAW
DELETE FROM bridgecart_customer_data.raw_customers
WHERE inserted_at IN (
  SELECT DISTINCT inserted_at
  FROM bridgecart_customer_data.raw_customers
  ORDER BY inserted_at DESC
  LIMIT 5
);

-- 2) Remove last 5 batch runs from SILVER
DELETE FROM bridgecart_customer_data.silver_customers
WHERE inserted_at IN (
  SELECT DISTINCT inserted_at
  FROM bridgecart_customer_data.silver_customers
  ORDER BY inserted_at DESC
  LIMIT 5
);

-- 3) Remove last 5 batch runs from GOLD (Segments)
DELETE FROM bridgecart_customer_data.gold_customers_segments
WHERE inserted_at IN (
  SELECT DISTINCT inserted_at
  FROM bridgecart_customer_data.gold_customers_segments
  ORDER BY inserted_at DESC
  LIMIT 5
);

-- 4) Remove last 5 batch runs from GOLD (SCD2)
DELETE FROM bridgecart_customer_data.gold_segment_metrics_scd2
WHERE scd_start_date IN (
  SELECT DISTINCT scd_start_date
  FROM bridgecart_customer_data.gold_segment_metrics_scd2
  ORDER BY scd_start_date DESC
  LIMIT 5
);

-- 5) Remove last 5 batch runs from Model Metrics
DELETE FROM bridgecart_customer_data.gold_model_metrics
WHERE inserted_at IN (
  SELECT DISTINCT inserted_at
  FROM bridgecart_customer_data.gold_model_metrics
  ORDER BY inserted_at DESC
  LIMIT 5
);
