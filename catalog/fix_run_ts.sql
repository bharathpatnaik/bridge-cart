-- EXAMPLE APPROACH:
-- We will re-map each DISTINCT timestamp to an evenly spaced series (e.g., 2‑hour intervals),
-- then update the rows to the new values. Adjust the base time, interval, or logic as needed.

------------------------------------------------------------------------------
-- 1) RAW Layer
------------------------------------------------------------------------------
WITH distinct_raw AS (
    -- 1. Collect each distinct inserted_at and assign row numbers
    SELECT
        inserted_at AS old_ts,
        ROW_NUMBER() OVER (ORDER BY inserted_at) AS rn
    FROM (
        SELECT DISTINCT inserted_at
        FROM bridgecart_customer_data.raw_customers
    ) t
),
mapped_raw AS (
    -- 2. Generate a new timestamp for each row number.
    --    Here, we start at '2025-04-19 17:00:00' and add (rn-1)*2 hours.
    SELECT
        old_ts,
        (TIMESTAMP '2025-04-19 17:00:00'
         + (rn-1) * INTERVAL '2 hours') AS new_ts
    FROM distinct_raw
)
UPDATE bridgecart_customer_data.raw_customers r
SET inserted_at = m.new_ts
FROM mapped_raw m
WHERE r.inserted_at = m.old_ts;

------------------------------------------------------------------------------
-- 2) SILVER Layer
------------------------------------------------------------------------------
WITH distinct_silver AS (
    SELECT
        inserted_at AS old_ts,
        ROW_NUMBER() OVER (ORDER BY inserted_at) AS rn
    FROM (
        SELECT DISTINCT inserted_at
        FROM bridgecart_customer_data.silver_customers
    ) t
),
mapped_silver AS (
    SELECT
        old_ts,
        (TIMESTAMP '2025-04-19 17:00:00'
         + (rn-1) * INTERVAL '2 hours') AS new_ts
    FROM distinct_silver
)
UPDATE bridgecart_customer_data.silver_customers s
SET inserted_at = m.new_ts
FROM mapped_silver m
WHERE s.inserted_at = m.old_ts;

------------------------------------------------------------------------------
-- 3) GOLD Layer (Segments)
------------------------------------------------------------------------------
WITH distinct_gold AS (
    SELECT
        inserted_at AS old_ts,
        ROW_NUMBER() OVER (ORDER BY inserted_at) AS rn
    FROM (
        SELECT DISTINCT inserted_at
        FROM bridgecart_customer_data.gold_customers_segments
    ) t
),
mapped_gold AS (
    SELECT
        old_ts,
        (TIMESTAMP '2025-04-19 17:00:00'
         + (rn-1) * INTERVAL '2 hours') AS new_ts
    FROM distinct_gold
)
UPDATE bridgecart_customer_data.gold_customers_segments g
SET inserted_at = m.new_ts
FROM mapped_gold m
WHERE g.inserted_at = m.old_ts;

------------------------------------------------------------------------------
-- 4) GOLD Layer (SCD2) - scd_start_date
------------------------------------------------------------------------------
-- If you want to shift scd_start_date in 2-hour increments
WITH distinct_scd_start AS (
    SELECT
        scd_start_date AS old_ts,
        ROW_NUMBER() OVER (ORDER BY scd_start_date) AS rn
    FROM (
        SELECT DISTINCT scd_start_date
        FROM bridgecart_customer_data.gold_segment_metrics_scd2
        WHERE scd_start_date IS NOT NULL
    ) t
),
mapped_scd_start AS (
    SELECT
        old_ts,
        (TIMESTAMP '2025-04-19 17:00:00'
         + (rn-1) * INTERVAL '2 hours') AS new_ts
    FROM distinct_scd_start
)
UPDATE bridgecart_customer_data.gold_segment_metrics_scd2 s
SET scd_start_date = m.new_ts
FROM mapped_scd_start m
WHERE s.scd_start_date = m.old_ts;

------------------------------------------------------------------------------
-- 5) GOLD Layer (SCD2) - scd_end_date
------------------------------------------------------------------------------
-- Similar approach for scd_end_date if it's not NULL
WITH distinct_scd_end AS (
    SELECT
        scd_end_date AS old_ts,
        ROW_NUMBER() OVER (ORDER BY scd_end_date) AS rn
    FROM (
        SELECT DISTINCT scd_end_date
        FROM bridgecart_customer_data.gold_segment_metrics_scd2
        WHERE scd_end_date IS NOT NULL
    ) t
),
mapped_scd_end AS (
    SELECT
        old_ts,
        (TIMESTAMP '2025-04-19 17:00:00'
         + (rn-1) * INTERVAL '2 hours') AS new_ts
    FROM distinct_scd_end
)
UPDATE bridgecart_customer_data.gold_segment_metrics_scd2 s
SET scd_end_date = m.new_ts
FROM mapped_scd_end m
WHERE s.scd_end_date = m.old_ts;

------------------------------------------------------------------------------

-- NOTES:
-- 1) Adjust the base time ('2025-04-19 17:00:00') and interval ('2 hours') to whatever spacing
--    you want for your chart (e.g., '4 hours', '12 hours', etc.).
-- 2) For scd_end_date = NULL rows, we leave them unchanged.
-- 3) This approach re-maps the distinct timestamps in ascending order, so your chart's x-axis
--    will appear more evenly spaced.



