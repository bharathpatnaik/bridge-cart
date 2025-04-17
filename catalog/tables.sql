-- ===========================
-- 1. RAW Layer
-- ===========================
CREATE TABLE IF NOT EXISTS raw_customers (
  id SERIAL PRIMARY KEY,
  customer_id VARCHAR(255),
  name VARCHAR(255),
  age INT,
  income INT,
  gender VARCHAR(1),
  mobile VARCHAR(50),
  purchase_date TIMESTAMP,
  purchase_amount DECIMAL(10, 2),
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata'),
  UNIQUE (customer_id, purchase_date)  -- uniqueness constraint
);

-- ===========================
-- 2. SILVER Layer
-- ===========================
CREATE TABLE IF NOT EXISTS silver_customers (
  id SERIAL PRIMARY KEY,
  customer_id VARCHAR(255),
  name VARCHAR(255),
  age INT,
  income INT,
  gender INT, -- 0 or 1 after standardization
  mobile VARCHAR(50),
  purchase_date TIMESTAMP,
  purchase_amount DECIMAL(10,2),
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata'),
  UNIQUE (customer_id, purchase_date)
);

-- ===========================
-- 3. GOLD Layer: Segmented Data
-- ===========================
CREATE TABLE IF NOT EXISTS gold_customers_segments (
  id SERIAL PRIMARY KEY,
  customer_id VARCHAR(255),
  name VARCHAR(255),
  age INT,
  income INT,
  gender INT,
  purchase_date TIMESTAMP,
  purchase_amount DECIMAL(10,2),
  clv DECIMAL(10,2),
  churn_flag INT,
  segment INT,
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata'),
  UNIQUE (customer_id, purchase_date)
);

-- ===========================
-- 4. GOLD Layer: Segment Metrics
-- ===========================
CREATE TABLE IF NOT EXISTS gold_segment_metrics (
  segment INT PRIMARY KEY,
  avg_clv DECIMAL(10,2),
  churn_rate DECIMAL(5,4),
  aov DECIMAL(10,2),
  segment_contribution DECIMAL(10,4),
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')
);


CREATE TABLE IF NOT EXISTS gold_segment_metrics_scd2 (
    id SERIAL PRIMARY KEY,
    segment INT NOT NULL,
    avg_clv DECIMAL(10, 2),
    churn_rate DECIMAL(5, 4),
    aov DECIMAL(10, 2),
    segment_contribution DECIMAL(10, 4),
    scd_start_date TIMESTAMP,
    scd_end_date TIMESTAMP,
    scd_is_current BOOLEAN DEFAULT TRUE
);

