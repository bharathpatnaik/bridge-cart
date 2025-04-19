-- Create a schema for our customer data
CREATE SCHEMA IF NOT EXISTS bridgecart_customer_data;

-- ===========================
-- 1. RAW Layer
-- (store data as close to the original as possible)
-- ===========================
CREATE TABLE IF NOT EXISTS bridgecart_customer_data.raw_customers (
  id SERIAL PRIMARY KEY,
  customer_id VARCHAR(255),
  name VARCHAR(255),
  age INT,
  income INT,
  gender VARCHAR(1),
  mobile VARCHAR(50),
  purchase_date TIMESTAMP,
  purchase_amount DECIMAL(10, 2),
  coupon_used BOOLEAN,
  discount_amount DECIMAL(10,2),
  payment_method VARCHAR(50),
  product_category VARCHAR(100),
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata'),
  UNIQUE (customer_id, purchase_date)
);

-- ===========================
-- 2. SILVER Layer
-- (cleaned / conformed data)
-- ===========================
CREATE TABLE IF NOT EXISTS bridgecart_customer_data.silver_customers (
  id SERIAL PRIMARY KEY,
  customer_id VARCHAR(255),
  name VARCHAR(255),
  age INT,
  income INT,
  gender INT, -- 0 or 1 after standardization
  mobile VARCHAR(50),
  purchase_date TIMESTAMP,
  purchase_amount DECIMAL(10,2),
  coupon_used BOOLEAN,
  discount_amount DECIMAL(10,2),
  payment_method VARCHAR(50),
  product_category VARCHAR(100),
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata'),
  UNIQUE (customer_id, purchase_date)
);

-- ===========================
-- 3. GOLD Layer: Segmented Data
-- (stores each customer's final cluster assignment, CLV, etc.)
-- ===========================
CREATE TABLE IF NOT EXISTS bridgecart_customer_data.gold_customers_segments (
  id SERIAL PRIMARY KEY,
  customer_id VARCHAR(255),
  name VARCHAR(255),
  age INT,
  income INT,
  gender INT,
  purchase_date TIMESTAMP,
  purchase_amount DECIMAL(10,2),
  coupon_used BOOLEAN,
  discount_amount DECIMAL(10,2),
  payment_method VARCHAR(50),
  product_category VARCHAR(100),
  clv DECIMAL(10,2),
  churn_flag INT,
  segment INT,
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata'),
  UNIQUE (customer_id, purchase_date)
);

-- ===========================
-- 4. GOLD Layer: Segment Metrics
-- (aggregated metrics per segment)
-- ===========================
CREATE TABLE IF NOT EXISTS bridgecart_customer_data.gold_segment_metrics (
  segment INT PRIMARY KEY,
  avg_clv DECIMAL(10,2),
  churn_rate DECIMAL(5,4),
  aov DECIMAL(10,2),
  segment_contribution DECIMAL(10,4),
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')
);

-- ===========================
-- 5. GOLD Layer: Segment Metrics (SCD2)
-- (historic records of each segment's metrics)
-- ===========================
CREATE TABLE IF NOT EXISTS bridgecart_customer_data.gold_segment_metrics_scd2 (
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

-- ===========================
-- 6. GOLD Layer: Model Metrics
-- (tracks performance of any ML models)
-- ===========================
CREATE TABLE IF NOT EXISTS bridgecart_customer_data.gold_model_metrics (
  id SERIAL PRIMARY KEY,
  model_name VARCHAR(255),
  model_version VARCHAR(50),
  train_date TIMESTAMP,
  train_accuracy DECIMAL(5,4),
  val_accuracy DECIMAL(5,4),
  precision DECIMAL(5,4),
  recall DECIMAL(5,4),
  f1_score DECIMAL(5,4),
  inserted_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')
);
