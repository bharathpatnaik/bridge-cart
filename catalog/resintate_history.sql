UPDATE bridgecart_customer_data.gold_customers_segments AS g
SET
  coupon_used      = s.coupon_used,
  discount_amount  = s.discount_amount,
  payment_method   = s.payment_method,
  product_category = s.product_category
FROM bridgecart_customer_data.silver_customers AS s
WHERE g.customer_id = s.customer_id
  AND g.purchase_date = s.purchase_date
  AND (
    g.coupon_used IS NULL
    OR g.discount_amount IS NULL
    OR g.payment_method IS NULL
    OR g.product_category IS NULL
  );
