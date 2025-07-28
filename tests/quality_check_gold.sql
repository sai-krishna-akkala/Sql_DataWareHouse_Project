-- Count of records in gold views
SELECT 
  (SELECT COUNT(*) FROM gold.dim_customers) AS customer_count,
  (SELECT COUNT(*) FROM gold.dim_products) AS product_count,
  (SELECT COUNT(*) FROM gold.fact_sales) AS sales_count;
-- Check for duplicates in customer_key
SELECT customer_key, COUNT(*) 
FROM gold.dim_customers 
GROUP BY customer_key 
HAVING COUNT(*) > 1;

-- Check for duplicates in product_key
SELECT product_key, COUNT(*) 
FROM gold.dim_products 
GROUP BY product_key 
HAVING COUNT(*) > 1;
-- Customers
SELECT * FROM gold.dim_customers WHERE customer_key IS NULL OR customer_id IS NULL;

-- Products
SELECT * FROM gold.dim_products WHERE product_key IS NULL OR product_i IS NULL;

-- Sales
SELECT * FROM gold.fact_sales 
WHERE product_key IS NULL OR customer_key IS NULL OR sls_ord_num IS NULL;
-- Check for missing product keys
SELECT DISTINCT s.sls_prd_key 
FROM silver.crm_sales_details s 
LEFT JOIN gold.dim_products p 
ON s.sls_prd_key = p.product_number 
WHERE p.product_key IS NULL;

-- Check for missing customer keys
SELECT DISTINCT s.sls_cust_id 
FROM silver.crm_sales_details s 
LEFT JOIN gold.dim_customers c 
ON s.sls_cust_id = c.customer_key 
WHERE c.customer_key IS NULL;
-- Gender values
SELECT DISTINCT gender FROM gold.dim_customers;

-- Product line values
SELECT DISTINCT product_line FROM gold.dim_products;

-- Marital status values
SELECT DISTINCT marital_status FROM gold.dim_customers;
-- Sales dates logic
SELECT * 
FROM gold.fact_sales 
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt;

-- Product start date shouldn't be null
SELECT * FROM gold.dim_products WHERE start_date IS NULL;
