/*****************************************************************************************
-- Purpose        : To validate and report data quality issues in the Bronze Layer tables
-- Description    : This script contains data profiling and quality checks performed 
                   before applying transformations and loading into Silver Layer.

-- Quality Checks Performed:
==========================================================================================
bronze.crm_cust_info:
  - Check for NULL customer IDs (violates primary key)
  - Identify duplicate customer IDs
  - Detect leading/trailing spaces in first/last names
  - Identify inconsistent or missing gender values
  - Identify inconsistent or missing marital status values

bronze.crm_prd_info:
  - Check for NULL or improperly formatted product keys
  - Detect invalid or unknown prd_line codes
  - Identify NULL or negative product cost values

bronze.crm_sales_details:
  - Detect incorrect date formats in order/ship/due dates
  - Identify rows where sales ≠ quantity * ABS(price)
  - Check for NULL or invalid quantity and price values

bronze.erp_cust_az12:
  - Detect 'NAS' prefixes in customer IDs
  - Identify birthdates in the future
  - Normalize inconsistent gender entries

bronze.erp_loc_a101:
  - Check for underscores in customer IDs
  - Standardize inconsistent country codes (e.g., 'US', 'USA', 'DE')

bronze.erp_px_cat_g1v2:
  - Validate presence of NULLs (data considered good)

-- Usage Notes:
  ⚠ This script is intended for diagnostic purposes prior to ETL execution.
  ⚠ It helps in profiling raw data and preparing transformation logic.

******************************************************************************************/

-- Check for null customer IDs (Primary Key issue)
SELECT COUNT(*) AS Null_Customer_IDs
FROM bronze.crm_cust_info
WHERE cst_id IS NULL;

-- Check for duplicate customer IDs (Should be unique)
SELECT cst_id, COUNT(*) AS Duplicate_Count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check for leading/trailing spaces in names
SELECT *
FROM bronze.crm_cust_info
WHERE cst_firstname LIKE ' %' OR cst_firstname LIKE '% ' 
   OR cst_lastname LIKE ' %' OR cst_lastname LIKE '% ';

-- Check for inconsistent or null gender values
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- Check for inconsistent or null marital status values
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;
-- Check for null or invalid product keys
SELECT COUNT(*) AS Null_Product_Keys
FROM bronze.crm_prd_info
WHERE prd_key IS NULL OR LEN(prd_key) < 7;

-- Check for invalid/missing prd_line codes
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for null or negative product cost
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;
-- Check for date fields with 0 or wrong length
SELECT *
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) != 8 OR LEN(sls_ship_dt) != 8 OR LEN(sls_due_dt) != 8;

-- Check for sales not equal to quantity * price
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * ABS(sls_price)
  AND sls_sales IS NOT NULL
  AND sls_quantity IS NOT NULL
  AND sls_price IS NOT NULL;

-- Check for null or zero quantities or prices
SELECT *
FROM bronze.crm_sales_details
WHERE sls_quantity IS NULL OR sls_price IS NULL OR sls_price <= 0;
-- Check for 'NAS' prefix in customer ID
SELECT *
FROM bronze.erp_cust_az12
WHERE cid LIKE 'NAS%';

-- Check for future birthdates
SELECT *
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE();

-- Check for gender inconsistencies
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;
-- Check for underscores in customer ID
SELECT *
FROM bronze.erp_loc_a101
WHERE cid LIKE '%_%';

-- Check for inconsistent country codes
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;
-- Check for nulls (data is already good, just validate)
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id IS NULL OR cat IS NULL OR subcat IS NULL OR maintenance IS NULL;
