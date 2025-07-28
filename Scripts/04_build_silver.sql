/*****************************************************************************************
-- Procedure Name : silver.silver_transform_load
-- Purpose        : Cleanses, transforms, and loads data from bronze layer to silver layer.
-- Description    : 
   This procedure standardizes and loads customer, product, sales, and ERP-related data 
   from the bronze layer into cleaned silver tables. It includes null handling, data type
   conversions, deduplication, and normalization of values.
-- WARNING:
   ⚠ Ensure that corresponding silver tables exist with correct schema before running.
   ⚠ Data in target silver tables will be **truncated**.
   ⚠ This procedure uses `DROP TABLE` to recreate some tables. Ensure no dependencies exist.
   ⚠ Validate format of dates in bronze tables before executing.
******************************************************************************************/

CREATE OR ALTER PROCEDURE silver.silver_transform_load
AS
BEGIN
    BEGIN TRY
        DECLARE @start_t DATETIME, @end_t DATETIME, @batch_start DATETIME, @batch_end DATETIME;

        SET @batch_start = GETDATE();

        /*************************************
         -- Step 1: Load silver.crm_cust_info
        *************************************/
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT 'Inserting data into crm_cust_info';

        SET @start_t = GETDATE();
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) temp
        WHERE flag_last = 1;

        SET @end_t = GETDATE();
        PRINT 'Load duration for silver.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

        /*************************************
         -- Step 2: Create & Load silver.crm_prd_info
        *************************************/
        IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
            DROP TABLE silver.crm_prd_info;

        CREATE TABLE silver.crm_prd_info (
            prd_id INT,
            cat_id NVARCHAR(50),
            prd_key NVARCHAR(50),
            prd_nm NVARCHAR(50),
            prd_cost INT,
            prd_line NVARCHAR(50),
            prd_start_dt DATE,
            prd_end_dt DATE,
            dwh_create_date DATETIME2 DEFAULT GETDATE()
        );

        TRUNCATE TABLE silver.crm_prd_info;
        PRINT 'Inserting data into crm_prd_info';

        SET @start_t = GETDATE();
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            DATEADD(DAY, -1, CAST(LEAD(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt) AS DATE))
        FROM bronze.crm_prd_info;

        SET @end_t = GETDATE();
        PRINT 'Load duration for silver.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

        /*************************************
         -- Step 3: Create & Load silver.crm_sales_details
        *************************************/
        IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
            DROP TABLE silver.crm_sales_details;

        CREATE TABLE silver.crm_sales_details (
            sls_ord_num NVARCHAR(50),
            sls_prd_key NVARCHAR(50),
            sls_cust_id INT,
            sls_order_dt DATE,
            sls_ship_dt DATE,
            sls_due_dt DATE,
            sls_sales INT,
            sls_quantity INT,
            sls_price INT,
            dwh_create_date DATETIME2 DEFAULT GETDATE()
        );

        TRUNCATE TABLE silver.crm_sales_details;
        PRINT 'Inserting data into crm_sales_details';

        SET @start_t = GETDATE();
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @end_t = GETDATE();
        PRINT 'Load duration for silver.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

        /*************************************
         -- Step 4: Load silver.erp_cust_az12
        *************************************/
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT 'Inserting data into erp_cust_az12';

        SET @start_t = GETDATE();
        INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @end_t = GETDATE();
        PRINT 'Load duration for silver.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

        /*************************************
         -- Step 5: Load silver.erp_loc_a101
        *************************************/
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT 'Inserting data into erp_loc_a101';

        SET @start_t = GETDATE();
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT 
            REPLACE(cid, '_', ''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @end_t = GETDATE();
        PRINT 'Load duration for silver.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

        /*************************************
         -- Step 6: Load silver.erp_px_cat_g1v2
        *************************************/
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT 'Inserting data into erp_px_cat_g1v2';

        SET @start_t = GETDATE();
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        SELECT * FROM bronze.erp_px_cat_g1v2;

        SET @end_t = GETDATE();
        PRINT 'Load duration for silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR);

        /*************************************
         -- Total Time
        *************************************/
        SET @batch_end = GETDATE();
        PRINT 'Total duration for loading entire data: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';

    END TRY
    BEGIN CATCH
        PRINT 'Error occurred during the load.';
        PRINT 'Error message: ' + CAST(ERROR_MESSAGE() AS NVARCHAR);
        PRINT 'Error number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    END CATCH
END;

-- To execute the procedure:
EXEC silver.silver_transform_load;
