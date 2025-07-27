/******************************************************************************************
   Purpose:
    - To load raw CSV files into the bronze layer tables using BULK INSERT.
    - Tracks time taken to load each table.
    - Implements basic error handling with try/catch.

⚠ WARNING ⚠
  This procedure **truncates** all BRONZE layer tables before loading new data.
  Ensure that data from these tables is backed up or not required before execution.

  Expected CSV File Locations:
    - Must be accessible from SQL Server instance
    - Paths are currently hardcoded for local testing (adjust for production)

  Author: [Your Name]
  Created On: [Date]
******************************************************************************************/

-- Loading data into BRONZE tables
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_t DATETIME, @end_t DATETIME, @start_batch DATETIME, @end_batch DATETIME;

  BEGIN TRY
    SET @start_batch = GETDATE();

    -- Load crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    SET @start_t = GETDATE();
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_t = GETDATE();
    PRINT 'Load duration for bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

    -- Load crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    SET @start_t = GETDATE();
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_t = GETDATE();
    PRINT 'Load duration for bronze.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

    -- Load crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    SET @start_t = GETDATE();
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_t = GETDATE();
    PRINT 'Load duration for bronze.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

    -- Load erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    SET @start_t = GETDATE();
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_t = GETDATE();
    PRINT 'Load duration for bronze.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

    -- Load erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    SET @start_t = GETDATE();
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_t = GETDATE();
    PRINT 'Load duration for bronze.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

    -- Load erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    SET @start_t = GETDATE();
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
      FIRSTROW = 2,
      FIELDTERMINATOR = ',',
      TABLOCK
    );
    SET @end_t = GETDATE();
    PRINT 'Load duration for bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_t, @end_t) AS NVARCHAR) + ' seconds';

    -- Batch-level duration
    SET @end_batch = GETDATE();
    PRINT '===============================================';
    PRINT 'Total duration for loading all tables: ' + CAST(DATEDIFF(SECOND, @start_batch, @end_batch) AS NVARCHAR) + ' seconds';
  END TRY
  BEGIN CATCH
    PRINT '❌ Error occurred while loading tables';
    PRINT 'Error message: ' + CAST(ERROR_MESSAGE() AS NVARCHAR);
    PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
  END CATCH
END;
EXEC bronze.load_bronze;
