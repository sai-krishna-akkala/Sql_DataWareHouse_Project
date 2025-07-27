/*******************************************************************************************************************************
-- SCRIPT:       Create Bronze Schema Tables (crm_cust_info, crm_prd_info, crm_sales_details, erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2)
-- PURPOSE:      Drops existing tables in the bronze schema if they exist and recreates them.
-- WARNING:      This script DROPS tables in the bronze schema. Existing data will be lost.
*******************************************************************************************************************************/

USE [DataWareHouse];
GO

-- === Table: bronze.crm_cust_info ===
IF OBJECT_ID(N'bronze.crm_cust_info', N'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.crm_cust_info;
END;
GO
CREATE TABLE bronze.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE
);
GO

-- === Table: bronze.crm_prd_info ===
IF OBJECT_ID(N'bronze.crm_prd_info', N'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.crm_prd_info;
END;
GO
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);
GO

-- === Table: bronze.crm_sales_details ===
IF OBJECT_ID(N'bronze.crm_sales_details', N'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.crm_sales_details;
END;
GO
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);
GO

-- === Table: bronze.erp_loc_a101 ===
IF OBJECT_ID(N'bronze.erp_loc_a101', N'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.erp_loc_a101;
END;
GO
CREATE TABLE bronze.erp_loc_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

-- === Table: bronze.erp_cust_az12 ===
IF OBJECT_ID(N'bronze.erp_cust_az12', N'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.erp_cust_az12;
END;
GO
CREATE TABLE bronze.erp_cust_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);
GO

-- === Table: bronze.erp_px_cat_g1v2 ===
IF OBJECT_ID(N'bronze.erp_px_cat_g1v2', N'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.erp_px_cat_g1v2;
END;
GO
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO
