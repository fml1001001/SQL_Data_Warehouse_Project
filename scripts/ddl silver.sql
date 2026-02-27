/*
================================================================================
Script:         silver_layer_ddl.sql
Author:         [Your Name]
Created:        2026-02-27
Last Modified:  2026-02-27
Description:    DDL script to create the Silver layer tables in a data warehouse
                environment. This script drops and recreates all Silver schema
                tables sourced from CRM and ERP systems, applying light 
                transformations and standardization from the Bronze (raw) layer.

Tables Created:
    - silver.crm_cust_info      | CRM customer information  (source: cust_info.csv)
    - silver.crm_prd_info       | CRM product information   (source: prd_info.csv)
    - silver.crm_sales_details  | CRM sales transactions    (source: sales_details.csv)
    - silver.erp_cust_az12      | ERP customer demographics (source: CUST_AZ12.csv)
    - silver.erp_loc_a101       | ERP customer locations    (source: LOC_A101.csv)
    - silver.erp_px_cat_g1v2    | ERP product categories    (source: PX_CAT_G1V2.csv)

Notes:
    - All tables include a dwh_create_date audit column (DATETIME2, defaults to GETDATE())
    - Script is idempotent: existing tables are dropped before recreation
================================================================================
*/

-- source_crm: cust_info.csv
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id               INT,
    cst_key              NVARCHAR(50),
    cst_firstname        NVARCHAR(50),
    cst_lastname         NVARCHAR(50),
    cst_material_status  NVARCHAR(50),
    cst_gndr             NVARCHAR(50),
    cst_create_date      DATE,
    dwh_create_date      DATETIME2 DEFAULT GETDATE()
);

-- source_crm: prd_info.csv
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id               INT,
    prd_key              NVARCHAR(50),
    prd_nm               NVARCHAR(50),
    prd_cost             DECIMAL(10,2),
    prd_line             NVARCHAR(50),
    prd_start_dt         DATE,
    prd_end_dt           DATE,
    dwh_create_date      DATETIME2 DEFAULT GETDATE()
);

-- source_crm: sales_details.csv
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num          NVARCHAR(50),
    sls_prd_key          NVARCHAR(50),
    sls_cust_id          INT,
    sls_order_dt         DATE,
    sls_ship_dt          DATE,
    sls_due_dt           DATE,
    sls_sales            INT,
    sls_quantity         INT,
    sls_price            INT,
    dwh_create_date      DATETIME2 DEFAULT GETDATE()
);

-- source_erp: CUST_AZ12.csv
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid                  NVARCHAR(50),
    bdate                DATE,
    gen                  NVARCHAR(50),
    dwh_create_date      DATETIME2 DEFAULT GETDATE()
);

-- source_erp: LOC_A101.csv
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid                  NVARCHAR(50),
    cntry                NVARCHAR(50),
    dwh_create_date      DATETIME2 DEFAULT GETDATE()
);

-- source_erp: PX_CAT_G1V2.csv
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id                   NVARCHAR(50),
    cat                  NVARCHAR(50),
    subcat               NVARCHAR(50),
    maintenance          NVARCHAR(50),
    dwh_create_date      DATETIME2 DEFAULT GETDATE()
);
