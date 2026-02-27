/*
================================================================================
Script:         silver_quality_checks.sql
Author:         [Your Name]
Created:        2026-02-27
Last Modified:  2026-02-27
Description:    Comprehensive quality checks for all Silver layer tables. Each
                section follows the full validation lifecycle for one table:
                Bronze QC (pre-load) → Clean & Load → Silver QC (post-load).

                Run sections sequentially when loading a specific table, or
                run the full script to validate the entire Silver layer.

Tables Covered:
    1. silver.crm_sales_details
    2. silver.erp_cust_az12
    3. silver.erp_loc_a101
    4. silver.erp_px_cat_g1v2
================================================================================
*/


-- ============================================================================== 
-- TABLE 1: silver.crm_sales_details
-- Source:  bronze.crm_sales_details (source file: sales_details.csv)
-- Transformations:
--     - Date integers cast to DATE type; invalid/zero values set to NULL
--     - Sales recalculated as Qty * ABS(Price) where missing, zero, or inconsistent
--     - Price recalculated as Sales / Qty where missing or zero
-- ==============================================================================


-- ------------------------------------------------------------------------------
-- 1.1 BRONZE QC: Pre-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Foreign Key Integrity
-- Ensure all product keys in sales details exist in the product table
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Check 2: Invalid Date Values
-- Flag dates that are zero, not 8 digits, or outside a reasonable range
SELECT
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
    OR LEN(sls_order_dt) <> 8
    OR sls_order_dt > 20500101
    OR sls_order_dt < 19000101;

-- Check 3: Invalid Date Order
-- Order date should never be later than ship or due date
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

-- Check 4: Business Rule Validation (Sales = Qty * Price)
-- Flag rows where sales, quantity, or price are NULL, zero, negative,
-- or inconsistent with each other
-- Rule: If sales is null/zero/neg → use Qty * ABS(Price)
--       If price is null/zero     → use Sales / Qty
--       If price is negative      → convert to positive
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price_cleaned,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales_cleaned
FROM bronze.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ------------------------------------------------------------------------------
-- 1.2 CLEAN & LOAD: bronze → silver.crm_sales_details
-- ------------------------------------------------------------------------------

PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data Into: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

-- ------------------------------------------------------------------------------
-- 1.3 SILVER QC: Post-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Re-run Foreign Key Check Against Silver
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Check 2: Confirm No Invalid Dates Remain
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
    OR sls_order_dt > '2050-01-01'
    OR sls_order_dt < '1900-01-01';

-- Check 3: Confirm No Invalid Date Order Remains
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

-- Check 4: Confirm Business Rule is Satisfied
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;


-- ==============================================================================
-- TABLE 2: silver.erp_cust_az12
-- Source:  bronze.erp_cust_az12 (source file: CUST_AZ12.csv)
-- Transformations:
--     - 'NAS' prefix stripped from cid to align with crm_cust_info foreign key
--     - Future birthdates set to NULL as out-of-range values
--     - Gender codes/variants standardized to 'Male', 'Female', or 'N/A'
-- ==============================================================================


-- ------------------------------------------------------------------------------
-- 2.1 BRONZE QC: Pre-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Foreign Key Integrity
-- Spot-check a known customer ID to confirm cid format vs crm_cust_info
SELECT
    cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%';

-- Check 2: Review silver.crm_cust_info keys to confirm expected join format
SELECT cst_key FROM silver.crm_cust_info;

-- Check 3: Out of Range Birthdates
-- Flag dates before 1924 or in the future
SELECT DISTINCT
    bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
    OR bdate > GETDATE();

-- Check 4: Gender Column Standardization
-- Review all distinct values and preview cleaned output
SELECT DISTINCT
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'N/A'
    END AS gen
FROM bronze.erp_cust_az12;

-- ------------------------------------------------------------------------------
-- 2.2 CLEAN & LOAD: bronze → silver.erp_cust_az12
-- ------------------------------------------------------------------------------

PRINT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting Data Into: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
         ELSE 'N/A'
    END AS gen
FROM bronze.erp_cust_az12;

-- ------------------------------------------------------------------------------
-- 2.3 SILVER QC: Post-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Confirm NAS Prefix Has Been Removed
SELECT
    cid,
    bdate,
    gen
FROM silver.erp_cust_az12
WHERE cid LIKE '%AW00011000%';

-- Check 2: Confirm No Out-of-Range Birthdates Remain
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
    OR bdate > GETDATE();

-- Check 3: Confirm Gender Values Are Standardized
SELECT DISTINCT gen
FROM silver.erp_cust_az12
ORDER BY gen;


-- ==============================================================================
-- TABLE 3: silver.erp_loc_a101
-- Source:  bronze.erp_loc_a101 (source file: LOC_A101.csv)
-- Transformations:
--     - Dashes removed from cid to align with crm_cust_info foreign key format
--     - Country codes standardized to full country names ('DE' → 'Germany',
--       'US'/'USA' → 'United States')
--     - NULL or blank country values set to 'N/A'
-- ==============================================================================


-- ------------------------------------------------------------------------------
-- 3.1 BRONZE QC: Pre-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Foreign Key Integrity
-- Preview cid format to confirm dashes need to be removed to match crm_cust_info
SELECT DISTINCT
    cid
FROM bronze.erp_loc_a101
WHERE cid LIKE '%-%'
ORDER BY cid;

-- Check 2: Review all distinct country values to identify codes needing standardization
SELECT DISTINCT
    cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- Check 3: Flag NULL or blank country values
SELECT *
FROM bronze.erp_loc_a101
WHERE cntry IS NULL
    OR TRIM(cntry) = '';

-- ------------------------------------------------------------------------------
-- 3.2 CLEAN & LOAD: bronze → silver.erp_loc_a101
-- ------------------------------------------------------------------------------

PRINT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting Data Into: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
         ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;

-- ------------------------------------------------------------------------------
-- 3.3 SILVER QC: Post-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Confirm No Dashes Remain in cid
SELECT *
FROM silver.erp_loc_a101
WHERE cid LIKE '%-%';

-- Check 2: Confirm All Country Values Are Standardized
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- Check 3: Full Table Review
SELECT *
FROM silver.erp_loc_a101;


-- ==============================================================================
-- TABLE 4: silver.erp_px_cat_g1v2
-- Source:  bronze.erp_px_cat_g1v2 (source file: PX_CAT_G1V2.csv)
-- Transformations:
--     - Pass-through load; no value transformations required
--     - Whitespace checks confirmed no trimming was necessary
-- ==============================================================================


-- ------------------------------------------------------------------------------
-- 4.1 BRONZE QC: Pre-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Full Bronze Table Preview
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

-- Check 2: Unwanted Leading/Trailing Spaces in cat
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat);

-- Check 3: Unwanted Leading/Trailing Spaces in subcat
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE subcat <> TRIM(subcat);

-- Check 4: Unwanted Leading/Trailing Spaces in maintenance
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE maintenance <> TRIM(maintenance);

-- Check 5: Data Standardization - Review all distinct category values
SELECT DISTINCT
    cat
FROM bronze.erp_px_cat_g1v2;

-- ------------------------------------------------------------------------------
-- 4.2 CLEAN & LOAD: bronze → silver.erp_px_cat_g1v2
-- ------------------------------------------------------------------------------

PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

-- ------------------------------------------------------------------------------
-- 4.3 SILVER QC: Post-Load Validation
-- ------------------------------------------------------------------------------

-- Check 1: Full Silver Table Review
SELECT *
FROM silver.erp_px_cat_g1v2;

-- Check 2: Confirm No Whitespace Issues Exist in Silver
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat)
    OR subcat <> TRIM(subcat)
    OR maintenance <> TRIM(maintenance);

-- Check 3: Confirm Distinct Category Values Look Clean
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2
ORDER BY cat;
