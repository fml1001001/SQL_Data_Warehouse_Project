/*
================================================================================
Script:         silver_load_procedure.sql
Author:         [Your Name]
Created:        2026-02-27
Last Modified:  2026-02-27
Description:    Stored procedure to load all Silver layer tables from the Bronze
                layer. Each table is truncated and reloaded with cleansed and
                standardized data. Execution metrics (rows inserted, load
                duration) are logged to silver.load_log after each table load.

Procedure:      silver.load_silver

Tables Loaded:
    - silver.crm_cust_info      | Deduplicates records; standardizes marital
                                  status and gender codes to full text values
    - silver.crm_prd_info       | Derives cat_id and prd_key from raw prd_key;
                                  standardizes product line codes; calculates
                                  prd_end_dt using LEAD window function
    - silver.crm_sales_details  | Validates and casts date integers; recalculates
                                  sls_sales and sls_price where values are
                                  missing, zero, or inconsistent
    - silver.erp_cust_az12      | Strips 'NAS' prefix from cid; nullifies future
                                  birthdates; standardizes gender values
    - silver.erp_loc_a101       | Removes dashes from cid; standardizes country
                                  codes to full country names
    - silver.erp_px_cat_g1v2    | Pass-through load with audit timestamp

Error Handling:
    - TRY/CATCH block captures and prints error details including the failed
      table name, error message, number, and state
    - Batch-level start/end times and total duration are printed on completion

Dependencies:
    - Requires all corresponding Bronze layer tables to be populated
    - Requires silver.load_log table to exist for metric logging
================================================================================
*/

-- Entire Silver Layer Load for Stored Procedure --

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,
            @batch_start_time DATETIME, @batch_end_time DATETIME,
            @current_table NVARCHAR(100), @rows_inserted INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=========================================================='
        PRINT 'Loading Silver Layer'
        PRINT '>> Batch Start Time: ' + CAST(@batch_start_time AS NVARCHAR)
        PRINT '=========================================================='

        PRINT '----------------------------------------------------------'
        PRINT 'Loading CRM Tables'
        PRINT '----------------------------------------------------------'

        SET @current_table = 'silver.crm_cust_info';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @current_table
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: ' + @current_table
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date)
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                 ELSE 'n/a'
            END AS cst_material_status,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) t WHERE flag_last = 1;
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Rows Inserted:    ' + CAST(@rows_inserted AS NVARCHAR)
        PRINT '>> Load Duration:    ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'
        INSERT INTO silver.load_log (table_name, rows_inserted, load_duration, load_date)
        VALUES (@current_table, @rows_inserted, DATEDIFF(millisecond, @start_time, @end_time), @start_time);
        PRINT '>> --------------------------------------------------'

        SET @current_table = 'silver.crm_prd_info';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @current_table
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data Into: ' + @current_table
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Rows Inserted:    ' + CAST(@rows_inserted AS NVARCHAR)
        PRINT '>> Load Duration:    ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'
        INSERT INTO silver.load_log (table_name, rows_inserted, load_duration, load_date)
        VALUES (@current_table, @rows_inserted, DATEDIFF(millisecond, @start_time, @end_time), @start_time);
        PRINT '>> --------------------------------------------------'

        SET @current_table = 'silver.crm_sales_details';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @current_table
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data Into: ' + @current_table
        INSERT INTO silver.crm_sales_details(
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
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Rows Inserted:    ' + CAST(@rows_inserted AS NVARCHAR)
        PRINT '>> Load Duration:    ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'
        INSERT INTO silver.load_log (table_name, rows_inserted, load_duration, load_date)
        VALUES (@current_table, @rows_inserted, DATEDIFF(millisecond, @start_time, @end_time), @start_time);
        PRINT '>> --------------------------------------------------'

        PRINT '----------------------------------------------------------'
        PRINT 'Loading ERP Tables'
        PRINT '----------------------------------------------------------'

        SET @current_table = 'silver.erp_cust_az12';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @current_table
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: ' + @current_table
        INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
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
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Rows Inserted:    ' + CAST(@rows_inserted AS NVARCHAR)
        PRINT '>> Load Duration:    ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'
        INSERT INTO silver.load_log (table_name, rows_inserted, load_duration, load_date)
        VALUES (@current_table, @rows_inserted, DATEDIFF(millisecond, @start_time, @end_time), @start_time);
        PRINT '>> --------------------------------------------------'

        SET @current_table = 'silver.erp_loc_a101';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @current_table
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Data Into: ' + @current_table
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
                 ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Rows Inserted:    ' + CAST(@rows_inserted AS NVARCHAR)
        PRINT '>> Load Duration:    ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'
        INSERT INTO silver.load_log (table_name, rows_inserted, load_duration, load_date)
        VALUES (@current_table, @rows_inserted, DATEDIFF(millisecond, @start_time, @end_time), @start_time);
        PRINT '>> --------------------------------------------------'

        SET @current_table = 'silver.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: ' + @current_table
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: ' + @current_table
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '>> Rows Inserted:    ' + CAST(@rows_inserted AS NVARCHAR)
        PRINT '>> Load Duration:    ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'
        INSERT INTO silver.load_log (table_name, rows_inserted, load_duration, load_date)
        VALUES (@current_table, @rows_inserted, DATEDIFF(millisecond, @start_time, @end_time), @start_time);
        PRINT '>> --------------------------------------------------'

    END TRY
    BEGIN CATCH
        PRINT '=================================================='
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
        PRINT '>> Failed on Table:  ' + @current_table
        PRINT '>> Error Message:    ' + ERROR_MESSAGE()
        PRINT '>> Error Number:     ' + CAST(ERROR_NUMBER() AS NVARCHAR)
        PRINT '>> Error State:      ' + CAST(ERROR_STATE() AS NVARCHAR)
        PRINT '=================================================='
    END CATCH

    SET @batch_end_time = GETDATE();
    PRINT '=========================================================='
    PRINT 'Silver Layer Load Complete'
    PRINT '>> Batch Start Time:     ' + CAST(@batch_start_time AS NVARCHAR)
    PRINT '>> Batch End Time:       ' + CAST(@batch_end_time AS NVARCHAR)
    PRINT '>> Total Load Duration:  ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' ms'
    PRINT '=========================================================='
END
