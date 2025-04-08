/*
============================================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze)
============================================================================================

Script Purpose:
	This stored procedure loads data into the silver schema from the bronze layer.
	It performs the following actions:
	- Truncates the silver tables before loading data.
	- Uses the 'INSERT' statment to load the data from the bronze tables.

	Parameters:
	None

	Usage Example:
	EXEC bronze.load_silver
*/
CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN

	DECLARE @start_time datetime, @end_time datetime
	DECLARE @silver_start datetime, @silver_end datetime
	BEGIN TRY
	PRINT '================================================================';
	PRINT '>> Loadig Silver Layer';	
	PRINT '================================================================';
	PRINT '---------------------------------------------------------------';
	PRINT '>> Loading CRM Tables';
	PRINT '---------------------------------------------------------------';

	SET @silver_start = GETDATE();
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO	silver.crm_cust_info(
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr, 
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		trim(cst_firstname) cst_firstname,
		trim(cst_lastname) cst_lastname,
		case when trim(upper(cst_marital_status)) = 'M' then 'Married'
			when trim(upper(cst_marital_status)) = 'S' then 'Single'
			else 'n/a' end cst_marital_status,
		case when trim(upper(cst_gndr)) = 'M' then 'Male'
			when trim(upper(cst_gndr)) = 'F' then 'Female'
			else 'n/a' end cst_gndr,
		cst_create_date
	FROM
		(
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info where cst_id IS NOT NULL
		) cust_info
	WHERE cust_info.flag_last = 1;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '-------------------';
	-----------------------------------------------------------------------------------------------------------------
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		prd_id, 
		cat_id, 
		prd_key, 
		prd_nm, 
		prd_cost, 
		prd_line, 
		prd_start_dt, 
		prd_end_dt
	)
	SELECT prd_id, 
		   replace(SUBSTRING(prd_key, 1,5), '-', '_') as cat_id,
		   substring(prd_key, 7, len(prd_key)) as prd_key,
		   prd_nm, 
		   ISNULL(prd_cost,0) prd_cost, 
		   case when upper(trim(prd_line)) = 'M' then 'Mountain'
		   when upper(trim(prd_line)) = 'R' then 'Road'
		   when upper(trim(prd_line)) = 'S' then 'Other Sales'
		   when upper(trim(prd_line)) = 'T' then 'Touring'
		   else 'n/a' end as prd_line,
		   cast(prd_start_dt as date) prd_start_dt, 
		   cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt
	FROM [DataWarehouse].[bronze].[crm_prd_info];
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '-------------------';
	-------------------------------------------------------------------------------------
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
		   sls_ord_num, 
		   sls_prd_key, 
		   sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	SELECT sls_ord_num, 
		   sls_prd_key, 
		   sls_cust_id, 
		   case 
			   when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			   else cast(cast(sls_order_dt as varchar) as date) 
					end as sls_order_dt,
			   case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
					else cast(cast(sls_ship_dt as varchar) as date) 
					end as sls_ship_dt,
			   case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
					else cast(cast(sls_due_dt as varchar) as date) 
					end as sls_due_dt,
			   case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs(sls_price) 
					else sls_sales 
					end as sls_sales, 
		   sls_quantity, 
		   case when sls_price is null or sls_price <= 0 
		   then sls_sales / nullif(sls_quantity, 0) 
		   else sls_price end as sls_price
		   FROM [DataWarehouse].[bronze].[crm_sales_details];
    SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '-------------------';
	   
    PRINT '------------------------------------------------------------';
	PRINT '>> Loading ERP Tables';
	PRINT '------------------------------------------------------------';
	------------------------------------------------------------------------------------
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(cid, bdate,gen)
	SELECT case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid)) 
				else cid end as cid, 
		   case when bdate < '1924-01-01' or bdate > getdate() then null 
				else bdate end as bdate,
		   case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
				when upper(trim(gen)) in ('M', 'MALE') then 'Male'
				else 'n/a' end as gen
	FROM [DataWarehouse].[bronze].[erp_cust_az12]; --customer
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '-------------------';
	-----------------------------------------------------------------------------------
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(cid, cntry)
	select trim(replace(cid, '-', '')) as cid, 
		   case when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('US', 'USA') then 'United States'
				when trim(cntry) = '' or cntry IS NULL then 'n/a'
				else trim(cntry) 
			end cntry
	from bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '-------------------';

	----------------------------------------------------------------
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2(id, cat,subcat,maintenance)
	select * from bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '-------------------';
	SET @silver_end = GETDATE();
	PRINT '>> Silver Layer Load Duration: ' + CAST(DATEDIFF(second, @silver_start, @silver_end) AS NVARCHAR) + ' seconds';
	PRINT '-------------------';
	END TRY
		BEGIN CATCH
			PRINT  '==============================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT  '==============================================';
		END CATCH
END;
