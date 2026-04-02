use DataWarehouse

EXEC bronze.load_bronze

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

===============================================================================
*/


create or alter procedure bronze.load_bronze as 
begin
	begin try
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		---full load ----
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;-- delete the data then we update it again
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\hsaime\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\hsaime\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\hsaime\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\hsaime\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\hsaime\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\hsaime\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '==========================================';
	end try 
	begin catch
	    PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	end catch
end
