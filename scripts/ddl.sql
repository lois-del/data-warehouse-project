/*
Scripts to create database tables for Silver and Bronze layers.
Replace 'silver' with 'bronze' and vice versa

The scripts check to see if the tables exists already. If it exists, it drops the table and recreate it.

Only needs to be run once unless the structure of the table changes
*/

IF  OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info 
(cst_id int,
 cst_key nvarchar(50),
 cst_firstname nvarchar(50),
 cst_lastname nvarchar(50),
 cst_marital_status nvarchar(50),
 cst_gndr nvarchar(50),
 cst_create_date date,
 dwh_create_date datetime2 DEFAULT GETDATE()
);

IF  OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info 
(
prd_id int,
prd_key nvarchar(500),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(20),
prd_start_dt datetime,
prd_end_dt datetime,
dwh_create_date datetime2 DEFAULT GETDATE()
);


IF  OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details 
(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12
(
CID nvarchar(30),
BDATE date,
GEN nvarchar(10),
dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101
(
CID nvarchar(30),
CNTRY nvarchar(30),
dwh_create_date datetime2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2
(
ID nvarchar(30),
CAT nvarchar(30),
SUBCAT nvarchar(30),
MAINTENANCE nvarchar(30),
dwh_create_date datetime2 DEFAULT GETDATE()
);
