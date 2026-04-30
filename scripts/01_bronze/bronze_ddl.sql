/*
--------------------------------------------------------------------------------------------------------
Script: Bronze Layer Initialization (DDL)

Purpose:
- Initialize the 'bronze' database representing the raw ingestion layer of the data warehouse.
- Drop and recreate source aligned tables to ensure schema consistency and safe re-run.
- Define table structures for ingesting data from multiple source systems (CRM and ERP)
  without transformation.

Context:
- The Bronze layer stores raw, unprocessed data as received from source systems.
- Acts as the single source for downstream transformations (Silver /Gold layers).
- Designed to support scalable ETL pipelines using SQL and Python.

Notes:
- Tables are recreated on each run to maintain a clean ingestion state.
- No business logic or transformations are applied at this stage.
--------------------------------------------------------------------------------------------------------
*/


-- Create bronze database 
create database if not exists bronze;
use bronze;

-- Create tables
-- ===============================================
--          *** For Source System CRM ***
-- ===============================================

-- Create customer information table
drop table if exists crm_cust_info; -- Drop table if it already exists
create table crm_cust_info(         -- Create fresh table 
	cst_id int,
	cst_key varchar(40),
	cst_firstname varchar(40),
	cst_lastname varchar(40),
	cst_marital_status varchar(20) ,
	cst_gndr varchar(20),
	cst_create_date date
);

-- Create product information table
drop table if exists crm_prd_info;
create table crm_prd_info(
	prd_id int,
    prd_key varchar(40),
    prd_nm varchar(40),
    prd_cost int,
    prd_line varchar(20),
    prd_start_dt date,
    prd_end_dt date
);

-- Create sales details table
drop table if exists crm_sales_details;
create table crm_sales_details(
	sls_ord_num varchar(40),
    sls_prd_key varchar(40),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);

-- ===============================================
--          *** For Source System ERP ***
-- ===============================================

-- Create table customer detail
drop table if exists erp_cust_az12;
create table erp_cust_az12(
	cid varchar(40),
    bdate date,
    gen varchar(20)
);

-- Create table customer location details
drop table if exists erp_loc_a101;
create table erp_loc_a101(
	cid varchar(40),
    cntry varchar(40)
);

-- Create table product category details
drop table if exists erp_px_cat_g1v2;
create table erp_px_cat_g1v2(
	id varchar(40),
    cat varchar(40),
    subcat varchar(40),
    maintenance varchar(20)
);

-- ----------------------------------------------------------------------
