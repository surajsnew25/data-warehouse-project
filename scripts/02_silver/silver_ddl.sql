/*
--------------------------------------------------------------------------------------------------------
Script: Silver Layer Initialization (DDL)

Purpose:
- Initialize the 'silver' database representing the cleansed and standardized layer of the data warehouse.
- Drop and recreate curated tables to ensure schema consistency and safe re-run.
- Define structured schemas for storing transformed data derived from multiple source systems (CRM and ERP).

Context:
- The Silver layer stores cleaned, validated, and standardized data sourced from the Bronze layer.
- Acts as an intermediate layer for data integration and transformation before serving analytics needs.
- Designed to support scalable ETL pipelines and ensure data quality for downstream consumption(Gold layer).

Notes:
- Tables are recreated on each run to maintain a consistent and reliable schema state.
- Basic transformations such as data type casting,null handling, standardization are expected upstream.
- Includes audit column (dwh_create_date) to track data load timestamps.
--------------------------------------------------------------------------------------------------------
*/

-- Create silver database
create database if not exists silver;
use silver;

-- Create tables
-- ===============================================
--          *** For Source System CRM ***
-- ===============================================

-- Create customer information table
drop table if exists crm_cust_info;  -- Drop table if it already exists
create table crm_cust_info(          -- Create fresh table
	cst_id int,
	cst_key varchar(40),
	cst_firstname varchar(40),
	cst_lastname varchar(40),
	cst_marital_status varchar(20) ,
	cst_gndr varchar(20),
	cst_create_date date,
    dwh_create_date datetime default now()
);

-- Create product information table
drop table if exists crm_prd_info;
create table crm_prd_info(
	prd_id int,
    cat_id varchar(40),
    prd_key varchar(40),
    prd_nm varchar(40),
    prd_cost int,
    prd_line varchar(20),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date datetime default now()
);

-- Create sales details table
drop table if exists crm_sales_details;
create table crm_sales_details(
	sls_ord_num varchar(40),
    sls_prd_key varchar(40),
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date datetime default now()
);

-- ===============================================
--          *** For Source System ERP ***
-- ===============================================**

-- Create table customer detail
drop table if exists erp_cust_az12;
create table erp_cust_az12(
	cid varchar(40),
    bdate date,
    gen varchar(20),
    dwh_create_date datetime default now()
);

-- Create table customer location details
drop table if exists erp_loc_a101;
create table erp_loc_a101(
	cid varchar(40),
    cntry varchar(40),
    dwh_create_date datetime default now()
);

-- Create table product category details
drop table if exists erp_px_cat_g1v2;
create table erp_px_cat_g1v2(
	id varchar(40),
    cat varchar(40),
    subcat varchar(40),
    maintenance varchar(20),
    dwh_create_date datetime default now()
);
-- ---------------------------------------------------------------------------