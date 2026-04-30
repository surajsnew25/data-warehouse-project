/*
------------------------------------------------------------------------------------------------------------
Script: Gold Layer Initialization (Views)

Purpose:
- Initialize the 'gold' database representing the presentation and analytics layer of the data warehouse.
- Drop and recreate analytical views to ensure consistency and safe re-run.
- Define dimension and fact views based on curated data from multiple source systems (CRM and ERP).

Context:
- The Gold layer stores business-ready, integrated, and analytics-optimized data derived from the Silver layer.
- Acts as the final layer for reporting, dashboarding, and advanced analytics use cases.
- Designed using a Star Schema model to enable efficient querying and simplified data consumption.

Notes:
- Views are recreated on each run to maintain a consistent and reliable analytical layer.
- Incorporates data integration, enrichment, and business-level transformations.
- Provides meaningful, standardized column naming for improved usability and clarity.
--------------------------------------------------------------------------------------------------------------         
*/

-- Create gold database
create database if not exists gold;

-- Create Views
-- ===============================================
--      *** Dimension : Customer Details ***
-- ===============================================
/* 
   Combine Customer Info ,additional details and location tables
   Use 'Left Join', keeping all records from customer info table and only 
    matching records from the rest.
*/

drop view if exists gold.dim_customers;   -- drop view if already exists

create view gold.dim_customers as         -- create view
select 
	row_number() over (order by ci.cst_id) as customer_key,  -- surrogate key (act as primary key)
	ci.cst_id   as customer_id,
	ci.cst_key 	as customer_number,
	ci.cst_firstname as first_name,
    ci.cst_lastname  as last_name,
    cl.cntry as country,
    ci.cst_marital_status as marital_status,
    
    case when ci.cst_gndr != 'N/A' then ci.cst_gndr
		else coalesce(ca.gen,'N/A')
	end  as gender,                           -- integrate gender values
											 -- CRM is the primary source for gender values
    ca.bdate as birthdate,
    ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca         -- left join on customer number
	on ci.cst_key=ca.cid
left join silver.erp_loc_a101 cl          -- left join on customer number
	on ci.cst_key=cl.cid;

-- ===============================================
--      *** Dimension : Product Details ***
-- ===============================================
/* 
   Combine Product Info and Product Category tables
   Use 'Left Join', keeping all records from product info table and only 
    matching records from the product category table.
*/

drop view if exists gold.dim_products;

create view gold.dim_products as
select 
	row_number() over(
		order by pi.prd_start_dt,pi.prd_key) as product_key, -- surrogate key
	pi.prd_id  as product_id,
	pi.prd_key as product_number,
    pi.prd_nm  as product_name,
	pi.cat_id  as category_id,
    pc.cat     as category,
    pc.subcat  as subcategory,
    pc.maintenance,
    pi.prd_cost     as cost, 
    pi.prd_line     as product_line, 
    pi.prd_start_dt as start_date
from silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 pc  -- left join on category id
	on pi.cat_id = pc.id
where pi.prd_end_dt is null;         -- filter out all historical data


-- =======================================
--      *** Fact : Sales Details ***
-- =======================================
/* 
   Create fact table and link it to dimension tables
   by adding their primary keys as foreign keys.
*/

drop view if exists gold.fact_sales;

create view gold.fact_sales as
select
    sd.sls_ord_num  as order_number,
    pr.product_key  as product_key,   -- dimension product's primary key
    cu.customer_key as customer_key,  -- dimension customer's  primary key
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt  as shipping_date,
    sd.sls_due_dt   as due_date,
    sd.sls_sales    as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price    as price
from silver.crm_sales_details sd
left join gold.dim_products pr
    on sd.sls_prd_key = pr.product_number      -- left join on product number
left join gold.dim_customers cu
    on sd.sls_cust_id = cu.customer_id;        -- left join on customer id
    
-- -------------------------------------------------------------------------------