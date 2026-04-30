/*
--------------------------------------------------------------------------------------------------------
Script: Quality Checks for Silver Database

Purpose: - Perform quality checks for data consistency, standardization and accuracy.
		 - Check Null,duplicates, invalid values, extra spaces and date order.
		 - Check logical relationships.
         
Note: This script performs only quality checks and no transformation.
--------------------------------------------------------------------------------------------------------         
*/

-- ===============================================
--          *** For Silver CRM Tables ***
-- ===============================================

-- -----------Table : crm_cust_info-----------

-- >>> Check for Nulls or Duplicates in Customer ID column.
-- Expectation : No Result
select cst_id,   -- customer id
	count(*) as count
from silver.crm_cust_info
group by cst_id
having count > 1 or cst_id is null;

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from silver.crm_cust_info
where cst_key != trim(cst_key)
	or cst_firstname != trim(cst_firstname)
    or cst_lastname != trim(cst_lastname)
    or cst_marital_status != trim(cst_marital_status )
    or cst_gndr != trim(cst_gndr);

-- >>> Data Standardization
select distinct cst_gndr     -- distinct gender values
from silver.crm_cust_info ;

-- -----------Table : crm_prd_info-----------

-- >>> Check for Nulls or Duplicates in Product Id column.
-- Expectation : No Result
select prd_id,    -- product id
	count(*) as count
from silver.crm_prd_info
group by prd_id
having count > 1 or prd_id is null;

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from silver.crm_prd_info
where cat_id != trim(cat_id)
	or prd_key != trim(prd_key)
    or prd_nm != trim(prd_nm)
    or prd_line != trim(prd_line);

-- >>> Check for Null or Negative values
-- Expectation : No Result
-- (Product Cost >= 0)
select prd_cost      -- product cost 
from silver.crm_prd_info
where prd_cost is null or prd_cost < 0 ;   -- check for null and negative values

-- >>> Data Standardization
select distinct prd_line     -- distinct product line values
from silver.crm_prd_info ;

-- >>> Check for Invalid Date Order
-- Expectation : No Result
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;   -- start date is after end date

-- -----------Table : crm_sales_details-----------

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from silver.crm_sales_details
where sls_ord_num != trim(sls_ord_num)
	or sls_prd_key != trim(sls_prd_key);

-- >>> Check Data Consistency

-- (Check if Product key in Sales table is consistent with
--   Product Key in Product info table)
select *
from silver.crm_sales_details s
where not exists( 
	select 1
    from silver.crm_prd_info p
    where s.sls_prd_key = p.prd_key);

-- (Check if Customer ID in Sales table is consistent with
--   Customer ID in Customer info table)
select * 
from silver.crm_sales_details s
where not exists(
	select 1
    from silver.crm_cust_info c
    where s.sls_cust_id = c.cst_id);
    
-- (Check if Sales values are consistent with Price and Quantity)
-- (Sales = Price * Quantity)
-- (Sales, Price and Quantity should be positive values)
select sls_sales,
	sls_quantity,
    sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity*sls_price          -- Check where sales is not consistent with price and quantity
	or sls_sales is null or sls_sales <= 0         -- Check for null ,zeroes and negative values
    or sls_quantity is null or sls_quantity <= 0 
    or sls_price is null or sls_price <= 0          
order by sls_sales,sls_quantity,sls_price;

-- >>> Check for Invalid Date value
-- Expectation : No Result
select *
from silver.crm_sales_details
where char_length(cast(sls_order_dt as char)) != 10     -- check for invalid length of date
	or char_length(cast(sls_ship_dt as char)) != 10
    or char_length(cast(sls_due_dt as char)) != 10
	or sls_order_dt > '2030-01-01' or sls_order_dt < '1950-01-01'    -- check for range of date
    or sls_ship_dt > '2030-01-01' or sls_ship_dt < '1950-01-01'
    or sls_due_dt > '2030-01-01' or sls_due_dt < '1950-01-01';

-- >>> Check for Invalid Date Order
-- Expectation : No Result
-- (Order date should be before shipment date and due date)
select * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt      -- order date is after shipment date
	or sls_order_dt > sls_due_dt;     -- order date is after due date


-- ===============================================
--          *** For Silver ERP Tables ***
-- ===============================================

-- -----------Table : erp_cust_az12-----------

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from silver.erp_cust_az12
where cid != trim(cid)
	or gen != trim(gen);
    
-- >>> Check Data Consistency

-- (Check if Customer Key in ERP customer birth-date table is consistent with
--   Customer Key in CRM customer info table)
select cid       -- Customer Key
from silver.erp_cust_az12 e
where not exists(
	select 1
    from silver.crm_cust_info c
    where e.cid=c.cst_key);

-- >>> Check for invalid birth-date
-- Expectation : No Result
select bdate     -- customer birth-date
from silver.erp_cust_az12
where bdate > now();   -- birth-date is in future

-- >>> Data Standardization
select distinct gen     -- distinct gender values
from silver.erp_cust_az12;

-- -----------Table : erp_loc_a101-----------

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from silver.erp_loc_a101
where cid != trim(cid)
	or cntry != trim(cntry);
    
-- >>> Check Data Consistency

-- (Check if Customer Key in ERP customer location table is consistent with
--   Customer Key in CRM customer info table)
select *
from silver.erp_loc_a101 l
where not exists(
	select 1
    from silver.crm_cust_info c
    where l.cid = c.cst_key);

-- >>> Data Standardization
select distinct cntry     -- distinct country values
from silver.erp_loc_a101;

-- -----------Table : erp_px_cat_g1v2-----------

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from silver.erp_px_cat_g1v2
where cat != trim(cat)
	or subcat != trim(subcat)
    or maintenance != trim(maintenance);
    
-- >>> Check Data Consistency

-- (Check if Category ID in ERP product category table is consistent with
--   Category ID in CRM product info table)
select id     -- category id
from silver.erp_px_cat_g1v2 px
where not exists(
	select 1
    from silver.crm_prd_info p
    where px.id = p.cat_id);

-- >>> Data Standardization
select distinct cat      -- distinct category values
from silver.erp_px_cat_g1v2;

-- ---------------------------------------------------------------------------------------------
