/*
--------------------------------------------------------------------------------------------------------
Script: Quality Checks for Gold Database

Purpose: - Perform quality checks for data consistency, standardization and accuracy.
		 - Check Null,duplicates invalid values, extra spaces and date order.
		 - Check connectivity between fact and dimension tables.
         
Note: This script performs only quality checks and no transformation.
--------------------------------------------------------------------------------------------------------         
*/

-- ===============================================
--         *** Dimension : Customers ***
-- ===============================================

-- >>> Check for Nulls or Duplicates in Customer Key column
-- Expectation : No Result
select customer_key,  -- customer key
	count(*) as count
from gold.dim_customers
group by customer_key
having count > 1 or customer_key is null; 

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from gold.dim_customers
where customer_number != trim(customer_number)
	or first_name != trim(first_name)
	or last_name  != trim(last_name)
	or country    != trim(country)
	or marital_status != trim(marital_status)
	or gender     != trim(gender);

-- >>> Check Data Consistency
-- Check if customer id is consistent with customer number
-- Expectation : No Result
-- (customer id matches with last 5 characters in customer number)
select *
from gold.dim_customers
where customer_id != right(customer_number,5);

-- >>> Check Invalid date records
-- Expectation : No Result
select *
from gold.dim_customers
where birthdate >= create_date;      -- birthdate is after create date

-- >>> Check Data Standardization

select distinct gender              -- distinct gender values
from gold.dim_customers;

select distinct country             -- distinct country values
from gold.dim_customers;

select distinct marital_status      -- distinct marital status values
from gold.dim_customers;


-- ===============================================
--         *** Dimension : Products ***
-- ===============================================

-- >>> Check for Nulls or Duplicates in Product Key column
-- Expectation : No Result
select product_key,     -- product_key
	count(*) as count
from gold.dim_products
group by product_key
having count > 1;

-- >>> Check for unwanted space
-- Expectation : No Result
select *
from gold.dim_products
where product_number != trim(product_number)
	or product_name  != trim(product_name)
	or category_id   != trim(category_id)
	or category      != trim(category)
	or subcategory   != trim(subcategory)
	or maintenance   != trim(maintenance)
    or product_line  != trim(product_line);

-- >>> Check Data Standardization

select distinct category     -- distinct category values
from gold.dim_products;

select distinct product_line  -- distinct product line values
from gold.dim_products;

-- ==========================================
--         *** Fact : Sales ***
-- ==========================================

-- >>> Check Data Consistency
-- Check if sales amount is consistent with price and quantity
-- Expectation : No Result
-- (Sales = Price * Quantity)
select *
from gold.fact_sales
where sales_amount != quantity*price;

-- >>> Check Data Model Connectivity
-- Check if fact and dimensions are correctly linked
-- Expectation : No Result
select *
from gold.fact_sales fs
left join gold.dim_customers dc           -- dimension : customers
	on fs.customer_key = dc.customer_key  -- left join on customer  key
left join gold.dim_products dp            -- dimension : products
	on fs.product_key = dp.product_key    -- left join on product key
where dc.customer_key is null
	or dp.product_key is null;

-- -----------------------------------------------------------------------------------------


