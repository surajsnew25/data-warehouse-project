# Gold Layer Data Dictionary

## Overview
The **Gold Layer** is the business-ready data representation optimized for consumption.
Its purpose is to deliver curated, aggregated data to support analytics and reporting use cases.

It follows **Star Schema** data model, consisting of ***fact*** tables and ***dimension*** tables, designed to provide fast and consistent query performance.

## Fact and Dimension Summary
### fact_sales ( Fact )
- Purpose : Store transactional sales data to support analyisis of different business performance metrics.

- Columns :

| **Column Name** | **Data Type** | **Description** |
|-------------|-----------|-------------|
| `order_number` | VARCHAR (AlphaNumeric) | Unique Identifier for each sales order |
| `product_key` | INT (Integer) | Surrogate key linking to the *Product Dimension* table(`dim_products`) |
| `customer_key` | INT (Integer) | Surrogate key linking to the *Customer Dimension* table(`dim_customers`) |
| `order_date` | DATE | The date on which the order was placed |
| `shipping_date` | DATE | The date on which the order was shipped to the customer |
| `due_date` | DATE | The date when the order payment was due |
| `sales_amount` | INT (Integer) | The total monetary value of a sales transaction |
| `quantity` | INT (Integer) | The number of units of the product ordered |
| `price` | INT (Integer) | The price per unit of a product |

---

### dim_customers (Dimension)
- Purpose : Store customer profile information, including personal details,location and demographic data.

- Columns : 

| **Column Name** | **Data Type** | **Description** |
|-------------|-----------|-------------|
| `customer_key` | INT (Integer) | Generated surrogate key , act as a unique identifier for each record in the dimension |
| `customer_id` | INT (Integer) | Source System ID assigned to each customer |
| `customer_number` | VARCHAR (AplhaNumeric) | Business-assigned reference number to each customer, used for tracking or referencing |
| `first_name` | VARCHAR (AlphaNumeric) | First name of the customer |
| `last_name` | VARCHAR (AlphaNumeric) | Last name of the customer |
| `country` | VARCHAR (AlphaNumeric) | The country residence for the customer |
| `marital_status` | VARCHAR (AlphaNumeric) | The marital status of the customer ( *Married* / *Single* ) |
| `gender` | VARCHAR (AlphaNumeric) | The gender of the customer ( *Male* / *Female* / *N/A* ) |
| `birthdate` | DATE | The date of birth of the customer |
| `create_date` | DATE | The date when customer profile was created in the system |


---

### dim_products (Dimension)
- Purpose : Store detailed information for each product, including category, subcategory and cost attributes.

- Columns : 

| **Column Name** | **Data Type** | **Description** |
|-------------|-----------|-------------|
| `product_key` | INT | Generated surrogate key , act as a unique identifier for each record in the dimension |
| `product_id` | INT | Source System ID assigned to each product |
| `product_number` | VARCHAR (AplhaNumeric) | Business-assigned reference number to each product, used for tracking or categorization |
| `product_name` | VARCHAR (AplhaNumeric) | Descriptive name of the product, including details such as type, color and size |
| `category_id` | VARCHAR (AplhaNumeric) | Unique Identifier of the product's category type |
| `category` | VARCHAR (AplhaNumeric) | High-level classification of the product to group similar items |
| `subcategory` | VARCHAR (AplhaNumeric) | Lower-level classification of the product within a broader category |
| `maintenance` | VARCHAR (AplhaNumeric) | Indicates whether the product require maintenance or not ( *Yes* / *No* )
| `cost` | INT (Integer) | The cost or base price of the product ,measured in monetary units |
| `product_line` | VARCHAR (AplhaNumeric) | The product line or series the product belongs |
| `start_date` | DATE | The date when the record  becomes valid |

---