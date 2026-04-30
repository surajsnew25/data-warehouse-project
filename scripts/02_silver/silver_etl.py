'''
-------------------------------------------------------------------------------------------------------
ETL Script: Bronze to Silver Layer Transformation

Purpose:
- Extract, transform, and load data from the Bronze layer into the Silver layer of the data warehouse.
- Apply data cleaning, validation, normalization, and standardization to improve data quality.
- Ensure consistent and reliable data loading using a full-refresh (truncate and load) strategy.

Scope:
- Handles end-to-end ETL processing for multiple source-aligned tables.
- Performs structured transformations including data validation, deduplication, formatting, and enrichment.
- Loads processed data into predefined Silver tables for downstream analytical use.

Dependencies: sqlalchemy, pymysql, pandas, python-dotenv
         
Notes:
- This script represents the Silver layer (cleansed and standardized layer) in a Medallion architecture.
- Data is transformed to ensure consistency, accuracy, and usability for analytics and reporting.
- Modular functions (extraction, transformation, ingestion) enable reusability and scalability.
- Logging is implemented for monitoring, debugging, and auditability of ETL operations.
- Designed for safe re-execution with consistent results.
--------------------------------------------------------------------------------------------------------         
'''

# import libraries
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from pathlib import Path
import os
import logging
import datetime
import time

# load env file and import credentials
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")
if not all([DB_USER, DB_HOST, DB_PASSWORD]):
    raise EnvironmentError("Missing required environment variables.")

# Defining project root and log directory
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != 'data-warehouse-project':
    if BASE_DIR.parent == BASE_DIR:
        raise Exception("data-warehouse-project directory not found in the path.")
    BASE_DIR = BASE_DIR.parent

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok = True)

# basic config for log file
logging.basicConfig(
    filename=LOG_DIR / "logs_silver_etl.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ======================== Extraction Function ========================
# ---------------------------------------------------------------------
''' The function expects parameters :   
        table_name : Name of source table from which data will be extracted.
        conn_engine: Database engine establishing connection to the source database.
    The function starts a connection to the source database , runs SQL query to extract
    required data and stores data in a dataframe.
'''
 
def data_extraction(table_name : str , conn_engine):
    logging.info(f"[{table_name}] Extraction Begin ...")

    if not isinstance(conn_engine, Engine):
        raise TypeError(f"[{table_name}] Extraction Failed : Invalid Database Connection Engine")
    
    if table_name not in table_names_list:
        raise ValueError(f"[{table_name}] Extraction failed: Input table name is invalid")
    
    start = time.time()

    # the sql query to extract data
    query = f"select * from {table_name};"

    # read and store data into a dataframe
    with conn_engine.connect() as conn:
        df= pd.read_sql(text(query),conn)

    end=time.time()

    if df.empty:
        raise ValueError(f"[{table_name}] Extraction failed: No data received from source query")
    
    time_taken = round(end-start,2)
    logging.info(f"[{table_name}] Extraction End | Records:{len(df)} | Time :{time_taken} seconds.")

    return df    # return created dataframe

# ===================== Transformation Function =====================
# --------------------------------------------------------------------
''' The function expects parameters :   
        table_name : Name of source table corresponding to the derived dataframe.
        df         : Dataframe created post extraction process.
    This function performs data transformation after validating the table name ,
    then returns the updated dataframe. 
'''
 
def data_transformation(table_name : str, df):
    logging.info(f"[{table_name}] Transformation Begin ...")

    if df.empty:
        raise ValueError(f"[{table_name}] Transformation failed: Input dataframe is empty")
    
    start = time.time()
    
    if table_name == 'crm_cust_info':     # Transform Customer Info table

        # Convert date columns to datetime
        date_cols = ["cst_create_date"]
        df[date_cols] = df[date_cols].apply(pd.to_datetime, errors='coerce')

        # Drop records where customer id is null
        df=df.dropna(subset=['cst_id']).copy()

        # Keep only latest record for duplicate customer id.
        df=df.sort_values(by='cst_create_date',ascending = False).\
                drop_duplicates(subset=['cst_id'],keep='first')
        
        # Remove unwanted space
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

        # Standardize gender values
        df['cst_gndr']=df['cst_gndr'].str.upper().map({'M':'Male','F':'Female'}).fillna('N/A')
        
        # Standardize marital status values
        df['cst_marital_status']=df['cst_marital_status'].str.upper().\
                                    map({'M':'Married','S':'Single'}).fillna('N/A')
        df=df.sort_values(by='cst_id')

    elif table_name == 'crm_prd_info':      # Transform Product Info table

        # Convert date columns to datetime
        date_cols = ["prd_start_dt","prd_end_dt"]
        df[date_cols] = df[date_cols].apply(pd.to_datetime, errors='coerce')

        # Remove unwanted space
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

        # Extract category id from product key
        df['cat_id']= df['prd_key'].str[0:5].str.replace('-','_')

        # Keep product key only (striping category id)
        df['prd_key']=df['prd_key'].str[6:]
    
        # Handle missing product cost
        df['prd_cost']=df['prd_cost'].fillna(0)

        # Standardize product line values
        df['prd_line']=df['prd_line'].str.upper().map(
                            {'M':'Mountain','R':'Road',
                            'S':'Other Sales','T':'Touring'}).fillna('N/A')
        
        # Derive end date as 1 day before start date of next record per product key
        df=df.sort_values(by=['prd_key','prd_start_dt'])
        df['prd_end_dt']=df.groupby('prd_key')['prd_start_dt'].shift(-1) - pd.Timedelta(days=1)
        df=df.sort_values(by='prd_id')

        # Reordering column as the per intended table structure
        cols = df.columns.tolist()
        cols.insert(1,cols.pop(cols.index('cat_id')))
        df=df[cols]

    elif table_name == 'crm_sales_details' :      # Transform Sales Details table

        # Remove unwanted space
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

        # Convert date columns to the date format (YYYYMMDD - YYYY-MM-DD)
        df['sls_order_dt']=pd.to_datetime(df['sls_order_dt'].astype(str),format='%Y%m%d',errors='coerce')
        df['sls_ship_dt']=pd.to_datetime(df['sls_ship_dt'].astype(str),format='%Y%m%d',errors='coerce')
        df['sls_due_dt']=pd.to_datetime(df['sls_due_dt'].astype(str),format='%Y%m%d',errors='coerce')

        # Validate Sales values (Sales = Price * Quantity  AND Sales > 0)
        df['sls_sales']=df['sls_sales'].where((df['sls_sales'] > 0 ) 
                                & (df['sls_sales'] == df['sls_price']*df['sls_quantity']),
                            df['sls_price'].abs()*df['sls_quantity'])
        
        # Validate Price values (Price = Sales / Quantity AND Price > 0)
        price_calc = df['sls_sales'] / df['sls_quantity'].replace(0, pd.NA)
        df['sls_price'] = df['sls_price'].where(df['sls_price'] > 0,price_calc)
        df['sls_price'] = df['sls_price'].fillna(0) 
        

    elif table_name == 'erp_cust_az12':    # Transform Customer additional Info table

        # Convert date columns to datetime
        date_cols = ["bdate"]
        df[date_cols] = df[date_cols].apply(pd.to_datetime, errors='coerce')

        # Remove unwanted space
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

        # Normalize customer key (remove prefix 'NAS' if exits)
        df['cid'] = df['cid'].astype(str)
        df['cid']=df['cid'].where(~df['cid'].str.startswith('NAS'),df['cid'].str[3:])

        # Validate birth-date values
        df['bdate']=df['bdate'].where(df['bdate'] < datetime.datetime.now(),pd.NaT)

        # Standardize gender values
        df['gen']=df['gen'].str.upper().\
                    map({'M':'Male','MALE':'Male','F':'Female','FEMALE':'Female'}).fillna('N/A')
        
    elif table_name == 'erp_loc_a101' :       # Transform Customer Location(country) table

        # Remove unwanted space
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

        # Normalize Customer key (remove hyphen '-')
        df['cid']=df['cid'].str.replace('-','')

        # Standardize country values
        df['cntry']=df['cntry'].apply(lambda x: 'N/A' if pd.isna(x) or x == ''
                                        else 'Germany' if x.upper()=='DE'
                                        else 'United States' if x.upper() in ('US','USA') 
                                        else x)

    elif table_name == 'erp_px_cat_g1v2' :     # Transform Product Category table

        # Remove unwanted space
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

        # No further transformation needed.Source data already clean

    else:
        raise ValueError(f"[{table_name}] Transformation failed: Input table name is invalid")
    
    end=time.time()

    time_taken = round(end-start,2)
    logging.info(f"[{table_name}] Transformation End | Records:{len(df)} | Time :{time_taken} seconds.")
    
    return df # return updated dataframe


# ======================== Ingestion Function ========================
# --------------------------------------------------------------------

''' The function expects parameters :
        df         : Dataframe from which data will be ingested into the target table.   
        table_name : Name of target table into which data will be ingested.
        conn_engine: Database engine establishing connection to the target database.
    The function starts a transaction and inserts data. In case of an error, entire
    operation rolls back.
'''
def data_ingestion(df ,table_name : str , conn_engine):
    logging.info(f"[{table_name}] Ingestion Begin ...")
    
    if not isinstance(conn_engine, Engine):
        raise TypeError(f"[{table_name}] Ingestion Failed : Invalid Database Connection Engine")
    
    if df.empty:
        raise ValueError(f"[{table_name}] Ingestion failed: Input dataframe is empty")
    
    if table_name not in table_names_list:
        raise ValueError(f"[{table_name}] Ingestion failed: Input table name is invalid")
    
    start = time.time()

    # start a transaction and perform operation
    with conn_engine.begin() as conn:
        conn.execute(text(f"truncate table {table_name};"))
        df.to_sql(table_name, conn, if_exists ='append', index =False)
        row_count = conn.execute(text(f"select count(*) from {table_name};")).scalar()

    if row_count == 0:
        raise ValueError(f"[{table_name}] Ingestion failed: target table is empty after ingestion")
    
    end = time.time()
    time_taken = round(end-start,2)
    logging.info(f"[{table_name}] Ingestion End | Records:{row_count} | Time :{time_taken} seconds.")



# =========================== ETL Process ===========================
# -------------------------------------------------------------------

table_names_list = ["crm_cust_info", "crm_prd_info", "crm_sales_details", 
               "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"]

if __name__ == '__main__' :
    final_start = time.time()

    bronze_eng = None
    silver_eng = None
    try:
        # Create database engine connection to "bronze" database
        bronze_url = URL.create(
                    drivername="mysql+pymysql",
                    username=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    database='bronze')
        bronze_eng = create_engine(bronze_url)

        with bronze_eng.connect() as conn:
            conn.execute(text("select 1;"))

        # Create database engine connection to "silver" database
        silver_url = URL.create(
                    drivername="mysql+pymysql",
                    username=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    database='silver')
        silver_eng = create_engine(silver_url)

        with silver_eng.connect() as conn:
            conn.execute(text("select 1;"))
        
        logging.info("<<< Database Connection Established >>>")

        logging.info("ETL Process Begin :")
        failed_table = []

        for t in table_names_list:
            try:
                logging.info(f"Table : {t}")

                # extract data from source table and store as dataframe (call extraction function)
                data_df = data_extraction(table_name= t ,conn_engine = bronze_eng)

                # update data dataframe after transformation (call transformation function)
                data_df = data_transformation(table_name= t, df= data_df)

                # load data into target table in the 'silver' database (call ingestion function)
                data_ingestion(
                    df = data_df,
                    table_name = t,
                    conn_engine = silver_eng)
        
            except (ValueError,TypeError) as e:
                logging.error(f"{e}")
                failed_table.append(t)
                
            except SQLAlchemyError as e:
                logging.exception(f"[{t}] : Database Error occured during ETL ")
                failed_table.append(t)

            except Exception as e:
                logging.exception(f"[{t}] : Unexpected Error occured during ETL ")
                failed_table.append(t)

    except SQLAlchemyError as e:
        logging.exception("Database Error : Failed to establish database connection")
        raise

    except Exception as e:
        logging.exception("Unexpected Error : Failed to establish database connection")
        raise

    finally:
        final_end = time.time()
        final_time = round(final_end - final_start,2)

        if failed_table :
            logging.warning(f"ETL Complete with Failures :{failed_table}.Time taken:{final_time} seconds")
        else:
            logging.info(f"ETL Complete for all tables.Time taken: {final_time} seconds")

        if bronze_eng:
            bronze_eng.dispose()
        if silver_eng:
            silver_eng.dispose()
        
        logging.info("xxx  Database Connection closed  xxx")

# ------------------------------------------------------------------------------