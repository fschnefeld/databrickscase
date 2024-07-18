-- Databricks notebook source
-- Create Bronze Table
SET spark.source;
CREATE OR REPLACE LIVE TABLE sales_bronze (
    invoice_id STRING,
    branch STRING,
    city STRING,
    customer_type STRING,
    gender STRING,
    product_line STRING,
    unit_price DOUBLE,
    quantity INT,
    tax DOUBLE,
    total DOUBLE,
    date DATE,
    created_at TIMESTAMP,
    payment STRING,
    cogs DOUBLE,
    gross_margin_percentage DOUBLE,
    gross_income DOUBLE,
    rating DOUBLE
)
TBLPROPERTIES ("quality" = "bronze", "inferSchema" = "true")
COMMENT "New customer sales data"
AS 
SELECT * 
FROM intellishorefunctioning.default.supermarket_sales_sheet_1
;

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE customer_bronze_clean_v(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_address EXPECT (address IS NOT NULL),
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(LIVE.customer_bronze);

-- COMMAND ----------

CREATE LIVE TABLE customer_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.customer_silver
FROM stream(LIVE.customer_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date 
  COLUMNS * EXCEPT (operation, operation_date, 
_rescued_data);  
