-- Create (if not exists) the Gold database
CREATE DATABASE IF NOT EXISTS icecreamdb.gold
COMMENT 'Gold layer for star schema tables (dimensions & facts)';

-- Create the dim_date table structure
CREATE TABLE IF NOT EXISTS icecreamdb.gold.dim_date (
    date_sk             INT,         -- Surrogate key in YYYYMMDD format
    full_date           DATE,        -- Actual date
    calendar_year       INT,
    calendar_month      INT,
    calendar_month_name STRING,      -- e.g., "January"
    calendar_day        INT,
    calendar_quarter    INT,
    week_of_year        INT,
    day_of_week         INT,         -- Typically 1=Sunday, 7=Saturday in Spark
    day_of_week_name    STRING       -- e.g., "Monday"
)
USING DELTA
COMMENT 'Calendar dimension table holding daily attributes'
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/gold/dim_date/';


WITH calendar_cte AS (
  SELECT sequence(
    TO_DATE('2023-01-01'),
    TO_DATE('2023-12-31'),
    INTERVAL 1 DAY
  ) AS all_days_in_2023
)
INSERT INTO icecreamdb.gold.dim_date
SELECT
  YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)     AS date_sk,          -- e.g. 20230101
  d                                             AS full_date,
  YEAR(d)                                       AS calendar_year,
  MONTH(d)                                      AS calendar_month,
  DATE_FORMAT(d, 'MMMM')                        AS calendar_month_name,  -- "January", etc.
  DAY(d)                                        AS calendar_day,
  QUARTER(d)                                    AS calendar_quarter,
  WEEKOFYEAR(d)                                 AS week_of_year,
  DAYOFWEEK(d)                                  AS day_of_week,         -- 1=Sunday ... 7=Saturday
  DATE_FORMAT(d, 'EEEE')                        AS day_of_week_name     -- "Monday", etc.
FROM calendar_cte
LATERAL VIEW EXPLODE(all_days_in_2023) AS d;



CREATE TABLE IF NOT EXISTS icecreamdb.gold.dim_product (
    product_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    product_code        STRING     NOT NULL,  -- Business key
    product_name        STRING     NOT NULL,
    price               DOUBLE,
    -- SCD Type 2 columns
    effective_start_dt  TIMESTAMP  NOT NULL,
    effective_end_dt    TIMESTAMP  NOT NULL,  -- typically some high date like '9999-12-31' if current
    is_current          BOOLEAN    NOT NULL,

    
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
)
USING DELTA
COMMENT 'SCD Type 2 dimension for products'
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/gold/dim_product/';


CREATE TABLE IF NOT EXISTS icecreamdb.gold.dim_country (
    country_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    country_name        STRING NOT NULL,
    country_index       DOUBLE,
    average_price       DOUBLE,
    -- SCD Type 2 columns
    effective_start_dt  TIMESTAMP NOT NULL,
    effective_end_dt    TIMESTAMP NOT NULL,
    is_current          BOOLEAN   NOT NULL,
    
    CONSTRAINT pk_dim_country PRIMARY KEY (country_sk)
)
USING DELTA
COMMENT 'SCD Type 2 dimension for country-level data'
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/gold/dim_country/';

-- Create the fact sales table
CREATE TABLE IF NOT EXISTS icecreamdb.gold.fact_sales (
    sales_sk            BIGINT GENERATED ALWAYS AS IDENTITY,
    
    -- Dimension Foreign Keys
    date_sk             INT ,           -- References dim_date
    product_sk          BIGINT ,        -- References dim_product (SCD Type 2)
    country_sk          BIGINT ,        -- References dim_country (SCD Type 2)
    
    -- Business Keys (for debugging/lineage)
    sales_date          DATE ,
    product_code        STRING ,
    
    -- Core Sales Measures
    total_sales_amount  DOUBLE,
    transaction_count   BIGINT,
    sales_qty_amnt      DOUBLE,                 -- Calculated: total_sales_amount * country_index/100
    sales_value_amnt    DOUBLE,                 -- Calculated: total_sales_amount * (country_index/100) * price
    
    -- Weather Measures (from global_country_weather)
    avg_temp_c          DOUBLE,
    avg_temp_f          DOUBLE,
    avg_humidity        DOUBLE,
    avg_precip_mm       DOUBLE,
    avg_wind_kph        DOUBLE,
    avg_pressure_mb     DOUBLE,
    avg_cloud_cover     DOUBLE,
    avg_uv_index        DOUBLE,
    
    -- Rental Price Information
    rental_price        DOUBLE,                 -- Monthly rental price in EUR
    rental_month        INT,
    rental_year         INT,
    
    -- Processing Metadata
    processing_date     DATE,
    processing_timestamp TIMESTAMP,
    processed_by        STRING
)
USING DELTA
COMMENT 'Fact table for ice cream sales with weather and rental price context'
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/gold/fact_sales/'
PARTITIONED BY (sales_date);


