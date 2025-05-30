-- ========================================
-- Silver Layer Unity Catalog Tables DDL
-- ========================================

-- 1. Date Weather Status Table
CREATE TABLE IF NOT EXISTS icecreamdb.silver.date_weather_status (
    weather_date DATE,
    dominant_weather_type STRING,
    dominant_temp_category STRING,
    processing_date DATE,
    processing_timestamp TIMESTAMP,
    processed_by STRING
)
USING DELTA
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/silver/date_weather_status/'
COMMENT 'Silver layer table for date weather status lookup data';

-- 2. Products Lookup Table
CREATE TABLE IF NOT EXISTS icecreamdb.silver.products_lookup (
    product_code STRING,
    product_name STRING,
    price DOUBLE,
    processing_date DATE,
    processing_timestamp TIMESTAMP,
    processed_by STRING
)
USING DELTA
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/silver/products_lookup/'
COMMENT 'Silver layer table for products lookup master data';

-- 3. Sales Transactions Table
CREATE TABLE IF NOT EXISTS icecreamdb.silver.sales_transactions (
    sales_date DATE,
    product_code STRING,
    total_sales_amount DOUBLE,
    transaction_count BIGINT,
    processing_date DATE,
    processing_timestamp TIMESTAMP,
    processed_by STRING
)
USING DELTA
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/silver/sales_transactions/'
COMMENT 'Silver layer table for aggregated sales transaction data';

-- 4. Weather API Table
CREATE TABLE IF NOT EXISTS icecreamdb.silver.weather_api (
    latitude DOUBLE,
    longitude DOUBLE,
    location_name STRING,
    timezone STRING,
    timezone_offset INT,
    weather_date STRING,
    temp_max_c DOUBLE,
    temp_min_c DOUBLE,
    temp_avg_c DOUBLE,
    humidity_pct DOUBLE,
    precipitation_mm DOUBLE,
    weather_conditions STRING,
    weather_description STRING,
    processing_date DATE,
    processing_timestamp TIMESTAMP,
    processed_by STRING
)
USING DELTA
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/silver/weather_api/'
COMMENT 'Silver layer table for processed weather API data';

-- Country Index Table DDL
CREATE TABLE IF NOT EXISTS icecreamdb.silver.country_index (
    country STRING,
    year INT,
    average_price DOUBLE,
    country_index DOUBLE,
    processing_date DATE,
    processing_timestamp TIMESTAMP,
    processed_by STRING
)
USING DELTA
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/silver/country_index/'
COMMENT 'Silver layer table for country index and pricing lookup data';

-- Rental Prices Monthly Table
CREATE TABLE IF NOT EXISTS icecreamdb.silver.rental_prices_monthly (
    country STRING,
    month INT,
    year INT,
    rental_price DOUBLE,
    processing_date DATE,
    processing_timestamp TIMESTAMP,
    processed_by STRING
)
USING DELTA
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/silver/rental_prices_monthly/'
COMMENT 'Silver layer table for monthly ice cream store rental prices by country in EUR';

-- Global Country Weather Table
CREATE TABLE IF NOT EXISTS icecreamdb.silver.global_country_weather (
    country STRING,
    weather_date DATE,
    avg_temp_c DOUBLE,
    avg_temp_f DOUBLE,
    avg_wind_mph DOUBLE,
    avg_wind_kph DOUBLE,
    avg_humidity DOUBLE,
    avg_feelslike_c DOUBLE,
    avg_feelslike_f DOUBLE,
    avg_pressure_mb DOUBLE,
    avg_precip_mm DOUBLE,
    avg_cloud_cover DOUBLE,
    avg_uv_index DOUBLE,
    avg_gust_mph DOUBLE,
    avg_gust_kph DOUBLE,
    processing_date DATE,
    processing_timestamp TIMESTAMP,
    processed_by STRING
)
USING DELTA
LOCATION 'abfss://icecreamblob@psmltest9911652735.dfs.core.windows.net/silver/global_country_weather/'
COMMENT 'Silver layer table for aggregated daily weather data by country from GlobalWeatherRepository';
