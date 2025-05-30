# Databricks notebook source
# MAGIC %sql
# MAGIC -- ==================================================
# MAGIC -- COMPLETE ETL PROCESSING SCRIPT FOR DATABRICKS
# MAGIC -- Run in order: dimensions first, then fact table
# MAGIC -- ==================================================
# MAGIC
# MAGIC -- 1. PROCESS DIM_PRODUCT
# MAGIC -- ==================================================
# MAGIC WITH watermark_cte AS (
# MAGIC     SELECT watermark_value 
# MAGIC     FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC     WHERE table_name = 'dim_product' AND status = 'ACTIVE'
# MAGIC ),
# MAGIC incremental_products AS (
# MAGIC     SELECT DISTINCT
# MAGIC         product_code,
# MAGIC         product_name,
# MAGIC         price,
# MAGIC         processing_timestamp
# MAGIC     FROM icecreamdb.silver.products_lookup
# MAGIC     CROSS JOIN watermark_cte
# MAGIC     WHERE processing_timestamp > watermark_cte.watermark_value
# MAGIC )
# MAGIC MERGE INTO icecreamdb.gold.dim_product AS target
# MAGIC USING incremental_products AS source
# MAGIC ON target.product_code = source.product_code 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.product_name != source.product_name OR
# MAGIC     target.price != source.price
# MAGIC ) THEN UPDATE SET
# MAGIC     effective_end_dt = source.processing_timestamp,
# MAGIC     is_current = false
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     product_code, product_name, price, 
# MAGIC     effective_start_dt, effective_end_dt, is_current
# MAGIC ) VALUES (
# MAGIC     source.product_code, source.product_name, source.price,
# MAGIC     source.processing_timestamp, TIMESTAMP('9999-12-31 23:59:59'), true
# MAGIC );
# MAGIC
# MAGIC -- Insert new versions for changed products
# MAGIC WITH watermark_cte AS (
# MAGIC     SELECT watermark_value 
# MAGIC     FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC     WHERE table_name = 'dim_product' AND status = 'ACTIVE'
# MAGIC )
# MAGIC INSERT INTO icecreamdb.gold.dim_product (
# MAGIC     product_code, product_name, price, 
# MAGIC     effective_start_dt, effective_end_dt, is_current
# MAGIC )
# MAGIC SELECT 
# MAGIC     source.product_code, source.product_name, source.price,
# MAGIC     source.processing_timestamp, TIMESTAMP('9999-12-31 23:59:59'), true
# MAGIC FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         product_code, product_name, price, processing_timestamp
# MAGIC     FROM icecreamdb.silver.products_lookup
# MAGIC     CROSS JOIN watermark_cte
# MAGIC     WHERE processing_timestamp > watermark_cte.watermark_value
# MAGIC ) source
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1 FROM icecreamdb.gold.dim_product target
# MAGIC     WHERE source.product_code = target.product_code
# MAGIC     AND target.is_current = false
# MAGIC     AND target.effective_end_dt = source.processing_timestamp
# MAGIC );
# MAGIC
# MAGIC -- Update dim_product watermark
# MAGIC UPDATE icecreamdb.metadata_config.gold_watermark 
# MAGIC SET watermark_value = COALESCE(
# MAGIC     (SELECT MAX(pl.processing_timestamp) 
# MAGIC      FROM icecreamdb.silver.products_lookup pl
# MAGIC      CROSS JOIN (
# MAGIC          SELECT watermark_value 
# MAGIC          FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC          WHERE table_name = 'dim_product' AND status = 'ACTIVE'
# MAGIC      ) w
# MAGIC      WHERE pl.processing_timestamp > w.watermark_value),
# MAGIC     watermark_value
# MAGIC ),
# MAGIC     last_updated = current_timestamp()
# MAGIC WHERE table_name = 'dim_product' AND status = 'ACTIVE';
# MAGIC
# MAGIC -- 2. PROCESS DIM_COUNTRY
# MAGIC -- ==================================================
# MAGIC WITH watermark_cte AS (
# MAGIC     SELECT watermark_value 
# MAGIC     FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC     WHERE table_name = 'dim_country' AND status = 'ACTIVE'
# MAGIC ),
# MAGIC incremental_countries AS (
# MAGIC     SELECT DISTINCT
# MAGIC         country,
# MAGIC         country_index,
# MAGIC         average_price,
# MAGIC         processing_timestamp
# MAGIC     FROM icecreamdb.silver.country_index
# MAGIC     CROSS JOIN watermark_cte
# MAGIC     WHERE processing_timestamp > watermark_cte.watermark_value
# MAGIC )
# MAGIC MERGE INTO icecreamdb.gold.dim_country AS target
# MAGIC USING incremental_countries AS source
# MAGIC ON target.country_name = source.country 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.country_index != source.country_index OR
# MAGIC     target.average_price != source.average_price
# MAGIC ) THEN UPDATE SET
# MAGIC     effective_end_dt = source.processing_timestamp,
# MAGIC     is_current = false
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     country_name, country_index, average_price,
# MAGIC     effective_start_dt, effective_end_dt, is_current
# MAGIC ) VALUES (
# MAGIC     source.country, source.country_index, source.average_price,
# MAGIC     source.processing_timestamp, TIMESTAMP('9999-12-31 23:59:59'), true
# MAGIC );
# MAGIC
# MAGIC -- Insert new versions for changed countries
# MAGIC WITH watermark_cte AS (
# MAGIC     SELECT watermark_value 
# MAGIC     FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC     WHERE table_name = 'dim_country' AND status = 'ACTIVE'
# MAGIC )
# MAGIC INSERT INTO icecreamdb.gold.dim_country (
# MAGIC     country_name, country_index, average_price,
# MAGIC     effective_start_dt, effective_end_dt, is_current
# MAGIC )
# MAGIC SELECT 
# MAGIC     source.country, source.country_index, source.average_price,
# MAGIC     source.processing_timestamp, TIMESTAMP('9999-12-31 23:59:59'), true
# MAGIC FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         country, country_index, average_price, processing_timestamp
# MAGIC     FROM icecreamdb.silver.country_index
# MAGIC     CROSS JOIN watermark_cte
# MAGIC     WHERE processing_timestamp > watermark_cte.watermark_value
# MAGIC ) source
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1 FROM icecreamdb.gold.dim_country target
# MAGIC     WHERE source.country = target.country_name
# MAGIC     AND target.is_current = false
# MAGIC     AND target.effective_end_dt = source.processing_timestamp
# MAGIC );
# MAGIC
# MAGIC -- Update dim_country watermark
# MAGIC UPDATE icecreamdb.metadata_config.gold_watermark 
# MAGIC SET watermark_value = COALESCE(
# MAGIC     (SELECT MAX(ci.processing_timestamp) 
# MAGIC      FROM icecreamdb.silver.country_index ci
# MAGIC      CROSS JOIN (
# MAGIC          SELECT watermark_value 
# MAGIC          FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC          WHERE table_name = 'dim_country' AND status = 'ACTIVE'
# MAGIC      ) w
# MAGIC      WHERE ci.processing_timestamp > w.watermark_value),
# MAGIC     watermark_value
# MAGIC ),
# MAGIC     last_updated = current_timestamp()
# MAGIC WHERE table_name = 'dim_country' AND status = 'ACTIVE';
# MAGIC
# MAGIC -- 3. PROCESS FACT_SALES
# MAGIC -- ==================================================
# MAGIC WITH watermark_cte AS (
# MAGIC     SELECT watermark_value 
# MAGIC     FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC     WHERE table_name = 'fact_sales' AND status = 'ACTIVE'
# MAGIC ),
# MAGIC incremental_sales AS (
# MAGIC     SELECT
# MAGIC         -- Dimension Keys
# MAGIC         dd.date_sk,
# MAGIC         dp.product_sk,
# MAGIC         dc.country_sk,
# MAGIC         
# MAGIC         -- Business Keys
# MAGIC         st.sales_date,
# MAGIC         st.product_code,
# MAGIC         
# MAGIC         -- Core Measures
# MAGIC         st.total_sales_amount,
# MAGIC         st.transaction_count,
# MAGIC         st.total_sales_amount * ci.country_index / 100 AS sales_qty_amnt,
# MAGIC         st.total_sales_amount * (ci.country_index / 100) * pl.price AS sales_value_amnt,
# MAGIC         
# MAGIC         -- Weather Measures
# MAGIC         gw.avg_temp_c,
# MAGIC         gw.avg_temp_f,
# MAGIC         gw.avg_humidity,
# MAGIC         gw.avg_precip_mm,
# MAGIC         gw.avg_wind_kph,
# MAGIC         gw.avg_pressure_mb,
# MAGIC         gw.avg_cloud_cover,
# MAGIC         gw.avg_uv_index,
# MAGIC         
# MAGIC         -- Rental Price Info
# MAGIC         rp.rental_price,
# MAGIC         rp.month AS rental_month,
# MAGIC         rp.year AS rental_year,
# MAGIC         
# MAGIC         -- Processing Metadata
# MAGIC         st.processing_date,
# MAGIC         st.processing_timestamp,
# MAGIC         'ETL_PROCESS' AS processed_by
# MAGIC
# MAGIC     FROM icecreamdb.silver.sales_transactions AS st
# MAGIC     CROSS JOIN watermark_cte
# MAGIC
# MAGIC     -- Join with products (for current price)
# MAGIC     JOIN icecreamdb.silver.products_lookup AS pl
# MAGIC         ON st.product_code = pl.product_code
# MAGIC
# MAGIC     -- Join with country index
# MAGIC     JOIN icecreamdb.silver.country_index AS ci
# MAGIC         ON YEAR(st.sales_date) = ci.year
# MAGIC
# MAGIC     -- Join with dimension tables using proper SCD lookups
# MAGIC     JOIN icecreamdb.gold.dim_date AS dd
# MAGIC         ON dd.date_sk = YEAR(st.sales_date) * 10000 + MONTH(st.sales_date) * 100 + DAY(st.sales_date)
# MAGIC
# MAGIC     JOIN icecreamdb.gold.dim_product AS dp
# MAGIC         ON dp.product_code = st.product_code
# MAGIC         AND dp.is_current = true
# MAGIC     
# MAGIC
# MAGIC     JOIN icecreamdb.gold.dim_country AS dc
# MAGIC         ON dc.country_name = ci.country
# MAGIC         AND dc.is_current = true
# MAGIC
# MAGIC
# MAGIC     -- Left join weather data
# MAGIC     LEFT JOIN icecreamdb.silver.global_country_weather AS gw
# MAGIC         ON st.sales_date = gw.weather_date
# MAGIC         AND UPPER(gw.country) = UPPER(ci.country)
# MAGIC
# MAGIC     -- Left join rental prices
# MAGIC     LEFT JOIN icecreamdb.silver.rental_prices_monthly AS rp
# MAGIC         ON YEAR(st.sales_date) = rp.year
# MAGIC         AND MONTH(st.sales_date) = rp.month
# MAGIC         AND UPPER(gw.country) = UPPER(rp.country)
# MAGIC
# MAGIC     -- INCREMENTAL FILTER: Only process new/updated records
# MAGIC     WHERE st.processing_timestamp > watermark_cte.watermark_value
# MAGIC )
# MAGIC MERGE INTO icecreamdb.gold.fact_sales AS target
# MAGIC USING incremental_sales AS source
# MAGIC ON target.sales_date = source.sales_date 
# MAGIC    AND target.product_code = source.product_code 
# MAGIC    AND target.country_sk = source.country_sk
# MAGIC
# MAGIC -- Update existing records
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     date_sk = source.date_sk,
# MAGIC     product_sk = source.product_sk,
# MAGIC     country_sk = source.country_sk,
# MAGIC     total_sales_amount = source.total_sales_amount,
# MAGIC     transaction_count = source.transaction_count,
# MAGIC     sales_qty_amnt = source.sales_qty_amnt,
# MAGIC     sales_value_amnt = source.sales_value_amnt,
# MAGIC     avg_temp_c = source.avg_temp_c,
# MAGIC     avg_temp_f = source.avg_temp_f,
# MAGIC     avg_humidity = source.avg_humidity,
# MAGIC     avg_precip_mm = source.avg_precip_mm,
# MAGIC     avg_wind_kph = source.avg_wind_kph,
# MAGIC     avg_pressure_mb = source.avg_pressure_mb,
# MAGIC     avg_cloud_cover = source.avg_cloud_cover,
# MAGIC     avg_uv_index = source.avg_uv_index,
# MAGIC     rental_price = source.rental_price,
# MAGIC     rental_month = source.rental_month,
# MAGIC     rental_year = source.rental_year,
# MAGIC     processing_date = source.processing_date,
# MAGIC     processing_timestamp = source.processing_timestamp,
# MAGIC     processed_by = source.processed_by
# MAGIC
# MAGIC -- Insert new records
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     date_sk, product_sk, country_sk,
# MAGIC     sales_date, product_code,
# MAGIC     total_sales_amount, transaction_count,
# MAGIC     sales_qty_amnt, sales_value_amnt,
# MAGIC     avg_temp_c, avg_temp_f, avg_humidity, avg_precip_mm,
# MAGIC     avg_wind_kph, avg_pressure_mb, avg_cloud_cover, avg_uv_index,
# MAGIC     rental_price, rental_month, rental_year,
# MAGIC     processing_date, processing_timestamp, processed_by
# MAGIC ) VALUES (
# MAGIC     source.date_sk, source.product_sk, source.country_sk,
# MAGIC     source.sales_date, source.product_code,
# MAGIC     source.total_sales_amount, source.transaction_count,
# MAGIC     source.sales_qty_amnt, source.sales_value_amnt,
# MAGIC     source.avg_temp_c, source.avg_temp_f, source.avg_humidity, source.avg_precip_mm,
# MAGIC     source.avg_wind_kph, source.avg_pressure_mb, source.avg_cloud_cover, source.avg_uv_index,
# MAGIC     source.rental_price, source.rental_month, source.rental_year,
# MAGIC     source.processing_date, source.processing_timestamp, source.processed_by
# MAGIC );
# MAGIC
# MAGIC -- Update fact_sales watermark
# MAGIC UPDATE icecreamdb.metadata_config.gold_watermark 
# MAGIC SET watermark_value = COALESCE(
# MAGIC     (SELECT MAX(st.processing_timestamp) 
# MAGIC      FROM icecreamdb.silver.sales_transactions st
# MAGIC      CROSS JOIN (
# MAGIC          SELECT watermark_value 
# MAGIC          FROM icecreamdb.metadata_config.gold_watermark 
# MAGIC          WHERE table_name = 'fact_sales' AND status = 'ACTIVE'
# MAGIC      ) w
# MAGIC      WHERE st.processing_timestamp > w.watermark_value),
# MAGIC     watermark_value
# MAGIC ),
# MAGIC     last_updated = current_timestamp()
# MAGIC WHERE table_name = 'fact_sales' AND status = 'ACTIVE';

# COMMAND ----------



# COMMAND ----------


