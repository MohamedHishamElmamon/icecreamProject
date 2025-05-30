# Databricks notebook source
# Silver Layer Weather Data Processing Pipeline
# Author: Data Engineering Team
# Purpose: Process weather API data from bronze to silver layer with incremental loading


def process_weather_silver_layer():
    """
    Main function to process weather data from bronze to silver layer
    """
    
    # Configuration
    storage_account = "psmltest9911652735"
    container = "icecreamblob"
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    
    try:
        # Step 1: Get last processed date from bronze watermark
        watermark_query = """
        SELECT 
            table_name,
            last_updated,
            DATE_FORMAT(last_updated, 'yyyy-MM-dd') as folder_date
        FROM icecreamdb.metadata_config.bronze_watermark 
        WHERE table_name = 'weather_api' AND status = 'ACTIVE'
        ORDER BY last_updated DESC LIMIT 1
        """
        
        watermark_df = spark.sql(watermark_query)
        
        if watermark_df.count() > 0:
            last_processed_date = watermark_df.collect()[0]['folder_date']
            print(f"INFO: Last processed date from watermark: {last_processed_date}")
        else:
            last_processed_date = "2025-05-29"
            print(f"WARNING: No watermark found, using default date: {last_processed_date}")
        
        # Step 2: Read JSON files from bronze layer
        json_path = f"{base_path}bronze/API/{last_processed_date}/"
        print(f"INFO: Processing files from path: {json_path}")
        
        files = dbutils.fs.ls(json_path)
        json_files = [f for f in files if f.name.endswith('.json')]
        
        if not json_files:
            print("ERROR: No JSON files found in the specified path")
            return False
        
        print(f"INFO: Found {len(json_files)} JSON files to process")
        
        # Step 3: Read and parse JSON data
        json_df = spark.read.option("multiline", "true").json(json_path + "*.json")
        
        # Step 4: Transform data for silver layer
        days_df = json_df.select(
            col("latitude"),
            col("longitude"),
            col("resolvedAddress"),
            col("timezone"),
            col("tzoffset"),
            explode(col("days")).alias("day_data")
        )
        
        weather_silver = days_df.select(
            col("latitude").cast("DOUBLE").alias("latitude"),
            col("longitude").cast("DOUBLE").alias("longitude"),
            col("resolvedAddress").alias("location_name"),
            col("timezone").alias("timezone"),
            col("tzoffset").cast("INTEGER").alias("timezone_offset"),
            col("day_data.datetime").alias("weather_date"),
            col("day_data.datetimeEpoch").cast("LONG").alias("date_epoch"),
            round(col("day_data.tempmax").cast("DOUBLE"), 2).alias("temp_max_c"),
            round(col("day_data.tempmin").cast("DOUBLE"), 2).alias("temp_min_c"),
            round(col("day_data.temp").cast("DOUBLE"), 2).alias("temp_avg_c"),
            round(col("day_data.feelslikemax").cast("DOUBLE"), 2).alias("feels_like_max_c"),
            round(col("day_data.feelslikemin").cast("DOUBLE"), 2).alias("feels_like_min_c"),
            round(col("day_data.feelslike").cast("DOUBLE"), 2).alias("feels_like_avg_c"),
            round(col("day_data.dew").cast("DOUBLE"), 2).alias("dew_point_c"),
            round(col("day_data.humidity").cast("DOUBLE"), 1).alias("humidity_pct"),
            round(col("day_data.precip").cast("DOUBLE"), 3).alias("precipitation_mm"),
            round(col("day_data.precipprob").cast("DOUBLE"), 1).alias("precipitation_prob_pct"),
            round(col("day_data.precipcover").cast("DOUBLE"), 1).alias("precipitation_cover_pct"),
            col("day_data.preciptype").alias("precipitation_type"),
            round(col("day_data.windgust").cast("DOUBLE"), 1).alias("wind_gust_kmh"),
            round(col("day_data.windspeed").cast("DOUBLE"), 1).alias("wind_speed_kmh"),
            col("day_data.winddir").cast("INTEGER").alias("wind_direction_deg"),
            round(col("day_data.pressure").cast("DOUBLE"), 1).alias("pressure_mb"),
            round(col("day_data.cloudcover").cast("DOUBLE"), 1).alias("cloud_cover_pct"),
            round(col("day_data.visibility").cast("DOUBLE"), 1).alias("visibility_km"),
            round(col("day_data.solarradiation").cast("DOUBLE"), 1).alias("solar_radiation"),
            round(col("day_data.solarenergy").cast("DOUBLE"), 1).alias("solar_energy"),
            col("day_data.uvindex").cast("INTEGER").alias("uv_index"),
            col("day_data.sunrise").alias("sunrise_time"),
            col("day_data.sunset").alias("sunset_time"),
            round(col("day_data.moonphase").cast("DOUBLE"), 2).alias("moon_phase"),
            col("day_data.conditions").alias("weather_conditions"),
            col("day_data.description").alias("weather_description"),
            col("day_data.icon").alias("weather_icon"),
            col("day_data.source").alias("data_source"),
            current_date().alias("processing_date"),
            current_timestamp().alias("processing_timestamp"),
            lit("Silver_Data_Processing").alias("processed_by")
        ).filter(
            col("weather_date").isNotNull() &
            col("temp_avg_c").isNotNull() &
            (col("temp_avg_c") >= -50) & (col("temp_avg_c") <= 60)
        )
        
        record_count = weather_silver.count()
        print(f"INFO: Processed {record_count} weather records")
        
        # Step 5: Save to silver layer
        silver_output_path = f"{base_path}silver/weather_api/"
        
        weather_silver.write \
            .mode("append") \
            .format("delta") \
            .partitionBy("processing_date") \
            .save(silver_output_path)
        
        print(f"SUCCESS: Data saved to silver layer: {silver_output_path}")
        
        # Step 6: Update watermark using MERGE
        current_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_date_obj = datetime.strptime(last_processed_date, '%Y-%m-%d')
        next_date_obj = current_date_obj + timedelta(days=1)
        next_processing_date = next_date_obj.strftime('%Y-%m-%d %H:%M:%S')
        
        new_watermark_data = spark.createDataFrame([
            (
                "weather_api",
                "bronze layer (ADLS)",
                next_processing_date,
                current_processing_time,
                current_processing_time,
                "ACTIVE",
                "Silver_Data_Processing",
                "Basic Transformation and cleaning for raw data in incremental way",
                "Silver_Data_Processing Notebook"
            )
        ], [
            "table_name", "source_system", "watermark_value",
            "last_updated", "created_date", "status",
            "pipeline_name", "description", "updated_by"
        ])
        
        new_watermark_data.createOrReplaceTempView("new_watermark")
        
        merge_query = """
        MERGE INTO icecreamdb.metadata_config.silver_watermark AS target
        USING new_watermark AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN 
            UPDATE SET
                watermark_value = source.watermark_value,
                last_updated = source.last_updated,
                status = source.status,
                pipeline_name = source.pipeline_name,
                description = source.description,
                updated_by = source.updated_by
        WHEN NOT MATCHED THEN
            INSERT (
                table_name, source_system, watermark_value,
                last_updated, created_date, status,
                pipeline_name, description, updated_by
            )
            VALUES (
                source.table_name, source.source_system, source.watermark_value,
                source.last_updated, source.created_date, source.status,
                source.pipeline_name, source.description, source.updated_by
            )
        """
        
        spark.sql(merge_query)
        print(f"SUCCESS: Watermark updated to {next_processing_date}")
        
        # Step 7: Data quality summary
        unique_dates = weather_silver.select("weather_date").distinct().count()
        unique_locations = weather_silver.select("location_name").distinct().count()
        
        temp_stats = weather_silver.agg(
            min("temp_avg_c").alias("min_temp"),
            max("temp_avg_c").alias("max_temp"),
            avg("temp_avg_c").alias("avg_temp")
        ).collect()[0]
        
        print(f"SUMMARY: Processed {record_count} records, {unique_dates} dates, {unique_locations} locations")
        print(f"SUMMARY: Temperature range {temp_stats['min_temp']:.1f}C to {temp_stats['max_temp']:.1f}C")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Pipeline failed - {str(e)}")
        dbutils.notebook.run(
            "/Workspace/Users/rami.moughrabi@paragonshift.com/etl_error_notebook",
            300,
            {
                "error_source": "process_weather_silver_layer",
                "error_message": str(e),
                "pipeline_name": "Silver_Data_Processing",
                "layer": "SILVER",
                "additional_details": "Failed while processing weather data"
            }
            )
        return False

# Execute the pipeline
pipeline_success = process_weather_silver_layer()

if pipeline_success:
    print("COMPLETED: Silver layer weather processing completed successfully")
else:
    print("FAILED: Silver layer weather processing failed")

# COMMAND ----------

# Sales Transaction Silver Layer Processing
# Author: Data Engineering Team
# Purpose: Process sales transaction data from bronze to silver layer

def process_sales_silver_layer():
    """
    Process sales transaction data from bronze to silver layer
    """
    
    # Configuration
    storage_account = "psmltest9911652735"
    container = "icecreamblob"
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    
    try:
        print("INFO: Starting sales transaction data processing")
        
        # Get last processed date for sales data
        sales_watermark_query = """
        SELECT 
            table_name,
            last_updated,
            DATE_FORMAT(last_updated, 'yyyy-MM-dd') as folder_date
        FROM icecreamdb.metadata_config.bronze_watermark 
        WHERE table_name = 'sales_transactions' AND status = 'ACTIVE'
        ORDER BY last_updated DESC LIMIT 1
        """
        
        sales_watermark_df = spark.sql(sales_watermark_query)
        
        if sales_watermark_df.count() > 0:
            sales_last_date = sales_watermark_df.collect()[0]['folder_date']
            print(f"INFO: Sales last processed date: {sales_last_date}")
        else:
            sales_last_date = datetime.now().strftime('%Y-%m-%d')
            print(f"WARNING: No sales watermark found, using today's date: {sales_last_date}")
        
        # Process sales data - read entire folder
        sales_parquet_path = f"{base_path}bronze/TransactionDB/{sales_last_date}/"
        print(f"INFO: Reading parquet files from: {sales_parquet_path}")
        
        try:
            # Read entire folder directly
            sales_raw_df = spark.read.parquet(sales_parquet_path)
            
            # Clean and transform sales data
            sales_silver = sales_raw_df.select(
                to_date(col("sales_date_time")).alias("sales_date"),
                trim(col("gtin")).alias("product_code"),
                col("sales_amount").cast("DOUBLE").alias("sales_amount")
            ).filter(
                col("sales_date").isNotNull() &
                col("product_code").isNotNull() &
                col("sales_amount").isNotNull() &
                (col("sales_amount") > 0)
            ).groupBy(
                "sales_date",
                "product_code"
            ).agg(
                sum("sales_amount").alias("total_sales_amount"),
                count("*").alias("transaction_count")
            ).withColumn(
                "processing_date", current_date()
            ).withColumn(
                "processing_timestamp", current_timestamp()
            ).withColumn(
                "processed_by", lit("Silver_Data_Processing")
            )
            
            sales_count = sales_silver.count()
            print(f"INFO: Processed {sales_count} aggregated sales records")
            
        except Exception as read_error:
            print(f"WARNING: No parquet files found in {sales_parquet_path} - {str(read_error)}")
            return False
        
        # Save sales data to silver
        sales_silver_path = f"{base_path}silver/sales_transactions/"
        sales_silver.write \
            .mode("append") \
            .format("delta") \
            .partitionBy("processing_date") \
            .save(sales_silver_path)
        
        print(f"SUCCESS: Sales data saved to: {sales_silver_path}")
        
        # Update sales watermark
        current_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sales_current_date = datetime.strptime(sales_last_date, '%Y-%m-%d')
        sales_next_date = sales_current_date + timedelta(days=1)
        sales_next_processing = sales_next_date.strftime('%Y-%m-%d %H:%M:%S')
        
        sales_watermark_data = spark.createDataFrame([
            (
                "sales_transactions",
                "bronze layer (ADLS)",
                sales_next_processing,
                current_processing_time,
                current_processing_time,
                "ACTIVE",
                "Silver_Data_Processing",
                "Basic Transformation and cleaning for sales data in incremental way",
                "Silver_Data_Processing Notebook"
            )
        ], [
            "table_name", "source_system", "watermark_value",
            "last_updated", "created_date", "status",
            "pipeline_name", "description", "updated_by"
        ])
        
        sales_watermark_data.createOrReplaceTempView("sales_watermark_new")
        
        spark.sql("""
        MERGE INTO icecreamdb.metadata_config.silver_watermark AS target
        USING sales_watermark_new AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN 
            UPDATE SET
                watermark_value = source.watermark_value,
                last_updated = source.last_updated,
                status = source.status,
                pipeline_name = source.pipeline_name,
                description = source.description,
                updated_by = source.updated_by
        WHEN NOT MATCHED THEN
            INSERT (
                table_name, source_system, watermark_value,
                last_updated, created_date, status,
                pipeline_name, description, updated_by
            )
            VALUES (
                source.table_name, source.source_system, source.watermark_value,
                source.last_updated, source.created_date, source.status,
                source.pipeline_name, source.description, source.updated_by
            )
        """)
        
        print(f"SUCCESS: Sales watermark updated to {sales_next_processing}")
        print(f"SUMMARY: Sales records processed: {sales_count}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Pipeline failed - {str(e)}")
        dbutils.notebook.run(
            "/Workspace/Users/rami.moughrabi@paragonshift.com/etl_error_notebook",
            300,
            {
                "error_source": "process_sales_silver_layer",
                "error_message": str(e),
                "pipeline_name": "Silver_Data_Processing",
                "layer": "SILVER",
                "additional_details": "Failed while processing sales data"
            }
            )
        return False

# Execute the sales processing pipeline
sales_success = process_sales_silver_layer()

if sales_success:
    print("COMPLETED: Sales silver layer processing completed successfully")
else:
    print("FAILED: Sales silver layer processing failed")

# COMMAND ----------

# Global Weather Repository Processing (Daily Aggregation by Country)
# Author: Data Engineering Team
# Purpose: Read GlobalWeatherRepository.csv from bronze, aggregate daily weather by country, and write to silver layer.

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

def process_global_weather_repository():
    """
    Process GlobalWeatherRepository.csv into daily-level weather info by country,
    storing aggregated results in silver/global_country_weather.
    """
    
    # Configuration
    storage_account = "psmltest9911652735"
    container = "icecreamblob"
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    
    try:
        print("INFO: Starting global weather repository daily processing")
        
        # 1. Read the CSV file from Bronze
        global_weather_csv_path = f"{base_path}bronze/Files/GlobalWeatherRepository.csv"
        print(f"INFO: Reading GlobalWeatherRepository CSV from: {global_weather_csv_path}")
        
        weather_raw_df = (spark.read
            .option("header", True)
            .option("inferSchema", True)  # or define a schema explicitly
            .csv(global_weather_csv_path)
        )
        
        # 2. Parse date from 'last_updated' (which appears as "2023-05-16 13:15", etc.)
        #    We'll store just the date portion (year-month-day).
        #    In case the time zone is relevant, you might need further adjustments.
        weather_with_date_df = weather_raw_df.withColumn(
            "weather_date",
            to_date(col("last_updated"), "yyyy-MM-dd HH:mm")
        )
        
        # 3. Group by country and date, then aggregate numeric columns (averages or max/min as you wish).
        #    Below, we demonstrate average for common numeric metrics.
        #    You can add or remove columns depending on your analytics needs.
        aggregated_df = (weather_with_date_df
            .groupBy("country", "weather_date")
            .agg(
                avg("temperature_celsius").alias("avg_temp_c"),
                avg("temperature_fahrenheit").alias("avg_temp_f"),
                avg("wind_mph").alias("avg_wind_mph"),
                avg("wind_kph").alias("avg_wind_kph"),
                avg("humidity").alias("avg_humidity"),
                avg("feels_like_celsius").alias("avg_feelslike_c"),
                avg("feels_like_fahrenheit").alias("avg_feelslike_f"),
                avg("pressure_mb").alias("avg_pressure_mb"),
                avg("precip_mm").alias("avg_precip_mm"),
                avg("cloud").alias("avg_cloud_cover"),
                avg("uv_index").alias("avg_uv_index"),
                avg("gust_mph").alias("avg_gust_mph"),
                avg("gust_kph").alias("avg_gust_kph")
            )
            .filter(col("weather_date").isNotNull() & col("country").isNotNull())
        )
        
        # 4. Add standard columns for Silver layer
        aggregated_silver = aggregated_df.select(
            trim(col("country")).alias("country"),
            col("weather_date"),
            col("avg_temp_c"),
            col("avg_temp_f"),
            col("avg_wind_mph"),
            col("avg_wind_kph"),
            col("avg_humidity"),
            col("avg_feelslike_c"),
            col("avg_feelslike_f"),
            col("avg_pressure_mb"),
            col("avg_precip_mm"),
            col("avg_cloud_cover"),
            col("avg_uv_index"),
            col("avg_gust_mph"),
            col("avg_gust_kph"),
            current_date().alias("processing_date"),
            current_timestamp().alias("processing_timestamp"),
            lit("global_weather_repository_processing").alias("processed_by")
        )
        
        record_count = aggregated_silver.count()
        print(f"INFO: Aggregated {record_count} daily weather rows by country.")
        
        # 5. Save to Silver as Delta
        weather_silver_path = f"{base_path}silver/global_country_weather/"
        (aggregated_silver
            .write
            .mode("overwrite")   # or "append" if you prefer incremental
            .format("delta")
            .save(weather_silver_path)
        )
        
        print(f"SUCCESS: Global weather data saved to: {weather_silver_path}")
        
        # 6. Optionally create or replace the silver table in Unity Catalog if you want
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS icecreamdb.silver.global_country_weather
            USING DELTA
            LOCATION '{weather_silver_path}'
        """)
        
        print("INFO: Verified/Created table: icecreamdb.silver.global_country_weather")
        
        # 7. Update watermark in silver_watermark
        current_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        watermark_df = spark.createDataFrame([
            (
                "global_country_weather",
                "bronze layer (ADLS)",
                current_processing_time,
                current_processing_time,
                current_processing_time,
                "ACTIVE",
                "global_weather_repository_processing",
                "Aggregated daily weather data by country",
                "global_weather_repo_notebook"
            )
        ], [
            "table_name", "source_system", "watermark_value",
            "last_updated", "created_date", "status",
            "pipeline_name", "description", "updated_by"
        ])
        
        watermark_df.createOrReplaceTempView("gw_watermark_new")
        
        spark.sql("""
        MERGE INTO icecreamdb.metadata_config.silver_watermark AS target
        USING gw_watermark_new AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN 
            UPDATE SET
                watermark_value = source.watermark_value,
                last_updated = source.last_updated,
                status = source.status,
                pipeline_name = source.pipeline_name,
                description = source.description,
                updated_by = source.updated_by
        WHEN NOT MATCHED THEN
            INSERT (
                table_name, source_system, watermark_value,
                last_updated, created_date, status,
                pipeline_name, description, updated_by
            )
            VALUES (
                source.table_name, source.source_system, source.watermark_value,
                source.last_updated, source.created_date, source.status,
                source.pipeline_name, source.description, source.updated_by
            )
        """)
        
        print(f"SUCCESS: Global weather watermark updated.")
        print(f"SUMMARY: Processed {record_count} daily country weather records.")
        
        return True
        
    except Exception as e:
        # If error, call your error notebook
        print(f"ERROR: Pipeline failed - {str(e)}")
        dbutils.notebook.run(
            "/Workspace/Users/rami.moughrabi@paragonshift.com/etl_error_notebook",
            300,
            {
                "error_source": "process_global_weather_repository",
                "error_message": str(e),
                "pipeline_name": "Silver_Data_Processing",
                "layer": "SILVER",
                "additional_details": "Failed while processing GlobalWeatherRepository data"
            }
        )
        return False


# Execute global weather processing
global_weather_success = process_global_weather_repository()

if global_weather_success:
    print("COMPLETED: Global weather repository processing completed successfully.")
else:
    print("FAILED: Global weather repository processing failed.")

# COMMAND ----------

# Products Lookup Processing
# Author: Data Engineering Team
# Purpose: Process products_lookup.csv from bronze to silver layer

def process_products_lookup_file():
    """
    Process products_lookup.csv file from bronze to silver layer
    """
    
    # Configuration
    storage_account = "psmltest9911652735"
    container = "icecreamblob"
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    
    try:
        print("INFO: Starting products lookup file processing")
        
        # Read products_lookup CSV file
        products_lookup_csv_path = f"{base_path}bronze/Files/products_lookup.csv"
        print(f"INFO: Reading products_lookup CSV from: {products_lookup_csv_path}")
        
        products_lookup_raw_df = spark.read.option("header", True).csv(products_lookup_csv_path)
        
        # Clean and transform products lookup data
        products_lookup_silver = products_lookup_raw_df.select(
            trim(col("gtin")).alias("product_code"),
            trim(col("product_name")).alias("product_name"),
            col("price").cast("DOUBLE").alias("price"),
            current_date().alias("processing_date"),
            current_timestamp().alias("processing_timestamp"),
            lit("products_lookup_processing").alias("processed_by")
        ).filter(
            col("product_code").isNotNull() &
            col("product_name").isNotNull() &
            col("price").isNotNull() &
            (col("price") > 0)
        )
        
        products_lookup_count = products_lookup_silver.count()
        print(f"INFO: Processed {products_lookup_count} products lookup records")
        
        # Save products lookup to silver delta lake
        products_lookup_silver_path = f"{base_path}silver/products_lookup/"
        products_lookup_silver.write \
            .mode("overwrite") \
            .format("delta") \
            .save(products_lookup_silver_path)
        
        print(f"SUCCESS: Products lookup data saved to: {products_lookup_silver_path}")
        
        # Log products lookup processing to silver watermark table
        current_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        products_lookup_watermark_data = spark.createDataFrame([
            (
                "products_lookup",
                "bronze layer (ADLS)",
                current_processing_time,
                current_processing_time,
                current_processing_time,
                "ACTIVE",
                "products_lookup_processing",
                "Lookup file processing for products master data",
                "products_lookup_processing_notebook"
            )
        ], [
            "table_name", "source_system", "watermark_value",
            "last_updated", "created_date", "status",
            "pipeline_name", "description", "updated_by"
        ])
        
        products_lookup_watermark_data.createOrReplaceTempView("products_lookup_watermark_new")
        
        spark.sql("""
        MERGE INTO icecreamdb.metadata_config.silver_watermark AS target
        USING products_lookup_watermark_new AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN 
            UPDATE SET
                watermark_value = source.watermark_value,
                last_updated = source.last_updated,
                status = source.status,
                pipeline_name = source.pipeline_name,
                description = source.description,
                updated_by = source.updated_by
        WHEN NOT MATCHED THEN
            INSERT (
                table_name, source_system, watermark_value,
                last_updated, created_date, status,
                pipeline_name, description, updated_by
            )
            VALUES (
                source.table_name, source.source_system, source.watermark_value,
                source.last_updated, source.created_date, source.status,
                source.pipeline_name, source.description, source.updated_by
            )
        """)
        
        print(f"SUCCESS: Products lookup watermark updated")
        print(f"SUMMARY: Products lookup records processed: {products_lookup_count}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Pipeline failed - {str(e)}")
        dbutils.notebook.run(
            "/Workspace/Users/rami.moughrabi@paragonshift.com/etl_error_notebook",
            300,
            {
                "error_source": "process_products_lookup_file",
                "error_message": str(e),
                "pipeline_name": "Silver_Data_Processing",
                "layer": "SILVER",
                "additional_details": "Failed while processing products_lookup data"
            }
            )
        return False

# Execute products lookup processing
products_lookup_success = process_products_lookup_file()

if products_lookup_success:
    print("COMPLETED: Products lookup file processing completed successfully")
else:
    print("FAILED: Products lookup file processing failed")

# COMMAND ----------

# Country Index Lookup Processing
# Author: Data Engineering Team
# Purpose: Process country_index.csv from bronze to silver layer

def process_country_index_lookup():
    """
    Process country_index.csv lookup file from bronze to silver layer
    """
    
    # Configuration
    storage_account = "psmltest9911652735"
    container = "icecreamblob"
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    
    try:
        print("INFO: Starting country index lookup processing")
        
        # Read country_index CSV file
        country_index_csv_path = f"{base_path}bronze/Files/country_index.csv"
        print(f"INFO: Reading country_index CSV from: {country_index_csv_path}")
        
        country_index_raw_df = spark.read.option("header", True).csv(country_index_csv_path)
        
        # Clean and transform country index data
        country_index_silver = country_index_raw_df.select(
            trim(col("Country")).alias("country"),
            col("Year").cast("INTEGER").alias("year"),
            col("average_price").cast("DOUBLE").alias("average_price"),
            col("country_index").cast("DOUBLE").alias("country_index"),
            current_date().alias("processing_date"),
            current_timestamp().alias("processing_timestamp"),
            lit("country_index_processing").alias("processed_by")
        ).filter(
            col("country").isNotNull() &
            col("year").isNotNull() &
            col("average_price").isNotNull() &
            col("country_index").isNotNull() &
            (col("average_price") > 0)
        )
        
        country_index_count = country_index_silver.count()
        print(f"INFO: Processed {country_index_count} country index records")
        
        # Save country index to silver delta lake
        country_index_silver_path = f"{base_path}silver/country_index/"
        country_index_silver.write \
            .mode("overwrite") \
            .format("delta") \
            .save(country_index_silver_path)
        
        print(f"SUCCESS: Country index data saved to: {country_index_silver_path}")
        
        # Log country index processing to silver watermark table
        current_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        country_index_watermark_data = spark.createDataFrame([
            (
                "country_index",
                "bronze layer (ADLS)",
                current_processing_time,
                current_processing_time,
                current_processing_time,
                "ACTIVE",
                "country_index_processing",
                "Lookup file processing for country index and pricing data",
                "country_index_processing_notebook"
            )
        ], [
            "table_name", "source_system", "watermark_value",
            "last_updated", "created_date", "status",
            "pipeline_name", "description", "updated_by"
        ])
        
        country_index_watermark_data.createOrReplaceTempView("country_index_watermark_new")
        
        spark.sql("""
        MERGE INTO icecreamdb.metadata_config.silver_watermark AS target
        USING country_index_watermark_new AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN 
            UPDATE SET
                watermark_value = source.watermark_value,
                last_updated = source.last_updated,
                status = source.status,
                pipeline_name = source.pipeline_name,
                description = source.description,
                updated_by = source.updated_by
        WHEN NOT MATCHED THEN
            INSERT (
                table_name, source_system, watermark_value,
                last_updated, created_date, status,
                pipeline_name, description, updated_by
            )
            VALUES (
                source.table_name, source.source_system, source.watermark_value,
                source.last_updated, source.created_date, source.status,
                source.pipeline_name, source.description, source.updated_by
            )
        """)
        
        print(f"SUCCESS: Country index watermark updated")
        print(f"SUMMARY: Country index records processed: {country_index_count}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Pipeline failed - {str(e)}")
        dbutils.notebook.run(
            "/Workspace/Users/rami.moughrabi@paragonshift.com/etl_error_notebook",
            300,
            {
                "error_source": "process_country_index_lookup",
                "error_message": str(e),
                "pipeline_name": "Silver_Data_Processing",
                "layer": "SILVER",
                "additional_details": "Failed while processing country_index_lookup data"
            }
            )
        return False

# Execute country index processing
country_index_success = process_country_index_lookup()

if country_index_success:
    print("COMPLETED: Country index lookup processing completed successfully")
else:
    print("FAILED: Country index lookup processing failed")

# COMMAND ----------

# Rental Prices Repository Processing (Monthly Rental Data by Country)
# Author: Data Engineering Team
# Purpose: Read rental_prices.csv from bronze, transform to normalized monthly format, and write to silver layer.

def process_rental_prices_repository():
    """
    Process rental_prices.csv into normalized monthly rental prices by country,
    storing results in silver/rental_prices_monthly.
    """
    
    # Configuration
    storage_account = "psmltest9911652735"
    container = "icecreamblob"
    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    
    try:
        print("INFO: Starting rental prices repository processing")
        
        # 1. Read the CSV file from Bronze
        rental_prices_csv_path = f"{base_path}bronze/Files/rental_prices.csv"
        print(f"INFO: Reading rental_prices CSV from: {rental_prices_csv_path}")
        
        rental_raw_df = (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(rental_prices_csv_path)
        )
        
        print(f"INFO: Raw dataframe columns: {rental_raw_df.columns}")
        print(f"INFO: Raw dataframe count: {rental_raw_df.count()}")
        
        # 2. Define month mapping for transforming column names to month numbers
        month_mapping = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
            "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
            "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
        }
        
        # 3. Prepare columns to unpivot (all month columns)
        month_columns = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        # 4. Alternative approach: Use union to unpivot the data
        unpivoted_dfs = []
        
        for month_name, month_num in month_mapping.items():
            month_df = rental_raw_df.select(
                col("Country").alias("country"),
                lit(month_num).alias("month"),
                lit(2023).alias("year"),
                col(month_name).cast("double").alias("rental_price")
            )
            unpivoted_dfs.append(month_df)
        
        # 5. Union all month dataframes
        normalized_df = unpivoted_dfs[0]
        for df in unpivoted_dfs[1:]:
            normalized_df = normalized_df.union(df)
        
        # 6. Filter out null values and clean data
        final_df = normalized_df.filter(
            col("rental_price").isNotNull() & 
            col("country").isNotNull()
        ).select(
            trim(col("country")).alias("country"),
            col("month"),
            col("year"),
            col("rental_price")
        )
        
        # 7. Add standard columns for Silver layer
        rental_silver = final_df.select(
            col("country"),
            col("month"),
            col("year"),
            col("rental_price"),
            current_date().alias("processing_date"),
            current_timestamp().alias("processing_timestamp"),
            lit("rental_prices_processing").alias("processed_by")
        )
        
        # Order by country and month for better organization
        rental_silver = rental_silver.orderBy("country", "month")
        
        record_count = rental_silver.count()
        print(f"INFO: Transformed {record_count} monthly rental price records.")
        
        # 8. Save to Silver as Delta
        rental_silver_path = f"{base_path}silver/rental_prices_monthly/"
        (rental_silver
            .write
            .mode("overwrite")
            .format("delta")
            .save(rental_silver_path)
        )
        
        print(f"SUCCESS: Rental prices data saved to: {rental_silver_path}")
        
        # 9. Create or replace the silver table in Unity Catalog
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS icecreamdb.silver.rental_prices_monthly
            USING DELTA
            LOCATION '{rental_silver_path}'
        """)
        
        print("INFO: Verified/Created table: icecreamdb.silver.rental_prices_monthly")
        
        # 10. Display sample of transformed data
        print("INFO: Sample of transformed rental data:")
        rental_silver.filter(col("country") == "Albania").show()
        
        # 11. Update watermark in silver_watermark
        current_processing_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create watermark data using Row objects
        from pyspark.sql import Row
        watermark_row = Row(
            table_name="rental_prices_monthly",
            source_system="bronze layer (ADLS)",
            watermark_value=current_processing_time,
            last_updated=current_processing_time,
            created_date=current_processing_time,
            status="ACTIVE",
            pipeline_name="rental_prices_processing",
            description="Monthly rental prices by country (EUR)",
            updated_by="rental_prices_notebook"
        )
        
        watermark_df = spark.createDataFrame([watermark_row])
        watermark_df.createOrReplaceTempView("rental_watermark_new")
        
        spark.sql("""
        MERGE INTO icecreamdb.metadata_config.silver_watermark AS target
        USING rental_watermark_new AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN 
            UPDATE SET
                watermark_value = source.watermark_value,
                last_updated = source.last_updated,
                status = source.status,
                pipeline_name = source.pipeline_name,
                description = source.description,
                updated_by = source.updated_by
        WHEN NOT MATCHED THEN
            INSERT (
                table_name, source_system, watermark_value,
                last_updated, created_date, status,
                pipeline_name, description, updated_by
            )
            VALUES (
                source.table_name, source.source_system, source.watermark_value,
                source.last_updated, source.created_date, source.status,
                source.pipeline_name, source.description, source.updated_by
            )
        """)
        
        print(f"SUCCESS: Rental prices watermark updated.")
        print(f"SUMMARY: Processed {record_count} monthly rental price records.")
        
        # 12. Show statistics
        print("\nINFO: Rental prices statistics:")
        rental_silver.groupBy("year").agg(
            count("*").alias("total_records"),
            countDistinct("country").alias("unique_countries"),
            avg("rental_price").alias("avg_rental_price"),
            min("rental_price").alias("min_rental_price"),
            max("rental_price").alias("max_rental_price")
        ).show()
        
        return True
        
    except Exception as e:
        # If error, call your error notebook
        print(f"ERROR: Pipeline failed - {str(e)}")
        dbutils.notebook.run(
            "/Workspace/Users/rami.moughrabi@paragonshift.com/etl_error_notebook",
            300,
            {
                "error_source": "process_rental_prices_repository",
                "error_message": str(e),
                "pipeline_name": "Silver_Data_Processing",
                "layer": "SILVER",
                "additional_details": "Failed while processing rental_prices data"
            }
        )
        return False


# Execute rental prices processing
rental_prices_success = process_rental_prices_repository()

if rental_prices_success:
    print("COMPLETED: Rental prices repository processing completed successfully.")
else:
    print("FAILED: Rental prices repository processing failed.")

# COMMAND ----------


