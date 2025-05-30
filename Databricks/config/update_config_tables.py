# Databricks notebook source
# Create notebook parameters/widgets
dbutils.widgets.text("pipeline_name", "", "Pipeline name to update")
dbutils.widgets.text("watermark_value", "", "New watermark timestamp value")
dbutils.widgets.text("target_table", "bronze_watermark", "Target watermark table (bronze_watermark, silver_watermark, gold_watermark)")

# Get parameter values
pipeline_name = dbutils.widgets.get("pipeline_name")
watermark_value = dbutils.widgets.get("watermark_value")
target_table = dbutils.widgets.get("target_table")

print(f"Pipeline Name: {pipeline_name}")
print(f"New Watermark Value: {watermark_value}")
print(f"Target Table: {target_table}")

# COMMAND ----------

# Validate required parameters
if not pipeline_name.strip():
    raise ValueError("pipeline_name is required")
if not watermark_value.strip():
    raise ValueError("watermark_value is required")

# Validate target table
valid_tables = ["bronze_watermark", "silver_watermark", "gold_watermark"]
if target_table not in valid_tables:
    raise ValueError(f"Invalid target_table. Must be one of: {', '.join(valid_tables)}")

print("✅ All parameters validated successfully")

# COMMAND ----------

try:
    # Build the UPDATE SQL query
    update_query = f"""
    UPDATE icecreamdb.metadata_config.{target_table} 
    SET 
        watermark_value = '{watermark_value}',
        last_updated = current_timestamp(),
        updated_by = 'ADF'
    WHERE pipeline_name = '{pipeline_name}'
    """
    
    print("Executing UPDATE query:")
    print(update_query)
    
    # Execute the update
    spark.sql(update_query)
    
    print(f"✅ Successfully updated watermark value for pipeline: {pipeline_name}")
    
except Exception as e:
    print(f"❌ Error updating watermark: {str(e)}")
    raise e

# COMMAND ----------


