# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp
from datetime import datetime
import json

# Create notebook parameters/widgets
dbutils.widgets.text("error_source", "", "Error source (function/task name)")
dbutils.widgets.text("error_message", "", "Error message")
dbutils.widgets.text("pipeline_name", "", "Pipeline or process name")
dbutils.widgets.text("layer", "", "Data layer (BRONZE, SILVER, GOLD)")
dbutils.widgets.text("additional_details", "", "Additional error details or context")

# Get parameter values
error_source = dbutils.widgets.get("error_source")
error_message = dbutils.widgets.get("error_message")
pipeline_name = dbutils.widgets.get("pipeline_name")
layer = dbutils.widgets.get("layer")
additional_details = dbutils.widgets.get("additional_details")

# COMMAND ----------

def log_etl_error():
    """
    Insert a new error record into icecreamdb.metadata_config.etl_error_logs using SQL.
    """
    try:
        # ------------------------------------------------------------------------------
        # 2a. Validate required parameters
        # ------------------------------------------------------------------------------
        if not error_source.strip():
            raise ValueError("error_source is required")
        if not error_message.strip():
            raise ValueError("error_message is required")

        print(f"INFO: Logging error from source: {error_source}")

        # ------------------------------------------------------------------------------
        # 2b. Build the INSERT query
        # ------------------------------------------------------------------------------
        # You can adjust columns if your table structure differs.
        # This assumes the table has columns:
        #   (error_id [IDENTITY], error_timestamp, error_source, error_message,
        #    pipeline_name, layer, additional_details)
        insert_query = f"""
        INSERT INTO icecreamdb.metadata_config.etl_error_logs
        (
            error_timestamp,
            error_source,
            error_message,
            pipeline_name,
            layer,
            additional_details
        )
        VALUES
        (
            current_timestamp(),
            '{error_source.replace("'", "''")}',
            '{error_message.replace("'", "''")}',
            '{pipeline_name.replace("'", "''")}' ,
            '{layer.strip().upper().replace("'", "''")}' ,
            '{additional_details.replace("'", "''")}'
        )
        """
        
        print("DEBUG: Executing INSERT query:")
        print(insert_query)

        # ------------------------------------------------------------------------------
        # 2c. Execute the INSERT
        # ------------------------------------------------------------------------------
        spark.sql(insert_query)

        print(f"SUCCESS: Error logged successfully for {error_source}")

        # ------------------------------------------------------------------------------
        # 3. Build success response JSON & exit
        # ------------------------------------------------------------------------------
        response = {
            "status": "SUCCESS",
            "message": f"Error logged for {error_source}",
            "error_source": error_source,
            "timestamp": datetime.now().isoformat()
        }
        dbutils.notebook.exit(json.dumps(response))

    except Exception as e:
        # ------------------------------------------------------------------------------
        # 4. If something fails, build a failure response
        # ------------------------------------------------------------------------------
        print(f"CRITICAL: Failed to log error - {str(e)}")
        failure_response = {
            "status": "FAILED",
            "message": f"Failed to log error: {str(e)}",
            "original_error_source": error_source,
            "original_error_message": error_message
        }
        #dbutils.notebook.exit(json.dumps(failure_response))

# ------------------------------------------------------------------------------
# 5. Execute the logging function
# ------------------------------------------------------------------------------
log_etl_error()

# COMMAND ----------


