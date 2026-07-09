# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

control_table = f"{catalog_name},{control_schema}.batch_control"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Row

control_table = control_table.replace(",", ".")

if v_batch_id:
    in_progress_df = (
        spark.createDataFrame(
            [Row(batch_id=v_batch_id, status="IN PROGRESS")]
        )
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )
    (
    in_progress_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(control_table)
    )

    print(f"Marked batch {v_batch_id} as IN PROGRESS")

else:
    raise Exception("No batch ID provided")
