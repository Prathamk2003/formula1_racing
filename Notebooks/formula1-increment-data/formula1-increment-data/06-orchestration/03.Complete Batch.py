# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

control_table = f"{catalog_name},{control_schema}.batch_control"

# COMMAND ----------

# DBTITLE 1,Cell 4
from pyspark.sql import functions as F
from delta.tables import DeltaTable

control_table = control_table.replace(",", ".")

if v_batch_id:
    delta_table = DeltaTable.forName(spark, control_table)

    source_df = (
        spark.createDataFrame([(v_batch_id,)], ["batch_id"])
        .withColumn("status", F.lit("STARTED"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    (
        delta_table.alias("t")
        .merge(
            source_df.alias("s"),
            "t.batch_id = s.batch_id AND t.status = 'in_progress'",
        )
        .whenMatchedUpdate(
            set={
                "status": "'STARTED'",
                "updated_timestamp": "s.ingestion_timestamp",
            }
        )
        .execute()
    )

    print("marked batch {v_batch_id} as started")

else:
    raise Exception("batch id is required")