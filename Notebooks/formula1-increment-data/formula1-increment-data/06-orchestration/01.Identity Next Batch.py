# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

control_table = f"{catalog_name},{control_schema}.batch_control"

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql import functions as F

control_table = control_table.replace(",", ".")

landing_batches = sorted([
    file.name.rstrip("/")
    for file in dbutils.fs.ls(landing_folder_path)
    if file .isDir()
])

if spark.catalog.tableExists(control_table):
    tracked_batches = [
        row.batch_id
        for row in (
            spark.table(control_table)
                .filter(F.col("status").isin("in_progress", "completed"))
                .select("batch_id")
                .distinct()
                .collect()
        )
    ]
else:
    tracked_batches = []

new_batches = sorted(list(set(landing_batches) - set(tracked_batches)))
next_batch = new_batches[0] if new_batches else None

print(f"Landing batches: {landing_batches}")
print(f"Tracked batches: {tracked_batches}")
print(f"Next batches to process: {new_batches}")

if next_batch is None:
    dbutils.jobs.taskValues.set(key="p_batch_id",value="")
    dbutils.jobs.taskValues.set(key="has_batch",value="false")

else:
    dbutils.jobs.taskValues.set(key="p_batch_id",value=next_batch)
    dbutils.jobs.taskValues.set(key="has_batch", value=True)