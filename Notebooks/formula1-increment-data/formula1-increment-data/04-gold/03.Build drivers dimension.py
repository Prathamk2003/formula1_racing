# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helper

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read source tables

# COMMAND ----------

drivers_df = (
    spark.table(f"{catalog_name}.{silver_schema}.drivers")
    .filter(F.col("batch_id") == v_batch_id)
    )

# COMMAND ----------

ref_nationality_region_map = (
    spark.table("formula1.gold.ref_nationality_region")
    )

# COMMAND ----------

# DBTITLE 1,Cell 6
dim_drivers_df = (
    drivers_df
    .join(ref_nationality_region_map,
           drivers_df.nationality == ref_nationality_region_map.nationality,
           "left")
    .select(
        drivers_df.driver_id,
        drivers_df.driver_name,
        drivers_df.date_of_birth,
        drivers_df.nationality,
        ref_nationality_region_map.region.alias("nationality_region"),
    )
)

# COMMAND ----------

display(dim_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - write the transformed data to gold dim_drivers table

# COMMAND ----------

write_to_gold(
    input_df=dim_drivers_df,
    target_table=target_table,
    merge_conditions="t.season = s.season AND t.round = s.round",
    columns_to_update=[
       "driver_id",
       "driver_name",
       "date_of_birth",
       "nationality",
       "nationality_region",
    ]
)

# COMMAND ----------

display(spark.table(target_table))