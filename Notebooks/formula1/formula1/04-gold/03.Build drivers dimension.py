# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read source tables

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_map = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

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

{
    dim_drivers_df
    .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(target_table)
}

# COMMAND ----------

display(spark.table(target_table))