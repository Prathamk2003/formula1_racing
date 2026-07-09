# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read source tables

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_map = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# DBTITLE 1,Cell 6
dim_constructors_df = (
    constructors_df
    .join(ref_nationality_region_map,
           constructors_df.nationality == ref_nationality_region_map.nationality,
           "left")
    .select(
        constructors_df.constructors_id,
        constructors_df.constructors_name,
        constructors_df.nationality,
        ref_nationality_region_map.region.alias("constructor_region"),
    )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - write the transformed data to gold dim_constructors table

# COMMAND ----------

{
    dim_constructors_df
    .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(target_table)
}

# COMMAND ----------

display(spark.table(target_table))