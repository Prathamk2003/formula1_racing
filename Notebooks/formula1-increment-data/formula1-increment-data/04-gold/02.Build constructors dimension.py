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

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read source tables

# COMMAND ----------

constructors_df = (
    spark.table(f"{catalog_name}.{silver_schema}.constructors")
    .filter(F.col("batch_id") == v_batch_id)
    )

# COMMAND ----------

# DBTITLE 1,Cell 8
ref_nationality_region_map = (
    spark.table("formula1.gold.ref_nationality_region")
    )

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

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_conditions="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "constructors_id",
        "constructors_name",
        "nationality",
        "constructor_region"
    ]
)

# COMMAND ----------

display(spark.table(target_table))