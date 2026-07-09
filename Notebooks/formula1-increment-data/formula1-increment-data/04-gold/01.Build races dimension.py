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

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read source tables

# COMMAND ----------

circuits_df = (
    spark.table(f"{catalog_name}.{silver_schema}.circuits")
    .filter(F.col("batch_id") == v_batch_id)
    )

# COMMAND ----------

# DBTITLE 1,Cell 8
races_df = (
    spark.table(f"{catalog_name}.{silver_schema}.races")
    .filter(F.col("source_file").contains(v_batch_id))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Join races with circuits using circuit_id

# COMMAND ----------

dim_races_df = (
    races_df
    .join(circuits_df,
           races_df.circuit_id == circuits_df.circuit_id,
           "inner")
    .select(
        races_df.season,
        races_df.round,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuit_name,
        circuits_df.locality,
        circuits_df.country
    )
)

# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - write the transformed data to gold dim_races table

# COMMAND ----------

# DBTITLE 1,Cell 13
write_to_gold(
    input_df=dim_races_df,
    target_table=target_table,
    merge_conditions="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)

# COMMAND ----------

display(spark.table(target_table))