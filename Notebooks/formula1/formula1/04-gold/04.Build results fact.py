# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read source tables

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .withColumn("session_type",F.lit("RACE"))
    .drop("race_name","race_date","ingestion_timestamp","source_file")
)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .withColumn("session_type", F.lit("SPRINT"))
    .drop(
        "race_name",
        "race_date",
        "ingestion_timestamp",
        "source_file"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - UNION results and sprints

# COMMAND ----------

results_sprint_df = results_df.unionByName(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Add derived columns

# COMMAND ----------

fact_session_results_df = (
    results_sprint_df
    .withColumn("is_win", F.col("final_position") == 1)
    .withColumn("is_podium", F.col("final_position").between(1,3))
    .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

display(fact_session_results_df.filter("season = 2025"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - write the transformed data to gold fact_session_results table

# COMMAND ----------

{
    fact_session_results_df
    .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(target_table)
}

# COMMAND ----------

display(spark.table(target_table))