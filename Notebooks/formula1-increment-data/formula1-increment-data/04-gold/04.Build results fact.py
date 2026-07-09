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

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read source tables

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .withColumn("session_type",F.lit("RACE"))
    .drop("race_name","race_date","ingestion_timestamp","source_file","created_timestamp","updated_timestamp","batch_id")
)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .withColumn("session_type", F.lit("SPRINT"))
    .drop(
        "race_name",
        "race_date",
        "ingestion_timestamp",
        "source_file",
        "created_timestamp",
        "updated_timestamp",
        "batch_id"
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

write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_conditions="""
    t.season = s.season
    AND t.round = s.round
    AND t.driver_id = s.driver_id
    AND t.session_type = s.session_type
    """,
    columns_to_update=[
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "is_win",
        "is_podium",
        "has_points"
    ]
)

# COMMAND ----------

display(spark.table(target_table))