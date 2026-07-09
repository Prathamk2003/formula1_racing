# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read bronze sprints table

# COMMAND ----------

sprints_df = (
    spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - keep only columns required for analytics (Drop url column)

# COMMAND ----------

sprints_selected_df = (
    sprints_df.select("season", 
                      "round", 
                      "constructorId",
                      "driverId",
                      "date", 
                      "raceName",
                      "grid",
                      "laps", 
                      "number", 
                      "points", 
                      "position", 
                      "positionText", 
                      "status",  
                      "ingestion_timestamp",
                      "source_file")
)

# COMMAND ----------

display(sprints_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3&4 - standardise column name

# COMMAND ----------

sprints_columned_rename = (
    sprints_selected_df
    .withColumnsRenamed({"constructorId":"constructors_id",
                        "driverID":"driver_id",
                        "raceName":"race_name",
                        "date":"race_date",
                        "grid":"grid_position",
                        "laps":"completed_laps",
                        "number":"car_number",
                        "position":"final_position",
                        "positionText":"final_position_text"})
)

# COMMAND ----------

display(sprints_columned_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Filter out rows where constructor_id or driver_id is null (business key validation)

# COMMAND ----------

sprints_valid_df = sprints_columned_rename.filter(
    F.col("season").isNotNull() &
    F.col("round").isNotNull() &
    F.col("constructors_id").isNotNull() &
    F.col("driver_id").isNotNull() 
)

# COMMAND ----------

display(sprints_valid_df)

# COMMAND ----------

display(sprints_selected_df.count() - sprints_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Remove duplicate records

# COMMAND ----------

sprints_distinct_df = sprints_valid_df.dropDuplicates(["season","round","constructors_id","driver_id"])

# COMMAND ----------

display(sprints_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Transform values of race_name to Title case

# COMMAND ----------

sprints_final_df = (
    sprints_distinct_df
    .withColumn('race_name', F.initcap(F.col('race_name')))
)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - write the transformed data to silver sprints table

# COMMAND ----------

write_to_silver(
    input_df=sprints_final_df,
    target_table=silver_table,
    merge_conditions="t.season = s.season AND t.round = s.round AND t.constructors_id = s.constructors_id AND t.driver_id =s.driver_id",
    columns_to_update=[
       "race_name",
       "race_date",
       "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
       "ingestion_timestamp",
       "source_file",
       "batch_id"

    ],
)

# COMMAND ----------

display(spark.table(silver_table))