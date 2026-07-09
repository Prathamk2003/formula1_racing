# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read bronze races table

# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - keep only columns required for analytics (Drop url column)

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    )

    # From this method we can directly alias the columns so this is preffered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3&4 - standardise column name

# COMMAND ----------

races_columned_rename = (
    races_selected_df
    .withColumnsRenamed({"circuitID":"circuit_id"
                        ,"raceName":"race_name"
                        ,"date":"race_date"})
)

# COMMAND ----------

display(races_columned_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Remove duplicate records

# COMMAND ----------

races_distinct_df = races_columned_rename.dropDuplicates(["season","round"])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Transform values of column race_name  and locality to Title case

# COMMAND ----------

races_final_df = (
    races_distinct_df
    .withColumn('race_name', F.initcap(F.col('race_name')))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - write the transformed data to silver races table

# COMMAND ----------

{
    races_final_df
    .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(silver_table)
}

# COMMAND ----------

display(spark.table(silver_table))