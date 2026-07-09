# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read bronze circuits table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf',0).table(bronze_table)

# COMMAND ----------

circuits_df = spark.table(bronze_table)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - keep only columns required for analytics (Drop url column)

# COMMAND ----------

# circuits_selected_df = circuits_df.select("circuitID"
#                    ,"circuitName"
#                    ,"lat"
#                    ,"long"
#                    ,"locality"
#                    ,"country"
#                    ,"ingestion_timestamp"
#                    ,"source_file"
#                    )

# COMMAND ----------

from pyspark.sql import functions as F

circuits_selected_df = circuits_df.select(
    F.col("circuitID"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
    )

    # From this method we can directly alias the columns so this is preffered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3&4 - standardise column name

# COMMAND ----------

# circuits_columned_rename = (
#     circuits_selected_df
#     .withColumnRenamed("circuitID","circuit_id")
#     .withColumnRenamed("circuitName","circuit_name")
#     .withColumnRenamed("lat","latitude")
#     .withColumnRenamed("long","longitude")
# )

# COMMAND ----------

circuits_columned_rename = (
    circuits_selected_df
    .withColumnsRenamed({"circuitID":"circuit_id"
                        ,"circuitName":"circuit_name"
                        ,"lat":"latitude"
                        ,"long":"longitude"})
)

# COMMAND ----------

display(circuits_columned_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Filter out rows where circuit_id is null (business key validation)

# COMMAND ----------

circuits_valid_df = circuits_columned_rename.filter(
    F.col("circuit_id").isNotNull()
)

# instead of using  F.col("circuit_id").isNotNull() we can also use  "circuit_id IS NOT NULL".

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Remove duplicate records

# COMMAND ----------

# circuits_distinct_df = circuits_valid_df.distinct()
# distinct() is a transformation that returns a new DataFrame with duplicate rows removed.

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])
# dropDuplicates() is a transformation that returns a new DataFrame with duplicate rows removed.Removes dulpicate columns according to the specified columns.

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Transform values of column circuit_name  and locality to Title case

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn('circuit_name', F.initcap(F.col('circuit_name')))
    .withColumn('locality', F.initcap(F.col('locality')))
)
# this is done so that the values of circuit_name and locality are in upper case(only the 1st letter of the word)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - write the transformed data to silver circuits table

# COMMAND ----------

{
    circuits_final_df
    .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(silver_table)
}

# COMMAND ----------

display(spark.table(silver_table))