# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helper

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read bronze circuits table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf',0).table(bronze_table)

# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)
    )

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

circuits_selected_df = circuits_df.select(
    F.col("circuitID"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
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

# DBTITLE 1,Create final circuits DataFrame
circuits_final_df = (
    circuits_distinct_df
    .withColumn("circuit_name", F.initcap(F.col("circuit_name")))
    .withColumn("locality", F.initcap(F.col("locality")))
)
# this is done so that the values of circuit_name and locality are in title case

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - write the transformed data to silver circuits table

# COMMAND ----------

# DBTITLE 1,Cell 27
# if not spark.catalog.tableExists(silver_table):
#     {
#         circuits_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(silver_table)
#     }

# else:
#     from delta.tables import DeltaTable

#     deltaTable = DeltaTable.forName(spark, silver_table)
#     (
#         deltaTable.alias("t")
#         .merge(
#             circuits_final_df.alias("s"),
#             "t.circuit_id = s.circuit_id"
#         )
#         .whenMatchedUpdate(
#             condition="s.batch_id >= t.batch_id",
#             set={
#                 "circuit_name": "s.circuit_name",
#                 "latitude": "s.latitude",
#                 "longitude": "s.longitude",
#                 "locality": "s.locality",
#                 "country": "s.country",
#                 "ingestion_timestamp": "s.ingestion_timestamp",
#                 "source_file": "s.source_file",
#                 "batch_id": "s.batch_id",
#                 "updated_timestamp": "s.updated_timestamp"
#             }
#         )
#     )

# COMMAND ----------

# DBTITLE 1,Write silver circuits table
write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_conditions="t.circuit_id = s.circuit_id",
    columns_to_update=[
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id",
    ],
)

# COMMAND ----------

# DBTITLE 1,Cell 29
display(circuits_final_df)

# COMMAND ----------

display(spark.table(silver_table))