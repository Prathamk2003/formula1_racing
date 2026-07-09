# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read bronze constructors table

# COMMAND ----------

constructors_df = (
    spark.table(bronze_table).filter(F.col("batch_id") == v_batch_id)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - keep only columns required for analytics (Drop url column)

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3&4 - standardise column name

# COMMAND ----------

constructors_columned_rename = (
    constructors_dropped_df
    .withColumnsRenamed({"constructorId":"constructors_id",
                        "name":"constructors_name"})
)

# COMMAND ----------

display(constructors_columned_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Remove duplicate records

# COMMAND ----------

constructors_distinct_df = constructors_columned_rename.dropDuplicates(["constructors_id"])

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Transform values of nationality  and locality to Title case

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
    .withColumn('nationality', F.initcap(F.col('nationality')))
)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - write the transformed data to silver constructors table

# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_conditions="t.circuit_id = s.circuit_id",
    columns_to_update=[
       "constructors_id",
       "constructor_name",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))