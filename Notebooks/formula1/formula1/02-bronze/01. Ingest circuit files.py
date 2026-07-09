# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

landing_folder_path

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read the CSV file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitID',StringType()),
    StructField('url',StringType()),
    StructField('circuitName',StringType()),
    StructField('lat',DoubleType()),
    StructField('long',DoubleType()),
    StructField('locality',StringType()),
    StructField('country',StringType())
])

# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header','true')
    #.option('inferSchema','true')
    .option('mode','PERMISSIVE')
    .schema(circuits_schema)
    .load(source_file))

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add metadata columns

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Write to bronze table

# COMMAND ----------

{
    circuits_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
}

# COMMAND ----------

display(spark.table(table_name))