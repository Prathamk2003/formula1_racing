# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read the CSV file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

races_schema = StructType([
    StructField('season',IntegerType()),
    StructField('round',IntegerType()),
    StructField('url',StringType()),
    StructField('raceName',StringType()),
    StructField('date',DateType()),
    StructField('circuitId',StringType()),
])

# COMMAND ----------

races_df = (
    spark.read
    .format('csv')
    .option('header','true')
    #.option('inferSchema','true')
    .option('mode','PERMISSIVE')
    .schema(races_schema)
    .load(source_file))

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add metadata columns

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Write to bronze table

# COMMAND ----------

{
    races_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
}

# COMMAND ----------

display(spark.table(table_name))