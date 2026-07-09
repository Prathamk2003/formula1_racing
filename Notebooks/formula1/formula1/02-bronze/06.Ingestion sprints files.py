# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the JSON file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType

sprints_schema = StructType([
    StructField('date',DateType()),
    StructField('raceName',StringType()),
    StructField('round',IntegerType()),
    StructField('season',IntegerType()),
    StructField('url',StringType()),
    StructField('constructorId',StringType()),
    StructField('driverId',StringType()),
    StructField('grid',FloatType()),
    StructField('laps',FloatType()),
    StructField('number',IntegerType()),
    StructField('points',FloatType()),
    StructField('position',IntegerType()),
    StructField('positionText',StringType()),
    StructField('status',StringType()),
])

# COMMAND ----------

sprints_df = (
    spark.read
        .format('json')
        .schema(sprints_schema)
        .option('mode','FAILFAST')
        .option('multiLine',True)
        .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add the metadata columns

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - write to bronze delta table

# COMMAND ----------

{
    sprints_final_df
    .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(table_name)
}

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*)
# MAGIC     from formula1.bronze.sprints
# MAGIC group by season
# MAGIC order by season;