# Databricks notebook source
dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the JSON file using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType

results_schema = StructType([
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

results_df = (
    spark.read
        .format('json')
        .schema(results_schema)
        .option('mode','FAILFAST')
        .load(source_file)
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add the metadata columns

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - write to bronze delta table

# COMMAND ----------

# DBTITLE 1,Cell 12
write_to_bronze (
    input_df=results_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*)
# MAGIC     from formula1.bronze.results
# MAGIC group by season
# MAGIC order by season;