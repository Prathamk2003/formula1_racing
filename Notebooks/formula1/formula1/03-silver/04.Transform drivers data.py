# Databricks notebook source
# MAGIC %run ../00-common/01.env-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read bronze drivers table

# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - keep only columns required for analytics (Drop url column)

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

display(drivers_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - standardise column name

# COMMAND ----------

drivers_columned_rename = (
    drivers_dropped_df
    .withColumnsRenamed({"driverID":"driver_id",
                        "dateOfBirth":"date_of_birth"})
)

# COMMAND ----------

display(drivers_columned_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Concatenate name.givenName and name.familyName to create a new column called driver_name

# COMMAND ----------

drivers_concatenated_df = (
    drivers_columned_rename
    .withColumn("driver_name", 
                F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
    .drop("name")
)

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Remove duplicate records

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Transform values of nationality to Title case

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
    .withColumn('nationality', F.initcap(F.col('nationality')))
)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - write the transformed data to silver drivers table

# COMMAND ----------

{
    drivers_final_df
    .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(silver_table)
}

# COMMAND ----------

display(spark.table(silver_table))