-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up project environment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Access cloud storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databricscourseextdl1pk.dfs.core.windows.net'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create external storage

-- COMMAND ----------

create external location if not exists databrics_course_ext_dl1ps_formula1_incr
url 'abfss://formula1-incr@databricscourseextdl1pk.dfs.core.windows.net'
with(storage credential `databrics-course-sc`)
comment 'External location for formula1-incr container'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create catalogs

-- COMMAND ----------

show catalogs

-- COMMAND ----------

create catalog if not exists formula1_incr
managed location 'abfss://formula1-incr@databricscourseextdl1pk.dfs.core.windows.net'
comment 'Main catalog for the formula1-incr database'

-- COMMAND ----------

show catalogs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## creates Schemas landing,bronze,silver,gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@databricscourseextdl1pk.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@databricscourseextdl1pk.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@databricscourseextdl1pk.dfs.core.windows.net/gold';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create volume files

-- COMMAND ----------

create external volume formula1_incr.landing.files
location 'abfss://formula1-incr@databricscourseextdl1pk.dfs.core.windows.net/landing'