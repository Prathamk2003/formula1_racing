-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Steup batch events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1 - Create control schema

-- COMMAND ----------

create schema if not exists formula1.control
    managed location 'abfss://formula1@databricscourseextdl1pk.dfs.core.windows.net/control'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2 - Create batch_events table

-- COMMAND ----------

create table if not exists formula1.control.batch_events
(
    batch_id INT,
    event_timestamp timestamp
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 3 - Insert an event record

-- COMMAND ----------

insert into formula1.control.batch_events
values(1,current_timestamp());

-- COMMAND ----------

select * from formula1.control.batch_events;