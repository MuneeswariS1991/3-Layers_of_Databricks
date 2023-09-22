# Databricks notebook source
display(dbutils.fs.ls("/"))

# COMMAND ----------

dbutils.fs.mkdirs("/mini_project/DataLake")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE bronze_layer LOCATION '/mini_project/DataLake/bronzelayer'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE silver_layer LOCATION '/mini_project/DataLake/silver_layer'

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE DATABASE gold_layer LOCATION '/mini_project/DataLake/gold_layer'

# COMMAND ----------

display(dbutils.fs.ls('/mini_project/DataLake'))

# COMMAND ----------


