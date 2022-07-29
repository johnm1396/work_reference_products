# Databricks notebook source
teller_union = spark.read.option("header", True).option("inferSchema", True).format('delta').parquet(f"/mnt/*hidden*/*.parquet")
display(teller_union)

# COMMAND ----------

accts = spark.read.option("header", True).option("inferSchema", True).format('delta').parquet(f"/mnt/*hidden*/*.parquet")
display(accts)

# COMMAND ----------

dynamic_branch_assignments = spark.read.option("header", True).option("inferSchema", True).format('delta').parquet(f"/mnt/*hidden*/*.parquet")
display(dynamic_branch_assignments)

# COMMAND ----------

#drop database
spark.sql("drop database dynamic_branch_assignments cascade")

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC create database dynamic_branch_assignments
# MAGIC location '/mnt/*hidden*'

# COMMAND ----------

#write DataFrames to delta tables
teller_union.write.mode('overwrite').format('delta').saveAsTable('dynamic_branch_assignments.teller_union')
accts.write.mode('overwrite').format('delta').saveAsTable('dynamic_branch_assignments.accts')
dynamic_branch_assignments.write.mode('overwrite').format('delta').saveAsTable('dynamic_branch_assignments.dynamic_branch_assignments')

# COMMAND ----------


