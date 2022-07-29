# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

# COMMAND ----------

sv_curated = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2021/12/08/*.parquet')

# COMMAND ----------

sv_curated = sv_curated.select('account_key', 'conformed_status', 'application', 'account_type', 'date_opened')

# COMMAND ----------

sv_raw = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2021/12/08/*.parquet')

# COMMAND ----------

sv_raw = sv_raw.select('account_key', 'opening_amount', 'opening_deposit')

# COMMAND ----------

sv_acct_data = sv_curated.join(sv_raw, 'account_key', 'left')

# COMMAND ----------

sv_acct_data = sv_acct_data.where(sv_acct_data.date_opened >= '2018-01-01')

# COMMAND ----------

trans = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2021/*/*/*.parquet')
trans.drop("last_changed_date/time")

# COMMAND ----------

sv_trans = trans.join(sv_acct_data, 'account_key', 'left')

# COMMAND ----------

sv_trans = sv_trans.where(~sv_trans.conformed_status.isNull())

# COMMAND ----------

sv_trans = sv_trans.where(sv_trans.transaction_amount > 0)

# COMMAND ----------

sv_trans = sv_trans.sort(col("transaction_date").asc())

# COMMAND ----------

sv_trans.dropDuplicates(['account_key'])

# COMMAND ----------

sv_trans = sv_trans.where((sv_trans.transaction_amount == sv_trans.opening_amount)|(sv_trans.transaction_amount == sv_trans.opening_deposit))

# COMMAND ----------

display(sv_trans)

# COMMAND ----------


