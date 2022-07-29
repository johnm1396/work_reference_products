# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

# COMMAND ----------

trans = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/all_transaction_daily/*/*/*/*.parquet")

trans = trans.where((trans.TransactionType != 'PFM Login') & (trans.TransactionType != 'Login') & (trans.TransactionType != 'Authenticate') & (trans.TransactionType != 'Logout'))

trans = trans.withColumnRenamed('TransactionDate&Time', 'TransactionDate')

trans = trans.withColumn('TransactionDate', expr("""case when cast(left(TransactionDate,10) as date) is null then to_date(left(TransactionDate,10),'M/dd/yyyy') else cast(left(TransactionDate,10) as date) end"""))

# COMMAND ----------

trans_grouped = trans.groupBy(['TransactionDate', 'TransactionType', 'TransactionChannel']).count().orderBy(['TransactionDate', 'TransactionType', 'TransactionChannel'], ascending=True)

# COMMAND ----------

display(trans_grouped)

# COMMAND ----------

print(trans_grouped.count())

# COMMAND ----------


