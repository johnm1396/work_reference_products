# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
from pyspark.sql.types import DateType
from pyspark.sql import functions as F

# COMMAND ----------

*hidden*_transactions = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")

# COMMAND ----------

func = udf(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'), DateType())

*hidden*_transactions = *hidden*_transactions.withColumn('date_col_converted', func(col('TransactionDate&Time')))

# COMMAND ----------

display(*hidden*_transactions)

# COMMAND ----------

counts = *hidden*_transactions.where(*hidden*_transactions.IndividualID.isNotNull())
display(counts.groupBy('TransactionType').count().orderBy('count', ascending=False))

# COMMAND ----------

users_with_pfm = *hidden*_transactions.where(*hidden*_transactions.TransactionType=='PFM Login')
users_with_pfm = users_with_pfm.select('IndividualID').dropDuplicates()

# COMMAND ----------

display(users_with_pfm)

# COMMAND ----------

users_with_no_pfm_transactions = *hidden*_transactions.join(users_with_pfm, ['IndividualID'], 'leftanti')

# COMMAND ----------

users_with_no_pfm_transactions = users_with_no_pfm_transactions.where(users_with_no_pfm_transactions.IndividualID>1)
users_with_no_pfm_transactions = users_with_no_pfm_transactions.select('IndividualID', 'UserID', 'CustomerFirstName', 'CustomerLastName').dropDuplicates()


# COMMAND ----------

display(users_with_no_pfm_transactions)

# COMMAND ----------

users_with_no_pfm_transactions.coalesce(1).write.option("header", True).csv('/mnt/*hidden*/')

# COMMAND ----------


