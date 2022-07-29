# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

# COMMAND ----------

trans = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/2022/05/01/*.parquet")
trans1 = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/2022/05/02/*.parquet")
trans2 = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/2022/05/03/*.parquet")

trans_rdc = trans.union(trans1)
trans_rdc = trans_rdc.union(trans2)

trans_rdc = trans_rdc.where(trans_rdc.TransactionType == 'Remote Deposit Capture')
trans_rdc = trans_rdc.where(trans_rdc.TransactionResult == 'FAILURE')

# COMMAND ----------

display(trans_rdc.where(trans_rdc.IndividualID == '*hidden*'))

# COMMAND ----------

display(trans_rdc.orderBy('TransactionDate&Time', ascending=False))

# COMMAND ----------

display(trans_rdc.where(trans_rdc['TransactionDate&Time'] >= date(2022,4,21)))

# COMMAND ----------

display(trans_rdc.where((trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*') | (trans_rdc.IndividualID == '*hidden*')))

# COMMAND ----------

# COMMAND ----------

trans_rdc = trans.where(trans.TransactionType == 'Remote Deposit Capture')
trans_rdc = trans.withColumn('TransactionDate&Time', substring(trans['TransactionDate&Time'], 0, 10))
display(trans.groupBy(['TransactionDate&Time', 'TransactionResult']).count().orderBy('TransactionDate&Time', ascending=True))

# COMMAND ----------

# COMMAND ----------

trans = trans.where(trans.TransactionType.contains('Login'))

# COMMAND ----------

display(trans.groupBy(['TransactionChannel', 'DeviceFormFactor']).count())

# COMMAND ----------

# COMMAND ----------

trans_browser = trans.where(trans.TransactionChannel == 'Browser')

# COMMAND ----------

display(trans.groupBy('DeviceFormFactor').count())

# COMMAND ----------

display(trans.groupBy('DeviceFormFactor').count())

# COMMAND ----------

# COMMAND ----------

failures = trans.where((trans.TransactionType=='Get RDC Configuration') & (trans.TransactionResult=='FAILURE'))
failures = failures.select('IndividualID', 'UserID', 'CustomerFirstName', 'CustomerLastName').dropDuplicates()

# COMMAND ----------

failure_trans = failures.join(trans, 'IndividualID', 'left')

# COMMAND ----------

failure_trans = failure_trans.where((failure_trans.TransactionType=='Remote Deposit Capture') & (failure_trans.TransactionResult=='SUCCESS'))
failure_trans = failure_trans.select('IndividualID').dropDuplicates()

# COMMAND ----------

display(failure_trans)

# COMMAND ----------

display(failures)

# COMMAND ----------

failures = failures.join(failure_trans, 'IndividualID', 'leftanti')

# COMMAND ----------

display(failures)

# COMMAND ----------

users = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/2022/02/08/*.parquet")
users = users.where(~users.IndividualID.isNull())

#users = users.orderBy('EnrollmentDate&Time', ascending=False)
#users = users.dropDuplicates(['IndividualID'])

# COMMAND ----------

display(users)

# COMMAND ----------

new_list = failures.join(users, 'UserID', 'left')

# COMMAND ----------

display(new_list)

# COMMAND ----------


