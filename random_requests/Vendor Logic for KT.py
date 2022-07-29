# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
from pyspark.sql.types import DateType

# COMMAND ----------

*hidden* = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")
*hidden*_transactions = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")

*hidden*_transactions = *hidden*_transactions.where((*hidden*_transactions['TransactionType'] == 'Login')&(*hidden*_transactions['DeviceFormFactor'] != 'UNKNOWN')&(*hidden*_transactions['DeviceFormFactor'].isNotNull()))

func = udf(lambda x: datetime.strptime(x, '%m/%d/%Y %H:%M:%S'), DateType())
*hidden*_transactions = *hidden*_transactions.withColumn('TransactionDate', func(col('TransactionDate&Time')))
*hidden*_transactions = *hidden*_transactions.where(*hidden*_transactions['TransactionDate'] >= (date.today()-timedelta(91)))

*hidden*_transactions = *hidden*_transactions.withColumn('Login_Method', concat(*hidden*_transactions.DeviceFormFactor, lit(" - "), *hidden*_transactions.TransactionChannel))

num_logins = *hidden*_transactions.groupBy("UserID").pivot("Login_Method").count()

*hidden*_user = *hidden*_user.where(*hidden*_user.IndividualID.isNotNull())
*hidden*_user = *hidden*_user.sort('EnrollmentDate&Time', ascending=False)
*hidden*_user = *hidden*_user.dropDuplicates(['IndividualID'])
*hidden*_user = *hidden*_user.select('UserID', 'IndividualID')

customer_digital_logins = *hidden*_user.join(num_logins, 'UserID', 'left')
customer_digital_logins = customer_digital_logins.drop("UserID")

display(customer_digital_logins)

# COMMAND ----------

*hidden*_transactions = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")
#*hidden*_transactions = *hidden*_transactions.where((*hidden*_transactions['TransactionType'] == 'Login'))
display(*hidden*_transactions)

# COMMAND ----------

display(*hidden*_transactions.select('TransactionType').distinct())

# COMMAND ----------




# COMMAND ----------



# COMMAND ----------

*hidden*_transactions.columns

# COMMAND ----------

*hidden*_transactions = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")

*hidden*_transactions = *hidden*_transactions.where(*hidden*_transactions['TransactionType'] == 'Login')
*hidden*_transactions = *hidden*_transactions.where(*hidden*_transactions['TransactionResult'] == 'SUCCESS')

# COMMAND ----------

display(*hidden*_transactions)

# COMMAND ----------

display(*hidden*_transactions.groupBy(['TransactionChannel', 'DeviceFormFactor']).count())

# COMMAND ----------


