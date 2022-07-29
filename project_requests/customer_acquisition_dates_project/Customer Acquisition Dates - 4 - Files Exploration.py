# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

current = date.today()
year = current.strftime("%Y")
month = current.strftime("%m")
day = current.strftime("%d")
current = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

# current delta table
df_delta_table = spark.table('customer_acquisition_dates.acquisition_dates')

# COMMAND ----------

display(df_delta_table.orderBy('customer_key', ascending=False))

# COMMAND ----------

# current historical file
df_file = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{current}/*.parquet')

# COMMAND ----------

print(df_file.count())
display(df_file.orderBy('customer_key', ascending=False))

# COMMAND ----------

df_delta_table = df_delta_table.orderBy('customer_key', ascending=True)

df_file = df_file.select('customer_key', 'original_acquisition_date', 'most_recent_acquisition_date', 'most_recent_lost_date')
df_file = df_file.orderBy('customer_key', ascending=True)

df_delta_table == df_file

# COMMAND ----------

test_1 = df_delta_table.withColumnRenamed('original_acquisition_date', 'original_acquisition_date_delta').withColumnRenamed('most_recent_acquisition_date', 'most_recent_acquisition_date_delta').withColumnRenamed('most_recent_lost_date', 'most_recent_lost_date_delta')
test_2 = df_file.withColumnRenamed('original_acquisition_date', 'original_acquisition_date_file').withColumnRenamed('most_recent_acquisition_date', 'most_recent_acquisition_date_file').withColumnRenamed('most_recent_lost_date', 'most_recent_lost_date_file')
test_3 = test_1.join(test_2, on='customer_key', how='outer')

# COMMAND ----------

print(test_1.count())
print(test_2.count())
print(test_3.count())
display(test_3.orderBy('customer_key', ascending=True))

# COMMAND ----------

display(test_3.where(test_3.original_acquisition_date_delta != test_3.original_acquisition_date_file))

# COMMAND ----------

display(test_3.where(test_3.most_recent_acquisition_date_delta != test_3.most_recent_acquisition_date_file))

# COMMAND ----------

display(test_3.where(test_3.most_recent_lost_date_delta != test_3.most_recent_lost_date_file))

# COMMAND ----------

# so they are exactly the same... stupid python... but good.

# COMMAND ----------


