# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType, DoubleType, StringType
import pyspark.pandas as ps

# date variables
todays_date = date.today()

# date constructs for filepaths
year = todays_date.strftime("%Y")
month = todays_date.strftime("%m")
day = todays_date.strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)


# COMMAND ----------

df = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/*/*/*/*.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.groupBy('customerid').count().orderBy('count', ascending=True))

# COMMAND ----------

df2 = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/*/*/*/*.parquet")

# COMMAND ----------

df2 = df2.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

# COMMAND ----------

df2 = df2.orderBy('utc_timestamp', ascending=False)
display(df2)

# COMMAND ----------

df2 = df2.orderBy('transferprocessedeventdate', ascending=False)

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df2.where(df2['transferprocessedeventdate']>date(2022,1,1)))

# COMMAND ----------

display(df2.groupBy('utc_timestamp').count())

# COMMAND ----------


