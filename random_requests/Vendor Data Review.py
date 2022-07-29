# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

users = spark.sql('''
  select * from *hidden*
''')

# COMMAND ----------

display(users)

# COMMAND ----------

# users.coalesce(1).write.mode("overwrite").option("header", True).parquet('/mnt/*hidden*/')

# COMMAND ----------

users.groupBy('gender').count().orderBy('count', ascending=False).show()

# COMMAND ----------

lookup = users.where(users.id=='*hidden*')

display(lookup)

# COMMAND ----------

accounts = spark.sql('''
  select * from mx_database.accounts
''')

# COMMAND ----------

display(accounts.where((accounts.institution_guid == '*hidden*') & (~accounts.id.contains('-'))))

# COMMAND ----------

accounts.groupBy('apy').count().orderBy('count', ascending=False).show()

# COMMAND ----------

transactions = spark.sql('''
  select * from *hidden*
''')

# COMMAND ----------

display(transactions.where(transactions.description.contains('Acorns Later')))

# COMMAND ----------

transactions.groupBy('parent_guid').count().orderBy('count', ascending=False).show()

# COMMAND ----------

transactions.columns

# COMMAND ----------

analytics_events = spark.table('*hidden*')

print(analytics_events.count())
display(analytics_events)

# COMMAND ----------

analytics_pageviews = spark.table('*hidden*')

print(analytics_pageviews.count())
display(analytics_pageviews)

# COMMAND ----------

analytics_pageviews.columns

# COMMAND ----------

analytics_events.columns

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

analytics_screenviews = spark.table('*hidden*')

print(analytics_screenviews.count())
display(analytics_screenviews)

# COMMAND ----------



# COMMAND ----------


