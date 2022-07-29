# Databricks notebook source
# What are Dynamic Branch Assignments?
#      Dynamic Branch Assignments are our first attempt at understanding which branch, on any given day, a customer should
#      be assigned to. Historically, we have based our branch assignments on where an account or customer was added to the
#      system and therefore it is only a representation of where that customer FIRST interacted with *hidden*, not where they
#      currently interact with *hidden* or where they have interacted with *hidden* over time. The intention is to use this
#      assignment instead of the rm or acct level branch coding for analytical and marketing purposes. I don't think we are
#      at a place where this will be used in finance just yet - it would blow up their cost centers.
# 
# For each customer key on any given day, the dynamic branch assignment is determined using the following:
#
# Step 1: identify the 'primary_branch' for the customer
#      Step 1a: for accounts that the customer is primary on, determine the branch with the most transactions in the prior year
#
# Step 2: identify the 'direct_branch' for the customer
#      Step 2a: for accounts that the customer is direct on, determine the branch with the most transactions in the prior year
#
# Step 3: identify the 'acct_branch' for the customer
#      Step 3a: for accounts that the customer is direct on, determine the branch of the most recently opened account
#
# Step 4: identify the 'opening_branch' for the customer
#      Step 4a: this is the RM branch for the customer key
#
# Step 5: calculate dynamic_branch_assignment using the 4 different branches above as follows:
#         if (primary_branch != null) then (primary_branch)
#         else if (direct_branch != null) then (direct_branch)
#         else if (acct_branch != null) then (acct_branch)
#         else (opening_branch)
#
# Step 6: calculate is_active flag on calc_day for customer key based on logic defined in *hidden* project
#         if (customer_key has at least one relationship on calc_day that is OPEN or DORMANT) 
#         then (Y)
#         else (N)
#
# Step 7: calculate is_direct flag on calc_day for customer key based on logic defined in *hidden* project
#         if (customer_key has at least one direct relationship to a lending or deposit product on calc_day that is OPEN or DORMANT) 
#         then (Y)
#         else (N)
#
# Step 8: add utc_timestamp
#         utc_timestamp == calc_day

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date, timedelta

# COMMAND ----------

# end product
dynamic_branch_assignments = spark.table("dynamic_branch_assignments.dynamic_branch_assignments")
display(dynamic_branch_assignments)

# COMMAND ----------

# counts by date - what's with 2016?
display(dynamic_branch_assignments.groupBy('utc_timestamp').count().orderBy('utc_timestamp', ascending=False))

# COMMAND ----------

# Purpose of Review
#     1. General Understanding of the Data Product
#     2. Assistance conceptualising any edge cases that need tested/examined
#     3. Failure points - what needs rewritten to make code production ready? Example, *hidden* suggested not doing *hidden* 
#                         in case we have to rerun the notebook one day as it could cause duplication.
