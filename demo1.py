# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark import SparkContext

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

karthick = list(range(1,1000))

# COMMAND ----------

print(karthick)

# COMMAND ----------

print(type(karthick))

# COMMAND ----------

karthickrdd = sc.parallelize(karthick)

# COMMAND ----------

print(type(karthickrdd))

# COMMAND ----------

print(karthickrdd)

# COMMAND ----------


