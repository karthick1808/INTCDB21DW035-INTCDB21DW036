# Databricks notebook source
import pyspark
import pyspark.sql
from pyspark.sql import Row

# COMMAND ----------

l = [('Ankit',25),('karthick',32),('selvam',55)]
print(l)
print(type(l))

# COMMAND ----------

rdd = sc.parallelize(l)
print(rdd)
print(type(rdd))

# COMMAND ----------

rdd.collect()

# COMMAND ----------

people = rdd.map(lambda x: Row(name=x[0],age=int(x[1])))

# COMMAND ----------

print(type(people))

# COMMAND ----------

people.collect()

# COMMAND ----------

schemaPeople = sqlContext.createDataFrame(people)

# COMMAND ----------

print(type(schemaPeople))

# COMMAND ----------

schemaPeople.collect()

# COMMAND ----------

schemaPeople.printSchema()

# COMMAND ----------


