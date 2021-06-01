# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark import SparkContext

# COMMAND ----------

karthick = [1,2,3,4,5,6,7,8,9,10]

# COMMAND ----------

print(type(karthick))

# COMMAND ----------

print(karthick)

# COMMAND ----------

selvam = sc.parallelize(karthick)

# COMMAND ----------

print(selvam)

# COMMAND ----------

print(type(selvam))

# COMMAND ----------

selvam.collect()

# COMMAND ----------

selvam.take(20)

# COMMAND ----------

demo = ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]

# COMMAND ----------

print(demo)

# COMMAND ----------

print(type(demo))

# COMMAND ----------

demo1=sc.parallelize(demo)

# COMMAND ----------

print(type(demo1))

# COMMAND ----------

qwerty = sc.parallelize(['scala', 'java', 'hadoop', 'spark', 'akka', 'spark vs hadoop', 'pyspark', 'pyspark and spark'])

# COMMAND ----------

print(qwerty)

# COMMAND ----------

qwerty.collect()

# COMMAND ----------

qwerty.count()

# COMMAND ----------

demo123=sc.parallelize([1,2,3,4])

# COMMAND ----------

demo123.count()

# COMMAND ----------

#def f(x): print(x)

# COMMAND ----------

qwerty_filter = qwerty.filter(lambda x: 'spark' in x)

# COMMAND ----------

qwerty_filter.collect()

# COMMAND ----------

qwerty.collect()

# COMMAND ----------

qwerty_map=qwerty.map(lambda x: (x,1))

# COMMAND ----------

qwerty_map.collect()

# COMMAND ----------

demo123.collect()

# COMMAND ----------

demo123_map=demo123.map(lambda x: x+1)

# COMMAND ----------

demo123=sc.parallelize(["karthick","selvam"])

# COMMAND ----------

demo123.collect()

# COMMAND ----------

joined = qwerty.join(demo123)

# COMMAND ----------

x = sc.parallelize([("spark",1),("hadoop",4)])
y = sc.parallelize([("spark",2),("hadoop",3)])

# COMMAND ----------

joined=x.join(y)

# COMMAND ----------

joined.collect()

# COMMAND ----------



# COMMAND ----------

|
