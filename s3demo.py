# Databricks notebook source
import pyspark

# COMMAND ----------

csvdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3://cdb20dw061-flightdata/Flightdata.csv")

# COMMAND ----------

print(csvdf)

# COMMAND ----------

type(csvdf)

# COMMAND ----------

csvdf.printSchema()

# COMMAND ----------

csvdf.count()

# COMMAND ----------

csvdf.take(2)

# COMMAND ----------

csvdf.columns

# COMMAND ----------

len(csvdf.columns)

# COMMAND ----------

csvdf.show()

# COMMAND ----------

csvdf.registerTempTable( "flightcsv" )

# COMMAND ----------

sqlContext.sql('select * from flightcsv limit 100').show(100)

# COMMAND ----------

query1 = sqlContext.sql("select Dest,TailNum,count(tailnum) as Total_count from flightcsv where TailNum != 'NA' and TailNum  NOT LIKE '0%' group by Dest, TailNum order by Total_count desc,dest")

# COMMAND ----------

query1.collect()

# COMMAND ----------

query1.show()

# COMMAND ----------


