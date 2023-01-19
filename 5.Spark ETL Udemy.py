# Databricks notebook source
dbutils.fs.rm('/FileStore/tables/',True)

#clearning the Datasets inside Filestore-tables because Spark streaming DF will read all the files inside the Dumpstorage but in case of Spark streaming RDD its different

# COMMAND ----------

#needed imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f

# COMMAND ----------

#Step 1 ---> EXTRACT from a source
spark = SparkSession.builder.appName("ETL Pipelin").getOrCreate() #Creation of spark session
df = spark.read.text('/FileStore/tables/WordDataETL.txt')
df.show()

# COMMAND ----------

#Step 2 ----> TRANSFORMations
df2 = df.withColumn('splitedData', f.split('value', ' '))
df2.show()

# COMMAND ----------

df3 = df2.withColumn('splitedData', explode('splitedData'))
df3 = df3.select('splitedData')
df3.show()

# COMMAND ----------

df3.groupBy('splitedData').count().show()

# COMMAND ----------


