# Databricks notebook source
dbutils.fs.rm('/FileStore/tables/',True)

#clearning the Datasets inside Filestore-tables because Spark streaming DF will read all the files inside the Dumpstorage but in case of Spark streaming RDD its different

# COMMAND ----------

#needed imports
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Streaming DF").getOrCreate() #Creation of spark session
word = spark.readStream.text('/FileStore/tables') #initaion of Dumpdata path 
word.writeStream.format("console").outputMode("append").start() #get Data out

# COMMAND ----------

#displaying data in Table Format
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Streaming DF").getOrCreate() #Creation of spark session
word = spark.readStream.text('/FileStore/tables') #initaion of Dumpdata path 

# COMMAND ----------

display(word)

# COMMAND ----------

#transformations in Streaming DataFrames
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Streaming DF").getOrCreate() #Creation of spark session
word = spark.readStream.text('/FileStore/tables') #initaion of Dumpdata path 
word = word.groupBy('value').count()

# COMMAND ----------

display(word)
