# Databricks notebook source
#needed imports
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Streaming') #creation of Congiguration
sc = SparkContext.getOrCreate(conf=conf) #creation of sprk context

ssc = StreamingContext(sc, 1) #creation of Spark Streaming Context

# COMMAND ----------

#Creation of RDD in Spark Streaming
rdd = ssc.textFileStream('/FileStore/tables/')

# COMMAND ----------

rdd.pprint()
ssc.start() #satrting the streaming of Data
ssc.awaitTerminationOrTimeout(100) #Terminating after 100 seconds

# COMMAND ----------

#Transformations with RDD
rdd = rdd.map(lambda x: (x,1) ).reduceByKey(lambda x,y: x+y)
rdd.pprint()
ssc.start() #satrting the streaming of Data
ssc.awaitTerminationOrTimeout(100) #Terminating after 100 seconds


# COMMAND ----------


