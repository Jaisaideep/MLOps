# Databricks notebook source
#Crea6tion of Spark session for Data Frames
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrame").getOrCreate()

# COMMAND ----------

#Reading a file into SparkDF
df = spark.read.option("header",True).csv('/FileStore/tables/StudentData.csv') #including the header
df.show()

# COMMAND ----------

#inferSchema
df = spark.read.options(inferSchema = 'True', header ='True', delimiter = ',').csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------

#COnverting "Roll" column to String
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("age",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("name",StringType(),True),
    StructField("course",StringType(),True),
    StructField("roll",StringType(),True),
    StructField("marks",IntegerType(),True),
    StructField("email",StringType(),True),
])

# COMMAND ----------

df = spark.read.options(header ='True').schema(schema).csv('/FileStore/tables/StudentData.csv')
df.printSchema()

# COMMAND ----------

#Create DF from RDD
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("RDD")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
header = rdd.first() #ELiminating Headers
MapRdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: [int(x[0]),x[1],x[2],x[3],x[4],int(x[5]),x[6]])
MapRdd.collect()


# COMMAND ----------

#converting RDD to DF
columns = header.split(',')
DFrdd = MapRdd.toDF(columns )
DFrdd.printSchema()

#By default afer converting RDD to DF the scheman will be of string Datatype

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("age",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("name",StringType(),True),
    StructField("course",StringType(),True),
    StructField("roll",StringType(),True),
    StructField("marks",IntegerType(),True),
    StructField("email",StringType(),True),
])
DFrdd = spark.createDataFrame(MapRdd,schema=schema)
DFrdd.printSchema()
DFrdd.show()

#Schema got changed from the existing RDD's schema

# COMMAND ----------

#Select Columns in DataFrame
#method 1
df.select("name","gender").show() 

# COMMAND ----------

#method 2
df.select(df.name,df.gender).show()

# COMMAND ----------

#method 3
from pyspark.sql.functions import col
df.select(col("name"),col("gender")).show()

# COMMAND ----------

#method 4
df.columns #gives all column names inside the DF
df.select(df.columns[2],df.columns[3]).show()

# COMMAND ----------

df.select('*').show()

# COMMAND ----------

#withColumn in Spark DF
#lets convert 'roll' to string and int again
from pyspark.sql.functions import col
df.withColumn('roll',col('roll').cast("Int")).printSchema() #converting roll to int
df.withColumn('roll',col('roll').cast("String")).printSchema() #converting roll to string

# COMMAND ----------

#Whole column manipulation
df1 = df.withColumn('AddMarks',col('marks')+10) #newColumns Creation
df1.show()
df1.withColumn('IncMarks',(col('AddMarks')-col('marks'))).show() #mathematical Column operations

# COMMAND ----------

from pyspark.sql.functions import col,lit
df1.withColumn("Country",lit('India')).show()

# COMMAND ----------

#multiple Columns Operations
df.withColumn('marks',col('marks')-10).withColumn('Updated Marks',col('marks')+10).withColumn('DiffMarks',(col('Updated Marks')-(col('marks')))).withColumn("Country",lit("China")).show()

# COMMAND ----------

#Renaming column name
df.withColumnRenamed('gender','sex').withColumnRenamed('email','email address').show()

# COMMAND ----------

#Alias to manipulate while reading
df.select(col("name").alias("Student Name")).show()

# COMMAND ----------

#where/filer opeartins in df
df.select(col('name').alias('Boy Name'),"gender").where(col('gender')=='Male').show() #where

# COMMAND ----------

#filter 
df.filter((col('course')=='DB') & (col('marks')>50) & (col('gender')=='Male')).show()

# COMMAND ----------

#isin in filter
ReqCourses = ['DB','Cloud']
df.filter(col('course').isin(ReqCourses)).show()

# COMMAND ----------

#startsWith
df.filter((col('name').startswith('A')) & col('name').endswith('n')).show()

# COMMAND ----------

#like ----> 'A%' '%a%' '%a'
#same we have .contain()
df.filter(col('name').like('A%n')).show()

# COMMAND ----------

#Quiz
dfQuiz = df
dfQuiz.show()

# COMMAND ----------

dfQuiz = dfQuiz.withColumn('Total Marks',lit(120)).withColumn('Percentage',((col('marks')/(col('Total Marks')))*100))
dfQuiz.show()

# COMMAND ----------

OOPdf = dfQuiz.filter((col('course')=='OOP') & (col('Percentage')>80))
OOPdf.show()

# COMMAND ----------

Clouddf = dfQuiz.filter((col('course')=='Cloud') & (col('Percentage')>60))
Clouddf.show()

# COMMAND ----------

Clouddf.select(col('name'),col('marks')).show()
OOPdf.select(col('name'),col('marks')).show()

# COMMAND ----------

#count in spark df
print(df.count())
df.filter(col('course') == 'DB').count()

# COMMAND ----------

#distinct
print(df.distinct().count()) #---> to find the dunlicate rows in the dataset
df.select('course').distinct().show()
df.select('name').distinct().count()

# COMMAND ----------

#dropDublicates
df.dropDuplicates(['gender']).show()

# COMMAND ----------

#Quiz 2
dfQuiz2 = df
dfQuiz2.dropDuplicates(['age','gender','course']).show()

# COMMAND ----------

#orderBy
df.orderBy(col('marks').desc()).show()

# COMMAND ----------

#sorting
df.sort(col('marks').desc(),'age').show()

# COMMAND ----------

#Quiz 3
df = spark.read.options(inferSchema = 'True', header ='True', delimiter = ',').csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------

EmpDF = spark.read.option('header',True).csv('/FileStore/tables/OfficeData.csv')
EmpDF.show()
NewDF = EmpDF.withColumn('salary',col('salary').cast('Int')).withColumn('age',col('age').cast('Int')).withColumn('bonus',col('bonus').cast('Int'))
NewDF.printSchema()

# COMMAND ----------

#sort bonus in aesc
NewDF.orderBy(col('bonus').asc()).show()

# COMMAND ----------

#sort age and salary in desc and aesc resp.
NewDF.sort(col('age').desc(),col('salary').asc()).show()

# COMMAND ----------

#sort by age bonus and salary in desc desc aesc
NewDF.sort(col('age').desc(),col('salary').desc(),'salary').show()

# COMMAND ----------

#groupBy
#in deafult we dont have show() in Group by, we need to specify some aggrications here like sum(),count(),max(),min(),avg(),mean()
df.groupBy('age').count().show()
df.groupBy('gender').count().show()
df.groupBy('course').sum('marks').show()

# COMMAND ----------

df.groupBy('course').max('marks').show()
df.groupBy('course').mean('marks').show()

# COMMAND ----------

#grouping on various columns
df.groupBy('course','gender').count().show()

# COMMAND ----------

from pyspark.sql.functions import *
#multiple aggrigation in groupBy by making use of (.agg)
df.groupBy('course').agg(sum('marks').alias('Total Marks'),min('marks').alias('Min Marks'),max('marks').alias("Max Marks"),count('marks').alias("Total Students"),avg('marks').alias('Avg Marks')).show()

# COMMAND ----------

#filtering in groupBy 
dfGB = df.filter(col('gender')=='Male').groupBy('course','gender').agg(count('gender').alias('Enrolled')) #Filtering before perfoming groupBy by 'filter'
dfGB.show()
dfGB.where(col('Enrolled')>80).show() #Filtering after perfoming groupBy by 'where'

# COMMAND ----------

#Quiz 4
df.groupBy('course','gender').agg(count('name').alias('Enrolled'),sum('marks').alias('Total Marks')).show()

# COMMAND ----------

df.groupBy('course','age').agg(max('marks').alias('MaxMarks'),min('marks').alias('MinMarks'),avg('marks').alias('AvgMarks')).show()

# COMMAND ----------

#Quiz 5
dfQ5 = spark.read.text('/FileStore/tables/WordData.txt')
dfQ5.show()


# COMMAND ----------

dfQ5.groupBy('value').agg(count('value').alias('Count')).show()

# COMMAND ----------

#User Defined Function in Rdd:
def get_total_salary(salary,bonus):
    return salary+bonus

totalSalaryUDF = udf(lambda x,y: get_total_salary(x,y),IntegerType())

NewDF.withColumn('TotalSalary',totalSalaryUDF('salary','bonus')).show()

# COMMAND ----------

#UDF Quiz
NewDF.show()

# COMMAND ----------

from pyspark.sql.types import *
def increment(state,salary,bonus):
    sum = 0
    if state == 'NY':
        sum = salary * 0.1
        sum += (bonus * 0.05)
    elif state == 'CA':
        sum = salary * 0.12
        sum += bonus * 0.03
    return sum

incrementUDF = udf(lambda x,y,z: increment(x,y,z),DoubleType())

NewDF.withColumn('Increment',incrementUDF('state','salary','bonus')).show()

# COMMAND ----------

#cache and Presists
Cdf = df.groupBy('course','gender','age').count()
CdF = Cdf.withColumn('Dummy',col('age')+100)
Cdf.show()

# COMMAND ----------

Cdf.cache()

# COMMAND ----------

Cdf.show()

# COMMAND ----------

#Df to rdd
rdd = df.rdd 
rdd.filter(lambda x: x[0]==29).collect() #filterin gour the age == 29 years

# COMMAND ----------

#Spark SQL:-
df.createOrReplaceTempView('Students') #creation of table
spark.sql("SELECT * FROM Students WHERE course == 'DB' and gender == 'Female'").show() #Query

# COMMAND ----------

spark.sql("SELECT course, count(name) as Enrolled FROM Students GROUP BY course ORDER BY Enrolled").show()

# COMMAND ----------

#Writing DF (saving the data)
dfSave = df.write.mode("overwrite").option('header',True).csv('/FileStore/tables/StudentData/output')

# COMMAND ----------


