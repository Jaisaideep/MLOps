# Databricks notebook source
#Mini Project - 1 SparkRDD

# COMMAND ----------

#Creation of RDD
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Projects") #for basic configuration
sc = SparkContext.getOrCreate(conf=conf) #for Spark Context creation

# COMMAND ----------

MP1 = sc.textFile('/FileStore/tables/StudentData.csv')
MP1.collect()

# COMMAND ----------

#Task 1: No of Students in the file
header = MP1.first()
MP = MP1.filter(lambda x: x!= header)   #for removing headers for perfoming Analysis
MP.count()

# COMMAND ----------

#Taks 2: Total Marks by Male and Female Student
TotalMarks = MP.map(lambda x: (x.split(',')[1],int(x.split(',')[5]))).reduceByKey(lambda x,y : x+y)
TotalMarks.collect()

# COMMAND ----------

#Task 3: Total Passed and Failed Students (50+ marks are required for Pass)
PF = MP.map(lambda x:int(x.split(',')[5]) )
passed = PF.filter(lambda x: x>50).count()
failed = PF.count() - passed
print(f"Passed Students: {passed} and Failed Students: {failed}")

# COMMAND ----------

#Task 4:Total Enrollments of students per Course
EnrollRdd = MP.map(lambda x: (x.split(',')[3],1)).reduceByKey(lambda x,y: x+y)
EnrollRdd.collect()

# COMMAND ----------

#Task 5: Total Marks per Course
TmRDD = MP.map(lambda x: (x.split(',')[3],int(x.split(',')[5]))).reduceByKey(lambda x,y: x+y)
TmRDD.collect()

# COMMAND ----------

#Task 6: Average marks per course
AvgRDD = MP.map(lambda x: (x.split(',')[3],(int(x.split(',')[5]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],x[1][0]/x[1][1]) )
AvgRDD.collect()

# COMMAND ----------

#Task 7: Max and Minimun marks in courses
MaxMinRDD = MP.map(lambda x: (x.split(',')[3],int(x.split(',')[5])))
print(MaxMinRDD.reduceByKey(lambda x,y: x if x>y else y).collect()) #Max marks 
print(MaxMinRDD.reduceByKey(lambda x,y: x if x<y else y).collect()) #Min marks

# COMMAND ----------

#Task 8: Average Age of Male and Female Students
AvgAgeRdd = MP.map(lambda x: (x.split(',')[1],(int(x.split(',')[0]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x[0]/x[1])
AvgAgeRdd.collect()

# COMMAND ----------

#Mini Project - 2 SparkDataFrame

# COMMAND ----------

#Crea6tion of Spark session for Data Frames
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrame").getOrCreate()

# COMMAND ----------

#majour Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.option('header',True).csv('/FileStore/tables/OfficeDataProject.csv')
df.printSchema()

#show change dataTypes for the Columns [salary,age,bonus]

# COMMAND ----------

#dataTypes Conversion
df = df.withColumn('salary',col('salary').cast("Int"))
df = df.withColumn('age',col('age').cast("Int"))
df = df.withColumn('bonus',col('bonus').cast("Int"))

df.printSchema()

# COMMAND ----------

#Task 1 : Print the total number of employees in the company
df.agg(count('*').alias("Total Employees")).show()

# COMMAND ----------

#Task 2: Print the total number of departments in the company
df.select('department').distinct().count()

# COMMAND ----------

#Task 3: Print the department names of the company
df.select('department').distinct().show()

# COMMAND ----------

#Task 4: Print the total number of employees in each department
df.groupBy('department').agg(count('employee_id').alias('Employees')).show()

# COMMAND ----------

#task 5: Print the total number of employees in each state
df.groupBy('state').agg(count('employee_id').alias('Employees')).show()

# COMMAND ----------

#task 6: Print the total number of employees in each state in each department
df.groupBy('state','department').agg(count('employee_id')).show()

# COMMAND ----------

#Task 6: Print the minimum and maximum salaries in each department and sort salaries in ascending order
df.groupBy('department').agg(min('salary').alias('MinSalary'),max('salary').alias('MaxSalary')).sort('MinSalary','MaxSalary').show()

# COMMAND ----------

#Task 7: Print the names of employees working in NY state under Finance department whose bonuses are greater than the average bonuses of employees in NY state

avg_bonus = df.where(col('state')=='NY').agg(avg('bonus').alias('avg_bonus')).collect()[0]['avg_bonus'] #saving in a variable
type(avg_bonus)
#saving the average value of bonus in a variable for comparison

# COMMAND ----------

#comparing the extracted value for getting results
#type a
df.select('employee_name','state','department','bonus').where((col('state')=='NY') & (col('department')=='Finance') & (col('bonus')>avg_bonus )).orderBy(col('bonus').asc()).show()

# COMMAND ----------

#type b
df.filter((col('state')=='NY') & (col('department')=='Finance') & (col('bonus')>avg_bonus )).select('employee_name','state','department','bonus').orderBy(col('bonus').asc()).show()

# COMMAND ----------

#Task 9: Raise the salaries $500 of all employees whose age is greater than 45
def increment(salary,age):
    if age >= 45:
        return salary+500
    else:
        return salary
UDF = udf(lambda x,y: increment(x,y),IntegerType())
df.withColumn('salary',UDF('salary','age')).show()

# COMMAND ----------

#Task 10: Create DF of all those employees whose age is greater than 45 and save them in a file
Df45 = df.filter(col('age')>45)
Df45.write.mode('overwrite').option('header',True).csv('/FileStore/tables/EmpData/Quizoutput')

# COMMAND ----------

df.show()

# COMMAND ----------


