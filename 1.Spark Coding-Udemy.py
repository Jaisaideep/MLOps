# Databricks notebook source
#Creation of RDD

# COMMAND ----------

from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("Read File")
#for basic configuration

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)
#for Spark Context creation

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_.txt')
#Reading the Textfile

# COMMAND ----------

rdd.collect()

# COMMAND ----------

#RDD FUNCTIONS

# COMMAND ----------

#MAP Function (lanbda)
rdd2 = rdd.map(lambda x: x.split(' '))

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3 = rdd.map(lambda x: x + " jai")
rdd3.collect()

# COMMAND ----------

#Map (simple Function)

# COMMAND ----------

def foo(x):  #'x' is a parameter in the Function 'foo'
    return x.split(' ')

rdd4 = rdd.map(foo)
rdd4.collect()

# COMMAND ----------

def foo(x):
  l = x.split()
  l2 = []
  for s in l:
    l2.append(int(s) + 10)
  return l2

rdd5 = rdd.map(foo)
rdd5.collect()

# COMMAND ----------

#Quich Quiz

# COMMAND ----------

rddQuiz = sc.textFile('/FileStore/tables/quickQuiz.txt')
rddQuiz.collect()

# COMMAND ----------

def quiz(x):
    l = x.split(' ')
    l2 = []
    for s in l:
        l2.append(len(s))
    return l2
QuizSol = rddQuiz.map(quiz)
QuizSol.collect()


# COMMAND ----------

QuizSol2 = rddQuiz.map(lambda x: [len(s) for s in x.split(' ')])
QuizSol2.collect()

# COMMAND ----------

#FlatMap():

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rddMap = rdd.map(lambda x: x.split())
rddMap.collect()

# COMMAND ----------

#flatMapping
rddFlat = rdd.flatMap(lambda x: x.split())
rddFlat.collect()

# COMMAND ----------

#Filter Function
rdd.collect()

# COMMAND ----------

rddFilter = rdd.filter(lambda x: x!='123 312 3123 14')
rddFilter.collect()

# COMMAND ----------

def filterFoo(x):
    if x == '123 312 3123 14':
        return False
    else:
        return True

rddFilter = rdd.filter(filterFoo)
rddFilter.collect()

# COMMAND ----------

#Filter Quiz
filterQuizRDD = sc.textFile('/FileStore/tables/filterQuiz.txt')
filterQuizRDD.collect()

# COMMAND ----------

filterQuizRDD2 = filterQuizRDD.flatMap(lambda x:x.split(' '))  
filterQuizRDD2.collect()


# COMMAND ----------

#in Function method
def filterAorC(x):
    if x.startswith('a') or x.startswith('c'):
        return False
    else:
        return True
        
filteredRDD = filterQuizRDD2.filter(filterAorC)
filteredRDD.collect()

# COMMAND ----------

#in RDD Filter method
filteredRDD2 = filterQuizRDD2.filter(lambda x: not (x.startswith('a') or x.startswith('c')))
filteredRDD2.collect()

# COMMAND ----------

#RDD Distinct Funtion
DisRDD = sc.textFile('/FileStore/tables/Distinct.txt')
DisRDD.collect()

# COMMAND ----------

DisRDD2 = DisRDD.flatMap(lambda x: x.split(' '))
DisRDD2.collect()

# COMMAND ----------

DisRDDFinal = DisRDD2.distinct()
DisRDDFinal.collect()

# COMMAND ----------

#or in simple way you can do the below technique 
DisRDD = sc.textFile('/FileStore/tables/Distinct.txt')
DisRDD.flatMap(lambda x: x.split(' ')).distinct().collect()

# COMMAND ----------

#groupByKey Funtion:-
GroupRdd = sc.textFile('/FileStore/tables/filterQuiz.txt')
GroupRdd.collect()

# COMMAND ----------

#Converting data into (Key,Value) Pairs
#rdd.groupByKey().mapValues(list) -----> Syntax for GroupBYKey function
GroupRdd.map(lambda x: (x,len(x.split(' ')))).collect()

# COMMAND ----------

GroupRdd2 = GroupRdd.map(lambda x: (x,len(x.split(' '))))
rddInverted = GroupRdd2.map(lambda x: (x[1], x[0]))
rddInverted.groupByKey().mapValues(list).collect()

# COMMAND ----------

GroupRdd2 = GroupRdd.flatMap(lambda x: x.split(' '))
MapRDD = GroupRdd2.map(lambda x: (x,len(x)))
rddInverted2 = MapRDD.map(lambda x: (x[1], x[0]))
rddInverted2.collect()

# COMMAND ----------

rddInverted.groupByKey().mapValues(list).collect()

# COMMAND ----------

#reduceByKey Function in RDD:-
RedRDD = rddInverted2
RedRDD.collect()

# COMMAND ----------

RedRDD.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

GroupRdd2 = GroupRdd.flatMap(lambda x: x.split(' '))
MapRDD = GroupRdd2.map(lambda x: (x,len(x)))
MapRDD.collect()

# COMMAND ----------

MapRDD.groupByKey().mapValues(list).collect()

# COMMAND ----------

MapRDD.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

#Quiz Word Count
QuizRddWC = sc.textFile('/FileStore/tables/QuizWordCount.txt')
QuizFlatMap = QuizRddWC.flatMap(lambda x: x.split(' '))
QuizMap = QuizFlatMap.map(lambda x: (x,1))
QuizMap.collect()

# COMMAND ----------

QuizMap.groupByKey().mapValues(list).collect()

# COMMAND ----------

QuizMap.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

#Samething in single line
QuizRddWC.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

#count() ---> action
rddCount = sc.textFile('/FileStore/tables/QuizWordCount.txt')
rddCount.collect()

# COMMAND ----------

rddCount.count()

# COMMAND ----------

rddCount.flatMap(lambda x: x.split(' ')).count()

# COMMAND ----------

#countByValue() ---> This action gives the number of times of elements inside RDD
rddCountofValues = rddCount.flatMap(lambda x: x.split(' '))
rddCountofValues.countByValue()

# COMMAND ----------

#saving a Rdd in a text File
rddCountofValues.saveAsTextFile('/FileStore/tables/AllOutputs/CountValuesOutput.txt')

# COMMAND ----------

#Partitions in RDD ---> repartition() and coalesce() 
rddCount.getNumPartitions()

# COMMAND ----------

rddCount.repartition(5).getNumPartitions()

# COMMAND ----------

rddCoal = rddCount.coalesce(3)
rddCoal.getNumPartitions()

# COMMAND ----------

#movie Rating Exercise
movieRdd = sc.textFile('/FileStore/tables/movie_rating.csv')
movieRdd.collect()

# COMMAND ----------

movieRdd2 = movieRdd.map(lambda x: (x.split(',')[0],(int(x.split(',')[1]),1)))
movieRdd2.collect()

# COMMAND ----------

movieRdd2.groupByKey().mapValues(list).collect()

# COMMAND ----------

movieRdd3 = movieRdd2.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
movieRdd3.collect()

# COMMAND ----------

movieRdd3.map(lambda x: (x[0],x[1][0]/x[1][1])).collect()

# COMMAND ----------

#Average Quiz for finding the average of months score in different cities
AvgQuizRdd = sc.textFile('/FileStore/tables/average_quiz_sample-1.csv')
AvgQuizRdd.collect()

# COMMAND ----------

AvgQuizRdd2 = AvgQuizRdd.map(lambda x: (x.split(',')[0],(float(x.split(',')[2]),1)))
AvgQuizRdd2.collect()

# COMMAND ----------

AvgQuizReduce = AvgQuizRdd2.reduceByKey(lambda x,y: ((x[0]+y[0]),(x[1]+y[1])))
AvgQuizReduce.collect()

# COMMAND ----------

AvgQuizReduce.map(lambda x: (x[0],(x[1][0]/x[1][1]))).collect()

# COMMAND ----------

#Min and Max
movieRdd = sc.textFile('/FileStore/tables/movie_rating.csv')
movieRdd.map(lambda x: (x.split(',')[0],(int(x.split(',')[1])))).reduceByKey(lambda x,y : x if x<y else y).collect()

# COMMAND ----------

#MinMax Quiz
#Max Rating
MinMaxQuiz = sc.textFile('/FileStore/tables/average_quiz_sample-1.csv')
MinMaxQuiz.map(lambda x: (x.split(',')[1],float(x.split(',')[2]))).reduceByKey(lambda x,y: x if x>y else y)).collect()

# COMMAND ----------

#Min Rating
MinMaxQuiz.map(lambda x: (x.split(',')[1],float(x.split(',')[2]))).reduceByKey(lambda x,y: x if x<y else y).collect()

# COMMAND ----------


