# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName('Collaboative Filtering').getOrCreate()

# COMMAND ----------

#importing Data and assining data into dataframes
movieDF = spark.read.options(header='True',inferSchema = 'True').csv('/FileStore/tables/movies.csv')
ratingDF = spark.read.options(header='True',inferSchema = 'True').csv('/FileStore/tables/ratings.csv')
movieDF.show()
ratingDF.show()

# COMMAND ----------

movieDF.printSchema()
ratingDF.printSchema()

# COMMAND ----------

#Joining DataFrames
JoinDf = ratingDF.join(movieDF,'movieId','left')
display(JoinDf)

# COMMAND ----------

#Train and Test Data
(train,test) = JoinDf.randomSplit([0.8,0.2])

# COMMAND ----------

print(JoinDf.count())
print(train.count())
print(test.count())
train.show()

# COMMAND ----------

#Model Creation
from pyspark.ml.recommendation import ALS
alsModel = ALS(userCol="userId",itemCol="movieId",ratingCol="rating",nonnegative=True,implicitPrefs=False,coldStartStrategy='drop')

# COMMAND ----------

#Hyperparameter Tunining and Cross Validating
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
alsModel = ALS(userCol="userId",itemCol="movieId",ratingCol="rating",nonnegative=True,implicitPrefs=False,coldStartStrategy='drop')

# COMMAND ----------

#Step 1:Paramgrid build:
param_grid = ParamGridBuilder() \
             .addGrid(alsModel.rank, [10,50,100,150]) \
             .addGrid(alsModel.regParam, [.01,.05,.1,.15]) \
             .build()

len(param_grid)
#creates 4x4=16 types of models

# COMMAND ----------

#Step 2:Regression Evaulator:
evaluator = RegressionEvaluator(
            metricName='rmse',
            labelCol='rating',
            predictionCol='prediction')


# COMMAND ----------

#Step 3:Cross Validator:
cv = CrossValidator(estimator=alsModel, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5)

# COMMAND ----------

#Best model and evaluate predictions:
model = cv.fit(train)  #fitting the model
bestModel = model.bestModel #getting the Best Model among all available models
testPreductions = bestModel.transform(test) #testing the Best Model
RMSE = evaluator.evaluate(testPreductions)
print(RMSE)

# COMMAND ----------

print(RMSE)
