import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime,timedelta
from pyspark.mllib.regression import LabeledPoint,LinearRegressionWithSGD
from pyspark.mllib.feature import HashingTF

inputPath = '/user/ssambasi/SFPD_parquet'
conf = SparkConf().setAppName('Crime Count Prediction')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crimeDF =sqlContext.read.parquet(inputPath).cache()

htf = HashingTF(5000)

#Get training data (till year 2013)
trainDataRDD = crimeDF.where(crimeDF.Year < 2014).select('PdDistrict','Date').rdd.map(lambda r: ((r[0],r[1]),1))
trainDataRDD = trainDataRDD.reduceByKey(lambda c1,c2 : c1+c2).map(lambda ((district,date),count): LabeledPoint(float(count),htf.transform((district,date)))).cache()

#Train model using LinearRegression 
lrm = LinearRegressionWithSGD.train(trainDataRDD, iterations=150,regParam=0.2,step=6.4)

#Evaluate model for training data
labelsAndPreds = trainDataRDD.map(lambda p: (int(p.label), int(lrm.predict(p.features))))

#Exception threshold for model
c = 3
trainAcc = (labelsAndPreds.filter(lambda (v, p): ((v+c>=p) and (v-c<=p))).count() / float(trainDataRDD.count()))*100
print("Training Accuracy = " + str(trainAcc))

#Get validation data (rest)
valDataRDD = crimeDF.where(crimeDF.Year > 2013).select('PdDistrict','Date').rdd.map(lambda r: ((r[0],r[1]),1))
valDataRDD = valDataRDD.reduceByKey(lambda c1,c2 : c1+c2).map(lambda ((district,date),count): LabeledPoint(float(count),htf.transform((district,date)))).cache()

#Evaluate the model
labelsAndPreds = valDataRDD.map(lambda p: (int(p.label), int(lrm.predict(p.features))))
valAcc = (labelsAndPreds.filter(lambda (v, p): ((v+c>=p) and (v-c<=p))).count() / float(valDataRDD.count()))*100
print("Validation Accuracy = " + str(valAcc))

#Save the model
lrm.save(sc,'/user/ssambasi/sfo/CrimeCountPredictionModel')
