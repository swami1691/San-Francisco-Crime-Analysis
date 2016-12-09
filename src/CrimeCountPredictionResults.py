import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime,timedelta
from pyspark.mllib.regression import LabeledPoint,LinearRegressionWithSGD,LinearRegressionModel
from pyspark.mllib.feature import HashingTF

inputPath = '/user/ssambasi/SFPD_parquet'
conf = SparkConf().setAppName('Predict Crime Count')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crimeDF =sqlContext.read.parquet(inputPath).cache()

htf = HashingTF(5000)

#Load the model
lrm = LinearRegressionModel.load(sc, '/user/ssambasi/sfo/CrimeCountPredictionModel')

#Load test data for demo
districtRDD = crimeDF.select('PdDistrict').distinct().rdd.filter(lambda r:r[0]!='').map(lambda r:r[0]).cache()
startDate = datetime.now()
dateList = []
for dateIndex in range(0,30):
    dateList.append(startDate + timedelta(days=dateIndex))
dateRDD = sc.parallelize(dateList).cache()
testDataRDD = districtRDD.cartesian(dateRDD).map(lambda (district,date): \
                ((district,date),LabeledPoint(1.0,htf.transform((district,date))))).cache()

#Predict Crime Count using the model
predictionDF = testDataRDD.map(lambda ((district,date),p): \
                  Row(District=district, Date=date, Count=int(lrm.predict(p.features)))).toDF().cache()

outputPath = '/user/ssambasi/sfo/CrimeCountPredictionResults'
predictionDF.coalesce(1).write.mode('append').json(outputPath)
