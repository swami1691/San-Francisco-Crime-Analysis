import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
import datetime

inputPath = '/user/ssambasi/SFPD_parquet'
conf = SparkConf().setAppName('Incident Severity')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crimeDF =sqlContext.read.parquet(inputPath)
crimeRDD = crimeDF.rdd.cache()

outputPath = '/user/ssambasi/sfo/IncidentWeights/'

#GEt Weights for incidents
incidentWeightsRDD = crimeRDD.map(lambda row: ((row['IncidntNum'],row['PdDistrict'],row['Year'],row['X'],row['Y']),1))\
                    .reduceByKey(lambda c1,c2:c1+c2)

incidentWeightsDF = incidentWeightsRDD.map(lambda((iid,dist,year,x,y),c): Row(IncidentID=iid,District=dist,Year=year,X=x,Y=y,Weight=c)).toDF()

incidentWeightsDF.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save(outputPath)
