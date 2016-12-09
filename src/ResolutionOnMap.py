import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
import datetime

inputPath = '/user/ssambasi/SFPD_parquet'
conf = SparkConf().setAppName('Incidents based on Resolution')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crimeDF =sqlContext.read.parquet(inputPath)

outputPath = '/user/ssambasi/sfo/ResolutionOnMap/'

#Get Incidents based on resolution
incidentResolutionDF = crimeDF.select(col('IncidntNum').alias('IncidentID'),col('PdDistrict').alias('District'),'Year','Resolution','X','Y').distinct()

incidentResolutionDF.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save(outputPath)
