import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
import datetime

inputPath = '/user/ssambasi/SFPD_parquet'
conf = SparkConf().setAppName('District wise Resolution')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crimeDF =sqlContext.read.parquet(inputPath)

outputPath = '/user/ssambasi/sfo/DistrictResolution'

#Get count of incidents based on resolution
districtResolutionRDD = crimeDF.select('PdDistrict','Resolution').rdd

districtResolutionCountRDD = districtResolutionRDD.map(lambda r: ((r[0], r[1]),1))\
    .reduceByKey(lambda c1,c2: c1+c2)
    
districtResolutionCountDF = districtResolutionCountRDD.map(lambda ((dist,res),count): Row(District=dist,Resolution=res,IncidentsCount=count)).toDF()

districtResolutionCountDF.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save(outputPath)
