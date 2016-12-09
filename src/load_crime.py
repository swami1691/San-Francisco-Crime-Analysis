import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, StringType,LongType
from pyspark.sql.functions import split,regexp_replace

inputs = sys.argv[1]
output = sys.argv[2]
conf = SparkConf().setAppName('Crime Analysis-loading data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#Defining schema for crime dataset
crime_schema = StructType([
    StructField('IncidntNum', LongType(),False),
    StructField('Category', StringType(),False),
    StructField('Descript', StringType(), False),    
    StructField('DayOfWeek', StringType(),False),
    StructField('Date',      StringType(),False),
    StructField('Time', StringType(),False),   
    StructField('PdDistrict', StringType(),False),
    StructField('Resolution', StringType(),False),
    StructField('Address', StringType(), False),    
    StructField('X', StringType(),False), 
    StructField('Y', StringType(),False),
    StructField('Location', StringType(),False), 
    StructField('PdId', StringType(),False),
    ])

#laoding csv to dataframe
df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='true',nullValue='null', treatEmptyValuesAsNulls = 'true') \
    .load(inputs,schema=crime_schema)

#Data Cleaning 
split_col = split(df['Date'], '/')
df = df.withColumn('Month', split_col.getItem(0))
df = df.withColumn('Day', split_col.getItem(1))
df = df.withColumn('Year', split_col.getItem(2))
df = df.withColumn('Address', regexp_replace('Address', '/', '-'))
df = df.withColumn('Category', regexp_replace('Category', '/', '-'))
df = df.withColumn('Category', regexp_replace('Category', ',', '-'))
df = df.withColumn('Resolution', regexp_replace('Resolution', ',', ';'))
df = df.withColumn('Descript', regexp_replace('Category', ',', ';'))



#writing and reading from parquet file 
df.write.format('parquet').save(output,mode='overwrite')