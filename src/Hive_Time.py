import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext,HiveContext
from pyspark.sql.functions import *

inputs=sys.argv[1]
conf = SparkConf().setAppName('Time-wise Top 3 Crimes')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


crime=sqlContext.read.parquet(inputs)
crime.registerTempTable('Crime')

Crime_Time=sqlContext.sql('''select  SUBSTR(Time,1,2) as hour,Category,count(Category) as cnt
    from Crime group by SUBSTR(Time,1,2),Category order by SUBSTR(Time,1,2)
    ''')

Crime_Time.registerTempTable('Crime_Time')
#loading only aggregated records to save in csv so that hive can Query only less amount of records .
Crime_Time.coalesce(1).write.format('com.databricks.spark.csv').save('TimeCategory')

# Using Hive and creating table from csv
hiveContext.sql("DROP TABLE IF EXISTS TimeCategory")
hiveContext.sql("CREATE TABLE TimeCategory (Hour STRING, Category STRING, counts int) row format delimited fields terminated by ',' stored as textfile")
#loading csv contents into csv
hiveContext.sql("LOAD DATA INPATH '/user/chandras/TimeCategory' INTO TABLE TimeCategory")


Time=hiveContext.sql("SELECT Hour,Category,counts FROM (SELECT Hour,Category,counts,dense_rank() OVER (PARTITION BY Hour ORDER BY counts DESC) as rank FROM TimeCategory) tmp WHERE rank <= 3");

#coealescing operation is safe to do  as results will not be affected 
Time.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('TimeCategory_rank')




