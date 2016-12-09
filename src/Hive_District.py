import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext,HiveContext
from pyspark.sql.functions import *

inputs=sys.argv[1]
conf = SparkConf().setAppName('District wise Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)



crime=sqlContext.read.parquet(inputs)
crime.registerTempTable('Crime')

Crime_District=sqlContext.sql('''select  PdDistrict,Category,count(Category) as cnt
    from Crime group by PdDistrict,Category order by PdDistrict
    ''')

Crime_District.registerTempTable('Crime_District')

Crime_count=sqlContext.sql('''select * from Crime_District where cnt<>1''')
#loading only aggregated records to save in csv so that hive can Query only less amount of records .
Crime_count.coalesce(1).write.format('com.databricks.spark.csv').save('District_top')

# Using Hive and creating table from csv
hiveContext.sql("DROP TABLE IF EXISTS district")
hiveContext.sql("CREATE TABLE district (PdDistrict STRING, Category STRING, counts int) row format delimited fields terminated by ',' stored as textfile")
#loading csv contents into csv
hiveContext.sql("LOAD DATA INPATH '/user/chandras/District_top' INTO TABLE district")

District=hiveContext.sql("SELECT PdDistrict,Category,counts FROM (SELECT PdDistrict,Category,counts,dense_rank() OVER (PARTITION BY PdDistrict ORDER BY counts DESC) as rank FROM district) tmp WHERE rank <= 3");

#coealescing operation is safe to do  as results will not be affected 
District.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('District_crime_top3')


#hiveContext.sql("insert overwrite local directory '/user/chandras' row format delimited fields terminated by ',' select * from SFPD_CRIME limit 2")


